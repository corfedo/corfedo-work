using System.Globalization;
using Corfedo.Work.SqlServer.Leases;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Calendars;

/// <summary>
/// Calendars by revision and published date sets in the framework's tables. Evaluators are built from the stored rows
/// read in the caller's transaction; a process-local cache keeps the parsed calendars and date sets by revision, so a
/// read costs one statement over the revisions while nothing changed. A calendar or date set is written under an
/// exclusive applock on its name, which also serializes the first write of a new name.
/// </summary>
internal sealed class SqlServerCalendarStore(string connectionString, SqlServerLeaseStore leases, string schema) : ICalendarStore
{
    private readonly string calendars = Tsql.Quote(schema) + ".calendars";
    private readonly string dates = Tsql.Quote(schema) + ".calendar_dates";
    private readonly string jobs = Tsql.Quote(schema) + ".jobs";
    private readonly string recurrences = Tsql.Quote(schema) + ".job_recurrences";
    private readonly string processes = Tsql.Quote(schema) + ".processes";
    private readonly string processCalendars = Tsql.Quote(schema) + ".process_calendars";
    private Snapshot cached = new([], [], new CalendarEvaluator([], []));

    public async ValueTask<WorkCalendar> StoreCalendarAsync(WorkCalendar calendar, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(calendar);
        await using var connection = await Tsql.OpenAsync(connectionString, cancellationToken);
        await using var transaction = await SqlServerLeaseStore.BeginAsync(connection, cancellationToken);
        WorkCalendar? stored = null;
        byte[]? digest = null;
        await using (var select = Tsql.Command(connection, transaction,
            LeaseSql.AppLock("@lock", "Exclusive") + $"SELECT revision, digest, [rule] FROM {calendars} WHERE name = @name;",
            Tsql.Text("@lock", Tsql.LockName("calendar", schema, calendar.Name), 255),
            Tsql.Text("@name", calendar.Name, 128)))
        {
            await using var reader = await select.ReaderAsync(cancellationToken);
            if (await reader.NextRowAsync(cancellationToken))
            {
                stored = WorkCalendar.Parse(calendar.Name, reader.GetInt64(0), reader.GetString(2));
                digest = reader.GetFieldValue<byte[]>(1);
            }
        }
        if (stored is not null && stored.Revision > calendar.Revision)
        {
            return stored;
        }
        if (stored is not null && stored.Revision == calendar.Revision)
        {
            if (!digest.AsSpan().SequenceEqual(calendar.Digest))
            {
                throw new CalendarConflictException(
                    $"Calendar {calendar.Name} revision {calendar.Revision} is stored with a different rule; increase the revision when the rule changes.");
            }
            return stored;
        }
        await using (var write = Tsql.Command(connection, transaction, stored is null
                ? $"INSERT INTO {calendars} (name, revision, digest, [rule]) VALUES (@name, @revision, @digest, @rule);"
                : $"UPDATE {calendars} SET revision = @revision, digest = @digest, [rule] = @rule, stored_at = SYSUTCDATETIME() WHERE name = @name;",
            Tsql.Text("@name", calendar.Name, 128),
            Tsql.Int64("@revision", calendar.Revision),
            Tsql.Bytes("@digest", calendar.Digest),
            Tsql.Text("@rule", calendar.Rule, -1)))
        {
            await write.NonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        return calendar;
    }

    public async ValueTask<IReadOnlyList<WorkCalendar>> GetCalendarsAsync(CancellationToken cancellationToken)
    {
        await using var connection = await Tsql.OpenAsync(connectionString, cancellationToken);
        await using var command = Tsql.Command(connection, null, $"SELECT name, revision, [rule] FROM {calendars} ORDER BY name;");
        var result = new List<WorkCalendar>();
        await using var reader = await command.ReaderAsync(cancellationToken);
        while (await reader.NextRowAsync(cancellationToken))
        {
            result.Add(WorkCalendar.Parse(reader.GetString(0), reader.GetInt64(1), reader.GetString(2)));
        }
        return result;
    }

    public async ValueTask<IReadOnlyList<CalendarDates>> GetDateSetsAsync(CancellationToken cancellationToken)
    {
        await using var connection = await Tsql.OpenAsync(connectionString, cancellationToken);
        await using var command = Tsql.Command(connection, null, $"SELECT name, revision, covers_through, dates FROM {dates} ORDER BY name;");
        var result = new List<CalendarDates>();
        await using var reader = await command.ReaderAsync(cancellationToken);
        while (await reader.NextRowAsync(cancellationToken))
        {
            result.Add(ReadDates(reader));
        }
        return result;
    }

    /// <summary>
    /// Locks the calendar, which holds off new references, and deletes it unless live work uses it: outstanding jobs,
    /// recurrences or another stored calendar composing with it. The refusal names them, with the outstanding jobs
    /// counted per definition. Ended jobs keep the name without a reference and do not block.
    /// </summary>
    public async ValueTask<bool> RemoveCalendarAsync(string name, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(name);
        await using var connection = await Tsql.OpenAsync(connectionString, cancellationToken);
        await using var transaction = await SqlServerLeaseStore.BeginAsync(connection, cancellationToken);
        var composing = new List<string>();
        var jobCounts = new List<string>();
        int recurrenceCount;
        await using (var command = Tsql.Command(connection, transaction, $"""
            DECLARE @found int;
            SELECT @found = 1 FROM {calendars} WITH (UPDLOCK, ROWLOCK) WHERE name = @name;
            SELECT @found;
            SELECT name, revision, [rule] FROM {calendars} WHERE name <> @name ORDER BY name;
            SELECT definition, COUNT(*) FROM {jobs} WHERE calendar = @name GROUP BY definition ORDER BY definition;
            SELECT COUNT(*) FROM {recurrences} WHERE calendar = @name;
            """,
            Tsql.Text("@name", name, 128)))
        {
            await using var reader = await command.ReaderAsync(cancellationToken);
            await reader.NextRowAsync(cancellationToken);
            if (reader.IsDBNull(0))
            {
                return false;
            }
            await reader.NextSetAsync(cancellationToken);
            while (await reader.NextRowAsync(cancellationToken))
            {
                if (WorkCalendar.Parse(reader.GetString(0), reader.GetInt64(1), reader.GetString(2)).References.Contains(name, StringComparer.Ordinal))
                {
                    composing.Add(reader.GetString(0));
                }
            }
            await reader.NextSetAsync(cancellationToken);
            while (await reader.NextRowAsync(cancellationToken))
            {
                jobCounts.Add(string.Create(CultureInfo.InvariantCulture, $"{reader.GetString(0)} ({reader.GetInt32(1)})"));
            }
            await reader.NextSetAsync(cancellationToken);
            await reader.NextRowAsync(cancellationToken);
            recurrenceCount = reader.GetInt32(0);
        }
        var blockers = new List<string>();
        if (jobCounts.Count > 0)
        {
            blockers.Add("outstanding jobs of " + string.Join(", ", jobCounts));
        }
        if (recurrenceCount > 0)
        {
            blockers.Add(string.Create(CultureInfo.InvariantCulture, $"{recurrenceCount} recurrences"));
        }
        if (composing.Count > 0)
        {
            blockers.Add("calendars " + string.Join(", ", composing) + " composing with it");
        }
        if (blockers.Count > 0)
        {
            throw new InvalidOperationException($"Calendar {name} cannot be removed while live work uses it: {string.Join("; ", blockers)}.");
        }
        await using (var delete = Tsql.Command(connection, transaction, $"DELETE FROM {calendars} WHERE name = @name;", Tsql.Text("@name", name, 128)))
        {
            await delete.NonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    /// <summary>Records the calendars a registered process registers; calendars it already recorded are left as they are.</summary>
    public async ValueTask RegisterProcessAsync(OwnerId owner, IReadOnlyList<WorkCalendar> registered, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(registered);
        if (registered.Count == 0)
        {
            return;
        }
        await using var connection = await Tsql.OpenAsync(connectionString, cancellationToken);
        await using var command = Tsql.Command(connection, null, Tsql.IgnoreUnique($"""
            INSERT INTO {processCalendars} (owner, calendar, revision)
            SELECT @owner, c.calendar, MAX(c.revision) FROM OPENJSON(@calendars) WITH (calendar nvarchar(128) '$[0]', revision bigint '$[1]') AS c
            WHERE NOT EXISTS (SELECT 1 FROM {processCalendars} AS e WHERE e.owner = @owner AND e.calendar = c.calendar)
            GROUP BY c.calendar;
            """),
            Tsql.Guid("@owner", owner.Value),
            Tsql.Json("@calendars", "[" + string.Join(",", registered.Select(c => "[" + System.Text.Json.JsonSerializer.Serialize(c.Name, SqlServerJson.Default.String) + "," + c.Revision.ToString(CultureInfo.InvariantCulture) + "]")) + "]"));
        await command.NonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Stored calendars that no live process registers, in one statement: a process is live while it has not stopped
    /// and its heartbeat plus its own stale window is after the statement's clock reading.
    /// </summary>
    public async ValueTask<IReadOnlyList<UnregisteredCalendar>> GetUnregisteredCalendarsAsync(CancellationToken cancellationToken)
    {
        await using var connection = await Tsql.OpenAsync(connectionString, cancellationToken);
        await using var command = Tsql.Command(connection, null, $"""
            DECLARE @now datetime2(7) = SYSUTCDATETIME();
            SELECT c.name, c.revision, c.stored_at FROM {calendars} AS c WHERE NOT EXISTS (SELECT 1 FROM {processCalendars} AS pc
            JOIN {processes} AS p ON p.owner = pc.owner WHERE pc.calendar = c.name AND p.stopped_at IS NULL AND {Tsql.AddTicks("p.heartbeat_at", "p.stale_after")} > @now)
            ORDER BY c.name;
            """);
        var list = new List<UnregisteredCalendar>();
        await using var reader = await command.ReaderAsync(cancellationToken);
        while (await reader.NextRowAsync(cancellationToken))
        {
            list.Add(new UnregisteredCalendar(reader.GetString(0), reader.GetInt64(1), Tsql.Time(reader, 2)));
        }
        return list;
    }

    /// <summary>
    /// Every published date set, and every one a stored calendar uses, with the calendars that use it, directly or
    /// through composition, as one transaction sees them. Work waits for a set when an outstanding job of such a calendar
    /// waits for dates and the set is unpublished or, among the sets that calendar uses, covers the fewest dates. A set
    /// runs out when its coverage ends within <paramref name="margin"/> of now, of an instant still to be planned of an
    /// outstanding job of such a calendar, or of a next instant of such a recurrence within the lookahead; an
    /// unpublished set always does. Every check is an existence check on <c>jobs_calendar_unavailable</c>,
    /// <c>jobs_calendar</c> and <c>job_recurrences_calendar</c>.
    /// </summary>
    public async ValueTask<IReadOnlyList<DateSetCoverage>> GetDateSetCoverageAsync(TimeSpan margin, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(margin, TimeSpan.Zero);
        await using var connection = await Tsql.OpenAsync(connectionString, cancellationToken);
        await using var transaction = await SqlServerLeaseStore.BeginAsync(connection, cancellationToken);
        var work = new SqlServerTransaction(transaction);
        var snapshot = await LoadSnapshotAsync(work, cancellationToken);
        var names = snapshot.Dates.Keys.Concat(snapshot.Calendars.Values.SelectMany(c => c.DateSets)).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
        if (names.Length == 0)
        {
            return [];
        }
        var users = names.ToDictionary(n => n, n => snapshot.Evaluator.DependingOn(n).Order(StringComparer.Ordinal).ToArray(), StringComparer.Ordinal);
        var waiting = new HashSet<string>(StringComparer.Ordinal);
        var needed = new HashSet<string>(StringComparer.Ordinal);
        var published = names.Where(snapshot.Dates.ContainsKey).ToArray();
        var text = new System.Text.StringBuilder($"""
            SELECT c.value FROM OPENJSON(@users) AS c WHERE EXISTS (SELECT 1 FROM {jobs} AS j WHERE j.calendar = c.value AND j.calendar_unavailable = 1 AND j.state = 0);

            """);
        var parameters = new List<SqlParameter>
        {
            Tsql.Strings("@users", users.Values.SelectMany(u => u).Distinct(StringComparer.Ordinal)),
            Tsql.Ticks("@lookahead", CalendarEvaluator.Lookahead),
        };
        for (var i = 0; i < published.Length; i++)
        {
            var name = published[i];
            // The instant the set's coverage ends, by UTC days, brought forward by the margin.
            var threshold = new DateTimeOffset(snapshot.Dates[name].CoversThrough.AddDays(1).ToDateTime(TimeOnly.MinValue), TimeSpan.Zero) - margin;
            text.Append($"""
                SELECT {Tsql.Bit($"@t{i} <= SYSUTCDATETIME() "
                    + $"OR EXISTS (SELECT 1 FROM OPENJSON(@c{i}) AS c WHERE EXISTS (SELECT 1 FROM {jobs} AS j WHERE j.calendar = c.value AND j.calendar_at >= @t{i} AND j.state = 0)) "
                    + $"OR EXISTS (SELECT 1 FROM OPENJSON(@c{i}) AS c WHERE EXISTS (SELECT 1 FROM {recurrences} AS r WHERE r.calendar = c.value AND r.next_at >= @t{i} AND r.next_at <= {Tsql.AddTicks("SYSUTCDATETIME()", "@lookahead")}))")};

                """);
            parameters.Add(Tsql.Strings("@c" + i, users[name]));
            parameters.Add(Tsql.Time("@t" + i, threshold));
        }
        await using (var command = Tsql.Command(work, text.ToString(), [.. parameters]))
        {
            await using var reader = await command.ReaderAsync(cancellationToken);
            while (await reader.NextRowAsync(cancellationToken))
            {
                waiting.Add(reader.GetString(0));
            }
            foreach (var name in published)
            {
                await reader.NextSetAsync(cancellationToken);
                await reader.NextRowAsync(cancellationToken);
                if (reader.GetBoolean(0))
                {
                    needed.Add(name);
                }
            }
        }
        await transaction.CommitAsync(cancellationToken);
        // The sets a waiting calendar waits for: those it uses that are unpublished, otherwise those covering the fewest dates.
        var waitedFor = new HashSet<string>(StringComparer.Ordinal);
        foreach (var calendar in waiting)
        {
            var used = names.Where(n => users[n].Contains(calendar, StringComparer.Ordinal)).ToArray();
            var unpublished = used.Where(n => !snapshot.Dates.ContainsKey(n)).ToArray();
            if (unpublished.Length > 0)
            {
                waitedFor.UnionWith(unpublished);
            }
            else if (used.Length > 0)
            {
                var least = used.Min(n => snapshot.Dates[n].CoversThrough);
                waitedFor.UnionWith(used.Where(n => snapshot.Dates[n].CoversThrough == least));
            }
        }
        return
        [
            .. names.Select(name =>
            {
                var set = snapshot.Dates.GetValueOrDefault(name);
                var state = waitedFor.Contains(name) ? DateSetCoverageState.WorkWaiting
                    : set is null || needed.Contains(name) ? DateSetCoverageState.RunningOut
                    : DateSetCoverageState.Covered;
                return new DateSetCoverage(name, set?.Revision, set?.CoversThrough, users[name], state);
            }),
        ];
    }

    /// <summary>
    /// Takes the date set's applock and replaces its row only with a newer revision. Then, with the stored calendars read
    /// in the same transaction, the jobs of every calendar that uses the set, directly or through composition, are
    /// planned again: outstanding ones that are not held, are not to be skipped and have a due time their calendar
    /// planned, not one an operator made due ignoring it, get it back as their calendar instant, and ones waiting for
    /// uncovered dates become plannable. The jobs are found without locks, locked in primary-key order and evaluated
    /// again in the statement after the lock. Each definition with such jobs is signalled to plan on commit.
    /// </summary>
    public async ValueTask<bool> PublishDatesAsync(WorkTransaction transaction, CalendarDates set, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(set);
        ArgumentException.ThrowIfNullOrWhiteSpace(set.Name, nameof(set));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(set.Revision);
        if (set.Dates.Any(d => d > set.CoversThrough))
        {
            throw new ArgumentException("A date set holds only dates it covers.", nameof(set));
        }
        var work = SqlServerTransaction.From(transaction);
        CalendarDates? stored = null;
        await using (var select = Tsql.Command(work,
            LeaseSql.AppLock("@lock", "Exclusive") + $"SELECT name, revision, covers_through, dates FROM {dates} WHERE name = @name;",
            Tsql.Text("@lock", Tsql.LockName("dates", schema, set.Name), 255),
            Tsql.Text("@name", set.Name, 128)))
        {
            await using var reader = await select.ReaderAsync(cancellationToken);
            if (await reader.NextRowAsync(cancellationToken))
            {
                stored = ReadDates(reader);
            }
        }
        if (stored is not null)
        {
            if (stored.Revision > set.Revision)
            {
                return false;
            }
            if (stored.Revision == set.Revision)
            {
                if (stored.CoversThrough != set.CoversThrough || !stored.Dates.SetEquals(set.Dates))
                {
                    throw new CalendarConflictException(
                        $"Date set {set.Name} revision {set.Revision} is stored with other dates; publish changes with a newer revision.");
                }
                return false;
            }
        }
        await using (var write = Tsql.Command(work, stored is null
                ? $"INSERT INTO {dates} (name, revision, covers_through, dates) VALUES (@name, @revision, @covers, @dates);"
                : $"UPDATE {dates} SET revision = @revision, covers_through = @covers, dates = @dates, published_at = SYSUTCDATETIME() WHERE name = @name;",
            Tsql.Text("@name", set.Name, 128),
            Tsql.Int64("@revision", set.Revision),
            new SqlParameter("@covers", System.Data.SqlDbType.Date) { Value = set.CoversThrough.ToDateTime(TimeOnly.MinValue) },
            Tsql.Text("@dates", string.Join(',', set.Dates.Order().Select(d => d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture))), -1)))
        {
            await write.NonQueryAsync(cancellationToken);
        }
        var depending = (await LoadAsync(work, cancellationToken)).DependingOn(set.Name);
        if (depending.Count == 0)
        {
            return true;
        }
        var replannable = $"r.state = 0 AND r.lease_expires_at IS NULL AND r.skip_pending = 0 "
            + $"AND (r.calendar_unavailable = 1 OR (r.due_at IS NOT NULL AND r.due_at < {Tsql.NeverLiteral} AND (r.window_end IS NULL OR r.window_end <> {Tsql.NeverLiteral})))";
        var definitions = new List<string>();
        await using (var replan = Tsql.Command(work, $"""
            DECLARE @ids TABLE (id uniqueidentifier NOT NULL PRIMARY KEY);
            INSERT INTO @ids (id) SELECT r.id FROM {jobs} AS r WHERE r.calendar IN (SELECT value FROM OPENJSON(@calendars)) AND {replannable};
            DECLARE @locked int;
            SELECT @locked = COUNT(*) FROM @ids AS i INNER LOOP JOIN {jobs} AS r WITH (UPDLOCK, ROWLOCK, FORCESEEK) ON r.id = i.id;
            DECLARE @planned TABLE (definition nvarchar(200) COLLATE {Tsql.Binary} NOT NULL);
            UPDATE r SET calendar_at = CASE WHEN r.calendar_unavailable = 1 THEN r.calendar_at ELSE r.due_at END, due_at = NULL, window_end = NULL, calendar_unavailable = 0
            OUTPUT inserted.definition INTO @planned
            FROM @ids AS i INNER LOOP JOIN {jobs} AS r WITH (FORCESEEK) ON r.id = i.id WHERE {replannable};
            SELECT DISTINCT definition FROM @planned;
            """,
            Tsql.Strings("@calendars", depending)))
        {
            await using var reader = await replan.ReaderAsync(cancellationToken);
            while (await reader.NextRowAsync(cancellationToken))
            {
                definitions.Add(reader.GetString(0));
            }
        }
        foreach (var definition in definitions)
        {
            await leases.NotifyAsync(work.Connection, work.Transaction, WorkTopics.Plan, JobDefinition.SourceNameOf(definition), cancellationToken);
        }
        return true;
    }

    /// <summary>
    /// An evaluator of the calendars and date sets stored as the transaction sees them: their revisions are read in one
    /// statement, and only those the cache does not hold at that revision are read in full.
    /// </summary>
    internal async Task<CalendarEvaluator> LoadAsync(SqlServerTransaction work, CancellationToken cancellationToken) =>
        (await LoadSnapshotAsync(work, cancellationToken)).Evaluator;

    /// <summary>The stored calendars and date sets with their evaluator, as <see cref="LoadAsync"/> reads them.</summary>
    private async Task<Snapshot> LoadSnapshotAsync(SqlServerTransaction work, CancellationToken cancellationToken)
    {
        var snapshot = cached;
        var calendarRevisions = new Dictionary<string, long>(StringComparer.Ordinal);
        var dateRevisions = new Dictionary<string, long>(StringComparer.Ordinal);
        await using (var command = Tsql.Command(work, $"SELECT name, revision FROM {calendars}; SELECT name, revision FROM {dates};"))
        {
            await using var reader = await command.ReaderAsync(cancellationToken);
            while (await reader.NextRowAsync(cancellationToken))
            {
                calendarRevisions[reader.GetString(0)] = reader.GetInt64(1);
            }
            await reader.NextSetAsync(cancellationToken);
            while (await reader.NextRowAsync(cancellationToken))
            {
                dateRevisions[reader.GetString(0)] = reader.GetInt64(1);
            }
        }
        var staleCalendars = calendarRevisions.Where(c => !snapshot.Calendars.TryGetValue(c.Key, out var known) || known.Revision != c.Value).Select(c => c.Key).ToArray();
        var staleDates = dateRevisions.Where(d => !snapshot.Dates.TryGetValue(d.Key, out var known) || known.Revision != d.Value).Select(d => d.Key).ToArray();
        if (staleCalendars.Length == 0 && staleDates.Length == 0
            && snapshot.Calendars.Count == calendarRevisions.Count && snapshot.Dates.Count == dateRevisions.Count)
        {
            return snapshot;
        }
        var loadedCalendars = snapshot.Calendars.Where(c => calendarRevisions.ContainsKey(c.Key)).ToDictionary(StringComparer.Ordinal);
        var loadedDates = snapshot.Dates.Where(d => dateRevisions.ContainsKey(d.Key)).ToDictionary(StringComparer.Ordinal);
        await using (var command = Tsql.Command(work, $"""
            SELECT name, revision, [rule] FROM {calendars} WHERE name IN (SELECT value FROM OPENJSON(@calendars));
            SELECT name, revision, covers_through, dates FROM {dates} WHERE name IN (SELECT value FROM OPENJSON(@dates));
            """,
            Tsql.Strings("@calendars", staleCalendars),
            Tsql.Strings("@dates", staleDates)))
        {
            await using var reader = await command.ReaderAsync(cancellationToken);
            while (await reader.NextRowAsync(cancellationToken))
            {
                var calendar = WorkCalendar.Parse(reader.GetString(0), reader.GetInt64(1), reader.GetString(2));
                loadedCalendars[calendar.Name] = calendar;
            }
            await reader.NextSetAsync(cancellationToken);
            while (await reader.NextRowAsync(cancellationToken))
            {
                var set = ReadDates(reader);
                loadedDates[set.Name] = set;
            }
        }
        var fresh = new Snapshot(loadedCalendars, loadedDates, new CalendarEvaluator(loadedCalendars.Values, loadedDates.Values));
        cached = fresh;
        return fresh;
    }

    /// <summary>A date set from the columns name, revision, covers_through and dates, the dates as yyyy-MM-dd separated by commas.</summary>
    private static CalendarDates ReadDates(SqlDataReader reader)
    {
        var text = reader.GetString(3);
        var days = text.Length == 0
            ? new HashSet<DateOnly>()
            : text.Split(',').Select(d => DateOnly.ParseExact(d, "yyyy-MM-dd", CultureInfo.InvariantCulture)).ToHashSet();
        return new(reader.GetString(0), reader.GetInt64(1), days, DateOnly.FromDateTime(reader.GetDateTime(2)));
    }

    private sealed record Snapshot(Dictionary<string, WorkCalendar> Calendars, Dictionary<string, CalendarDates> Dates, CalendarEvaluator Evaluator);
}
