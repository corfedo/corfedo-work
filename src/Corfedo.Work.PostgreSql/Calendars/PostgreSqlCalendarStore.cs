using System.Globalization;
using Corfedo.Work.PostgreSql.Leases;
using Npgsql;
using NpgsqlTypes;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Calendars;

/// <summary>
/// Calendars by revision and published date sets in the framework's tables. Evaluators are built from the stored rows
/// read in the caller's transaction; a process-local cache keeps the parsed calendars and date sets by revision, so a
/// read costs one statement over the revisions while nothing changed.
/// </summary>
internal sealed class PostgreSqlCalendarStore(NpgsqlDataSource dataSource, PostgreSqlLeaseStore leases, string schema) : ICalendarStore
{
    private readonly string calendars = LeaseSql.Quote(schema) + ".calendars";
    private readonly string dates = LeaseSql.Quote(schema) + ".calendar_dates";
    private readonly string jobs = LeaseSql.Quote(schema) + ".jobs";
    private readonly string recurrences = LeaseSql.Quote(schema) + ".job_recurrences";
    private readonly string processes = LeaseSql.Quote(schema) + ".processes";
    private readonly string processCalendars = LeaseSql.Quote(schema) + ".process_calendars";
    private Snapshot cached = new([], [], new CalendarEvaluator([], []));

    public async ValueTask<WorkCalendar> StoreCalendarAsync(WorkCalendar calendar, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(calendar);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        WorkCalendar? stored = null;
        byte[]? digest = null;
        await using (var select = new NpgsqlCommand($"SELECT revision, digest, rule FROM {calendars} WHERE name = $1 FOR UPDATE", connection, transaction))
        {
            select.Parameters.Add(Text(calendar.Name));
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
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
        await using (var write = new NpgsqlCommand(
            $"INSERT INTO {calendars} (name, revision, digest, rule) VALUES ($1, $2, $3, $4) "
            + "ON CONFLICT (name) DO UPDATE SET revision = EXCLUDED.revision, digest = EXCLUDED.digest, rule = EXCLUDED.rule, stored_at = clock_timestamp()",
            connection,
            transaction))
        {
            write.Parameters.Add(Text(calendar.Name));
            write.Parameters.Add(Int64(calendar.Revision));
            write.Parameters.Add(new NpgsqlParameter<byte[]> { TypedValue = calendar.Digest, NpgsqlDbType = NpgsqlDbType.Bytea });
            write.Parameters.Add(Text(calendar.Rule));
            await write.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        return calendar;
    }

    public async ValueTask<IReadOnlyList<WorkCalendar>> GetCalendarsAsync(CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand($"SELECT name, revision, rule FROM {calendars} ORDER BY name");
        var result = new List<WorkCalendar>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(WorkCalendar.Parse(reader.GetString(0), reader.GetInt64(1), reader.GetString(2)));
        }
        return result;
    }

    public async ValueTask<IReadOnlyList<CalendarDates>> GetDateSetsAsync(CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand($"SELECT name, revision, covers_through, dates FROM {dates} ORDER BY name");
        var result = new List<CalendarDates>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
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
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        var composing = new List<string>();
        var jobCounts = new List<string>();
        long recurrenceCount;
        await using (var batch = new NpgsqlBatch(connection, transaction))
        {
            batch.BatchCommands.Add(Command($"SELECT 1 FROM {calendars} WHERE name = $1 FOR UPDATE", Text(name)));
            batch.BatchCommands.Add(Command($"SELECT name, revision, rule FROM {calendars} WHERE name <> $1 ORDER BY name", Text(name)));
            batch.BatchCommands.Add(Command($"SELECT definition, count(*) FROM {jobs} WHERE calendar = $1 GROUP BY definition ORDER BY definition", Text(name)));
            batch.BatchCommands.Add(Command($"SELECT count(*) FROM {recurrences} WHERE calendar = $1", Text(name)));
            await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return false;
            }
            await reader.NextResultAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (WorkCalendar.Parse(reader.GetString(0), reader.GetInt64(1), reader.GetString(2)).References.Contains(name, StringComparer.Ordinal))
                {
                    composing.Add(reader.GetString(0));
                }
            }
            await reader.NextResultAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                jobCounts.Add(string.Create(CultureInfo.InvariantCulture, $"{reader.GetString(0)} ({reader.GetInt64(1)})"));
            }
            await reader.NextResultAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            recurrenceCount = reader.GetInt64(0);
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
        await using (var delete = new NpgsqlCommand($"DELETE FROM {calendars} WHERE name = $1", connection, transaction))
        {
            delete.Parameters.Add(Text(name));
            await delete.ExecuteNonQueryAsync(cancellationToken);
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
        await using var command = dataSource.CreateCommand(
            $"INSERT INTO {processCalendars} (owner, calendar, revision) SELECT $1, c.calendar, c.revision FROM unnest($2::text[], $3::bigint[]) AS c(calendar, revision) "
            + "ON CONFLICT (owner, calendar) DO NOTHING");
        command.Parameters.Add(Uuid(owner.Value));
        command.Parameters.Add(Names([.. registered.Select(c => c.Name)]));
        command.Parameters.Add(new NpgsqlParameter<long[]> { TypedValue = [.. registered.Select(c => c.Revision)], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Stored calendars that no live process registers, in one statement: a process is live while it has not stopped
    /// and its heartbeat plus its own stale window is after the statement's <c>now()</c>.
    /// </summary>
    public async ValueTask<IReadOnlyList<UnregisteredCalendar>> GetUnregisteredCalendarsAsync(CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(
            $"SELECT c.name, c.revision, c.stored_at FROM {calendars} AS c WHERE NOT EXISTS (SELECT 1 FROM {processCalendars} AS pc "
            + $"JOIN {processes} AS p ON p.owner = pc.owner WHERE pc.calendar = c.name AND p.stopped_at IS NULL AND p.heartbeat_at + p.stale_after > now()) "
            + "ORDER BY c.name");
        var list = new List<UnregisteredCalendar>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            list.Add(new UnregisteredCalendar(reader.GetString(0), reader.GetInt64(1), reader.GetFieldValue<DateTimeOffset>(2)));
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
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        var work = new PostgreSqlTransaction(transaction);
        var snapshot = await LoadSnapshotAsync(work, cancellationToken);
        var names = snapshot.Dates.Keys.Concat(snapshot.Calendars.Values.SelectMany(c => c.DateSets)).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
        if (names.Length == 0)
        {
            return [];
        }
        var users = names.ToDictionary(n => n, n => snapshot.Evaluator.DependingOn(n).Order(StringComparer.Ordinal).ToArray(), StringComparer.Ordinal);
        var waiting = new HashSet<string>(StringComparer.Ordinal);
        var needed = new HashSet<string>(StringComparer.Ordinal);
        await using (var batch = new NpgsqlBatch(connection, transaction))
        {
            batch.BatchCommands.Add(Command(
                $"SELECT c FROM unnest($1::text[]) AS c WHERE EXISTS (SELECT 1 FROM {jobs} AS j WHERE j.calendar = c AND j.calendar_unavailable AND j.state = 0)",
                Names([.. users.Values.SelectMany(u => u).Distinct(StringComparer.Ordinal)])));
            var published = names.Where(snapshot.Dates.ContainsKey).ToArray();
            foreach (var name in published)
            {
                // The instant the set's coverage ends, by UTC days, brought forward by the margin.
                var threshold = new DateTimeOffset(snapshot.Dates[name].CoversThrough.AddDays(1).ToDateTime(TimeOnly.MinValue), TimeSpan.Zero) - margin;
                batch.BatchCommands.Add(Command(
                    "SELECT $2 <= clock_timestamp() "
                    + $"OR EXISTS (SELECT 1 FROM unnest($1::text[]) AS c WHERE EXISTS (SELECT 1 FROM {jobs} AS j WHERE j.calendar = c AND j.calendar_at >= $2 AND j.state = 0)) "
                    + $"OR EXISTS (SELECT 1 FROM unnest($1::text[]) AS c WHERE EXISTS (SELECT 1 FROM {recurrences} AS r WHERE r.calendar = c AND r.next_at >= $2 AND r.next_at <= clock_timestamp() + $3))",
                    Names(users[name]),
                    new NpgsqlParameter<DateTimeOffset> { TypedValue = threshold, NpgsqlDbType = NpgsqlDbType.TimestampTz },
                    new NpgsqlParameter<TimeSpan> { TypedValue = CalendarEvaluator.Lookahead, NpgsqlDbType = NpgsqlDbType.Interval }));
            }
            await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                waiting.Add(reader.GetString(0));
            }
            foreach (var name in published)
            {
                await reader.NextResultAsync(cancellationToken);
                await reader.ReadAsync(cancellationToken);
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
    /// Locks the date set's row, or its key for a new set, and replaces it only with a newer revision. Then, with the
    /// stored calendars read in the same transaction, the jobs of every calendar that uses the set, directly or through
    /// composition, are planned again: outstanding ones that are not held, are not to be skipped and have a due time
    /// their calendar planned, not one an operator made due ignoring it, get it back as their calendar instant, and ones waiting for uncovered dates become plannable. Each definition
    /// with such jobs is signalled to plan on commit.
    /// </summary>
    public async ValueTask<bool> PublishDatesAsync(IWorkTransaction transaction, CalendarDates set, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(set);
        ArgumentException.ThrowIfNullOrWhiteSpace(set.Name, nameof(set));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(set.Revision);
        if (set.Dates.Any(d => d > set.CoversThrough))
        {
            throw new ArgumentException("A date set holds only dates it covers.", nameof(set));
        }
        var work = PostgreSqlTransaction.From(transaction);
        var ordered = set.Dates.Order().ToArray();
        await using (var select = new NpgsqlCommand($"SELECT name, revision, covers_through, dates FROM {dates} WHERE name = $1 FOR UPDATE", work.Connection, work.Transaction))
        {
            select.Parameters.Add(Text(set.Name));
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                var stored = ReadDates(reader);
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
        }
        await using (var write = new NpgsqlCommand(
            $"INSERT INTO {dates} (name, revision, covers_through, dates) VALUES ($1, $2, $3, $4) "
            + "ON CONFLICT (name) DO UPDATE SET revision = EXCLUDED.revision, covers_through = EXCLUDED.covers_through, dates = EXCLUDED.dates, published_at = clock_timestamp() "
            + $"WHERE {dates}.revision < EXCLUDED.revision",
            work.Connection,
            work.Transaction))
        {
            write.Parameters.Add(Text(set.Name));
            write.Parameters.Add(Int64(set.Revision));
            write.Parameters.Add(new NpgsqlParameter<DateOnly> { TypedValue = set.CoversThrough, NpgsqlDbType = NpgsqlDbType.Date });
            write.Parameters.Add(new NpgsqlParameter<DateOnly[]> { TypedValue = ordered, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date });
            if (await write.ExecuteNonQueryAsync(cancellationToken) == 0)
            {
                // A concurrent publisher stored the set first and committed; its revision is the same or newer.
                return false;
            }
        }
        var depending = (await LoadAsync(work, cancellationToken)).DependingOn(set.Name);
        if (depending.Count == 0)
        {
            return true;
        }
        var definitions = new HashSet<string>(StringComparer.Ordinal);
        await using (var replan = new NpgsqlCommand(
            $"UPDATE {jobs} AS r SET calendar_at = CASE WHEN r.calendar_unavailable THEN r.calendar_at ELSE r.due_at END, due_at = NULL, window_end = NULL, "
            + "calendar_unavailable = false "
            + "WHERE r.calendar = ANY($1) AND r.state = 0 AND r.lease_expires_at IS NULL AND NOT r.skip_pending "
            + "AND (r.calendar_unavailable OR (r.due_at IS NOT NULL AND r.due_at < 'infinity' AND r.window_end IS DISTINCT FROM 'infinity')) RETURNING r.definition",
            work.Connection,
            work.Transaction))
        {
            replan.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = [.. depending], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text });
            await using var reader = await replan.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
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
    internal async Task<CalendarEvaluator> LoadAsync(PostgreSqlTransaction work, CancellationToken cancellationToken) =>
        (await LoadSnapshotAsync(work, cancellationToken)).Evaluator;

    /// <summary>The stored calendars and date sets with their evaluator, as <see cref="LoadAsync"/> reads them.</summary>
    private async Task<Snapshot> LoadSnapshotAsync(PostgreSqlTransaction work, CancellationToken cancellationToken)
    {
        var snapshot = cached;
        var calendarRevisions = new Dictionary<string, long>(StringComparer.Ordinal);
        var dateRevisions = new Dictionary<string, long>(StringComparer.Ordinal);
        await using (var batch = new NpgsqlBatch(work.Connection, work.Transaction))
        {
            batch.BatchCommands.Add(Command($"SELECT name, revision FROM {calendars}"));
            batch.BatchCommands.Add(Command($"SELECT name, revision FROM {dates}"));
            await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                calendarRevisions[reader.GetString(0)] = reader.GetInt64(1);
            }
            await reader.NextResultAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
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
        await using (var batch = new NpgsqlBatch(work.Connection, work.Transaction))
        {
            batch.BatchCommands.Add(Command($"SELECT name, revision, rule FROM {calendars} WHERE name = ANY($1)", Names(staleCalendars)));
            batch.BatchCommands.Add(Command($"SELECT name, revision, covers_through, dates FROM {dates} WHERE name = ANY($1)", Names(staleDates)));
            await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var calendar = WorkCalendar.Parse(reader.GetString(0), reader.GetInt64(1), reader.GetString(2));
                loadedCalendars[calendar.Name] = calendar;
            }
            await reader.NextResultAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var set = ReadDates(reader);
                loadedDates[set.Name] = set;
            }
        }
        var fresh = new Snapshot(loadedCalendars, loadedDates, new CalendarEvaluator(loadedCalendars.Values, loadedDates.Values));
        cached = fresh;
        return fresh;
    }

    private static CalendarDates ReadDates(NpgsqlDataReader reader) =>
        new(reader.GetString(0), reader.GetInt64(1), reader.GetFieldValue<DateOnly[]>(3).ToHashSet(), reader.GetFieldValue<DateOnly>(2));

    private static NpgsqlParameter<string[]> Names(string[] names) => new() { TypedValue = names, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text };

    private sealed record Snapshot(Dictionary<string, WorkCalendar> Calendars, Dictionary<string, CalendarDates> Dates, CalendarEvaluator Evaluator);
}
