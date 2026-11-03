using System.Globalization;
using System.Text;
using Corfedo.Work.SqlServer.Leases;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Jobs;

internal sealed partial class SqlServerJobStore
{
    /// <summary>The error number of a lock not granted within <c>LOCK_TIMEOUT</c>.</summary>
    private const int LockNotAvailable = 1222;

    /// <summary>
    /// How long, in milliseconds, the planner's writes may wait for a row or index entry another transaction holds. Past
    /// it the recurrence is left for the next pass.
    /// </summary>
    private const int PlannerLockTimeout = 50;

    /// <summary>Whether no stored pause holds the subject of recurrence <paramref name="alias"/>.</summary>
    private string SubjectUnpaused(string alias) => $"{PausedCopy($"{alias}.[definition]", $"{alias}.subject_kind", $"{alias}.subject_id")} = 0";

    /// <summary>Whether recurrence <paramref name="alias"/> has an outstanding occurrence, not one to be skipped.</summary>
    private string OccurrenceOutstanding(string alias) =>
        $"EXISTS (SELECT 1 FROM {Jobs} AS o WHERE o.recurrence_id IS NOT NULL AND o.recurrence_id = {alias}.id AND o.state = 0 AND o.skip_pending = 0)";

    /// <summary>
    /// Whether recurrence <paramref name="alias"/> can plan its due instant: an after-completion instant waits while one
    /// of its occurrences is outstanding, whose end signals planning.
    /// </summary>
    private string DueCanPlan(string alias) =>
        $"({alias}.interval_kind IS NULL OR {alias}.interval_kind <> {(short)Interval.AfterCompletion} OR NOT {OccurrenceOutstanding(alias)})";

    private sealed record PlannedRecurrence(
        Guid Id,
        Schedule Schedule,
        Missed Missed,
        Overlap Overlap,
        Guid? Awaited,
        DateTimeOffset? NextAt,
        DateTimeOffset? WaitingAt,
        bool SubjectPaused);

    private sealed record EndedOccurrence(Guid Id, Guid Recurrence, DateTimeOffset EndedAt);

    /// <summary>
    /// One planning pass in its own transaction. It tries the definition fence in shared mode and does nothing while a
    /// definition is being stored or paused. It locks up to <paramref name="limit"/> recurrences of each kind of work
    /// through their key, skipping locked ones: due by their next instant, with a waiting occurrence and no outstanding
    /// occurrence, or with unconsumed ended occurrences. Due and waiting ones count only while neither the definition
    /// nor their subject is paused, a due after-completion one only while none of its occurrences is outstanding, and so
    /// does the next instant it returns. Then, with the store's clock read after the locks, each recurrence in a
    /// savepoint of its own: ended occurrences are consumed (the one an after-completion schedule awaits sets its next
    /// instant); unless the definition or the subject is paused, a waiting occurrence is created once none is
    /// outstanding, and due instants pass the missed rule and then the overlap policy. Only the recurrence's own
    /// occurrences count. Writes wait for other transactions at most <see cref="PlannerLockTimeout"/> milliseconds; a
    /// recurrence that would wait longer is rolled back to its savepoint and left for the next pass. Ended occurrences of
    /// removed recurrences are cleared. No outcome hook runs here.
    /// </summary>
    public async ValueTask<PlanningPass> PlanAsync(LeaseSource<JobId> source, IRecurrenceCalculator calculator, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(calculator);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var sql = await SqlAsync(source, cancellationToken);
        return await SqlServerLeaseStore.RetryDeadlocksAsync(async () =>
        {
            await using var connection = await leases.OpenAsync(cancellationToken);
            await using var transaction = await SqlServerLeaseStore.BeginAsync(connection, cancellationToken);
            var work = new SqlServerTransaction(transaction);
            var pass = await PlanAsync(work, source, sql, calculator, limit, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return pass;
        });
    }

    private async Task<PlanningPass> PlanAsync(SqlServerTransaction work, LeaseSource<JobId> source, LeaseSql sql, IRecurrenceCalculator calculator, int limit, CancellationToken cancellationToken)
    {
        var definition = sql.Job!.Definition;
        bool definitionPaused;
        long? revision;
        await using (var fence = Tsql.Command(work, $"""
            EXEC @lock_result = sp_getapplock @Resource = @fence, @LockMode = 'Shared', @LockOwner = 'Transaction', @LockTimeout = 0;
            IF @lock_result < 0
                SELECT CAST(0 AS bit), CAST(0 AS bit), CAST(NULL AS bigint);
            ELSE
                SELECT CAST(1 AS bit), COALESCE(d.paused, CAST(0 AS bit)), d.revision FROM (SELECT 1 AS one) AS o LEFT JOIN {definitions} AS d ON d.[key] = @definition;
            """,
            Tsql.Text("@fence", LeaseSql.DefinitionLockName(schema, definition), 255),
            Tsql.Text("@definition", definition, 200)))
        await using (var reader = await fence.ReaderAsync(cancellationToken))
        {
            await reader.NextRowAsync(cancellationToken);
            if (!reader.GetBoolean(0))
            {
                return new(0, 0, TimeSpan.FromMilliseconds(100));
            }
            definitionPaused = reader.GetBoolean(1);
            revision = reader.IsDBNull(2) ? null : reader.GetInt64(2);
        }

        var unconsumed = $"j.[definition] = @definition AND j.unconsumed = 1 AND NOT EXISTS (SELECT 1 FROM {recurrences} AS c WHERE c.id = j.recurrence_id)";
        var planned = new List<PlannedRecurrence>();
        var ended = new List<EndedOccurrence>();
        var outstanding = new HashSet<Guid>();
        DateTimeOffset now;
        // Due and waiting recurrences of a paused definition or subject, and after-completion ones with an outstanding
        // occurrence, cannot progress; left in, they would fill every pass ahead of recurrences that can. The copied
        // flag keeps paused ones out of the indexes; the stored pause also excludes those whose flag is stale.
        await using (var locks = Tsql.Command(work, $"""
            DECLARE @now datetime2(7) = SYSUTCDATETIME();
            DECLARE @cleared TABLE (id uniqueidentifier NOT NULL PRIMARY KEY);
            {Tsql.LockEach("cleared_scan", "@limit",
                $"SELECT j.id FROM {Jobs} AS j WITH (INDEX(jobs_unconsumed)) WHERE {unconsumed}",
                Jobs, Tsql.IdKey, unconsumed.Replace("j.", "r.", StringComparison.Ordinal), "@cleared")}
            UPDATE r SET unconsumed = 0 FROM @cleared AS u INNER LOOP JOIN {Jobs} AS r WITH (INDEX(jobs_key), FORCESEEK) ON r.id = u.id;
            DECLARE @ids TABLE (id uniqueidentifier NOT NULL PRIMARY KEY);
            IF @definition_paused = 0
            BEGIN
                {Tsql.LockEach("due_scan", "@limit",
                    $"SELECT c.id FROM {recurrences} AS c WITH (INDEX(job_recurrences_next)) WHERE c.[definition] = @definition AND c.next_at IS NOT NULL "
                    + $"AND c.next_at <= @now AND c.paused = 0 AND {SubjectUnpaused("c")} AND {DueCanPlan("c")} ORDER BY c.next_at",
                    recurrences, Tsql.IdKey,
                    $"r.next_at <= @now AND r.paused = 0 AND {SubjectUnpaused("r")} AND {DueCanPlan("r")}", "@ids")}
                {Tsql.LockEach("waiting_scan", "@limit",
                    $"SELECT c.id FROM {recurrences} AS c WITH (INDEX(job_recurrences_waiting)) WHERE c.[definition] = @definition AND c.waiting_at IS NOT NULL "
                    + $"AND c.paused = 0 AND {SubjectUnpaused("c")} AND NOT {OccurrenceOutstanding("c")}",
                    recurrences, Tsql.IdKey,
                    $"r.waiting_at IS NOT NULL AND r.paused = 0 AND {SubjectUnpaused("r")} AND NOT {OccurrenceOutstanding("r")}", "@ids")}
            END
            {Tsql.LockEach("unconsumed_scan", "@limit",
                $"SELECT DISTINCT j.recurrence_id FROM {Jobs} AS j WITH (INDEX(jobs_unconsumed)) WHERE j.[definition] = @definition AND j.unconsumed = 1 AND j.recurrence_id IS NOT NULL",
                recurrences, Tsql.IdKey, "1 = 1", "@ids")}
            SELECT c.id, c.{ScheduleColumns.Replace(", ", ", c.", StringComparison.Ordinal)},
                c.missed, c.overlap, c.awaited_job, c.next_at, c.waiting_at, {PausedCopy("c.[definition]", "c.subject_kind", "c.subject_id")}
            FROM @ids AS i INNER LOOP JOIN {recurrences} AS c WITH (FORCESEEK) ON c.id = i.id;
            DECLARE @candidates TABLE (id uniqueidentifier NOT NULL PRIMARY KEY);
            INSERT INTO @candidates (id) SELECT j.id FROM @ids AS i JOIN {Jobs} AS j ON j.recurrence_id = i.id WHERE j.[definition] = @definition AND j.unconsumed = 1;
            SELECT r.id, r.recurrence_id, r.ended_at FROM @candidates AS u INNER LOOP JOIN {Jobs} AS r WITH (UPDLOCK, ROWLOCK, READPAST, INDEX(jobs_key), FORCESEEK) ON r.id = u.id
            WHERE r.unconsumed = 1 AND r.[definition] = @definition;
            SELECT DISTINCT o.recurrence_id FROM @ids AS i JOIN {Jobs} AS o ON o.recurrence_id = i.id WHERE o.state = 0 AND o.skip_pending = 0;
            SELECT SYSUTCDATETIME();
            """,
            Tsql.Text("@definition", definition, 200),
            Tsql.Int32("@limit", limit),
            Tsql.Bool("@definition_paused", definitionPaused)))
        await using (var reader = await locks.ReaderAsync(cancellationToken))
        {
            while (await reader.NextRowAsync(cancellationToken))
            {
                planned.Add(new(
                    reader.GetGuid(0),
                    ReadSchedule(reader, 1),
                    (Missed)reader.GetInt16(7),
                    (Overlap)reader.GetInt16(8),
                    reader.IsDBNull(9) ? null : reader.GetGuid(9),
                    Tsql.NullableTime(reader, 10),
                    Tsql.NullableTime(reader, 11),
                    reader.GetBoolean(12)));
            }
            await reader.NextSetAsync(cancellationToken);
            while (await reader.NextRowAsync(cancellationToken))
            {
                ended.Add(new(reader.GetGuid(0), reader.GetGuid(1), Tsql.Time(reader, 2)));
            }
            await reader.NextSetAsync(cancellationToken);
            while (await reader.NextRowAsync(cancellationToken))
            {
                outstanding.Add(reader.GetGuid(0));
            }
            await reader.NextSetAsync(cancellationToken);
            await reader.NextRowAsync(cancellationToken);
            now = Tsql.Time(reader, 0);
        }

        var occurrences = 0;
        foreach (var recurrence in planned)
        {
            occurrences += await PlanRecurrenceAsync(
                work,
                sql,
                calculator,
                recurrence,
                [.. ended.Where(e => e.Recurrence == recurrence.Id)],
                outstanding.Contains(recurrence.Id),
                definitionPaused,
                now,
                cancellationToken);
        }
        // Resuming a definition or subject and ending an occurrence signal planning, so the next instant counts only
        // recurrences that can progress.
        TimeSpan? nextIn = null;
        if (!definitionPaused)
        {
            await using var next = Tsql.Command(work, $"""
                SELECT TOP (1) {Tsql.Micros("SYSUTCDATETIME()", "c.next_at")} FROM {recurrences} AS c WITH (INDEX(job_recurrences_next))
                WHERE c.[definition] = @definition AND c.next_at IS NOT NULL AND c.paused = 0 AND {SubjectUnpaused("c")} AND {DueCanPlan("c")}
                ORDER BY c.next_at;
                """,
                Tsql.Text("@definition", definition, 200));
            nextIn = Tsql.Wait(await next.ScalarAsync(cancellationToken));
        }
        if (occurrences > 0)
        {
            await NotifyDueAsync(work, source, cancellationToken);
        }
        return new(planned.Count, occurrences, nextIn, revision);
    }

    /// <summary>
    /// Plans one locked recurrence in one batch within a savepoint, with lock waits bounded by
    /// <see cref="PlannerLockTimeout"/>; returns how many occurrences it created, none when it rolled back to the
    /// savepoint after a lock timeout.
    /// </summary>
    private async Task<int> PlanRecurrenceAsync(
        SqlServerTransaction work,
        LeaseSql sql,
        IRecurrenceCalculator calculator,
        PlannedRecurrence recurrence,
        List<EndedOccurrence> ended,
        bool held,
        bool definitionPaused,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        var schedule = recurrence.Schedule;
        var afterCompletion = schedule is { Kind: ScheduleKind.Every, Interval: Interval.AfterCompletion };
        var next = recurrence.NextAt;
        var waiting = recurrence.WaitingAt;
        var awaited = recurrence.Awaited;
        // The occurrence an after-completion schedule awaits; it is named by its occurrence identity when planned in
        // this pass, so an insert that met an existing occurrence names that one.
        string? awaitedOccurrence = null;
        foreach (var occurrence in ended)
        {
            if (afterCompletion && next is null && occurrence.Id == awaited)
            {
                next = occurrence.EndedAt + schedule.Period;
                awaited = null;
            }
        }
        var created = 0;
        var text = new StringBuilder();
        var parameters = new List<SqlParameter> { Tsql.Guid("@rid", recurrence.Id) };
        if (ended.Count > 0)
        {
            text.Append($"""
                UPDATE r SET unconsumed = 0 FROM OPENJSON(@ended) WITH (id uniqueidentifier '$') AS e
                INNER LOOP JOIN {sql.Table} AS r WITH (INDEX(jobs_key), FORCESEEK) ON r.id = e.id;

                """);
            parameters.Add(Tsql.Guids("@ended", ended.Select(e => e.Id).Order()));
        }
        // An after-completion schedule awaits only an instant it planned; its waiting occurrence comes from a trigger.
        // A released waiting occurrence is planned at the next allowed instant of its calendar, a scheduled instant only
        // when the calendar allows it. Each insert sets the variables InsertOccurrence reads.
        void Insert(DateTimeOffset instant, bool skip, bool awaits, bool released = false)
        {
            var n = parameters.Count;
            var occurrence = OccurrenceOf(instant);
            text.Append($"SELECT @job = @job_{n}, @occurrence = @occurrence_{n}, @due = @due_{n};\n")
                .Append(InsertOccurrence(sql, "@due", skip, released ? CalendarStart.MoveForward : CalendarStart.Instant));
            parameters.Add(Tsql.Guid($"@job_{n}", JobId.New().Value));
            parameters.Add(Tsql.Text($"@occurrence_{n}", occurrence, 200));
            parameters.Add(Tsql.Time($"@due_{n}", skip ? (DateTimeOffset?)null : instant));
            if (!skip)
            {
                created++;
                if (afterCompletion && awaits)
                {
                    awaitedOccurrence = occurrence;
                }
            }
        }
        if (!definitionPaused && !recurrence.SubjectPaused)
        {
            if (waiting is { } released && !held)
            {
                Insert(released, skip: false, awaits: false, released: true);
                waiting = null;
                held = true;
            }
            if (next is { } due && due <= now && !(afterCompletion && held))
            {
                // An after-completion instant due while another occurrence is outstanding stays due until it ends.
                var instants = calculator.Plan(schedule, recurrence.Missed, due, now);
                next = instants.Next;
                if (instants.Planned is { } instant)
                {
                    if (!held)
                    {
                        Insert(instant, skip: false, awaits: true);
                    }
                    else
                    {
                        switch (recurrence.Overlap)
                        {
                            case Overlap.Wait:
                                waiting = instant;
                                break;
                            case Overlap.Skip:
                                Insert(instant, skip: true, awaits: false);
                                break;
                            default:
                                Insert(instant, skip: false, awaits: true);
                                break;
                        }
                    }
                }
            }
        }
        text.Append($"""
            UPDATE {recurrences} SET next_at = @next, waiting_at = @waiting, updated_at = SYSUTCDATETIME(),
                awaited_job = CASE WHEN @awaited_occurrence IS NULL THEN @awaited
                    ELSE (SELECT j.id FROM {sql.Table} AS j WHERE j.recurrence_id = @rid AND j.occurrence = @awaited_occurrence) END
            WHERE id = @rid;

            """);
        parameters.Add(Tsql.Time("@next", next));
        parameters.Add(Tsql.Time("@waiting", waiting));
        parameters.Add(Tsql.Guid("@awaited", awaited));
        parameters.Add(Tsql.Text("@awaited_occurrence", awaitedOccurrence, 200));
        // XACT_ABORT is off within the savepoint, so a lock timeout ends only its statement and the recurrence rolls back
        // to the savepoint; any other error is raised again and fails the pass.
        await using var command = Tsql.Command(work, $"""
            DECLARE @job uniqueidentifier, @occurrence nvarchar(200), @due datetime2(7), @planned bit = 1;
            SET XACT_ABORT OFF;
            SET LOCK_TIMEOUT {PlannerLockTimeout.ToString(CultureInfo.InvariantCulture)};
            SAVE TRANSACTION cw_plan;
            BEGIN TRY
                {text.ToString().Replace("\n", "\n    ", StringComparison.Ordinal).TrimEnd()}
            END TRY
            BEGIN CATCH
                IF ERROR_NUMBER() <> {LockNotAvailable} OR XACT_STATE() <> 1 THROW;
                ROLLBACK TRANSACTION cw_plan;
                SET @planned = 0;
            END CATCH
            SELECT @planned;
            """, [.. parameters]);
        return Convert.ToBoolean(await command.ScalarAsync(cancellationToken), CultureInfo.InvariantCulture) ? created : 0;
    }

    /// <summary>
    /// Ends jobs recorded to be skipped, one per transaction: each is found through the index of jobs to be skipped, or
    /// of occurrences whose chain settled, without a lock, locked through its key without waiting and ended skipped
    /// through the terminal path, which runs the outcome hook, while no recurrence is locked, with the reason its
    /// calendar or chain recorded or <c>overlap</c>; an occurrence whose chain settled ends with the chain's outcome.
    /// They need no claim, so pause and removal of their recurrence do not hold them back. One whose transition fails,
    /// for example in its outcome hook, stays recorded and is skipped for the rest of the call.
    /// </summary>
    public async ValueTask<int> FinalizeSkippedAsync(LeaseSource<JobId> source, JobEndedCallback? ended, int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var sql = await SqlAsync(source, cancellationToken);
        const string skipping = "r.state = 0 AND (r.skip_pending = 1 OR r.chain_outcome IS NOT NULL)";
        const string notFailed = "x.id NOT IN (SELECT value FROM OPENJSON(@failed) WITH (value uniqueidentifier '$'))";
        var locked = $"{sql.Scope} AND {skipping}";
        var failed = new List<Guid>();
        var count = 0;
        while (count < limit)
        {
            var outcome = await SqlServerLeaseStore.RetryDeadlocksAsync(async () =>
            {
                await using var connection = await leases.OpenAsync(cancellationToken);
                await using var transaction = await SqlServerLeaseStore.BeginAsync(connection, cancellationToken);
                var work = new SqlServerTransaction(transaction);
                Guid job;
                string? reason;
                var ending = JobOutcome.Skipped;
                await using (var select = Tsql.Command(work, $"""
                    {sql.ScanVariables}DECLARE @picked TABLE (k0 uniqueidentifier NOT NULL PRIMARY KEY);
                    {sql.LockScan("skip_scan", "1", "jobs_skip", $"x.[definition] = @definition AND x.skip_pending = 1 AND x.state = 0 AND {notFailed}", locked, "x.id", "@picked", "k0", "r.id")}
                    IF NOT EXISTS (SELECT 1 FROM @picked)
                    BEGIN
                        {sql.LockScan("owner_scan", "1", "jobs_chain_owner", $"x.[definition] = @definition AND x.chain_outcome IS NOT NULL AND x.state = 0 AND {notFailed}", locked, "x.id", "@picked", "k0", "r.id")}
                    END
                    SELECT r.id, COALESCE(r.skip_reason, @overlap), r.skip_pending, r.chain_outcome FROM @picked AS p JOIN {sql.Table} AS r ON r.id = p.k0;
                    """,
                    Tsql.Text("@definition", sql.Job!.Definition, 200),
                    Tsql.Guids("@failed", failed),
                    Tsql.Text("@overlap", JobReasons.Overlap)))
                await using (var reader = await select.ReaderAsync(cancellationToken))
                {
                    if (!await reader.NextRowAsync(cancellationToken))
                    {
                        return (Done: true, Ended: false);
                    }
                    job = reader.GetGuid(0);
                    reason = reader.GetString(1);
                    if (!reader.GetBoolean(2))
                    {
                        // An occurrence whose chain settled ends with the chain's outcome.
                        ending = (JobOutcome)reader.GetInt16(3);
                        reason = OwnerReason(ending);
                    }
                }
                try
                {
                    // A standalone job skipped by its calendar starts a pending follow-up; occurrences have none.
                    if (await EndJobAsync(work, source, new JobId(job), ending, reason, discardPending: false, skipping, [], ended, cancellationToken))
                    {
                        await transaction.CommitAsync(cancellationToken);
                        return (Done: false, Ended: true);
                    }
                    return (Done: false, Ended: false);
                }
                catch (Exception exception) when (!cancellationToken.IsCancellationRequested && !(exception is SqlException e && Tsql.IsDeadlock(e)))
                {
                    failed.Add(job);
                    LogFinalizeFailed(logger, telemetry.Logged(exception), sql.Job!.Definition, job, telemetry.Describe(exception));
                    return (Done: false, Ended: false);
                }
            });
            if (outcome.Done)
            {
                return count;
            }
            if (outcome.Ended)
            {
                count++;
            }
        }
        return count;
    }
}
