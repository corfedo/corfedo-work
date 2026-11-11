using Corfedo.Work.PostgreSql.Leases;
using Npgsql;
using NpgsqlTypes;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Jobs;

internal sealed partial class PostgreSqlJobStore
{
    /// <summary>The SQLSTATE of a lock not granted within <c>lock_timeout</c>.</summary>
    private const string LockNotAvailable = "55P03";

    /// <summary>
    /// How long the planner's writes may wait for a row or index entry another transaction holds. Past it the
    /// recurrence is left for the next pass.
    /// </summary>
    private const string PlannerLockTimeout = "50ms";

    /// <summary>Whether a stored pause holds the subject of recurrence <c>c</c>.</summary>
    private string SubjectPaused => PausedCopy("c.definition", "c.subject_kind", "c.subject_id");

    /// <summary>Whether recurrence <c>c</c> has an outstanding occurrence, not one to be skipped.</summary>
    private static string OccurrenceOutstanding(LeaseSql sql) =>
        $"EXISTS (SELECT 1 FROM {sql.Table} AS r WHERE r.recurrence_id = c.id AND r.state = 0 AND NOT r.skip_pending)";

    /// <summary>
    /// Whether recurrence <c>c</c> can plan its due instant: an after-completion instant waits while one of its
    /// occurrences is outstanding, whose end signals planning.
    /// </summary>
    private static string DueCanPlan(LeaseSql sql) =>
        $"(c.interval_kind IS DISTINCT FROM {(short)Interval.AfterCompletion} OR NOT {OccurrenceOutstanding(sql)})";

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
    /// definition is being stored or paused. It locks up to <paramref name="limit"/> recurrences of each kind of work,
    /// skipping locked ones: due by their next instant, with a waiting occurrence and no outstanding occurrence, or with
    /// unconsumed ended occurrences. Due and waiting ones count only while neither the definition nor their subject is
    /// paused, a due after-completion one only while none of its occurrences is outstanding, and so does the next
    /// instant it returns. Then, with the store's clock read after the locks, each recurrence in a savepoint of its own:
    /// ended occurrences are consumed (the one an after-completion schedule awaits sets its next instant); unless the
    /// definition or the subject is paused, a waiting occurrence is created once none is outstanding, and due instants
    /// pass the missed rule and then the overlap policy. Only the recurrence's own occurrences count. Writes wait for
    /// other transactions at most <see cref="PlannerLockTimeout"/>; a recurrence that would wait longer is rolled back
    /// to its savepoint and left for the next pass. Ended occurrences of removed recurrences are cleared. No outcome
    /// hook runs here.
    /// </summary>
    public async ValueTask<PlanningPass> PlanAsync(LeaseSource<JobId> source, IRecurrenceCalculator calculator, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(calculator);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var sql = Sql(source);
        var definition = sql.Job!.Definition;
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        var work = new PostgreSqlTransaction(transaction);
        bool definitionPaused;
        long? revision = null;
        await using (var batch = new NpgsqlBatch(connection, transaction))
        {
            batch.BatchCommands.Add(Command("SELECT pg_try_advisory_xact_lock_shared(hashtextextended($1, 0))", Text(LeaseSql.DefinitionLockKey(schema, definition))));
            batch.BatchCommands.Add(Command($"SELECT paused, revision FROM {definitions} WHERE key = $1", Text(definition)));
            batch.BatchCommands.Add(Command($"SET LOCAL lock_timeout = '{PlannerLockTimeout}'"));
            await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            if (!reader.GetBoolean(0))
            {
                return new(0, 0, TimeSpan.FromMilliseconds(100));
            }
            await reader.NextResultAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                definitionPaused = reader.GetBoolean(0);
                revision = reader.GetInt64(1);
            }
            else
            {
                definitionPaused = false;
            }
        }

        var ids = new HashSet<Guid>();
        await using (var batch = new NpgsqlBatch(connection, transaction))
        {
            batch.BatchCommands.Add(Command(
                $"UPDATE {sql.Table} SET unconsumed = false WHERE id IN (SELECT j.id FROM {sql.Table} AS j WHERE j.definition = $1 AND j.unconsumed "
                + $"AND NOT EXISTS (SELECT 1 FROM {recurrences} AS c WHERE c.id = j.recurrence_id) LIMIT $2 FOR UPDATE OF j SKIP LOCKED)",
                Text(definition),
                Int32(limit)));
            // Due and waiting recurrences of a paused definition or subject, and after-completion ones with an
            // outstanding occurrence, cannot progress; left in, they would fill every pass ahead of recurrences that
            // can. The copied flag keeps paused ones out of the indexes; the stored pause also excludes those whose
            // flag is stale.
            if (!definitionPaused)
            {
                batch.BatchCommands.Add(Command(
                    $"SELECT c.id FROM {recurrences} AS c WHERE c.definition = $1 AND c.next_at <= now() AND NOT c.paused AND NOT {SubjectPaused} AND {DueCanPlan(sql)} "
                    + "ORDER BY c.next_at LIMIT $2 FOR UPDATE OF c SKIP LOCKED",
                    Text(definition),
                    Int32(limit)));
                batch.BatchCommands.Add(Command(
                    $"SELECT c.id FROM {recurrences} AS c WHERE c.definition = $1 AND c.waiting_at IS NOT NULL AND NOT c.paused AND NOT {SubjectPaused} "
                    + $"AND NOT {OccurrenceOutstanding(sql)} LIMIT $2 FOR UPDATE OF c SKIP LOCKED",
                    Text(definition),
                    Int32(limit)));
            }
            batch.BatchCommands.Add(Command(
                $"SELECT c.id FROM {recurrences} AS c WHERE c.id IN (SELECT j.recurrence_id FROM {sql.Table} AS j WHERE j.definition = $1 AND j.unconsumed) "
                + "LIMIT $2 FOR UPDATE OF c SKIP LOCKED",
                Text(definition),
                Int32(limit)));
            // The reader skips the UPDATE, which returns no rows: it starts at the first SELECT.
            await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
            do
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    ids.Add(reader.GetGuid(0));
                }
            }
            while (await reader.NextResultAsync(cancellationToken));
        }
        var locked = ids.ToArray();
        var planned = new List<PlannedRecurrence>(locked.Length);
        var ended = new List<EndedOccurrence>();
        var outstanding = new HashSet<Guid>();
        DateTimeOffset now;
        await using (var batch = new NpgsqlBatch(connection, transaction))
        {
            batch.BatchCommands.Add(Command(
                $"SELECT c.id, c.{ScheduleColumns.Replace(", ", ", c.", StringComparison.Ordinal)}, c.missed, c.overlap, c.awaited_job, c.next_at, c.waiting_at, "
                + $"{SubjectPaused} FROM {recurrences} AS c WHERE c.id = ANY($1)",
                Uuids(locked)));
            batch.BatchCommands.Add(Command(
                $"SELECT r.id, r.recurrence_id, r.ended_at FROM {sql.Table} AS r WHERE r.definition = $1 AND r.unconsumed AND r.recurrence_id = ANY($2) "
                + "FOR UPDATE OF r SKIP LOCKED",
                Text(definition),
                Uuids(locked)));
            batch.BatchCommands.Add(Command(
                $"SELECT DISTINCT r.recurrence_id FROM {sql.Table} AS r WHERE r.recurrence_id = ANY($1) AND r.state = 0 AND NOT r.skip_pending",
                Uuids(locked)));
            batch.BatchCommands.Add(Command("SELECT clock_timestamp()"));
            await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                planned.Add(new(
                    reader.GetGuid(0),
                    ReadSchedule(reader, 1),
                    (Missed)reader.GetInt16(7),
                    (Overlap)reader.GetInt16(8),
                    reader.IsDBNull(9) ? null : reader.GetGuid(9),
                    reader.IsDBNull(10) ? null : reader.GetFieldValue<DateTimeOffset>(10),
                    reader.IsDBNull(11) ? null : reader.GetFieldValue<DateTimeOffset>(11),
                    reader.GetBoolean(12)));
            }
            await reader.NextResultAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                ended.Add(new(reader.GetGuid(0), reader.GetGuid(1), reader.GetFieldValue<DateTimeOffset>(2)));
            }
            await reader.NextResultAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                outstanding.Add(reader.GetGuid(0));
            }
            await reader.NextResultAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            now = reader.GetFieldValue<DateTimeOffset>(0);
        }

        var occurrences = 0;
        foreach (var recurrence in planned)
        {
            var savepoint = work.NextSavepoint();
            await transaction.SaveAsync(savepoint, cancellationToken);
            try
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
                await transaction.ReleaseAsync(savepoint, cancellationToken);
            }
            catch (PostgresException exception) when (exception.SqlState == LockNotAvailable)
            {
                await transaction.RollbackAsync(savepoint, cancellationToken);
            }
        }
        // Resuming a definition or subject and ending an occurrence signal planning, so the next instant
        // counts only recurrences that can progress.
        TimeSpan? nextIn = null;
        if (!definitionPaused)
        {
            await using var next = new NpgsqlCommand(
                $"SELECT extract(epoch FROM c.next_at - clock_timestamp())::float8 FROM {recurrences} AS c WHERE c.definition = $1 AND c.next_at IS NOT NULL AND NOT c.paused AND NOT {SubjectPaused} "
                + $"AND {DueCanPlan(sql)} ORDER BY c.next_at LIMIT 1",
                connection,
                transaction);
            next.Parameters.Add(Text(definition));
            nextIn = await next.ExecuteScalarAsync(cancellationToken) is double seconds ? Seconds(seconds) : null;
        }
        if (occurrences > 0)
        {
            await NotifyDueAsync(work, source, cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        return new(locked.Length, occurrences, nextIn, revision);
    }

    /// <summary>Plans one locked recurrence; returns how many occurrences it created.</summary>
    private async Task<int> PlanRecurrenceAsync(
        PostgreSqlTransaction work,
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
        DateTimeOffset? lastEnded = null;
        // The occurrence an after-completion schedule awaits; it is named by its occurrence identity when planned in
        // this pass, so an insert that met an existing occurrence names that one.
        string? awaitedOccurrence = null;
        foreach (var occurrence in ended)
        {
            if (afterCompletion && next is null && occurrence.Id == awaited)
            {
                lastEnded = occurrence.EndedAt;
                next = lastEnded + schedule.Period;
                awaited = null;
            }
        }
        var created = 0;
        var commands = new List<NpgsqlBatchCommand>();
        if (ended.Count > 0)
        {
            commands.Add(Command($"UPDATE {sql.Table} SET unconsumed = false WHERE id = ANY($1)", Uuids([.. ended.Select(e => e.Id)])));
        }
        // An after-completion schedule awaits only an instant it planned; its waiting occurrence comes from a trigger.
        // A released waiting occurrence is planned at the next allowed instant of its calendar, a scheduled instant only
        // when the calendar allows it.
        void Insert(DateTimeOffset instant, bool skip, bool awaits, bool released = false)
        {
            var occurrence = OccurrenceOf(instant);
            var command = Command(InsertOccurrence(sql, "$4", skip, released ? CalendarStart.MoveForward : CalendarStart.Instant), Uuid(JobId.New().Value), Uuid(recurrence.Id), Text(occurrence));
            if (!skip)
            {
                command.Parameters.Add(new NpgsqlParameter<DateTimeOffset> { TypedValue = instant.ToUniversalTime(), NpgsqlDbType = NpgsqlDbType.TimestampTz });
                created++;
                if (afterCompletion && awaits)
                {
                    awaitedOccurrence = occurrence;
                }
            }
            commands.Add(command);
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
        commands.Add(Command(
            $"UPDATE {recurrences} SET next_at = $2, waiting_at = $3, last_ended_at = COALESCE($6, last_ended_at), updated_at = clock_timestamp(), "
            + $"awaited_job = CASE WHEN $5::text IS NULL THEN $4 ELSE (SELECT j.id FROM {sql.Table} AS j WHERE j.recurrence_id = $1 AND j.occurrence = $5) END WHERE id = $1",
            Uuid(recurrence.Id),
            Nullable(next?.ToUniversalTime(), NpgsqlDbType.TimestampTz),
            Nullable(waiting?.ToUniversalTime(), NpgsqlDbType.TimestampTz),
            Nullable(awaited, NpgsqlDbType.Uuid),
            Nullable(awaitedOccurrence, NpgsqlDbType.Text),
            Nullable(lastEnded?.ToUniversalTime(), NpgsqlDbType.TimestampTz)));
        await using var batch = new NpgsqlBatch(work.Connection, work.Transaction);
        foreach (var command in commands)
        {
            batch.BatchCommands.Add(command);
        }
        await batch.ExecuteNonQueryAsync(cancellationToken);
        return created;
    }

    /// <summary>
    /// Ends jobs recorded to be skipped, one per transaction: each is locked without waiting and ended skipped through
    /// the terminal path, which runs the outcome hook, while no recurrence is locked, with the reason its calendar or
    /// chain recorded or <c>overlap</c>; an occurrence whose chain settled ends with the chain's outcome. They need no
    /// claim, so pause and removal of their recurrence do not hold them back.
    /// One whose transition fails, for example in its outcome hook, stays recorded and is skipped for the rest of the
    /// call.
    /// </summary>
    public async ValueTask<int> FinalizeSkippedAsync(LeaseSource<JobId> source, JobEndedCallback? ended, int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var sql = Sql(source);
        const string skipping = "r.state = 0 AND (r.skip_pending OR r.chain_outcome IS NOT NULL)";
        var failed = new List<Guid>();
        var count = 0;
        while (count < limit)
        {
            await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
            var work = new PostgreSqlTransaction(transaction);
            Guid job;
            string? reason;
            var outcome = JobOutcome.Skipped;
            await using (var select = new NpgsqlCommand(
                $"SELECT r.id, COALESCE(r.skip_reason, $2), r.skip_pending, r.chain_outcome FROM {sql.Table} AS r WHERE {sql.Scope} AND {skipping} AND r.id <> ALL($1) ORDER BY r.id LIMIT 1 FOR UPDATE OF r SKIP LOCKED",
                connection,
                transaction))
            {
                select.Parameters.Add(Uuids([.. failed]));
                select.Parameters.Add(Text(JobReasons.Overlap));
                await using var reader = await select.ExecuteReaderAsync(cancellationToken);
                if (!await reader.ReadAsync(cancellationToken))
                {
                    return count;
                }
                job = reader.GetGuid(0);
                reason = reader.GetString(1);
                if (!reader.GetBoolean(2))
                {
                    // An occurrence whose chain settled ends with the chain's outcome.
                    outcome = (JobOutcome)reader.GetInt16(3);
                    reason = OwnerReason(outcome);
                }
            }
            try
            {
                // A standalone job skipped by its calendar starts a pending follow-up; occurrences have none.
                if (await EndJobAsync(work, source, new JobId(job), outcome, reason, discardPending: false, skipping, [], ended, cancellationToken))
                {
                    await transaction.CommitAsync(cancellationToken);
                    count++;
                }
            }
            catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
            {
                failed.Add(job);
                LogFinalizeFailed(logger, telemetry.Logged(exception), sql.Job!.Definition, job, telemetry.Describe(exception));
            }
        }
        return count;
    }
}
