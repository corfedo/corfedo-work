using System.Globalization;
using Corfedo.Work.PostgreSql.Leases;
using Npgsql;
using NpgsqlTypes;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Jobs;

internal sealed partial class PostgreSqlJobStore
{
    private const string ScheduleColumns = "schedule_kind, period, interval_kind, anchor, expression, time_zone";

    public async ValueTask<RecurrenceOutcome> ScheduleAsync(WorkTransaction transaction, LeaseSource<JobId> source, RecurrenceData recurrence, IRecurrenceCalculator calculator, CancellationToken cancellationToken)
    {
        Validate(recurrence, calculator);
        return await StoreRecurrenceAsync(PostgreSqlTransaction.From(transaction), source, recurrence, calculator, null, null, cancellationToken);
    }

    public async ValueTask<RecurrenceOutcome> DeclareRecurrenceAsync(LeaseSource<JobId> source, RecurrenceData recurrence, long revision, byte[] digest, IRecurrenceCalculator calculator, CancellationToken cancellationToken)
    {
        Validate(recurrence, calculator);
        ArgumentNullException.ThrowIfNull(digest);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        var outcome = await StoreRecurrenceAsync(new PostgreSqlTransaction(transaction), source, recurrence, calculator, revision, digest, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return outcome;
    }

    private static void Validate(RecurrenceData recurrence, IRecurrenceCalculator calculator)
    {
        ArgumentNullException.ThrowIfNull(recurrence);
        ArgumentNullException.ThrowIfNull(calculator);
        if (!recurrence.Schedule.Recurring)
        {
            throw new ArgumentException($"A recurrence needs a recurring schedule, not {recurrence.Schedule.Kind}.", nameof(recurrence));
        }
        if (recurrence.Mode == RequestMode.New)
        {
            throw new ArgumentException("An identity has one recurrence; RequestMode.New does not apply to recurring schedules.", nameof(recurrence));
        }
    }

    /// <summary>
    /// Requires READ COMMITTED as every recurrence operation does, then locks the identity's recurrence, waiting, or
    /// creates it. A declared recurrence (<paramref name="revision"/> set) replaces only a recurrence stored by an older
    /// revision or by none. The stored recurrence copies its subject's pause to its flag; a copy left stale by a
    /// concurrent pause or resume is repaired by maintenance. The first instant follows the store's clock read after the
    /// lock. The recurrence's key is queued for the monitoring loop, which is signalled. Standalone jobs of the identity
    /// are left alone.
    /// </summary>
    private async Task<RecurrenceOutcome> StoreRecurrenceAsync(PostgreSqlTransaction work, LeaseSource<JobId> source, RecurrenceData recurrence, IRecurrenceCalculator calculator, long? revision, byte[]? digest, CancellationToken cancellationToken)
    {
        var sql = Sql(source);
        var definition = sql.Job!.Definition;
        var partition = recurrence.Partition ?? "";
        if (calculator.First(recurrence.Schedule, DateTimeOffset.UtcNow) is null)
        {
            throw new ArgumentException("The schedule has no future occurrence.", nameof(recurrence));
        }
        await RequireRecurrenceIsolationAsync(work, cancellationToken);
        if (recurrence.Schedule.Calendar is { } calendar && !(await calendars.LoadAsync(work, cancellationToken)).Contains(calendar))
        {
            throw new ArgumentException($"Calendar {calendar} is not stored; register it with AddCalendar.", nameof(recurrence));
        }
        Guid id;
        bool created;
        while (true)
        {
            await using (var select = new NpgsqlCommand(
                $"SELECT id, partition_key, declared_revision, declared_digest FROM {recurrences} WHERE definition = $1 AND merge_key = $2 FOR UPDATE",
                work.Connection,
                work.Transaction))
            {
                select.Parameters.Add(Text(definition));
                select.Parameters.Add(Text(recurrence.Identity));
                await using var reader = await select.ExecuteReaderAsync(cancellationToken);
                if (await reader.ReadAsync(cancellationToken))
                {
                    id = reader.GetGuid(0);
                    if (reader.GetString(1) != partition)
                    {
                        throw new JobPartitionMismatchException($"The recurrence of {definition} with this identity belongs to another partition.");
                    }
                    created = false;
                    if (revision is { } declared && !reader.IsDBNull(2))
                    {
                        var stored = reader.GetInt64(2);
                        if (stored > declared)
                        {
                            return RecurrenceOutcome.Existing;
                        }
                        if (stored == declared)
                        {
                            if (!reader.GetFieldValue<byte[]>(3).AsSpan().SequenceEqual(digest))
                            {
                                throw new JobDefinitionConflictException(
                                    $"The recurrence of job definition {definition} revision {declared} is stored with different content; increase the revision when the recurrence changes.");
                            }
                            return RecurrenceOutcome.Existing;
                        }
                    }
                    if (recurrence.Mode == RequestMode.IfAbsent)
                    {
                        return RecurrenceOutcome.Existing;
                    }
                    break;
                }
            }
            await using var insert = new NpgsqlCommand(
                $"INSERT INTO {recurrences} (id, definition, merge_key, subject_kind, subject_id, partition_key, args, args_version, {ScheduleColumns}, "
                + "missed, overlap, paused, created_at, updated_at) "
                + $"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, NULL, NULL, NULL, NULL, NULL, 0, 0, {PausedCopy("$2", "$4", "$5")}, clock_timestamp(), clock_timestamp()) "
                + "ON CONFLICT (definition, merge_key) DO NOTHING",
                work.Connection,
                work.Transaction);
            id = Guid.CreateVersion7();
            insert.Parameters.Add(Uuid(id));
            insert.Parameters.Add(Text(definition));
            insert.Parameters.Add(Text(recurrence.Identity));
            insert.Parameters.Add(Nullable(recurrence.Subject?.Kind, NpgsqlDbType.Text));
            insert.Parameters.Add(Nullable(recurrence.Subject?.Id, NpgsqlDbType.Text));
            insert.Parameters.Add(Text(partition));
            insert.Parameters.Add(new NpgsqlParameter<byte[]> { TypedValue = recurrence.Args, NpgsqlDbType = NpgsqlDbType.Bytea });
            insert.Parameters.Add(Int32(recurrence.ArgsVersion));
            if (await insert.ExecuteNonQueryAsync(cancellationToken) == 1)
            {
                created = true;
                break;
            }
            // A concurrent transaction created it and committed; lock that one.
        }

        DateTimeOffset now;
        await using (var clock = new NpgsqlCommand("SELECT clock_timestamp()", work.Connection, work.Transaction))
        await using (var reader = await clock.ExecuteReaderAsync(cancellationToken))
        {
            await reader.ReadAsync(cancellationToken);
            now = reader.GetFieldValue<DateTimeOffset>(0);
        }
        var schedule = recurrence.Schedule;
        var next = calculator.First(schedule, now) ?? throw new ArgumentException("The schedule has no future occurrence.", nameof(recurrence));
        await using (var update = new NpgsqlCommand(
            $"WITH updated AS (UPDATE {recurrences} AS c SET args = $2, args_version = $3, schedule_kind = $4, period = $5, interval_kind = $6, anchor = $7, expression = $8, "
            + "time_zone = $9, missed = $10, overlap = $11, awaited_job = NULL, next_at = $12, waiting_at = NULL, declared_revision = COALESCE($13, declared_revision), "
            + $"declared_digest = COALESCE($14, declared_digest), calendar = $15, calendar_closed = $16, "
            + $"paused = {PausedCopy("c.definition", "c.subject_kind", "c.subject_id")}, updated_at = clock_timestamp() WHERE id = $1 "
            + $"RETURNING c.definition, c.subject_kind, c.subject_id), queued AS (INSERT INTO {changes} (definition, subject_kind, subject_id) SELECT * FROM updated RETURNING definition) "
            + NotifyMonitor("queued", 17),
            work.Connection,
            work.Transaction))
        {
            update.Parameters.Add(Uuid(id));
            update.Parameters.Add(new NpgsqlParameter<byte[]> { TypedValue = recurrence.Args, NpgsqlDbType = NpgsqlDbType.Bytea });
            update.Parameters.Add(Int32(recurrence.ArgsVersion));
            AddSchedule(update.Parameters, schedule);
            update.Parameters.Add(new NpgsqlParameter<short> { TypedValue = (short)recurrence.Missed });
            update.Parameters.Add(new NpgsqlParameter<short> { TypedValue = (short)recurrence.Overlap });
            update.Parameters.Add(Nullable(next.ToUniversalTime(), NpgsqlDbType.TimestampTz));
            update.Parameters.Add(Nullable(revision, NpgsqlDbType.Bigint));
            update.Parameters.Add(Nullable(digest, NpgsqlDbType.Bytea));
            update.Parameters.Add(Nullable(schedule.Calendar, NpgsqlDbType.Text));
            update.Parameters.Add(Nullable(schedule.Calendar is null ? null : (short)schedule.Closed, NpgsqlDbType.Smallint));
            update.Parameters.Add(MonitorSignal(source));
            await signals.ExecuteAsync(update, cancellationToken);
        }
        await NotifyPlanAsync(work, source, cancellationToken);
        return created ? RecurrenceOutcome.Created : RecurrenceOutcome.Replaced;
    }

    /// <summary>
    /// Requires READ COMMITTED, locks the recurrence, waiting, then its outstanding occurrences in id order; cancels
    /// them, clears the unconsumed marks of its ended occurrences and deletes it, queueing its key for the monitoring
    /// loop and signalling it. Occurrences to be skipped are left to finalization.
    /// </summary>
    public async ValueTask<bool> RemoveAsync(WorkTransaction transaction, LeaseSource<JobId> source, string identity, JobEndedCallback? ended, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(identity);
        ArgumentNullException.ThrowIfNull(definitions);
        var work = PostgreSqlTransaction.From(transaction);
        var sql = Sql(source);
        await RequireRecurrenceIsolationAsync(work, cancellationToken);
        if (await LockRecurrenceAsync(work, sql, identity, cancellationToken) is not { } recurrence)
        {
            return false;
        }
        var locked = await LockForCancelAsync(
            work,
            $"SELECT r.id, r.chain_id, r.owned_chain FROM {sql.Table} AS r WHERE {sql.Scope} AND r.recurrence_id = $1 AND r.state = 0 AND NOT r.skip_pending",
            [Uuid(recurrence.Id)],
            cancellationToken);
        await CancelSelectedAsync(work, locked, key => key == sql.Job!.Definition ? (source, ended) : definitions(key), cancellationToken);
        await using var batch = new NpgsqlBatch(work.Connection, work.Transaction);
        batch.BatchCommands.Add(Command($"UPDATE {sql.Table} SET unconsumed = false WHERE recurrence_id = $1 AND unconsumed", Uuid(recurrence.Id)));
        batch.BatchCommands.Add(Command(
            $"WITH removed AS (DELETE FROM {recurrences} WHERE id = $1 RETURNING definition, subject_kind, subject_id), "
            + $"queued AS (INSERT INTO {changes} (definition, subject_kind, subject_id) SELECT * FROM removed RETURNING definition) "
            + NotifyMonitor("queued", 2),
            Uuid(recurrence.Id),
            MonitorSignal(source)));
        await signals.ExecuteAsync(batch, cancellationToken);
        return true;
    }

    /// <summary>
    /// Requires READ COMMITTED, locks the recurrence, waiting, then its outstanding occurrences, and triggers it as
    /// <see cref="TriggerLockedAsync"/> describes. The recurrence's schedule does not move.
    /// </summary>
    public async ValueTask<RunNowResult> TriggerAsync(WorkTransaction transaction, LeaseSource<JobId> source, string identity, bool ignoreCalendar, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(identity);
        var work = PostgreSqlTransaction.From(transaction);
        var sql = Sql(source);
        await RequireRecurrenceIsolationAsync(work, cancellationToken);
        var recurrence = await LockRecurrenceAsync(work, sql, identity, cancellationToken)
            ?? throw new InvalidOperationException($"Job definition {sql.Job!.Definition} has no recurrence with this identity.");
        var jobs = await LockOccurrencesAsync(work, sql, recurrence.Id, cancellationToken);
        return await TriggerLockedAsync(work, source, recurrence, jobs, ignoreCalendar, cancellationToken);
    }

    /// <summary>
    /// Triggers a locked recurrence whose outstanding occurrences are locked. A running occurrence follows the overlap
    /// policy: under <see cref="Overlap.Wait"/> the recurrence records its waiting occurrence, which the planner starts
    /// once no occurrence is outstanding, under <see cref="Overlap.Skip"/> it is reported running, and under
    /// <see cref="Overlap.Allow"/> another occurrence starts. Otherwise one that is not running is made due now with its
    /// attempts kept, and with none a triggered occurrence is created due now; with a calendar both are planned at the
    /// next allowed instant unless <paramref name="ignoreCalendar"/>.
    /// </summary>
    private async Task<RunNowResult> TriggerLockedAsync(PostgreSqlTransaction work, LeaseSource<JobId> source, LockedRecurrence recurrence, List<LockedOccurrence> jobs, bool ignoreCalendar, CancellationToken cancellationToken)
    {
        var sql = Sql(source);
        if (jobs.FirstOrDefault(j => j.Running) is { } running && recurrence.Overlap != Overlap.Allow)
        {
            if (recurrence.Overlap == Overlap.Skip)
            {
                return new(new JobId(running.Id), RunNowOutcome.AlreadyRunning);
            }
            await using var wait = new NpgsqlCommand(
                $"UPDATE {recurrences} SET waiting_at = COALESCE(waiting_at, clock_timestamp()), updated_at = clock_timestamp() WHERE id = $1",
                work.Connection,
                work.Transaction);
            wait.Parameters.Add(Uuid(recurrence.Id));
            await wait.ExecuteNonQueryAsync(cancellationToken);
            return new(new JobId(running.Id), RunNowOutcome.Waiting);
        }
        RunNowResult result;
        if (jobs.FirstOrDefault(j => !j.Running) is { } waiting)
        {
            await MakeDueNowAsync(work, sql, waiting.Id, ignoreCalendar, cancellationToken);
            result = new(new JobId(waiting.Id), RunNowOutcome.MadeDue);
        }
        else
        {
            var id = JobId.New().Value;
            await using var insert = new NpgsqlCommand(
                InsertOccurrence(sql, "clock_timestamp()", skip: false, ignoreCalendar ? CalendarStart.Ignore : CalendarStart.MoveForward),
                work.Connection,
                work.Transaction);
            insert.Parameters.Add(Uuid(id));
            insert.Parameters.Add(Uuid(recurrence.Id));
            insert.Parameters.Add(Text("trigger:" + Guid.CreateVersion7().ToString("N")));
            await insert.ExecuteNonQueryAsync(cancellationToken);
            result = new(new JobId(id), RunNowOutcome.Started);
        }
        await (recurrence.Calendar && !ignoreCalendar ? NotifyPlanAsync(work, source, cancellationToken) : NotifyDueAsync(work, source, cancellationToken));
        return result;
    }

    private sealed record LockedRecurrence(Guid Id, Overlap Overlap, bool Calendar);

    private sealed record LockedOccurrence(Guid Id, bool Running);

    /// <summary>
    /// Refuses a caller's transaction that is not READ COMMITTED before any lock or write: recurrence operations find
    /// occurrences the planner committed after the transaction began.
    /// </summary>
    private static async Task RequireRecurrenceIsolationAsync(PostgreSqlTransaction work, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("SELECT current_setting('transaction_isolation')", work.Connection, work.Transaction);
        var level = (string)(await command.ExecuteScalarAsync(cancellationToken))!;
        if (level != "read committed")
        {
            throw new InvalidOperationException($"Recurrence operations in the caller's transaction need READ COMMITTED isolation; the transaction uses {level.ToUpperInvariant()}.");
        }
    }

    /// <summary>Locks the recurrence with its id, waiting; null when there is none.</summary>
    private async Task<LockedRecurrence?> LockRecurrenceAsync(PostgreSqlTransaction work, Guid id, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand($"SELECT id, overlap, calendar IS NOT NULL FROM {recurrences} WHERE id = $1 FOR UPDATE", work.Connection, work.Transaction);
        command.Parameters.Add(Uuid(id));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? new(reader.GetGuid(0), (Overlap)reader.GetInt16(1), reader.GetBoolean(2)) : null;
    }

    private async Task<LockedRecurrence?> LockRecurrenceAsync(PostgreSqlTransaction work, LeaseSql sql, string identity, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand($"SELECT id, overlap, calendar IS NOT NULL FROM {recurrences} WHERE definition = $1 AND merge_key = $2 FOR UPDATE", work.Connection, work.Transaction);
        command.Parameters.Add(Text(sql.Job!.Definition));
        command.Parameters.Add(Text(identity));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? new(reader.GetGuid(0), (Overlap)reader.GetInt16(1), reader.GetBoolean(2)) : null;
    }

    /// <summary>
    /// Locks the recurrence's outstanding occurrences in id order, waiting, leaving out those to be skipped; running is
    /// evaluated after the locks are held.
    /// </summary>
    private static async Task<List<LockedOccurrence>> LockOccurrencesAsync(PostgreSqlTransaction work, LeaseSql sql, Guid recurrence, CancellationToken cancellationToken)
    {
        await using var batch = new NpgsqlBatch(work.Connection, work.Transaction);
        batch.BatchCommands.Add(Command(
            $"SELECT r.id FROM {sql.Table} AS r WHERE {sql.Scope} AND r.recurrence_id = $1 AND r.state = 0 AND NOT r.skip_pending ORDER BY r.id FOR UPDATE OF r",
            Uuid(recurrence)));
        batch.BatchCommands.Add(Command(
            $"SELECT r.id, {Running} FROM {sql.Table} AS r "
            + $"WHERE {sql.Scope} AND r.recurrence_id = $1 AND r.state = 0 AND NOT r.skip_pending ORDER BY r.id",
            Uuid(recurrence)));
        await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
        await reader.NextResultAsync(cancellationToken);
        var jobs = new List<LockedOccurrence>();
        while (await reader.ReadAsync(cancellationToken))
        {
            jobs.Add(new(reader.GetGuid(0), reader.GetBoolean(1)));
        }
        return jobs;
    }

    /// <summary>
    /// Inserts an occurrence of the recurrence in <c>$2</c> with job id <c>$1</c> and occurrence identity <c>$3</c>,
    /// taking arguments, subject, partition and calendar from the recurrence. One to be skipped has no due time; with a
    /// calendar, <paramref name="due"/> is its calendar instant, planned under <paramref name="start"/>. Occurrences have
    /// no merge key, so they never meet standalone jobs on the outstanding-identity index.
    /// </summary>
    private string InsertOccurrence(LeaseSql sql, string due, bool skip, CalendarStart start = CalendarStart.Instant)
    {
        var calendar = start == CalendarStart.Ignore ? "false" : "c.calendar IS NOT NULL";
        var blocked = start == CalendarStart.Instant ? Blocked.Skip : Blocked.MoveForward;
        return $"INSERT INTO {sql.Table} (id, definition, recurrence_id, subject_kind, subject_id, partition_key, "
            + "args, args_version, due_at, calendar_at, window_end, calendar, calendar_blocked, calendar_closed, skip_pending, first_requested_at, created_at, execution_id, occurrence, paused) "
            + $"SELECT $1, c.definition, c.id, c.subject_kind, c.subject_id, c.partition_key, c.args, c.args_version, "
            + (skip ? "NULL, NULL, " : $"CASE WHEN NOT {calendar} THEN {due} END, CASE WHEN {calendar} THEN {due} END, ")
            + (start == CalendarStart.Ignore ? "CASE WHEN c.calendar IS NOT NULL THEN 'infinity'::timestamptz END, " : "NULL, ")
            + $"c.calendar, CASE WHEN c.calendar IS NOT NULL THEN {(short)blocked} END, c.calendar_closed, {(skip ? "true" : "false")}, clock_timestamp(), clock_timestamp(), gen_random_uuid(), $3, "
            + $"{PausedCopy("c.definition", "c.subject_kind", "c.subject_id")} FROM {recurrences} AS c WHERE c.id = $2 "
            + "ON CONFLICT (recurrence_id, occurrence) WHERE recurrence_id IS NOT NULL DO NOTHING";
    }

    /// <summary>How a new occurrence of a recurrence with a calendar is planned.</summary>
    private enum CalendarStart
    {
        /// <summary>A scheduled instant: recorded skipped when the calendar blocks it.</summary>
        Instant,

        /// <summary>A waiting or triggered occurrence: planned at the next allowed instant.</summary>
        MoveForward,

        /// <summary>A triggered occurrence that ignores the calendar: due at once, with an endless window.</summary>
        Ignore,
    }

    private static void AddSchedule(NpgsqlParameterCollection parameters, Schedule schedule)
    {
        parameters.Add(new NpgsqlParameter<short> { TypedValue = (short)schedule.Kind });
        var every = schedule.Kind == ScheduleKind.Every;
        parameters.Add(Nullable(every ? schedule.Period : null, NpgsqlDbType.Interval));
        parameters.Add(Nullable(every ? (short)schedule.Interval : null, NpgsqlDbType.Smallint));
        parameters.Add(Nullable(every && schedule.Interval == Interval.FixedRate ? schedule.Anchor.ToUniversalTime() : null, NpgsqlDbType.TimestampTz));
        parameters.Add(Nullable(schedule.Expression, NpgsqlDbType.Text));
        parameters.Add(Nullable(schedule.TimeZone, NpgsqlDbType.Text));
    }

    /// <summary>Reads a schedule stored by <see cref="AddSchedule"/> from six columns starting at <paramref name="first"/>.</summary>
    private static Schedule ReadSchedule(NpgsqlDataReader reader, int first) =>
        (ScheduleKind)reader.GetInt16(first) == ScheduleKind.Cron
            ? Schedule.Cron(reader.GetString(first + 4), reader.GetString(first + 5))
            : (Interval)reader.GetInt16(first + 2) == Interval.FixedRate
                ? Schedule.Every(reader.GetFieldValue<TimeSpan>(first + 1), Interval.FixedRate, reader.GetFieldValue<DateTimeOffset>(first + 3))
                : Schedule.Every(reader.GetFieldValue<TimeSpan>(first + 1), Interval.AfterCompletion);

    /// <summary>The occurrence identity of a scheduled instant: ISO 8601 in UTC.</summary>
    private static string OccurrenceOf(DateTimeOffset instant) => instant.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'", CultureInfo.InvariantCulture);
}
