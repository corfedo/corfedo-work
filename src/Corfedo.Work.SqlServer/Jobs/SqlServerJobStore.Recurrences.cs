using System.Data;
using System.Globalization;
using Corfedo.Work.SqlServer.Leases;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Jobs;

internal sealed partial class SqlServerJobStore
{
    private const string ScheduleColumns = "schedule_kind, period, interval_kind, anchor, expression, time_zone";

    public async ValueTask<RecurrenceOutcome> ScheduleAsync(WorkTransaction transaction, LeaseSource<JobId> source, RecurrenceData recurrence, IRecurrenceCalculator calculator, CancellationToken cancellationToken)
    {
        Validate(recurrence, calculator);
        return await StoreRecurrenceAsync(SqlServerTransaction.From(transaction), source, recurrence, calculator, null, null, cancellationToken);
    }

    public async ValueTask<RecurrenceOutcome> DeclareRecurrenceAsync(LeaseSource<JobId> source, RecurrenceData recurrence, long revision, byte[] digest, IRecurrenceCalculator calculator, CancellationToken cancellationToken)
    {
        Validate(recurrence, calculator);
        ArgumentNullException.ThrowIfNull(digest);
        return await InTransactionAsync(work => StoreRecurrenceAsync(work, source, recurrence, calculator, revision, digest, cancellationToken), cancellationToken);
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
    /// creates it; a creation that meets one committed meanwhile locks that one. A declared recurrence
    /// (<paramref name="revision"/> set) replaces only a recurrence stored by an older revision or by none. The stored
    /// recurrence copies its subject's pause to its flag; a copy left stale by a concurrent pause or resume is repaired
    /// by maintenance. The first instant follows the store's clock read after the lock. The recurrence's key is queued
    /// for the monitoring loop, which is signalled. Standalone jobs of the identity are left alone.
    /// </summary>
    private async Task<RecurrenceOutcome> StoreRecurrenceAsync(SqlServerTransaction work, LeaseSource<JobId> source, RecurrenceData recurrence, IRecurrenceCalculator calculator, long? revision, byte[]? digest, CancellationToken cancellationToken)
    {
        var sql = await SqlAsync(source, cancellationToken);
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
        await using (var select = Tsql.Command(work, $"""
            DECLARE @rid uniqueidentifier, @created bit = 0;
            {LockRecurrenceStatements()}
            IF @rid IS NULL
            BEGIN
                SET XACT_ABORT OFF;
                BEGIN TRY
                    INSERT INTO {recurrences} (id, [definition], merge_key, subject_kind, subject_id, partition_key, args, args_version, {ScheduleColumns},
                        missed, overlap, paused, created_at, updated_at)
                    VALUES (@new_id, @definition, @identity, @kind, @subject, @partition, @args, @args_version, 0, NULL, NULL, NULL, NULL, NULL, 0, 0,
                        {PausedCopy("@definition", "@kind", "@subject")}, SYSUTCDATETIME(), SYSUTCDATETIME());
                    SELECT @rid = @new_id, @created = 1;
                END TRY
                BEGIN CATCH
                    SET XACT_ABORT ON;
                    IF ERROR_NUMBER() NOT IN (2601, 2627) THROW;
                END CATCH
                SET XACT_ABORT ON;
                IF @rid IS NULL
                BEGIN
                    -- A concurrent transaction created it and committed; lock that one.
                    {LockRecurrenceStatements(declare: false).Replace("\n", "\n        ", StringComparison.Ordinal)}
                END
            END
            SELECT id, partition_key, declared_revision, declared_digest, @created FROM {recurrences} WHERE id = @rid;
            """,
            Tsql.Guid("@new_id", Guid.CreateVersion7()),
            Tsql.Text("@definition", definition, 200),
            Tsql.Text("@identity", recurrence.Identity, 512),
            Tsql.Text("@kind", recurrence.Subject?.Kind, 100),
            Tsql.Text("@subject", recurrence.Subject?.Id, 400),
            Tsql.Text("@partition", partition, 400),
            Tsql.Bytes("@args", recurrence.Args),
            Tsql.Int32("@args_version", recurrence.ArgsVersion)))
        await using (var reader = await select.ReaderAsync(cancellationToken))
        {
            if (!await reader.NextRowAsync(cancellationToken))
            {
                throw new InvalidOperationException($"The recurrence of {definition} with this identity was removed while it was being stored.");
            }
            id = reader.GetGuid(0);
            created = reader.GetBoolean(4);
            if (!created)
            {
                if (reader.GetString(1) != partition)
                {
                    throw new JobPartitionMismatchException($"The recurrence of {definition} with this identity belongs to another partition.");
                }
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
            }
        }

        var now = await ClockAsync(work, cancellationToken);
        var schedule = recurrence.Schedule;
        var next = calculator.First(schedule, now) ?? throw new ArgumentException("The schedule has no future occurrence.", nameof(recurrence));
        await using (var update = Tsql.Command(work, $"""
            UPDATE c SET args = @args, args_version = @args_version, schedule_kind = @schedule_kind, period = @period, interval_kind = @interval_kind,
                anchor = @anchor, expression = @expression, time_zone = @time_zone, missed = @missed, overlap = @overlap, awaited_job = NULL, next_at = @next,
                waiting_at = NULL, declared_revision = COALESCE(@revision, c.declared_revision), declared_digest = COALESCE(@digest, c.declared_digest),
                calendar = @calendar, calendar_closed = @calendar_closed, paused = {PausedCopy("c.[definition]", "c.subject_kind", "c.subject_id")},
                updated_at = SYSUTCDATETIME()
            OUTPUT inserted.[definition], inserted.subject_kind, inserted.subject_id INTO {changes} ([definition], subject_kind, subject_id)
            FROM {recurrences} AS c WHERE c.id = @rid;
            {NotifyMonitor("@@ROWCOUNT > 0")}
            """,
            [
                Tsql.Guid("@rid", id),
                Tsql.Text("@definition", definition, 200),
                Tsql.Bytes("@args", recurrence.Args),
                Tsql.Int32("@args_version", recurrence.ArgsVersion),
                .. ScheduleParameters(schedule),
                Tsql.Int16("@missed", (short)recurrence.Missed),
                Tsql.Int16("@overlap", (short)recurrence.Overlap),
                Tsql.Time("@next", next),
                Tsql.Int64("@revision", revision),
                Tsql.Bytes("@digest", digest),
                Tsql.Text("@calendar", schedule.Calendar, 128),
                Tsql.Int16("@calendar_closed", schedule.Calendar is null ? null : (short)schedule.Closed),
                MonitorSignal(source),
            ]))
        {
            await update.NonQueryAsync(cancellationToken);
        }
        await NotifyPlanAsync(work, source, cancellationToken);
        return created ? RecurrenceOutcome.Created : RecurrenceOutcome.Replaced;
    }

    /// <summary>
    /// Statements that lock the recurrence of <c>@definition</c> and <c>@identity</c> through its key and set its id in
    /// <c>@rid</c>, null when there is none: the id is read without a lock, the row locked by id, waiting, and read again
    /// after the lock, so a recurrence removed and created again meanwhile is found anew.
    /// </summary>
    private string LockRecurrenceStatements(bool declare = true) => (declare ? "DECLARE @found_rid uniqueidentifier, @recurrence_tries int;\n" : "") + $"""
        SET @recurrence_tries = 0;
        WHILE 1 = 1
        BEGIN
            SET @found_rid = NULL;
            SELECT @found_rid = id FROM {recurrences} WHERE [definition] = @definition AND merge_key = @identity;
            IF @found_rid IS NULL BREAK;
            SELECT @found_rid = id FROM {recurrences} WITH (UPDLOCK, ROWLOCK) WHERE id = @found_rid;
            IF EXISTS (SELECT 1 FROM {recurrences} WHERE id = @found_rid AND [definition] = @definition AND merge_key = @identity) BREAK;
            SET @recurrence_tries += 1;
            IF @recurrence_tries >= 10 THROW 51010, N'The recurrence kept being replaced while it was locked.', 1;
        END
        SET @rid = @found_rid;
        """;

    /// <summary>
    /// Requires READ COMMITTED, locks the recurrence, waiting, then its outstanding occurrences in the cancellation
    /// order; cancels them, clears the unconsumed marks of its ended occurrences and deletes it, queueing its key for the
    /// monitoring loop and signalling it. Occurrences to be skipped are left to finalization.
    /// </summary>
    public async ValueTask<bool> RemoveAsync(WorkTransaction transaction, LeaseSource<JobId> source, string identity, JobEndedCallback? ended, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(identity);
        ArgumentNullException.ThrowIfNull(definitions);
        var work = SqlServerTransaction.From(transaction);
        var sql = await SqlAsync(source, cancellationToken);
        await RequireRecurrenceIsolationAsync(work, cancellationToken);
        if (await LockRecurrenceAsync(work, sql, identity, cancellationToken) is not { } recurrence)
        {
            return false;
        }
        var locked = await LockForCancelAsync(
            work,
            $"{sql.Scope} AND r.recurrence_id = @rid AND r.state = 0 AND r.skip_pending = 0",
            [Tsql.Guid("@rid", recurrence.Id)],
            cancellationToken);
        await CancelSelectedAsync(work, locked, key => key == sql.Job!.Definition ? (source, ended) : definitions(key), cancellationToken);
        await using var command = Tsql.Command(work, $"""
            DECLARE @unconsumed nvarchar(max) = (SELECT r.id FROM {sql.Table} AS r WITH (INDEX(jobs_unconsumed))
                WHERE r.[definition] = @definition AND r.unconsumed = 1 AND r.recurrence_id = @rid ORDER BY r.id FOR JSON PATH);
            UPDATE r SET unconsumed = 0 FROM OPENJSON(@unconsumed) WITH (id uniqueidentifier '$.id') AS u
            INNER LOOP JOIN {sql.Table} AS r WITH (UPDLOCK, ROWLOCK, INDEX(jobs_key), FORCESEEK) ON r.id = u.id WHERE r.unconsumed = 1;
            DELETE FROM {recurrences} OUTPUT deleted.[definition], deleted.subject_kind, deleted.subject_id INTO {changes} ([definition], subject_kind, subject_id) WHERE id = @rid;
            {NotifyMonitor("@@ROWCOUNT > 0")}
            """,
            Tsql.Guid("@rid", recurrence.Id),
            Tsql.Text("@definition", sql.Job!.Definition, 200),
            MonitorSignal(source));
        await command.NonQueryAsync(cancellationToken);
        return true;
    }

    /// <summary>
    /// Requires READ COMMITTED, locks the recurrence, waiting, then its outstanding occurrences, and triggers it as
    /// <see cref="TriggerLockedAsync"/> describes. The recurrence's schedule does not move.
    /// </summary>
    public async ValueTask<RunNowResult> TriggerAsync(WorkTransaction transaction, LeaseSource<JobId> source, string identity, bool ignoreCalendar, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(identity);
        var work = SqlServerTransaction.From(transaction);
        var sql = await SqlAsync(source, cancellationToken);
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
    private async Task<RunNowResult> TriggerLockedAsync(SqlServerTransaction work, LeaseSource<JobId> source, LockedRecurrence recurrence, List<LockedOccurrence> jobs, bool ignoreCalendar, CancellationToken cancellationToken)
    {
        var sql = await SqlAsync(source, cancellationToken);
        if (jobs.FirstOrDefault(j => j.Running) is { } running && recurrence.Overlap != Overlap.Allow)
        {
            if (recurrence.Overlap == Overlap.Skip)
            {
                return new(new JobId(running.Id), RunNowOutcome.AlreadyRunning);
            }
            await using var wait = Tsql.Command(work,
                $"UPDATE {recurrences} SET waiting_at = COALESCE(waiting_at, SYSUTCDATETIME()), updated_at = SYSUTCDATETIME() WHERE id = @rid;",
                Tsql.Guid("@rid", recurrence.Id));
            await wait.NonQueryAsync(cancellationToken);
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
            await using var insert = Tsql.Command(work,
                InsertOccurrence(sql, "SYSUTCDATETIME()", skip: false, ignoreCalendar ? CalendarStart.Ignore : CalendarStart.MoveForward),
                Tsql.Guid("@job", id),
                Tsql.Guid("@rid", recurrence.Id),
                Tsql.Text("@occurrence", "trigger:" + Guid.CreateVersion7().ToString("N"), 200));
            await insert.NonQueryAsync(cancellationToken);
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
    private static Task RequireRecurrenceIsolationAsync(SqlServerTransaction work, CancellationToken cancellationToken) =>
        SqlServerLeaseStore.RequireReadCommittedAsync(work, "Recurrence operations in the caller's transaction", cancellationToken);

    /// <summary>Locks the recurrence with its id, waiting, and reads it after the lock; null when there is none.</summary>
    private async Task<LockedRecurrence?> LockRecurrenceAsync(SqlServerTransaction work, Guid id, CancellationToken cancellationToken)
    {
        await using var command = Tsql.Command(work, $"""
            DECLARE @found int;
            SELECT @found = 1 FROM {recurrences} WITH (UPDLOCK, ROWLOCK) WHERE id = @rid;
            SELECT id, overlap, {Tsql.Bit("calendar IS NOT NULL")} FROM {recurrences} WHERE id = @rid;
            """, Tsql.Guid("@rid", id));
        return await ReadLockedRecurrenceAsync(command, cancellationToken);
    }

    private async Task<LockedRecurrence?> LockRecurrenceAsync(SqlServerTransaction work, LeaseSql sql, string identity, CancellationToken cancellationToken)
    {
        await using var command = Tsql.Command(work, $"""
            DECLARE @rid uniqueidentifier;
            {LockRecurrenceStatements()}
            SELECT id, overlap, {Tsql.Bit("calendar IS NOT NULL")} FROM {recurrences} WHERE id = @rid;
            """,
            Tsql.Text("@definition", sql.Job!.Definition, 200),
            Tsql.Text("@identity", identity, 512));
        return await ReadLockedRecurrenceAsync(command, cancellationToken);
    }

    private static async Task<LockedRecurrence?> ReadLockedRecurrenceAsync(SqlCommand command, CancellationToken cancellationToken)
    {
        await using var reader = await command.ReaderAsync(cancellationToken);
        return await reader.NextRowAsync(cancellationToken) ? new(reader.GetGuid(0), (Overlap)reader.GetInt16(1), reader.GetBoolean(2)) : null;
    }

    /// <summary>
    /// Locks the recurrence's outstanding occurrences in id order, waiting, leaving out those to be skipped; they are
    /// found without locks, which the recurrence's lock makes complete since occurrences are created under it, and
    /// running is evaluated after the locks are held.
    /// </summary>
    private static async Task<List<LockedOccurrence>> LockOccurrencesAsync(SqlServerTransaction work, LeaseSql sql, Guid recurrence, CancellationToken cancellationToken)
    {
        await using var command = Tsql.Command(work, $"""
            DECLARE @occurrences nvarchar(max) = (SELECT r.id FROM {sql.Table} AS r WITH (INDEX(jobs_recurrence))
                WHERE r.recurrence_id IS NOT NULL AND r.recurrence_id = @rid AND r.state = 0 AND r.skip_pending = 0 AND {sql.Scope} ORDER BY r.id FOR JSON PATH);
            DECLARE @locked int;
            SELECT @locked = COUNT(*) FROM OPENJSON(@occurrences) WITH (id uniqueidentifier '$.id') AS o INNER LOOP JOIN {sql.Table} AS r WITH (UPDLOCK, ROWLOCK, INDEX(jobs_key), FORCESEEK) ON r.id = o.id;
            SELECT r.id, {Tsql.Bit(Running)} FROM OPENJSON(@occurrences) WITH (id uniqueidentifier '$.id') AS o INNER LOOP JOIN {sql.Table} AS r WITH (INDEX(jobs_key), FORCESEEK) ON r.id = o.id
            WHERE r.state = 0 AND r.skip_pending = 0 ORDER BY r.id;
            """, Tsql.Guid("@rid", recurrence));
        await using var reader = await command.ReaderAsync(cancellationToken);
        var jobs = new List<LockedOccurrence>();
        while (await reader.NextRowAsync(cancellationToken))
        {
            jobs.Add(new(reader.GetGuid(0), reader.GetBoolean(1)));
        }
        return jobs;
    }

    /// <summary>
    /// Inserts an occurrence of the recurrence in <c>@rid</c> with job id <c>@job</c> and occurrence identity
    /// <c>@occurrence</c>, taking arguments, subject, partition and calendar from the recurrence, unless the recurrence
    /// already has it: occurrences are created under the recurrence's lock, so the check sees every one committed. One
    /// to be skipped has no due time; with a calendar, <paramref name="due"/> is its calendar instant, planned under
    /// <paramref name="start"/>. Occurrences have no merge key, so they never meet standalone jobs on the
    /// outstanding-identity index.
    /// </summary>
    private string InsertOccurrence(LeaseSql sql, string due, bool skip, CalendarStart start = CalendarStart.Instant)
    {
        var calendar = start == CalendarStart.Ignore ? "1 = 0" : "c.calendar IS NOT NULL";
        var blocked = start == CalendarStart.Instant ? Blocked.Skip : Blocked.MoveForward;
        return $"INSERT INTO {sql.Table} (id, [definition], recurrence_id, subject_kind, subject_id, partition_key, "
            + "args, args_version, due_at, calendar_at, window_end, calendar, calendar_blocked, calendar_closed, skip_pending, first_requested_at, created_at, execution_id, occurrence, paused) "
            + "SELECT @job, c.[definition], c.id, c.subject_kind, c.subject_id, c.partition_key, c.args, c.args_version, "
            + (skip ? "NULL, NULL, " : $"CASE WHEN NOT ({calendar}) THEN {due} END, CASE WHEN {calendar} THEN {due} END, ")
            + (start == CalendarStart.Ignore ? $"CASE WHEN c.calendar IS NOT NULL THEN {Tsql.NeverLiteral} END, " : "NULL, ")
            + $"c.calendar, CASE WHEN c.calendar IS NOT NULL THEN {(short)blocked} END, c.calendar_closed, {(skip ? "1" : "0")}, SYSUTCDATETIME(), SYSUTCDATETIME(), NEWID(), @occurrence, "
            + $"{PausedCopy("c.[definition]", "c.subject_kind", "c.subject_id")} FROM {recurrences} AS c WHERE c.id = @rid "
            + $"AND NOT EXISTS (SELECT 1 FROM {sql.Table} AS o WHERE o.recurrence_id IS NOT NULL AND o.recurrence_id = @rid AND o.occurrence = @occurrence);\n";
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

    private static SqlParameter[] ScheduleParameters(Schedule schedule)
    {
        var every = schedule.Kind == ScheduleKind.Every;
        return
        [
            Tsql.Int16("@schedule_kind", (short)schedule.Kind),
            Tsql.Ticks("@period", every ? schedule.Period : null),
            Tsql.Int16("@interval_kind", every ? (short)schedule.Interval : null),
            Tsql.Time("@anchor", every && schedule.Interval == Interval.FixedRate ? schedule.Anchor : null),
            Tsql.Text("@expression", schedule.Expression, 400),
            Tsql.Text("@time_zone", schedule.TimeZone, 100),
        ];
    }

    /// <summary>Reads a schedule stored by <see cref="ScheduleParameters"/> from six columns starting at <paramref name="first"/>.</summary>
    private static Schedule ReadSchedule(SqlDataReader reader, int first) =>
        (ScheduleKind)reader.GetInt16(first) == ScheduleKind.Cron
            ? Schedule.Cron(reader.GetString(first + 4), reader.GetString(first + 5))
            : (Interval)reader.GetInt16(first + 2) == Interval.FixedRate
                ? Schedule.Every(TimeSpan.FromTicks(reader.GetInt64(first + 1)), Interval.FixedRate, Tsql.Time(reader, first + 3))
                : Schedule.Every(TimeSpan.FromTicks(reader.GetInt64(first + 1)), Interval.AfterCompletion);

    /// <summary>The occurrence identity of a scheduled instant: ISO 8601 in UTC.</summary>
    private static string OccurrenceOf(DateTimeOffset instant) => instant.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'", CultureInfo.InvariantCulture);
}
