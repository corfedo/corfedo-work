using Corfedo.Work.PostgreSql.Leases;
using Npgsql;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Jobs;

internal sealed partial class PostgreSqlJobStore
{
    /// <summary>The SQLSTATE of a unique violation.</summary>
    private const string UniqueViolation = "23505";

    /// <summary>The SQLSTATE of a foreign key violation.</summary>
    private const string ForeignKeyViolation = "23503";

    /// <summary>
    /// Reads the job's recurrence without a lock; a chain step is refused. An occurrence of a stored recurrence takes the recurrence's lock and
    /// then the locks of its outstanding occurrences, the order of recurrence operations: an outstanding one is
    /// evaluated as a standalone job, one waiting to be recorded skipped, which those locks leave out, is reported
    /// waiting from a read after them, and an ended one triggers the recurrence. Any other job is locked alone.
    /// </summary>
    public async ValueTask<RunNowResult> RunNowAsync(LeaseSource<JobId> source, JobId job, bool ignoreCalendar, CancellationToken cancellationToken)
    {
        var sql = Sql(source);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        var work = new PostgreSqlTransaction(transaction);
        Guid? recurrenceId;
        await using (var read = new NpgsqlCommand($"SELECT r.recurrence_id, r.chain_id IS NOT NULL FROM {sql.Table} AS r WHERE r.id = $1 AND {sql.Scope}", connection, transaction))
        {
            read.Parameters.Add(Uuid(job.Value));
            await using var reader = await read.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return new(job, RunNowOutcome.NotFound);
            }
            if (reader.GetBoolean(1))
            {
                return new(job, RunNowOutcome.ChainStep);
            }
            recurrenceId = reader.IsDBNull(0) ? null : reader.GetGuid(0);
        }
        RunNowResult result;
        if (recurrenceId is { } id && await LockRecurrenceAsync(work, id, cancellationToken) is { } recurrence)
        {
            var jobs = await LockOccurrencesAsync(work, sql, recurrence.Id, cancellationToken);
            result = jobs.Any(j => j.Id == job.Value)
                ? await RunLockedNowAsync(work, source, job, ignoreCalendar, cancellationToken)
                : await OutstandingAsync(work, sql, job, cancellationToken)
                    ? new(job, RunNowOutcome.Waiting)
                    : await TriggerLockedAsync(work, source, recurrence, jobs, ignoreCalendar, cancellationToken);
        }
        else
        {
            result = await RunLockedNowAsync(work, source, job, ignoreCalendar, cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        return result;
    }

    /// <summary>
    /// Whether the job is outstanding, read without a lock: an occurrence outside its recurrence's locked ones is waiting
    /// to be recorded skipped, and a finalization that has not committed leaves it so.
    /// </summary>
    private static async Task<bool> OutstandingAsync(PostgreSqlTransaction work, LeaseSql sql, JobId job, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand($"SELECT 1 FROM {sql.Table} AS r WHERE r.id = $1 AND {sql.Scope} AND r.state = 0", work.Connection, work.Transaction);
        command.Parameters.Add(Uuid(job.Value));
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    /// <summary>
    /// Locks the job, waiting, and evaluates it with a clock reading taken after the lock is held: a terminal one is
    /// ended, a held one or an occurrence waiting for its chain running, one with an expired lease already due for its takeover, an occurrence to be recorded
    /// skipped waiting. Any other is made due now with its attempts kept, reported paused while its definition or
    /// subject is paused; one with a calendar is planned at the next allowed instant unless
    /// <paramref name="ignoreCalendar"/>.
    /// </summary>
    private async Task<RunNowResult> RunLockedNowAsync(PostgreSqlTransaction work, LeaseSource<JobId> source, JobId job, bool ignoreCalendar, CancellationToken cancellationToken)
    {
        var sql = Sql(source);
        await using (var batch = new NpgsqlBatch(work.Connection, work.Transaction))
        {
            batch.BatchCommands.Add(Command($"SELECT 1 FROM {sql.Table} AS r WHERE r.id = $1 AND {sql.Scope} FOR UPDATE OF r", Uuid(job.Value)));
            batch.BatchCommands.Add(Command(
                $"SELECT r.state <> 0, {Running}, r.lease_expires_at IS NOT NULL, r.skip_pending, "
                + $"r.due_at <= clock_timestamp(), {sql.SubjectPaused} OR (SELECT d.paused FROM {definitions} AS d WHERE d.key = r.definition) "
                + $"FROM {sql.Table} AS r WHERE r.id = $1 AND {sql.Scope}",
                Uuid(job.Value)));
            await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
            await reader.NextResultAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return new(job, RunNowOutcome.NotFound);
            }
            var outcome = reader.GetBoolean(0) ? RunNowOutcome.Ended
                : !reader.IsDBNull(1) && reader.GetBoolean(1) ? RunNowOutcome.AlreadyRunning
                : reader.GetBoolean(2) ? RunNowOutcome.AlreadyDue
                : reader.GetBoolean(3) ? RunNowOutcome.Waiting
                : reader.GetBoolean(5) ? RunNowOutcome.Paused
                : !reader.IsDBNull(4) && reader.GetBoolean(4) ? RunNowOutcome.AlreadyDue
                : RunNowOutcome.MadeDue;
            if (outcome is not (RunNowOutcome.MadeDue or RunNowOutcome.Paused))
            {
                return new(job, outcome);
            }
            await reader.CloseAsync();
            var planned = await MakeDueNowAsync(work, sql, job.Value, ignoreCalendar, cancellationToken);
            if (planned)
            {
                await NotifyPlanAsync(work, source, cancellationToken);
            }
            else if (outcome == RunNowOutcome.MadeDue)
            {
                await NotifyDueAsync(work, source, cancellationToken);
            }
            return new(job, outcome);
        }
    }

    /// <summary>
    /// Makes the locked job due now, or one with a calendar plannable from now at the next allowed instant unless
    /// <paramref name="ignoreCalendar"/>; true when its calendar plans it.
    /// </summary>
    private static async Task<bool> MakeDueNowAsync(PostgreSqlTransaction work, LeaseSql sql, Guid job, bool ignoreCalendar, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand($"UPDATE {sql.Table} AS r SET {DueNow("$2")} WHERE r.id = $1 RETURNING r.calendar_at IS NOT NULL", work.Connection, work.Transaction);
        command.Parameters.Add(Uuid(job));
        command.Parameters.Add(Bool(ignoreCalendar));
        return await command.ExecuteScalarAsync(cancellationToken) is true;
    }

    /// <summary>
    /// SET clauses that make job <c>r</c> due now, keeping an earlier due time, or for a job with the calendar
    /// <paramref name="calendar"/>, unless the boolean <paramref name="ignoreCalendar"/> holds, its calendar instant now,
    /// planned at the next allowed instant. Ignoring the calendar sets an endless window, which claims accept and a
    /// date-set publish does not plan again.
    /// </summary>
    private static string DueNow(string ignoreCalendar, string calendar = "r.calendar") =>
        $"due_at = CASE WHEN {calendar} IS NULL OR {ignoreCalendar} THEN LEAST(COALESCE(r.due_at, clock_timestamp()), clock_timestamp()) END, "
        + $"calendar_at = CASE WHEN {calendar} IS NOT NULL AND NOT {ignoreCalendar} THEN clock_timestamp() END, "
        + $"window_end = CASE WHEN {calendar} IS NOT NULL AND {ignoreCalendar} THEN 'infinity'::timestamptz END, calendar_unavailable = false, "
        + $"calendar_blocked = CASE WHEN {calendar} IS NOT NULL AND NOT {ignoreCalendar} THEN {(short)Blocked.MoveForward} ELSE r.calendar_blocked END";

    /// <summary>
    /// Locks the job, waiting, and evaluates it in a new statement: a chain step, a job with an outstanding execution, an occurrence,
    /// and one whose identity another outstanding standalone job holds are refused. A request that takes the identity
    /// concurrently makes the update fail on the outstanding-identity index, which is reported as held. The record
    /// keeps its lease epoch, so earlier leases stay invalid and run history entries distinct, and the ended
    /// execution's outcome and result. A job with a calendar gets it back and is planned at the next allowed instant
    /// unless <paramref name="ignoreCalendar"/>; the foreign key refuses it, reported as removed, when the calendar was
    /// removed, and holds a removal off until the rerun commits.
    /// </summary>
    public async ValueTask<RerunResult> RerunAsync(LeaseSource<JobId> source, JobId job, bool ignoreCalendar, CancellationToken cancellationToken)
    {
        var sql = Sql(source);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        RerunOutcome outcome;
        await using (var batch = new NpgsqlBatch(connection, transaction))
        {
            batch.BatchCommands.Add(Command($"SELECT 1 FROM {sql.Table} AS r WHERE r.id = $1 AND {sql.Scope} FOR UPDATE OF r", Uuid(job.Value)));
            batch.BatchCommands.Add(Command(
                "SELECT r.state = 0, r.recurrence_id IS NOT NULL, r.chain_id IS NOT NULL, "
                + $"EXISTS (SELECT 1 FROM {sql.Table} AS o WHERE o.definition = r.definition AND o.merge_key = r.merge_key AND o.state = 0) "
                + $"FROM {sql.Table} AS r WHERE r.id = $1 AND {sql.Scope}",
                Uuid(job.Value)));
            await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
            await reader.NextResultAsync(cancellationToken);
            outcome = !await reader.ReadAsync(cancellationToken) ? RerunOutcome.NotFound
                : reader.GetBoolean(2) ? RerunOutcome.ChainStep
                : reader.GetBoolean(0) ? RerunOutcome.Outstanding
                : reader.GetBoolean(1) ? RerunOutcome.Recurring
                : reader.GetBoolean(3) ? RerunOutcome.IdentityHeld
                : RerunOutcome.Rerun;
        }
        if (outcome != RerunOutcome.Rerun)
        {
            return new(job, outcome);
        }
        bool planned;
        await using (var rerun = new NpgsqlCommand(
            $"UPDATE {sql.Table} AS r SET state = 0, {DueNow("$2", "r.ended_calendar")}, calendar = r.ended_calendar, ended_calendar = NULL, first_requested_at = clock_timestamp(), execution_id = gen_random_uuid(), "
            + "attempts = 0, started = false, takeovers = 0, failures = 0, cancel_requested_at = NULL, progress_percent = NULL, progress_message = NULL, "
            + $"paused = {sql.SubjectPaused} WHERE r.id = $1 AND {sql.Scope} RETURNING r.calendar_at IS NOT NULL",
            connection,
            transaction))
        {
            rerun.Parameters.Add(Uuid(job.Value));
            rerun.Parameters.Add(Bool(ignoreCalendar));
            try
            {
                planned = await rerun.ExecuteScalarAsync(cancellationToken) is true;
            }
            catch (PostgresException exception) when (exception.SqlState == UniqueViolation)
            {
                return new(job, RerunOutcome.IdentityHeld);
            }
            catch (PostgresException exception) when (exception.SqlState == ForeignKeyViolation)
            {
                return new(job, RerunOutcome.CalendarRemoved);
            }
        }
        await (planned ? NotifyPlanAsync(new PostgreSqlTransaction(transaction), source, cancellationToken) : NotifyDueAsync(new PostgreSqlTransaction(transaction), source, cancellationToken));
        await transaction.CommitAsync(cancellationToken);
        return new(job, RerunOutcome.Rerun);
    }
}
