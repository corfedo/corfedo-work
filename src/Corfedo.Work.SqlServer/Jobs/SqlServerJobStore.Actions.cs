using Corfedo.Work.SqlServer.Leases;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Jobs;

internal sealed partial class SqlServerJobStore
{
    /// <summary>The error number of a foreign key violation.</summary>
    private const int ForeignKeyViolation = 547;

    /// <summary>
    /// Reads the job's recurrence without a lock; a chain step is refused. An occurrence of a stored recurrence takes the
    /// recurrence's lock and then the locks of its outstanding occurrences, the order of recurrence operations: an
    /// outstanding one is evaluated as a standalone job, one waiting to be recorded skipped, which those locks leave
    /// out, is reported waiting from a read after them, and an ended one triggers the recurrence. Any other job is
    /// locked alone.
    /// </summary>
    public async ValueTask<RunNowResult> RunNowAsync(LeaseSource<JobId> source, JobId job, bool ignoreCalendar, CancellationToken cancellationToken)
    {
        var sql = await SqlAsync(source, cancellationToken);
        return await InTransactionAsync(async work =>
        {
            Guid? recurrenceId;
            await using (var read = Tsql.Command(work,
                $"SELECT r.recurrence_id, {Tsql.Bit("r.chain_id IS NOT NULL")} FROM {sql.Table} AS r WHERE r.id = @job AND {sql.Scope};",
                Tsql.Guid("@job", job.Value)))
            await using (var reader = await read.ReaderAsync(cancellationToken))
            {
                if (!await reader.NextRowAsync(cancellationToken))
                {
                    return new RunNowResult(job, RunNowOutcome.NotFound);
                }
                if (reader.GetBoolean(1))
                {
                    return new RunNowResult(job, RunNowOutcome.ChainStep);
                }
                recurrenceId = reader.IsDBNull(0) ? null : reader.GetGuid(0);
            }
            if (recurrenceId is { } id && await LockRecurrenceAsync(work, id, cancellationToken) is { } recurrence)
            {
                var jobs = await LockOccurrencesAsync(work, sql, recurrence.Id, cancellationToken);
                return jobs.Any(j => j.Id == job.Value)
                    ? await RunLockedNowAsync(work, source, job, ignoreCalendar, cancellationToken)
                    : await OutstandingAsync(work, sql, job, cancellationToken)
                        ? new RunNowResult(job, RunNowOutcome.Waiting)
                        : await TriggerLockedAsync(work, source, recurrence, jobs, ignoreCalendar, cancellationToken);
            }
            return await RunLockedNowAsync(work, source, job, ignoreCalendar, cancellationToken);
        }, cancellationToken);
    }

    /// <summary>
    /// Whether the job is outstanding, read without a lock: an occurrence outside its recurrence's locked ones is waiting
    /// to be recorded skipped, and a finalization that has not committed leaves it so.
    /// </summary>
    private static async Task<bool> OutstandingAsync(SqlServerTransaction work, LeaseSql sql, JobId job, CancellationToken cancellationToken)
    {
        await using var command = Tsql.Command(work, $"SELECT 1 FROM {sql.Table} AS r WHERE r.id = @job AND {sql.Scope} AND r.state = 0;", Tsql.Guid("@job", job.Value));
        return await command.ScalarAsync(cancellationToken) is not null;
    }

    /// <summary>
    /// Locks the job, waiting, and evaluates it with a clock reading taken after the lock is held: a terminal one is
    /// ended, a held one or an occurrence waiting for its chain running, one with an expired lease already due for its
    /// takeover, an occurrence to be recorded skipped waiting. Any other is made due now with its attempts kept,
    /// reported paused while its definition or subject is paused; one with a calendar is planned at the next allowed
    /// instant unless <paramref name="ignoreCalendar"/>.
    /// </summary>
    private async Task<RunNowResult> RunLockedNowAsync(SqlServerTransaction work, LeaseSource<JobId> source, JobId job, bool ignoreCalendar, CancellationToken cancellationToken)
    {
        var sql = await SqlAsync(source, cancellationToken);
        RunNowOutcome outcome;
        await using (var command = Tsql.Command(work, $"""
            DECLARE @held int;
            SELECT @held = 1 FROM {sql.Table} AS r WITH (UPDLOCK, ROWLOCK) WHERE r.id = @job AND {sql.Scope};
            SELECT {Tsql.Bit("r.state <> 0")}, {Tsql.Bit(Running)}, {Tsql.Bit("r.lease_expires_at IS NOT NULL")}, r.skip_pending,
                {Tsql.Bit("r.due_at <= SYSUTCDATETIME()")}, {Tsql.Bit($"{sql.SubjectPaused} OR EXISTS (SELECT 1 FROM {definitions} AS d WHERE d.[key] = r.[definition] AND d.paused = 1)")}
            FROM {sql.Table} AS r WHERE r.id = @job AND {sql.Scope};
            """, Tsql.Guid("@job", job.Value)))
        await using (var reader = await command.ReaderAsync(cancellationToken))
        {
            if (!await reader.NextRowAsync(cancellationToken))
            {
                return new(job, RunNowOutcome.NotFound);
            }
            outcome = reader.GetBoolean(0) ? RunNowOutcome.Ended
                : reader.GetBoolean(1) ? RunNowOutcome.AlreadyRunning
                : reader.GetBoolean(2) ? RunNowOutcome.AlreadyDue
                : reader.GetBoolean(3) ? RunNowOutcome.Waiting
                : reader.GetBoolean(5) ? RunNowOutcome.Paused
                : reader.GetBoolean(4) ? RunNowOutcome.AlreadyDue
                : RunNowOutcome.MadeDue;
        }
        if (outcome is not (RunNowOutcome.MadeDue or RunNowOutcome.Paused))
        {
            return new(job, outcome);
        }
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

    /// <summary>
    /// Makes the locked job due now, or one with a calendar plannable from now at the next allowed instant unless
    /// <paramref name="ignoreCalendar"/>; true when its calendar plans it.
    /// </summary>
    private static async Task<bool> MakeDueNowAsync(SqlServerTransaction work, LeaseSql sql, Guid job, bool ignoreCalendar, CancellationToken cancellationToken)
    {
        await using var command = Tsql.Command(work, $"""
            UPDATE r SET {DueNow("@ignore")} OUTPUT {Tsql.Bit("inserted.calendar_at IS NOT NULL")} FROM {sql.Table} AS r WHERE r.id = @job;
            """,
            Tsql.Guid("@job", job),
            Tsql.Bool("@ignore", ignoreCalendar));
        return await command.ScalarAsync(cancellationToken) is true;
    }

    /// <summary>
    /// SET clauses that make job <c>r</c> due now, keeping an earlier due time, or for a job with the calendar
    /// <paramref name="calendar"/>, unless the bit <paramref name="ignoreCalendar"/> is set, its calendar instant now,
    /// planned at the next allowed instant. Ignoring the calendar sets an endless window, which claims accept and a
    /// date-set publish does not plan again.
    /// </summary>
    private static string DueNow(string ignoreCalendar, string calendar = "r.calendar") =>
        $"due_at = CASE WHEN {calendar} IS NULL OR {ignoreCalendar} = 1 THEN LEAST(COALESCE(r.due_at, SYSUTCDATETIME()), SYSUTCDATETIME()) END, "
        + $"calendar_at = CASE WHEN {calendar} IS NOT NULL AND {ignoreCalendar} = 0 THEN SYSUTCDATETIME() END, "
        + $"window_end = CASE WHEN {calendar} IS NOT NULL AND {ignoreCalendar} = 1 THEN {Tsql.NeverLiteral} END, calendar_unavailable = 0, "
        + $"calendar_blocked = CASE WHEN {calendar} IS NOT NULL AND {ignoreCalendar} = 0 THEN {(short)Blocked.MoveForward} ELSE r.calendar_blocked END";

    /// <summary>
    /// Locks the job, waiting, and evaluates it in a new statement: a chain step, a job with an outstanding execution, an
    /// occurrence, and one whose identity another outstanding standalone job holds are refused. A request that takes the
    /// identity concurrently makes the update fail on the outstanding-identity index, which is reported as held. The
    /// record keeps its lease epoch, so earlier leases stay invalid and run history entries distinct, and the ended
    /// execution's outcome and result. A job with a calendar gets it back and is planned at the next allowed instant
    /// unless <paramref name="ignoreCalendar"/>; the foreign key refuses it, reported as removed, when the calendar was
    /// removed, and holds a removal off until the rerun commits.
    /// </summary>
    public async ValueTask<RerunResult> RerunAsync(LeaseSource<JobId> source, JobId job, bool ignoreCalendar, CancellationToken cancellationToken)
    {
        var sql = await SqlAsync(source, cancellationToken);
        return await SqlServerLeaseStore.RetryDeadlocksAsync(async () =>
        {
            await using var connection = await leases.OpenAsync(cancellationToken);
            await using var transaction = await SqlServerLeaseStore.BeginAsync(connection, cancellationToken);
            var work = new SqlServerTransaction(transaction);
            RerunOutcome outcome;
            await using (var command = Tsql.Command(work, $"""
                DECLARE @held int;
                SELECT @held = 1 FROM {sql.Table} AS r WITH (UPDLOCK, ROWLOCK) WHERE r.id = @job AND {sql.Scope};
                SELECT {Tsql.Bit("r.state = 0")}, {Tsql.Bit("r.recurrence_id IS NOT NULL")}, {Tsql.Bit("r.chain_id IS NOT NULL")},
                    {Tsql.Bit($"EXISTS (SELECT 1 FROM {sql.Table} AS o WITH (INDEX(jobs_merge)) WHERE o.[definition] = r.[definition] AND o.merge_key IS NOT NULL AND o.merge_key = r.merge_key AND o.state = 0)")}
                FROM {sql.Table} AS r WHERE r.id = @job AND {sql.Scope};
                """, Tsql.Guid("@job", job.Value)))
            await using (var reader = await command.ReaderAsync(cancellationToken))
            {
                outcome = !await reader.NextRowAsync(cancellationToken) ? RerunOutcome.NotFound
                    : reader.GetBoolean(2) ? RerunOutcome.ChainStep
                    : reader.GetBoolean(0) ? RerunOutcome.Outstanding
                    : reader.GetBoolean(1) ? RerunOutcome.Recurring
                    : reader.GetBoolean(3) ? RerunOutcome.IdentityHeld
                    : RerunOutcome.Rerun;
            }
            if (outcome != RerunOutcome.Rerun)
            {
                return new RerunResult(job, outcome);
            }
            bool planned;
            await using (var rerun = Tsql.Command(work, $"""
                UPDATE r SET state = 0, {DueNow("@ignore", "r.ended_calendar")}, calendar = r.ended_calendar, ended_calendar = NULL, first_requested_at = SYSUTCDATETIME(),
                    execution_id = NEWID(), attempts = 0, started = 0, takeovers = 0, failures = 0, cancel_requested_at = NULL, progress_percent = NULL, progress_message = NULL,
                    paused = {Tsql.Bit(sql.SubjectPaused)}
                OUTPUT {Tsql.Bit("inserted.calendar_at IS NOT NULL")}
                FROM {sql.Table} AS r WHERE r.id = @job AND {sql.Scope};
                """,
                Tsql.Guid("@job", job.Value),
                Tsql.Bool("@ignore", ignoreCalendar)))
            {
                try
                {
                    planned = await rerun.ScalarAsync(cancellationToken) is true;
                }
                catch (SqlException exception) when (Tsql.IsUniqueViolation(exception))
                {
                    return new RerunResult(job, RerunOutcome.IdentityHeld);
                }
                catch (SqlException exception) when (Tsql.HasError(exception, ForeignKeyViolation))
                {
                    return new RerunResult(job, RerunOutcome.CalendarRemoved);
                }
            }
            await (planned ? NotifyPlanAsync(work, source, cancellationToken) : NotifyDueAsync(work, source, cancellationToken));
            await transaction.CommitAsync(cancellationToken);
            return new RerunResult(job, RerunOutcome.Rerun);
        });
    }
}
