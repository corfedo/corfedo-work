using System.Globalization;
using Corfedo.Work.SqlServer.Leases;
using Corfedo.Work.SqlServer.Monitoring;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Jobs;

internal sealed partial class SqlServerJobStore
{
    public async ValueTask CompleteAsync(WorkTransaction transaction, Lease<JobId> lease, JobTransition transition, JobEndedCallback? ended, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(transition);
        if (!await CompleteAsync(SqlServerTransaction.From(transaction), lease, transition, ended, cancellationToken))
        {
            throw new LeaseLostException($"The lease on {lease.Source.Name} record {lease.Key} at epoch {lease.Epoch} is no longer valid.");
        }
    }

    public async ValueTask<bool> CompleteAsync(Lease<JobId> lease, JobTransition transition, JobEndedCallback? ended, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(transition);
        return await SqlServerLeaseStore.RetryDeadlocksAsync(async () =>
        {
            await using var connection = await leases.OpenAsync(cancellationToken);
            await using var transaction = await SqlServerLeaseStore.BeginAsync(connection, cancellationToken);
            if (!await CompleteAsync(new SqlServerTransaction(transaction), lease, transition, ended, cancellationToken))
            {
                return false;
            }
            await transaction.CommitAsync(cancellationToken);
            return true;
        });
    }

    /// <summary>
    /// Locks the job, then reads its attempts, its cancellation request and the stored policy with the lease's validity
    /// evaluated by a clock reading taken after the lock is held. A retry, or a timeout the policy retries, gives the job
    /// back due after the longer of the backoff for the attempts so far and the handler's delay, a job with a calendar at
    /// the next allowed instant from then without using an attempt; with no attempt left the job ends failed with
    /// <c>attempts_exhausted</c>, or timed out. A job with a cancellation request ends cancelled instead of retrying.
    /// Every other transition is terminal.
    /// </summary>
    private async Task<bool> CompleteAsync(SqlServerTransaction work, Lease<JobId> lease, JobTransition transition, JobEndedCallback? ended, CancellationToken cancellationToken)
    {
        var source = lease.Source;
        var sql = await SqlAsync(source, cancellationToken);
        int attempts;
        bool cancelRequested;
        bool calendar;
        JobPolicy policy;
        await using (var read = SqlServerLeaseStore.KeyCommand(work.Connection, work.Transaction, sql, source, lease.Key, $"""
            DECLARE @held int;
            SELECT @held = 1 FROM {sql.Table} AS r WITH (UPDLOCK, ROWLOCK) WHERE {sql.KeyMatch};
            SELECT r.attempts, {Tsql.Bit("r.cancel_requested_at IS NOT NULL")}, d.attempts, d.backoff_kind, d.backoff_initial, d.backoff_maximum, d.retry_timeouts,
                {Tsql.Bit("r.calendar IS NOT NULL")}
            FROM {sql.Table} AS r JOIN {definitions} AS d ON d.[key] = r.[definition] WHERE {sql.KeyMatch} AND r.state = 0 AND {sql.Valid};
            """,
            Tsql.Guid("@owner", lease.Owner.Value),
            Tsql.Int64("@epoch", lease.Epoch)))
        await using (var reader = await read.ExecuteReaderAsync(cancellationToken))
        {
            if (!await reader.ReadAsync(cancellationToken))
            {
                return false;
            }
            attempts = reader.GetInt32(0);
            cancelRequested = reader.GetBoolean(1);
            calendar = reader.GetBoolean(7);
            var initial = TimeSpan.FromTicks(reader.GetInt64(4));
            policy = new JobPolicy
            {
                Attempts = reader.GetInt32(2),
                Backoff = (BackoffKind)reader.GetInt16(3) switch
                {
                    BackoffKind.None => Backoff.None,
                    BackoffKind.Constant => Backoff.Constant(initial),
                    _ => Backoff.Exponential(initial, TimeSpan.FromTicks(reader.GetInt64(5))),
                },
                RetryTimeouts = reader.GetBoolean(6),
            };
        }
        var retries = transition.Kind == JobTransitionKind.Retry || (transition.Kind == JobTransitionKind.TimeOut && policy.RetryTimeouts);
        (JobOutcome? Outcome, string? Reason) decision = transition.Kind switch
        {
            _ when retries && cancelRequested => (JobOutcome.Cancelled, JobReasons.Cancelled),
            JobTransitionKind.Retry when attempts >= policy.Attempts => (JobOutcome.Failed, JobReasons.AttemptsExhausted),
            JobTransitionKind.Retry => (null, null),
            JobTransitionKind.TimeOut when retries && attempts < policy.Attempts => (null, null),
            JobTransitionKind.TimeOut => (JobOutcome.TimedOut, transition.Code),
            JobTransitionKind.Complete => (JobOutcome.Completed, null),
            JobTransitionKind.Cancel => (JobOutcome.Cancelled, transition.Code),
            JobTransitionKind.Skip => (JobOutcome.Skipped, transition.Code),
            _ => (JobOutcome.Failed, transition.Code),
        };
        if (decision.Outcome is { } terminal)
        {
            var result = terminal == JobOutcome.Completed ? transition : null;
            return await EndJobAsync(work, source, lease.Key, terminal, decision.Reason, discardPending: false, sql.Valid,
                [Tsql.Guid("@owner", lease.Owner.Value), Tsql.Int64("@epoch", lease.Epoch)], ended, cancellationToken, (JobRunOutcome)terminal, result);
        }
        var delay = policy.Backoff.Delay(Math.Max(attempts - 1, 0));
        var after = Tsql.AddTicks("SYSUTCDATETIME()", "@delay");
        // A retry of a job with a calendar is planned at the next allowed instant from the end of its backoff.
        await using (var retry = SqlServerLeaseStore.KeyCommand(work.Connection, work.Transaction, sql, source, lease.Key, $"""
            DECLARE @valid TABLE (id uniqueidentifier NOT NULL PRIMARY KEY);
            INSERT INTO @valid (id) SELECT r.id FROM {sql.Table} AS r WHERE {sql.KeyMatch} AND {sql.Valid};
            {sql.RecordRun($"{sql.Table} AS r JOIN @valid AS v ON v.id = r.id", "1 = 1", ((int)JobRunOutcome.Retried).ToString(CultureInfo.InvariantCulture), "@reason", "SYSUTCDATETIME()")}
            UPDATE r SET lease_expires_at = NULL, takeovers = 0, failures = r.failures + 1, attempt_started_at = NULL,
                due_at = CASE WHEN r.calendar IS NULL THEN {after} END, calendar_at = CASE WHEN r.calendar IS NOT NULL THEN {after} END,
                window_end = NULL, calendar_blocked = CASE WHEN r.calendar IS NOT NULL THEN {(short)Blocked.MoveForward} END
            FROM {sql.Table} AS r JOIN @valid AS v ON v.id = r.id;
            SELECT @@ROWCOUNT;
            """,
            Tsql.Guid("@owner", lease.Owner.Value),
            Tsql.Int64("@epoch", lease.Epoch),
            Tsql.Text("@reason", transition.Code ?? JobReasons.HandlerFailed, -1),
            Tsql.Ticks("@delay", delay > transition.After ? delay : transition.After)))
        {
            if (Convert.ToInt32(await retry.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture) != 1)
            {
                return false;
            }
        }
        await (calendar ? NotifyPlanAsync(work, source, cancellationToken) : NotifyDueAsync(work, source, cancellationToken));
        return true;
    }

    /// <summary>
    /// The one terminal path: it locks the job and, if it still meets <paramref name="condition"/> (with
    /// <paramref name="parameters"/>) by a clock reading taken after the lock is held, records the outcome with the ended
    /// execution and the result of <paramref name="completion"/>, ends an open attempt in the run history as
    /// <paramref name="attempt"/> (a lost lease unless a holder ends it), and gives up its lease, then runs the outcome
    /// callback in the same transaction. When no execution of the job is left outstanding and a caller waits for it, it
    /// clears the mark and signals waiting callers on commit. A standalone job with a pending follow-up, unless
    /// <paramref name="discardPending"/>, becomes outstanding again with the same id, the pending arguments and due
    /// time, a new execution id, fresh attempts and not started; occurrences have no follow-ups; a follow-up with a
    /// calendar gets its pending due time as its calendar instant. An ended job keeps the name of its calendar without
    /// a reference, so only live work blocks the calendar's removal. An occurrence keeps its completion, unconsumed until
    /// the planner consumed it, unless the overlap policy recorded it to be skipped. The callback sees the ended
    /// execution. For a definition with expectations, the end is recorded against them after the job's update (see
    /// <see cref="SqlServerMonitoringStore.RecordEndAsync"/>). A chain step then decides its children and counts itself
    /// settled in its group, after the callback. False when the condition no longer holds.
    /// </summary>
    private async Task<bool> EndJobAsync(
        SqlServerTransaction work,
        LeaseSource<JobId> source,
        JobId job,
        JobOutcome outcome,
        string? reason,
        bool discardPending,
        string condition,
        SqlParameter[] parameters,
        JobEndedCallback? ended,
        CancellationToken cancellationToken,
        JobRunOutcome attempt = JobRunOutcome.LeaseLost,
        JobTransition? completion = null)
    {
        var sql = await SqlAsync(source, cancellationToken);
        var pending = discardPending ? "0" : "CASE WHEN r.pending_args IS NOT NULL THEN 1 ELSE 0 END";
        string Next(string column, string value) => $"CASE WHEN old.restart = 1 THEN {value} ELSE r.{column} END";
        // The condition is evaluated once, after the lock, into @old; the attempt is recorded from the row as locked,
        // before the update changes it. An attempt its chain holds ends with the job's outcome, whose values the
        // attempt outcomes share.
        var text = $"""
            DECLARE @held int;
            SELECT @held = 1 FROM {sql.Table} AS r WITH (UPDLOCK, ROWLOCK) WHERE {sql.KeyMatch};
            DECLARE @old TABLE (id uniqueidentifier NOT NULL PRIMARY KEY, execution_id uniqueidentifier NOT NULL, attempts int NOT NULL,
                subject_kind nvarchar(100) COLLATE {Tsql.Binary}, subject_id nvarchar(400) COLLATE {Tsql.Binary}, partition_key nvarchar(400) COLLATE {Tsql.Binary} NOT NULL, occurrence nvarchar(200) COLLATE {Tsql.Binary},
                waited bit NOT NULL, restart bit NOT NULL, recurring bit NOT NULL, expects bit NOT NULL, expect_failures int);
            INSERT INTO @old SELECT r.id, r.execution_id, r.attempts, r.subject_kind, r.subject_id, r.partition_key, r.occurrence, r.waited,
                {pending}, {Tsql.Bit("r.recurrence_id IS NOT NULL")}, {Tsql.Bit(SqlServerMonitoringStore.HasExpectations)}, d.expect_failures
            FROM {sql.Table} AS r JOIN {definitions} AS d ON d.[key] = r.[definition] WHERE {sql.KeyMatch} AND r.state = 0 AND {condition};
            {sql.RecordRun($"{sql.Table} AS r JOIN @old AS old ON old.id = r.id", "1 = 1", "CASE WHEN r.owned_chain IS NOT NULL THEN @outcome ELSE @attempt END", "@reason", "SYSUTCDATETIME()", "old.occurrence")}
            DECLARE @ended TABLE (planned bit NOT NULL, chain_id uniqueidentifier, chain_root bit NOT NULL, chain_group uniqueidentifier, ended_at datetime2(7) NOT NULL);
            UPDATE r SET outcome = @outcome, reason = @reason, ended_at = SYSUTCDATETIME(), lease_expires_at = NULL, cancel_requested_at = NULL,
                ended_execution_id = old.execution_id, result = @result, result_version = @result_version, attempt_started_at = NULL,
                waited = CASE WHEN old.restart = 1 AND r.waited = 1 THEN 1 ELSE 0 END, takeovers = 0, failures = 0,
                state = CASE WHEN old.restart = 1 THEN 0 ELSE @outcome END,
                due_at = CASE WHEN old.restart = 1 AND r.calendar IS NULL THEN r.pending_due END,
                calendar_at = CASE WHEN old.restart = 1 AND r.calendar IS NOT NULL THEN r.pending_due END,
                window_end = NULL, calendar_unavailable = 0, chain_waiting = 0, chain_outcome = NULL,
                calendar = CASE WHEN old.restart = 1 THEN r.calendar END, ended_calendar = CASE WHEN old.restart = 0 THEN r.calendar END,
                unconsumed = CASE WHEN old.recurring = 1 AND NOT (r.skip_pending = 1 AND r.skip_reason IS NULL) THEN 1 ELSE 0 END, skip_pending = 0, skip_reason = NULL,
                args = {Next("args", "r.pending_args")}, args_version = {Next("args_version", "r.pending_args_version")},
                first_requested_at = {Next("first_requested_at", "r.pending_first_requested_at")},
                execution_id = {Next("execution_id", "NEWID()")}, attempts = {Next("attempts", "0")}, started = {Next("started", "0")},
                pending_args = NULL, pending_args_version = NULL, pending_due = NULL, pending_first_requested_at = NULL
            OUTPUT {Tsql.Bit("inserted.calendar_at IS NOT NULL")}, inserted.chain_id, {Tsql.Bit("inserted.chain_parent IS NULL")}, inserted.chain_group, inserted.ended_at INTO @ended
            FROM {sql.Table} AS r JOIN @old AS old ON old.id = r.id;
            SELECT old.execution_id, old.attempts, old.subject_kind, old.subject_id, old.partition_key, old.restart, old.occurrence, old.recurring,
                {Tsql.Bit("old.waited = 1 AND old.restart = 0")}, e.planned, e.chain_id, e.chain_root, e.chain_group, old.expects, old.expect_failures, e.ended_at
            FROM @old AS old CROSS JOIN @ended AS e;
            """;
        JobEnd end;
        bool startsFollowUp;
        bool occurrence;
        bool waited;
        bool plansCalendar;
        (Guid Id, bool Root, Guid? Group)? chain;
        (int? FailureLimit, DateTimeOffset EndedAt)? expects;
        await using (var command = SqlServerLeaseStore.KeyCommand(work.Connection, work.Transaction, sql, source, job, text,
            [
                Tsql.Int16("@outcome", (short)outcome),
                Tsql.Text("@reason", reason, -1),
                Tsql.Int16("@attempt", (short)attempt),
                Tsql.Bytes("@result", completion?.Result),
                Tsql.Int32("@result_version", completion?.Result is null ? null : completion.ResultVersion),
                .. parameters,
            ]))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            if (!await reader.ReadAsync(cancellationToken))
            {
                return false;
            }
            var partition = reader.GetString(4);
            end = new JobEnd(
                sql.Job!.Definition,
                job,
                reader.IsDBNull(2) ? null : Subject.Of(reader.GetString(2), reader.GetString(3)),
                partition.Length == 0 ? null : partition,
                reader.GetGuid(0),
                outcome,
                reason,
                reader.GetInt32(1),
                reader.IsDBNull(6) ? null : reader.GetString(6));
            startsFollowUp = reader.GetBoolean(5);
            occurrence = reader.GetBoolean(7);
            waited = reader.GetBoolean(8);
            plansCalendar = reader.GetBoolean(9);
            chain = reader.IsDBNull(10) ? null : (reader.GetGuid(10), reader.GetBoolean(11), reader.IsDBNull(12) ? null : reader.GetGuid(12));
            expects = reader.GetBoolean(13) ? (reader.IsDBNull(14) ? null : reader.GetInt32(14), Tsql.Time(reader, 15)) : null;
        }
        if (expects is { } expectation)
        {
            await monitoring.RecordEndAsync(work, end.Definition, job, end.Subject, outcome, expectation.EndedAt, expectation.FailureLimit, cancellationToken);
        }
        if (ended is not null)
        {
            await ended(end, work.Work, cancellationToken);
        }
        if (chain is { } step)
        {
            await SettleStepAsync(work, step.Id, job.Value, step.Group, step.Root, outcome, cancellationToken);
        }
        // A restarted row is due; an ended job frees a slot of the definition's limits; an ended occurrence waits for
        // the planner.
        if (startsFollowUp || source.Policy.HasStoreLimits)
        {
            await NotifyDueAsync(work, source, cancellationToken);
        }
        // An ended occurrence, and a follow-up with a calendar, wait for the planner.
        if (occurrence || plansCalendar)
        {
            await NotifyPlanAsync(work, source, cancellationToken);
        }
        // Only a job a caller waits for signals its end. A follow-up that reuses the job id keeps the mark until it ends.
        if (waited)
        {
            await leases.NotifyAsync(work.Connection, work.Transaction, WorkTopics.Ended, source.Name, cancellationToken);
        }
        return true;
    }
}
