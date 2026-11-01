using System.Globalization;
using Corfedo.Work.PostgreSql.Leases;
using Corfedo.Work.PostgreSql.Monitoring;
using Npgsql;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Jobs;

internal sealed partial class PostgreSqlJobStore
{
    public async ValueTask CompleteAsync(WorkTransaction transaction, Lease<JobId> lease, JobTransition transition, JobEndedCallback? ended, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(transition);
        if (!await CompleteAsync(PostgreSqlTransaction.From(transaction), lease, transition, ended, cancellationToken))
        {
            throw new LeaseLostException($"The lease on {lease.Source.Name} record {lease.Key} at epoch {lease.Epoch} is no longer valid.");
        }
    }

    public async ValueTask<bool> CompleteAsync(Lease<JobId> lease, JobTransition transition, JobEndedCallback? ended, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(transition);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        if (!await CompleteAsync(new PostgreSqlTransaction(transaction), lease, transition, ended, cancellationToken))
        {
            return false;
        }
        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    /// <summary>
    /// Locks the job, then reads its attempts, its cancellation request and the stored policy with the lease's validity
    /// evaluated by a clock reading taken after the lock is held. A retry, or a timeout the policy retries, gives the job
    /// back due after the longer of the backoff for the attempts so far and the handler's delay, a job with a calendar at
    /// the next allowed instant from then without using an attempt; with no attempt left
    /// the job ends failed with <c>attempts_exhausted</c>, or timed out. A job with a cancellation request ends
    /// cancelled instead of retrying. Every other transition is terminal.
    /// </summary>
    private async Task<bool> CompleteAsync(PostgreSqlTransaction work, Lease<JobId> lease, JobTransition transition, JobEndedCallback? ended, CancellationToken cancellationToken)
    {
        var source = lease.Source;
        var sql = Sql(source);
        int attempts;
        bool cancelRequested;
        bool calendar;
        JobPolicy policy;
        await using (var batch = new NpgsqlBatch(work.Connection, work.Transaction))
        {
            batch.BatchCommands.Add(KeyCommand($"SELECT 1 FROM {sql.Table} AS r WHERE {sql.KeyMatch(1)} FOR UPDATE OF r", source, lease.Key));
            batch.BatchCommands.Add(KeyCommand(
                $"SELECT r.attempts, r.cancel_requested_at IS NOT NULL, d.attempts, d.backoff_kind, d.backoff_initial, d.backoff_maximum, d.retry_timeouts, r.calendar IS NOT NULL "
                + $"FROM {sql.Table} AS r JOIN {definitions} AS d ON d.key = r.definition WHERE {sql.KeyMatch(1)} AND r.state = 0 AND {sql.Valid(2, 3)}",
                source,
                lease.Key,
                Uuid(lease.Owner.Value),
                Int64(lease.Epoch)));
            await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
            await reader.NextResultAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return false;
            }
            attempts = reader.GetInt32(0);
            cancelRequested = reader.GetBoolean(1);
            calendar = reader.GetBoolean(7);
            var initial = reader.GetFieldValue<TimeSpan>(4);
            policy = new JobPolicy
            {
                Attempts = reader.GetInt32(2),
                Backoff = (BackoffKind)reader.GetInt16(3) switch
                {
                    BackoffKind.None => Backoff.None,
                    BackoffKind.Constant => Backoff.Constant(initial),
                    _ => Backoff.Exponential(initial, reader.GetFieldValue<TimeSpan>(5)),
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
            return await EndJobAsync(work, source, lease.Key, terminal, decision.Reason, discardPending: false, $"{sql.Valid(4, 5)}", [Uuid(lease.Owner.Value), Int64(lease.Epoch)], ended, cancellationToken, (JobRunOutcome)terminal, result);
        }
        var delay = policy.Backoff.Delay(Math.Max(attempts - 1, 0));
        await using (var retry = new NpgsqlBatch(work.Connection, work.Transaction))
        {
            retry.BatchCommands.Add(KeyCommand(
                sql.RecordRun($"{sql.Table} AS r", $"{sql.KeyMatch(1)} AND {sql.Valid(2, 3)}", ((int)JobRunOutcome.Retried).ToString(CultureInfo.InvariantCulture), "$4", "clock_timestamp()"),
                source,
                lease.Key,
                Uuid(lease.Owner.Value),
                Int64(lease.Epoch),
                Text(transition.Code ?? JobReasons.HandlerFailed)));
            retry.BatchCommands.Add(KeyCommand(
                $"UPDATE {sql.Table} AS r SET lease_expires_at = NULL, takeovers = 0, failures = r.failures + 1, attempt_started_at = NULL, "
                // A retry of a job with a calendar is planned at the next allowed instant from the end of its backoff.
                + "due_at = CASE WHEN r.calendar IS NULL THEN clock_timestamp() + $4 END, calendar_at = CASE WHEN r.calendar IS NOT NULL THEN clock_timestamp() + $4 END, "
                + $"window_end = NULL, calendar_blocked = CASE WHEN r.calendar IS NOT NULL THEN {(short)Blocked.MoveForward} END "
                + $"WHERE {sql.KeyMatch(1)} AND {sql.Valid(2, 3)}",
                source,
                lease.Key,
                Uuid(lease.Owner.Value),
                Int64(lease.Epoch),
                Interval(delay > transition.After ? delay : transition.After)));
            await retry.ExecuteNonQueryAsync(cancellationToken);
            if (retry.BatchCommands[1].RecordsAffected != 1)
            {
                return false;
            }
        }
        await (calendar ? NotifyPlanAsync(work, source, cancellationToken) : NotifyDueAsync(work, source, cancellationToken));
        return true;
    }

    /// <summary>
    /// The one terminal path: if the locked job still meets <paramref name="condition"/> (parameters from <c>$4</c>), it
    /// records the outcome with the ended execution and the result of <paramref name="completion"/>, ends an open
    /// attempt in the run history as <paramref name="attempt"/> (a lost lease unless a holder ends it), and gives up
    /// its lease, then runs the outcome callback in the same transaction. When no execution of the job is left
    /// outstanding and a caller waits for it, it clears the mark and signals waiting callers on commit. A standalone job
    /// with a pending follow-up, unless <paramref name="discardPending"/>, becomes outstanding again with the same id,
    /// the pending arguments and due time, a new execution id, fresh attempts and not started; occurrences have no
    /// follow-ups; a follow-up with a calendar gets its pending due time as its calendar instant. An ended job keeps the
    /// name of its calendar without a reference, so only live work blocks the calendar's removal. An occurrence keeps its
    /// completion, unconsumed until the planner consumed it, unless the overlap policy recorded it to be skipped. The
    /// callback sees the ended execution. For a definition with expectations, the end is recorded against them after the
    /// job's update (see <see cref="PostgreSqlMonitoringStore.RecordEndAsync"/>). A chain step then decides its children
    /// and counts itself settled in its group, after the callback. False when the condition no longer holds.
    /// </summary>
    private async Task<bool> EndJobAsync(
        PostgreSqlTransaction work,
        LeaseSource<JobId> source,
        JobId job,
        JobOutcome outcome,
        string? reason,
        bool discardPending,
        string condition,
        NpgsqlParameter[] parameters,
        JobEndedCallback? ended,
        CancellationToken cancellationToken,
        JobRunOutcome attempt = JobRunOutcome.LeaseLost,
        JobTransition? completion = null)
    {
        var sql = Sql(source);
        var pending = discardPending ? "false" : "r.pending_args IS NOT NULL";
        var p = 4 + parameters.Length;
        string Next(string column, string value) => $"CASE WHEN old.restart THEN {value} ELSE r.{column} END";
        // The attempt is recorded from the row as locked, before the update below changes it, and only when the update
        // applies, since both read the same locked row. An attempt its chain holds ends with the job's outcome, whose
        // values the attempt outcomes share.
        var text = $"WITH old AS (SELECT r.id, r.execution_id, r.attempts, r.subject_kind, r.subject_id, r.partition_key, r.occurrence, r.waited, "
            + $"{pending} AS restart, r.recurrence_id IS NOT NULL AS recurring, {PostgreSqlMonitoringStore.HasExpectations} AS expects, d.expect_failures "
            + $"FROM {sql.Table} AS r JOIN {definitions} AS d ON d.key = r.definition WHERE {sql.KeyMatch(1)}), "
            + $"run AS ({sql.RecordRun($"{sql.Table} AS r JOIN old ON old.id = r.id", $"{sql.KeyMatch(1)} AND r.state = 0 AND {condition}", $"CASE WHEN r.owned_chain IS NOT NULL THEN $2 ELSE ${p} END", "$3", "clock_timestamp()", "old.occurrence")}) "
            + $"UPDATE {sql.Table} AS r SET outcome = $2, reason = $3, ended_at = clock_timestamp(), lease_expires_at = NULL, cancel_requested_at = NULL, "
            + $"ended_execution_id = old.execution_id, result = ${p + 1}, result_version = ${p + 2}, attempt_started_at = NULL, waited = old.restart AND r.waited, "
            + "takeovers = 0, failures = 0, "
            + "state = CASE WHEN old.restart THEN 0 ELSE $2 END, "
            + "due_at = CASE WHEN old.restart AND r.calendar IS NULL THEN r.pending_due END, "
            + "calendar_at = CASE WHEN old.restart AND r.calendar IS NOT NULL THEN r.pending_due END, "
            + "window_end = NULL, calendar_unavailable = false, chain_waiting = false, chain_outcome = NULL, "
            + "calendar = CASE WHEN old.restart THEN r.calendar END, ended_calendar = CASE WHEN NOT old.restart THEN r.calendar END, "
            + "unconsumed = old.recurring AND NOT (r.skip_pending AND r.skip_reason IS NULL), skip_pending = false, skip_reason = NULL, "
            + $"args = {Next("args", "r.pending_args")}, args_version = {Next("args_version", "r.pending_args_version")}, "
            + $"first_requested_at = {Next("first_requested_at", "r.pending_first_requested_at")}, "
            + $"execution_id = {Next("execution_id", "gen_random_uuid()")}, attempts = {Next("attempts", "0")}, started = {Next("started", "false")}, "
            + "pending_args = NULL, pending_args_version = NULL, pending_due = NULL, pending_first_requested_at = NULL "
            + $"FROM old WHERE r.id = old.id AND {sql.KeyMatch(1)} AND r.state = 0 AND {condition} "
            + "RETURNING old.execution_id, old.attempts, old.subject_kind, old.subject_id, old.partition_key, old.restart, old.occurrence, old.recurring, "
            + "old.waited AND NOT old.restart, r.calendar_at IS NOT NULL, r.chain_id, r.chain_parent IS NULL, r.chain_group, old.expects, old.expect_failures, r.ended_at";
        JobEnd end;
        bool startsFollowUp;
        bool occurrence;
        bool waited;
        bool plansCalendar;
        (Guid Id, bool Root, Guid? Group)? chain;
        (int? FailureLimit, DateTimeOffset EndedAt)? expects;
        await using (var command = new NpgsqlCommand(text, work.Connection, work.Transaction))
        {
            AddKey(command, source, job);
            command.Parameters.Add(new NpgsqlParameter<short> { TypedValue = (short)outcome });
            command.Parameters.Add(new NpgsqlParameter { Value = reason ?? (object)DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            command.Parameters.AddRange(parameters);
            command.Parameters.Add(new NpgsqlParameter<short> { TypedValue = (short)attempt });
            command.Parameters.Add(new NpgsqlParameter { Value = completion?.Result ?? (object)DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bytea });
            command.Parameters.Add(new NpgsqlParameter { Value = completion?.Result is null ? DBNull.Value : completion.ResultVersion, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
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
            expects = reader.GetBoolean(13) ? (reader.IsDBNull(14) ? null : reader.GetInt32(14), reader.GetFieldValue<DateTimeOffset>(15)) : null;
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
        // Only a job a caller waits for signals its end: any notification serializes the commits of notifying
        // transactions. A follow-up that reuses the job id keeps the mark until it ends.
        if (waited)
        {
            await leases.NotifyAsync(work.Connection, work.Transaction, WorkTopics.Ended, source.Name, cancellationToken);
        }
        return true;
    }
}
