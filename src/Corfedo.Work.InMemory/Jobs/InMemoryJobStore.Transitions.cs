using Corfedo.Work.InMemory.Database;
using static Corfedo.Work.InMemory.Leases.InMemoryLeaseStore;

namespace Corfedo.Work.InMemory.Jobs;

internal sealed partial class InMemoryJobStore
{
    public async ValueTask CompleteAsync(IWorkTransaction transaction, Lease<JobId> lease, JobTransition transition, JobEndedCallback? ended, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(transition);
        if (!await CompleteAsync(Join(transaction), lease, transition, ended, cancellationToken))
        {
            throw new LeaseLostException($"The lease on {lease.Source.Name} record {lease.Key} at epoch {lease.Epoch} is no longer valid.");
        }
    }

    public async ValueTask<bool> CompleteAsync(Lease<JobId> lease, JobTransition transition, JobEndedCallback? ended, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(transition);
        using var transaction = await database.BeginAsync(cancellationToken);
        if (!await CompleteAsync(transaction, lease, transition, ended, cancellationToken))
        {
            return false;
        }
        transaction.Commit();
        return true;
    }

    /// <summary>
    /// Reads the job's attempts, its cancellation request and the stored policy, with the lease's validity. A retry, or a
    /// timeout the policy retries, gives the job back due after the longer of the backoff for the attempts so far and
    /// the handler's delay, a job with a calendar at the next allowed instant from then, without using an attempt; with
    /// no attempt left the job ends failed with <c>attempts_exhausted</c>, or timed out. A job with a cancellation
    /// request ends cancelled instead of retrying. Every other transition is terminal.
    /// </summary>
    private async Task<bool> CompleteAsync(InMemoryTransaction work, Lease<JobId> lease, JobTransition transition, JobEndedCallback? ended, CancellationToken cancellationToken)
    {
        var source = lease.Source;
        var rules = Rules(source);
        var now = Now;
        bool Valid(Record r) => r.Outstanding() && rules.Valid(r, lease.Owner, lease.Epoch, Now, 0);
        if (rules.Find(work, Key(lease.Key.Value)) is not { } r || !Valid(r) || Stored(work, rules.Job!.Definition) is not { } stored)
        {
            return false;
        }
        var policy = stored.Policy;
        var attempts = r.Int32(JobColumns.Attempts);
        var cancelRequested = r.Time(LeaseFields.Default.CancelRequestedAt) is not null;
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
            return await EndJobAsync(work, source, lease.Key, terminal, decision.Reason, discardPending: false, Valid, ended, cancellationToken, (JobRunOutcome)terminal, result);
        }
        var backoff = policy.Backoff.Delay(Math.Max(attempts - 1, 0));
        var retryAt = now + (backoff > transition.After ? backoff : transition.After);
        JobRuns.Record(work, application, r, JobRunOutcome.Retried, transition.Code ?? JobReasons.HandlerFailed, now);
        var calendar = r.Text(JobColumns.Calendar) is not null;
        // A retry of a job with a calendar is planned at the next allowed instant from the end of its backoff.
        Write(work, r, [
            Set(LeaseFields.Default.LeaseExpiresAt, null),
            Set(LeaseFields.Default.Takeovers, 0),
            Set(LeaseFields.Default.Failures, r.Int32(LeaseFields.Default.Failures) + 1),
            Set(JobColumns.AttemptStartedAt, null),
            Set(LeaseFields.Default.DueAt, calendar ? null : retryAt),
            Set(JobColumns.CalendarAt, calendar ? retryAt : null),
            Set(JobColumns.WindowEnd, null),
            Set(JobColumns.CalendarBlocked, calendar ? (int)Blocked.MoveForward : null),
        ]);
        if (calendar)
        {
            NotifyPlan(work, source);
        }
        else
        {
            NotifyDue(work, source);
        }
        return true;
    }

    /// <summary>
    /// The one terminal path: if the outstanding job still meets <paramref name="condition"/>, it records the outcome
    /// with the ended execution and the result of <paramref name="completion"/>, ends an open attempt in the run history
    /// as <paramref name="attempt"/> (a lost lease unless a holder ends it), and gives up its lease, then runs the outcome
    /// callback in the same transaction. When no execution of the job is left outstanding and a caller waits for it, it
    /// clears the mark and signals waiting callers on commit. A standalone job with a pending follow-up, unless
    /// <paramref name="discardPending"/>, becomes outstanding again with the same id, the pending arguments and due time,
    /// a new execution id, fresh attempts and not started; a follow-up with a calendar gets its pending due time as its
    /// calendar instant. An ended job keeps the name of its calendar in <c>ended_calendar</c>. An occurrence keeps its
    /// completion, unconsumed until the planner consumed it, unless the overlap policy recorded it to be skipped. The
    /// callback sees the ended execution. For a definition with expectations, the end is recorded against them after the
    /// job's update. A chain step then decides its children and counts itself settled in its group, after the callback.
    /// False when the condition no longer holds.
    /// </summary>
    private async Task<bool> EndJobAsync(
        InMemoryTransaction work,
        LeaseSource<JobId> source,
        JobId job,
        JobOutcome outcome,
        string? reason,
        bool discardPending,
        Func<Record, bool> condition,
        JobEndedCallback? ended,
        CancellationToken cancellationToken,
        JobRunOutcome attempt = JobRunOutcome.LeaseLost,
        JobTransition? completion = null)
    {
        var rules = Rules(source);
        if (rules.Find(work, Key(job.Value)) is not { } r || !r.Outstanding() || !condition(r))
        {
            return false;
        }
        var now = Now;
        var restart = !discardPending && r.Bytes(JobColumns.PendingArgs) is not null;
        var recurring = r.Uuid(JobColumns.RecurrenceId) is not null;
        var calendar = r.Text(JobColumns.Calendar);
        // An attempt its chain holds ends with the job's outcome, whose values the attempt outcomes share.
        JobRuns.Record(work, application, r, r.Uuid(JobColumns.OwnedChain) is not null ? (JobRunOutcome)outcome : attempt, reason, now);
        var updated = Write(work, r, [
            Set(JobColumns.Outcome, (int)outcome),
            Set(JobColumns.Reason, reason),
            Set(JobColumns.EndedAt, now),
            Set(LeaseFields.Default.LeaseExpiresAt, null),
            Set(LeaseFields.Default.CancelRequestedAt, null),
            Set(JobColumns.EndedExecutionId, r.Uuid(LeaseFields.Default.ExecutionId)),
            Set(JobColumns.Result, completion?.Result),
            Set(JobColumns.ResultVersion, completion?.Result is null ? null : completion.ResultVersion),
            Set(JobColumns.AttemptStartedAt, null),
            Set(JobColumns.Waited, restart && r.Bool(JobColumns.Waited)),
            Set(LeaseFields.Default.Takeovers, 0),
            Set(LeaseFields.Default.Failures, 0),
            Set(JobColumns.State, restart ? 0 : (int)outcome),
            Set(LeaseFields.Default.DueAt, restart && calendar is null ? r.Time(JobColumns.PendingDue) : null),
            Set(JobColumns.CalendarAt, restart && calendar is not null ? r.Time(JobColumns.PendingDue) : null),
            Set(JobColumns.WindowEnd, null),
            Set(JobColumns.CalendarUnavailable, false),
            Set(JobColumns.ChainWaiting, false),
            Set(JobColumns.ChainOutcome, null),
            Set(JobColumns.Calendar, restart ? calendar : null),
            Set(JobColumns.EndedCalendar, restart ? null : calendar),
            Set(JobColumns.Unconsumed, recurring && !(r.Bool(JobColumns.SkipPending) && r.Text(JobColumns.SkipReason) is null)),
            Set(JobColumns.SkipPending, false),
            Set(JobColumns.SkipReason, null),
            Set(JobColumns.Args, restart ? r.Bytes(JobColumns.PendingArgs) : r.Bytes(JobColumns.Args)),
            Set(JobColumns.ArgsVersion, restart ? r.Int32(JobColumns.PendingArgsVersion) : r.Int32(JobColumns.ArgsVersion)),
            Set(JobColumns.FirstRequestedAt, restart ? r.Time(JobColumns.PendingFirstRequestedAt) : r.Time(JobColumns.FirstRequestedAt)),
            Set(LeaseFields.Default.ExecutionId, restart ? Guid.NewGuid() : r.Uuid(LeaseFields.Default.ExecutionId)),
            Set(JobColumns.Attempts, restart ? 0 : r.Int32(JobColumns.Attempts)),
            Set(JobColumns.Started, !restart && r.Bool(JobColumns.Started)),
            Set(JobColumns.PendingArgs, null),
            Set(JobColumns.PendingArgsVersion, null),
            Set(JobColumns.PendingDue, null),
            Set(JobColumns.PendingFirstRequestedAt, null),
        ]);
        var partition = r.Text(JobColumns.Partition) ?? "";
        var end = new JobEnd(
            rules.Job!.Definition,
            job,
            r.Subject(),
            partition.Length == 0 ? null : partition,
            r.Uuid(LeaseFields.Default.ExecutionId)!.Value,
            outcome,
            reason,
            r.Int32(JobColumns.Attempts),
            r.Text(JobColumns.Occurrence));
        if (Stored(work, rules.Job.Definition)?.Policy.Expect is { IsEmpty: false } expectation)
        {
            RecordEnd(work, end, expectation, now);
        }
        if (ended is not null)
        {
            await ended(end, work, cancellationToken);
        }
        if (r.Uuid(JobColumns.ChainId) is { } chain)
        {
            SettleStep(work, chain, job.Value, r.Uuid(JobColumns.ChainGroup), r.Uuid(JobColumns.ChainParent) is null, outcome);
        }
        // A restarted job is due; an ended job frees a slot of the definition's limits.
        if (restart || source.Policy.HasStoreLimits)
        {
            NotifyDue(work, source);
        }
        // An ended occurrence, and a follow-up with a calendar, wait for the planner.
        if (recurring || updated.Time(JobColumns.CalendarAt) is not null)
        {
            NotifyPlan(work, source);
        }
        // Only a job a caller waits for signals its end; a follow-up that reuses the job id keeps the mark until it ends.
        if (r.Bool(JobColumns.Waited) && !restart)
        {
            leases.Signal(work, WorkTopics.Ended, source.Name);
        }
        return true;
    }
}
