using System.Collections.Concurrent;

namespace Corfedo.Work.Conformance;

/// <summary>Cancellation, completion with domain output and outcome hooks.</summary>
public abstract class JobOutcomeScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    [Fact]
    public async Task J17_AScheduledJobCancelledInATransactionNeverRuns()
    {
        var definition = Definition(subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var journal = await Provider.CreateTableAsync([""], Cancel);
        var ended = new EndedScript { Ended = (end, transaction, cancellationToken) => new ValueTask(Provider.JournalAsync(transaction, journal, end.Outcome.ToString(), cancellationToken)) };
        var client = await ClientAsync(ended, definition);
        var job = await RequestAsync(client, definition, "x", Schedule.After(TimeSpan.FromSeconds(1)), For(subject));
        await using (var rolledBack = await BeginAsync())
        {
            Assert.Equal(1, await Jobs(client).CancelAsync(rolledBack.Work, definition, subject, Cancel));
            await rolledBack.RollbackAsync(Cancel);
        }
        Assert.Null((await ReadJobAsync(job.Id)).State);
        Assert.Empty(await Provider.ReadJournalAsync(journal, Cancel));
        await using (var transaction = await BeginAsync())
        {
            Assert.Equal(1, await Jobs(client).CancelAsync(transaction.Work, definition, subject, Cancel));
            await transaction.CommitAsync(Cancel);
        }
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        await WorkerAsync(definition, Recording(attempts));

        await Task.Delay(2500, Cancel);
        Assert.Empty(attempts);
        var state = await ReadJobAsync(job.Id);
        Assert.Equal((JobOutcome.Cancelled, JobReasons.Cancelled), (state.State, state.Reason));
        Assert.Equal(["Cancelled"], await Provider.ReadJournalAsync(journal, Cancel));
    }

    [Fact]
    public async Task J17_ACancellationRequestOutlivesACrashedHolder()
    {
        var definition = Definition(new JobPolicy { Backoff = Backoff.None }, subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var ended = new EndedScript();
        var client = await ClientAsync(ended, definition);
        var job = await RequestAsync(client, definition, "x", options: For(subject));
        await CrashClaimAsync(client, definition, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5));
        await using (var transaction = await BeginAsync())
        {
            Assert.Equal(1, await Jobs(client).CancelAsync(transaction.Work, definition, subject, Cancel));
            await transaction.CommitAsync(Cancel);
        }
        Assert.True((await ReadJobAsync(job.Id)) is { Held: true, CancelRequested: true, State: null });
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        await WorkerAsync(definition, Recording(attempts), ended);

        var state = await EndedAsync(job.Id, TimeSpan.FromSeconds(10));
        Assert.Equal((JobOutcome.Cancelled, JobReasons.Cancelled, 1), (state.State, state.Reason, state.Attempts));
        Assert.Empty(attempts);
        Assert.Equal(JobOutcome.Cancelled, Assert.Single(ended.Calls).Outcome);
    }

    [Fact]
    public async Task J17_ACancellationRequestTakesPrecedenceOverExhaustionAfterACrash()
    {
        var definition = Definition(new JobPolicy { Attempts = 1, Backoff = Backoff.None }, subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var journal = await Provider.CreateTableAsync([""], Cancel);
        var ended = new EndedScript { Ended = (end, transaction, cancellationToken) => new ValueTask(Provider.JournalAsync(transaction, journal, $"{end.Outcome}:{end.Reason}", cancellationToken)) };
        var client = await ClientAsync(ended, definition);
        var job = await RequestAsync(client, definition, "x", options: For(subject));
        await CrashClaimAsync(client, definition, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5));
        await using (var transaction = await BeginAsync())
        {
            Assert.Equal(1, await Jobs(client).CancelAsync(transaction.Work, definition, subject, Cancel));
            await transaction.CommitAsync(Cancel);
        }
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        await WorkerAsync(definition, Recording(attempts), ended);

        var state = await EndedAsync(job.Id, TimeSpan.FromSeconds(10));
        Assert.Equal((JobOutcome.Cancelled, JobReasons.Cancelled, 1), (state.State, state.Reason, state.Attempts));
        Assert.Empty(attempts);
        Assert.Equal(["Cancelled:cancelled"], await Provider.ReadJournalAsync(journal, Cancel));
    }

    [Fact]
    public async Task D1_DeletingTheSubjectCancelsItsRunningJobThroughItsLease()
    {
        var definition = Definition(subject: "feed");
        var other = Definition(subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var observed = new TaskCompletionSource<LeaseStopReason?>();
        var ended = new EndedScript();
        await WorkerAsync(definition, new JobScript
        {
            Run = async (_, run, cancellationToken) =>
            {
                try
                {
                    await Task.Delay(Timeout.Infinite, cancellationToken);
                }
                finally
                {
                    observed.TrySetResult(run.StopReason);
                }
                return JobResult.Completed;
            },
        }, ended);
        var deleter = await ClientAsync(null, definition, other);
        var running = await RequestAsync(deleter, definition, "running", options: For(subject));
        await EventuallyAsync(async () => (await ReadJobAsync(running.Id)).Held, TimeSpan.FromSeconds(10), "the job runs");
        var scheduled = await RequestAsync(deleter, other, "scheduled", Schedule.After(TimeSpan.FromHours(1)), For(subject));
        var follow = await RequestAsync(deleter, definition, "follow-up", options: For(subject));
        Assert.Equal(JobRequestOutcome.FollowUp, follow.Outcome);

        await using (var transaction = await BeginAsync())
        {
            Assert.Equal(2, await Jobs(deleter).CancelAllAsync(transaction.Work, subject, Cancel));
            await transaction.CommitAsync(Cancel);
        }
        Assert.Equal(JobOutcome.Cancelled, (await ReadJobAsync(scheduled.Id)).State);
        Assert.Equal(LeaseStopReason.CancelRequested, await observed.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel));
        var state = await EndedAsync(running.Id, TimeSpan.FromSeconds(10));
        Assert.Equal((JobOutcome.Cancelled, (string?)null), (state.State, state.PendingArgs));
        Assert.Equal(JobOutcome.Cancelled, Assert.Single(ended.Calls).Outcome);
    }

    [Fact]
    public async Task D1_CancellingAllOfASubjectIsRefusedForUnregisteredDefinitions()
    {
        var registered = Definition(subject: "feed");
        var unregistered = Definition(subject: "feed");
        // A subject of its own: the refused cancellation leaves this job outstanding.
        var subject = Subject.Of("feed", "unregistered");
        var both = await ClientAsync(null, registered, unregistered);
        await RequestAsync(both, unregistered, "x", Schedule.After(TimeSpan.FromHours(1)), For(subject));
        var partial = await ClientAsync(null, registered);
        await using var transaction = await BeginAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await Jobs(partial).CancelAllAsync(transaction.Work, subject, Cancel));
    }

    [Fact]
    public async Task D2_CompletionCommitsWithTheDomainOutput()
    {
        var definition = Definition();
        var journal = await Provider.CreateTableAsync([""], Cancel);
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        var ended = new EndedScript();
        await WorkerAsync(definition, Recording(attempts, async (_, run, cancellationToken) =>
        {
            await using var transaction = await Provider.BeginAsync(cancellationToken);
            await Provider.JournalAsync(transaction.Work, journal, "synced", cancellationToken);
            await run.CompleteAsync(transaction.Work, JobResult.Completed, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            // The process dies right after the commit: nothing more is applied.
            throw new InvalidOperationException("crashed after commit");
        }), ended);
        var client = await ClientAsync(null, definition);
        var job = await RequestAsync(client, definition, "x");

        var state = await EndedAsync(job.Id, TimeSpan.FromSeconds(10));
        await Task.Delay(1500, Cancel);
        Assert.Equal(JobOutcome.Completed, state.State);
        Assert.Single(attempts);
        Assert.Equal(["synced"], await Provider.ReadJournalAsync(journal, Cancel));
        Assert.Single(ended.Calls);
    }

    [Fact]
    public async Task D4_EveryTerminalTransitionRunsTheHookOnceInItsTransaction()
    {
        var definition = Definition(new JobPolicy { Attempts = 1 }, subject: "feed");
        var journal = await Provider.CreateTableAsync([""], Cancel);
        var hook = new EndedScript { Ended = (end, transaction, cancellationToken) => new ValueTask(Provider.JournalAsync(transaction, journal, end.Subject!.Id + ":" + end.Outcome, cancellationToken)) };
        var worker = await WorkerAsync(definition, new JobScript
        {
            Run = async (args, run, cancellationToken) =>
            {
                switch (args.Value)
                {
                    case "in-transaction":
                        await using (var transaction = await Provider.BeginAsync(cancellationToken))
                        {
                            await run.CompleteAsync(transaction.Work, JobResult.Fail("bad_input"), cancellationToken);
                            await transaction.CommitAsync(cancellationToken);
                        }
                        return run.Completed;
                    default:
                        return JobResult.Completed;
                }
            },
        }, hook);
        var requester = await ClientAsync(hook, definition);
        var inTransaction = await RequestAsync(worker, definition, "in-transaction", options: For(Subject.Of("feed", "a")));
        var returned = await RequestAsync(worker, definition, "returned", options: For(Subject.Of("feed", "b")));
        var cancelled = await RequestAsync(requester, definition, "cancelled", Schedule.After(TimeSpan.FromHours(1)), For(Subject.Of("feed", "c")));
        await using (var transaction = await BeginAsync())
        {
            await Jobs(requester).CancelAsync(transaction.Work, definition, Subject.Of("feed", "c"), Cancel);
            await transaction.CommitAsync(Cancel);
        }

        await EndedAsync(inTransaction.Id, TimeSpan.FromSeconds(10));
        await EndedAsync(returned.Id, TimeSpan.FromSeconds(10));
        await Task.Delay(1000, Cancel);
        Assert.Equal(["a:Failed", "b:Completed", "c:Cancelled"], (await Provider.ReadJournalAsync(journal, Cancel)).Order());
        Assert.Equal(JobOutcome.Cancelled, (await ReadJobAsync(cancelled.Id)).State);
    }

    [Fact]
    public async Task D5_AFailingHookRollsTheTransitionBackAndItIsRetried()
    {
        var definition = Definition();
        var journal = await Provider.CreateTableAsync([""], Cancel);
        var calls = 0;
        var hook = new EndedScript
        {
            Ended = async (end, transaction, cancellationToken) =>
            {
                await Provider.JournalAsync(transaction, journal, "recorded", cancellationToken);
                if (Interlocked.Increment(ref calls) == 1)
                {
                    throw new InvalidOperationException("hook failed");
                }
            },
        };
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        await WorkerAsync(definition, Recording(attempts), hook);
        var client = await ClientAsync(null, definition);
        var job = await RequestAsync(client, definition, "x");

        Assert.Equal(JobOutcome.Completed, (await EndedAsync(job.Id, TimeSpan.FromSeconds(10))).State);
        Assert.Equal(2, calls);
        Assert.Single(attempts);
        Assert.Equal(["recorded"], await Provider.ReadJournalAsync(journal, Cancel));
    }
}
