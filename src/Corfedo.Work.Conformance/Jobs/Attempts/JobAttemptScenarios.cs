using System.Collections.Concurrent;

namespace Corfedo.Work.Conformance;

/// <summary>Attempts, retries, exhaustion, lateness, timeouts and graceful releases.</summary>
public abstract class JobAttemptScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    [Fact]
    public async Task J10_ACrashCountsAsAnAttemptAndTheLastFailureExhaustsTheJob()
    {
        var definition = Definition(new JobPolicy { Attempts = 3, Backoff = Backoff.None });
        var client = await ClientAsync(null, definition);
        var job = await RequestAsync(client, definition, "x");
        var crashed = await CrashClaimAsync(client, definition, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5));
        Assert.Equal(JobClaimKind.Run, crashed.Job!.Kind);
        Assert.Equal(1, crashed.Job.Attempt);

        var attempts = new ConcurrentQueue<ObservedAttempt>();
        var ended = new EndedScript();
        await WorkerAsync(definition, Recording(attempts, (_, _, _) => throw new InvalidOperationException("boom")), ended);

        var state = await EndedAsync(job.Id, TimeSpan.FromSeconds(15));
        Assert.Equal(JobOutcome.Failed, state.State);
        Assert.Equal(JobReasons.AttemptsExhausted, state.Reason);
        Assert.Equal(3, state.Attempts);
        Assert.Equal([2, 3], attempts.Select(a => a.Attempt));
        Assert.All(attempts, a => Assert.Equal(crashed.ExecutionId, a.ExecutionId));
        var end = Assert.Single(ended.Calls);
        Assert.Equal((JobOutcome.Failed, JobReasons.AttemptsExhausted, 3), (end.Outcome, end.Reason, end.Attempts));
    }

    [Fact]
    public async Task J11_ARetryWaitsForTheLongerOfItsDelayAndTheBackoff()
    {
        var definition = Definition(new JobPolicy { Attempts = 5, Backoff = Backoff.Constant(TimeSpan.FromSeconds(10)) });
        var worker = await WorkerAsync(definition, new JobScript { Run = (_, _, _) => ValueTask.FromResult<JobDisposition>(JobResult.Retry(TimeSpan.FromHours(1), "later")) });
        var job = await RequestAsync(worker, definition, "x");

        await EventuallyAsync(async () => (await ReadJobAsync(job.Id)) is { Attempts: 1, Held: false }, TimeSpan.FromSeconds(10), "the attempt asks for a retry");
        var state = await ReadJobAsync(job.Id);
        Assert.Null(state.State);
        Assert.InRange(state.DueIn!.Value, TimeSpan.FromMinutes(59), TimeSpan.FromMinutes(60));
    }

    [Fact]
    public async Task J7_AJobLaterThanItsGraceEndsSkippedWithoutAnAttempt()
    {
        var definition = Definition(new JobPolicy { Late = Late.SkipAfter(TimeSpan.FromSeconds(2)) });
        var client = await ClientAsync(null, definition);
        var late = await RequestAsync(client, definition, "late", Schedule.At(DateTimeOffset.UtcNow.AddSeconds(-3)));
        var inTime = await RequestAsync(client, definition, "in-time", Schedule.At(DateTimeOffset.UtcNow.AddSeconds(-1)));
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        var ended = new EndedScript();
        await WorkerAsync(definition, Recording(attempts), ended);

        var skipped = await EndedAsync(late.Id, TimeSpan.FromSeconds(10));
        var ran = await EndedAsync(inTime.Id, TimeSpan.FromSeconds(10));
        Assert.Equal((JobOutcome.Skipped, "late", 0, false), (skipped.State, skipped.Reason, skipped.Attempts, skipped.Started));
        Assert.Equal(JobOutcome.Completed, ran.State);
        Assert.Equal("in-time", Assert.Single(attempts).Args);
        Assert.Contains(ended.Calls, e => e.Job == late.Id && e.Outcome == JobOutcome.Skipped);
    }

    [Fact]
    public async Task J7_ARequestIntoALateClaimedJobBecomesItsFollowUp()
    {
        var definition = Definition(new JobPolicy { Late = Late.SkipAfter(TimeSpan.FromSeconds(2)) }, subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var ended = new EndedScript();
        var client = await ClientAsync(ended, definition);
        var job = await RequestAsync(client, definition, "late", Schedule.At(DateTimeOffset.UtcNow.AddSeconds(-3)), For(subject));
        var claim = await CrashClaimAsync(client, definition, TimeSpan.FromMilliseconds(300), TimeSpan.FromSeconds(5));
        Assert.Equal(JobClaimKind.Late, claim.Job!.Kind);

        var merged = await RequestAsync(client, definition, "fresh", options: For(subject));
        Assert.Equal(new JobRequestResult(job.Id, JobRequestOutcome.FollowUp), merged);
        var claimed = await ReadJobAsync(job.Id);
        Assert.Equal(("late", "fresh", false, claim.ExecutionId), (claimed.Args, claimed.PendingArgs, claimed.Started, claimed.ExecutionId));
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        await WorkerAsync(definition, Recording(attempts), ended);

        var state = await EndedAsync(job.Id, TimeSpan.FromSeconds(10));
        Assert.Equal((JobOutcome.Completed, "fresh"), (state.State, state.Args));
        var run = Assert.Single(attempts);
        Assert.Equal(("fresh", 1), (run.Args, run.Attempt));
        Assert.NotEqual(claim.ExecutionId, run.ExecutionId);
        Assert.Equal([JobOutcome.Skipped, JobOutcome.Completed], ended.Calls.Select(c => c.Outcome));
    }

    [Fact]
    public async Task J7_AFollowUpOverdueAfterALongRunEndsSkippedWithoutAnAttempt()
    {
        var definition = Definition(new JobPolicy { Late = Late.SkipAfter(TimeSpan.FromSeconds(1)) }, subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var release = new TaskCompletionSource();
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        var ended = new EndedScript();
        var worker = await WorkerAsync(definition, Recording(attempts, async (args, _, cancellationToken) =>
        {
            if (args.Value == "first")
            {
                await release.Task.WaitAsync(cancellationToken);
            }
            return JobResult.Completed;
        }), ended);
        var job = await RequestAsync(worker, definition, "first", options: For(subject));
        await EventuallyAsync(() => attempts.Count == 1, TimeSpan.FromSeconds(10), "the first execution runs");
        Assert.Equal(JobRequestOutcome.FollowUp, (await RequestAsync(worker, definition, "second", options: For(subject))).Outcome);
        await Task.Delay(2000, Cancel);
        release.SetResult();

        await EventuallyAsync(() => ended.Calls.Count == 2, TimeSpan.FromSeconds(10), "the follow-up ends");
        var state = await ReadJobAsync(job.Id);
        Assert.Equal((JobOutcome.Skipped, "late", 0, false, "second"), (state.State, state.Reason, state.Attempts, state.Started, state.Args));
        Assert.Equal(["first"], attempts.Select(a => a.Args));
        Assert.Equal([JobOutcome.Completed, JobOutcome.Skipped], ended.Calls.Select(c => c.Outcome));
    }

    [Fact]
    public async Task J27_ATimedOutAttemptEndsTimedOutOrRetriesAndKeepsItsLeaseUntilItsHandlerReturns()
    {
        var honouring = Definition(new JobPolicy { Timeout = TimeSpan.FromSeconds(1) });
        var retrying = Definition(new JobPolicy { Timeout = TimeSpan.FromSeconds(1), RetryTimeouts = true, Attempts = 2, Backoff = Backoff.None });
        var ignoring = Definition(new JobPolicy { Timeout = TimeSpan.FromSeconds(1) });
        var waits = new JobScript { Run = async (_, _, cancellationToken) => { await Task.Delay(Timeout.Infinite, cancellationToken); return JobResult.Completed; } };
        var ignored = new ConcurrentQueue<ObservedAttempt>();
        var worker = await WorkerAsync(honouring, waits, also: work => work
            .AddJob<TestArgs, ScriptedJobHandler>(retrying, job => job.Lease(TimeSpan.FromSeconds(2))));
        var other = await WorkerAsync(ignoring, Recording(ignored, async (_, _, _) =>
        {
            await Task.Delay(TimeSpan.FromSeconds(6), Cancel);
            return JobResult.Completed;
        }));
        var timedOut = await RequestAsync(worker, honouring, "x");
        var retried = await RequestAsync(worker, retrying, "x");
        var slow = await RequestAsync(other, ignoring, "x");

        var first = await EndedAsync(timedOut.Id, TimeSpan.FromSeconds(10));
        Assert.Equal((JobOutcome.TimedOut, "timeout", 1), (first.State, first.Reason, first.Attempts));
        var second = await EndedAsync(retried.Id, TimeSpan.FromSeconds(15));
        Assert.Equal((JobOutcome.TimedOut, "timeout", 2), (second.State, second.Reason, second.Attempts));

        await EventuallyAsync(() => other.Logs.Events.Contains("job.timeout_ignored"), TimeSpan.FromSeconds(10), "the ignored timeout is reported");
        Assert.True((await ReadJobAsync(slow.Id)).Held);
        var third = await EndedAsync(slow.Id, TimeSpan.FromSeconds(15));
        Assert.Equal(JobOutcome.TimedOut, third.State);
        Assert.Single(ignored);
    }

    [Fact]
    public async Task J5_AGracefulReleaseReturnsTheAttemptAndKeepsTheExecution()
    {
        var definition = Definition(new JobPolicy { Attempts = 1 });
        var stopping = new ConcurrentQueue<ObservedAttempt>();
        var first = await WorkerAsync(definition, Recording(stopping, async (_, _, cancellationToken) =>
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
            return JobResult.Completed;
        }), shutdownGrace: TimeSpan.FromMilliseconds(200));
        var job = await RequestAsync(first, definition, "x");
        await EventuallyAsync(() => !stopping.IsEmpty, TimeSpan.FromSeconds(10), "the first attempt runs");
        await first.StopAsync(Cancel);
        var released = await ReadJobAsync(job.Id);
        Assert.Equal((false, 0, true, (JobOutcome?)null), (released.Held, released.Attempts, released.Started, released.State));

        var attempts = new ConcurrentQueue<ObservedAttempt>();
        await WorkerAsync(definition, Recording(attempts));
        var state = await EndedAsync(job.Id, TimeSpan.FromSeconds(10));
        Assert.Equal(JobOutcome.Completed, state.State);
        var resumed = Assert.Single(attempts);
        Assert.Equal(1, resumed.Attempt);
        Assert.Equal(Assert.Single(stopping).ExecutionId, resumed.ExecutionId);
        Assert.NotEqual(stopping.Single().AttemptId, resumed.AttemptId);
    }

    [Fact]
    public async Task J5_ARequestAfterARefundedAttemptBecomesTheFollowUp()
    {
        var definition = Definition(new JobPolicy { Attempts = 2 }, subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var stopping = new ConcurrentQueue<ObservedAttempt>();
        var first = await WorkerAsync(definition, Recording(stopping, async (_, _, cancellationToken) =>
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
            return JobResult.Completed;
        }), shutdownGrace: TimeSpan.FromMilliseconds(200));
        var job = await RequestAsync(first, definition, "a", options: For(subject));
        await EventuallyAsync(() => !stopping.IsEmpty, TimeSpan.FromSeconds(10), "the first attempt runs");
        await first.StopAsync(Cancel);

        var client = await ClientAsync(null, definition);
        var merged = await RequestAsync(client, definition, "b", options: For(subject));
        Assert.Equal(new JobRequestResult(job.Id, JobRequestOutcome.FollowUp), merged);
        var state = await ReadJobAsync(job.Id);
        Assert.Equal(("a", "b", 0), (state.Args, state.PendingArgs, state.Attempts));
    }

    [Fact]
    public async Task D3_ExhaustionByLostLeasesRunsTheHookWithoutAnotherAttempt()
    {
        var definition = Definition(new JobPolicy { Attempts = 2, Backoff = Backoff.None });
        var client = await ClientAsync(null, definition);
        var job = await RequestAsync(client, definition, "x");
        var journal = await Provider.CreateTableAsync([""], Cancel);
        var first = await CrashClaimAsync(client, definition, TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(5));
        var second = await CrashClaimAsync(client, definition, TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(5));
        Assert.Equal((1, 2), (first.Job!.Attempt, second.Job!.Attempt));
        Assert.Equal(first.ExecutionId, second.ExecutionId);
        Assert.NotEqual(first.AttemptId, second.AttemptId);

        var attempts = new ConcurrentQueue<ObservedAttempt>();
        var ended = new EndedScript { Ended = (end, transaction, cancellationToken) => new ValueTask(Provider.JournalAsync(transaction, journal, $"{end.Outcome}:{end.Reason}", cancellationToken)) };
        await WorkerAsync(definition, Recording(attempts), ended);

        var state = await EndedAsync(job.Id, TimeSpan.FromSeconds(10));
        Assert.Equal((JobOutcome.Failed, JobReasons.AttemptsExhausted, 2), (state.State, state.Reason, state.Attempts));
        Assert.Empty(attempts);
        Assert.Equal(["Failed:attempts_exhausted"], await Provider.ReadJournalAsync(journal, Cancel));
    }

    [Fact]
    public async Task J39_LeaseOperationsRefuseTheLifecycleOfJobs()
    {
        var definition = Definition(new JobPolicy { Backoff = Backoff.None });
        var client = await ClientAsync(null, definition);
        var job = await RequestAsync(client, definition, "x");
        var claim = await CrashClaimAsync(client, definition, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(5));
        var (_, source) = await JobSourceAsync(client, definition, TimeSpan.FromSeconds(30));
        var operations = client.Operations;
        var parsed = source.ParseToken(claim.Lease.Token);
        var refused = new List<Func<WorkTransaction, Task>>
        {
            _ => operations.ClaimDueAsync(source, 1, Cancel).AsTask(),
            _ => operations.ClaimAsync(source, job.Id, Cancel).AsTask(),
            transaction => operations.ClaimAsync(transaction, source, [job.Id], 1, Cancel).AsTask(),
            _ => operations.ReleaseAsync(claim.Lease, Due.Now, cancellationToken: Cancel).AsTask(),
            _ => operations.ReleaseAsync(parsed, Due.Now, failed: true, Cancel).AsTask(),
            transaction => operations.EndAsync(parsed, transaction, cancellationToken: Cancel).AsTask(),
            transaction => operations.ScheduleAsync(transaction, source, job.Id, Due.Now, Guid.NewGuid(), Cancel).AsTask(),
            transaction => operations.RequestCancelAsync(transaction, source, job.Id, Cancel).AsTask(),
            transaction => operations.ClearCancelAsync(transaction, source, job.Id, Cancel).AsTask(),
        };
        foreach (var operation in refused)
        {
            await using var transaction = await BeginAsync();
            await Assert.ThrowsAsync<ArgumentException>(() => operation(transaction.Work));
            await transaction.RollbackAsync(Cancel);
        }

        // The holder's own renewals and guards stay available.
        Assert.True((await operations.RenewAsync(parsed, Cancel)).Valid);
        await using (var transaction = await BeginAsync())
        {
            await operations.GuardAsync(parsed, transaction.Work, Cancel);
            await transaction.CommitAsync(Cancel);
        }
        var state = await ReadJobAsync(job.Id);
        Assert.Equal(((JobOutcome?)null, true, false, claim.ExecutionId), (state.State, state.Held, state.CancelRequested, state.ExecutionId));
    }
}
