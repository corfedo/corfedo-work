using System.Collections.Concurrent;

namespace Corfedo.Work.Conformance;

/// <summary>Run history entries: one per attempt, on every path that ends an attempt.</summary>
public abstract class JobRunHistoryScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    [Fact]
    public async Task D8_EveryAttemptIsRecordedWithItsEpochOutcomeAndReason()
    {
        var definition = Definition(new JobPolicy { Attempts = 3, Backoff = Backoff.None, RunHistory = true }, subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        var worker = await WorkerAsync(definition, Recording(attempts, (_, run, _) => run.Attempt switch
        {
            1 => ValueTask.FromResult<JobDisposition>(JobResult.Retry(TimeSpan.Zero, "busy")),
            2 => throw new InvalidOperationException("broken"),
            _ => ValueTask.FromResult<JobDisposition>(JobResult.Completed),
        }));
        var job = await RequestAsync(worker, definition, "x", options: For(subject));
        await EndedAsync(job.Id, TimeSpan.FromSeconds(10));

        var page = await Jobs(worker).GetRunsAsync(definition, subject, cancellationToken: Cancel);
        Assert.Null(page.Next);
        Assert.Equal(
            [(3, JobRunOutcome.Completed, (string?)null), (2, JobRunOutcome.Retried, JobReasons.HandlerFailed), (1, JobRunOutcome.Retried, "busy")],
            page.Runs.Select(r => (r.Attempt, r.Outcome, r.Reason)));
        Assert.All(page.Runs, r =>
        {
            Assert.Equal((job.Id, subject, attempts.First().ExecutionId, worker.Owner), (r.Job, r.Subject, r.ExecutionId, r.Owner));
            Assert.True(r.StartedAt <= r.EndedAt);
        });
        Assert.Equal(3, page.Runs.Select(r => r.Epoch).Distinct().Count());
        Assert.True(page.Runs[0].Epoch > page.Runs[1].Epoch && page.Runs[1].Epoch > page.Runs[2].Epoch);
        Assert.Equal(page.Runs, (await Jobs(worker).GetRunsAsync(definition, job.Id, cancellationToken: Cancel)).Runs);

        var first = await Jobs(worker).GetRunsAsync(definition, subject, limit: 2, cancellationToken: Cancel);
        Assert.Equal(page.Runs.Take(2), first.Runs);
        var rest = await Jobs(worker).GetRunsAsync(definition, subject, limit: 2, after: first.Next, cancellationToken: Cancel);
        Assert.Equal([page.Runs[2]], rest.Runs);
        Assert.Null(rest.Next);
        // A cursor whose start is the same instant in another offset continues the same page.
        foreach (var offset in new[] { TimeSpan.FromHours(2), TimeSpan.FromHours(-7) })
        {
            var shifted = first.Next! with { StartedAt = first.Next!.StartedAt.ToOffset(offset) };
            Assert.Equal(rest.Runs, (await Jobs(worker).GetRunsAsync(definition, subject, limit: 2, after: shifted, cancellationToken: Cancel)).Runs);
        }
    }

    [Fact]
    public async Task D8_ALostLeaseIsRecordedOnceAndTheTakeoverSeparately()
    {
        var definition = Definition(new JobPolicy { Attempts = 3, Backoff = Backoff.None, RunHistory = true }, subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var client = await ClientAsync(null, definition);
        var job = await RequestAsync(client, definition, "x", options: For(subject));
        var crashed = await CrashClaimAsync(client, definition, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5));
        await WorkerAsync(definition, new JobScript());
        await EndedAsync(job.Id, TimeSpan.FromSeconds(10));

        var runs = (await Jobs(client).GetRunsAsync(definition, subject, cancellationToken: Cancel)).Runs;
        Assert.Equal([(2, JobRunOutcome.Completed), (1, JobRunOutcome.LeaseLost)], runs.Select(r => (r.Attempt, r.Outcome)));
        Assert.Equal((crashed.Lease.Epoch, client.Owner), (runs[1].Epoch, runs[1].Owner));
        Assert.True(runs[1].EndedAt <= runs[0].StartedAt);
    }

    [Fact]
    public async Task D8_AClaimThatEndsALostJobWithoutAHandlerRecordsOnlyTheLostAttempt()
    {
        var definition = Definition(new JobPolicy { Attempts = 1, Backoff = Backoff.None, RunHistory = true }, subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var client = await ClientAsync(null, definition);
        var job = await RequestAsync(client, definition, "x", options: For(subject));
        await CrashClaimAsync(client, definition, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5));
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        await WorkerAsync(definition, Recording(attempts));

        var state = await EndedAsync(job.Id, TimeSpan.FromSeconds(10));
        Assert.Equal((JobOutcome.Failed, JobReasons.AttemptsExhausted), (state.State, state.Reason));
        Assert.Empty(attempts);
        var run = Assert.Single((await Jobs(client).GetRunsAsync(definition, subject, cancellationToken: Cancel)).Runs);
        Assert.Equal((1, JobRunOutcome.LeaseLost), (run.Attempt, run.Outcome));
    }

    [Fact]
    public async Task D8_AGracefulReleaseAndACancellationAreRecorded()
    {
        var definition = Definition(new JobPolicy { Attempts = 1, Backoff = Backoff.None, RunHistory = true }, subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var running = new ConcurrentQueue<ObservedAttempt>();
        JobScript Blocking() => Recording(running, async (_, _, cancellationToken) =>
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
            return JobResult.Completed;
        });
        var first = await WorkerAsync(definition, Blocking(), shutdownGrace: TimeSpan.FromMilliseconds(200));
        var job = await RequestAsync(first, definition, "x", options: For(subject));
        await EventuallyAsync(() => running.Count == 1, TimeSpan.FromSeconds(10), "the first attempt runs");
        await first.StopAsync(Cancel);
        var second = await WorkerAsync(definition, Blocking());
        await EventuallyAsync(() => running.Count == 2, TimeSpan.FromSeconds(10), "the second attempt runs");
        await InTransactionAsync(transaction => Jobs(second).CancelAsync(transaction, definition, subject, Cancel));
        await EndedAsync(job.Id, TimeSpan.FromSeconds(10));

        var runs = (await Jobs(second).GetRunsAsync(definition, job.Id, cancellationToken: Cancel)).Runs;
        Assert.Equal(
            [(1, JobRunOutcome.Cancelled, JobReasons.Cancelled, second.Owner), (1, JobRunOutcome.Released, (string?)null, first.Owner)],
            runs.Select(r => (r.Attempt, r.Outcome, r.Reason, r.Owner)));
    }

    [Fact]
    public async Task D8_RunHistoryIsOffByDefault()
    {
        var definition = Definition(new JobPolicy { Backoff = Backoff.None }, subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var worker = await WorkerAsync(definition, new JobScript());
        var job = await RequestAsync(worker, definition, "x", options: For(subject));
        await EndedAsync(job.Id, TimeSpan.FromSeconds(10));

        Assert.Empty((await Jobs(worker).GetRunsAsync(definition, subject, cancellationToken: Cancel)).Runs);
    }

    [Fact]
    public async Task D8_TerminalOutcomesAndRetriedTimeoutsAreRecordedWithTheirReasons()
    {
        var subject = Subject.Of("feed", "npm");
        static async ValueTask<JobDisposition> Hang(CancellationToken cancellationToken)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
            return JobResult.Completed;
        }
        var retrying = Definition(new JobPolicy { Attempts = 3, Backoff = Backoff.None, Timeout = TimeSpan.FromMilliseconds(300), RetryTimeouts = true, RunHistory = true }, subject: "feed");
        var timingOut = Definition(new JobPolicy { Attempts = 3, Backoff = Backoff.None, Timeout = TimeSpan.FromMilliseconds(300), RunHistory = true }, subject: "feed");
        var failing = Definition(new JobPolicy { Attempts = 3, Backoff = Backoff.None, RunHistory = true }, subject: "feed");
        var worker = await WorkerAsync(retrying, new JobScript
        {
            Run = (_, run, cancellationToken) => run.Attempt == 1 ? Hang(cancellationToken) : ValueTask.FromResult<JobDisposition>(JobResult.Retry(TimeSpan.Zero, "busy")),
        });
        var timingOutWorker = await WorkerAsync(timingOut, new JobScript { Run = (_, _, cancellationToken) => Hang(cancellationToken) });
        var failingWorker = await WorkerAsync(failing, new JobScript { Run = (_, _, _) => ValueTask.FromResult<JobDisposition>(JobResult.Fail("broken")) });
        var cases = new[] { (worker, retrying), (timingOutWorker, timingOut), (failingWorker, failing) };
        foreach (var (process, definition) in cases)
        {
            await EndedAsync((await RequestAsync(process, definition, "x", options: For(subject))).Id, TimeSpan.FromSeconds(15));
        }

        async Task<(int, JobRunOutcome, string?)[]> RunsOf(TestProcess process, JobDefinition definition) =>
            [.. (await Jobs(process).GetRunsAsync(definition, subject, cancellationToken: Cancel)).Runs.Select(r => (r.Attempt, r.Outcome, r.Reason))];
        Assert.Equal(
            [(3, JobRunOutcome.Failed, JobReasons.AttemptsExhausted), (2, JobRunOutcome.Retried, "busy"), (1, JobRunOutcome.Retried, "timeout")],
            await RunsOf(worker, retrying));
        Assert.Equal([(1, JobRunOutcome.TimedOut, "timeout")], await RunsOf(timingOutWorker, timingOut));
        Assert.Equal([(1, JobRunOutcome.Failed, "broken")], await RunsOf(failingWorker, failing));
    }

    [Fact]
    public async Task D8_ALateClaimRunsNoHandlerAndRecordsNoEntry()
    {
        var definition = Definition(new JobPolicy { Late = Late.SkipAfter(TimeSpan.FromSeconds(2)), RunHistory = true }, subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var client = await ClientAsync(null, definition);
        var late = await RequestAsync(client, definition, "late", Schedule.At(DateTimeOffset.UtcNow.AddSeconds(-3)), For(subject));
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        await WorkerAsync(definition, Recording(attempts));

        Assert.Equal((JobOutcome.Skipped, "late"), ((await EndedAsync(late.Id, TimeSpan.FromSeconds(10))).State, (await ReadJobAsync(late.Id)).Reason));
        Assert.Empty(attempts);
        Assert.Empty((await Jobs(client).GetRunsAsync(definition, late.Id, cancellationToken: Cancel)).Runs);
    }

    [Fact]
    public async Task D8_OccurrencesAreRecordedWithTheirOccurrence()
    {
        var definition = Definition(new JobPolicy { Backoff = Backoff.None, RunHistory = true }, subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var worker = await WorkerAsync(definition, Occurrences(runs), pollingInterval: TimeSpan.FromMilliseconds(250));
        await ScheduleAsync(worker, definition, "x", Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate, await Provider.NowAsync(Cancel)), Recurring(subject));
        var observed = (await RunsAsync(runs, 2, TimeSpan.FromSeconds(15))).Take(2).ToList();

        // A handler returns before its completion commits, so the entries are read once both are recorded.
        IReadOnlyList<JobRunEntry> entries = [];
        await EventuallyAsync(
            async () =>
            {
                entries = (await Jobs(worker).GetRunsAsync(definition, subject, cancellationToken: Cancel)).Runs;
                return observed.All(run => entries.Any(e => e.Job == run.Job));
            },
            TimeSpan.FromSeconds(10),
            "both occurrences are recorded");
        foreach (var run in observed)
        {
            Assert.StartsWith("20", run.Occurrence, StringComparison.Ordinal);
            var entry = Assert.Single(entries, e => e.Job == run.Job);
            Assert.Equal((run.Occurrence, run.ExecutionId, JobRunOutcome.Completed), (entry.Occurrence, entry.ExecutionId, entry.Outcome));
        }
    }

    [Fact]
    public async Task D8_AGracefulReleaseReturnsItsAttemptAndAFailedOneIsALostLease()
    {
        var definition = Definition(new JobPolicy { Attempts = 3, Backoff = Backoff.None, RunHistory = true }, subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var client = await ClientAsync(null, definition);
        var job = await RequestAsync(client, definition, "x", options: For(subject));
        var (store, source) = await JobSourceAsync(client, definition, TimeSpan.FromSeconds(30));
        async Task<LeaseClaim<JobId>> ClaimAsync() =>
            Assert.Single((await store.Leases.ClaimDueAsync(source, LeaseCallbacks<JobId>.None, client.Owner, 1, Cancel)).Claims);

        var graceful = await ClaimAsync();
        Assert.True(await store.Leases.ReleaseAsync(graceful.Lease, Due.Now, failed: false, Cancel));
        var failed = await ClaimAsync();
        Assert.True(await store.Leases.ReleaseAsync(failed.Lease, Due.Now, failed: true, Cancel));
        var last = await ClaimAsync();
        // A stale release changes nothing and records nothing.
        Assert.False(await store.Leases.ReleaseAsync(failed.Lease, Due.Now, failed: true, Cancel));
        Assert.True(await store.Jobs.CompleteAsync(last.Lease, JobTransition.From(JobResult.Completed), null, Cancel));

        var runs = (await Jobs(client).GetRunsAsync(definition, job.Id, cancellationToken: Cancel)).Runs;
        Assert.Equal(
            [(2, JobRunOutcome.Completed, last.Lease.Epoch), (1, JobRunOutcome.LeaseLost, failed.Lease.Epoch), (1, JobRunOutcome.Released, graceful.Lease.Epoch)],
            runs.Select(r => (r.Attempt, r.Outcome, r.Epoch)));
        // The failed release ends its entry when it is released, long before its lease would have expired.
        Assert.True(runs[1].EndedAt - runs[1].StartedAt < TimeSpan.FromSeconds(10), $"ended {runs[1].EndedAt - runs[1].StartedAt} after its start");
    }

    [Fact]
    public async Task D8_AnExpiredLeaseSettledWithoutAClaimIsRecordedAtItsExpiry()
    {
        var definition = Definition(new JobPolicy { Attempts = 3, Backoff = Backoff.None, RunHistory = true }, subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var client = await ClientAsync(null, definition);
        var job = await RequestAsync(client, definition, "x", options: For(subject));
        var (store, source) = await JobSourceAsync(client, definition, TimeSpan.FromSeconds(1));
        var crashed = Assert.Single((await store.Leases.ClaimDueAsync(source, LeaseCallbacks<JobId>.None, client.Owner, 1, Cancel)).Claims);
        // A stale release after the expiry records nothing; settlement records the loss.
        await EventuallyAsync(async () => !(await ReadJobAsync(job.Id)).Held, TimeSpan.FromSeconds(5), "the lease expires");
        Assert.False(await store.Leases.ReleaseAsync(crashed.Lease, Due.Now, failed: true, Cancel));

        // Nothing claims the job again: the client's expiry processor, or this settlement if it has not yet, settles it.
        await store.Leases.SettleExpiredAsync(source, LeaseCallbacks<JobId>.None, 10, Cancel);
        var run = Assert.Single((await Jobs(client).GetRunsAsync(definition, subject, cancellationToken: Cancel)).Runs);
        Assert.Equal((1, JobRunOutcome.LeaseLost, crashed.Lease.Epoch), (run.Attempt, run.Outcome, run.Epoch));
        Assert.InRange(run.EndedAt - run.StartedAt, TimeSpan.FromSeconds(0.9), TimeSpan.FromSeconds(1.1));
        var state = await ReadJobAsync(job.Id);
        Assert.Equal(((JobOutcome?)null, false, 1), (state.State, state.Held, state.Attempts));
    }
}
