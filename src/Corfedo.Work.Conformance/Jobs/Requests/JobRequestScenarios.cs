using System.Collections.Concurrent;

namespace Corfedo.Work.Conformance;

/// <summary>Requests, schedules, merging, follow-ups and fan-out of one-shot jobs.</summary>
public abstract class JobRequestScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    [Fact]
    public async Task J1_RequestCommitsWithTheDomainChange()
    {
        var definition = Definition();
        var client = await ClientAsync(null, definition);
        await RequestAsync(client, definition, "committed");
        await using (var rolledBack = await BeginAsync())
        {
            await Jobs(client).RequestAsync(rolledBack.Work, definition, new TestArgs("rolled-back"), cancellationToken: Cancel);
            await rolledBack.RollbackAsync(Cancel);
        }
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        await WorkerAsync(definition, Recording(attempts));

        await EventuallyAsync(() => attempts.Count == 1, TimeSpan.FromSeconds(10), "the committed job runs");
        await Task.Delay(1500, Cancel);
        Assert.Equal("committed", Assert.Single(attempts).Args);
        var job = Assert.Single(await ReadJobsAsync(definition));
        Assert.Equal(JobOutcome.Completed, job.State);
    }

    [Fact]
    public async Task J2_OneShotSchedulesRunOnceAndNotBeforeTheirTime()
    {
        var definition = Definition();
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        var worker = await WorkerAsync(definition, Recording(attempts));
        var requested = Clock.Elapsed;
        await RequestAsync(worker, definition, "now");
        await RequestAsync(worker, definition, "after", Schedule.After(TimeSpan.FromSeconds(1.5)));
        await RequestAsync(worker, definition, "at", Schedule.At(DateTimeOffset.UtcNow.AddSeconds(2.5)));

        await EventuallyAsync(() => attempts.Count == 3, TimeSpan.FromSeconds(15), "every job runs");
        await Task.Delay(1000, Cancel);
        var byArgs = attempts.ToDictionary(a => a.Args);
        Assert.Equal(3, attempts.Count);
        Assert.InRange(byArgs["after"].At - requested, TimeSpan.FromSeconds(1.4), TimeSpan.FromSeconds(10));
        Assert.InRange(byArgs["at"].At - requested, TimeSpan.FromSeconds(2.3), TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task J8_RequestsMergeBySubjectAndARequestDuringARunCausesOneFollowUp()
    {
        var definition = Definition(subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var client = await ClientAsync(null, definition);
        var first = await RequestAsync(client, definition, "a", options: For(subject));
        var second = await RequestAsync(client, definition, "b", options: For(subject));
        Assert.Equal(JobRequestOutcome.Created, first.Outcome);
        Assert.Equal(JobRequestOutcome.Merged, second.Outcome);
        Assert.Equal(first.Id, second.Id);
        Assert.Equal("b", (await ReadJobAsync(first.Id)).Args);

        var release = new TaskCompletionSource();
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        await WorkerAsync(definition, Recording(attempts, async (args, _, cancellationToken) =>
        {
            if (args.Value == "b")
            {
                await release.Task.WaitAsync(cancellationToken);
            }
            return JobResult.Completed;
        }));
        await EventuallyAsync(() => attempts.Count == 1, TimeSpan.FromSeconds(10), "the merged job runs");
        var during = await RequestAsync(client, definition, "c", options: For(subject));
        var again = await RequestAsync(client, definition, "d", options: For(subject));
        Assert.Equal(JobRequestOutcome.FollowUp, during.Outcome);
        Assert.Equal(JobRequestOutcome.FollowUp, again.Outcome);
        var running = await ReadJobAsync(first.Id);
        Assert.Equal("b", running.Args);
        Assert.Equal("d", running.PendingArgs);
        release.SetResult();

        await EventuallyAsync(() => attempts.Count == 2, TimeSpan.FromSeconds(10), "one follow-up runs");
        var job = await EndedAsync(first.Id, TimeSpan.FromSeconds(10));
        await Task.Delay(1500, Cancel);
        var runs = attempts.ToArray();
        Assert.Equal(["b", "d"], runs.Select(a => a.Args));
        Assert.All(runs, a => Assert.Equal(first.Id, a.Job));
        Assert.NotEqual(runs[0].ExecutionId, runs[1].ExecutionId);
        Assert.Equal(1, runs[1].Attempt);
        Assert.Equal(JobOutcome.Completed, job.State);
        Assert.Single(await ReadJobsAsync(definition));
    }

    [Fact]
    public async Task J8_PendingDueTimesMergeEarliestFirst()
    {
        var definition = Definition(subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var release = new TaskCompletionSource();
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        var worker = await WorkerAsync(definition, Recording(attempts, async (args, _, cancellationToken) =>
        {
            if (args.Value == "a")
            {
                await release.Task.WaitAsync(cancellationToken);
            }
            return JobResult.Completed;
        }));
        var job = await RequestAsync(worker, definition, "a", options: For(subject));
        await EventuallyAsync(() => attempts.Count == 1, TimeSpan.FromSeconds(10), "the job runs");
        await RequestAsync(worker, definition, "later", Schedule.After(TimeSpan.FromHours(1)), For(subject));
        await RequestAsync(worker, definition, "sooner", Schedule.After(TimeSpan.FromSeconds(30)), For(subject));
        var pending = await ReadJobAsync(job.Id);
        Assert.Equal("sooner", pending.PendingArgs);
        Assert.InRange(pending.PendingDueIn!.Value, TimeSpan.FromSeconds(25), TimeSpan.FromSeconds(31));
        await RequestAsync(worker, definition, "latest", Schedule.After(TimeSpan.FromHours(2)), For(subject));
        pending = await ReadJobAsync(job.Id);
        Assert.Equal("latest", pending.PendingArgs);
        Assert.InRange(pending.PendingDueIn!.Value, TimeSpan.FromSeconds(25), TimeSpan.FromSeconds(31));
        release.SetResult();
    }

    [Fact]
    public async Task J1_DebounceMovesTheDueTimeUpToItsMaximum()
    {
        var definition = Definition();
        var client = await ClientAsync(null, definition);
        var options = new JobRequestOptions { MergeKey = "recompute" };
        var first = await RequestAsync(client, definition, "a", Schedule.Debounce(TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(30)), options);
        Assert.InRange((await ReadJobAsync(first.Id)).DueIn!.Value, TimeSpan.FromSeconds(18), TimeSpan.FromSeconds(20.5));
        await Task.Delay(1500, Cancel);
        await RequestAsync(client, definition, "b", Schedule.Debounce(TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(30)), options);
        var moved = (await ReadJobAsync(first.Id)).DueIn!.Value;
        Assert.InRange(moved, TimeSpan.FromSeconds(19), TimeSpan.FromSeconds(20.5));
        await RequestAsync(client, definition, "c", Schedule.Debounce(TimeSpan.FromSeconds(29.5), TimeSpan.FromSeconds(30)), options);
        var capped = await ReadJobAsync(first.Id);
        Assert.InRange(capped.DueIn!.Value, TimeSpan.FromSeconds(26), TimeSpan.FromSeconds(29));
        Assert.Equal("c", capped.Args);
    }

    [Fact]
    public async Task J9_ARequestIntoAnotherPartitionsJobIsRefused()
    {
        var definition = Definition(subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var client = await ClientAsync(null, definition);
        await RequestAsync(client, definition, "a", options: For(subject, partition: "tenant-1"));

        await Assert.ThrowsAsync<JobPartitionMismatchException>(() => RequestAsync(client, definition, "b", options: For(subject, partition: "tenant-2")));
        await Assert.ThrowsAsync<JobPartitionMismatchException>(() => RequestAsync(client, definition, "b", options: For(subject, RequestMode.IfAbsent, "tenant-2")));
        Assert.Equal("a", Assert.Single(await ReadJobsAsync(definition)).Args);
    }

    [Fact]
    public async Task J25_IfAbsentLeavesAnOutstandingJobUntouched()
    {
        var definition = Definition(new JobPolicy { Attempts = 5, Backoff = Backoff.Constant(TimeSpan.FromMinutes(5)) }, subject: "feed");
        var waiting = Subject.Of("feed", "waiting");
        var running = Subject.Of("feed", "running");
        var release = new TaskCompletionSource();
        var worker = await WorkerAsync(definition, new JobScript
        {
            Run = async (args, _, cancellationToken) =>
            {
                if (args.Value == "waiting")
                {
                    return JobResult.Retry(TimeSpan.Zero, "not_yet");
                }
                await release.Task.WaitAsync(cancellationToken);
                return JobResult.Completed;
            },
        });
        var retried = await RequestAsync(worker, definition, "waiting", options: For(waiting));
        await EventuallyAsync(async () => (await ReadJobAsync(retried.Id)) is { Attempts: 1, Held: false }, TimeSpan.FromSeconds(10), "the first attempt fails and waits");
        var before = await ReadJobAsync(retried.Id);
        var held = await RequestAsync(worker, definition, "running", options: For(running));
        await EventuallyAsync(async () => (await ReadJobAsync(held.Id)).Held, TimeSpan.FromSeconds(10), "the second job runs");

        var onWaiting = await RequestAsync(worker, definition, "other", options: For(waiting, RequestMode.IfAbsent));
        var onRunning = await RequestAsync(worker, definition, "other", options: For(running, RequestMode.IfAbsent));
        Assert.Equal(new JobRequestResult(retried.Id, JobRequestOutcome.Existing), onWaiting);
        Assert.Equal(new JobRequestResult(held.Id, JobRequestOutcome.Existing), onRunning);
        var after = await ReadJobAsync(retried.Id);
        Assert.Equal(before with { DueIn = after.DueIn }, after);
        Assert.InRange(before.DueIn!.Value - after.DueIn!.Value, TimeSpan.Zero, TimeSpan.FromSeconds(5));
        Assert.Null((await ReadJobAsync(held.Id)).PendingArgs);

        release.SetResult();
        await EndedAsync(held.Id, TimeSpan.FromSeconds(10));
        var created = await RequestAsync(worker, definition, "new", options: For(running, RequestMode.IfAbsent));
        Assert.Equal(JobRequestOutcome.Created, created.Outcome);
        Assert.NotEqual(held.Id, created.Id);
    }

    [Fact]
    public async Task J19_FanOutCommitsAllOrNone()
    {
        const int count = 50_000;
        var definition = Definition(subject: "tenant");
        var client = await ClientAsync(null, definition);
        var items = Enumerable.Range(0, count).Select(i => new JobRequestItem<TestArgs>(new TestArgs("t" + i), For(Subject.Of("tenant", "t" + i)))).ToList();
        await using (var crashed = await BeginAsync())
        {
            await Jobs(client).RequestManyAsync(crashed.Work, definition, items, Schedule.After(TimeSpan.FromHours(1)), Cancel);
            await crashed.RollbackAsync(Cancel);
        }
        Assert.Empty(await ReadJobsAsync(definition));

        await using (var transaction = await BeginAsync())
        {
            var results = await Jobs(client).RequestManyAsync(transaction.Work, definition, items, Schedule.After(TimeSpan.FromHours(1)), Cancel);
            Assert.Equal(count, results.Select(r => r.Id).Distinct().Count());
            Assert.All(results, r => Assert.Equal(JobRequestOutcome.Created, r.Outcome));
            await transaction.CommitAsync(Cancel);
        }
        var jobs = await ReadJobsAsync(definition);
        Assert.Equal(count, jobs.Count);
        Assert.Equal(count, jobs.Select(j => j.Args).Distinct().Count());
    }

    [Fact]
    public async Task J19_FanOutMergesItemsWithOneIdentityInOrder()
    {
        var definition = Definition(subject: "tenant");
        var client = await ClientAsync(null, definition);
        var subject = Subject.Of("tenant", "same");
        await using var transaction = await BeginAsync();
        var results = await Jobs(client).RequestManyAsync(
            transaction.Work,
            definition,
            [new(new TestArgs("first"), For(subject)), new(new TestArgs("second"), For(subject)), new(new TestArgs("other"), For(Subject.Of("tenant", "other")))],
            Schedule.After(TimeSpan.FromHours(1)),
            Cancel);
        await transaction.CommitAsync(Cancel);

        Assert.Equal([JobRequestOutcome.Created, JobRequestOutcome.Merged, JobRequestOutcome.Created], results.Select(r => r.Outcome));
        Assert.Equal(results[0].Id, results[1].Id);
        Assert.Equal("second", (await ReadJobAsync(results[0].Id)).Args);
    }

    [Fact]
    public async Task J33_OperationsOnAnUnregisteredDefinitionAreRefused()
    {
        var registered = Definition(subject: "feed");
        var other = Definition(subject: "feed");
        var client = await ClientAsync(null, registered);
        await using var transaction = await BeginAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await Jobs(client).RequestAsync(transaction.Work, other, new TestArgs("x"), Schedule.Now, For(Subject.Of("feed", "x")), Cancel));
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await Jobs(client).CancelAsync(transaction.Work, other, Subject.Of("feed", "x"), Cancel));
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await Jobs(client).RequestAsync(transaction.Work, Definition(subject: "feed", key: registered.Key), new TestArgs("x"), Schedule.Now, For(Subject.Of("feed", "x")), Cancel));
    }

    [Fact]
    public async Task L30_AJobDueAfterAClaimsCutoffMakesTheNextDueTimeZero()
    {
        var definition = Definition(new JobPolicy { Backoff = Backoff.None });
        var client = await ClientAsync(null, definition);
        var job = await RequestAsync(client, definition, "x", Schedule.After(TimeSpan.FromSeconds(2)));
        var (store, source) = await JobSourceAsync(client, definition, TimeSpan.FromSeconds(30));

        var before = await store.Leases.ClaimDueAsync(source, LeaseCallbacks<JobId>.None, client.Owner, 10, Cancel);
        Assert.Empty(before.Claims);
        var next = await store.Leases.NextDueAsync(source, before.Cutoff, Cancel);
        Assert.NotNull(next);
        Assert.InRange(next.Value, TimeSpan.FromTicks(1), TimeSpan.FromSeconds(2));

        await EventuallyAsync(async () => await store.Leases.NextDueAsync(source, before.Cutoff, Cancel) == TimeSpan.Zero, TimeSpan.FromSeconds(5), "the job becomes due");
        var claim = Assert.Single((await store.Leases.ClaimDueAsync(source, LeaseCallbacks<JobId>.None, client.Owner, 10, Cancel)).Claims);
        Assert.Equal(job.Id, claim.Key);
    }

    [Fact]
    public async Task J2_AnAtScheduleKeepsItsInstantWhateverItsOffset()
    {
        var definition = Definition(new JobPolicy { Backoff = Backoff.None });
        var client = await ClientAsync(null, definition);
        var at = DateTimeOffset.UtcNow.AddMinutes(10);
        var utc = await RequestAsync(client, definition, "utc", Schedule.At(at));
        var east = await RequestAsync(client, definition, "east", Schedule.At(at.ToOffset(TimeSpan.FromHours(5.5))));
        var west = await RequestAsync(client, definition, "west", Schedule.At(at.ToOffset(TimeSpan.FromHours(-8))));

        var due = new List<TimeSpan>();
        foreach (var job in new[] { utc, east, west })
        {
            due.Add((await ReadJobAsync(job.Id)).DueIn!.Value);
        }
        Assert.InRange(due[0], TimeSpan.FromMinutes(9), TimeSpan.FromMinutes(10));
        Assert.All(due, d => Assert.InRange(d - due[0], TimeSpan.FromMilliseconds(-500), TimeSpan.FromMilliseconds(500)));
    }
}
