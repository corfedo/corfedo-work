using System.Collections.Concurrent;

namespace Corfedo.Work.Conformance;

/// <summary>
/// Standalone jobs and recurrences as separate lifecycles: requests never join a recurrence's occurrences, and work
/// that must coalesce goes through a dispatcher recurrence that requests standalone jobs.
/// </summary>
public abstract class JobLifecycleScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    protected static readonly TimeSpan Polling = TimeSpan.FromMilliseconds(250);

    /// <summary>A fixed-rate schedule whose next instant is an hour away.</summary>
    private async Task<Schedule> HourlyAsync() => Schedule.Every(TimeSpan.FromHours(1), Interval.FixedRate, await Provider.NowAsync(Cancel));

    [Fact]
    public async Task J37_RequestsWithARecurrencesIdentityCreateAndMergeStandaloneJobsOnly()
    {
        var definition = Definition(subject: "feed");
        var subject = Subject.Of("feed", "a");
        var client = await ClientAsync(null, definition);
        await ScheduleAsync(client, definition, "scheduled", await HourlyAsync(), Recurring(subject));
        var triggered = await InTransactionAsync(t => Jobs(client).TriggerAsync(t, definition, subject, Cancel));

        var absent = await RequestAsync(client, definition, "first", Schedule.After(TimeSpan.FromHours(1)), For(subject, RequestMode.IfAbsent));
        var merged = await RequestAsync(client, definition, "second", Schedule.After(TimeSpan.FromHours(1)), For(subject));
        var existing = await RequestAsync(client, definition, "third", options: For(subject, RequestMode.IfAbsent));

        Assert.Equal(JobRequestOutcome.Created, absent.Outcome);
        Assert.NotEqual(triggered.Job, absent.Id);
        Assert.Equal(new JobRequestResult(absent.Id, JobRequestOutcome.Merged), merged);
        Assert.Equal(new JobRequestResult(absent.Id, JobRequestOutcome.Existing), existing);
        var occurrence = await ReadJobAsync(triggered.Job);
        Assert.Equal(("scheduled", false), (occurrence.Args, occurrence.HoldsIdentity));
        var job = await ReadJobAsync(absent.Id);
        Assert.Equal(("second", true, (string?)null), (job.Args, job.HoldsIdentity, job.Occurrence));

        Assert.Equal(RecurrenceOutcome.Replaced, await ScheduleAsync(client, definition, "replaced", await HourlyAsync(), Recurring(subject)));
        var standalone = await ReadJobAsync(absent.Id);
        Assert.Equal(("second", (JobOutcome?)null, (string?)null), (standalone.Args, standalone.State, standalone.Occurrence));
        Assert.Equal(2, (await ReadJobsAsync(definition)).Count(j => j.State is null));

        Assert.Equal(2, await InTransactionAsync(t => Jobs(client).CancelAsync(t, definition, subject, Cancel)));
        Assert.All(await ReadJobsAsync(definition), j => Assert.Equal(JobOutcome.Cancelled, j.State));
    }

    [Fact]
    public async Task J37_AStandaloneJobRunsBesideARunningOccurrenceOfItsSubject()
    {
        var definition = Definition(subject: "feed");
        var subject = Subject.Of("feed", "a");
        var release = new TaskCompletionSource();
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var worker = await WorkerAsync(definition, Occurrences(runs, async (_, run, cancellationToken) =>
        {
            if (run.Occurrence is not null)
            {
                await release.Task.WaitAsync(cancellationToken);
            }
            return JobResult.Completed;
        }), configure: j => j.Capacity(2), pollingInterval: Polling);
        await ScheduleAsync(worker, definition, "scheduled", await HourlyAsync(), Recurring(subject));
        var triggered = await InTransactionAsync(t => Jobs(worker).TriggerAsync(t, definition, subject, Cancel));
        await EventuallyAsync(async () => (await ReadJobAsync(triggered.Job)).Held, TimeSpan.FromSeconds(10), "the triggered occurrence runs");

        var requested = await RequestAsync(worker, definition, "requested", options: For(subject));

        Assert.Equal(JobRequestOutcome.Created, requested.Outcome);
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(requested.Id, TimeSpan.FromSeconds(10))).State);
        Assert.Null((await ReadJobAsync(triggered.Job)).State);
        release.SetResult();
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(triggered.Job, TimeSpan.FromSeconds(10))).State);
    }

    [Fact]
    public async Task J40_ADispatcherAndManualRequestsOfASubjectNeverRunItTwiceAtOnce()
    {
        var dispatcher = Definition(subject: "tenant");
        var sync = Definition(subject: "repo");
        string[] repos = ["a", "b"];
        var running = new ConcurrentDictionary<string, int>();
        var most = 0;
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var syncWorker = await WorkerAsync(sync, Occurrences(runs, async (_, run, cancellationToken) =>
        {
            var now = running.AddOrUpdate(run.Subject!.Id, 1, (_, n) => n + 1);
            InterlockedMax(ref most, now);
            try
            {
                await Task.Delay(400, cancellationToken);
            }
            finally
            {
                running.AddOrUpdate(run.Subject!.Id, 0, (_, n) => n - 1);
            }
            return JobResult.Completed;
        }), configure: j => j.Capacity(4), pollingInterval: Polling);
        var dispatches = 0;
        var dispatcherWorker = await WorkerAsync(dispatcher, new JobScript
        {
            Run = async (_, _, cancellationToken) =>
            {
                await using var transaction = await Provider.BeginAsync(cancellationToken);
                await Jobs(syncWorker).RequestManyAsync(
                    transaction.Work, sync, repos.Select(r => new JobRequestItem<TestArgs>(new TestArgs("scheduled"), For(Subject.Of("repo", r)))), Schedule.Now, cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                Interlocked.Increment(ref dispatches);
                return JobResult.Completed;
            },
        }, pollingInterval: Polling);
        await ScheduleAsync(dispatcherWorker, dispatcher, "dispatch", Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate, await Provider.NowAsync(Cancel)), Recurring(Subject.Of("tenant", "t")));

        var outcomes = new List<JobRequestOutcome>();
        for (var i = 0; i < 12; i++)
        {
            foreach (var repo in repos)
            {
                outcomes.Add((await RequestAsync(syncWorker, sync, "manual", options: For(Subject.Of("repo", repo)))).Outcome);
            }
            await Task.Delay(250, Cancel);
        }
        await EventuallyAsync(() => Volatile.Read(ref dispatches) >= 3, TimeSpan.FromSeconds(10), "the dispatcher runs");
        await EventuallyAsync(async () => (await ReadJobsAsync(sync)).All(j => j.State is not null), TimeSpan.FromSeconds(15), "requested syncs end");

        Assert.Equal(1, most);
        Assert.Contains(JobRequestOutcome.FollowUp, outcomes);
        Assert.True(runs.Count >= repos.Length, $"{runs.Count} runs");
        Assert.All(await ReadJobsAsync(sync), j => Assert.True(j.HoldsIdentity && j.Occurrence is null));
    }

    [Fact]
    public async Task J40_ATriggerDuringARunUnderWaitQueuesOneRunAfterIt()
    {
        var definition = Definition(subject: "feed");
        var subject = Subject.Of("feed", "a");
        var release = new TaskCompletionSource();
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var worker = await WorkerAsync(definition, Occurrences(runs, async (_, _, cancellationToken) =>
        {
            if (runs.IsEmpty)
            {
                await release.Task.WaitAsync(cancellationToken);
            }
            return JobResult.Completed;
        }), configure: j => j.Capacity(2), pollingInterval: Polling);
        await ScheduleAsync(worker, definition, "x", await HourlyAsync(), Recurring(subject, Overlap.Wait));
        var started = await InTransactionAsync(t => Jobs(worker).TriggerAsync(t, definition, subject, Cancel));
        await EventuallyAsync(async () => (await ReadJobAsync(started.Job)).Held, TimeSpan.FromSeconds(10), "the triggered occurrence runs");

        Assert.Equal(new RunNowResult(started.Job, RunNowOutcome.Waiting), await InTransactionAsync(t => Jobs(worker).TriggerAsync(t, definition, subject, Cancel)));
        Assert.Equal(new RunNowResult(started.Job, RunNowOutcome.Waiting), await InTransactionAsync(t => Jobs(worker).TriggerAsync(t, definition, subject, Cancel)));
        await Task.Delay(1000, Cancel);
        Assert.Single(await ReadJobsAsync(definition));

        release.SetResult();
        var observed = await RunsAsync(runs, 2, TimeSpan.FromSeconds(10));
        Assert.Equal(started.Job, observed[0].Job);
        Assert.NotEqual(started.Job, observed[1].Job);
        Assert.True(observed[1].Started >= observed[0].Returned);
        await Task.Delay(1500, Cancel);
        Assert.Equal(2, runs.Count);
    }

    private static void InterlockedMax(ref int target, int value)
    {
        var current = Volatile.Read(ref target);
        while (value > current)
        {
            var seen = Interlocked.CompareExchange(ref target, value, current);
            if (seen == current)
            {
                return;
            }
            current = seen;
        }
    }
}
