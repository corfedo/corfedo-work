using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>Operator actions on jobs by id: run now and reruns (J21).</summary>
public abstract class JobActionScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    protected static readonly TimeSpan Polling = TimeSpan.FromMilliseconds(250);

    private static readonly Schedule Later = Schedule.After(TimeSpan.FromHours(1));

    [Fact]
    public async Task J30_RunNowMakesAScheduledJobDueAndReportsItAlreadyDueAfterwards()
    {
        var definition = Definition();
        var client = await ClientAsync(null, definition);
        var job = await RequestAsync(client, definition, "x", Later);

        Assert.Equal(new RunNowResult(job.Id, RunNowOutcome.MadeDue), await Jobs(client).RunNowAsync(definition, job.Id, Cancel));
        var state = await ReadJobAsync(job.Id);
        Assert.True(state.DueIn <= TimeSpan.Zero);
        Assert.Equal(new RunNowResult(job.Id, RunNowOutcome.AlreadyDue), await Jobs(client).RunNowAsync(definition, job.Id, Cancel));
        Assert.Equal((0, state.ExecutionId), ((await ReadJobAsync(job.Id)).Attempts, (await ReadJobAsync(job.Id)).ExecutionId));

        var attempts = new ConcurrentQueue<ObservedAttempt>();
        await WorkerAsync(definition, Recording(attempts));
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(job.Id, TimeSpan.FromSeconds(10))).State);
    }

    [Fact]
    public async Task J30_RunNowEndsARetryBackoffWithTheAttemptsKept()
    {
        var definition = Definition(new JobPolicy { Attempts = 3, Backoff = Backoff.None });
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        var worker = await WorkerAsync(definition, Recording(attempts, (_, run, _) => ValueTask.FromResult<JobDisposition>(
            run.Attempt == 1 ? JobResult.Retry(TimeSpan.FromHours(1), "busy") : JobResult.Completed)), pollingInterval: Polling);
        var job = await RequestAsync(worker, definition, "x");
        await EventuallyAsync(async () => (await ReadJobAsync(job.Id)) is { Attempts: 1, Held: false, DueIn: { } due } && due > TimeSpan.Zero, TimeSpan.FromSeconds(10), "the job waits for its retry");

        Assert.Equal(new RunNowResult(job.Id, RunNowOutcome.MadeDue), await Jobs(worker).RunNowAsync(definition, job.Id, Cancel));

        Assert.Equal(JobOutcome.Completed, (await EndedAsync(job.Id, TimeSpan.FromSeconds(10))).State);
        Assert.Equal([1, 2], attempts.Select(a => a.Attempt));
        Assert.Single(attempts.Select(a => a.ExecutionId).Distinct());
    }

    [Fact]
    public async Task J30_RunNowOnARunningJobQueuesNothing()
    {
        var definition = Definition(subject: "feed");
        var subject = Subject.Of("feed", "a");
        var release = new TaskCompletionSource();
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        var worker = await WorkerAsync(definition, Recording(attempts, async (_, _, cancellationToken) =>
        {
            await release.Task.WaitAsync(cancellationToken);
            return JobResult.Completed;
        }));
        var job = await RequestAsync(worker, definition, "x", options: For(subject));
        await EventuallyAsync(() => attempts.Count == 1, TimeSpan.FromSeconds(10), "the job runs");

        Assert.Equal(new RunNowResult(job.Id, RunNowOutcome.AlreadyRunning), await Jobs(worker).RunNowAsync(definition, job.Id, Cancel));
        Assert.Null((await ReadJobAsync(job.Id)).PendingArgs);

        release.SetResult();
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(job.Id, TimeSpan.FromSeconds(10))).State);
        await Task.Delay(500, Cancel);
        Assert.Single(attempts);
    }

    [Fact]
    public async Task J30_RunNowOnAPausedJobMakesItDueToRunOnceResumed()
    {
        var definition = Definition(subject: "feed");
        var subject = Subject.Of("feed", "a");
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        var worker = await WorkerAsync(definition, Recording(attempts), pollingInterval: Polling);
        await InTransactionAsync(t => Jobs(worker).PauseAsync(t, definition, subject, Cancel));
        var job = await RequestAsync(worker, definition, "x", Later, For(subject));

        Assert.Equal(new RunNowResult(job.Id, RunNowOutcome.Paused), await Jobs(worker).RunNowAsync(definition, job.Id, Cancel));
        Assert.True((await ReadJobAsync(job.Id)).DueIn <= TimeSpan.Zero);
        await Task.Delay(500, Cancel);
        Assert.Empty(attempts);

        await InTransactionAsync(t => Jobs(worker).ResumeAsync(t, definition, subject, Cancel));
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(job.Id, TimeSpan.FromSeconds(10))).State);

        var other = await RequestAsync(worker, definition, "y", Later, For(Subject.Of("feed", "b")));
        await InTransactionAsync(t => Jobs(worker).PauseAsync(t, definition, Cancel));
        Assert.Equal(new RunNowResult(other.Id, RunNowOutcome.Paused), await Jobs(worker).RunNowAsync(definition, other.Id, Cancel));
    }

    [Fact]
    public async Task J30_RunNowReportsEndedAndUnknownJobs()
    {
        var definition = Definition();
        var worker = await WorkerAsync(definition, new JobScript());
        var job = await RequestAsync(worker, definition, "x");
        var ended = await EndedAsync(job.Id, TimeSpan.FromSeconds(10));
        var unknown = JobId.New();

        Assert.Equal(new RunNowResult(job.Id, RunNowOutcome.Ended), await Jobs(worker).RunNowAsync(definition, job.Id, Cancel));
        Assert.Equal(new RunNowResult(unknown, RunNowOutcome.NotFound), await Jobs(worker).RunNowAsync(definition, unknown, Cancel));
        Assert.Equal(ended, await ReadJobAsync(job.Id) with { DueIn = ended.DueIn });
    }

    [Fact]
    public async Task J30_RunNowOnARecurringJobTriggersItOrReportsItsOccurrence()
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
        await ScheduleAsync(worker, definition, "scheduled", Schedule.Every(TimeSpan.FromHours(1), Interval.FixedRate, await Provider.NowAsync(Cancel)), Recurring(subject));
        var triggered = await InTransactionAsync(t => Jobs(worker).TriggerAsync(t, definition, subject, Cancel));
        await EventuallyAsync(async () => (await ReadJobAsync(triggered.Job)).Started, TimeSpan.FromSeconds(10), "the triggered occurrence runs");

        Assert.Equal(new RunNowResult(triggered.Job, RunNowOutcome.AlreadyRunning), await Jobs(worker).RunNowAsync(definition, triggered.Job, Cancel));

        release.SetResult();
        await EndedAsync(triggered.Job, TimeSpan.FromSeconds(10));
        var again = await Jobs(worker).RunNowAsync(definition, triggered.Job, Cancel);
        Assert.Equal(RunNowOutcome.Started, again.Outcome);
        Assert.NotEqual(triggered.Job, again.Job);
        var observed = await RunsAsync(runs, 2, TimeSpan.FromSeconds(10));
        Assert.Equal(again.Job, observed[1].Job);
        Assert.StartsWith("trigger:", observed[1].Occurrence, StringComparison.Ordinal);
    }

    [Fact]
    public async Task J30_RunNowOnAnOccurrenceWaitingToBeRecordedSkippedTriggersNothing()
    {
        var definition = Definition(subject: "feed");
        var failing = new EndedScript { Ended = (end, _, _) => end.Outcome == JobOutcome.Skipped ? throw new InvalidOperationException("hook down") : ValueTask.CompletedTask };
        var release = new TaskCompletionSource();
        var worker = await WorkerAsync(definition, Occurrences(new ConcurrentQueue<ObservedOccurrence>(), async (_, _, cancellationToken) =>
        {
            await release.Task.WaitAsync(cancellationToken);
            return JobResult.Completed;
        }), failing, pollingInterval: Polling);
        await ScheduleAsync(worker, definition, "x", Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate, await Provider.NowAsync(Cancel)), Recurring(Subject.Of("feed", "a"), Overlap.Skip));
        JobState? recorded = null;
        await EventuallyAsync(async () => (recorded = (await ReadJobsAsync(definition)).FirstOrDefault(j => j.SkipPending)) is not null, TimeSpan.FromSeconds(10), "an occurrence is recorded to be skipped");
        await EventuallyAsync(() => failing.Calls.Any(c => c.Job == recorded!.Id), TimeSpan.FromSeconds(5), "its hook fails");

        Assert.Equal(new RunNowResult(recorded!.Id, RunNowOutcome.Waiting), await Jobs(worker).RunNowAsync(definition, recorded.Id, Cancel));

        var jobs = await ReadJobsAsync(definition);
        Assert.DoesNotContain(jobs, j => j.Occurrence?.StartsWith("trigger:", StringComparison.Ordinal) == true);
        Assert.Equal((true, (JobOutcome?)null), (jobs.Single(j => j.Id == recorded.Id).SkipPending, jobs.Single(j => j.Id == recorded.Id).State));
        release.SetResult();
    }

    [Fact]
    public async Task J30_J36_D9_ARerunKeepsTheJobIdWithANewExecutionAndFreshAttempts()
    {
        var definition = Definition(new JobPolicy { Attempts = 2, Backoff = Backoff.None, RunHistory = true }, subject: "feed");
        var subject = Subject.Of("feed", "a");
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        var fail = true;
        var worker = await WorkerAsync(definition, Recording(attempts, (_, _, _) => ValueTask.FromResult<JobDisposition>(
            Volatile.Read(ref fail) ? JobResult.Retry(TimeSpan.Zero, "busy") : JobResult.Completed)));
        var job = await RequestAsync(worker, definition, "x", options: For(subject));
        Assert.Equal((JobOutcome.Failed, JobReasons.AttemptsExhausted), ((await EndedAsync(job.Id, TimeSpan.FromSeconds(10))).State, (await ReadJobAsync(job.Id)).Reason));
        var first = attempts.First().ExecutionId;
        Volatile.Write(ref fail, false);

        Assert.Equal(new RerunResult(job.Id, RerunOutcome.Rerun), await Jobs(worker).RerunAsync(definition, job.Id, Cancel));

        Assert.Equal(JobOutcome.Completed, (await EndedAsync(job.Id, TimeSpan.FromSeconds(10))).State);
        var all = attempts.ToArray();
        Assert.Equal([(1, first), (2, first)], all.Take(2).Select(a => (a.Attempt, a.ExecutionId)));
        Assert.Equal(1, all[2].Attempt);
        Assert.Equal(job.Id, all[2].Job);
        Assert.NotEqual(first, all[2].ExecutionId);
        Assert.Equal(3, all.Select(a => a.AttemptId).Distinct().Count());
        var outcome = await Jobs(worker).GetOutcomeAsync(definition, job.Id, Cancel);
        Assert.Equal((true, all[2].ExecutionId, JobOutcome.Completed), (outcome!.Ended, outcome.ExecutionId, outcome.Outcome));

        var runs = (await Jobs(worker).GetRunsAsync(definition, job.Id, cancellationToken: Cancel)).Runs;
        Assert.Equal(
            [(1, all[2].ExecutionId, JobRunOutcome.Completed), (2, first, JobRunOutcome.Failed), (1, first, JobRunOutcome.Retried)],
            runs.Select(r => (r.Attempt, r.ExecutionId, r.Outcome)));
        Assert.Equal(3, runs.Select(r => r.Epoch).Distinct().Count());
        Assert.True(runs[0].Epoch > runs[1].Epoch);
    }

    [Fact]
    public async Task J30_ARerunKeepsTheEndedOutcomeReadableUntilItsExecutionEnds()
    {
        var definition = Definition();
        var client = await ClientAsync(null, definition);
        var job = await RequestAsync(client, definition, "x");
        var worker = await WorkerAsync(definition, new JobScript());
        await EndedAsync(job.Id, TimeSpan.FromSeconds(10));
        await worker.StopAsync(Cancel);
        var ended = (await Jobs(client).GetOutcomeAsync(definition, job.Id, Cancel))!;

        Assert.Equal(RerunOutcome.Rerun, (await Jobs(client).RerunAsync(definition, job.Id, Cancel)).Outcome);

        var rerun = await ReadJobAsync(job.Id);
        Assert.Equal((null, 0, false), (rerun.State, rerun.Attempts, rerun.Started));
        Assert.True(rerun.DueIn <= TimeSpan.Zero);
        Assert.NotEqual(ended.ExecutionId, rerun.ExecutionId);
        Assert.Equal(ended with { Ended = false }, await Jobs(client).GetOutcomeAsync(definition, job.Id, Cancel));

        var waiting = Jobs(client).WaitAsync(definition, job.Id, TimeSpan.FromSeconds(20), Cancel).AsTask();
        await Task.Delay(300, Cancel);
        await WorkerAsync(definition, new JobScript());
        var outcome = await waiting;
        Assert.Equal((true, rerun.ExecutionId, JobOutcome.Completed), (outcome!.Ended, outcome.ExecutionId, outcome.Outcome));
    }

    [Fact]
    public async Task J30_ARerunIsRefusedWhileTheJobOrItsIdentityIsOutstanding()
    {
        var definition = Definition(subject: "feed");
        var subject = Subject.Of("feed", "a");
        var release = new TaskCompletionSource();
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        var worker = await WorkerAsync(definition, Recording(attempts, async (args, _, cancellationToken) =>
        {
            if (args.Value == "running")
            {
                await release.Task.WaitAsync(cancellationToken);
            }
            return JobResult.Completed;
        }));
        var scheduled = await RequestAsync(worker, definition, "scheduled", Later, For(Subject.Of("feed", "s")));
        var running = await RequestAsync(worker, definition, "running", options: For(subject));
        await EventuallyAsync(() => attempts.Count == 1, TimeSpan.FromSeconds(10), "the job runs");

        Assert.Equal(new RerunResult(scheduled.Id, RerunOutcome.Outstanding), await Jobs(worker).RerunAsync(definition, scheduled.Id, Cancel));
        Assert.Equal(new RerunResult(running.Id, RerunOutcome.Outstanding), await Jobs(worker).RerunAsync(definition, running.Id, Cancel));

        release.SetResult();
        await EndedAsync(running.Id, TimeSpan.FromSeconds(10));
        var holder = await RequestAsync(worker, definition, "holder", Later, For(subject));
        Assert.NotEqual(running.Id, holder.Id);
        var before = await ReadJobAsync(running.Id);

        Assert.Equal(new RerunResult(running.Id, RerunOutcome.IdentityHeld), await Jobs(worker).RerunAsync(definition, running.Id, Cancel));
        Assert.Equal(before with { DueIn = null }, await ReadJobAsync(running.Id) with { DueIn = null });
        var unknown = JobId.New();
        Assert.Equal(new RerunResult(unknown, RerunOutcome.NotFound), await Jobs(worker).RerunAsync(definition, unknown, Cancel));
    }

    [Fact]
    public async Task J30_ARerunOfAnOccurrenceIsRefusedAndAStandaloneJobOfItsSubjectIsNot()
    {
        var definition = Definition(subject: "feed");
        var subject = Subject.Of("feed", "a");
        var worker = await WorkerAsync(definition, new JobScript(), pollingInterval: Polling);
        var oneShot = await RequestAsync(worker, definition, "x", options: For(subject));
        await EndedAsync(oneShot.Id, TimeSpan.FromSeconds(10));
        await ScheduleAsync(worker, definition, "scheduled", Schedule.Every(TimeSpan.FromHours(1), Interval.FixedRate, await Provider.NowAsync(Cancel)), Recurring(subject));
        var triggered = await InTransactionAsync(t => Jobs(worker).TriggerAsync(t, definition, subject, Cancel));
        await EndedAsync(triggered.Job, TimeSpan.FromSeconds(10));

        Assert.Equal(new RerunResult(triggered.Job, RerunOutcome.Recurring), await Jobs(worker).RerunAsync(definition, triggered.Job, Cancel));
        Assert.Equal(new RerunResult(oneShot.Id, RerunOutcome.Rerun), await Jobs(worker).RerunAsync(definition, oneShot.Id, Cancel));
    }

    [Fact]
    public async Task J30_RerunManyReportsEveryJobInOrderWithDuplicatesAndRefusals()
    {
        var definition = Definition();
        var client = await ClientAsync(null, definition);
        var worker = await WorkerAsync(definition, new JobScript());
        var a = await RequestAsync(client, definition, "a");
        var b = await RequestAsync(client, definition, "b");
        await EndedAsync(a.Id, TimeSpan.FromSeconds(10));
        await EndedAsync(b.Id, TimeSpan.FromSeconds(10));
        await worker.StopAsync(Cancel);
        var outstanding = await RequestAsync(client, definition, "c", Later);
        var unknown = JobId.New();

        var results = await Jobs(client).RerunManyAsync(definition, [a.Id, outstanding.Id, a.Id, unknown, b.Id], Cancel);

        Assert.Equal(
            [
                new RerunResult(a.Id, RerunOutcome.Rerun),
                new RerunResult(outstanding.Id, RerunOutcome.Outstanding),
                new RerunResult(a.Id, RerunOutcome.Duplicate),
                new RerunResult(unknown, RerunOutcome.NotFound),
                new RerunResult(b.Id, RerunOutcome.Rerun),
            ],
            results);
        foreach (var id in new[] { a.Id, b.Id })
        {
            Assert.Null((await ReadJobAsync(id)).State);
        }
    }

    [Fact]
    public async Task D4_ARerunCancelledByAnOperatorRunsTheHookOnceWithItsExecution()
    {
        var definition = Definition(subject: "feed");
        var subject = Subject.Of("feed", "a");
        var ended = new EndedScript();
        var client = await ClientAsync(ended, definition);
        var job = await RequestAsync(client, definition, "x", options: For(subject));
        await InTransactionAsync(t => Jobs(client).CancelAsync(t, definition, subject, Cancel));
        Assert.Single(ended.Calls);

        Assert.Equal(RerunOutcome.Rerun, (await Jobs(client).RerunAsync(definition, job.Id, Cancel)).Outcome);
        var rerun = await ReadJobAsync(job.Id);
        Assert.Single(ended.Calls);
        Assert.Equal(1, await InTransactionAsync(t => Jobs(client).CancelAsync(t, definition, subject, Cancel)));

        Assert.Equal(2, ended.Calls.Count);
        var end = ended.Calls.Last();
        Assert.Equal((job.Id, rerun.ExecutionId, JobOutcome.Cancelled), (end.Job, end.ExecutionId, end.Outcome));
    }

    [Fact]
    public async Task J30_ARerunNeverResetsLeaseEpochsSoOldLeasesStayInvalid()
    {
        var definition = Definition(new JobPolicy { Attempts = 3, Backoff = Backoff.None, RunHistory = true }, subject: "feed");
        var subject = Subject.Of("feed", "a");
        var client = await ClientAsync(null, definition);
        var job = await RequestAsync(client, definition, "x", options: For(subject));
        var crashed = await CrashClaimAsync(client, definition, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5));
        var worker = await WorkerAsync(definition, new JobScript());
        await EndedAsync(job.Id, TimeSpan.FromSeconds(10));
        await worker.StopAsync(Cancel);

        Assert.Equal(RerunOutcome.Rerun, (await Jobs(client).RerunAsync(definition, job.Id, Cancel)).Outcome);
        var store = client.Services.GetRequiredService<IWorkStore>();
        Assert.False((await store.Leases.RenewAsync(crashed.Lease, Cancel)).Valid);
        var again = await CrashClaimAsync(client, definition, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(5));
        Assert.True(again.Lease.Epoch > crashed.Lease.Epoch + 1);
        Assert.False((await store.Leases.RenewAsync(crashed.Lease, Cancel)).Valid);
        Assert.True((await store.Leases.RenewAsync(again.Lease, Cancel)).Valid);

        var runs = (await Jobs(client).GetRunsAsync(definition, job.Id, cancellationToken: Cancel)).Runs;
        Assert.Equal(runs.Count, runs.Select(r => r.Epoch).Distinct().Count());
        Assert.Equal([JobRunOutcome.Completed, JobRunOutcome.LeaseLost], runs.Select(r => r.Outcome));
    }
}
