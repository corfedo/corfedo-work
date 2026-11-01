using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>Status by subject and progress of running attempts.</summary>
public abstract class JobStatusScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    [Fact]
    public async Task D5_APageOfSubjectsReadsItsStatusInOneCall()
    {
        var definition = Definition(new JobPolicy { Backoff = Backoff.None }, subject: "feed");
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reported = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var worker = await WorkerAsync(definition, new JobScript
        {
            Run = async (args, run, cancellationToken) =>
            {
                if (args.Value == "running")
                {
                    await run.ReportProgressAsync(40, "half", cancellationToken);
                    reported.TrySetResult();
                    await release.Task.WaitAsync(cancellationToken);
                }
                return args.Value == "fails" ? JobResult.Fail("broken") : JobResult.Completed;
            },
        });
        var scheduled = Enumerable.Range(0, 495).Select(i => Subject.Of("feed", $"s{i}")).ToArray();
        var ended = Subject.Of("feed", "ended");
        var running = Subject.Of("feed", "running");
        var recurring = Subject.Of("feed", "recurring");
        var paused = Subject.Of("feed", "paused");
        var unknown = Subject.Of("feed", "unknown");
        await InTransactionAsync(transaction => Jobs(worker).RequestManyAsync(
            transaction, definition, scheduled.Select(s => new JobRequestItem<TestArgs>(new TestArgs("later"), For(s))), Schedule.After(TimeSpan.FromHours(1)), Cancel));
        var endedJob = await RequestAsync(worker, definition, "fails", options: For(ended));
        await EndedAsync(endedJob.Id, TimeSpan.FromSeconds(10));
        var runningJob = await RequestAsync(worker, definition, "running", options: For(running));
        await reported.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);
        Assert.Equal(JobRequestOutcome.FollowUp, (await RequestAsync(worker, definition, "again", options: For(running))).Outcome);
        await ScheduleAsync(worker, definition, "recurring", Schedule.Every(TimeSpan.FromHours(1), Interval.FixedRate), Recurring(recurring));
        await InTransactionAsync(transaction => Jobs(worker).PauseAsync(transaction, definition, paused, Cancel));
        var pausedJob = await RequestAsync(worker, definition, "paused", options: For(paused));
        var now = await Provider.NowAsync(Cancel);

        var page = await Jobs(worker).GetManyAsync(definition, [.. scheduled, ended, running, recurring, paused, unknown], Cancel);

        Assert.Equal(499, page.Count);
        Assert.False(page.ContainsKey(unknown));
        Assert.All(scheduled, s =>
        {
            var status = page[s];
            Assert.Equal((JobStatusState.Scheduled, 0, (JobOutcome?)null, false, false), (status.State, status.Attempt, status.LastOutcome, status.Paused, status.FollowUpPending));
            Assert.InRange(status.DueAt!.Value, now.AddMinutes(59), now.AddMinutes(61));
        });
        var endedStatus = page[ended];
        Assert.Equal((JobStatusState.Idle, (JobId?)null, JobOutcome.Failed, "broken"), (endedStatus.State, endedStatus.Job, endedStatus.LastOutcome, endedStatus.LastReason));
        Assert.NotNull(endedStatus.LastEndedAt);
        var runningStatus = page[running];
        Assert.Equal((JobStatusState.Running, runningJob.Id, 1, new JobProgress(40, "half"), true), (runningStatus.State, runningStatus.Job, runningStatus.Attempt, runningStatus.Progress, runningStatus.FollowUpPending));
        Assert.InRange(runningStatus.RunningSince!.Value, now.AddSeconds(-10), now);
        Assert.Null(runningStatus.DueAt);
        Assert.InRange(page[recurring].NextOccurrence!.Value, now, now.AddHours(1));
        var pausedStatus = page[paused];
        Assert.Equal((JobStatusState.Scheduled, pausedJob.Id, true), (pausedStatus.State, pausedStatus.Job, pausedStatus.Paused));
        Assert.Equal(runningStatus, await Jobs(worker).GetAsync(definition, running, Cancel));
        Assert.Null(await Jobs(worker).GetAsync(definition, unknown, Cancel));

        release.SetResult();
        await EventuallyAsync(async () => (await Jobs(worker).GetAsync(definition, running, Cancel)) is { State: JobStatusState.Idle, LastOutcome: JobOutcome.Completed }, TimeSpan.FromSeconds(10), "the follow-up ends");
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await Jobs(worker).GetManyAsync(definition, Enumerable.Range(0, JobOperations.MaximumSubjects + 1).Select(i => Subject.Of("feed", $"x{i}")), Cancel));
    }

    [Fact]
    public async Task D5_TheStatusOfARecurringSubjectShowsItsOccurrenceAndLastOutcome()
    {
        var definition = Definition(new JobPolicy { Backoff = Backoff.None }, subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var worker = await WorkerAsync(definition, new JobScript());
        await ScheduleAsync(worker, definition, "x", Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate), Recurring(subject));

        JobStatus? status = null;
        await EventuallyAsync(async () => (status = await Jobs(worker).GetAsync(definition, subject, Cancel)) is { LastOutcome: JobOutcome.Completed }, TimeSpan.FromSeconds(10), "an occurrence ends");
        Assert.NotNull(status!.NextOccurrence);
        Assert.NotNull(status.LastEndedAt);
        await InTransactionAsync(transaction => Jobs(worker).PauseAsync(transaction, definition, Cancel));
        Assert.True((await Jobs(worker).GetAsync(definition, subject, Cancel))!.Paused);
    }

    [Fact]
    public async Task J28_ProgressIsFencedToItsAttemptAndClearedWhenTheNextStarts()
    {
        var definition = Definition(new JobPolicy { Attempts = 3, Backoff = Backoff.None }, subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var client = await ClientAsync(null, definition);
        var job = await RequestAsync(client, definition, "x", options: For(subject));
        var store = client.Services.GetRequiredService<IWorkStore>();
        var stale = await CrashClaimAsync(client, definition, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5));
        Assert.True(await store.Jobs.ReportProgressAsync(stale.Lease, new JobProgress(30, "first"), Cancel));
        Assert.Equal(new JobProgress(30, "first"), (await Jobs(client).GetAsync(definition, subject, Cancel))!.Progress);

        var second = new TaskCompletionSource<JobProgress?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var reported = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var refusals = new List<Type>();
        await WorkerAsync(definition, new JobScript
        {
            Run = async (_, run, cancellationToken) =>
            {
                second.TrySetResult((await Jobs(client).GetAsync(definition, subject, cancellationToken))!.Progress);
                foreach (var refused in new Func<Task>[]
                {
                    () => run.ReportProgressAsync(101, null, cancellationToken).AsTask(),
                    () => run.ReportProgressAsync(-1, null, cancellationToken).AsTask(),
                    () => run.ReportProgressAsync(50, new string('m', JobRun.MaximumProgressMessage + 1), cancellationToken).AsTask(),
                })
                {
                    refusals.Add((await Assert.ThrowsAnyAsync<ArgumentException>(refused)).GetType());
                }
                await run.ReportProgressAsync(60, "second", cancellationToken);
                reported.TrySetResult();
                await release.Task.WaitAsync(cancellationToken);
                return JobResult.Completed;
            },
        });

        Assert.Null(await second.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel));
        await reported.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);
        Assert.Equal(3, refusals.Count);
        Assert.False(await store.Jobs.ReportProgressAsync(stale.Lease, new JobProgress(90, "late"), Cancel));
        var status = await Jobs(client).GetAsync(definition, subject, Cancel);
        Assert.Equal((JobStatusState.Running, 2, new JobProgress(60, "second")), (status!.State, status.Attempt, status.Progress));
        release.SetResult();
        await EndedAsync(job.Id, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task D5_UnderOverlapAllowTheStatusDescribesTheRunningOccurrence()
    {
        var definition = Definition(new JobPolicy { Backoff = Backoff.None }, subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        // Capacity 1 and a first occurrence that runs until cancelled: later occurrences wait as jobs of their own.
        var worker = await WorkerAsync(definition, Occurrences(runs, async (_, _, cancellationToken) =>
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
            return JobResult.Completed;
        }), pollingInterval: TimeSpan.FromMilliseconds(250));
        await ScheduleAsync(worker, definition, "x", Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate, await Provider.NowAsync(Cancel)), Recurring(subject, Overlap.Allow));
        await EventuallyAsync(async () => (await ReadJobsAsync(definition)).Count(j => j.State is null) >= 3, TimeSpan.FromSeconds(10), "occurrences accumulate");

        var outstanding = (await ReadJobsAsync(definition)).Where(j => j.State is null).ToArray();
        var holder = Assert.Single(outstanding, j => j.Held);
        var status = await Jobs(worker).GetAsync(definition, subject, Cancel);
        Assert.NotNull(status);
        Assert.Equal((JobStatusState.Running, holder.Id, holder.Occurrence), (status.State, status.Job, status.Occurrence));
    }
}
