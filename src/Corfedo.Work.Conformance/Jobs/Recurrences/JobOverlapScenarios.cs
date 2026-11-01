using System.Collections.Concurrent;

namespace Corfedo.Work.Conformance;

/// <summary>
/// Overlap policies while an occurrence retries or runs long, triggering, execution identity of triggered occurrences
/// and outcome hooks of skipped occurrences.
/// </summary>
public abstract class JobOverlapScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    protected static readonly TimeSpan Polling = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// A recurrence every second whose first occurrence fails its first attempt and is retried 2.5 seconds later; it
    /// returns the runs once the first occurrence ended and the next 1.5 seconds passed.
    /// </summary>
    private async Task<(ObservedOccurrence[] Runs, IReadOnlyList<JobState> Jobs)> RetryingAsync(Overlap overlap)
    {
        var definition = Definition(new JobPolicy { Attempts = 3, Backoff = Backoff.None }, subject: "feed");
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        string? first = null;
        var worker = await WorkerAsync(
            definition,
            Occurrences(runs, (_, run, _) =>
            {
                first ??= run.Occurrence;
                return ValueTask.FromResult<JobDisposition>(run.Occurrence == first && run.Attempt == 1 ? JobResult.Retry(TimeSpan.FromSeconds(2.5), "flaky") : JobResult.Completed);
            }),
            pollingInterval: Polling);
        await ScheduleAsync(worker, definition, "x", Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate, await Provider.NowAsync(Cancel)), Recurring(Subject.Of("feed", "a"), overlap));
        await EventuallyAsync(() => runs.Any(r => r.Occurrence == first && r.Attempt == 2), TimeSpan.FromSeconds(15), "the first occurrence is retried");
        await Task.Delay(1500, Cancel);
        return ([.. runs.OrderBy(r => r.Started)], await ReadJobsAsync(definition));
    }

    [Fact]
    public async Task J14_WaitStartsTheNextOccurrenceAfterTheRetryingOneEnds()
    {
        var (runs, _) = await RetryingAsync(Overlap.Wait);

        var retried = runs.Single(r => r.Attempt == 2);
        Assert.All(runs.Where(r => r.Occurrence != retried.Occurrence), r => Assert.True(r.Started >= retried.Returned, $"{r.Occurrence} started while the first occurrence was retrying"));
        Assert.Contains(runs, r => r.Occurrence != retried.Occurrence);
    }

    [Fact]
    public async Task J14_SkipRecordsTheNextOccurrencesSkipped()
    {
        var (runs, jobs) = await RetryingAsync(Overlap.Skip);

        var retried = runs.Single(r => r.Attempt == 2);
        Assert.All(runs.Where(r => r.Occurrence != retried.Occurrence), r => Assert.True(r.Started >= retried.Returned, $"{r.Occurrence} started while the first occurrence was retrying"));
        Assert.Contains(jobs, j => (j.State, j.Reason) == (JobOutcome.Skipped, JobReasons.Overlap));
    }

    [Fact]
    public async Task J14_AllowRunsTheNextOccurrenceWhileTheFirstOneRetries()
    {
        var (runs, jobs) = await RetryingAsync(Overlap.Allow);

        var retried = runs.Single(r => r.Attempt == 2);
        Assert.Contains(runs, r => r.Occurrence != retried.Occurrence && r.Started < retried.Started);
        Assert.DoesNotContain(jobs, j => j.State == JobOutcome.Skipped);
    }

    [Fact]
    public async Task J15_TriggerStartsAnOccurrenceMakesARetryDueOrReportsARunningOne()
    {
        var definition = Definition(new JobPolicy { Attempts = 3 }, subject: "feed");
        var subject = Subject.Of("feed", "a");
        var release = new TaskCompletionSource();
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var worker = await WorkerAsync(definition, Occurrences(runs, async (_, run, cancellationToken) =>
        {
            if (run.Attempt > 1)
            {
                return JobResult.Completed;
            }
            await release.Task.WaitAsync(cancellationToken);
            return JobResult.Retry(TimeSpan.FromHours(1), "later");
        }), pollingInterval: Polling);
        await ScheduleAsync(worker, definition, "x", Schedule.Every(TimeSpan.FromHours(1), Interval.FixedRate, await Provider.NowAsync(Cancel)), Recurring(subject, Overlap.Skip));

        var started = await InTransactionAsync(t => Jobs(worker).TriggerAsync(t, definition, subject, Cancel));
        Assert.Equal(RunNowOutcome.Started, started.Outcome);
        Assert.StartsWith("trigger:", (await ReadJobAsync(started.Job)).Occurrence, StringComparison.Ordinal);
        await EventuallyAsync(async () => (await ReadJobAsync(started.Job)).Held, TimeSpan.FromSeconds(10), "the triggered occurrence runs");

        Assert.Equal(new RunNowResult(started.Job, RunNowOutcome.AlreadyRunning), await InTransactionAsync(t => Jobs(worker).TriggerAsync(t, definition, subject, Cancel)));
        release.SetResult();
        await EventuallyAsync(async () => (await ReadJobAsync(started.Job)) is { Held: false, Attempts: 1 }, TimeSpan.FromSeconds(10), "the occurrence waits for its retry");

        Assert.Equal(new RunNowResult(started.Job, RunNowOutcome.MadeDue), await InTransactionAsync(t => Jobs(worker).TriggerAsync(t, definition, subject, Cancel)));
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(started.Job, TimeSpan.FromSeconds(10))).State);
        await Task.Delay(1000, Cancel);
        Assert.Equal([1, 2], runs.Select(r => r.Attempt));
        Assert.Single(await ReadJobsAsync(definition));
    }

    [Fact]
    public async Task J16_TwoProcessesWithSkipNeverRunTwoOccurrencesAtOnce()
    {
        var definition = Definition(subject: "feed");
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var running = 0;
        var most = 0;
        var script = Occurrences(runs, async (_, _, cancellationToken) =>
        {
            var now = Interlocked.Increment(ref running);
            InterlockedMax(ref most, now);
            try
            {
                await Task.Delay(runs.IsEmpty ? 2500 : 300, cancellationToken);
            }
            finally
            {
                Interlocked.Decrement(ref running);
            }
            return JobResult.Completed;
        });
        var first = await WorkerAsync(definition, script, pollingInterval: Polling);
        await WorkerAsync(definition, script, pollingInterval: Polling);
        await ScheduleAsync(first, definition, "x", Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate, await Provider.NowAsync(Cancel)), Recurring(Subject.Of("feed", "a"), Overlap.Skip));

        await RunsAsync(runs, 3, TimeSpan.FromSeconds(15));
        Assert.Equal(1, most);
        await EventuallyAsync(async () => (await ReadJobsAsync(definition)).Any(j => (j.State, j.Reason) == (JobOutcome.Skipped, JobReasons.Overlap)), TimeSpan.FromSeconds(5), "occurrences due during the long one are skipped");
    }

    [Fact]
    public async Task J36_ATriggeredOccurrenceHasItsOwnExecutionKeptAcrossItsRetries()
    {
        var definition = Definition(new JobPolicy { Attempts = 3, Backoff = Backoff.None }, subject: "feed");
        var subject = Subject.Of("feed", "a");
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var worker = await WorkerAsync(definition, Occurrences(runs, (_, run, _) =>
            ValueTask.FromResult<JobDisposition>(run.Attempt == 1 ? JobResult.Retry(TimeSpan.Zero, "flaky") : JobResult.Completed)), pollingInterval: Polling);
        await ScheduleAsync(worker, definition, "x", Schedule.Every(TimeSpan.FromHours(1), Interval.FixedRate, await Provider.NowAsync(Cancel)), Recurring(subject));

        var first = await InTransactionAsync(t => Jobs(worker).TriggerAsync(t, definition, subject, Cancel));
        await EndedAsync(first.Job, TimeSpan.FromSeconds(10));
        var second = await InTransactionAsync(t => Jobs(worker).TriggerAsync(t, definition, subject, Cancel));
        await EndedAsync(second.Job, TimeSpan.FromSeconds(10));

        var observed = await RunsAsync(runs, 4, TimeSpan.FromSeconds(5));
        Assert.NotEqual(first.Job, second.Job);
        Assert.Equal([1, 2, 1, 2], observed.Select(r => r.Attempt));
        Assert.Equal(observed[0].ExecutionId, observed[1].ExecutionId);
        Assert.Equal(observed[2].ExecutionId, observed[3].ExecutionId);
        Assert.NotEqual(observed[0].ExecutionId, observed[2].ExecutionId);
    }

    [Fact]
    public async Task D4_ASkippedOccurrenceEndsThroughTheOutcomeHookInItsTransaction()
    {
        var definition = Definition(subject: "feed");
        var journal = await Provider.CreateTableAsync([""], Cancel);
        var ended = new EndedScript
        {
            Ended = (end, transaction, cancellationToken) => end.Outcome == JobOutcome.Skipped
                ? new ValueTask(Provider.JournalAsync(transaction, journal, $"{end.Job}:{end.Reason}", cancellationToken))
                : ValueTask.CompletedTask,
        };
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var worker = await WorkerAsync(definition, Occurrences(runs, async (_, _, cancellationToken) =>
        {
            await Task.Delay(runs.IsEmpty ? 2500 : 0, cancellationToken);
            return JobResult.Completed;
        }), ended, pollingInterval: Polling);
        await ScheduleAsync(worker, definition, "x", Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate, await Provider.NowAsync(Cancel)), Recurring(Subject.Of("feed", "a"), Overlap.Skip));
        await RunsAsync(runs, 2, TimeSpan.FromSeconds(15));

        var skipped = (await ReadJobsAsync(definition)).Where(j => j.State == JobOutcome.Skipped).ToArray();
        Assert.NotEmpty(skipped);
        Assert.All(skipped, j => Assert.Equal(JobReasons.Overlap, j.Reason));
        Assert.Equal(skipped.Select(j => $"{j.Id}:{JobReasons.Overlap}").Order(), (await Provider.ReadJournalAsync(journal, Cancel)).Where(e => skipped.Any(j => e.StartsWith(j.Id.ToString(), StringComparison.Ordinal))).Order());
        Assert.Equal(skipped.Length, ended.Calls.Count(c => c.Outcome == JobOutcome.Skipped));
    }

    [Fact]
    public async Task D4_ASkippedOccurrenceWhoseHookFailsStaysRecordedAndAnotherProcessFinalizesIt()
    {
        var definition = Definition(subject: "feed");
        var journal = await Provider.CreateTableAsync([""], Cancel);
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
        Assert.Equal((true, (JobOutcome?)null), ((await ReadJobAsync(recorded!.Id)).SkipPending, (await ReadJobAsync(recorded.Id)).State));

        release.SetResult();
        await worker.DisposeAsync();
        var working = new EndedScript { Ended = (end, transaction, cancellationToken) => new ValueTask(Provider.JournalAsync(transaction, journal, end.Job.ToString(), cancellationToken)) };
        await WorkerAsync(definition, Occurrences(new ConcurrentQueue<ObservedOccurrence>()), working, pollingInterval: Polling);

        var finalized = await EndedAsync(recorded.Id, TimeSpan.FromSeconds(10));
        Assert.Equal((JobOutcome.Skipped, JobReasons.Overlap, false), (finalized.State, finalized.Reason, finalized.SkipPending));
        Assert.Contains(recorded.Id.ToString(), await Provider.ReadJournalAsync(journal, Cancel));
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
