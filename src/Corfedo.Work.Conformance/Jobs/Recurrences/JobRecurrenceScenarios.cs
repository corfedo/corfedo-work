using System.Collections.Concurrent;

namespace Corfedo.Work.Conformance;

/// <summary>
/// Recurring schedules, the missed rule, attempts within an occurrence, cancellation of the current occurrence,
/// code-defined recurrences by revision and schedule replacement. Minutes and days are scaled to seconds.
/// </summary>
public abstract class JobRecurrenceScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    protected static readonly TimeSpan Polling = TimeSpan.FromMilliseconds(250);

    /// <summary>The scenario clock and the store clock run at one rate; this absorbs scheduling jitter only.</summary>
    protected static readonly TimeSpan Jitter = TimeSpan.FromMilliseconds(50);

    [Fact]
    public async Task J3_AfterCompletionKeepsTheIntervalBetweenAnEndAndTheNextStart()
    {
        var definition = Definition(subject: "feed");
        var lengths = new[] { 700, 50, 400 };
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var worker = await WorkerAsync(
            definition,
            Occurrences(runs, async (_, _, cancellationToken) =>
            {
                await Task.Delay(lengths[Math.Min(runs.Count, lengths.Length - 1)], cancellationToken);
                return JobResult.Completed;
            }),
            pollingInterval: Polling);

        Assert.Equal(RecurrenceOutcome.Created, await ScheduleAsync(worker, definition, "x", Schedule.Every(TimeSpan.FromSeconds(1), Interval.AfterCompletion), Recurring(Subject.Of("feed", "a"))));

        var observed = await RunsAsync(runs, 4, TimeSpan.FromSeconds(15));
        for (var i = 1; i < observed.Length; i++)
        {
            Assert.True(observed[i].Started - observed[i - 1].Returned >= TimeSpan.FromSeconds(1) - Jitter, $"run {i} started {observed[i].Started - observed[i - 1].Returned} after the previous one ended");
        }
        Assert.Equal(observed.Length, observed.Select(r => r.ExecutionId).Distinct().Count());
        Assert.All(observed, r => Assert.NotNull(r.Occurrence));
    }

    [Fact]
    public async Task J3_AnAfterCompletionInstantDueWhileATriggeredOccurrenceRunsWaitsForItsEnd()
    {
        var definition = Definition(subject: "feed");
        var subject = Subject.Of("feed", "a");
        var running = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var worker = await WorkerAsync(
            definition,
            Occurrences(runs, async (_, run, cancellationToken) =>
            {
                if (run.Occurrence!.StartsWith("trigger:", StringComparison.Ordinal))
                {
                    running.TrySetResult();
                    await release.Task.WaitAsync(cancellationToken);
                }
                return JobResult.Completed;
            }),
            pollingInterval: TimeSpan.FromSeconds(30));
        await ScheduleAsync(worker, definition, "x", Schedule.Every(TimeSpan.FromSeconds(1), Interval.AfterCompletion), Recurring(subject));
        await EndedAsync((await RunsAsync(runs, 1, TimeSpan.FromSeconds(10)))[0].Job, TimeSpan.FromSeconds(10));

        var triggered = await InTransactionAsync(t => Jobs(worker).TriggerAsync(t, definition, subject, Cancel));
        await running.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);
        await Task.Delay(1500, Cancel);

        // A pass that waits commits a few transactions per polling interval; one that repeats at once commits thousands.
        var before = await Provider.CommittedTransactionsAsync(Cancel);
        await Task.Delay(3000, Cancel);
        var committed = await Provider.CommittedTransactionsAsync(Cancel) - before;
        Assert.True(committed < 500, $"{committed} transactions committed in 3 s");
        Assert.Single(runs);

        // The polling interval is longer than the wait: the end of the triggered occurrence signals planning.
        release.SetResult();
        var observed = await RunsAsync(runs, 3, TimeSpan.FromSeconds(10));
        Assert.Equal(triggered.Job, observed[1].Job);
        Assert.False(observed[2].Occurrence!.StartsWith("trigger:", StringComparison.Ordinal));
    }

    [Fact]
    public async Task J4_FixedRateStartsStayOnTheGridAndOccurrencesDueDuringASlowOneCollapse()
    {
        var definition = Definition(subject: "feed");
        var anchor = await Provider.NowAsync(Cancel);
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var worker = await WorkerAsync(
            definition,
            Occurrences(runs, async (_, _, cancellationToken) =>
            {
                if (runs.IsEmpty)
                {
                    await Task.Delay(2600, cancellationToken);
                }
                return JobResult.Completed;
            }),
            pollingInterval: Polling);
        await ScheduleAsync(worker, definition, "x", Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate, anchor), Recurring(Subject.Of("feed", "a")));

        var observed = await RunsAsync(runs, 4, TimeSpan.FromSeconds(15));
        Assert.All(observed, r => Assert.Equal(0, (r.Instant - anchor).Ticks % TimeSpan.TicksPerSecond));
        var slow = observed[0];
        var collapsed = observed[1];
        // The instants due while the slow one ran collapse into the latest, which starts as soon as it ends.
        Assert.True(collapsed.Started - slow.Returned < TimeSpan.FromSeconds(0.9), "the collapsed occurrence starts when the slow one ends");
        Assert.True(collapsed.Instant >= slow.Instant + TimeSpan.FromSeconds(2), "the collapsed occurrence is the latest instant due during the slow one");
        Assert.Equal(collapsed.Instant + TimeSpan.FromSeconds(1), observed[2].Instant);
        var jobs = await ReadJobsAsync(definition);
        Assert.DoesNotContain(jobs, j => j.Occurrence == Occurrence(slow.Instant + TimeSpan.FromSeconds(1)));
        Assert.DoesNotContain(jobs, j => j.Outcome == JobOutcome.Skipped);
    }

    [Fact]
    public async Task J6_SkipRunsNothingMissedDuringDowntime()
    {
        var (runs, started, _) = await DowntimeAsync(Missed.Skip);

        var observed = await RunsAsync(runs, 2, TimeSpan.FromSeconds(10));
        Assert.All(observed, r => Assert.True(r.Instant > started, $"{r.Occurrence} was missed"));
    }

    [Fact]
    public async Task J6_RunLatestRunsTheLatestMissedOccurrenceOnceThenContinues()
    {
        var (runs, started, scheduled) = await DowntimeAsync(Missed.RunLatest);

        var observed = await RunsAsync(runs, 3, TimeSpan.FromSeconds(10));
        var missed = observed.Where(r => r.Instant <= started).ToArray();
        var latest = Assert.Single(missed);
        Assert.True(latest.Instant >= started - TimeSpan.FromSeconds(1), "the latest missed instant runs, not an earlier one");
        Assert.True(latest.Instant > scheduled + TimeSpan.FromSeconds(1));
        Assert.Equal(latest.Instant + TimeSpan.FromSeconds(1), observed[1].Instant);
    }

    /// <summary>A recurrence every second is scheduled while no process runs the definition, which starts 3.5 seconds later.</summary>
    private async Task<(ConcurrentQueue<ObservedOccurrence> Runs, DateTimeOffset Started, DateTimeOffset Scheduled)> DowntimeAsync(Missed missed)
    {
        var definition = Definition(subject: "feed");
        var client = await ClientAsync(null, definition);
        var scheduled = await Provider.NowAsync(Cancel);
        await ScheduleAsync(client, definition, "x", Schedule.Cron("* * * * * *", "UTC"), Recurring(Subject.Of("feed", "a"), missed: missed));
        await Task.Delay(3500, Cancel);
        var started = await Provider.NowAsync(Cancel);
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        await WorkerAsync(definition, Occurrences(runs), pollingInterval: Polling);
        return (runs, started, scheduled);
    }

    [Fact]
    public async Task J12_AttemptsBelongToTheirOccurrenceAndTheNextOneStartsFresh()
    {
        var definition = Definition(new JobPolicy { Attempts = 5, Backoff = Backoff.Constant(TimeSpan.FromMilliseconds(200)) }, subject: "feed");
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        string? first = null;
        var worker = await WorkerAsync(
            definition,
            Occurrences(runs, (_, run, _) =>
            {
                first ??= run.Occurrence;
                return ValueTask.FromResult<JobDisposition>(run.Occurrence == first && run.Attempt <= 2 ? JobResult.Retry(TimeSpan.Zero, "flaky") : JobResult.Completed);
            }),
            pollingInterval: Polling);
        var anchor = await Provider.NowAsync(Cancel);
        await ScheduleAsync(worker, definition, "x", Schedule.Every(TimeSpan.FromSeconds(3), Interval.FixedRate, anchor), Recurring(Subject.Of("feed", "a")));

        var observed = await RunsAsync(runs, 4, TimeSpan.FromSeconds(15));
        Assert.Equal([1, 2, 3], observed[..3].Select(r => r.Attempt));
        Assert.Single(observed[..3].Select(r => (r.Job, r.Occurrence, r.ExecutionId)).Distinct());
        Assert.Equal(JobOutcome.Completed, (await ReadJobAsync(observed[0].Job)).State);
        Assert.Equal(observed[0].Instant + TimeSpan.FromSeconds(3), observed[3].Instant);
        Assert.Equal(1, observed[3].Attempt);
        Assert.NotEqual(observed[0].ExecutionId, observed[3].ExecutionId);
    }

    [Fact]
    public async Task J13_AnExhaustedOccurrenceEndsFailedAndTheRecurrenceContinues()
    {
        var definition = Definition(new JobPolicy { Attempts = 2, Backoff = Backoff.None }, subject: "feed");
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        string? first = null;
        var worker = await WorkerAsync(
            definition,
            Occurrences(runs, (_, run, _) =>
            {
                first ??= run.Occurrence;
                return ValueTask.FromResult<JobDisposition>(run.Occurrence == first ? JobResult.Retry(TimeSpan.Zero, "broken") : JobResult.Completed);
            }),
            pollingInterval: Polling);
        await ScheduleAsync(worker, definition, "x", Schedule.Every(TimeSpan.FromSeconds(2), Interval.FixedRate, await Provider.NowAsync(Cancel)), Recurring(Subject.Of("feed", "a")));

        var observed = await RunsAsync(runs, 3, TimeSpan.FromSeconds(15));
        var failed = await ReadJobAsync(observed[0].Job);
        Assert.Equal((JobOutcome.Failed, JobReasons.AttemptsExhausted, first), (failed.State, failed.Reason, failed.Occurrence));
        Assert.NotEqual(first, observed[2].Occurrence);
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(observed[2].Job, TimeSpan.FromSeconds(5))).State);
    }

    [Fact]
    public async Task J17_CancellingTheCurrentOccurrenceKeepsTheRecurrence()
    {
        var definition = Definition(subject: "feed");
        var subject = Subject.Of("feed", "a");
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var worker = await WorkerAsync(
            definition,
            Occurrences(runs, async (_, _, cancellationToken) =>
            {
                if (runs.IsEmpty)
                {
                    await Task.Delay(Timeout.Infinite, cancellationToken);
                }
                return JobResult.Completed;
            }),
            pollingInterval: Polling);
        await ScheduleAsync(worker, definition, "x", Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate), Recurring(subject));
        await EventuallyAsync(async () => (await ReadJobsAsync(definition)).Any(j => j.Held), TimeSpan.FromSeconds(10), "the first occurrence runs");

        Assert.Equal(1, await InTransactionAsync(t => Jobs(worker).CancelAsync(t, definition, subject, Cancel)));

        var observed = await RunsAsync(runs, 2, TimeSpan.FromSeconds(10));
        var cancelled = await EndedAsync(observed[0].Job, TimeSpan.FromSeconds(5));
        Assert.Equal(JobOutcome.Cancelled, cancelled.State);
        Assert.NotEqual(observed[0].Occurrence, observed[1].Occurrence);
    }

    [Fact]
    public async Task J20_ACodeDefinedRecurrenceFollowsTheNewestRevision()
    {
        var key = Definition().Key;
        var newer = Definition(revision: 2, key: key);
        var older = Definition(revision: 1, key: key);
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        await WorkerAsync(newer, Occurrences(runs), also: work => work.AddRecurring(newer, new TestArgs("newer"), Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate)), pollingInterval: Polling);
        await ProcessAsync(work => work.AddJobDefinition(older).AddRecurring(older, new TestArgs("older"), Schedule.Every(TimeSpan.FromHours(1), Interval.FixedRate)));

        var observed = await RunsAsync(runs, 3, TimeSpan.FromSeconds(10));
        Assert.All(observed, r => Assert.Equal("newer", r.Args));
        Assert.Equal(observed[1].Instant + TimeSpan.FromSeconds(1), observed[2].Instant);
        await Assert.ThrowsAsync<JobDefinitionConflictException>(() =>
            ProcessAsync(work => work.AddJobDefinition(newer).AddRecurring(newer, new TestArgs("different"), Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate))));
    }

    [Fact]
    public async Task J23_ACronExpressionWithoutAFutureOccurrenceIsRefused()
    {
        var definition = Definition(subject: "feed");
        var subject = Subject.Of("feed", "a");
        var client = await ClientAsync(null, definition);

        await Assert.ThrowsAsync<ArgumentException>(() => ScheduleAsync(client, definition, "x", Schedule.Cron("0 0 30 2 *", "UTC"), Recurring(subject)));
        await Assert.ThrowsAsync<InvalidOperationException>(() => InTransactionAsync(t => Jobs(client).TriggerAsync(t, definition, subject, Cancel)));
    }

    [Fact]
    public async Task D7_ChangingTheIntervalWhileAnOccurrenceRunsAppliesFromTheNextOccurrence()
    {
        var definition = Definition(subject: "feed");
        var subject = Subject.Of("feed", "a");
        var running = new TaskCompletionSource();
        var replaced = new TaskCompletionSource();
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var worker = await WorkerAsync(
            definition,
            Occurrences(runs, async (_, _, cancellationToken) =>
            {
                if (runs.IsEmpty)
                {
                    running.TrySetResult();
                    await replaced.Task.WaitAsync(cancellationToken);
                }
                return JobResult.Completed;
            }),
            pollingInterval: Polling);
        await ScheduleAsync(worker, definition, "x", Schedule.Every(TimeSpan.FromSeconds(1), Interval.AfterCompletion), Recurring(subject));
        await running.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);

        Assert.Equal(RecurrenceOutcome.Replaced, await ScheduleAsync(worker, definition, "x", Schedule.Every(TimeSpan.FromSeconds(3), Interval.AfterCompletion), Recurring(subject)));
        replaced.SetResult();

        var observed = await RunsAsync(runs, 2, TimeSpan.FromSeconds(10));
        Assert.Equal(JobOutcome.Completed, (await ReadJobAsync(observed[0].Job)).State);
        Assert.True(observed[1].Started - observed[0].Returned >= TimeSpan.FromSeconds(3) - Jitter, "the new interval applies from the next occurrence");
    }

    protected static string Occurrence(DateTimeOffset instant) => instant.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'", System.Globalization.CultureInfo.InvariantCulture);
}
