using System.Collections.Concurrent;

namespace Corfedo.Work.Conformance;

/// <summary>
/// Definition and subject pauses: nothing paused is claimed or planned, claims serialize with an open pause, requests
/// and schedules that race a pause or a resume end with the stored pause deciding, maintenance repairs copied flags
/// past locked rows, and a fan-out reaches a paused subject.
/// </summary>
public abstract class JobPauseScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    protected static readonly TimeSpan Polling = TimeSpan.FromMilliseconds(250);

    private static readonly Subject A = Subject.Of("feed", "a");

    private static readonly Subject B = Subject.Of("feed", "b");

    [Fact]
    public async Task J18_APausedDefinitionClaimsAndPlansNothingUntilResumed()
    {
        var definition = Definition(subject: "feed");
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        var worker = await WorkerAsync(definition, Recording(attempts), pollingInterval: Polling);
        await ScheduleAsync(worker, definition, "scheduled", Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate, await Provider.NowAsync(Cancel)), Recurring(A));
        await EventuallyAsync(() => !attempts.IsEmpty, TimeSpan.FromSeconds(10), "the recurrence runs");

        await InTransactionAsync(t => Jobs(worker).PauseAsync(t, definition, Cancel));
        var paused = Clock.Elapsed;
        var pausedAt = await Provider.NowAsync(Cancel);
        var oneShot = await RequestAsync(worker, definition, "one-shot", options: For(B));
        await Task.Delay(2500, Cancel);
        Assert.DoesNotContain(attempts, a => a.At > paused);
        Assert.Null((await ReadJobAsync(oneShot.Id)).State);

        await InTransactionAsync(t => Jobs(worker).ResumeAsync(t, definition, Cancel));
        var resumed = await Provider.NowAsync(Cancel);
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(oneShot.Id, TimeSpan.FromSeconds(10))).State);
        await EventuallyAsync(() => attempts.Count(a => a.At > paused && a.Args == "scheduled") >= 2, TimeSpan.FromSeconds(10), "the recurrence continues");
        // Missed.Skip: no instant that passed while the definition was paused is planned.
        var instants = (await ReadJobsAsync(definition))
            .Where(j => j.Occurrence is not null)
            .Select(j => DateTimeOffset.Parse(j.Occurrence!, System.Globalization.CultureInfo.InvariantCulture));
        Assert.DoesNotContain(instants, i => i > pausedAt && i < resumed - TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task J18_APausedSubjectWaitsWhileOtherSubjectsRun()
    {
        var definition = Definition(subject: "feed");
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        var worker = await WorkerAsync(definition, Recording(attempts), pollingInterval: Polling);
        await InTransactionAsync(t => Jobs(worker).PauseAsync(t, definition, A, Cancel));

        var paused = await RequestAsync(worker, definition, "a", options: For(A));
        var other = await RequestAsync(worker, definition, "b", options: For(B));

        Assert.Equal(JobOutcome.Completed, (await EndedAsync(other.Id, TimeSpan.FromSeconds(10))).State);
        await Task.Delay(1500, Cancel);
        var waiting = await ReadJobAsync(paused.Id);
        Assert.Equal(((JobOutcome?)null, true, 0), (waiting.State, waiting.Paused, waiting.Attempts));

        await InTransactionAsync(t => Jobs(worker).ResumeAsync(t, definition, A, Cancel));
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(paused.Id, TimeSpan.FromSeconds(10))).State);
        Assert.Equal(["b", "a"], attempts.Select(a => a.Args));
    }

    [Fact]
    public async Task J18_OverdueRecurrencesOfPausedSubjectsHoldBackNeitherActiveOnesNorMaintenance()
    {
        const int count = 120;
        var definition = Definition(subject: "feed");
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var worker = await WorkerAsync(definition, Occurrences(runs), pollingInterval: Polling);
        var paused = Enumerable.Range(0, count).Select(i => Subject.Of("feed", $"p{i}")).ToArray();
        await InTransactionAsync(async t =>
        {
            foreach (var subject in paused)
            {
                await Jobs(worker).PauseAsync(t, definition, subject, Cancel);
                await Jobs(worker).ScheduleAsync(t, definition, new TestArgs("paused"), Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate), Recurring(subject), Cancel);
            }
        });
        await Task.Delay(1500, Cancel);

        await ScheduleAsync(worker, definition, "active", Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate), Recurring(Subject.Of("feed", "active")));

        await RunsAsync(runs, 2, TimeSpan.FromSeconds(10));
        Assert.All(runs, r => Assert.Equal("active", r.Args));
        // A pass that waits commits a few transactions per polling interval; one that repeats at once commits thousands.
        var before = await Provider.CommittedTransactionsAsync(Cancel);
        await Task.Delay(3000, Cancel);
        var committed = await Provider.CommittedTransactionsAsync(Cancel) - before;
        Assert.True(committed < 500, $"{committed} transactions committed in 3 s");
    }

    [Fact]
    public async Task J18_MaintenanceWaitsWhileADefinitionWithOverdueRecurrencesIsPaused()
    {
        var definition = Definition(subject: "feed");
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var worker = await WorkerAsync(definition, Occurrences(runs), pollingInterval: Polling);
        await InTransactionAsync(t => Jobs(worker).PauseAsync(t, definition, Cancel));
        await ScheduleAsync(worker, definition, "x", Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate), Recurring(A));
        await Task.Delay(2000, Cancel);

        var before = await Provider.CommittedTransactionsAsync(Cancel);
        await Task.Delay(3000, Cancel);
        var committed = await Provider.CommittedTransactionsAsync(Cancel) - before;
        Assert.True(committed < 500, $"{committed} transactions committed in 3 s");
        Assert.Empty(runs);

        await InTransactionAsync(t => Jobs(worker).ResumeAsync(t, definition, Cancel));
        await RunsAsync(runs, 1, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task ClaimsWaitForAPauseThatHasNotCommitted()
    {
        RequireConcurrentTransactions();
        var definition = Definition(subject: "feed");
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        var worker = await WorkerAsync(definition, Recording(attempts), pollingInterval: Polling);
        await using var pausing = await BeginAsync();
        await Jobs(worker).PauseAsync(pausing.Work, definition, A, Cancel);

        var job = await RequestAsync(worker, definition, "a", options: For(A));

        await Task.Delay(1500, Cancel);
        Assert.Empty(attempts);
        Assert.False((await ReadJobAsync(job.Id)).Paused);
        await pausing.RollbackAsync(Cancel);
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(job.Id, TimeSpan.FromSeconds(10))).State);
    }

    [Fact]
    public async Task ARequestCommittedBeforeAPauseCommitsStaysUnclaimableAndMaintenanceSetsItsFlag()
    {
        RequireConcurrentTransactions();
        var definition = Definition(subject: "feed");
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        var worker = await WorkerAsync(definition, Recording(attempts), pollingInterval: Polling);
        await using (var pausing = await BeginAsync())
        {
            await Jobs(worker).PauseAsync(pausing.Work, definition, A, Cancel);
            var requested = await RequestAsync(worker, definition, "a", options: For(A));
            Assert.False((await ReadJobAsync(requested.Id)).Paused);
            await pausing.CommitAsync(Cancel);

            await EventuallyAsync(async () => (await ReadJobAsync(requested.Id)).Paused, TimeSpan.FromSeconds(10), "maintenance sets the flag");
            await Task.Delay(1000, Cancel);
            Assert.Empty(attempts);

            await InTransactionAsync(t => Jobs(worker).ResumeAsync(t, definition, A, Cancel));
            Assert.Equal(JobOutcome.Completed, (await EndedAsync(requested.Id, TimeSpan.FromSeconds(10))).State);
        }
    }

    [Fact]
    public async Task ARequestThatRacedAResumeIsClaimedWithoutFurtherRequests()
    {
        RequireConcurrentTransactions();
        var definition = Definition(subject: "feed");
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        var worker = await WorkerAsync(definition, Recording(attempts), pollingInterval: Polling);
        await InTransactionAsync(t => Jobs(worker).PauseAsync(t, definition, A, Cancel));
        JobRequestResult requested;
        await using (var requesting = await BeginAsync())
        {
            requested = await Jobs(worker).RequestAsync(requesting.Work, definition, new TestArgs("a"), Schedule.Now, For(A), Cancel);
            await InTransactionAsync(t => Jobs(worker).ResumeAsync(t, definition, A, Cancel));
            await requesting.CommitAsync(Cancel);
        }

        Assert.Equal(JobOutcome.Completed, (await EndedAsync(requested.Id, TimeSpan.FromSeconds(10))).State);
        Assert.Single(attempts);
    }

    [Fact]
    public async Task PausingASubjectFlagsItsRecurrenceWhichRunsAgainAfterResume()
    {
        var definition = Definition(subject: "feed");
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var worker = await WorkerAsync(definition, Occurrences(runs), pollingInterval: Polling);
        await ScheduleAsync(worker, definition, "x", Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate), Recurring(A));
        await RunsAsync(runs, 1, TimeSpan.FromSeconds(10));

        await InTransactionAsync(t => Jobs(worker).PauseAsync(t, definition, A, Cancel));
        Assert.True(await Provider.ReadRecurrencePausedAsync(definition.Key, A, Cancel));
        await Task.Delay(1000, Cancel);
        var paused = runs.Count;
        await Task.Delay(2000, Cancel);
        Assert.Equal(paused, runs.Count);

        await InTransactionAsync(t => Jobs(worker).ResumeAsync(t, definition, A, Cancel));
        Assert.False(await Provider.ReadRecurrencePausedAsync(definition.Key, A, Cancel));
        await RunsAsync(runs, paused + 1, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task AScheduleThatRacedAPauseIsFlaggedByMaintenanceAndWaits()
    {
        RequireConcurrentTransactions();
        var definition = Definition(subject: "feed");
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var worker = await WorkerAsync(definition, Occurrences(runs), pollingInterval: Polling);
        await using (var scheduling = await BeginAsync())
        {
            await Jobs(worker).ScheduleAsync(scheduling.Work, definition, new TestArgs("x"), Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate), Recurring(A), Cancel);
            await InTransactionAsync(t => Jobs(worker).PauseAsync(t, definition, A, Cancel));
            await scheduling.CommitAsync(Cancel);
        }
        Assert.False(await Provider.ReadRecurrencePausedAsync(definition.Key, A, Cancel));

        await EventuallyAsync(() => Provider.ReadRecurrencePausedAsync(definition.Key, A, Cancel), TimeSpan.FromSeconds(10), "maintenance sets the flag");
        await Task.Delay(1500, Cancel);
        Assert.Empty(runs);

        await InTransactionAsync(t => Jobs(worker).ResumeAsync(t, definition, A, Cancel));
        await RunsAsync(runs, 1, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task AScheduleCommittedLateAfterAResumeRunsWithoutFurtherRequests()
    {
        RequireConcurrentTransactions();
        var definition = Definition(subject: "feed");
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var worker = await WorkerAsync(definition, Occurrences(runs), pollingInterval: Polling);
        await InTransactionAsync(t => Jobs(worker).PauseAsync(t, definition, A, Cancel));
        await using (var scheduling = await BeginAsync())
        {
            await Jobs(worker).ScheduleAsync(scheduling.Work, definition, new TestArgs("x"), Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate), Recurring(A), Cancel);
            await InTransactionAsync(t => Jobs(worker).ResumeAsync(t, definition, A, Cancel));
            // Maintenance passes run meanwhile; the stale flag commits after them.
            await Task.Delay(1000, Cancel);
            await scheduling.CommitAsync(Cancel);
        }

        await RunsAsync(runs, 1, TimeSpan.FromSeconds(10));
        Assert.False(await Provider.ReadRecurrencePausedAsync(definition.Key, A, Cancel));
    }

    [Fact]
    public async Task ConcurrentPausesAndResumesLeaveTheFlagsOfASubjectInAgreement()
    {
        var definition = Definition(subject: "feed");
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var worker = await WorkerAsync(definition, Occurrences(runs), pollingInterval: Polling);
        await ScheduleAsync(worker, definition, "x", Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate), Recurring(A));
        var requested = await RequestAsync(worker, definition, "later", Schedule.After(TimeSpan.FromHours(1)), For(Subject.Of("feed", "a"), RequestMode.New));

        async Task ToggleAsync(bool paused)
        {
            for (var i = 0; i < 20; i++)
            {
                await InTransactionAsync(t => paused ? Jobs(worker).PauseAsync(t, definition, A, Cancel) : Jobs(worker).ResumeAsync(t, definition, A, Cancel));
            }
        }
        await Task.WhenAll(ToggleAsync(true), ToggleAsync(false));
        Assert.Equal((await ReadJobAsync(requested.Id)).Paused, await Provider.ReadRecurrencePausedAsync(definition.Key, A, Cancel));

        await InTransactionAsync(t => Jobs(worker).PauseAsync(t, definition, A, Cancel));
        Assert.True(await Provider.ReadRecurrencePausedAsync(definition.Key, A, Cancel));
        Assert.True((await ReadJobAsync(requested.Id)).Paused);
        await Task.Delay(1000, Cancel);
        var paused = runs.Count;
        await Task.Delay(1500, Cancel);
        Assert.Equal(paused, runs.Count);

        await InTransactionAsync(t => Jobs(worker).ResumeAsync(t, definition, A, Cancel));
        await RunsAsync(runs, paused + 1, TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task MaintenanceSkipsLockedStaleFlagsWithoutRepeatingAndRepairsThemInALaterPass()
    {
        RequireConcurrentTransactions();
        var definition = Definition(subject: "feed");
        var client = await ClientAsync(null, definition);
        await InTransactionAsync(t => Jobs(client).PauseAsync(t, definition, A, Cancel));
        JobRequestResult requested;
        await using (var racing = await BeginAsync())
        {
            await Jobs(client).ScheduleAsync(racing.Work, definition, new TestArgs("x"), Schedule.Every(TimeSpan.FromHours(1), Interval.FixedRate), Recurring(A), Cancel);
            requested = await Jobs(client).RequestAsync(racing.Work, definition, new TestArgs("requested"), Schedule.Now, For(A), Cancel);
            await InTransactionAsync(t => Jobs(client).ResumeAsync(t, definition, A, Cancel));
            await racing.CommitAsync(Cancel);
        }
        Assert.True(await Provider.ReadRecurrencePausedAsync(definition.Key, A, Cancel));
        Assert.True((await ReadJobAsync(requested.Id)).Paused);

        await using var locking = await BeginAsync();
        await Provider.LockSubjectAsync(locking.Work, definition.Key, A, Cancel);
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        await WorkerAsync(definition, Occurrences(runs), pollingInterval: Polling);
        await Task.Delay(1500, Cancel);
        // A pass that waits commits a few transactions per polling interval; one that repeats at once commits thousands.
        var before = await Provider.CommittedTransactionsAsync(Cancel);
        await Task.Delay(3000, Cancel);
        var committed = await Provider.CommittedTransactionsAsync(Cancel) - before;
        Assert.True(committed < 500, $"{committed} transactions committed in 3 s");
        Assert.True(await Provider.ReadRecurrencePausedAsync(definition.Key, A, Cancel));
        Assert.True((await ReadJobAsync(requested.Id)).Paused);
        Assert.Empty(runs);

        await locking.CommitAsync(Cancel);
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(requested.Id, TimeSpan.FromSeconds(10))).State);
        await EventuallyAsync(async () => !await Provider.ReadRecurrencePausedAsync(definition.Key, A, Cancel), TimeSpan.FromSeconds(10), "maintenance clears the recurrence flag");
    }

    [Fact]
    public async Task AFanOutOf50000RequestsReachesAPausedSubject()
    {
        const int count = 50_000;
        var definition = Definition(subject: "feed");
        var client = await ClientAsync(null, definition);
        var store = Jobs(client);
        await InTransactionAsync(t => store.PauseAsync(t, definition, Subject.Of("feed", "s0"), Cancel));
        var items = Enumerable.Range(0, count).Select(i => new JobRequestItem<TestArgs>(new TestArgs("x"), For(Subject.Of("feed", $"s{i}")))).ToArray();

        var results = await InTransactionAsync(t => store.RequestManyAsync(t, definition, items, Schedule.After(TimeSpan.FromHours(1)), Cancel));

        Assert.Equal(count, results.Count);
        Assert.All(results, r => Assert.Equal(JobRequestOutcome.Created, r.Outcome));
        Assert.True((await ReadJobAsync(results[0].Id)).Paused);
        Assert.False((await ReadJobAsync(results[1].Id)).Paused);
        Assert.False((await ReadJobAsync(results[count - 1].Id)).Paused);
    }
}
