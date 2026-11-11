using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>
/// Expectations and incidents: failures in a row and running attempts recorded per subject, completion deadlines
/// checked by monitoring processes, and the registry's report of definitions no process monitors. Deadlines are a few
/// seconds ahead of the store's clock in a fixed-offset time zone where it is near noon; multi-day deadline behaviour
/// runs as tests of the deadline calculator.
/// </summary>
public abstract class JobMonitoringScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    private static readonly TimeSpan Polling = TimeSpan.FromMilliseconds(250);
    private static int names;

    [Fact]
    public async Task M2_AKeyFailingMoreThanItsLimitInARowOpensOneIncidentItsNextSuccessResolves()
    {
        var definition = Definition(new JobPolicy { Attempts = 1, Expect = Expect.FailsAtMostInARow(3) }, subject: "feed");
        var (failing, other) = (Subject.Of("feed", "failing"), Subject.Of("feed", "other"));
        var script = new JobScript
        {
            Run = (args, _, _) => ValueTask.FromResult<JobDisposition>(args.Value == "fail" ? JobResult.Fail("broken") : JobResult.Completed),
        };
        var worker = await WorkerAsync(definition, script, pollingInterval: Polling);
        async Task<JobId> EndAsync(Subject subject, string args, JobOutcome expected)
        {
            var job = await RequestAsync(worker, definition, args, options: For(subject, RequestMode.New));
            Assert.Equal(expected, (await EndedAsync(job.Id, TimeSpan.FromSeconds(10))).State);
            return job.Id;
        }

        // Three failures stay within the limit; a cancellation neither counts nor resets; the other subject succeeds.
        for (var i = 0; i < 3; i++)
        {
            await EndAsync(failing, "fail", JobOutcome.Failed);
            await EndAsync(other, "ok", JobOutcome.Completed);
            if (i == 1)
            {
                await RequestAsync(worker, definition, "later", Schedule.After(TimeSpan.FromHours(1)), For(failing, RequestMode.New));
                Assert.Equal(1, await InTransactionAsync(t => Jobs(worker).CancelAsync(t, definition, failing, Cancel)));
            }
        }
        Assert.Empty(await IncidentsAsync(worker, definition));

        // The fourth opens one incident naming the job and the subject; a fifth adds nothing.
        var fourth = await EndAsync(failing, "fail", JobOutcome.Failed);
        var incident = Assert.Single(await IncidentsAsync(worker, definition));
        Assert.Equal((WorkIncidentKind.FailsAtMostInARow, definition.Key, failing, fourth, (DateTimeOffset?)null), (incident.Kind, incident.Definition, incident.Subject, incident.Job, incident.ResolvedAt));
        await EndAsync(failing, "fail", JobOutcome.Failed);
        await EndAsync(other, "ok", JobOutcome.Completed);
        Assert.Equal(incident with { }, Assert.Single(await IncidentsAsync(worker, definition)));

        // The subject's next success resolves it and starts the count again.
        await EndAsync(failing, "ok", JobOutcome.Completed);
        var resolved = Assert.Single(await IncidentsAsync(worker, definition));
        Assert.Equal(incident.Id, resolved.Id);
        Assert.True(resolved.ResolvedAt >= resolved.OpenedAt);
        for (var i = 0; i < 3; i++)
        {
            await EndAsync(failing, "fail", JobOutcome.Failed);
        }
        Assert.Empty(await IncidentsAsync(worker, definition, openOnly: true));
    }

    [Fact]
    public async Task M1_ARecurringChainLateForItsDeadlineOpensAnIncidentAtItThatItsCompletionResolves()
    {
        var clock = await ClockAsync();
        var run = clock.WholeSecondAfter(TimeSpan.FromSeconds(2));
        var deadline = clock.WholeSecondAfter(TimeSpan.FromSeconds(4));
        var local = TimeOnly.FromDateTime(deadline.UtcDateTime + clock.Offset);
        var holidays = WorkCalendar.Create(Name(), 1).InTimeZone(clock.Zone).ExcludeDate(clock.Today);
        var step = Definition();
        var late = Definition(new JobPolicy { Expect = Expect.CompletesBy(local, clock.Zone) });
        var onTime = Definition(new JobPolicy { Expect = Expect.CompletesBy(local, clock.Zone) });
        var holiday = Definition(new JobPolicy { Expect = Expect.CompletesBy(local, clock.Zone, holidays.Name) });
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var script = new JobScript
        {
            Run = async (args, _, cancellationToken) =>
            {
                if (args.Value == "late")
                {
                    await release.Task.WaitAsync(cancellationToken);
                }
                return JobResult.Completed;
            },
        };
        var runLocal = run.ToOffset(clock.Offset);
        var cron = Schedule.Cron($"{runLocal.Second} {runLocal.Minute} {runLocal.Hour} * * *", clock.Zone);
        var worker = await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(script);
                work.AddJob<TestArgs, ScriptedJobHandler>(step, job => job.Lease(TimeSpan.FromSeconds(2)).Capacity(4));
                work.AddCalendar(holidays).AddMonitoring();
                foreach (var (definition, args) in new[] { (late, "late"), (onTime, "on time") })
                {
                    work.AddRecurringChain(definition, new TestArgs(args), cron, chain => chain.StartWith(step, new TestArgs(args)));
                }
                work.AddRecurringChain(holiday, new TestArgs("holiday"), cron.Within(holidays.Name), chain => chain.StartWith(step, new TestArgs("holiday")));
            },
            pollingInterval: Polling);

        // The late chain's incident opens at the deadline and resolves when the chain completes.
        await EventuallyAsync(async () => (await IncidentsAsync(worker, late)).Count > 0, TimeSpan.FromSeconds(15), "the late chain's incident opens");
        var opened = Assert.Single(await IncidentsAsync(worker, late));
        Assert.Equal((WorkIncidentKind.CompletesBy, (Subject?)null, deadline, (DateTimeOffset?)null), (opened.Kind, opened.Subject, opened.OpenedAt, opened.ResolvedAt));
        var occurrence = Assert.Single(await ReadJobsAsync(late));
        Assert.Null(occurrence.State);
        release.SetResult();
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(occurrence.Id, TimeSpan.FromSeconds(10))).State);
        var resolved = Assert.Single(await IncidentsAsync(worker, late));
        Assert.Equal(opened.Id, resolved.Id);
        Assert.True(resolved.ResolvedAt > deadline);

        // The chain that completed in time and the one blocked by the holiday open nothing.
        Assert.Equal(JobOutcome.Completed, Assert.Single(await ReadJobsAsync(onTime)).State);
        Assert.Equal((JobOutcome.Skipped, JobReasons.CalendarBlocked), (Assert.Single(await ReadJobsAsync(holiday)).State, Assert.Single(await ReadJobsAsync(holiday)).Reason));
        Assert.Empty(await IncidentsAsync(worker, onTime));
        Assert.Empty(await IncidentsAsync(worker, holiday));
    }

    [Fact]
    public async Task M1_WithEveryWorkerOfTheDefinitionStoppedAMonitoringProcessOpensTheIncidentAtTheDeadline()
    {
        var clock = await ClockAsync();
        var deadline = clock.WholeSecondAfter(TimeSpan.FromSeconds(3));
        var definition = Definition(new JobPolicy { Expect = Expect.CompletesBy(TimeOnly.FromDateTime(deadline.UtcDateTime + clock.Offset), clock.Zone) });
        var worker = await WorkerAsync(definition, new JobScript(), also: w => w.AddRecurring(definition, new TestArgs("daily"), Schedule.Cron("0 0 0 1 1 *", clock.Zone)), pollingInterval: Polling);
        await worker.StopAsync(Cancel);
        var monitor = await ProcessAsync(w => w.AddMonitoring(), pollingInterval: Polling);

        await EventuallyAsync(async () => (await IncidentsAsync(monitor, definition)).Count > 0, TimeSpan.FromSeconds(10), "the incident opens");
        var incident = Assert.Single(await IncidentsAsync(monitor, definition));
        Assert.Equal((WorkIncidentKind.CompletesBy, deadline, (DateTimeOffset?)null), (incident.Kind, incident.OpenedAt, incident.ResolvedAt));
    }

    [Fact]
    public async Task M1_RecurrencesCreatedRemovedPausedAndResumedWhileAProcessMonitorsChangeWhichKeysAreChecked()
    {
        var clock = await ClockAsync();
        var deadline = clock.WholeSecondAfter(TimeSpan.FromSeconds(8));
        var definition = Definition(new JobPolicy { Expect = Expect.CompletesBy(Local(clock, deadline), clock.Zone) }, subject: "feed");
        var worker = await WorkerAsync(definition, new JobScript(), also: w => w.AddMonitoring(), pollingInterval: Polling);
        var (kept, removed, paused, resumed) = (Subject.Of("feed", "kept"), Subject.Of("feed", "removed"), Subject.Of("feed", "paused"), Subject.Of("feed", "resumed"));
        foreach (var subject in new[] { kept, removed, paused, resumed })
        {
            await ScheduleAsync(worker, definition, subject.Id, Yearly(clock), Recurring(subject));
        }

        // Monitoring passes see every key, then one recurrence is removed, one subject paused and one paused and resumed.
        await Task.Delay(Polling * 3, Cancel);
        Assert.True(await InTransactionAsync(t => Jobs(worker).RemoveAsync(t, definition, removed, Cancel)));
        await InTransactionAsync(t => Jobs(worker).PauseAsync(t, definition, paused, Cancel));
        await InTransactionAsync(t => Jobs(worker).PauseAsync(t, definition, resumed, Cancel));
        await Task.Delay(Polling * 3, Cancel);
        await InTransactionAsync(t => Jobs(worker).ResumeAsync(t, definition, resumed, Cancel));
        Assert.True(await Provider.NowAsync(Cancel) < deadline - (Polling * 2), "the changes are made before the deadline");

        // Only the kept and the resumed keys miss the deadline.
        await EventuallyAsync(async () => (await IncidentsAsync(worker, definition)).Count >= 2, TimeSpan.FromSeconds(15), "the incidents of the expected keys open");
        await Task.Delay(Polling * 3, Cancel);
        var incidents = await IncidentsAsync(worker, definition);
        Assert.Equal(["kept", "resumed"], incidents.Select(i => i.Subject!.Id).Order());
        Assert.All(incidents, i => Assert.Equal((WorkIncidentKind.CompletesBy, deadline, (DateTimeOffset?)null), (i.Kind, i.OpenedAt, i.ResolvedAt)));

        // Removing a recurrence and pausing a subject resolve their keys' incidents.
        Assert.True(await InTransactionAsync(t => Jobs(worker).RemoveAsync(t, definition, kept, Cancel)));
        await InTransactionAsync(t => Jobs(worker).PauseAsync(t, definition, resumed, Cancel));
        await EventuallyAsync(async () => (await IncidentsAsync(worker, definition, openOnly: true)).Count == 0, TimeSpan.FromSeconds(10), "the incidents resolve");
        Assert.Equal(2, (await IncidentsAsync(worker, definition)).Count);
    }

    [Fact]
    public async Task M1_ExpectedKeyChangesWakeTheMonitoringLoopWithoutWaitingForItsPollingInterval()
    {
        var clock = await ClockAsync();
        var deadline = clock.WholeSecondAfter(TimeSpan.FromSeconds(6));
        var definition = Definition(new JobPolicy { Expect = Expect.CompletesBy(Local(clock, deadline), clock.Zone) }, subject: "feed");
        var worker = await WorkerAsync(definition, new JobScript(), also: w => w.AddMonitoring(), pollingInterval: TimeSpan.FromSeconds(30));
        var (removed, paused) = (Subject.Of("feed", "removed"), Subject.Of("feed", "paused"));

        // Recurrences created after the first pass are checked at the deadline, long before the next polling pass.
        await Task.Delay(TimeSpan.FromSeconds(1), Cancel);
        await ScheduleAsync(worker, definition, "removed", Yearly(clock), Recurring(removed));
        await ScheduleAsync(worker, definition, "paused", Yearly(clock), Recurring(paused));
        await EventuallyAsync(async () => (await IncidentsAsync(worker, definition)).Count == 2, TimeSpan.FromSeconds(10), "the incidents open at the deadline");
        Assert.All(await IncidentsAsync(worker, definition), i => Assert.Equal((WorkIncidentKind.CompletesBy, deadline), (i.Kind, i.OpenedAt)));

        // Removing a recurrence and pausing a subject resolve their keys' incidents within a few seconds.
        Assert.True(await InTransactionAsync(t => Jobs(worker).RemoveAsync(t, definition, removed, Cancel)));
        await InTransactionAsync(t => Jobs(worker).PauseAsync(t, definition, paused, Cancel));
        await EventuallyAsync(async () => (await IncidentsAsync(worker, definition, openOnly: true)).Count == 0, TimeSpan.FromSeconds(5), "the incidents resolve");
    }

    [Fact]
    public async Task M1_AWindowLockedWhileANewRevisionMovesItsDeadlineIsMovedOnceTheLockIsReleased()
    {
        var clock = await ClockAsync();
        var deadline = clock.WholeSecondAfter(TimeSpan.FromSeconds(12));
        var older = Definition(new JobPolicy { Attempts = 1, Expect = Expect.CompletesBy(clock.Local(TimeSpan.FromHours(1)), clock.Zone).FailsAtMostInARow(5) }, subject: "feed");
        var newer = Definition(new JobPolicy { Attempts = 1, Expect = Expect.CompletesBy(Local(clock, deadline), clock.Zone).FailsAtMostInARow(5) }, subject: "feed", revision: 2, key: older.Key);
        var key = Subject.Of("feed", "held");
        var locked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var script = new JobScript
        {
            Run = async (_, run, cancellationToken) =>
            {
                // Recording the failure locks the key's expectation row until the transaction commits.
                await using var transaction = await Provider.BeginAsync(cancellationToken);
                await run.CompleteAsync(transaction.Work, JobResult.Fail("broken"), cancellationToken);
                locked.SetResult();
                await release.Task.WaitAsync(cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                return run.Completed;
            },
        };
        var worker = await WorkerAsync(older, script, also: w => w.AddMonitoring(), pollingInterval: Polling);
        await ScheduleAsync(worker, older, "daily", Yearly(clock), Recurring(key));
        await Task.Delay(Polling * 3, Cancel);
        var job = await RequestAsync(worker, older, "held", options: For(key, RequestMode.New));
        await locked.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);

        // Monitoring passes see the newer revision while the key's row is locked, then the lock is released.
        await ProcessAsync(work => work.AddJobDefinition(newer));
        await Task.Delay(Polling * 4, Cancel);
        release.SetResult();
        Assert.Equal(JobOutcome.Failed, (await EndedAsync(job.Id, TimeSpan.FromSeconds(10))).State);
        Assert.True(await Provider.NowAsync(Cancel) < deadline - (Polling * 2), "the lock is released before the newer deadline");

        await EventuallyAsync(async () => (await IncidentsAsync(worker, older)).Count > 0, TimeSpan.FromSeconds(20), "the incident opens at the newer revision's deadline");
        var incident = Assert.Single(await IncidentsAsync(worker, older));
        Assert.Equal((WorkIncidentKind.CompletesBy, (Subject?)key, deadline, (DateTimeOffset?)null), (incident.Kind, incident.Subject, incident.OpenedAt, incident.ResolvedAt));
    }

    [Fact]
    public async Task M1_ARecurrenceThatRacedASubjectPauseIsNotCheckedOnceMaintenanceRepairsItsFlag()
    {
        RequireConcurrentTransactions();
        var clock = await ClockAsync();
        var deadline = clock.WholeSecondAfter(TimeSpan.FromSeconds(8));
        var definition = Definition(new JobPolicy { Expect = Expect.CompletesBy(Local(clock, deadline), clock.Zone) }, subject: "feed");
        var worker = await WorkerAsync(definition, new JobScript(), also: w => w.AddMonitoring(), pollingInterval: Polling);
        var (active, raced) = (Subject.Of("feed", "active"), Subject.Of("feed", "raced"));
        await ScheduleAsync(worker, definition, "active", Yearly(clock), Recurring(active));

        // The pause commits while the schedule is open, so the stored recurrence copies no pause; the pause does not
        // wait for the schedule.
        await using (var scheduling = await BeginAsync())
        {
            await Jobs(worker).ScheduleAsync(scheduling.Work, definition, new TestArgs("raced"), Yearly(clock), Recurring(raced), Cancel);
            await InTransactionAsync(t => Jobs(worker).PauseAsync(t, definition, raced, Cancel)).WaitAsync(TimeSpan.FromSeconds(5), Cancel);
            await scheduling.CommitAsync(Cancel);
        }
        await EventuallyAsync(() => Provider.ReadRecurrencePausedAsync(definition.Key, raced, Cancel), TimeSpan.FromSeconds(10), "maintenance sets the flag");
        Assert.True(await Provider.NowAsync(Cancel) < deadline - (Polling * 2), "maintenance repairs the flag before the deadline");

        // Only the active key misses the deadline.
        await EventuallyAsync(async () => (await IncidentsAsync(worker, definition)).Count > 0, TimeSpan.FromSeconds(15), "the active key's incident opens");
        await Task.Delay(Polling * 3, Cancel);
        var incident = Assert.Single(await IncidentsAsync(worker, definition));
        Assert.Equal((WorkIncidentKind.CompletesBy, (Subject?)active, deadline), (incident.Kind, incident.Subject, incident.OpenedAt));
    }

    [Fact]
    public async Task M2_AnAttemptRunningLongerThanItsLimitOpensAnIncidentItsSubjectsNextSuccessResolves()
    {
        var definition = Definition(new JobPolicy { Expect = Expect.RunsAtMost(TimeSpan.FromSeconds(1)) }, subject: "feed");
        var (slow, fast) = (Subject.Of("feed", "slow"), Subject.Of("feed", "fast"));
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var script = new JobScript
        {
            Run = async (args, _, cancellationToken) =>
            {
                if (args.Value == "slow")
                {
                    await release.Task.WaitAsync(cancellationToken);
                }
                return JobResult.Completed;
            },
        };
        var worker = await WorkerAsync(definition, script, configure: j => j.Capacity(2), also: w => w.AddMonitoring(), pollingInterval: Polling);
        var running = await RequestAsync(worker, definition, "slow", options: For(slow));
        for (var i = 0; i < 3; i++)
        {
            var quick = await RequestAsync(worker, definition, "fast", options: For(fast, RequestMode.New));
            Assert.Equal(JobOutcome.Completed, (await EndedAsync(quick.Id, TimeSpan.FromSeconds(10))).State);
        }

        await EventuallyAsync(async () => (await IncidentsAsync(worker, definition)).Count > 0, TimeSpan.FromSeconds(10), "the running attempt's incident opens");
        var incident = Assert.Single(await IncidentsAsync(worker, definition));
        Assert.Equal((WorkIncidentKind.RunsAtMost, slow, (JobId?)running.Id, (DateTimeOffset?)null), (incident.Kind, incident.Subject, incident.Job, incident.ResolvedAt));
        await Task.Delay(1000, Cancel);
        Assert.Single(await IncidentsAsync(worker, definition));

        release.SetResult();
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(running.Id, TimeSpan.FromSeconds(10))).State);
        Assert.NotNull(Assert.Single(await IncidentsAsync(worker, definition)).ResolvedAt);
    }

    [Fact]
    public async Task M2_AHeldAttemptLongerThanItsLimitOpensOneIncidentWithoutAMonitoringProcess()
    {
        var definition = Definition(new JobPolicy { Expect = Expect.RunsAtMost(TimeSpan.FromSeconds(1)) }, subject: "feed");
        var slow = Subject.Of("feed", "slow");
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var worker = await WorkerAsync(definition, Blocking(release), pollingInterval: Polling);
        var running = await RequestAsync(worker, definition, "slow", options: For(slow));

        await EventuallyAsync(async () => (await IncidentsAsync(worker, definition)).Count > 0, TimeSpan.FromSeconds(5), "the attempt's incident opens");
        var incident = Assert.Single(await IncidentsAsync(worker, definition));
        Assert.Equal((WorkIncidentKind.RunsAtMost, slow, (JobId?)running.Id, (DateTimeOffset?)null), (incident.Kind, incident.Subject, incident.Job, incident.ResolvedAt));
        await Task.Delay(1000, Cancel);
        Assert.Single(await IncidentsAsync(worker, definition));

        release.SetResult();
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(running.Id, TimeSpan.FromSeconds(10))).State);
        Assert.NotNull(Assert.Single(await IncidentsAsync(worker, definition)).ResolvedAt);
    }

    [Fact]
    public async Task M2_ARevisionChangingTheRunningLimitDuringAnAttemptAppliesToIt()
    {
        var older = Definition(new JobPolicy { Expect = Expect.RunsAtMost(TimeSpan.FromHours(1)) }, subject: "feed");
        var newer = Definition(new JobPolicy { Expect = Expect.RunsAtMost(TimeSpan.FromSeconds(2)) }, subject: "feed", revision: 2, key: older.Key);
        var slow = Subject.Of("feed", "slow");
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var worker = await WorkerAsync(older, Blocking(release), pollingInterval: Polling);
        var running = await RequestAsync(worker, older, "slow", options: For(slow));
        await EventuallyAsync(async () => (await ReadJobAsync(running.Id)).Attempts == 1, TimeSpan.FromSeconds(5), "the attempt starts");

        // A process storing the newer revision lowers the limit of the running attempt.
        await ProcessAsync(work => work.AddJobDefinition(newer));
        await EventuallyAsync(async () => (await IncidentsAsync(worker, older)).Count > 0, TimeSpan.FromSeconds(10), "the attempt's incident opens under the newer limit");
        var incident = Assert.Single(await IncidentsAsync(worker, older));
        Assert.Equal((WorkIncidentKind.RunsAtMost, (JobId?)running.Id), (incident.Kind, incident.Job));
        release.SetResult();
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(running.Id, TimeSpan.FromSeconds(10))).State);
    }

    [Fact]
    public async Task M2_AttemptsCompletingWithinTheirLimitOpenNothing()
    {
        var definition = Definition(new JobPolicy { Expect = Expect.RunsAtMost(TimeSpan.FromSeconds(1)) }, subject: "feed");
        var script = new JobScript
        {
            Run = async (_, _, cancellationToken) =>
            {
                await Task.Delay(300, cancellationToken);
                return JobResult.Completed;
            },
        };
        var worker = await WorkerAsync(definition, script, also: w => w.AddMonitoring(), pollingInterval: Polling);
        for (var i = 0; i < 3; i++)
        {
            await EndAsync(worker, definition, Subject.Of("feed", "quick"), "quick", JobOutcome.Completed);
        }

        await Task.Delay(1500, Cancel);
        Assert.Empty(await IncidentsAsync(worker, definition));
    }

    [Fact]
    public async Task M2_ATakeoverArmsTheTimerOfTheNewHolder()
    {
        var definition = Definition(new JobPolicy { Attempts = 3, Backoff = Backoff.None, Expect = Expect.RunsAtMost(TimeSpan.FromSeconds(1)) }, subject: "feed");
        var slow = Subject.Of("feed", "slow");
        var client = await ClientAsync(null, definition);
        var job = await RequestAsync(client, definition, "slow", options: For(slow));
        Assert.Equal(1, (await CrashClaimAsync(client, definition, TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(5))).Job!.Attempt);

        // The holder that takes the job over runs its second attempt past the limit.
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var worker = await WorkerAsync(definition, Blocking(release), pollingInterval: Polling);
        await EventuallyAsync(async () => (await IncidentsAsync(worker, definition)).Count > 0, TimeSpan.FromSeconds(10), "the new holder's incident opens");
        var incident = Assert.Single(await IncidentsAsync(worker, definition));
        Assert.Equal((WorkIncidentKind.RunsAtMost, slow, (JobId?)job.Id), (incident.Kind, incident.Subject, incident.Job));
        Assert.Equal(2, (await ReadJobAsync(job.Id)).Attempts);
        release.SetResult();
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(job.Id, TimeSpan.FromSeconds(10))).State);
    }

    [Fact]
    public async Task M2_ARecurringChainOccurrenceLongerThanItsLimitOpensAnIncidentFromTheMonitoringLoop()
    {
        var clock = await ClockAsync();
        var run = clock.WholeSecondAfter(TimeSpan.FromSeconds(2)).ToOffset(clock.Offset);
        var step = Definition();
        var occurrence = Definition(new JobPolicy { Expect = Expect.RunsAtMost(TimeSpan.FromSeconds(1)) });
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var worker = await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(Blocking(release));
                work.AddJob<TestArgs, ScriptedJobHandler>(step, job => job.Lease(TimeSpan.FromSeconds(2)));
                work.AddMonitoring();
                work.AddRecurringChain(occurrence, new TestArgs("chain"), Schedule.Cron($"{run.Second} {run.Minute} {run.Hour} * * *", clock.Zone), chain => chain.StartWith(step, new TestArgs("slow")));
            },
            pollingInterval: Polling);

        await EventuallyAsync(async () => (await IncidentsAsync(worker, occurrence)).Count > 0, TimeSpan.FromSeconds(15), "the occurrence's incident opens");
        var incident = Assert.Single(await IncidentsAsync(worker, occurrence));
        var job = Assert.Single(await ReadJobsAsync(occurrence));
        Assert.Equal((WorkIncidentKind.RunsAtMost, (JobId?)job.Id, (DateTimeOffset?)null), (incident.Kind, incident.Job, incident.ResolvedAt));
        release.SetResult();
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(job.Id, TimeSpan.FromSeconds(10))).State);
        Assert.NotNull(Assert.Single(await IncidentsAsync(worker, occurrence)).ResolvedAt);
    }

    [Fact]
    public async Task M3_IncidentsAreDeliveredAgainAfterACrashAndTheOpeningBeforeTheResolution()
    {
        var definition = Definition(new JobPolicy { Attempts = 1, Expect = Expect.FailsAtMostInARow(0) }, subject: "feed");
        var (first, second) = (Subject.Of("feed", "first"), Subject.Of("feed", "second"));
        var worker = await WorkerAsync(definition, FailOrComplete(), pollingInterval: Polling);
        var store = worker.Services.GetRequiredService<IWorkStore>();

        // A process claims the first incident for delivery and crashes before delivering it. Incidents of other
        // scenarios may be due in the same store, so it claims this one by its key.
        await EndAsync(worker, definition, first, "fail", JobOutcome.Failed);
        var opened = Assert.Single(await IncidentsAsync(worker, definition));
        var crashed = LeaseSource.Define(IncidentSource.Name, store.Monitoring.Incidents, IncidentSource.Codec, new LeasePolicy { Duration = TimeSpan.FromSeconds(1) });
        Assert.NotNull(await store.Leases.ClaimAsync(crashed, LeaseCallbacks<Guid>.None, OwnerId.New(), opened.Id, Cancel));

        // The second incident resolves before its opening was delivered.
        await EndAsync(worker, definition, second, "fail", JobOutcome.Failed);
        await EndAsync(worker, definition, second, "ok", JobOutcome.Completed);
        var resolved = Assert.Single(await IncidentsAsync(worker, definition), i => i.Subject == second);
        Assert.NotNull(resolved.ResolvedAt);

        // A handler, whose first call fails, receives every event: the crashed claim's again once its lease expired, and
        // the resolved incident's opening before its resolution.
        var script = new IncidentScript();
        var failures = 0;
        script.Then = (_, _, _) => Interlocked.Increment(ref failures) == 1 ? throw new InvalidOperationException("handler unavailable") : ValueTask.CompletedTask;
        var handler = await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(script);
                work.OnIncident<ScriptedIncidentHandler>();
            },
            pollingInterval: Polling);
        await EventuallyAsync(() => script.Delivered(opened.Id).Length == 1 && script.Delivered(resolved.Id).Length == 2, TimeSpan.FromSeconds(15), "every event is delivered");
        Assert.Equal(["opened"], script.Delivered(opened.Id));
        Assert.Equal(["opened", "resolved"], script.Delivered(resolved.Id));
        Assert.Equal(opened with { }, script.Calls.First(c => c.Incident.Id == opened.Id).Incident);

        // Nothing is delivered twice; the first incident's resolution is delivered once it resolves.
        await Task.Delay(1000, Cancel);
        Assert.Equal((1, 2), (script.Delivered(opened.Id).Length, script.Delivered(resolved.Id).Length));
        await EndAsync(worker, definition, first, "ok", JobOutcome.Completed);
        await EventuallyAsync(() => script.Delivered(opened.Id).Length == 2, TimeSpan.FromSeconds(10), "the resolution is delivered");
        Assert.Equal(["opened", "resolved"], script.Delivered(opened.Id));
        Assert.NotNull(script.Calls.Last(c => c.Incident.Id == opened.Id).Incident.ResolvedAt);
        Assert.Equal(2, (await IncidentsAsync(worker, definition)).Count);
    }

    [Fact]
    public async Task M5_DefinitionsWithExpectationsAreReportedWhileNoLiveProcessMonitors()
    {
        var expecting = Definition(new JobPolicy { Expect = Expect.FailsAtMostInARow(1) });
        var plain = Definition();
        var client = await ClientAsync(null, expecting, plain);
        var jobs = Jobs(client);

        var unmonitored = await jobs.GetUnmonitoredAsync(Cancel);
        Assert.Contains(expecting.Key, unmonitored);
        Assert.DoesNotContain(plain.Key, unmonitored);

        var monitor = await ProcessAsync(w => w.AddMonitoring(), pollingInterval: Polling);
        Assert.Empty(await jobs.GetUnmonitoredAsync(Cancel));
        await monitor.StopAsync(Cancel);
        Assert.Contains(expecting.Key, await jobs.GetUnmonitoredAsync(Cancel));
    }

    [Fact]
    public async Task M6_WorkUnservedForLongerThanTheGraceOpensAnIncidentThatOnlyServingItResolves()
    {
        var policy = new JobPolicy { Expect = Expect.FailsAtMostInARow(1) };
        var oldDefinition = Definition(policy);
        var newDefinition = Definition(policy, argsVersion: 2, key: oldDefinition.Key);
        var oldWorker = await WorkerAsync(oldDefinition, new JobScript(), pollingInterval: Polling);
        var client = await ClientAsync(null, newDefinition);
        var monitor = await ProcessAsync(w => w.AddMonitoring().UnservedGrace(TimeSpan.FromSeconds(1)), pollingInterval: Polling);
        var waiting = await RequestAsync(client, newDefinition, "v2");

        await EventuallyAsync(async () => (await IncidentsAsync(monitor, newDefinition, openOnly: true)).Count > 0, TimeSpan.FromSeconds(10), "the incident opens");
        var incident = Assert.Single(await IncidentsAsync(monitor, newDefinition));
        Assert.Equal((WorkIncidentKind.Unserved, newDefinition.Key, (Subject?)null, (JobId?)null, (DateTimeOffset?)null), (incident.Kind, incident.Definition, incident.Subject, incident.Job, incident.ResolvedAt));

        // A success of the version the old worker runs resolves the key's expectation incidents, not this one.
        await EndedAsync((await RequestAsync(oldWorker, oldDefinition, "v1")).Id, TimeSpan.FromSeconds(10));
        await Task.Delay(Polling * 2, Cancel);
        Assert.Equal(incident.Id, Assert.Single(await IncidentsAsync(monitor, newDefinition, openOnly: true)).Id);

        await WorkerAsync(newDefinition, new JobScript(), configure: j => j.AcceptsArgs(1, 2), pollingInterval: Polling);
        await EndedAsync(waiting.Id, TimeSpan.FromSeconds(10));
        await EventuallyAsync(async () => (await IncidentsAsync(monitor, newDefinition, openOnly: true)).Count == 0, TimeSpan.FromSeconds(10), "the incident resolves");
        Assert.Equal(incident.Id, Assert.Single(await IncidentsAsync(monitor, newDefinition)).Id);
    }

    /// <summary>Requests a job of <paramref name="subject"/> and waits until it ends as <paramref name="expected"/>.</summary>
    protected async Task<JobId> EndAsync(TestProcess process, JobDefinition<TestArgs> definition, Subject subject, string args, JobOutcome expected)
    {
        var job = await RequestAsync(process, definition, args, options: For(subject, RequestMode.New));
        Assert.Equal(expected, (await EndedAsync(job.Id, TimeSpan.FromSeconds(10))).State);
        return job.Id;
    }

    /// <summary>A script that completes jobs once <paramref name="release"/> completes.</summary>
    private static JobScript Blocking(TaskCompletionSource release) => new()
    {
        Run = async (_, _, cancellationToken) =>
        {
            await release.Task.WaitAsync(cancellationToken);
            return JobResult.Completed;
        },
    };

    /// <summary>A script that fails jobs whose arguments are <c>fail</c> and completes the others.</summary>
    protected static JobScript FailOrComplete() => new()
    {
        Run = (args, _, _) => ValueTask.FromResult<JobDisposition>(args.Value == "fail" ? JobResult.Fail("broken") : JobResult.Completed),
    };

    /// <summary>The incidents of the definition, newest first.</summary>
    protected static async Task<IReadOnlyList<WorkIncident>> IncidentsAsync(TestProcess process, JobDefinition definition, bool openOnly = false) =>
        (await process.Services.GetRequiredService<IncidentOperations>().GetIncidentsAsync(openOnly, definition.Key, cancellationToken: Cancel)).Incidents;

    /// <summary>The local time of day of <paramref name="instant"/> in the clock's time zone.</summary>
    private static TimeOnly Local(CalendarClock clock, DateTimeOffset instant) => TimeOnly.FromDateTime(instant.UtcDateTime + clock.Offset);

    /// <summary>A schedule whose first occurrence is months away, so recurrences run nothing during a scenario.</summary>
    private static Schedule Yearly(CalendarClock clock) => Schedule.Cron("0 0 0 1 1 *", clock.Zone);

    private static string Name() => $"mon{Interlocked.Increment(ref names)}-{Guid.NewGuid():N}"[..16];
}

/// <summary>What a scenario's incident handler does: records every delivery, then does what <see cref="Then"/> does.</summary>
public sealed class IncidentScript
{
    public ConcurrentQueue<(string Event, WorkIncident Incident)> Calls { get; } = new();

    public Func<string, WorkIncident, CancellationToken, ValueTask> Then { get; set; } = (_, _, _) => ValueTask.CompletedTask;

    /// <summary>The events delivered for the incident, in order, without the calls that failed.</summary>
    public string[] Delivered(Guid incident) => [.. Calls.Where(c => c.Incident.Id == incident && c.Event[0] != '!').Select(c => c.Event)];
}

public sealed class ScriptedIncidentHandler(IncidentScript script) : IWorkIncidentHandler
{
    public ValueTask OpenedAsync(WorkIncident incident, CancellationToken cancellationToken) => CallAsync("opened", incident, cancellationToken);

    public ValueTask ResolvedAsync(WorkIncident incident, CancellationToken cancellationToken) => CallAsync("resolved", incident, cancellationToken);

    private async ValueTask CallAsync(string name, WorkIncident incident, CancellationToken cancellationToken)
    {
        try
        {
            await script.Then(name, incident, cancellationToken);
        }
        catch
        {
            script.Calls.Enqueue(("!" + name, incident));
            throw;
        }
        script.Calls.Enqueue((name, incident));
    }
}
