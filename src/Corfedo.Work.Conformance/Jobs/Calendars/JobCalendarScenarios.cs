using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>
/// Calendars on recurring and one-shot schedules: blocked instants, closed windows, retries, operator actions and
/// published date sets. Days and hours are scaled to seconds: calendars use windows a few seconds long around the
/// store's clock, in a fixed-offset time zone whose local time is near noon, so a scenario never crosses midnight.
/// Allowed days across weeks, windows crossing midnight and daylight-saving changes run as evaluator tests.
/// </summary>
public abstract class JobCalendarScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    private static readonly TimeSpan Polling = TimeSpan.FromMilliseconds(250);
    private static int names;

    [Fact]
    public async Task CAL1_ADailyCronRunsAtItsLocalTimeOnAllowedDaysAndIsRecordedSkippedOnBlockedOnes()
    {
        var clock = await ClockAsync();
        var definition = Definition(subject: "feed");
        var weekend = WorkCalendar.Create(Name(), 1).InTimeZone(clock.Zone)
            .AllowWeekdays([.. Enum.GetValues<DayOfWeek>().Where(d => d != clock.Today.DayOfWeek)]);
        var holidays = Name();
        var publicHolidays = WorkCalendar.Create(Name(), 1).InTimeZone(clock.Zone).ExcludeDates(holidays);
        var otherHolidays = Name();
        var business = WorkCalendar.Create(Name(), 1).InTimeZone(clock.Zone)
            .AllowBetween(new TimeOnly(6, 0), new TimeOnly(20, 0))
            .ExcludeDates(otherHolidays);
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var ended = new EndedScript();
        var worker = await WorkerAsync(definition, Occurrences(runs), ended, also: w => w.AddCalendar(weekend).AddCalendar(publicHolidays).AddCalendar(business), pollingInterval: Polling);
        Assert.True(await PublishAsync(worker, holidays, 1, [clock.Today], clock.Today.AddDays(30)));
        Assert.True(await PublishAsync(worker, otherHolidays, 1, [clock.Today.AddDays(1)], clock.Today.AddDays(30)));

        // A daily cron at a local time a few seconds ahead: blocked by the weekday, by a published holiday, and allowed by
        // a business-day calendar whose published holidays are other days.
        var instant = clock.WholeSecondAfter(TimeSpan.FromSeconds(3));
        var local = instant.ToOffset(clock.Offset);
        var cron = Schedule.Cron($"{local.Second} {local.Minute} {local.Hour} * * *", clock.Zone);
        await ScheduleAsync(worker, definition, "weekend", cron.Within(weekend.Name), Recurring(Subject.Of("feed", "weekend")));
        await ScheduleAsync(worker, definition, "holiday", cron.Within(publicHolidays.Name), Recurring(Subject.Of("feed", "holiday")));
        await ScheduleAsync(worker, definition, "business", cron.Within(business.Name), Recurring(Subject.Of("feed", "business")));

        var ran = Assert.Single(await RunsAsync(runs, 1, TimeSpan.FromSeconds(15)));
        await EventuallyAsync(() => ended.Calls.Count >= 3, TimeSpan.FromSeconds(10), "every occurrence ended");
        Assert.Equal("business", ran.Args);
        Assert.Equal(instant, ran.Instant);
        var calls = ended.Calls.ToDictionary(c => c.Subject!.Id);
        Assert.Equal((JobOutcome.Completed, (string?)null), (calls["business"].Outcome, calls["business"].Reason));
        foreach (var blocked in new[] { "weekend", "holiday" })
        {
            Assert.Equal((JobOutcome.Skipped, JobReasons.CalendarBlocked, 0), (calls[blocked].Outcome, calls[blocked].Reason, calls[blocked].Attempts));
            Assert.Equal(instant, DateTimeOffset.Parse(calls[blocked].Occurrence!, System.Globalization.CultureInfo.InvariantCulture));
        }
        await Task.Delay(1000, Cancel);
        Assert.Single(runs);
    }

    [Fact]
    public async Task CAL2_AOneShotJobAtABlockedInstantIsRefusedSkippedOrMovedForward()
    {
        var clock = await ClockAsync();
        var definition = Definition();
        var calendar = WorkCalendar.Create(Name(), 1).InTimeZone(clock.Zone).AllowBetween(clock.Local(TimeSpan.FromSeconds(4)), clock.Local(TimeSpan.FromSeconds(9)));
        var starts = new ConcurrentDictionary<string, DateTimeOffset>();
        var ended = new EndedScript();
        var worker = await WorkerAsync(definition, StartsIn(starts), ended, also: w => w.AddCalendar(calendar), pollingInterval: Polling);
        var blocked = clock.Start + TimeSpan.FromSeconds(1);

        var refused = await Assert.ThrowsAsync<CalendarBlockedException>(() => RequestAsync(worker, definition, "reject", Schedule.At(blocked).Within(calendar.Name, Blocked.Reject)));
        Assert.False(refused.Unavailable);
        var skip = await RequestAsync(worker, definition, "skip", Schedule.At(blocked).Within(calendar.Name, Blocked.Skip));
        var move = await RequestAsync(worker, definition, "move", Schedule.At(blocked).Within(calendar.Name));
        var allowed = await RequestAsync(worker, definition, "allowed", Schedule.At(clock.Start + TimeSpan.FromSeconds(5)).Within(calendar.Name, Blocked.Reject));

        var skipped = await EndedAsync(skip.Id, TimeSpan.FromSeconds(10));
        Assert.Equal((JobOutcome.Skipped, JobReasons.CalendarBlocked, 0), (skipped.State, skipped.Reason, skipped.Attempts));
        Assert.True(skipped.EndedAt >= blocked, $"ended at {skipped.EndedAt:O}, before its due time {blocked:O}");
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(move.Id, TimeSpan.FromSeconds(15))).State);
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(allowed.Id, TimeSpan.FromSeconds(15))).State);
        Assert.InRange(starts["move"], clock.Start + TimeSpan.FromSeconds(4), clock.Start + TimeSpan.FromSeconds(9));
        Assert.InRange(starts["allowed"], clock.Start + TimeSpan.FromSeconds(5), clock.Start + TimeSpan.FromSeconds(9));
        Assert.Equal(["allowed", "move"], starts.Keys.Order());
        Assert.Equal(3, (await ReadJobsAsync(definition)).Count);
    }

    [Fact]
    public async Task CAL3_AComposedCalendarAllowsWhatItsRuleAllows()
    {
        var clock = await ClockAsync();
        var definition = Definition();
        var lunch = WorkCalendar.Create(Name(), 1).InTimeZone(clock.Zone).AllowBetween(clock.Local(TimeSpan.FromSeconds(4)), clock.Local(TimeSpan.FromSeconds(7)));
        var office = WorkCalendar.Create(Name(), 1).InTimeZone(clock.Zone).AllowBetween(clock.Local(TimeSpan.FromSeconds(2)), clock.Local(TimeSpan.FromSeconds(11))).Except(lunch.Name);
        var starts = new ConcurrentDictionary<string, DateTimeOffset>();
        var worker = await WorkerAsync(definition, StartsIn(starts), also: w => w.AddCalendar(lunch).AddCalendar(office), pollingInterval: Polling);

        var before = await RequestAsync(worker, definition, "before", Schedule.At(clock.Start + TimeSpan.FromSeconds(3)).Within(office.Name, Blocked.Reject));
        await Assert.ThrowsAsync<CalendarBlockedException>(() => RequestAsync(worker, definition, "lunch", Schedule.At(clock.Start + TimeSpan.FromSeconds(5)).Within(office.Name, Blocked.Reject)));
        var moved = await RequestAsync(worker, definition, "moved", Schedule.At(clock.Start + TimeSpan.FromSeconds(5)).Within(office.Name));

        Assert.Equal(JobOutcome.Completed, (await EndedAsync(before.Id, TimeSpan.FromSeconds(15))).State);
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(moved.Id, TimeSpan.FromSeconds(15))).State);
        Assert.InRange(starts["before"], clock.Start + TimeSpan.FromSeconds(3), clock.Start + TimeSpan.FromSeconds(4));
        Assert.InRange(starts["moved"], clock.Start + TimeSpan.FromSeconds(7), clock.Start + TimeSpan.FromSeconds(11));
    }

    [Fact]
    public async Task CAL3_CalendarsComposingInACycleOrWithAnUnknownOneFailAtStartup()
    {
        var (a, b) = (Name(), Name());
        var cycle = await Assert.ThrowsAsync<InvalidOperationException>(() => ProcessAsync(w => w.AddCalendar(WorkCalendar.Create(a, 1).Except(b)).AddCalendar(WorkCalendar.Create(b, 1).Also(a))));
        Assert.Contains("cycle", cycle.Message, StringComparison.Ordinal);
        await Assert.ThrowsAsync<InvalidOperationException>(() => ProcessAsync(w => w.AddCalendar(WorkCalendar.Create(a, 1).Except(Name()))));
    }

    [Fact]
    public async Task CAL4_WorkThatCannotStartBeforeItsWindowClosesWaitsForTheNextWindowOrIsSkipped()
    {
        var clock = await ClockAsync();
        var definition = Definition();
        var calendar = WorkCalendar.Create(Name(), 1).InTimeZone(clock.Zone)
            .AllowBetween(clock.Local(TimeSpan.FromSeconds(2)), clock.Local(TimeSpan.FromSeconds(5)))
            .AllowBetween(clock.Local(TimeSpan.FromSeconds(9)), clock.Local(TimeSpan.FromSeconds(12)));
        var starts = new ConcurrentDictionary<string, DateTimeOffset>();
        var ended = new EndedScript();
        var worker = await WorkerAsync(
            definition,
            StartsIn(starts, async (args, cancellationToken) =>
            {
                if (args.Value == "blocker")
                {
                    // Holds the only slot until after the first window closed.
                    var left = clock.Start + TimeSpan.FromSeconds(6) - await Provider.NowAsync(cancellationToken);
                    await Task.Delay(left > TimeSpan.Zero ? left : TimeSpan.Zero, cancellationToken);
                }
            }),
            ended,
            configure: job => job.Capacity(1),
            also: w => w.AddCalendar(calendar),
            pollingInterval: Polling);

        var blocker = await RequestAsync(worker, definition, "blocker");
        await EventuallyAsync(() => starts.ContainsKey("blocker"), TimeSpan.FromSeconds(5), "the blocker started");
        var wait = await RequestAsync(worker, definition, "wait", Schedule.At(clock.Start + TimeSpan.FromSeconds(3)).Within(calendar.Name, closed: Closed.Wait));
        var skip = await RequestAsync(worker, definition, "skip", Schedule.At(clock.Start + TimeSpan.FromSeconds(3)).Within(calendar.Name, closed: Closed.Skip));

        var skipped = await EndedAsync(skip.Id, TimeSpan.FromSeconds(15));
        Assert.Equal((JobOutcome.Skipped, JobReasons.WindowClosed, 0), (skipped.State, skipped.Reason, skipped.Attempts));
        Assert.True(skipped.EndedAt >= clock.Start + TimeSpan.FromSeconds(5), $"ended at {skipped.EndedAt:O}, before the window closed");
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(wait.Id, TimeSpan.FromSeconds(20))).State);
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(blocker.Id, TimeSpan.FromSeconds(5))).State);
        Assert.InRange(starts["wait"], clock.Start + TimeSpan.FromSeconds(9), clock.Start + TimeSpan.FromSeconds(12));
        Assert.Equal(["blocker", "wait"], starts.Keys.Order());
        Assert.Contains(ended.Calls, c => c.Job == skip.Id && c.Reason == JobReasons.WindowClosed);
    }

    [Fact]
    public async Task CAL5_ARetryWhoseBackoffEndsOutsideTheWindowStartsInTheNextOneWithoutAnExtraAttempt()
    {
        var clock = await ClockAsync();
        var definition = Definition(new JobPolicy { Attempts = 3, Backoff = Backoff.None });
        var calendar = WorkCalendar.Create(Name(), 1).InTimeZone(clock.Zone)
            .AllowBetween(clock.Local(TimeSpan.FromSeconds(1)), clock.Local(TimeSpan.FromSeconds(4)))
            .AllowBetween(clock.Local(TimeSpan.FromSeconds(8)), clock.Local(TimeSpan.FromSeconds(11)));
        var attempts = new ConcurrentQueue<(int Attempt, DateTimeOffset At)>();
        var worker = await WorkerAsync(
            definition,
            new JobScript
            {
                Run = async (_, run, cancellationToken) =>
                {
                    attempts.Enqueue((run.Attempt, await Provider.NowAsync(cancellationToken)));
                    return run.Attempt == 1 ? JobResult.Retry(TimeSpan.FromSeconds(4), "later") : JobResult.Completed;
                },
            },
            also: w => w.AddCalendar(calendar),
            pollingInterval: Polling);

        var job = await RequestAsync(worker, definition, "x", Schedule.At(clock.Start + TimeSpan.FromMilliseconds(1500)).Within(calendar.Name));

        var state = await EndedAsync(job.Id, TimeSpan.FromSeconds(20));
        Assert.Equal((JobOutcome.Completed, 2), (state.State, state.Attempts));
        var observed = attempts.ToArray();
        Assert.Equal([1, 2], observed.Select(a => a.Attempt));
        Assert.InRange(observed[0].At, clock.Start + TimeSpan.FromMilliseconds(1500), clock.Start + TimeSpan.FromSeconds(4));
        Assert.InRange(observed[1].At, clock.Start + TimeSpan.FromSeconds(8), clock.Start + TimeSpan.FromSeconds(11));
    }

    [Fact]
    public async Task CAL5_OperatorActionsOutsideTheWindowWaitForItUnlessTheyIgnoreTheCalendar()
    {
        var clock = await ClockAsync();
        var definition = Definition(subject: "feed");
        var calendar = WorkCalendar.Create(Name(), 1).InTimeZone(clock.Zone).AllowBetween(clock.Local(TimeSpan.FromSeconds(6)), clock.Local(TimeSpan.FromSeconds(9)));
        var starts = new ConcurrentDictionary<string, DateTimeOffset>();
        var worker = await WorkerAsync(definition, StartsIn(starts), configure: job => job.Capacity(4), also: w => w.AddCalendar(calendar), pollingInterval: Polling);
        var yearly = Schedule.Cron("0 0 0 1 1 *", clock.Zone).Within(calendar.Name);
        await ScheduleAsync(worker, definition, "respects", yearly, Recurring(Subject.Of("feed", "respects")));
        await ScheduleAsync(worker, definition, "ignores", yearly, Recurring(Subject.Of("feed", "ignores")));
        var standalone = await RequestAsync(worker, definition, "run-now", Schedule.At(clock.Start + TimeSpan.FromSeconds(1)).Within(calendar.Name), For(Subject.Of("feed", "run-now")));

        var respects = await InTransactionAsync(t => Jobs(worker).TriggerAsync(t, definition, Subject.Of("feed", "respects"), Cancel));
        var ignores = await InTransactionAsync(t => Jobs(worker).TriggerAsync(t, definition, Subject.Of("feed", "ignores"), JobActionOptions.IgnoreCalendar, Cancel));
        Assert.Equal((RunNowOutcome.Started, RunNowOutcome.Started), (respects.Outcome, ignores.Outcome));
        Assert.Equal(new RunNowResult(standalone.Id, RunNowOutcome.MadeDue), await Jobs(worker).RunNowAsync(definition, standalone.Id, JobActionOptions.IgnoreCalendar, Cancel));

        Assert.Equal(JobOutcome.Completed, (await EndedAsync(ignores.Job, TimeSpan.FromSeconds(5))).State);
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(standalone.Id, TimeSpan.FromSeconds(5))).State);
        Assert.True(starts["ignores"] < clock.Start + TimeSpan.FromSeconds(6), $"the trigger ignoring the calendar started at {starts["ignores"]:O}");
        Assert.True(starts["run-now"] < clock.Start + TimeSpan.FromSeconds(6), $"the job run now ignoring the calendar started at {starts["run-now"]:O}");
        Assert.False(starts.ContainsKey("respects"));
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(respects.Job, TimeSpan.FromSeconds(15))).State);
        Assert.InRange(starts["respects"], clock.Start + TimeSpan.FromSeconds(6), clock.Start + TimeSpan.FromSeconds(9));
    }

    [Fact]
    public async Task CAL6_PublishedDateSetsPlanEveryProcessAlikeAndUncoveredInstantsWaitForCoverage()
    {
        var clock = await ClockAsync();
        var definition = Definition(subject: "feed");
        var holidays = Name();
        var calendar = WorkCalendar.Create(Name(), 1).InTimeZone(clock.Zone).ExcludeDates(holidays);
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var started = 0;
        var release = new TaskCompletionSource();
        JobScript Script() => Occurrences(runs, async (_, _, cancellationToken) =>
        {
            Interlocked.Increment(ref started);
            await release.Task.WaitAsync(cancellationToken);
            return JobResult.Completed;
        });
        var first = await WorkerAsync(definition, Script(), also: w => w.AddCalendar(calendar), pollingInterval: Polling);
        var second = await WorkerAsync(definition, Script(), also: w => w.AddCalendar(calendar), pollingInterval: Polling);

        // Coverage ends yesterday: occurrences planned today wait, and the date set is reported with work waiting.
        Assert.True(await PublishAsync(first, holidays, 1, [], clock.Today.AddDays(-1)));
        await ScheduleAsync(first, definition, "x", Schedule.Cron("* * * * * *", clock.Zone).Within(calendar.Name), Recurring(Subject.Of("feed", "a"), Overlap.Allow));
        await EventuallyAsync(async () => (await ReadJobsAsync(definition)).Count(j => j.CalendarUnavailable) >= 3, TimeSpan.FromSeconds(10), "occurrences wait for coverage");
        Assert.All(await ReadJobsAsync(definition), j => Assert.Equal((null, (TimeSpan?)null), (j.State, j.DueIn)));
        Assert.Equal(0, Volatile.Read(ref started));
        var waiting = await CoverageAsync(second, holidays);
        Assert.Equal(((long?)1, (DateOnly?)clock.Today.AddDays(-1), DateSetCoverageState.WorkWaiting), (waiting.Revision, waiting.CoversThrough, waiting.State));
        Assert.Equal([calendar.Name], waiting.Calendars);
        await EventuallyAsync(() => first.Logs.Events.Contains("calendar.work_waiting"), TimeSpan.FromSeconds(5), "the waiting work is logged");

        // A covering revision plans them: two run, one per process, the others wait for capacity. The set is covered.
        Assert.True(await PublishAsync(second, holidays, 2, [], clock.Today.AddDays(30)));
        var covered = await CoverageAsync(first, holidays);
        Assert.Equal((DateSetCoverageState.Covered, (long?)2), (covered.State, covered.Revision));
        await EventuallyAsync(() => first.Logs.Events.Contains("calendar.date_set_covered"), TimeSpan.FromSeconds(5), "the covered set is logged");
        await EventuallyAsync(() => Volatile.Read(ref started) == 2, TimeSpan.FromSeconds(10), "two occurrences started");
        await EventuallyAsync(async () => (await ReadJobsAsync(definition)).Any(j => j is { State: null, Held: false, CalendarUnavailable: false, DueIn: not null }), TimeSpan.FromSeconds(5), "planned occurrences wait for capacity");

        // A newer revision with today as a holiday plans them again: they are recorded skipped, and so is every later one.
        Assert.True(await PublishAsync(first, holidays, 3, [clock.Today], clock.Today.AddDays(30)));
        Assert.False(await PublishAsync(second, holidays, 2, [], clock.Today.AddDays(30)));
        Assert.False(await PublishAsync(second, holidays, 3, [clock.Today], clock.Today.AddDays(30)));
        await Assert.ThrowsAsync<CalendarConflictException>(() => PublishAsync(second, holidays, 3, [], clock.Today.AddDays(30)));
        await EventuallyAsync(async () => (await ReadJobsAsync(definition)).All(j => j.State is not null || j.Held || j.SkipPending), TimeSpan.FromSeconds(10), "only running occurrences remain");
        release.SetResult();
        await RunsAsync(runs, 2, TimeSpan.FromSeconds(5));
        await Task.Delay(1500, Cancel);

        var jobs = await ReadJobsAsync(definition);
        Assert.Equal((2, 2), (Volatile.Read(ref started), runs.Count));
        Assert.Equal(jobs.Count, jobs.Select(j => j.Occurrence).Distinct().Count());
        Assert.All(jobs, j => Assert.Equal(0, DateTimeOffset.Parse(j.Occurrence!, System.Globalization.CultureInfo.InvariantCulture).Millisecond));
        Assert.Equal(2, jobs.Count(j => j.State == JobOutcome.Completed));
        Assert.All(jobs.Where(j => j.State is not (null or JobOutcome.Completed)), j => Assert.Equal((JobOutcome.Skipped, JobReasons.CalendarBlocked), (j.State, j.Reason)));
    }

    [Fact]
    public async Task CAL6_ADateSetIsReportedRunningOutWithinTheMarginOfTheInstantsPlanningNeeds()
    {
        var clock = await ClockAsync();
        var definition = Definition(subject: "feed");
        var (holidays, closures) = (Name(), Name());
        var calendar = WorkCalendar.Create(Name(), 1).InTimeZone(clock.Zone).ExcludeDates(holidays).ExcludeDates(closures);
        var worker = await WorkerAsync(definition, new JobScript(), also: w => w.AddCalendar(calendar).CalendarCoverage(TimeSpan.FromDays(10)), pollingInterval: Polling);
        async Task<DateSetCoverageState> StateAsync(string set) => (await CoverageAsync(worker, set)).State;

        // Unpublished sets run out; one covering only a few days ahead runs out by now, one covering 30 days does not.
        var unpublished = await CoverageAsync(worker, holidays);
        Assert.Equal(((long?)null, (DateOnly?)null, DateSetCoverageState.RunningOut), (unpublished.Revision, unpublished.CoversThrough, unpublished.State));
        Assert.Equal([calendar.Name], unpublished.Calendars);
        Assert.True(await PublishAsync(worker, holidays, 1, [], clock.Today.AddDays(30)));
        Assert.True(await PublishAsync(worker, closures, 1, [], clock.Today.AddDays(3)));
        Assert.Equal((DateSetCoverageState.Covered, DateSetCoverageState.RunningOut), (await StateAsync(holidays), await StateAsync(closures)));
        await EventuallyAsync(() => worker.Logs.Events.Contains("calendar.date_set_running_out"), TimeSpan.FromSeconds(5), "the set running out is logged");
        Assert.True(await PublishAsync(worker, closures, 2, [], clock.Today.AddDays(400)));

        // A job to be planned in 25 days needs dates within the margin of the 30 covered.
        await RequestAsync(worker, definition, "later", Schedule.At(clock.Start + TimeSpan.FromDays(25)).Within(calendar.Name), For(Subject.Of("feed", "later")));
        Assert.Equal(DateSetCoverageState.RunningOut, await StateAsync(holidays));
        Assert.True(await PublishAsync(worker, holidays, 2, [], clock.Today.AddDays(60)));
        Assert.Equal(DateSetCoverageState.Covered, await StateAsync(holidays));

        // So does a recurrence whose next instant is in 55 days.
        var date = clock.Today.AddDays(55);
        date = date is { Month: 2, Day: 29 } ? date.AddDays(1) : date;
        await ScheduleAsync(worker, definition, "yearly", Schedule.Cron($"0 0 0 {date.Day} {date.Month} *", clock.Zone).Within(calendar.Name), Recurring(Subject.Of("feed", "yearly")));
        Assert.Equal(DateSetCoverageState.RunningOut, await StateAsync(holidays));
        Assert.True(await PublishAsync(worker, holidays, 3, [], clock.Today.AddDays(120)));
        Assert.Equal(DateSetCoverageState.Covered, await StateAsync(holidays));
    }

    [Fact]
    public async Task CAL6_AMonitoringProcessOpensDateSetIncidentsWhileTheirConditionsHoldAndResolvesThem()
    {
        var clock = await ClockAsync();
        var definition = Definition(subject: "feed");
        var holidays = Name();
        var calendar = WorkCalendar.Create(Name(), 1).InTimeZone(clock.Zone).ExcludeDates(holidays);
        var worker = await WorkerAsync(definition, new JobScript(), also: w => w.AddCalendar(calendar).CalendarCoverage(TimeSpan.FromDays(10)).AddMonitoring(), pollingInterval: Polling);
        var incidents = worker.Services.GetRequiredService<IncidentOperations>();
        async Task<WorkIncident[]> OpenAsync() =>
            [.. (await incidents.GetIncidentsAsync(openOnly: true, cancellationToken: Cancel)).Incidents.Where(i => i.DateSet == holidays).OrderBy(i => i.Kind)];

        // An unpublished set runs out.
        await EventuallyAsync(async () => (await OpenAsync()).Length > 0, TimeSpan.FromSeconds(5), "running out opens");
        var runningOut = Assert.Single(await OpenAsync());
        Assert.Equal((WorkIncidentKind.DateSetRunningOut, (string?)null, (Subject?)null), (runningOut.Kind, runningOut.Definition, runningOut.Subject));

        // Work waiting for it opens the second incident while the first stays open.
        Assert.True(await PublishAsync(worker, holidays, 1, [], clock.Today.AddDays(-1)));
        await RequestAsync(worker, definition, "now", Schedule.Now.Within(calendar.Name), For(Subject.Of("feed", "now")));
        await EventuallyAsync(async () => (await OpenAsync()).Length == 2, TimeSpan.FromSeconds(5), "work waiting opens");
        var open = await OpenAsync();
        Assert.Equal(runningOut.Id, open[0].Id);
        Assert.Equal(WorkIncidentKind.DateSetWorkWaiting, open[1].Kind);

        // Coverage within the margin resolves the waiting work only; coverage beyond it resolves running out.
        Assert.True(await PublishAsync(worker, holidays, 2, [], clock.Today.AddDays(3)));
        await EventuallyAsync(async () => (await OpenAsync()).Length == 1, TimeSpan.FromSeconds(5), "work waiting resolves");
        Assert.Equal(runningOut.Id, Assert.Single(await OpenAsync()).Id);
        Assert.True(await PublishAsync(worker, holidays, 3, [], clock.Today.AddDays(400)));
        await EventuallyAsync(async () => (await OpenAsync()).Length == 0, TimeSpan.FromSeconds(5), "running out resolves");
        var all = (await incidents.GetIncidentsAsync(cancellationToken: Cancel)).Incidents.Where(i => i.DateSet == holidays).ToArray();
        Assert.Equal(2, all.Length);
        Assert.All(all, i => Assert.NotNull(i.ResolvedAt));
    }

    [Fact]
    public async Task CAL7_ACalendarIsRemovedOnlyOnceNoLiveWorkUsesItAndARerunOfItsEndedJobIsThenRefused()
    {
        var clock = await ClockAsync();
        var definition = Definition(subject: "feed");
        var calendar = WorkCalendar.Create(Name(), 1).InTimeZone(clock.Zone);
        var composing = WorkCalendar.Create(Name(), 1).InTimeZone(clock.Zone).Except(calendar.Name);
        var worker = await WorkerAsync(definition, new JobScript(), also: w => w.AddCalendar(calendar).AddCalendar(composing), pollingInterval: Polling);
        var calendars = worker.Services.GetRequiredService<CalendarOperations>();

        var ran = await RequestAsync(worker, definition, "now", Schedule.Now.Within(calendar.Name), For(Subject.Of("feed", "now")));
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(ran.Id, TimeSpan.FromSeconds(10))).State);
        await RequestAsync(worker, definition, "later", Schedule.At(clock.Start + TimeSpan.FromDays(1)).Within(calendar.Name), For(Subject.Of("feed", "later")));
        await ScheduleAsync(worker, definition, "yearly", Schedule.Cron("0 0 0 1 1 *", clock.Zone).Within(calendar.Name), Recurring(Subject.Of("feed", "yearly")));

        // Live work blocks the removal, and the refusal names it.
        var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => calendars.RemoveAsync(calendar.Name, Cancel).AsTask());
        Assert.Contains($"{definition.Key} (1)", refused.Message, StringComparison.Ordinal);
        Assert.Contains("1 recurrences", refused.Message, StringComparison.Ordinal);
        Assert.Contains(composing.Name, refused.Message, StringComparison.Ordinal);

        // Once only ended jobs remain, it is removed; a rerun of one of them is refused, also when it ignores the calendar.
        Assert.Equal(1, await InTransactionAsync(t => Jobs(worker).CancelAsync(t, definition, Subject.Of("feed", "later"), Cancel)));
        Assert.True(await InTransactionAsync(t => Jobs(worker).RemoveAsync(t, definition, Subject.Of("feed", "yearly"), Cancel)));
        Assert.True(await calendars.RemoveAsync(composing.Name, Cancel));
        Assert.True(await calendars.RemoveAsync(calendar.Name, Cancel));
        Assert.False(await calendars.RemoveAsync(calendar.Name, Cancel));
        Assert.Equal(new RerunResult(ran.Id, RerunOutcome.CalendarRemoved), await Jobs(worker).RerunAsync(definition, ran.Id, Cancel));
        Assert.Equal(new RerunResult(ran.Id, RerunOutcome.CalendarRemoved), await Jobs(worker).RerunAsync(definition, ran.Id, JobActionOptions.IgnoreCalendar, Cancel));
        Assert.Equal(JobOutcome.Completed, (await ReadJobAsync(ran.Id)).State);

        // Stored again, the rerun applies the calendar again.
        await worker.Services.GetRequiredService<IWorkStore>().Calendars.StoreCalendarAsync(calendar, Cancel);
        Assert.Equal(new RerunResult(ran.Id, RerunOutcome.Rerun), await Jobs(worker).RerunAsync(definition, ran.Id, Cancel));
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(ran.Id, TimeSpan.FromSeconds(10))).State);
    }

    [Fact]
    public async Task CAL7_StoredCalendarsThatNoLiveProcessRegistersAreReported()
    {
        var (kept, left) = (WorkCalendar.Create(Name(), 1), WorkCalendar.Create(Name(), 2));
        var live = await ProcessAsync(w => w.AddCalendar(kept));
        var stopping = await ProcessAsync(w => w.AddCalendar(left).AddCalendar(kept));
        var calendars = live.Services.GetRequiredService<CalendarOperations>();
        async Task<string[]> UnregisteredAsync() => [.. (await calendars.GetUnregisteredCalendarsAsync(Cancel)).Select(c => c.Name).Where(n => n == kept.Name || n == left.Name)];

        Assert.Empty(await UnregisteredAsync());
        await stopping.StopAsync(Cancel);
        var unregistered = Assert.Single((await calendars.GetUnregisteredCalendarsAsync(Cancel)).Where(c => c.Name == left.Name || c.Name == kept.Name));
        Assert.Equal((left.Name, 2L), (unregistered.Name, unregistered.Revision));
        Assert.True(await calendars.RemoveAsync(left.Name, Cancel));
        Assert.Empty(await UnregisteredAsync());
    }

    private static async Task<DateSetCoverage> CoverageAsync(TestProcess process, string name) =>
        Assert.Single((await process.Services.GetRequiredService<CalendarOperations>().GetDateSetCoverageAsync(Cancel)).Where(c => c.Name == name));

    private Task<bool> PublishAsync(TestProcess process, string name, long revision, DateOnly[] dates, DateOnly coversThrough) =>
        InTransactionAsync(t => process.Services.GetRequiredService<CalendarOperations>().PublishDatesAsync(t, name, revision, dates, coversThrough, Cancel));

    /// <summary>A script that records when each job started by the store's clock, after doing what <paramref name="then"/> does.</summary>
    private JobScript StartsIn(ConcurrentDictionary<string, DateTimeOffset> starts, Func<TestArgs, CancellationToken, Task>? then = null) => new()
    {
        Run = async (args, _, cancellationToken) =>
        {
            starts[args.Value] = await Provider.NowAsync(cancellationToken);
            if (then is not null)
            {
                await then(args, cancellationToken);
            }
            return JobResult.Completed;
        },
    };

    private static string Name(string? suffix = null) => $"cal{Interlocked.Increment(ref names)}-{Guid.NewGuid():N}"[..16] + suffix;
}
