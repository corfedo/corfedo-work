using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>Scenarios that span days of a clock the scenario advances (H-6); stores whose clock follows real time skip them.</summary>
public abstract class ControlledClockScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    [Fact]
    public async Task H6_ADailyCompletionDeadlineIsMetMissedAndResolvedOverThreeDaysOfTheControlledClock()
    {
        var clock = RequireControlledClock();
        var start = await Provider.NowAsync(Cancel);
        var day = new DateTimeOffset(start.UtcDateTime.Date, TimeSpan.Zero);
        var (runAt, deadline) = (TimeSpan.FromHours(8), TimeSpan.FromHours(9));
        Assert.True(start < day + runAt, "the scenario starts before the first run");
        var definition = Definition(new JobPolicy { Attempts = 1, Expect = Expect.CompletesBy(TimeOnly.FromTimeSpan(deadline), "UTC") }, subject: "feed");
        var feed = Subject.Of("feed", "npm");
        var polling = TimeSpan.FromMinutes(1);
        var runs = new ConcurrentQueue<DateTimeOffset>();
        var script = new JobScript
        {
            Run = (_, _, _) =>
            {
                var now = clock.Clock.GetUtcNow();
                runs.Enqueue(now);
                // The second day's run fails, so its deadline is missed.
                return ValueTask.FromResult<JobDisposition>(now - day < TimeSpan.FromDays(1) || now - day >= TimeSpan.FromDays(2) ? JobResult.Completed : JobResult.Fail("broken"));
            },
        };
        var worker = await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(script);
                // A dispatcher measures a claim's validity from before its claim query, so a claim in flight while the clock
                // jumps further than the lease would find its lease used up and not run; the lease outlasts every jump.
                work.AddJob<TestArgs, ScriptedJobHandler>(definition, job => job.Lease(TimeSpan.FromDays(3)));
                work.AddMonitoring();
            },
            time: clock.Clock,
            pollingInterval: polling);
        await ScheduleAsync(worker, definition, "daily", Schedule.Cron("0 8 * * *", "UTC"), Recurring(feed));
        var store = worker.Services.GetRequiredService<IWorkStore>();
        await EventuallyAsync(
            async () => (await store.Monitoring.GetExpectationsAsync(Cancel)).Definitions.Single(d => d.Definition == definition.Key) is { HasChanges: false },
            TimeSpan.FromSeconds(10),
            "the monitoring loop tracks the recurrence's key");

        // A background loop arms its next timer from the clock's time when it gets to it, which may be after an advance
        // returned; every loop wakes at least once per polling interval, so the clock then moves on interval by interval.
        async Task AdvanceUntilAsync(DateTimeOffset at, Func<Task<bool>> condition, string because)
        {
            clock.Advance(at - await Provider.NowAsync(Cancel));
            for (var steps = 0; !await condition(); steps++)
            {
                var waited = Stopwatch.StartNew();
                while (waited.Elapsed < TimeSpan.FromSeconds(1) && !await condition())
                {
                    await Task.Delay(20, Cancel);
                }
                if (await condition())
                {
                    return;
                }
                Assert.True(steps < 10, $"Not within ten polling intervals after {at:O}: {because}; {runs.Count} runs.");
                clock.Advance(polling);
            }
        }
        async Task<bool> EndedAsync(int count) => (await ReadJobsAsync(definition)).Count(j => j.State is not null) == count;

        // Day one completes before its deadline.
        await AdvanceUntilAsync(day + runAt, () => EndedAsync(1), "the first day's occurrence ends");
        // Day two fails, and its deadline passes without a completion.
        await AdvanceUntilAsync(day + TimeSpan.FromDays(1) + runAt, () => EndedAsync(2), "the second day's occurrence ends");
        Assert.Empty(await IncidentsAsync(worker, definition));
        await AdvanceUntilAsync(day + TimeSpan.FromDays(1) + deadline, async () => (await IncidentsAsync(worker, definition)).Count > 0, "the missed deadline opens an incident");
        // Day three completes and resolves it.
        await AdvanceUntilAsync(
            day + TimeSpan.FromDays(2) + runAt,
            async () => (await IncidentsAsync(worker, definition)).All(i => i.ResolvedAt is not null),
            "the third day's completion resolves the incident");

        var observed = runs.ToArray();
        Assert.Equal(3, observed.Length);
        for (var i = 0; i < observed.Length; i++)
        {
            Assert.InRange(observed[i], day + TimeSpan.FromDays(i) + runAt, day + TimeSpan.FromDays(i) + runAt + (polling * 10));
        }
        var incident = Assert.Single(await IncidentsAsync(worker, definition));
        Assert.Equal(
            (WorkIncidentKind.CompletesBy, (Subject?)feed, day + TimeSpan.FromDays(1) + deadline, (DateTimeOffset?)observed[2]),
            (incident.Kind, incident.Subject, incident.OpenedAt, incident.ResolvedAt));
    }

    private static async Task<IReadOnlyList<WorkIncident>> IncidentsAsync(TestProcess process, JobDefinition definition) =>
        (await process.Services.GetRequiredService<IncidentOperations>().GetIncidentsAsync(false, definition.Key, cancellationToken: Cancel)).Incidents;
}
