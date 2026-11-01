using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>Two applications in one database with the same definition keys, calendar names and subjects (R19).</summary>
public abstract class ApplicationIsolationScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    private static readonly TimeSpan Short = TimeSpan.FromSeconds(1);

    [Fact]
    public async Task H2_TwoApplicationsWithTheSameKeysNeitherClaimReadReportMonitorDeliverNorRetainTheOthersWork()
    {
        var shared = Definition(new JobPolicy { Attempts = 1, Expect = Expect.FailsAtMostInARow(0), Retention = Short }, subject: "feed");
        var idle = Definition(new JobPolicy { Retention = Short });
        var calendar = "c-" + Guid.NewGuid().ToString("N")[..12];
        var type = "t-" + Guid.NewGuid().ToString("N")[..12];
        var subject = Subject.Of("feed", "npm");
        var orders = new Application();
        var billing = new Application();

        // The applications register the same calendar name with different rules and the same definition key; in one
        // store the second start would conflict with the first.
        var ordersWorker = await ProcessAsync(work =>
        {
            orders.Register(work, shared, type);
            work.AddCalendar(WorkCalendar.Create(calendar, 1).AllowWeekdays([.. Enum.GetValues<DayOfWeek>()]));
            work.AddMonitoring().OnIncident<ScriptedIncidentHandler>().AddRetention();
        });
        var billingWorker = await ProcessAsync(
            work =>
            {
                billing.Register(work, shared, type);
                work.AddCalendar(WorkCalendar.Create(calendar, 1).AllowWeekdays([DayOfWeek.Monday]));
                work.AddJobDefinition(idle);
            },
            application: "billing");

        // Each application claims only its own job of the same definition and subject.
        var ordersJob = await RequestAsync(ordersWorker, shared, "orders", options: For(subject));
        var billingJob = await RequestAsync(billingWorker, shared, "fail", options: For(subject));
        await InTransactionAsync(tx => Jobs(billingWorker).RequestAsync(tx, idle, new TestArgs("idle"), cancellationToken: Cancel));
        await EventuallyAsync(async () => await Jobs(ordersWorker).GetOutcomeAsync(shared, ordersJob.Id, Cancel) is { Ended: true }, Settles, "the orders job ends");
        await EventuallyAsync(async () => await Jobs(billingWorker).GetOutcomeAsync(shared, billingJob.Id, Cancel) is { Ended: true }, Settles, "the billing job ends");
        Assert.Equal(["orders"], orders.Runs);
        Assert.Equal(["fail"], billing.Runs);

        // Reads see only the application's own jobs.
        Assert.Null(await Jobs(ordersWorker).GetOutcomeAsync(shared, billingJob.Id, Cancel));
        Assert.Null(await Jobs(billingWorker).GetOutcomeAsync(shared, ordersJob.Id, Cancel));
        Assert.Equal(JobOutcome.Completed, (await Jobs(ordersWorker).GetAsync(shared, subject, Cancel))!.LastOutcome);
        Assert.Equal(JobOutcome.Failed, (await Jobs(billingWorker).GetAsync(shared, subject, Cancel))!.LastOutcome);

        // Reports: the billing definition no process runs, and the expectation only orders monitors.
        Assert.DoesNotContain((await Jobs(ordersWorker).GetUnservedAsync(Cancel)).Definitions, d => d.Definition == idle.Key);
        await EventuallyAsync(async () => (await Jobs(billingWorker).GetUnservedAsync(Cancel)).Definitions.Any(d => d.Definition == idle.Key), Settles, "billing reports its unserved definition");
        Assert.DoesNotContain(shared.Key, await Jobs(ordersWorker).GetUnmonitoredAsync(Cancel));
        Assert.Contains(shared.Key, await Jobs(billingWorker).GetUnmonitoredAsync(Cancel));
        Assert.Equal([ordersWorker.Owner], (await Registry(ordersWorker).ListAsync(type, cancellationToken: Cancel)).Select(p => p.Owner));
        Assert.Equal([billingWorker.Owner], (await Registry(billingWorker).ListAsync(type, cancellationToken: Cancel)).Select(p => p.Owner));

        // The failed billing job opens an incident in billing; orders neither lists nor delivers it.
        Assert.Single((await Incidents(billingWorker).GetIncidentsAsync(definition: shared.Key, cancellationToken: Cancel)).Incidents);
        Assert.Empty((await Incidents(ordersWorker).GetIncidentsAsync(definition: shared.Key, cancellationToken: Cancel)).Incidents);

        // Retention in orders deletes its ended job and leaves billing's, ended as long ago.
        await Task.Delay(Short + TimeSpan.FromMilliseconds(500), Cancel);
        var retention = await InTransactionAsync(tx => Jobs(ordersWorker).TriggerAsync(tx, WorkRetention.Definition, Cancel));
        await EventuallyAsync(async () => await Jobs(ordersWorker).GetOutcomeAsync(WorkRetention.Definition, retention.Job, Cancel) is { Ended: true }, Settles, "retention runs");
        Assert.Null(await Jobs(ordersWorker).GetOutcomeAsync(shared, ordersJob.Id, Cancel));
        Assert.NotNull(await Jobs(billingWorker).GetOutcomeAsync(shared, billingJob.Id, Cancel));
        Assert.Empty(orders.Script.Calls);
    }

    private static readonly TimeSpan Settles = TimeSpan.FromSeconds(15);

    private static ProcessRegistry Registry(TestProcess process) => process.Services.GetRequiredService<ProcessRegistry>();

    private static IncidentOperations Incidents(TestProcess process) => process.Services.GetRequiredService<IncidentOperations>();

    /// <summary>One application's handler, which fails jobs whose argument is <c>fail</c>, and its incident handler.</summary>
    private sealed class Application
    {
        public ConcurrentQueue<string> Runs { get; } = new();

        public IncidentScript Script { get; } = new();

        public void Register(WorkBuilder work, JobDefinition<TestArgs> definition, string type)
        {
            work.Services.AddSingleton(new JobScript
            {
                Run = (args, _, _) =>
                {
                    Runs.Enqueue(args.Value);
                    return ValueTask.FromResult<JobDisposition>(args.Value == "fail" ? JobResult.Fail("broken") : JobResult.Completed);
                },
            });
            work.Services.AddSingleton(Script);
            work.Process(p => p.Type(type));
            work.AddJob<TestArgs, ScriptedJobHandler>(definition, job => job.Lease(TimeSpan.FromSeconds(2)));
        }
    }
}
