using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>Process records: registration with and without work, heartbeat, states, listing and retention (J12).</summary>
public abstract class ProcessRegistryScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    /// <summary>A process type unique to this run of the suite.</summary>
    private static string NewType() => "t-" + Guid.NewGuid().ToString("N")[..12];

    /// <summary>A heartbeat every 200 ms, stale after a second.</summary>
    private static ProcessBuilder Fast(ProcessBuilder process) => process.Heartbeat(TimeSpan.FromMilliseconds(200)).StaleAfter(TimeSpan.FromSeconds(1));

    [Fact]
    public async Task AProcessWithoutWorkIsListedByTypeHeartbeatsAndIsMarkedStopped()
    {
        var type = NewType();
        var first = await ServicesAsync(services => services.AddWorkProcess(p => Fast(p.Type(type).Release("1.5"))));
        var second = await ServicesAsync(services => services.AddWorkProcess(p => Fast(p.Type(type).Release("1.5"))));

        var live = await Registry(first).ListAsync(type, cancellationToken: Cancel);
        Assert.Equal(new[] { first.Owner, second.Owner }.OrderBy(o => o.Value), live.Select(p => p.Owner).OrderBy(o => o.Value));
        Assert.All(live, p => Assert.Equal((type, "1.5", ProcessState.Live, (DateTimeOffset?)null), (p.Type, p.Release, p.State, p.StoppedAt)));
        Assert.Empty((await ReadProcessAsync(first.Owner)).Definitions);

        await Provider.AgeProcessAsync(first.Owner, TimeSpan.FromMinutes(1), Cancel);
        await EventuallyAsync(async () => (await ReadProcessAsync(first.Owner)).HeartbeatAge < TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5), "the heartbeat advances");
        await second.StopAsync(Cancel);
        Assert.Equal([first.Owner], (await Registry(first).ListAsync(type, cancellationToken: Cancel)).Select(p => p.Owner));
        var stopped = Assert.Single(await Registry(first).ListAsync(type, [ProcessState.Stopped], Cancel));
        Assert.Equal(second.Owner, stopped.Owner);
        Assert.NotNull(stopped.StoppedAt);
    }

    [Fact]
    public async Task AProcessWhoseHeartbeatIsOlderThanItsStaleWindowIsListedStale()
    {
        var type = NewType();
        var live = await ServicesAsync(services => services.AddWorkProcess(p => Fast(p.Type(type))));
        var crashed = OwnerId.New();
        await Store(live).Processes.RegisterAsync(new ProcessRegistration(crashed, type, null, TimeSpan.FromSeconds(1)), Cancel);
        Assert.Equal(2, (await Registry(live).ListAsync(type, cancellationToken: Cancel)).Count);

        await EventuallyAsync(async () => (await Registry(live).ListAsync(type, cancellationToken: Cancel)).Count == 1, TimeSpan.FromSeconds(5), "the crashed process becomes stale");
        var stale = Assert.Single(await Registry(live).ListAsync(type, [ProcessState.Stale], Cancel));
        Assert.Equal((crashed, ProcessState.Stale), (stale.Owner, stale.State));
        Assert.Equal(2, (await Registry(live).ListAsync(type, [ProcessState.Live, ProcessState.Stale, ProcessState.Stopped], Cancel)).Count);
    }

    [Fact]
    public async Task AProcessThatAddsWorkRegistersWithItsDefinitionsUnlessItOnlyRequestsWork()
    {
        var definition = Definition();
        var worker = await WorkerAsync(definition, new JobScript(), configure: job => job.AcceptsArgs(1, 2), also: work => work.Process(p => p.Type("indexing-worker").Release("1.4")));
        var client = await ClientAsync(null, definition);
        var described = await ProcessAsync(work => work.AddJobDefinition(definition).Process(p => p.Type("api")));

        var registered = await ReadProcessAsync(worker.Owner);
        Assert.Equal(("indexing-worker", "1.4", false), (registered.Type, registered.Release, registered.Stopped));
        var registration = Assert.Single(registered.Definitions);
        Assert.Equal((definition.Key, 1L, true, false), (registration.Definition, registration.Revision, registration.Runs, registration.DeclaresRecurrence));
        Assert.Equal([1, 2], registration.AcceptedArgs);
        Assert.Null(await Provider.ReadProcessAsync(client.Owner, Cancel));
        var api = Assert.Single((await ReadProcessAsync(described.Owner)).Definitions);
        Assert.Equal((false, (IReadOnlyList<int>)[]), (api.Runs, api.AcceptedArgs));

        await worker.StopAsync(Cancel);
        Assert.True((await ReadProcessAsync(worker.Owner)).Stopped);
    }

    [Fact]
    public async Task AProcessWhoseRecordWasDeletedRegistersAgainWithItsDefinitions()
    {
        var definition = Definition();
        var worker = await WorkerAsync(definition, new JobScript(), also: work => work.Process(p => Fast(p)));
        await Provider.DeleteProcessAsync(worker.Owner, Cancel);

        ProcessRecord? record = null;
        await EventuallyAsync(async () => (record = await Provider.ReadProcessAsync(worker.Owner, Cancel)) is { Definitions.Count: 1 }, TimeSpan.FromSeconds(5), "the process registers again");
        Assert.Equal(definition.Key, record!.Definitions[0].Definition);
    }

    [Fact]
    public async Task J24_AStartingProcessDeletesRecordsSevenDaysAfterTheyStoppedOrBecameStale()
    {
        var type = NewType();
        var stopped = await ServicesAsync(services => services.AddWorkProcess(p => p.Type(type)));
        await stopped.StopAsync(Cancel);
        var recentlyStopped = await ServicesAsync(services => services.AddWorkProcess(p => p.Type(type)));
        await recentlyStopped.StopAsync(Cancel);
        var live = await ServicesAsync(services => services.AddWorkProcess(p => p.Type(type)));
        var store = Store(live);
        var abandoned = OwnerId.New();
        var recentlyAbandoned = OwnerId.New();
        foreach (var owner in new[] { abandoned, recentlyAbandoned })
        {
            await store.Processes.RegisterAsync(new ProcessRegistration(owner, type, null, TimeSpan.FromMinutes(1)), Cancel);
        }
        await store.Jobs.RegisterProcessAsync(abandoned, [new ProcessDefinition("retired-definition", 1, true, [1], false)], Cancel);
        await Provider.AgeProcessAsync(stopped.Owner, TimeSpan.FromDays(7) + TimeSpan.FromMinutes(1), Cancel);
        await Provider.AgeProcessAsync(recentlyStopped.Owner, TimeSpan.FromDays(7) - TimeSpan.FromMinutes(1), Cancel);
        await Provider.AgeProcessAsync(abandoned, TimeSpan.FromDays(7) + TimeSpan.FromMinutes(2), Cancel);
        await Provider.AgeProcessAsync(recentlyAbandoned, TimeSpan.FromDays(7), Cancel);

        var starting = await ServicesAsync(services => services.AddWorkProcess(p => p.Type(type)));

        Assert.Null(await Provider.ReadProcessAsync(stopped.Owner, Cancel));
        Assert.Null(await Provider.ReadProcessAsync(abandoned, Cancel));
        Assert.NotNull(await Provider.ReadProcessAsync(recentlyStopped.Owner, Cancel));
        Assert.NotNull(await Provider.ReadProcessAsync(recentlyAbandoned, Cancel));
        Assert.NotNull(await Provider.ReadProcessAsync(live.Owner, Cancel));
        Assert.NotNull(await Provider.ReadProcessAsync(starting.Owner, Cancel));
    }

    private async Task<ProcessRecord> ReadProcessAsync(OwnerId owner) =>
        await Provider.ReadProcessAsync(owner, Cancel) ?? throw new InvalidOperationException($"Process {owner} is not registered.");

    private static ProcessRegistry Registry(TestProcess process) => process.Services.GetRequiredService<ProcessRegistry>();

    private static IWorkStore Store(TestProcess process) => process.Services.GetRequiredService<IWorkStore>();
}
