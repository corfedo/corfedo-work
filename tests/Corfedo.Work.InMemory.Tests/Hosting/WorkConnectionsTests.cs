using Corfedo.Work.Conformance;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.InMemory.Tests.Hosting;

/// <summary>The connections a Work host holds are computed from its registrations, without creating the store.</summary>
public sealed class WorkConnectionsTests
{
    public sealed record NameArgs(string Name);

    private static readonly LeaseSource<TestKey> Orders = LeaseSource.Define("orders", StoreLocation.Table("app", "orders"), TestKey.Codec, new LeasePolicy { Duration = TimeSpan.FromMinutes(1) });

    private static readonly LeaseSource<TestKey> Invoices = LeaseSource.Define("invoices", StoreLocation.Table("app", "invoices"), TestKey.Codec, new LeasePolicy { Duration = TimeSpan.FromMinutes(1) });

    [Fact]
    public void TheCountsFollowTheRegisteredLoopsAndCapacitiesWithoutCreatingTheStore()
    {
        var services = new ServiceCollection();
        services.AddInMemoryWorkStore(new InMemoryWorkDatabase());
        services.AddWork(work => work
            .AddPool("shared", 5)
            .AddLeaseHandler<TestKey, NoHandler>(Orders, source => source.Capacity(3))
            .AddLeaseHandler<TestKey, NoHandler>(Invoices, source => source.Pool("shared"))
            .AddJob(Definition("connections.a"), Completed, job => job.Capacity(2))
            .AddJob(Definition("connections.b"), Completed, job => job.Pool("shared"))
            .AddJob(Definition("connections.c"), Completed, job => job.Pool("shared"))
            .AddJob(Definition("connections.d"), Completed, job => job.Pool("shared"))
            .AddJobDefinition(Definition("connections.requested"))
            .AddMonitoring());
        var created = 0;
        var store = services.Single(d => d.ServiceType == typeof(IWorkStore));
        services.Remove(store);
        services.AddSingleton<IWorkStore>(provider =>
        {
            created++;
            return (IWorkStore)store.ImplementationFactory!(provider);
        });
        using var provider = services.BuildServiceProvider();

        var connections = provider.GetRequiredService<WorkConnections>();

        // Dispatch: orders, connections.a and the shared pool (3). Expiry: one pass for all job definitions (1).
        // Maintenance: the probe and four definitions, on the loop's four workers (4).
        // Housekeeping: heartbeat, signal sender and monitoring. Runs: orders 3, connections.a 2 and the shared pool 5.
        Assert.Equal(new WorkConnections(Listener: 1, Passes: 8, Housekeeping: 3, Runs: 10), connections);
        Assert.Equal(0, created);
    }

    [Fact]
    public void AProcessThatOnlyRequestsJobsHoldsOnlyTheSignalConnections()
    {
        var services = new ServiceCollection();
        services.AddInMemoryWorkStore(new InMemoryWorkDatabase());
        services.AddWork(work => work.AddJobDefinition(Definition("connections.requested")));
        using var provider = services.BuildServiceProvider();

        Assert.Equal(new WorkConnections(Listener: 1, Passes: 0, Housekeeping: 1, Runs: 0), provider.GetRequiredService<WorkConnections>());
    }

    private static JobDefinition<NameArgs> Definition(string key) => JobDefinition.Create<NameArgs>(key);

    private static ValueTask<JobDisposition> Completed(NameArgs args, JobRun run, IServiceProvider services, CancellationToken cancellationToken) =>
        ValueTask.FromResult<JobDisposition>(JobResult.Completed);

    private sealed class NoHandler : ILeaseHandler<TestKey>
    {
        public ValueTask RunAsync(LeaseRun<TestKey> run, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask ExhaustedAsync(LeaseRun<TestKey> run, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }
}
