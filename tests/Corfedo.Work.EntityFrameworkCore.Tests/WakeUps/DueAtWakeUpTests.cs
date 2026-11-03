using System.Collections.Concurrent;
using System.Diagnostics;
using Corfedo.Work.EntityFrameworkCore.Tests.Mapping;
using Corfedo.Work.EntityFrameworkCore.Tests.Provisioning;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Corfedo.Work.EntityFrameworkCore.Tests.WakeUps;

/// <summary>
/// Saving tracked changes of a mapped <c>DueAt</c> wakes the source's dispatchers at once, on commit of the
/// transaction that saved them, instead of at their next poll.
/// </summary>
public abstract class DueAtWakeUpTests(TestDatabase database) : IAsyncLifetime
{
    private readonly ConcurrentQueue<InstanceKey> runs = new();
    private ServiceProvider services = null!;
    private IHostedService[] hosted = [];

    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    private IWorkSignals Signals => services.GetRequiredService<IWorkSignals>();

    public async ValueTask InitializeAsync()
    {
        await using (var context = new FleetContext(database))
        {
            await context.Database.EnsureCreatedAsync(Cancel);
        }
        // A record due at start is claimed by the first pass, so the dispatcher is idle until a signal or its poll.
        var probe = new InstanceKey("probe-" + Guid.NewGuid().ToString("N"), 1);
        await using (var context = new FleetContext(database))
        {
            context.Instances.Add(new Instance { TenantId = probe.TenantId, Id = probe.Id, Name = "probe", DueAt = DateTimeOffset.UtcNow });
            await context.SaveChangesAsync(Cancel);
        }
        var collection = new ServiceCollection();
        collection.AddSingleton(runs);
        collection.AddSingleton(database);
        database.AddWorkStore(collection);
        // Without a wake-up, a record that becomes due waits for the next poll, after the test's timeout.
        collection.AddWork(work => work.PollingInterval(TimeSpan.FromMinutes(5)).AddLeaseHandler<InstanceKey, EndingHandler>(Fleet.Instances));
        services = collection.BuildServiceProvider();
        hosted = [.. services.GetServices<IHostedService>()];
        foreach (var service in hosted)
        {
            await service.StartAsync(Cancel);
        }
        await RunsAsync(probe);
        await Task.Delay(300, Cancel);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var service in hosted.Reverse())
        {
            await service.StopAsync(CancellationToken.None);
        }
        await services.DisposeAsync();
    }

    [Fact]
    public async Task SavingANewRecordWithADueTimeWakesTheDispatcher()
    {
        var key = new InstanceKey("acme", 2);

        await AddAsync(key, DateTimeOffset.UtcNow);

        await RunsAsync(key);
    }

    [Fact]
    public async Task SavingADueTimeWithoutATransactionWakesTheDispatcher()
    {
        var key = new InstanceKey("acme", 3);
        await AddAsync(key, null);
        await Task.Delay(300, Cancel);

        await using (var context = new FleetContext(database, Signals))
        {
            var instance = await context.Instances.SingleAsync(i => i.TenantId == key.TenantId && i.Id == key.Id, Cancel);
            instance.DueAt = DateTimeOffset.UtcNow;
            await context.SaveChangesAsync(Cancel);
        }

        await RunsAsync(key);
    }

    [Fact]
    public async Task SavingADueTimeInATransactionWakesTheDispatcherOnCommit()
    {
        var key = new InstanceKey("acme", 4);
        await AddAsync(key, null);
        await Task.Delay(300, Cancel);

        await using (var context = new FleetContext(database, Signals))
        {
            await using var transaction = await context.Database.BeginTransactionAsync(Cancel);
            var instance = await context.Instances.SingleAsync(i => i.TenantId == key.TenantId && i.Id == key.Id, Cancel);
            instance.DueAt = DateTimeOffset.UtcNow;
            await context.SaveChangesAsync(Cancel);
            await transaction.CommitAsync(Cancel);
        }

        await RunsAsync(key);
    }

    private async Task AddAsync(InstanceKey key, DateTimeOffset? due)
    {
        await using var context = new FleetContext(database, Signals);
        context.Instances.Add(new Instance { TenantId = key.TenantId, Id = key.Id, Name = "web", DueAt = due });
        await context.SaveChangesAsync(Cancel);
    }

    private async Task RunsAsync(InstanceKey key)
    {
        var waited = Stopwatch.StartNew();
        while (!runs.Contains(key))
        {
            Assert.True(waited.Elapsed < TimeSpan.FromSeconds(10), $"{key} did not run within ten seconds.");
            await Task.Delay(20, Cancel);
        }
    }
}

public sealed class PostgreSqlDueAtWakeUpTests(PostgreSqlTestDatabase database) : DueAtWakeUpTests(database), IClassFixture<PostgreSqlTestDatabase>;

public sealed class SqlServerDueAtWakeUpTests(SqlServerTestDatabase database) : DueAtWakeUpTests(database), IClassFixture<SqlServerTestDatabase>;

/// <summary>Records the claimed key and ends the record in a <see cref="DbContext"/> transaction.</summary>
internal sealed class EndingHandler(ConcurrentQueue<InstanceKey> runs, TestDatabase database) : ILeaseHandler<InstanceKey>
{
    public async ValueTask RunAsync(LeaseRun<InstanceKey> run, CancellationToken cancellationToken)
    {
        await using var context = new FleetContext(database);
        await using var transaction = await context.Database.BeginTransactionAsync(cancellationToken);
        await run.EndAsync(context.Database.WorkTransaction(), cancellationToken: cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        runs.Enqueue(run.Key);
    }

    public ValueTask ExhaustedAsync(LeaseRun<InstanceKey> run, CancellationToken cancellationToken) => RunAsync(run, cancellationToken);
}
