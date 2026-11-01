using System.Collections.Concurrent;
using System.Diagnostics;
using Corfedo.Work.EntityFrameworkCore.Tests.Mapping;
using Corfedo.Work.PostgreSql;
using Corfedo.Work.PostgreSql.Tests.Provisioning;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.EntityFrameworkCore.Tests.Signals;

/// <summary>
/// An application that uses only signals sends them in a <c>DbContext</c>'s transaction: they are delivered when its
/// changes commit and never after a rollback. No framework schema is applied.
/// </summary>
public sealed class DbContextSignalTests(PostgreSqlDatabase database) : IClassFixture<PostgreSqlDatabase>
{
    private static readonly SignalTopic Instances = new("fleet.instances");

    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task SignalsSentInAContextTransactionAreDeliveredOnlyOnCommit()
    {
        var collection = new ServiceCollection();
        collection.AddPostgreSqlSignals(database.DataSource, "fleet");
        await using var services = collection.BuildServiceProvider();
        var signals = services.GetRequiredService<IWorkSignals>();
        var subscriber = new Recorder();
        using var subscription = signals.Subscribe(Instances, subscriber);
        await EventuallyAsync(() => subscriber.Resynced, "the subscriber resyncs once listening");

        await using (var context = new FleetContext(database.ConnectionString))
        {
            await using var transaction = await context.Database.BeginTransactionAsync(Cancel);
            await signals.SendAsync(context.Database.WorkTransaction(), Instances, "rolled-back", Cancel);
            await transaction.RollbackAsync(Cancel);
        }
        await using (var context = new FleetContext(database.ConnectionString))
        {
            await using var transaction = await context.Database.BeginTransactionAsync(Cancel);
            await signals.SendAsync(context.Database.WorkTransaction(), Instances, "committed", Cancel);
            await transaction.CommitAsync(Cancel);
        }

        await EventuallyAsync(() => subscriber.Keys.Contains("committed"), "the committed signal arrives");
        await Task.Delay(300, Cancel);
        Assert.Equal(["committed"], subscriber.Keys);
    }

    [Fact]
    public async Task AContextWithoutATransactionCannotSend()
    {
        var collection = new ServiceCollection();
        collection.AddPostgreSqlSignals(database.DataSource, "fleet");
        await using var services = collection.BuildServiceProvider();
        await using var context = new FleetContext(database.ConnectionString);

        Assert.Throws<InvalidOperationException>(() => context.Database.WorkTransaction());
    }

    private static async Task EventuallyAsync(Func<bool> condition, string because)
    {
        var stopwatch = Stopwatch.StartNew();
        while (!condition())
        {
            if (stopwatch.Elapsed > TimeSpan.FromSeconds(10))
            {
                Assert.Fail("Timed out: " + because);
            }
            await Task.Delay(25, Cancel);
        }
    }

    private sealed class Recorder : ISignalSubscriber
    {
        private volatile bool resynced;

        public ConcurrentQueue<string> Keys { get; } = new();

        public bool Resynced => resynced;

        public void Received(string key) => Keys.Enqueue(key);

        public void Resync() => resynced = true;
    }
}
