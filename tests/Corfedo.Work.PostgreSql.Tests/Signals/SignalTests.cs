using System.Collections.Concurrent;
using System.Diagnostics;
using Corfedo.Work.Conformance;
using Corfedo.Work.PostgreSql.Tests.Conformance;
using Corfedo.Work.PostgreSql.Tests.Provisioning;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;

namespace Corfedo.Work.PostgreSql.Tests.Signals;

public sealed class SignalTests(PostgreSqlDatabase database) : IClassFixture<PostgreSqlDatabase>
{
    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    /// <summary>S-6: after the listening backend is terminated, every subscriber resyncs and later signals arrive.</summary>
    [Fact]
    public async Task SubscribersResyncAfterTheListeningConnectionIsTerminated()
    {
        var store = new PostgreSqlWorkStore(database.DataSource, "signals_s6", NullLoggerFactory.Instance, TimeProvider.System);
        await using var signals = new WorkSignals(store.Signals);
        var orders = new Recorder();
        var invoices = new Recorder();
        using var a = signals.Subscribe(Orders, orders);
        using var b = signals.Subscribe(Invoices, invoices);
        await EventuallyAsync(() => orders.Resyncs == 1 && invoices.Resyncs == 1, "the listener connects");

        Assert.Equal(1, await TerminateListenersAsync("corfedo_work:signals_s6"));
        await EventuallyAsync(() => orders.Resyncs == 2 && invoices.Resyncs == 2, "the listener reconnects");
        await signals.SendAsync(Orders, "1", Cancel);

        await EventuallyAsync(() => orders.Keys.Contains("1"), "a signal sent after reconnecting arrives");
        Assert.Empty(invoices.Keys);
    }

    /// <summary>A signal is sent once its transaction commits, however long after the send that is.</summary>
    [Fact]
    public async Task ASignalIsSentWhenItsLongTransactionCommitsAndNeverWhenItRollsBack()
    {
        var store = new PostgreSqlWorkStore(database.DataSource, "signals_long", NullLoggerFactory.Instance, TimeProvider.System);
        await using var signals = new WorkSignals(store.Signals);
        var orders = new Recorder();
        using var subscription = signals.Subscribe(Orders, orders);
        await EventuallyAsync(() => orders.Resyncs == 1, "the listener connects");

        await using (var connection = await database.DataSource.OpenConnectionAsync(Cancel))
        {
            await using var committed = await connection.BeginTransactionAsync(Cancel);
            await signals.SendAsync(committed, Orders, "committed", Cancel);
            await Task.Delay(300, Cancel);
            Assert.Empty(orders.Keys);
            await committed.CommitAsync(Cancel);
        }
        await using (var connection = await database.DataSource.OpenConnectionAsync(Cancel))
        {
            await using var rolledBack = await connection.BeginTransactionAsync(Cancel);
            await signals.SendAsync(rolledBack, Orders, "rolled-back", Cancel);
            await Task.Delay(300, Cancel);
            await rolledBack.RollbackAsync(Cancel);
        }

        await EventuallyAsync(() => orders.Keys.Contains("committed"), "the committed signal arrives");
        await Task.Delay(300, Cancel);
        Assert.Equal(["committed"], orders.Keys);
    }

    /// <summary>
    /// Every signal of many transactions committing at once arrives, also of those still running when a batch first
    /// checks them.
    /// </summary>
    [Fact]
    public async Task EverySignalOfConcurrentlyCommittingTransactionsArrives()
    {
        var store = new PostgreSqlWorkStore(database.DataSource, "signals_concurrent", NullLoggerFactory.Instance, TimeProvider.System);
        await using var signals = new WorkSignals(store.Signals);
        var orders = new Recorder();
        using var subscription = signals.Subscribe(Orders, orders);
        await EventuallyAsync(() => orders.Resyncs == 1, "the listener connects");

        await Task.WhenAll(Enumerable.Range(0, 8).Select(writer => Task.Run(async () =>
        {
            await using var connection = await database.DataSource.OpenConnectionAsync(Cancel);
            for (var i = 0; i < 50; i++)
            {
                await using var transaction = await connection.BeginTransactionAsync(Cancel);
                await signals.SendAsync(transaction, Orders, $"{writer}:{i}", Cancel);
                // Some transactions are still running at their first check and commit during later batches.
                if (i % 3 == 0)
                {
                    await Task.Delay(Random.Shared.Next(1, 4), Cancel);
                }
                await transaction.CommitAsync(Cancel);
            }
        }, Cancel)));

        await EventuallyAsync(() => orders.Keys.Distinct().Count() == 400, "every committed signal arrives");
    }

    /// <summary>Transactions that commit close together send the same topic and key once per batch.</summary>
    [Fact]
    public async Task SignalsWithTheSameKeyFromTransactionsCommittedCloseTogetherArriveMerged()
    {
        var store = new PostgreSqlWorkStore(database.DataSource, "signals_merged", NullLoggerFactory.Instance, TimeProvider.System);
        await using var signals = new WorkSignals(store.Signals);
        var orders = new Recorder();
        using var subscription = signals.Subscribe(Orders, orders);
        await EventuallyAsync(() => orders.Resyncs == 1, "the listener connects");

        await using (var connection = await database.DataSource.OpenConnectionAsync(Cancel))
        {
            for (var i = 0; i < 50; i++)
            {
                await using var transaction = await connection.BeginTransactionAsync(Cancel);
                await signals.SendAsync(transaction, Orders, "same", Cancel);
                await transaction.CommitAsync(Cancel);
            }
        }

        await EventuallyAsync(() => !orders.Keys.IsEmpty, "the signal arrives");
        await Task.Delay(300, Cancel);
        Assert.InRange(orders.Keys.Count, 1, 49);
    }

    /// <summary>Disposing the transport sends the signals of committed transactions it holds instead of dropping them.</summary>
    [Fact]
    public async Task DisposingTheTransportSendsTheSignalsOfCommittedTransactions()
    {
        var listening = new PostgreSqlWorkStore(database.DataSource, "signals_dispose", NullLoggerFactory.Instance, TimeProvider.System);
        await using var signals = new WorkSignals(listening.Signals);
        var orders = new Recorder();
        using var subscription = signals.Subscribe(Orders, orders);
        await EventuallyAsync(() => orders.Resyncs == 1, "the listener connects");
        var sending = new PostgreSqlWorkStore(database.DataSource, "signals_dispose", NullLoggerFactory.Instance, TimeProvider.System);

        await using (var connection = await database.DataSource.OpenConnectionAsync(Cancel))
        {
            await using var transaction = await connection.BeginTransactionAsync(Cancel);
            await sending.Signals.SendAsync(transaction, Orders, "last", Cancel);
            await transaction.CommitAsync(Cancel);
        }
        await ((IAsyncDisposable)sending.Signals).DisposeAsync();

        await EventuallyAsync(() => orders.Keys.Contains("last"), "the committed signal arrives");
    }

    /// <summary>The channel name is at most 63 bytes: <c>pg_notify</c> refuses longer ones and <c>LISTEN</c> truncates them.</summary>
    [Fact]
    public void ASchemaLongerThanItsSignalChannelAllowsIsRefused()
    {
        _ = new PostgreSqlWorkStore(database.DataSource, new string('s', 50), NullLoggerFactory.Instance, TimeProvider.System);

        Assert.Throws<ArgumentException>(() => new PostgreSqlWorkStore(database.DataSource, new string('s', 51), NullLoggerFactory.Instance, TimeProvider.System));
        Assert.Throws<ArgumentException>(() => new ServiceCollection().AddPostgreSqlSignals(database.DataSource, new string('é', 26)));
    }

    private static readonly SignalTopic Orders = new("app.orders");
    private static readonly SignalTopic Invoices = new("app.invoices");

    private async Task<int> TerminateListenersAsync(string channel)
    {
        await using var terminate = database.DataSource.CreateCommand(
            "SELECT count(pg_terminate_backend(pid))::int4 FROM pg_stat_activity WHERE datname = current_database() AND query = $1");
        terminate.Parameters.Add(new NpgsqlParameter<string> { TypedValue = $"LISTEN \"{channel}\"" });
        return (int)(await terminate.ExecuteScalarAsync(Cancel))!;
    }

    private sealed class Recorder : ISignalSubscriber
    {
        private int resyncs;

        public ConcurrentQueue<string> Keys { get; } = new();

        public int Resyncs => Volatile.Read(ref resyncs);

        public void Received(string key) => Keys.Enqueue(key);

        public void Resync() => Interlocked.Increment(ref resyncs);
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
}

/// <summary>S-7: a host with a dispatcher and a job waiter in one process listen on one connection.</summary>
public sealed class SingleListenerTests(PostgreSqlConformanceProvider provider) : JobScenarioBase(provider), IClassFixture<PostgreSqlConformanceProvider>
{
    [Fact]
    public async Task AHostAndAJobWaiterListenOnOneConnection()
    {
        var application = "s7_" + Guid.NewGuid().ToString("N")[..8];
        var definition = Definition();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var worker = await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(new JobScript
                {
                    Run = async (_, _, cancellationToken) =>
                    {
                        await gate.Task.WaitAsync(cancellationToken);
                        return JobResult.Completed;
                    },
                });
                work.Services.AddSingleton(new EndedScript());
                work.AddJob<TestArgs, ScriptedJobHandler>(definition, job => job.Lease(TimeSpan.FromSeconds(2)).OnEnded<ScriptedJobEnded>());
            },
            application: application);
        var job = await RequestAsync(worker, definition, "waited");

        var waiting = Jobs(worker).WaitAsync(definition, job.Id, TimeSpan.FromSeconds(10), Cancel).AsTask();
        await Task.Delay(300, Cancel);
        var listeners = await ListenersAsync("corfedo_work:work_" + application);
        gate.SetResult();

        Assert.Equal(1, listeners);
        Assert.True((await waiting)!.Ended);
    }

    private async Task<int> ListenersAsync(string channel)
    {
        await using var transaction = await BeginAsync();
        var npgsql = transaction.Work.Npgsql();
        await using var count = new NpgsqlCommand(
            "SELECT count(*)::int4 FROM pg_stat_activity WHERE datname = current_database() AND query = $1", npgsql.Connection, npgsql);
        count.Parameters.Add(new NpgsqlParameter<string> { TypedValue = $"LISTEN \"{channel}\"" });
        return (int)(await count.ExecuteScalarAsync(Cancel))!;
    }
}

/// <summary>A cancellation signal names the holder's owner, so only the holder's process renews early to observe it.</summary>
public sealed class CancelSignalTests(PostgreSqlConformanceProvider provider) : LeaseScenarioBase(provider), IClassFixture<PostgreSqlConformanceProvider>
{
    [Fact]
    public async Task CancellationRenewsOnlyTheHoldersProcess()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30)));
        await InsertManyAsync(source, 2);
        var runs = new ConcurrentDictionary<TestKey, TaskCompletionSource<LeaseStopReason?>>();
        var script = new HandlerScript
        {
            Run = async (run, cancellationToken) =>
            {
                var stopped = runs.GetOrAdd(run.Key, _ => new(TaskCreationOptions.RunContinuationsAsynchronously));
                await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                stopped.TrySetResult(run.Hold.StopReason);
                await run.ReleaseAsync(Due.None, cancellationToken: CancellationToken.None);
            },
        };
        await WorkerAsync(source, script, s => s.Capacity(1));
        await WorkerAsync(source, script, s => s.Capacity(1));
        await EventuallyAsync(() => runs.Count == 2, TimeSpan.FromSeconds(10), "each process holds one record");
        var cancelled = runs.Keys.First();
        var other = runs.Keys.Single(k => k != cancelled);
        var before = await ExpiresAtAsync(source, other);
        var requester = await ClientAsync(source);

        await using (var transaction = await BeginAsync())
        {
            await requester.Operations.RequestCancelAsync(transaction.Work, source, cancelled, Cancel);
            await transaction.CommitAsync(Cancel);
        }

        Assert.Equal(LeaseStopReason.CancelRequested, await runs[cancelled].Task.WaitAsync(TimeSpan.FromSeconds(5), Cancel));
        await Task.Delay(300, Cancel);
        Assert.False(runs[other].Task.IsCompleted);
        Assert.Equal(before, await ExpiresAtAsync(source, other));
    }

    private async Task<DateTime> ExpiresAtAsync(LeaseSource<TestKey> source, TestKey key)
    {
        await using var transaction = await BeginAsync();
        var npgsql = transaction.Work.Npgsql();
        await using var command = new NpgsqlCommand($"SELECT lease_expires_at FROM {source.Store.Schema}.{source.Store.Name} WHERE tenant = $1 AND id = $2", npgsql.Connection, npgsql);
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = key.Tenant });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = key.Id });
        return (DateTime)(await command.ExecuteScalarAsync(Cancel))!;
    }
}
