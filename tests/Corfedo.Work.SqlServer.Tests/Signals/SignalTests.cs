using System.Collections.Concurrent;
using System.Diagnostics;
using Corfedo.Work.SqlServer.Signals;
using Corfedo.Work.SqlServer.Tests.Provisioning;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work.SqlServer.Tests.Signals;

/// <summary>
/// The signals table transport: a signal is a row of the sending transaction, read by the listener only below
/// <c>MIN_ACTIVE_ROWVERSION()</c>, so rolled-back rows wake nobody and an open transaction that signalled holds later
/// signals back until it ends.
/// </summary>
public sealed class SignalTests(SqlServerDatabase database) : IClassFixture<SqlServerDatabase>
{
    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    /// <summary>How long a test waits before concluding that a signal was not delivered.</summary>
    private static readonly TimeSpan Quiet = TimeSpan.FromMilliseconds(500);

    private static readonly SignalTopic Orders = new("app.orders");

    [Fact]
    public async Task ARolledBackSignalIsNeverDelivered()
    {
        await using var listening = await ListenAsync("signals_rollback");
        await using (var connection = await OpenAsync())
        await using (var transaction = connection.BeginTransaction())
        {
            await listening.Store.Signals.SendAsync(transaction, Orders, "rolled-back", Cancel);
            await transaction.RollbackAsync(Cancel);
        }
        await listening.Store.Signals.SendAsync(null, Orders, "sent", Cancel);

        await EventuallyAsync(() => listening.Keys.Contains("sent"), "a later signal arrives");
        Assert.DoesNotContain("rolled-back", listening.Keys);
    }

    [Fact]
    public async Task ASignalRolledBackToASavepointIsNeverDeliveredAndTheOthersAre()
    {
        await using var listening = await ListenAsync("signals_savepoint");
        await using (var connection = await OpenAsync())
        await using (var transaction = connection.BeginTransaction())
        {
            await listening.Store.Signals.SendAsync(transaction, Orders, "before", Cancel);
            transaction.Save("partial");
            await listening.Store.Signals.SendAsync(transaction, Orders, "rolled-back", Cancel);
            transaction.Rollback("partial");
            await listening.Store.Signals.SendAsync(transaction, Orders, "after", Cancel);
            await transaction.CommitAsync(Cancel);
        }

        await EventuallyAsync(() => listening.Keys.Contains("before") && listening.Keys.Contains("after"), "the signals that stayed arrive");
        await Task.Delay(Quiet, Cancel);
        Assert.DoesNotContain("rolled-back", listening.Keys);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AnOpenTransactionThatSignalledHoldsLaterSignalsBackUntilItEnds(bool commit)
    {
        await using var listening = await ListenAsync("signals_watermark_" + (commit ? "commit" : "rollback"));
        await using var connection = await OpenAsync();
        await using var transaction = connection.BeginTransaction();
        await listening.Store.Signals.SendAsync(transaction, Orders, "open", Cancel);

        await listening.Store.Signals.SendAsync(null, Orders, "later", Cancel);
        await Task.Delay(Quiet, Cancel);
        Assert.Empty(listening.Keys);

        if (commit)
        {
            await transaction.CommitAsync(Cancel);
        }
        else
        {
            await transaction.RollbackAsync(Cancel);
        }
        await EventuallyAsync(() => listening.Keys.Contains("later"), "the held signal arrives once the transaction ended");
        Assert.Equal(commit ? ["open", "later"] : ["later"], listening.Keys.ToArray());
    }

    [Fact]
    public async Task SubscribersResyncAfterTheListeningSessionIsKilled()
    {
        await using var listening = await ListenAsync("signals_killed");

        Assert.Equal(1, await KillListenersAsync());
        await EventuallyAsync(() => listening.Resyncs == 2, "the listener reconnects");
        await listening.Store.Signals.SendAsync(null, Orders, "1", Cancel);

        await EventuallyAsync(() => listening.Keys.Contains("1"), "a signal sent after reconnecting arrives");
    }

    [Fact]
    public async Task TheListenerPrunesSignalsOlderThanTheRetention()
    {
        var options = new SqlServerSignalOptions { MaximumWait = TimeSpan.FromMilliseconds(100), Retention = TimeSpan.FromMilliseconds(300), PruneInterval = TimeSpan.FromMilliseconds(100) };
        await using var listening = await ListenAsync("signals_pruned", options);
        await listening.Store.Signals.SendAsync(null, Orders, "pruned", Cancel);
        await EventuallyAsync(() => listening.Keys.Contains("pruned"), "the signal arrives");

        var table = $"[{listening.Schema}].signals";
        await EventuallyAsync(async () => (int)(await database.ScalarAsync($"SELECT COUNT(*) FROM {table}", Cancel))! == 0, "the listener deletes the signal");
    }

    private async Task<SqlConnection> OpenAsync()
    {
        var connection = new SqlConnection(database.ConnectionString);
        await connection.OpenAsync(Cancel);
        return connection;
    }

    /// <summary>Migrates a store of the schema and subscribes to <see cref="Orders"/> until the listener connected.</summary>
    private async Task<Listening> ListenAsync(string schema, SqlServerSignalOptions? options = null)
    {
        var store = new SqlServerWorkStore(database.ConnectionString, schema, options ?? new SqlServerSignalOptions(), NullLoggerFactory.Instance, TimeProvider.System);
        await store.MigrateAsync(Cancel);
        var signals = new WorkSignals(store.Signals);
        var listening = new Listening(schema, store, signals);
        listening.Subscription = signals.Subscribe(Orders, listening);
        await EventuallyAsync(() => listening.Resyncs == 1, "the listener connects");
        return listening;
    }

    /// <summary>Kills the sessions of this database that run the listener's server-side loop.</summary>
    private async Task<int> KillListenersAsync()
    {
        await using var connection = await OpenAsync();
        var sessions = new List<short>();
        await using (var find = new SqlCommand(
            "SELECT r.session_id FROM sys.dm_exec_requests AS r CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS t "
            + "WHERE r.database_id = DB_ID() AND r.session_id <> @@SPID AND t.text LIKE N'%MIN_ACTIVE_ROWVERSION()%WAITFOR%'", connection))
        await using (var reader = await find.ExecuteReaderAsync(Cancel))
        {
            while (await reader.ReadAsync(Cancel))
            {
                sessions.Add(reader.GetInt16(0));
            }
        }
        foreach (var session in sessions)
        {
            await using var kill = new SqlCommand($"KILL {session}", connection);
            await kill.ExecuteNonQueryAsync(Cancel);
        }
        return sessions.Count;
    }

    private sealed class Listening(string schema, SqlServerWorkStore store, WorkSignals signals) : ISignalSubscriber, IAsyncDisposable
    {
        private int resyncs;

        public string Schema { get; } = schema;

        public SqlServerWorkStore Store { get; } = store;

        public IDisposable? Subscription { get; set; }

        public ConcurrentQueue<string> Keys { get; } = new();

        public int Resyncs => Volatile.Read(ref resyncs);

        public void Received(string key) => Keys.Enqueue(key);

        public void Resync() => Interlocked.Increment(ref resyncs);

        public async ValueTask DisposeAsync()
        {
            Subscription?.Dispose();
            await signals.DisposeAsync();
        }
    }

    private static Task EventuallyAsync(Func<bool> condition, string because) => EventuallyAsync(() => Task.FromResult(condition()), because);

    private static async Task EventuallyAsync(Func<Task<bool>> condition, string because)
    {
        var stopwatch = Stopwatch.StartNew();
        while (!await condition())
        {
            if (stopwatch.Elapsed > TimeSpan.FromSeconds(10))
            {
                Assert.Fail("Timed out: " + because);
            }
            await Task.Delay(25, Cancel);
        }
    }
}
