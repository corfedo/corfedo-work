using System.Data;
using Corfedo.Work.SqlServer.Leases;
using Corfedo.Work.SqlServer.Tests.Provisioning;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.SqlServer.Tests.Leases;

/// <summary>
/// A transaction chosen as a deadlock victim while it waits for an applock (merge identities, fences, limit scopes) is
/// rolled back and reported as a deadlock: the caller sees <see cref="Tsql.LockDeadlock"/>, and the store retries its
/// own transactions as it does after 1205.
/// </summary>
public sealed class AppLockDeadlockTests(SqlServerDatabase database) : IClassFixture<SqlServerDatabase>, IAsyncLifetime
{
    private static readonly JobDefinition<string> Definition = JobDefinition.Create("deadlock.feed", 1, new TextArgs(), null, subject: "feed");

    private ServiceProvider services = null!;

    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    private JobOperations Jobs => services.GetRequiredService<JobOperations>();

    public async ValueTask InitializeAsync()
    {
        var collection = new ServiceCollection();
        collection.AddSqlServerWorkStore(database.ConnectionString);
        collection.AddWork(work => work.AddJobDefinition(Definition));
        services = collection.BuildServiceProvider();
        await services.GetRequiredService<IWorkStore>().PrepareAsync(Cancel);
    }

    public async ValueTask DisposeAsync() => await services.DisposeAsync();

    [Fact]
    public async Task CallerTransactionsRequestingIdentitiesInOppositeOrdersReportTheVictimAsADeadlock()
    {
        var (a, b) = (Subject.Of("feed", "a"), Subject.Of("feed", "b"));
        await using var firstConnection = await OpenAsync();
        await using var first = await BeginAsync(firstConnection);
        await using var secondConnection = await OpenAsync();
        await using var second = await BeginAsync(secondConnection);
        await RequestAsync(first, a);
        await RequestAsync(second, b);

        // Each waits for the identity the other holds; SQL Server ends one of them.
        var firstWaits = RequestAsync(first, b);
        await Task.Delay(500, Cancel);
        var secondWaits = RequestAsync(second, a);
        var outcomes = await Task.WhenAll(OutcomeAsync(firstWaits), OutcomeAsync(secondWaits));

        var victim = Assert.Single(outcomes, o => o is not null)!;
        Assert.True(Tsql.HasError(victim, Tsql.LockDeadlock));
        Assert.True(Tsql.IsDeadlock(victim));
        var (survivor, lost) = outcomes[0] is null ? (first, second) : (second, first);
        Assert.Null(lost.Connection);
        await Assert.ThrowsAsync<InvalidOperationException>(() => lost.CommitAsync(Cancel));
        await survivor.CommitAsync(Cancel);
        Assert.Equal(2, await database.ScalarAsync("SELECT COUNT(*) FROM work.jobs WHERE state = 0;", Cancel));
    }

    [Fact]
    public async Task OwnTransactionChosenAsDeadlockVictimOnAnAppLockIsRetried()
    {
        var (x, y) = (Tsql.Literal(Tsql.LockName("test", "x")), Tsql.Literal(Tsql.LockName("test", "y")));
        await using var holding = await OpenAsync();
        await using (var priority = new SqlCommand("SET DEADLOCK_PRIORITY HIGH;", holding))
        {
            await priority.ExecuteNonQueryAsync(Cancel);
        }
        await using var holder = await BeginAsync(holding);
        await LockAsync(holding, holder, y);

        // The store's transaction takes x and waits for y; the holder then waits for x and wins the deadlock.
        var attempts = 0;
        var own = SqlServerLeaseStore.RetryDeadlocksAsync(async () =>
        {
            Interlocked.Increment(ref attempts);
            await using var connection = await OpenAsync();
            await using var transaction = await BeginAsync(connection);
            await LockAsync(connection, transaction, x, y);
            await transaction.CommitAsync(Cancel);
            return Volatile.Read(ref attempts);
        });
        await Task.Delay(500, Cancel);
        Assert.False(own.IsCompleted);
        await LockAsync(holding, holder, x);
        await holder.CommitAsync(Cancel);

        Assert.Equal(2, await own);
    }

    private Task RequestAsync(SqlTransaction transaction, Subject subject) =>
        Jobs.RequestAsync(transaction, Definition, "a", null, new JobRequestOptions { Subject = subject }, Cancel).AsTask();

    private static async Task<SqlException?> OutcomeAsync(Task request)
    {
        try
        {
            await request;
            return null;
        }
        catch (SqlException exception)
        {
            return exception;
        }
    }

    /// <summary>Takes applocks with the store's statement, in order.</summary>
    private static async Task LockAsync(SqlConnection connection, SqlTransaction transaction, params string[] resources)
    {
        var text = "SET XACT_ABORT ON; DECLARE @lock_result int;\n" + string.Concat(resources.Select(r => LeaseSql.AppLock(r, "Exclusive")));
        await using var command = new SqlCommand(text, connection, transaction);
        command.Parameters.AddWithValue("@unused", 0);
        await command.ExecuteNonQueryAsync(Cancel);
    }

    private async Task<SqlConnection> OpenAsync()
    {
        var connection = new SqlConnection(database.ConnectionString);
        await connection.OpenAsync(Cancel);
        return connection;
    }

    private static async Task<SqlTransaction> BeginAsync(SqlConnection connection) =>
        (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, Cancel);

    private sealed class TextArgs : IJobArgs<string>
    {
        public int Version => 1;

        public byte[] Write(string args) => System.Text.Encoding.UTF8.GetBytes(args);

        public string Read(ReadOnlySpan<byte> data, int version) => System.Text.Encoding.UTF8.GetString(data);
    }
}
