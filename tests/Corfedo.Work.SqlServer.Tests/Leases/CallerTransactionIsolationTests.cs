using System.Data;
using Corfedo.Work.Conformance;
using Corfedo.Work.SqlServer.Tests.Provisioning;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work.SqlServer.Tests.Leases;

/// <summary>
/// Claims in the caller's transaction count other records' leases against limits, so a snapshot taken before a
/// concurrent claim committed must not be used to count. Claims nested in a claim hook share the caller's savepoint
/// names, so a failing hook rolls back only its own claim.
/// </summary>
public sealed class CallerTransactionIsolationTests(SqlServerDatabase database) : IClassFixture<SqlServerDatabase>
{
    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(IsolationLevel.Snapshot)]
    [InlineData(IsolationLevel.RepeatableRead)]
    [InlineData(IsolationLevel.Serializable)]
    public async Task ClaimAtAnotherIsolationLevelIsRefusedAndTheOwnerLimitHolds(IsolationLevel level)
    {
        var (source, store) = await SourceAsync("isolation_" + level.ToString().ToLowerInvariant());
        var owner = OwnerId.New();
        await using var laterConnection = await OpenAsync();
        await using var later = (SqlTransaction)await laterConnection.BeginTransactionAsync(level, Cancel);
        // The later transaction's first read starts its snapshot; it reads a table of its own, so a serializable
        // transaction's range locks do not hold off the claim committed after it.
        await using (var snapshot = new SqlCommand($"SELECT COUNT(*) FROM {source.Store.Name}_marker", laterConnection, later))
        {
            await snapshot.ExecuteScalarAsync(Cancel);
        }
        Assert.Single(await ClaimCommittedAsync(store, source, owner, new TestKey("t", 1)));

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await store.ClaimAsync(later, source, LeaseCallbacks<TestKey>.None, owner, [new TestKey("t", 2)], 1, Cancel));
        Assert.Contains("READ COMMITTED", error.Message, StringComparison.Ordinal);
        await later.RollbackAsync(Cancel);

        Assert.Equal(1, await HeldAsync(source));
    }

    [Fact]
    public async Task ReadCommittedClaimSeesALeaseCommittedAfterItBegan()
    {
        var (source, store) = await SourceAsync("read_committed");
        var owner = OwnerId.New();
        await using var laterConnection = await OpenAsync();
        await using var later = (SqlTransaction)await laterConnection.BeginTransactionAsync(IsolationLevel.ReadCommitted, Cancel);
        await using (var snapshot = new SqlCommand($"SELECT COUNT(*) FROM {source.Store.Name}_marker", laterConnection, later))
        {
            await snapshot.ExecuteScalarAsync(Cancel);
        }
        Assert.Single(await ClaimCommittedAsync(store, source, owner, new TestKey("t", 1)));

        Assert.Empty(await store.ClaimAsync(later, source, LeaseCallbacks<TestKey>.None, owner, [new TestKey("t", 2)], 1, Cancel));
        await later.CommitAsync(Cancel);

        Assert.Equal(1, await HeldAsync(source));
    }

    [Fact]
    public async Task IsolationSetInSqlIsSeenThroughTheReadCommittedHandle()
    {
        var (source, store) = await SourceAsync("set_in_sql");
        await using var connection = await OpenAsync();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, Cancel);
        await using (var change = new SqlCommand("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", connection, transaction))
        {
            await change.ExecuteNonQueryAsync(Cancel);
        }

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await store.ClaimAsync(transaction, source, LeaseCallbacks<TestKey>.None, OwnerId.New(), [new TestKey("t", 1)], 1, Cancel));
    }

    [Fact]
    public async Task FailingHookRollsBackItsClaimWhenTheHookClaimedThroughANewHandle()
    {
        var (outer, store) = await SourceAsync("nested_outer");
        var (inner, _) = await SourceAsync("nested_inner");
        var owner = OwnerId.New();
        var failing = new LeaseCallbacks<TestKey>((_, _, _) => throw new InvalidOperationException("inner hook"), null);
        var nested = new LeaseCallbacks<TestKey>(async (_, work, ct) =>
        {
            await store.ClaimAsync(work.SqlServer(), inner, failing, owner, [new TestKey("t", 1)], 1, ct);
            throw new InvalidOperationException("outer hook");
        }, null);
        await using var connection = await OpenAsync();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, Cancel);

        Assert.Empty(await store.ClaimAsync(transaction, outer, nested, owner, [new TestKey("t", 1)], 1, Cancel));
        await transaction.CommitAsync(Cancel);

        Assert.Equal((0, 0), (await HeldAsync(outer), await HeldAsync(inner)));
    }

    private async Task<SqlConnection> OpenAsync()
    {
        var connection = new SqlConnection(database.ConnectionString);
        await connection.OpenAsync(Cancel);
        return connection;
    }

    private async Task<IReadOnlyList<LeaseClaim<TestKey>>> ClaimCommittedAsync(ILeaseStore store, LeaseSource<TestKey> source, OwnerId owner, TestKey key)
    {
        await using var connection = await OpenAsync();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, Cancel);
        var claims = await store.ClaimAsync(transaction, source, LeaseCallbacks<TestKey>.None, owner, [key], 1, Cancel);
        await transaction.CommitAsync(Cancel);
        return claims;
    }

    private async Task<int> HeldAsync(LeaseSource<TestKey> source) =>
        (int)(await database.ScalarAsync($"SELECT COUNT(*) FROM {source.Store.Name} WHERE lease_expires_at IS NOT NULL", Cancel))!;

    private async Task<(LeaseSource<TestKey> Source, ILeaseStore Store)> SourceAsync(string table)
    {
        await database.ExecuteAsync($"""
            CREATE TABLE {table} (
                tenant nvarchar(100) NOT NULL, id bigint NOT NULL, due_at datetime2(7), lease_owner uniqueidentifier, lease_epoch bigint NOT NULL DEFAULT 0,
                lease_expires_at datetime2(7), takeovers int NOT NULL DEFAULT 0, failures int NOT NULL DEFAULT 0,
                settled_epoch bigint NOT NULL DEFAULT 0, lease_outstanding AS (CASE WHEN settled_epoch < lease_epoch THEN 1 END) PERSISTED,
                cancel_requested_at datetime2(7), execution_id uniqueidentifier, ended_completion_id uniqueidentifier, ended_digest varbinary(64),
                PRIMARY KEY (tenant, id));
            ALTER TABLE {table} SET (LOCK_ESCALATION = DISABLE);
            CREATE INDEX {table}_due ON {table} (due_at) WHERE lease_expires_at IS NULL;
            CREATE INDEX {table}_expiry ON {table} (lease_expires_at) WHERE lease_expires_at IS NOT NULL;
            CREATE INDEX {table}_settlement ON {table} (lease_outstanding, lease_expires_at) WHERE lease_expires_at IS NOT NULL;
            CREATE INDEX {table}_owner ON {table} (lease_owner) INCLUDE (lease_expires_at, lease_outstanding) WHERE lease_expires_at IS NOT NULL;
            INSERT INTO {table} (tenant, id, due_at) VALUES ('t', 1, SYSUTCDATETIME()), ('t', 2, SYSUTCDATETIME());
            CREATE TABLE {table}_marker (n int NOT NULL PRIMARY KEY);
            INSERT INTO {table}_marker (n) VALUES (1);
            """, Cancel);
        var source = LeaseSource.Define(table, StoreLocation.Table("dbo", table), TestKey.Codec, new LeasePolicy { Duration = TimeSpan.FromMinutes(1), OwnerLimit = 1 });
        return (source, new SqlServerWorkStore(database.ConnectionString, "work", NullLoggerFactory.Instance, TimeProvider.System).Leases);
    }
}
