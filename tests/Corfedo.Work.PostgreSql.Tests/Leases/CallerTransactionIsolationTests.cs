using System.Data;
using Corfedo.Work.Conformance;
using Corfedo.Work.PostgreSql.Tests.Provisioning;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;

namespace Corfedo.Work.PostgreSql.Tests.Leases;

/// <summary>
/// Claims in the caller's transaction count other records' leases against limits, so a snapshot taken before a
/// concurrent claim committed must not be used to count. Claims nested in a claim hook share the caller's savepoint
/// names, so a failing hook rolls back only its own claim.
/// </summary>
public sealed class CallerTransactionIsolationTests(PostgreSqlDatabase database) : IClassFixture<PostgreSqlDatabase>
{
    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(IsolationLevel.RepeatableRead)]
    [InlineData(IsolationLevel.Serializable)]
    public async Task SnapshotIsolatedClaimIsRefusedAndTheOwnerLimitHolds(IsolationLevel level)
    {
        var (source, store) = await SourceAsync("snapshot_" + level.ToString().ToLowerInvariant());
        var owner = OwnerId.New();
        await using var laterConnection = await database.DataSource.OpenConnectionAsync(Cancel);
        await using var later = await laterConnection.BeginTransactionAsync(level, Cancel);
        await using (var snapshot = new NpgsqlCommand($"SELECT count(*) FROM {source.Store.Name}", laterConnection, later))
        {
            await snapshot.ExecuteScalarAsync(Cancel);
        }
        Assert.Single(await ClaimCommittedAsync(store, source, owner, new TestKey("t", 1)));

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await store.ClaimAsync(later, source, LeaseCallbacks<TestKey>.None, owner, [new TestKey("t", 2)], 1, Cancel));
        await later.RollbackAsync(Cancel);

        Assert.Equal(1L, await HeldAsync(source));
    }

    [Fact]
    public async Task ReadCommittedClaimSeesALeaseCommittedAfterItBegan()
    {
        var (source, store) = await SourceAsync("read_committed");
        var owner = OwnerId.New();
        await using var laterConnection = await database.DataSource.OpenConnectionAsync(Cancel);
        await using var later = await laterConnection.BeginTransactionAsync(IsolationLevel.ReadCommitted, Cancel);
        await using (var snapshot = new NpgsqlCommand($"SELECT count(*) FROM {source.Store.Name}", laterConnection, later))
        {
            await snapshot.ExecuteScalarAsync(Cancel);
        }
        Assert.Single(await ClaimCommittedAsync(store, source, owner, new TestKey("t", 1)));

        Assert.Empty(await store.ClaimAsync(later, source, LeaseCallbacks<TestKey>.None, owner, [new TestKey("t", 2)], 1, Cancel));
        await later.CommitAsync(Cancel);

        Assert.Equal(1L, await HeldAsync(source));
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
            await store.ClaimAsync(work.Npgsql(), inner, failing, owner, [new TestKey("t", 1)], 1, ct);
            throw new InvalidOperationException("outer hook");
        }, null);
        await using var connection = await database.DataSource.OpenConnectionAsync(Cancel);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, Cancel);

        Assert.Empty(await store.ClaimAsync(transaction, outer, nested, owner, [new TestKey("t", 1)], 1, Cancel));
        await transaction.CommitAsync(Cancel);

        Assert.Equal((0L, 0L), (await HeldAsync(outer), await HeldAsync(inner)));
    }

    private async Task<IReadOnlyList<LeaseClaim<TestKey>>> ClaimCommittedAsync(ILeaseStore store, LeaseSource<TestKey> source, OwnerId owner, TestKey key)
    {
        await using var connection = await database.DataSource.OpenConnectionAsync(Cancel);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, Cancel);
        var claims = await store.ClaimAsync(transaction, source, LeaseCallbacks<TestKey>.None, owner, [key], 1, Cancel);
        await transaction.CommitAsync(Cancel);
        return claims;
    }

    private async Task<long> HeldAsync(LeaseSource<TestKey> source)
    {
        await using var command = database.DataSource.CreateCommand($"SELECT count(*) FROM {source.Store.Name} WHERE lease_expires_at IS NOT NULL");
        return (long)(await command.ExecuteScalarAsync(Cancel))!;
    }

    private async Task<(LeaseSource<TestKey> Source, ILeaseStore Store)> SourceAsync(string table)
    {
        await using (var command = database.DataSource.CreateCommand($"""
            CREATE TABLE {table} (
                tenant text NOT NULL, id bigint NOT NULL, due_at timestamptz, lease_owner uuid, lease_epoch bigint NOT NULL DEFAULT 0,
                lease_expires_at timestamptz, takeovers integer NOT NULL DEFAULT 0, failures integer NOT NULL DEFAULT 0,
                settled_epoch bigint NOT NULL DEFAULT 0, lease_outstanding integer GENERATED ALWAYS AS (CASE WHEN settled_epoch < lease_epoch THEN 1 END) STORED,
                cancel_requested_at timestamptz, execution_id uuid, ended_completion_id uuid, ended_digest bytea, PRIMARY KEY (tenant, id));
            INSERT INTO {table} (tenant, id, due_at) VALUES ('t', 1, clock_timestamp()), ('t', 2, clock_timestamp());
            """))
        {
            await command.ExecuteNonQueryAsync(Cancel);
        }
        var source = LeaseSource.Define(table, StoreLocation.Table("public", table), TestKey.Codec, new LeasePolicy { Duration = TimeSpan.FromMinutes(1), OwnerLimit = 1 });
        return (source, new PostgreSqlWorkStore(database.DataSource, "work", NullLoggerFactory.Instance, TimeProvider.System).Leases);
    }
}
