using System.Data;
using Corfedo.Work.Conformance;
using Corfedo.Work.SqlServer.Tests.Provisioning;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work.SqlServer.Tests.Reconcile;

/// <summary>
/// Holder writes on superseding sources compare the generation after the fence is granted. A snapshot taken before an
/// invalidation committed would still show the old generation, so those transactions must be READ COMMITTED; a refused
/// fence rolls the transaction back.
/// </summary>
public sealed class GenerationFenceIsolationTests(SqlServerDatabase database) : IClassFixture<SqlServerDatabase>
{
    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    private readonly OwnerId owner = OwnerId.New();

    private IWorkStore Store => new SqlServerWorkStore(database.ConnectionString, "work", NullLoggerFactory.Instance, TimeProvider.System);

    [Theory]
    [InlineData(Operation.Renew, IsolationLevel.Snapshot)]
    [InlineData(Operation.Complete, IsolationLevel.Snapshot)]
    [InlineData(Operation.End, IsolationLevel.Snapshot)]
    [InlineData(Operation.Renew, IsolationLevel.RepeatableRead)]
    [InlineData(Operation.Complete, IsolationLevel.RepeatableRead)]
    [InlineData(Operation.End, IsolationLevel.Serializable)]
    public async Task HolderWriteAtAnotherIsolationLevelIsRefusedAndRollsTheTransactionBack(Operation operation, IsolationLevel isolation)
    {
        var (source, lease) = await ClaimedAsync();
        await using var connection = await OpenAsync();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(isolation, Cancel);
        // The first write starts the transaction's snapshot; it touches a table of its own, so a serializable
        // transaction's range locks do not hold off the invalidation.
        await using (var earlier = new SqlCommand($"UPDATE dbo.[{source.Store.Name}_marker] SET n = 2", connection, transaction))
        {
            await earlier.ExecuteNonQueryAsync(Cancel);
        }
        await InvalidateAsync(source);

        await Assert.ThrowsAsync<InvalidOperationException>(() => WriteAsync(operation, transaction, lease));
        // The refusal rolled the transaction back: its earlier write does not commit, and neither does the transaction.
        // SqlClient runs commands issued afterwards with the completed transaction outside any transaction.
        await Assert.ThrowsAsync<InvalidOperationException>(() => transaction.CommitAsync(Cancel));
        await using (var open = new SqlCommand("SELECT @@TRANCOUNT", connection))
        {
            Assert.Equal(0, (int)(await open.ExecuteScalarAsync(Cancel))!);
        }

        Assert.Equal<long?>(lease.Epoch, await ValidEpochAsync(source));
        Assert.Equal(1, (int)(await database.ScalarAsync($"SELECT n FROM dbo.[{source.Store.Name}_marker]", Cancel))!);
    }

    [Theory]
    [InlineData(Operation.Renew)]
    [InlineData(Operation.Complete)]
    [InlineData(Operation.End)]
    public async Task ReadCommittedHolderWriteSeesAnInvalidationThatCommittedAfterItBegan(Operation operation)
    {
        var (source, lease) = await ClaimedAsync();
        await using var connection = await OpenAsync();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, Cancel);
        await SnapshotAsync(transaction);
        await InvalidateAsync(source);

        await Assert.ThrowsAsync<LeaseLostException>(() => WriteAsync(operation, transaction, lease));
    }

    public enum Operation
    {
        Renew,
        Complete,
        End,
    }

    private async Task WriteAsync(Operation operation, WorkTransaction transaction, Lease<TestKey> lease)
    {
        switch (operation)
        {
            case Operation.Renew:
                if (!(await Store.Leases.RenewAsync(transaction, lease, Cancel)).Valid)
                {
                    throw new LeaseLostException("renewal refused");
                }
                break;
            case Operation.Complete:
                await Store.Reconcile.CompleteAsync(transaction, lease, ReconcileResult.Converged, Cancel);
                break;
            case Operation.End:
                await Store.Leases.EndAsync(transaction, lease, null, Cancel);
                break;
        }
    }

    private async Task<SqlConnection> OpenAsync()
    {
        var connection = new SqlConnection(database.ConnectionString);
        await connection.OpenAsync(Cancel);
        return connection;
    }

    private static async Task SnapshotAsync(SqlTransaction transaction)
    {
        await using var command = new SqlCommand("SELECT COUNT(*) FROM work.reconcile_generations", transaction.Connection, transaction);
        await command.ExecuteScalarAsync(Cancel);
    }

    private async Task<(ReconcileSource<TestKey> Source, Lease<TestKey> Lease)> ClaimedAsync()
    {
        await Store.MigrateAsync(Cancel);
        var table = "fence_" + Guid.NewGuid().ToString("N");
        await database.ExecuteAsync($"""
            CREATE TABLE dbo.{table} (
                tenant nvarchar(100) NOT NULL, id bigint NOT NULL, progress nvarchar(max),
                due_at datetime2(7), lease_owner uniqueidentifier, lease_epoch bigint NOT NULL DEFAULT 0,
                lease_expires_at datetime2(7), takeovers int NOT NULL DEFAULT 0, failures int NOT NULL DEFAULT 0,
                settled_epoch bigint NOT NULL DEFAULT 0, cancel_requested_at datetime2(7),
                ended_completion_id uniqueidentifier, ended_digest varbinary(64),
                requested_revision bigint NOT NULL DEFAULT 0, claimed_revision bigint,
                converged_revision bigint NOT NULL DEFAULT 0, first_requested_at datetime2(7),
                not_before datetime2(7), attempted_generation bigint NOT NULL DEFAULT 0,
                converged_generation bigint NOT NULL DEFAULT 0, PRIMARY KEY (tenant, id));
            ALTER TABLE dbo.{table} SET (LOCK_ESCALATION = DISABLE);
            CREATE INDEX {table}_due ON dbo.{table} (due_at) WHERE lease_expires_at IS NULL;
            CREATE INDEX {table}_expiry ON dbo.{table} (lease_expires_at) WHERE lease_expires_at IS NOT NULL;
            CREATE INDEX {table}_generation ON dbo.{table} (attempted_generation);
            INSERT INTO dbo.{table} (tenant, id, due_at) VALUES ('t', 1, SYSUTCDATETIME());
            CREATE TABLE dbo.{table}_marker (n int NOT NULL);
            INSERT INTO dbo.{table}_marker (n) VALUES (1);
            """, Cancel);
        var source = ReconcileSource.Define(table, StoreLocation.Table("dbo", table), TestKey.Codec, new ReconcilePolicy
        {
            Lease = new LeasePolicy { Duration = TimeSpan.FromMinutes(1) },
            Requests = RequestPolicy.Supersede,
        });
        var claim = await Store.Leases.ClaimAsync(source.Leases, LeaseCallbacks<TestKey>.None, owner, new TestKey("t", 1), Cancel);
        return (source, claim!.Lease);
    }

    private async Task InvalidateAsync(ReconcileSource<TestKey> source)
    {
        await using var connection = await OpenAsync();
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, Cancel);
        await Store.Reconcile.InvalidateAllAsync(transaction, source, Cancel);
        await transaction.CommitAsync(Cancel);
    }

    /// <summary>The epoch of the record's lease if it is still held, so a refused write left the lease as it was.</summary>
    private async Task<long?> ValidEpochAsync(ReconcileSource<TestKey> source) =>
        (long?)await database.ScalarAsync($"SELECT lease_epoch FROM dbo.[{source.Store.Name}] WHERE lease_expires_at > SYSUTCDATETIME()", Cancel);
}
