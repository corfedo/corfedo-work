using System.Data;
using Corfedo.Work.Conformance;
using Corfedo.Work.PostgreSql.Tests.Provisioning;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;

namespace Corfedo.Work.PostgreSql.Tests.Reconcile;

/// <summary>
/// Holder writes on superseding sources compare the generation after the fence is granted. A snapshot taken before an
/// invalidation committed would still show the old generation, so those transactions must be READ COMMITTED.
/// </summary>
public sealed class GenerationFenceIsolationTests(PostgreSqlDatabase database) : IClassFixture<PostgreSqlDatabase>
{
    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    private readonly OwnerId owner = OwnerId.New();

    private IWorkStore Store => new PostgreSqlWorkStore(database.DataSource, "work", NullLoggerFactory.Instance, TimeProvider.System);

    [Theory]
    [InlineData(Operation.Renew, IsolationLevel.RepeatableRead)]
    [InlineData(Operation.Complete, IsolationLevel.RepeatableRead)]
    [InlineData(Operation.End, IsolationLevel.RepeatableRead)]
    [InlineData(Operation.Renew, IsolationLevel.Serializable)]
    [InlineData(Operation.Complete, IsolationLevel.Serializable)]
    [InlineData(Operation.End, IsolationLevel.Serializable)]
    public async Task SnapshotIsolatedHolderWriteIsRefused(Operation operation, IsolationLevel isolation)
    {
        var (source, lease) = await ClaimedAsync();
        await using var connection = await database.DataSource.OpenConnectionAsync(Cancel);
        await using var transaction = await connection.BeginTransactionAsync(isolation, Cancel);
        await SnapshotAsync(connection, transaction);
        await InvalidateAsync(source);

        await Assert.ThrowsAsync<InvalidOperationException>(() => WriteAsync(operation, transaction, lease));
        // The refusal aborts the transaction, so output written and committed after ignoring it does not commit.
        await Record.ExceptionAsync(async () =>
        {
            await using (var output = new NpgsqlCommand($"UPDATE public.{source.Store.Name} SET progress = 'obsolete'", connection, transaction))
            {
                await output.ExecuteNonQueryAsync(Cancel);
            }
            await transaction.CommitAsync(Cancel);
        });

        Assert.Equal<long?>(lease.Epoch, await ValidEpochAsync(source));
        await using var progress = database.DataSource.CreateCommand($"SELECT count(*) FROM public.{source.Store.Name} WHERE progress IS NOT NULL");
        Assert.Equal(0L, (long)(await progress.ExecuteScalarAsync(Cancel))!);
    }

    [Theory]
    [InlineData(Operation.Renew)]
    [InlineData(Operation.Complete)]
    [InlineData(Operation.End)]
    public async Task ReadCommittedHolderWriteSeesAnInvalidationThatCommittedAfterItBegan(Operation operation)
    {
        var (source, lease) = await ClaimedAsync();
        await using var connection = await database.DataSource.OpenConnectionAsync(Cancel);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, Cancel);
        await SnapshotAsync(connection, transaction);
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

    private static async Task SnapshotAsync(NpgsqlConnection connection, NpgsqlTransaction transaction)
    {
        await using var command = new NpgsqlCommand("SELECT count(*) FROM work.reconcile_generations", connection, transaction);
        await command.ExecuteScalarAsync(Cancel);
    }

    private async Task<(ReconcileSource<TestKey> Source, Lease<TestKey> Lease)> ClaimedAsync()
    {
        await Store.MigrateAsync(Cancel);
        var table = "fence_" + Guid.NewGuid().ToString("N");
        await using (var command = database.DataSource.CreateCommand($"""
            CREATE TABLE public.{table} (
                tenant text NOT NULL, id bigint NOT NULL, progress text,
                due_at timestamptz, lease_owner uuid, lease_epoch bigint NOT NULL DEFAULT 0,
                lease_expires_at timestamptz, takeovers integer NOT NULL DEFAULT 0, failures integer NOT NULL DEFAULT 0,
                settled_epoch bigint NOT NULL DEFAULT 0, cancel_requested_at timestamptz,
                ended_completion_id uuid, ended_digest bytea,
                requested_revision bigint NOT NULL DEFAULT 0, claimed_revision bigint,
                converged_revision bigint NOT NULL DEFAULT 0, first_requested_at timestamptz,
                not_before timestamptz, attempted_generation bigint NOT NULL DEFAULT 0,
                converged_generation bigint NOT NULL DEFAULT 0, PRIMARY KEY (tenant, id));
            INSERT INTO public.{table} (tenant, id, due_at) VALUES ('t', 1, clock_timestamp());
            """))
        {
            await command.ExecuteNonQueryAsync(Cancel);
        }
        var source = ReconcileSource.Define(table, StoreLocation.Table("public", table), TestKey.Codec, new ReconcilePolicy
        {
            Lease = new LeasePolicy { Duration = TimeSpan.FromMinutes(1) },
            Requests = RequestPolicy.Supersede,
        });
        var claim = await Store.Leases.ClaimAsync(source.Leases, LeaseCallbacks<TestKey>.None, owner, new TestKey("t", 1), Cancel);
        return (source, claim!.Lease);
    }

    private async Task InvalidateAsync(ReconcileSource<TestKey> source)
    {
        await using var connection = await database.DataSource.OpenConnectionAsync(Cancel);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, Cancel);
        await Store.Reconcile.InvalidateAllAsync(transaction, source, Cancel);
        await transaction.CommitAsync(Cancel);
    }

    /// <summary>The epoch of the record's lease if it is still held, so a refused write left the lease as it was.</summary>
    private async Task<long?> ValidEpochAsync(ReconcileSource<TestKey> source)
    {
        await using var command = database.DataSource.CreateCommand($"SELECT lease_epoch FROM public.{source.Store.Name} WHERE lease_expires_at > clock_timestamp()");
        return (long?)await command.ExecuteScalarAsync(Cancel);
    }
}
