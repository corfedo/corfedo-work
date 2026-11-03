using System.Data;
using Corfedo.Work.Conformance;
using Corfedo.Work.SqlServer.Tests.Provisioning;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work.SqlServer.Tests.Leases;

/// <summary>
/// The locks a due claim holds until it commits: rows it reads and does not claim stay unlocked, rows other
/// transactions hold are skipped, and no index entry is locked before its row.
/// </summary>
public sealed class ClaimLockTests(SqlServerDatabase database) : IClassFixture<SqlServerDatabase>
{
    private const string Table = "claim_locks";

    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ADueClaimLocksOnlyTheRowsItClaims()
    {
        await database.ExecuteAsync($"""
            CREATE TABLE {Table} (
                tenant nvarchar(100) NOT NULL, id bigint NOT NULL, due_at datetime2(7), lease_owner uniqueidentifier, lease_epoch bigint NOT NULL DEFAULT 0,
                lease_expires_at datetime2(7), takeovers int NOT NULL DEFAULT 0, failures int NOT NULL DEFAULT 0,
                settled_epoch bigint NOT NULL DEFAULT 0, cancel_requested_at datetime2(7), execution_id uniqueidentifier,
                ended_completion_id uniqueidentifier, ended_digest varbinary(64), CONSTRAINT {Table}_key PRIMARY KEY (tenant, id));
            ALTER TABLE {Table} SET (LOCK_ESCALATION = DISABLE);
            CREATE INDEX {Table}_due ON {Table} (due_at) WHERE lease_expires_at IS NULL;
            CREATE INDEX {Table}_expiry ON {Table} (lease_expires_at) WHERE lease_expires_at IS NOT NULL;
            INSERT INTO {Table} (tenant, id, due_at)
            SELECT 't', n, DATEADD(millisecond, -n, SYSUTCDATETIME()) FROM (SELECT TOP (1000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_columns) AS v;
            """, Cancel);
        var source = LeaseSource.Define(Table, StoreLocation.Table("dbo", Table), TestKey.Codec, new LeasePolicy { Duration = TimeSpan.FromMinutes(1) });
        var store = new SqlServerWorkStore(database.ConnectionString, "work", NullLoggerFactory.Instance, TimeProvider.System).Leases;
        // Another transaction holds the 100 records due first.
        await using var holder = new SqlConnection(database.ConnectionString);
        await holder.OpenAsync(Cancel);
        await using var held = (SqlTransaction)await holder.BeginTransactionAsync(IsolationLevel.ReadCommitted, Cancel);
        await using (var hold = new SqlCommand($"SELECT COUNT(*) FROM {Table} WITH (UPDLOCK, ROWLOCK) WHERE tenant = 't' AND id > 900", holder, held))
        {
            await hold.ExecuteScalarAsync(Cancel);
        }
        var locks = new List<Dictionary<string, int>>();
        var callbacks = new LeaseCallbacks<TestKey>(async (_, work, ct) => locks.Add(await LocksAsync(work.SqlServer(), ct)), null);

        var claims = await store.ClaimDueAsync(source, callbacks, OwnerId.New(), 32, Cancel);

        Assert.Equal(32, claims.Claims.Count);
        Assert.All(claims.Claims, c => Assert.InRange(c.Lease.Key.Id, 869, 900));
        // At the first claim hook all 32 candidates are locked through the key, and only the first claimed row has
        // changed its index entries.
        var first = locks[0];
        Assert.Equal(32, first.GetValueOrDefault($"KEY {Table}_key"));
        Assert.InRange(first.GetValueOrDefault($"KEY {Table}_due"), 0, 1);
        Assert.InRange(first.GetValueOrDefault($"KEY {Table}_expiry"), 0, 1);
        Assert.DoesNotContain(first.Keys, k => k is "OBJECT X" or "OBJECT S" or "OBJECT U" or "OBJECT SIX");
        await held.RollbackAsync(Cancel);
    }

    /// <summary>The locks the transaction holds by kind and mode for objects, and by index for keys.</summary>
    private static async Task<Dictionary<string, int>> LocksAsync(SqlTransaction transaction, CancellationToken cancellationToken)
    {
        await using var command = new SqlCommand("""
            SELECT CASE WHEN l.resource_type = 'KEY' THEN 'KEY ' + i.name ELSE l.resource_type + ' ' + l.request_mode END, COUNT(*)
            FROM sys.dm_tran_locks AS l
            LEFT JOIN sys.partitions AS p ON l.resource_type IN ('KEY', 'PAGE') AND p.hobt_id = l.resource_associated_entity_id
            LEFT JOIN sys.indexes AS i ON i.object_id = p.object_id AND i.index_id = p.index_id
            WHERE l.request_session_id = @@SPID AND l.resource_database_id = DB_ID()
            GROUP BY CASE WHEN l.resource_type = 'KEY' THEN 'KEY ' + i.name ELSE l.resource_type + ' ' + l.request_mode END
            """, transaction.Connection, transaction);
        var locks = new Dictionary<string, int>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            locks[reader.GetString(0)] = reader.GetInt32(1);
        }
        return locks;
    }
}
