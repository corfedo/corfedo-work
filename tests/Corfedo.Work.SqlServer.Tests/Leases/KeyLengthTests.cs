using Corfedo.Work.SqlServer.Tests.Provisioning;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work.SqlServer.Tests.Leases;

/// <summary>
/// The length of <c>varchar</c> key columns is counted in bytes of the column's code page: a key longer than that is no
/// record's key, and one that fits matches its record.
/// </summary>
public sealed class KeyLengthTests(SqlServerDatabase database) : IClassFixture<SqlServerDatabase>
{
    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task AKeyLongerInUtf8BytesThanItsColumnMatchesNoRecord()
    {
        // Eight characters and nine UTF-8 bytes; the column holds eight bytes, and the record's key is the first seven.
        var (source, store) = await SourceAsync("utf8_keys", "Latin1_General_100_BIN2_UTF8", "abcdefg");

        await using (var connection = new SqlConnection(database.ConnectionString))
        {
            await connection.OpenAsync(Cancel);
            await using var transaction = connection.BeginTransaction();
            Assert.False(await store.Leases.ScheduleAsync(transaction, source, "abcdefgé", Due.None, null, Cancel));
            transaction.Commit();
        }
        Assert.Null(await store.Leases.ClaimAsync(source, LeaseCallbacks<string>.None, OwnerId.New(), "abcdefgé", Cancel));

        var claim = await store.Leases.ClaimAsync(source, LeaseCallbacks<string>.None, OwnerId.New(), "abcdefg", Cancel);
        Assert.Equal(("abcdefg", 1L), (claim?.Lease.Key, claim?.Lease.Epoch));
    }

    [Fact]
    public async Task AKeyThatFitsItsCodePageMatchesItsRecord()
    {
        // Eight characters, eight bytes in code page 1252 and nine in UTF-8.
        var (source, store) = await SourceAsync("latin1_keys", "Latin1_General_100_BIN2", "abcdefgé");

        var claim = await store.Leases.ClaimAsync(source, LeaseCallbacks<string>.None, OwnerId.New(), "abcdefgé", Cancel);

        Assert.Equal("abcdefgé", claim?.Lease.Key);
    }

    private async Task<(LeaseSource<string> Source, SqlServerWorkStore Store)> SourceAsync(string table, string collation, string key)
    {
        await database.ExecuteAsync($"""
            CREATE TABLE {table} (
                id varchar(8) COLLATE {collation} NOT NULL, due_at datetime2(7), lease_owner uniqueidentifier, lease_epoch bigint NOT NULL DEFAULT 0,
                lease_expires_at datetime2(7), takeovers int NOT NULL DEFAULT 0, failures int NOT NULL DEFAULT 0,
                settled_epoch bigint NOT NULL DEFAULT 0, cancel_requested_at datetime2(7), execution_id uniqueidentifier,
                ended_completion_id uniqueidentifier, ended_digest varbinary(64), CONSTRAINT {table}_key PRIMARY KEY (id));
            ALTER TABLE {table} SET (LOCK_ESCALATION = DISABLE);
            CREATE INDEX {table}_due ON {table} (due_at) WHERE lease_expires_at IS NULL;
            CREATE INDEX {table}_expiry ON {table} (lease_expires_at) WHERE lease_expires_at IS NOT NULL;
            INSERT INTO {table} (id, due_at) VALUES (N'{key}', SYSUTCDATETIME());
            """, Cancel);
        var store = new SqlServerWorkStore(database.ConnectionString, "work", NullLoggerFactory.Instance, TimeProvider.System);
        await store.MigrateAsync(Cancel);
        return (LeaseSource.Define(table, StoreLocation.Table("dbo", table), KeyCodec.String("id"), new LeasePolicy { Duration = TimeSpan.FromMinutes(1) }), store);
    }
}
