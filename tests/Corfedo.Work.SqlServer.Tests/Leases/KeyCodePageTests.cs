using Corfedo.Work.Conformance;
using Corfedo.Work.SqlServer.Tests.Provisioning;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work.SqlServer.Tests.Leases;

/// <summary>
/// Keys of <c>varchar</c> and <c>char</c> columns whose code page is not the database's. Each table also holds the key
/// as the database's code page would turn it, with <c>?</c> for the characters it lacks, which no operation on the key
/// may reach. A key with characters the column's own code page lacks is no record's key.
/// </summary>
public sealed class KeyCodePageTests(SqlServerDatabase database) : IClassFixture<SqlServerDatabase>
{
    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("cyrillic", "varchar(8) COLLATE Cyrillic_General_100_BIN2", "Привет", "??????")]
    [InlineData("utf8", "varchar(16) COLLATE Latin1_General_100_BIN2_UTF8", "Привет", "??????")]
    [InlineData("japanese", "varchar(8) COLLATE Japanese_XJIS_140_BIN2", "日本語", "???")]
    [InlineData("long_cyrillic", "varchar(5000) COLLATE Cyrillic_General_100_BIN2", "Привет", "??????")]
    public async Task EveryOperationReachesTheRecordOfAKeyOutsideTheDatabasesCodePage(string table, string column, string key, string lossy)
    {
        var (source, store) = await SourceAsync(table, column, KeyCodec.String("id"), $"(N'{key}'), (N'{lossy}')");

        Assert.True(await ScheduleAsync(store, source, key));
        Assert.Equal([key], await DueAsync(table));
        var due = await store.Leases.ClaimDueAsync(source, LeaseCallbacks<string>.None, OwnerId.New(), 10, Cancel);
        var lease = Assert.Single(due.Claims).Lease;
        Assert.Equal(key, lease.Key);
        Assert.True((await store.Leases.RenewAsync(lease, Cancel)).Valid);
        Assert.True(await store.Leases.ReleaseAsync(lease, Due.Now, false, Cancel));

        var claim = await store.Leases.ClaimAsync(source, LeaseCallbacks<string>.None, OwnerId.New(), key, Cancel);

        Assert.Equal((key, 2L), (claim?.Lease.Key, claim?.Lease.Epoch));
        Assert.Equal(0L, await ScalarAsync<long>($"SELECT lease_epoch FROM {table} WHERE id = '{lossy}'"));
    }

    [Fact]
    public async Task ACharKeyOutsideTheDatabasesCodePageReachesItsPaddedRecord()
    {
        var (source, store) = await SourceAsync("char_cyrillic", "char(8) COLLATE Cyrillic_General_100_BIN2", KeyCodec.String("id"), "(N'Привет'), (N'??????')");

        Assert.True(await ScheduleAsync(store, source, "Привет"));
        var due = await store.Leases.ClaimDueAsync(source, LeaseCallbacks<string>.None, OwnerId.New(), 10, Cancel);
        Assert.Equal("Привет  ", Assert.Single(due.Claims).Lease.Key);
        Assert.True(await store.Leases.ReleaseAsync(due.Claims[0].Lease, Due.Now, false, Cancel));

        var claim = await store.Leases.ClaimAsync(source, LeaseCallbacks<string>.None, OwnerId.New(), "Привет", Cancel);

        Assert.Equal(2L, claim?.Lease.Epoch);
    }

    [Fact]
    public async Task ACompositeKeyOutsideTheDatabasesCodePageIsClaimedWhenDue()
    {
        var (source, store) = await SourceAsync("composite_cyrillic", "varchar(16) COLLATE Cyrillic_General_100_BIN2", TestKey.Codec, "(N'арендатор', 1), (N'?????????', 1)", composite: true);
        var key = new TestKey("арендатор", 1);

        Assert.True(await ScheduleAsync(store, source, key));
        var due = await store.Leases.ClaimDueAsync(source, LeaseCallbacks<TestKey>.None, OwnerId.New(), 10, Cancel);

        Assert.Equal([key], due.Claims.Select(c => c.Lease.Key));
        Assert.Equal(0L, await ScalarAsync<long>("SELECT lease_epoch FROM composite_cyrillic WHERE tenant = '?????????'"));
    }

    [Theory]
    [InlineData("latin1_question_marks", "Привет", "??????")]
    [InlineData("latin1_best_fit", "ābc", "abc")]
    public async Task AKeyWithCharactersTheColumnLacksMatchesNoRecord(string table, string key, string stored)
    {
        // SQL Server stores the key as the column's code page turns it, so the table holds only that record.
        var (source, store) = await SourceAsync(table, "varchar(8) COLLATE Latin1_General_100_BIN2", KeyCodec.String("id"), $"(N'{key}')");
        Assert.Equal(stored, await ScalarAsync<string>($"SELECT CONVERT(nvarchar(8), id) FROM {table}"));

        Assert.False(await ScheduleAsync(store, source, key));
        Assert.Null(await store.Leases.ClaimAsync(source, LeaseCallbacks<string>.None, OwnerId.New(), key, Cancel));
        await using (var connection = new SqlConnection(database.ConnectionString))
        {
            await connection.OpenAsync(Cancel);
            await using var transaction = connection.BeginTransaction();
            Assert.Empty(await store.Leases.ClaimAsync(transaction, source, LeaseCallbacks<string>.None, OwnerId.New(), [key], 10, Cancel));
            transaction.Commit();
        }

        Assert.Equal(0L, await ScalarAsync<long>($"SELECT lease_epoch FROM {table}"));
        Assert.Empty(await DueAsync(table));
    }

    private async Task<(LeaseSource<TKey> Source, SqlServerWorkStore Store)> SourceAsync<TKey>(string table, string column, IKeyCodec<TKey> codec, string rows, bool composite = false)
        where TKey : notnull
    {
        Assert.Equal(1252, await ScalarAsync<int>("SELECT CAST(COLLATIONPROPERTY(CONVERT(nvarchar(128), DATABASEPROPERTYEX(DB_NAME(), 'Collation')), 'CodePage') AS int)"));
        var (keys, keyColumns) = composite ? ("tenant, id", $"tenant {column} NOT NULL, id bigint NOT NULL") : ("id", $"id {column} NOT NULL");
        await database.ExecuteAsync($"""
            CREATE TABLE {table} (
                {keyColumns}, due_at datetime2(7), lease_owner uniqueidentifier, lease_epoch bigint NOT NULL DEFAULT 0,
                lease_expires_at datetime2(7), takeovers int NOT NULL DEFAULT 0, failures int NOT NULL DEFAULT 0,
                settled_epoch bigint NOT NULL DEFAULT 0, cancel_requested_at datetime2(7), execution_id uniqueidentifier,
                ended_completion_id uniqueidentifier, ended_digest varbinary(64), CONSTRAINT {table}_key PRIMARY KEY ({keys}));
            ALTER TABLE {table} SET (LOCK_ESCALATION = DISABLE);
            CREATE INDEX {table}_due ON {table} (due_at) WHERE lease_expires_at IS NULL;
            CREATE INDEX {table}_expiry ON {table} (lease_expires_at) WHERE lease_expires_at IS NOT NULL;
            INSERT INTO {table} ({keys}) VALUES {rows};
            """, Cancel);
        var store = new SqlServerWorkStore(database.ConnectionString, "work", NullLoggerFactory.Instance, TimeProvider.System);
        await store.MigrateAsync(Cancel);
        return (LeaseSource.Define(table, StoreLocation.Table("dbo", table), codec, new LeasePolicy { Duration = TimeSpan.FromMinutes(1) }), store);
    }

    private async Task<bool> ScheduleAsync<TKey>(SqlServerWorkStore store, LeaseSource<TKey> source, TKey key)
        where TKey : notnull
    {
        await using var connection = new SqlConnection(database.ConnectionString);
        await connection.OpenAsync(Cancel);
        await using var transaction = connection.BeginTransaction();
        var scheduled = await store.Leases.ScheduleAsync(transaction, source, key, Due.Now, null, Cancel);
        transaction.Commit();
        return scheduled;
    }

    /// <summary>The keys of the table's due records, as Unicode.</summary>
    private async Task<List<string>> DueAsync(string table)
    {
        await using var connection = new SqlConnection(database.ConnectionString);
        await connection.OpenAsync(Cancel);
        await using var command = new SqlCommand($"SELECT CONVERT(nvarchar(4000), id) FROM {table} WHERE due_at IS NOT NULL", connection);
        var keys = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(Cancel);
        while (await reader.ReadAsync(Cancel))
        {
            keys.Add(reader.GetString(0));
        }
        return keys;
    }

    private async Task<T> ScalarAsync<T>(string text) => (T)(await database.ScalarAsync(text, Cancel))!;
}
