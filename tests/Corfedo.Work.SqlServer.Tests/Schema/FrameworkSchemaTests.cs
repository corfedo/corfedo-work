using Corfedo.Work.SqlServer.Tests.Provisioning;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work.SqlServer.Tests.Schema;

public sealed class FrameworkSchemaTests(SqlServerInstance server, SqlServerDatabase database) : IClassFixture<SqlServerDatabase>
{
    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ConcurrentStartsApplyEachScriptOnceWithLockEscalationDisabled()
    {
        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
            await new SqlServerWorkStore(database.ConnectionString, "concurrent", NullLoggerFactory.Instance, TimeProvider.System).PrepareAsync(Cancel))));

        Assert.Equal("1", await database.ScalarAsync("SELECT STRING_AGG(CAST(version AS nvarchar(10)), N',') WITHIN GROUP (ORDER BY version) FROM concurrent.schema_versions", Cancel));
        Assert.Equal(0, await database.ScalarAsync(
            "SELECT COUNT(*) FROM sys.tables AS t JOIN sys.schemas AS s ON s.schema_id = t.schema_id WHERE s.name = N'concurrent' AND t.lock_escalation_desc <> N'DISABLE'", Cancel));
        Assert.NotNull(await database.ScalarAsync("SELECT OBJECT_ID(N'concurrent.signals', N'U')", Cancel));
    }

    [Fact]
    public async Task StartRefusesADatabaseWithoutReadCommittedSnapshots()
    {
        var connectionString = await server.CreateDatabaseAsync(snapshotReads: false, Cancel);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await new SqlServerWorkStore(connectionString, "work", NullLoggerFactory.Instance, TimeProvider.System).PrepareAsync(Cancel));

        Assert.Contains("SET READ_COMMITTED_SNAPSHOT ON", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BaselineKeepsASignalsTableCreatedBeforeIt()
    {
        var store = new SqlServerWorkStore(database.ConnectionString, "signals_first", NullLoggerFactory.Instance, TimeProvider.System);
        await store.Signals.SendAsync(null, WorkTopics.Due, "early", Cancel);

        await store.PrepareAsync(Cancel);

        Assert.Equal(1, await database.ScalarAsync("SELECT COUNT(*) FROM signals_first.signals", Cancel));
        Assert.Equal(1, await database.ScalarAsync("SELECT COUNT(*) FROM signals_first.schema_versions", Cancel));
    }

    [Fact]
    public async Task AVerifyingStoreStartsUnderARuntimeUserTheMigrateStepGranted()
    {
        var (user, connectionString) = await server.CreateLoginAsync(database.ConnectionString, Cancel);
        await SqlServerWorkSchema.MigrateAsync(database.ConnectionString, "granted", [user], Cancel);

        await Verifying(connectionString, "granted").PrepareAsync(Cancel);

        // The runtime user writes framework tables and changes no schema.
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(Cancel);
        await using var write = new SqlCommand("DELETE FROM granted.job_pauses", connection);
        await write.ExecuteNonQueryAsync(Cancel);
        await using var create = new SqlCommand("CREATE TABLE granted.extra (id int)", connection);
        Assert.Equal(262, (await Assert.ThrowsAsync<SqlException>(async () => await create.ExecuteNonQueryAsync(Cancel))).Number);
    }

    [Fact]
    public async Task AVerifyingStoreRefusesAMissingSchemaAnOlderVersionAndMissingPermissions()
    {
        var (user, connectionString) = await server.CreateLoginAsync(database.ConnectionString, Cancel);

        var missing = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Verifying(connectionString, "absent").PrepareAsync(Cancel));
        Assert.Contains("does not exist", missing.Message, StringComparison.Ordinal);

        await SqlServerWorkSchema.MigrateAsync(database.ConnectionString, "ungranted", cancellationToken: Cancel);
        await database.ExecuteAsync($"GRANT SELECT ON ungranted.schema_versions TO [{user}]", Cancel);
        var permissions = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Verifying(connectionString, "ungranted").PrepareAsync(Cancel));
        Assert.Contains("job_pauses", permissions.Message, StringComparison.Ordinal);
        Assert.Contains("signals", permissions.Message, StringComparison.Ordinal);

        await SqlServerWorkSchema.MigrateAsync(database.ConnectionString, "ungranted", [user], Cancel);
        await database.ExecuteAsync("DELETE FROM ungranted.schema_versions", Cancel);
        var older = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Verifying(connectionString, "ungranted").PrepareAsync(Cancel));
        Assert.Contains("is at version 0; this release needs version 1", older.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GrantsLeaveEachRuntimeUserWithExactlyTheRuntimePermissions()
    {
        var (user, _) = await server.CreateLoginAsync(database.ConnectionString, Cancel);
        await SqlServerWorkSchema.MigrateAsync(database.ConnectionString, "exact", [user], Cancel);
        await database.ExecuteAsync($"GRANT ALTER ON exact.jobs TO [{user}]; GRANT CONTROL ON SCHEMA::exact TO [{user}]; DENY DELETE ON exact.job_runs TO [{user}]", Cancel);

        await SqlServerWorkSchema.MigrateAsync(database.ConnectionString, "exact", [user], Cancel);

        Assert.Equal("1", await database.ScalarAsync("SELECT STRING_AGG(CAST(version AS nvarchar(10)), N',') FROM exact.schema_versions", Cancel));
        Assert.Equal(
            "GRANT DELETE schema,GRANT INSERT schema,GRANT SELECT schema,GRANT UPDATE schema,GRANT VIEW DEFINITION schema",
            await database.ScalarAsync($"""
                SELECT STRING_AGG(CONCAT(p.state_desc, N' ', p.permission_name, N' ', COALESCE(o.name, N'schema')) COLLATE DATABASE_DEFAULT, N',') WITHIN GROUP (ORDER BY p.permission_name)
                FROM sys.database_permissions AS p LEFT JOIN sys.objects AS o ON p.class = 1 AND o.object_id = p.major_id
                WHERE p.grantee_principal_id = DATABASE_PRINCIPAL_ID(N'{user}') AND p.class IN (1, 3)
                """, Cancel));
    }

    [Fact]
    public async Task TheScriptAppliesTheSchemaAndGrantsAsOneBatch()
    {
        var (user, connectionString) = await server.CreateLoginAsync(database.ConnectionString, Cancel);

        await database.ExecuteAsync(SqlServerWorkSchema.Script("scripted", [user]), Cancel);
        await database.ExecuteAsync(SqlServerWorkSchema.Script("scripted", [user]), Cancel);

        Assert.Equal("1", await database.ScalarAsync("SELECT STRING_AGG(CAST(version AS nvarchar(10)), N',') FROM scripted.schema_versions", Cancel));
        await Verifying(connectionString, "scripted").PrepareAsync(Cancel);
    }

    [Fact]
    public async Task AMigrationWaitsForTheSessionSchemaLockOfAnEarlierRelease()
    {
        // Releases up to 0.3.0 hold the same applock at session level while they migrate.
        await using var earlier = new SqlConnection(database.ConnectionString);
        await earlier.OpenAsync(Cancel);
        var name = Tsql.LockName("schema", "locked");
        await using (var take = new SqlCommand($"EXEC sp_getapplock @Resource = N'{name}', @LockMode = 'Exclusive', @LockOwner = 'Session'", earlier))
        {
            await take.ExecuteNonQueryAsync(Cancel);
        }

        var migration = SqlServerWorkSchema.MigrateAsync(database.ConnectionString, "locked", cancellationToken: Cancel);
        await Task.Delay(500, Cancel);
        Assert.False(migration.IsCompleted);

        await using (var release = new SqlCommand($"EXEC sp_releaseapplock @Resource = N'{name}', @LockOwner = 'Session'", earlier))
        {
            await release.ExecuteNonQueryAsync(Cancel);
        }
        await migration;
        Assert.Equal("1", await database.ScalarAsync("SELECT STRING_AGG(CAST(version AS nvarchar(10)), N',') FROM locked.schema_versions", Cancel));
    }

    private static SqlServerWorkStore Verifying(string connectionString, string schema) =>
        new(connectionString, schema, NullLoggerFactory.Instance, TimeProvider.System, WorkSchemaMode.Verify);
}
