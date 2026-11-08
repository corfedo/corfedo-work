using Corfedo.Work.PostgreSql.Tests.Provisioning;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;

namespace Corfedo.Work.PostgreSql.Tests.Schema;

public sealed class FrameworkSchemaTests(PostgreSqlDatabase database) : IClassFixture<PostgreSqlDatabase>
{
    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ConcurrentStartsApplyEachScriptOnce()
    {
        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
            await new PostgreSqlWorkStore(database.DataSource, "concurrent", NullLoggerFactory.Instance, TimeProvider.System).PrepareAsync(Cancel))));

        Assert.Equal(new[] { 14 }, await VersionsAsync("concurrent"));
    }

    [Fact]
    public async Task AVerifyingStoreStartsUnderARuntimeLoginTheMigrateStepGranted()
    {
        var (role, connectionString) = await database.CreateLoginAsync(Cancel);
        await PostgreSqlWorkSchema.MigrateAsync(database.DataSource, "granted", [role], Cancel);
        await using var runtime = NpgsqlDataSource.Create(connectionString);

        await Verifying(runtime, "granted").PrepareAsync(Cancel);

        // The runtime login writes framework tables and changes no schema.
        await using var write = runtime.CreateCommand("DELETE FROM granted.job_pauses");
        await write.ExecuteNonQueryAsync(Cancel);
        await using var create = runtime.CreateCommand("CREATE TABLE granted.extra (id int)");
        Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, (await Assert.ThrowsAsync<PostgresException>(async () => await create.ExecuteNonQueryAsync(Cancel))).SqlState);
    }

    [Fact]
    public async Task AVerifyingStoreRefusesAMissingSchemaAnOlderVersionAndMissingPrivileges()
    {
        var (role, connectionString) = await database.CreateLoginAsync(Cancel);
        await using var runtime = NpgsqlDataSource.Create(connectionString);

        var missing = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Verifying(runtime, "absent").PrepareAsync(Cancel));
        Assert.Contains("does not exist", missing.Message, StringComparison.Ordinal);

        await PostgreSqlWorkSchema.MigrateAsync(database.DataSource, "ungranted", cancellationToken: Cancel);
        var privileges = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Verifying(runtime, "ungranted").PrepareAsync(Cancel));
        Assert.Contains("USAGE on schema ungranted", privileges.Message, StringComparison.Ordinal);
        Assert.Contains("job_pauses", privileges.Message, StringComparison.Ordinal);

        await PostgreSqlWorkSchema.MigrateAsync(database.DataSource, "ungranted", [role], Cancel);
        await ExecuteAsync("DELETE FROM ungranted.schema_versions");
        var older = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Verifying(runtime, "ungranted").PrepareAsync(Cancel));
        Assert.Contains("is at version 0; this release needs version 14", older.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GrantsLeaveEachRuntimeRoleWithExactlyTheRuntimePrivileges()
    {
        var (role, _) = await database.CreateLoginAsync(Cancel);
        await PostgreSqlWorkSchema.MigrateAsync(database.DataSource, "exact", [role], Cancel);
        await ExecuteAsync($"GRANT TRUNCATE, TRIGGER ON exact.jobs TO {role}; GRANT CREATE ON SCHEMA exact TO {role}; REVOKE DELETE ON exact.job_runs FROM {role}");

        await PostgreSqlWorkSchema.MigrateAsync(database.DataSource, "exact", [role], Cancel);

        Assert.Equal(new[] { 14 }, await VersionsAsync("exact"));
        await using var command = database.DataSource.CreateCommand($"""
            SELECT has_table_privilege('{role}', 'exact.jobs', 'TRUNCATE'), has_table_privilege('{role}', 'exact.jobs', 'TRIGGER'),
                   has_schema_privilege('{role}', 'exact', 'CREATE'), has_table_privilege('{role}', 'exact.job_runs', 'DELETE')
            """);
        await using var reader = await command.ExecuteReaderAsync(Cancel);
        await reader.ReadAsync(Cancel);
        Assert.Equal((false, false, false, true), (reader.GetBoolean(0), reader.GetBoolean(1), reader.GetBoolean(2), reader.GetBoolean(3)));
    }

    [Fact]
    public async Task TheOwnerOfASchemaCreatedForItMigratesWithoutCreateOnTheDatabase()
    {
        var (role, connectionString) = await database.CreateLoginAsync(Cancel);
        await ExecuteAsync($"CREATE SCHEMA owned AUTHORIZATION {role}");
        await using var owner = NpgsqlDataSource.Create(connectionString);

        await new PostgreSqlWorkStore(owner, "owned", NullLoggerFactory.Instance, TimeProvider.System).PrepareAsync(Cancel);

        Assert.Equal(new[] { 14 }, await VersionsAsync("owned"));
    }

    [Fact]
    public async Task TheScriptAppliesTheSchemaAndGrantsAsOneStatement()
    {
        var (role, connectionString) = await database.CreateLoginAsync(Cancel);

        await ExecuteAsync(PostgreSqlWorkSchema.Script("scripted", [role]));
        await ExecuteAsync(PostgreSqlWorkSchema.Script("scripted", [role]));

        Assert.Equal(new[] { 14 }, await VersionsAsync("scripted"));
        await using var runtime = NpgsqlDataSource.Create(connectionString);
        await Verifying(runtime, "scripted").PrepareAsync(Cancel);
    }

    private static PostgreSqlWorkStore Verifying(NpgsqlDataSource dataSource, string schema) =>
        new(dataSource, schema, NullLoggerFactory.Instance, TimeProvider.System, WorkSchemaMode.Verify);

    private async Task<int[]> VersionsAsync(string schema)
    {
        await using var command = database.DataSource.CreateCommand($"SELECT array_agg(version ORDER BY version) FROM {schema}.schema_versions");
        return (int[])(await command.ExecuteScalarAsync(Cancel))!;
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var command = database.DataSource.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(Cancel);
    }
}
