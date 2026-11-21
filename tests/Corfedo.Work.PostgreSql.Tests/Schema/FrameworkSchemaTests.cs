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

        Assert.Equal(new[] { 14, 15, 16 }, await VersionsAsync("concurrent"));
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
        Assert.Contains("is at version 0; this release needs version 16", older.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GrantsLeaveEachRuntimeRoleWithExactlyTheRuntimePrivileges()
    {
        var (role, _) = await database.CreateLoginAsync(Cancel);
        await PostgreSqlWorkSchema.MigrateAsync(database.DataSource, "exact", [role], Cancel);
        await ExecuteAsync($"GRANT TRUNCATE, TRIGGER ON exact.jobs TO {role}; GRANT CREATE ON SCHEMA exact TO {role}; REVOKE DELETE ON exact.job_runs FROM {role}");

        await PostgreSqlWorkSchema.MigrateAsync(database.DataSource, "exact", [role], Cancel);

        Assert.Equal(new[] { 14, 15, 16 }, await VersionsAsync("exact"));
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

        Assert.Equal(new[] { 14, 15, 16 }, await VersionsAsync("owned"));
    }

    [Fact]
    public async Task TheScriptAppliesTheSchemaAndGrantsAsOneStatement()
    {
        var (role, connectionString) = await database.CreateLoginAsync(Cancel);

        await ExecuteAsync(PostgreSqlWorkSchema.Script("scripted", [role]));
        await ExecuteAsync(PostgreSqlWorkSchema.Script("scripted", [role]));

        Assert.Equal(new[] { 14, 15, 16 }, await VersionsAsync("scripted"));
        await using var runtime = NpgsqlDataSource.Create(connectionString);
        await Verifying(runtime, "scripted").PrepareAsync(Cancel);
    }

    [Fact]
    public async Task AVerifyingStoreReportsASchemaCreatedForTheMigrateStepAsVersionZero()
    {
        var (role, connectionString) = await database.CreateLoginAsync(Cancel);
        await ExecuteAsync($"CREATE SCHEMA unmigrated; GRANT USAGE ON SCHEMA unmigrated TO {role}");
        await using var runtime = NpgsqlDataSource.Create(connectionString);

        var older = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Verifying(runtime, "unmigrated").PrepareAsync(Cancel));

        Assert.Contains("is at version 0; this release needs version 16", older.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheScriptQuotesSchemaAndRoleNamesThatHoldItsDollarQuoteTags()
    {
        var role = "r$corfedo_work$" + Guid.NewGuid().ToString("N")[..8];
        await ExecuteAsync($"CREATE ROLE \"{role}\"");

        await ExecuteAsync(PostgreSqlWorkSchema.Script("s$corfedo_work$x$corfedo_work_15$", [role]));

        Assert.Equal(new[] { 14, 15, 16 }, await VersionsAsync("\"s$corfedo_work$x$corfedo_work_15$\""));
    }

    [Fact]
    public async Task AMigrationWaitsForTheSessionSchemaLockOfAnEarlierRelease()
    {
        // Releases up to 0.3.0 hold the same advisory key at session level while they migrate.
        await using var earlier = await database.DataSource.OpenConnectionAsync(Cancel);
        await using (var take = new NpgsqlCommand("SELECT pg_advisory_lock(hashtextextended('corfedo-work|schema|locked', 0))", earlier))
        {
            await take.ExecuteNonQueryAsync(Cancel);
        }

        var migration = PostgreSqlWorkSchema.MigrateAsync(database.DataSource, "locked", cancellationToken: Cancel);
        await Task.Delay(500, Cancel);
        Assert.False(migration.IsCompleted);

        await using (var release = new NpgsqlCommand("SELECT pg_advisory_unlock(hashtextextended('corfedo-work|schema|locked', 0))", earlier))
        {
            await release.ExecuteNonQueryAsync(Cancel);
        }
        await migration;
        Assert.Equal(new[] { 14, 15, 16 }, await VersionsAsync("locked"));
    }

    [Fact]
    public async Task TheFailedRetentionUpgradeKeepsTheJobWindowOfStoredDefinitionsForTheirFailedJobs()
    {
        var store = new PostgreSqlWorkStore(database.DataSource, "failed_upgrade", NullLoggerFactory.Instance, TimeProvider.System);
        await store.PrepareAsync(Cancel);
        var definition = JobDefinition.Create<string>("failed.upgrade");
        var policy = new JobPolicy { RunHistory = false, Retention = TimeSpan.FromDays(3), RunHistoryRetention = TimeSpan.FromDays(30), FailedRetention = Timeout.InfiniteTimeSpan };
        await store.Jobs.StoreDefinitionAsync(definition, policy, Cancel);
        // The schema and the stored definition as the previous release leaves them.
        await ExecuteAsync("""
            ALTER TABLE failed_upgrade.job_definitions DROP COLUMN failed_retention;
            DROP INDEX failed_upgrade.jobs_failed;
            DROP INDEX failed_upgrade.jobs_retention;
            CREATE INDEX jobs_retention ON failed_upgrade.jobs (definition, ended_at) WHERE state <> 0 AND chain_id IS NULL;
            DELETE FROM failed_upgrade.schema_versions WHERE version = 16;
            """);

        await new PostgreSqlWorkStore(database.DataSource, "failed_upgrade", NullLoggerFactory.Instance, TimeProvider.System).PrepareAsync(Cancel);

        Assert.Equal(new[] { 14, 15, 16 }, await VersionsAsync("failed_upgrade"));
        var stored = (await store.Jobs.GetDefinitionAsync(definition.Key, Cancel))!;
        Assert.Equal(TimeSpan.FromDays(3), stored.Policy.FailedRetention);
        await using var command = database.DataSource.CreateCommand(
            "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'failed_upgrade' AND indexname IN ('jobs_retention', 'jobs_failed') ORDER BY indexname");
        await using var reader = await command.ExecuteReaderAsync(Cancel);
        var indexes = new List<string>();
        while (await reader.ReadAsync(Cancel))
        {
            indexes.Add(reader.GetString(1));
        }
        Assert.Collection(
            indexes,
            failed => Assert.EndsWith("(definition, ended_at, id) WHERE ((state = ANY (ARRAY[2, 4])) AND (chain_id IS NULL))", failed),
            retention => Assert.EndsWith("(definition, ended_at) WHERE ((state <> 0) AND (state <> 2) AND (state <> 4) AND (chain_id IS NULL))", retention));
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
