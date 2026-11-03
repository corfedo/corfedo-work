using Corfedo.Work.SqlServer.Tests.Provisioning;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work.SqlServer.Tests.Schema;

public sealed class FrameworkSchemaTests(SqlServerInstance server, SqlServerDatabase database) : IClassFixture<SqlServerDatabase>
{
    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ConcurrentStartsApplyEachScriptOnceWithLockEscalationDisabled()
    {
        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
            await new SqlServerWorkStore(database.ConnectionString, "concurrent", NullLoggerFactory.Instance, TimeProvider.System).MigrateAsync(Cancel))));

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
            await new SqlServerWorkStore(connectionString, "work", NullLoggerFactory.Instance, TimeProvider.System).MigrateAsync(Cancel));

        Assert.Contains("SET READ_COMMITTED_SNAPSHOT ON", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BaselineKeepsASignalsTableCreatedBeforeIt()
    {
        var store = new SqlServerWorkStore(database.ConnectionString, "signals_first", NullLoggerFactory.Instance, TimeProvider.System);
        await store.Signals.SendAsync(null, WorkTopics.Due, "early", Cancel);

        await store.MigrateAsync(Cancel);

        Assert.Equal(1, await database.ScalarAsync("SELECT COUNT(*) FROM signals_first.signals", Cancel));
        Assert.Equal(1, await database.ScalarAsync("SELECT COUNT(*) FROM signals_first.schema_versions", Cancel));
    }
}
