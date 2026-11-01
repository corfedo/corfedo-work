using Corfedo.Work.PostgreSql.Tests.Provisioning;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work.PostgreSql.Tests.Schema;

public sealed class FrameworkSchemaTests(PostgreSqlDatabase database) : IClassFixture<PostgreSqlDatabase>
{
    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ConcurrentStartsApplyEachScriptOnce()
    {
        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
            await new PostgreSqlWorkStore(database.DataSource, "concurrent", NullLoggerFactory.Instance, TimeProvider.System).MigrateAsync(Cancel))));

        await using var command = database.DataSource.CreateCommand("SELECT array_agg(version ORDER BY version) FROM concurrent.schema_versions");
        var versions = (int[])(await command.ExecuteScalarAsync(Cancel))!;
        Assert.Equal([14], versions);
    }
}
