namespace Corfedo.Work.PostgreSql.Tests.Provisioning;

public sealed class ProvisioningTests(PostgreSqlDatabase database) : IClassFixture<PostgreSqlDatabase>
{
    [Fact]
    public async Task DatabaseRunsPostgreSql18()
    {
        await using var command = database.DataSource.CreateCommand("SHOW server_version_num");
        var version = (string)(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
        Assert.StartsWith("18", version);
    }
}
