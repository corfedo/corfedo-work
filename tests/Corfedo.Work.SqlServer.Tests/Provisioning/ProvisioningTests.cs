namespace Corfedo.Work.SqlServer.Tests.Provisioning;

public sealed class ProvisioningTests(SqlServerDatabase database) : IClassFixture<SqlServerDatabase>
{
    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task DatabaseReadsCommittedSnapshotsOnASupportedVersion()
    {
        var major = (string)(await database.ScalarAsync("SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS nvarchar(10))", Cancel))!;
        Assert.Contains(major, new[] { "16", "17" });
        Assert.Equal(true, await database.ScalarAsync("SELECT is_read_committed_snapshot_on FROM sys.databases WHERE database_id = DB_ID()", Cancel));
    }

    [Fact]
    public async Task TempdbMetadataIsMemoryOptimized() =>
        Assert.Equal(1, await database.ScalarAsync("SELECT CAST(SERVERPROPERTY('IsTempdbMetadataMemoryOptimized') AS int)", Cancel));
}
