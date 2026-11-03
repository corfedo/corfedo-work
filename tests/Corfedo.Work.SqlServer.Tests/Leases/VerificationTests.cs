using Corfedo.Work.Conformance;
using Corfedo.Work.SqlServer.Tests.Provisioning;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work.SqlServer.Tests.Leases;

public sealed class VerificationTests(SqlServerDatabase database) : IClassFixture<SqlServerDatabase>
{
    private const string Indexes = """
        CREATE INDEX {0}_due ON {0} (due_at) WHERE lease_expires_at IS NULL;
        CREATE INDEX {0}_expiry ON {0} (lease_expires_at) INCLUDE (settled_epoch, lease_epoch) WHERE lease_expires_at IS NOT NULL;
        """;

    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ASourceWithItsIndexesAndWithoutLockEscalationIsAccepted()
    {
        var source = await SourceAsync("complete", Indexes);

        await Store().VerifyAsync(source, Cancel);
    }

    [Fact]
    public async Task DueIndexMustLeaveHeldRecordsOut()
    {
        var source = await SourceAsync("plain_due_index", """
            CREATE INDEX {0}_due ON {0} (due_at);
            CREATE INDEX {0}_expiry ON {0} (lease_expires_at) INCLUDE (settled_epoch, lease_epoch) WHERE lease_expires_at IS NOT NULL;
            """);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Store().VerifyAsync(source, Cancel));

        Assert.Contains("no index led by due_at WHERE lease_expires_at IS NULL", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SettlingSourcesNeedTheEpochsInTheExpiryIndex()
    {
        var source = await SourceAsync("expiry_without_epochs", """
            CREATE INDEX {0}_due ON {0} (due_at) WHERE lease_expires_at IS NULL;
            CREATE INDEX {0}_expiry ON {0} (lease_expires_at) WHERE lease_expires_at IS NOT NULL;
            CREATE INDEX {0}_owner ON {0} (lease_owner);
            """, new LeasePolicy { Duration = TimeSpan.FromMinutes(1), OwnerLimit = 1 });

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Store().VerifyAsync(source, Cancel));

        Assert.Contains("no index led by lease_expires_at carrying settled_epoch and lease_epoch", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PartitionLimitNeedsAPartitionIndexOverHeldLeases()
    {
        var policy = new LeasePolicy { Duration = TimeSpan.FromMinutes(1), PartitionLimit = 1 };
        var plain = await SourceAsync("plain_partition_index", Indexes + "CREATE INDEX {0}_tenant ON {0} (tenant);", policy, "tenant");
        var held = await SourceAsync("held_partition_index", Indexes + "CREATE INDEX {0}_tenant ON {0} (tenant) INCLUDE (settled_epoch, lease_epoch) WHERE lease_expires_at IS NOT NULL;", policy, "tenant");

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Store().VerifyAsync(plain, Cancel));

        Assert.Contains("no index led by tenant WHERE lease_expires_at IS NOT NULL carrying settled_epoch and lease_epoch", error.Message, StringComparison.Ordinal);
        await Store().VerifyAsync(held, Cancel);
    }

    [Fact]
    public async Task ATableThatEscalatesLocksIsRefused()
    {
        var source = await SourceAsync("escalating", Indexes, escalation: false);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Store().VerifyAsync(source, Cancel));

        Assert.Contains("SET (LOCK_ESCALATION = DISABLE)", error.Message, StringComparison.Ordinal);
    }

    private ILeaseStore Store() => new SqlServerWorkStore(database.ConnectionString, "work", NullLoggerFactory.Instance, TimeProvider.System).Leases;

    private async Task<LeaseSource<TestKey>> SourceAsync(string table, string indexes, LeasePolicy? policy = null, string? partition = null, bool escalation = true)
    {
        await database.ExecuteAsync($"""
            CREATE TABLE {table} (
                tenant nvarchar(100) NOT NULL, id bigint NOT NULL, due_at datetime2(7), lease_owner uniqueidentifier, lease_epoch bigint NOT NULL DEFAULT 0,
                lease_expires_at datetime2(7), takeovers int NOT NULL DEFAULT 0, failures int NOT NULL DEFAULT 0,
                settled_epoch bigint NOT NULL DEFAULT 0, cancel_requested_at datetime2(7), execution_id uniqueidentifier,
                ended_completion_id uniqueidentifier, ended_digest varbinary(64), PRIMARY KEY (tenant, id));
            {(escalation ? $"ALTER TABLE {table} SET (LOCK_ESCALATION = DISABLE);" : "")}
            {string.Format(null, indexes, table)}
            """, Cancel);
        return LeaseSource.Define(table, StoreLocation.Table("dbo", table), TestKey.Codec, policy ?? new LeasePolicy { Duration = TimeSpan.FromMinutes(1) }, partition);
    }
}
