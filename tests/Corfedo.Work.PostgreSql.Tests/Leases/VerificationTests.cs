using Corfedo.Work.Conformance;
using Corfedo.Work.PostgreSql.Tests.Provisioning;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work.PostgreSql.Tests.Leases;

public sealed class VerificationTests(PostgreSqlDatabase database) : IClassFixture<PostgreSqlDatabase>
{
    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task DueIndexMustLeaveHeldRecordsOut()
    {
        var source = await SourceAsync("plain_due_index", "CREATE INDEX ON plain_due_index (due_at)", new LeasePolicy { Duration = TimeSpan.FromMinutes(1) });

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Store().VerifyAsync(source, Cancel));

        Assert.Contains("no index led by due_at WHERE lease_expires_at IS NULL", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PartitionLimitNeedsAPartitionIndexOverOutstandingLeases()
    {
        var policy = new LeasePolicy { Duration = TimeSpan.FromMinutes(1), PartitionLimit = 1 };
        var unheld = "CREATE INDEX ON {0} (due_at) WHERE lease_expires_at IS NULL";
        var plain = await SourceAsync("plain_partition_index", string.Format(null, unheld, "plain_partition_index") + "; CREATE INDEX ON plain_partition_index (tenant)", policy, "tenant");
        var outstanding = await SourceAsync(
            "outstanding_partition_index",
            string.Format(null, unheld, "outstanding_partition_index") + "; CREATE INDEX ON outstanding_partition_index (tenant) WHERE settled_epoch < lease_epoch AND lease_expires_at IS NOT NULL",
            policy,
            "tenant");

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Store().VerifyAsync(plain, Cancel));

        Assert.Contains("no index led by tenant WHERE lease_expires_at IS NOT NULL AND settled_epoch < lease_epoch", error.Message, StringComparison.Ordinal);
        await Store().VerifyAsync(outstanding, Cancel);
    }

    [Fact]
    public async Task SettlingSourcesNeedAnExpiryIndexOverUnsettledLeases()
    {
        var policy = new LeasePolicy { Duration = TimeSpan.FromMinutes(1), OwnerLimit = 1 };
        var due = "CREATE INDEX ON {0} (due_at) WHERE lease_expires_at IS NULL; CREATE INDEX ON {0} (lease_owner)";
        var all = await SourceAsync("all_expired_index", string.Format(null, due, "all_expired_index"), policy, settlementIndex: false);
        var unsettled = await SourceAsync("unsettled_expired_index", string.Format(null, due, "unsettled_expired_index"), policy);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Store().VerifyAsync(all, Cancel));

        Assert.Contains("no index led by lease_expires_at WHERE lease_expires_at IS NOT NULL AND settled_epoch < lease_epoch (expiry settlement)", error.Message, StringComparison.Ordinal);
        await Store().VerifyAsync(unsettled, Cancel);
    }

    [Fact]
    public async Task SettlingExpiryNeedsTheSettlementRetryFields()
    {
        var source = await SourceAsync(
            "no_retry_fields",
            "CREATE INDEX ON no_retry_fields (due_at) WHERE lease_expires_at IS NULL",
            new LeasePolicy { Duration = TimeSpan.FromMinutes(1), SettleExpiry = true });

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Store().VerifyAsync(source, Cancel));

        Assert.Contains("field settle_failures is missing", error.Message, StringComparison.Ordinal);
        Assert.Contains("field settle_retry_at is missing", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NullablePartitionIsRefusedForPartitionLimitsAndClaimedWithoutThem()
    {
        await using (var command = database.DataSource.CreateCommand("""
            CREATE TABLE regional (
                tenant text NOT NULL, id bigint NOT NULL, region text, due_at timestamptz DEFAULT clock_timestamp(), lease_owner uuid,
                lease_epoch bigint NOT NULL DEFAULT 0, lease_expires_at timestamptz, takeovers integer NOT NULL DEFAULT 0,
                failures integer NOT NULL DEFAULT 0, settled_epoch bigint NOT NULL DEFAULT 0, cancel_requested_at timestamptz,
                execution_id uuid, ended_completion_id uuid, ended_digest bytea, PRIMARY KEY (tenant, id));
            CREATE INDEX ON regional (lease_expires_at);
            CREATE INDEX ON regional (due_at) WHERE lease_expires_at IS NULL;
            CREATE INDEX ON regional (region);
            INSERT INTO regional (tenant, id) VALUES ('t', 1);
            """))
        {
            await command.ExecuteNonQueryAsync(Cancel);
        }
        var duration = TimeSpan.FromMinutes(1);
        var limited = LeaseSource.Define("regional-limited", StoreLocation.Table("public", "regional"), TestKey.Codec, new LeasePolicy { Duration = duration, PartitionLimit = 1 }, "region");
        var unlimited = LeaseSource.Define("regional", StoreLocation.Table("public", "regional"), TestKey.Codec, new LeasePolicy { Duration = duration }, "region");

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Store().VerifyAsync(limited, Cancel));
        Assert.Contains("partition field region must be NOT NULL", error.Message, StringComparison.Ordinal);
        await Store().VerifyAsync(unlimited, Cancel);
        var claims = await Store().ClaimDueAsync(unlimited, LeaseCallbacks<TestKey>.None, OwnerId.New(), 1, Cancel);
        Assert.Null(Assert.Single(claims.Claims).Partition);
    }

    private ILeaseStore Store() => new PostgreSqlWorkStore(database.DataSource, "work", NullLoggerFactory.Instance, TimeProvider.System).Leases;

    private async Task<LeaseSource<TestKey>> SourceAsync(string table, string dueIndex, LeasePolicy policy, string? partition = null, bool settlementIndex = true)
    {
        await using (var command = database.DataSource.CreateCommand($"""
            CREATE TABLE {table} (
                tenant text NOT NULL, id bigint NOT NULL, due_at timestamptz, lease_owner uuid, lease_epoch bigint NOT NULL DEFAULT 0,
                lease_expires_at timestamptz, takeovers integer NOT NULL DEFAULT 0, failures integer NOT NULL DEFAULT 0,
                settled_epoch bigint NOT NULL DEFAULT 0, cancel_requested_at timestamptz, execution_id uuid,
                ended_completion_id uuid, ended_digest bytea, PRIMARY KEY (tenant, id));
            CREATE INDEX ON {table} (lease_expires_at);
            {(settlementIndex ? $"CREATE INDEX ON {table} (lease_expires_at) WHERE lease_expires_at IS NOT NULL AND settled_epoch < lease_epoch;" : "")}
            {dueIndex};
            """))
        {
            await command.ExecuteNonQueryAsync(Cancel);
        }
        return LeaseSource.Define(table, StoreLocation.Table("public", table), TestKey.Codec, policy, partition);
    }
}
