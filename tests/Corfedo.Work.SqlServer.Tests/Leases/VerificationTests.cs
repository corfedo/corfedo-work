using Corfedo.Work.Conformance;
using Corfedo.Work.SqlServer.Tests.Provisioning;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work.SqlServer.Tests.Leases;

public sealed class VerificationTests(SqlServerDatabase database) : IClassFixture<SqlServerDatabase>
{
    private const string Indexes = """
        CREATE INDEX {0}_due ON {0} (due_at) WHERE lease_expires_at IS NULL;
        CREATE INDEX {0}_expiry ON {0} (lease_expires_at) WHERE lease_expires_at IS NOT NULL;
        """;

    private const string Settlement = "CREATE INDEX {0}_settlement ON {0} (lease_outstanding, lease_expires_at) WHERE lease_expires_at IS NOT NULL;";

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
            CREATE INDEX {0}_expiry ON {0} (lease_expires_at) WHERE lease_expires_at IS NOT NULL;
            """);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Store().VerifyAsync(source, Cancel));

        Assert.Contains("no index led by due_at WHERE lease_expires_at IS NULL", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SettlingSourcesNeedASettlementIndexLedByTheOutstandingColumn()
    {
        var policy = new LeasePolicy { Duration = TimeSpan.FromMinutes(1), OwnerLimit = 1 };
        var missing = await SourceAsync("no_settlement_index", Indexes + "CREATE INDEX {0}_owner ON {0} (lease_owner);", policy);
        var settling = await SourceAsync("settlement_index", Indexes + Settlement + "CREATE INDEX {0}_owner ON {0} (lease_owner);", policy);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Store().VerifyAsync(missing, Cancel));

        Assert.Contains(
            "no index led by lease_outstanding, lease_expires_at WHERE lease_expires_at IS NOT NULL (expiry settlement and next expiry)",
            error.Message,
            StringComparison.Ordinal);
        await Store().VerifyAsync(settling, Cancel);
    }

    [Fact]
    public async Task PartitionLimitNeedsAPartitionIndexLedByTheOutstandingColumn()
    {
        var policy = new LeasePolicy { Duration = TimeSpan.FromMinutes(1), PartitionLimit = 1 };
        var plain = await SourceAsync("plain_partition_index", Indexes + Settlement + "CREATE INDEX {0}_tenant ON {0} (tenant);", policy, "tenant");
        var held = await SourceAsync(
            "held_partition_index",
            Indexes + Settlement + "CREATE INDEX {0}_tenant ON {0} (lease_outstanding, tenant) WHERE lease_expires_at IS NOT NULL;",
            policy,
            "tenant");

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Store().VerifyAsync(plain, Cancel));

        Assert.Contains("no index led by lease_outstanding, tenant WHERE lease_expires_at IS NOT NULL (partition and spread limits)", error.Message, StringComparison.Ordinal);
        await Store().VerifyAsync(held, Cancel);
    }

    [Theory]
    [InlineData("AS (CASE WHEN settled_epoch <= lease_epoch THEN 1 END) PERSISTED")]
    [InlineData("AS (CASE WHEN settled_epoch < lease_epoch THEN 1 END)")]
    [InlineData("int")]
    public async Task TheOutstandingColumnMustBeThePersistedExpression(string declaration)
    {
        var table = "outstanding_" + Guid.NewGuid().ToString("N")[..8];
        var source = await SourceAsync(
            table,
            Indexes + Settlement + "CREATE INDEX {0}_owner ON {0} (lease_owner);",
            new LeasePolicy { Duration = TimeSpan.FromMinutes(1), OwnerLimit = 1 },
            outstanding: declaration);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Store().VerifyAsync(source, Cancel));

        Assert.Contains(
            "field lease_outstanding must be a persisted computed column, CASE WHEN [settled_epoch] < [lease_epoch] THEN 1 END",
            error.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task VerificationDisablesLockEscalationOnASourceTable()
    {
        var source = await SourceAsync("escalating", Indexes, escalation: false);

        await Store().VerifyAsync(source, Cancel);

        Assert.Equal("DISABLE", (string?)await database.ScalarAsync("SELECT lock_escalation_desc FROM sys.tables WHERE object_id = OBJECT_ID(N'dbo.escalating')", Cancel));
    }

    [Fact]
    public async Task ALoginThatCannotAlterAnEscalatingTableIsRefusedWithTheStatement()
    {
        var source = await SourceAsync("escalating_readonly", Indexes, escalation: false);
        var login = "reader_" + Guid.NewGuid().ToString("N")[..8];
        var password = "Pw-" + Guid.NewGuid().ToString("N");
        await database.ExecuteAsync($"""
            CREATE LOGIN [{login}] WITH PASSWORD = '{password}', CHECK_POLICY = OFF;
            CREATE USER [{login}] FOR LOGIN [{login}];
            GRANT SELECT, INSERT, UPDATE, DELETE ON dbo.escalating_readonly TO [{login}];
            """, Cancel);
        var connectionString = new SqlConnectionStringBuilder(database.ConnectionString) { UserID = login, Password = password, Pooling = false }.ConnectionString;
        try
        {
            var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await Store(connectionString).VerifyAsync(source, Cancel));

            Assert.Contains("ALTER TABLE [dbo].[escalating_readonly] SET (LOCK_ESCALATION = DISABLE)", error.Message, StringComparison.Ordinal);
            Assert.Equal("TABLE", (string?)await database.ScalarAsync("SELECT lock_escalation_desc FROM sys.tables WHERE object_id = OBJECT_ID(N'dbo.escalating_readonly')", Cancel));
        }
        finally
        {
            // Logins belong to the server, which test processes may share. A login is dropped once its sessions have
            // ended, which a closed connection's logout or KILL only requests.
            await database.ExecuteAsync($"""
                DECLARE @kill nvarchar(max) = N'', @waits int = 0;
                SELECT @kill += N'KILL ' + CAST(session_id AS nvarchar(10)) + N';' FROM sys.dm_exec_sessions WHERE login_name = N'{login}';
                EXEC (@kill);
                WHILE EXISTS (SELECT 1 FROM sys.dm_exec_sessions WHERE login_name = N'{login}') AND @waits < 100
                BEGIN
                    WAITFOR DELAY '00:00:00.020';
                    SET @waits += 1;
                END
                DROP USER [{login}];
                DROP LOGIN [{login}];
                """, Cancel);
        }
    }

    private ILeaseStore Store(string? connectionString = null) =>
        new SqlServerWorkStore(connectionString ?? database.ConnectionString, "work", NullLoggerFactory.Instance, TimeProvider.System).Leases;

    private async Task<LeaseSource<TestKey>> SourceAsync(
        string table,
        string indexes,
        LeasePolicy? policy = null,
        string? partition = null,
        bool escalation = true,
        string outstanding = "AS (CASE WHEN settled_epoch < lease_epoch THEN 1 END) PERSISTED")
    {
        await database.ExecuteAsync($"""
            CREATE TABLE {table} (
                tenant nvarchar(100) NOT NULL, id bigint NOT NULL, due_at datetime2(7), lease_owner uniqueidentifier, lease_epoch bigint NOT NULL DEFAULT 0,
                lease_expires_at datetime2(7), takeovers int NOT NULL DEFAULT 0, failures int NOT NULL DEFAULT 0,
                settled_epoch bigint NOT NULL DEFAULT 0, lease_outstanding {outstanding}, cancel_requested_at datetime2(7), execution_id uniqueidentifier,
                ended_completion_id uniqueidentifier, ended_digest varbinary(64), PRIMARY KEY (tenant, id));
            {(escalation ? $"ALTER TABLE {table} SET (LOCK_ESCALATION = DISABLE);" : "")}
            {string.Format(null, indexes, table)}
            """, Cancel);
        return LeaseSource.Define(table, StoreLocation.Table("dbo", table), TestKey.Codec, policy ?? new LeasePolicy { Duration = TimeSpan.FromMinutes(1) }, partition);
    }
}
