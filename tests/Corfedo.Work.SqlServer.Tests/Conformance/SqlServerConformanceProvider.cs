using System.Data;
using System.Text;
using Corfedo.Work.Conformance;
using Corfedo.Work.SqlServer.Tests.Provisioning;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work.SqlServer.Tests.Conformance;

/// <summary>Runs the conformance scenarios against a fresh SQL Server database per test class.</summary>
public sealed class SqlServerConformanceProvider(SqlServerInstance server) : IReconcileConformanceProvider, IJobConformanceProvider, IAsyncLifetime
{
    private const string Schema = "app";
    private string connectionString = "";
    private readonly SemaphoreSlim clockGate = new(1, 1);
    private SqlConnection clock = null!;
    private int tables;

    public bool HasSignals => true;

    public bool ConcurrentTransactions => true;

    /// <summary>
    /// Creates the database with the framework schema already migrated, as a deployment migrates before clients run:
    /// the signals table is a framework table, and processes that are never started send signals too.
    /// </summary>
    public async ValueTask InitializeAsync()
    {
        connectionString = await server.CreateDatabaseAsync();
        await ExecuteAsync($"CREATE SCHEMA {Schema}", CancellationToken.None);
        await CreateStore().MigrateAsync(CancellationToken.None);
        clock = await OpenAsync(CancellationToken.None);
    }

    public ValueTask DisposeAsync() => clock.DisposeAsync();

    public void AddStore(IServiceCollection services) => services.AddSqlServerWorkStore(connectionString);

    /// <summary>Each application has its own framework schema in the shared database.</summary>
    public void AddApplicationStore(IServiceCollection services, string application) => services.AddSqlServerWorkStore(connectionString, "work_" + application);

    public IWorkStore CreateStore() => new SqlServerWorkStore(connectionString, "work", NullLoggerFactory.Instance, TimeProvider.System);

    /// <summary>
    /// A table with the lease fields of every prefix and the indexes SQL Server sources need: the due index over unheld
    /// records, the expiry index over held ones, the settlement and partition indexes led by the outstanding column over
    /// held ones, and the owner index. Lock escalation is disabled, as the store requires.
    /// </summary>
    public async Task<StoreLocation> CreateTableAsync(IReadOnlyList<string> prefixes, CancellationToken cancellationToken)
    {
        var name = "records_" + Interlocked.Increment(ref tables);
        var columns = string.Join(",\n", prefixes.Select(p => $"""
            {p}due_at datetime2(7),
            {p}lease_owner uniqueidentifier,
            {p}lease_epoch bigint NOT NULL DEFAULT 0,
            {p}lease_expires_at datetime2(7),
            {p}takeovers int NOT NULL DEFAULT 0,
            {p}failures int NOT NULL DEFAULT 0,
            {p}settled_epoch bigint NOT NULL DEFAULT 0,
            {p}lease_outstanding AS (CASE WHEN {p}settled_epoch < {p}lease_epoch THEN 1 END) PERSISTED,
            {p}settle_failures int NOT NULL DEFAULT 0,
            {p}settle_retry_at datetime2(7),
            {p}cancel_requested_at datetime2(7),
            {p}execution_id uniqueidentifier,
            {p}ended_completion_id uniqueidentifier,
            {p}ended_digest varbinary(64)
            """));
        var indexes = string.Join("\n", prefixes.Select(p => $"""
            CREATE INDEX {name}_{p}due ON {Schema}.{name} ({p}due_at) WHERE {p}lease_expires_at IS NULL;
            CREATE INDEX {name}_{p}expiry ON {Schema}.{name} ({p}lease_expires_at) WHERE {p}lease_expires_at IS NOT NULL;
            CREATE INDEX {name}_{p}settlement ON {Schema}.{name} ({p}lease_outstanding, {p}lease_expires_at) WHERE {p}lease_expires_at IS NOT NULL;
            CREATE INDEX {name}_{p}partition ON {Schema}.{name} ({p}lease_outstanding, tenant, {p}lease_owner) WHERE {p}lease_expires_at IS NOT NULL;
            CREATE INDEX {name}_{p}owner ON {Schema}.{name} ({p}lease_owner) INCLUDE ({p}lease_expires_at, {p}lease_outstanding) WHERE {p}lease_expires_at IS NOT NULL;
            """));
        await ExecuteAsync($"""
            CREATE TABLE {Schema}.{name} (
                tenant nvarchar(100) COLLATE Latin1_General_100_BIN2 NOT NULL,
                id bigint NOT NULL,
                progress nvarchar(max),
                {columns},
                CONSTRAINT {name}_key PRIMARY KEY (tenant, id)
            );
            ALTER TABLE {Schema}.{name} SET (LOCK_ESCALATION = DISABLE);
            {indexes}
            CREATE TABLE {Schema}.{name}_journal (n bigint IDENTITY PRIMARY KEY, entry nvarchar(max) NOT NULL);
            """, cancellationToken);
        return StoreLocation.Table(Schema, name);
    }

    public async Task<StoreLocation> CreateReconcileTableAsync(CancellationToken cancellationToken)
    {
        var table = await CreateTableAsync([""], cancellationToken);
        await ExecuteAsync($"""
            ALTER TABLE {Name(table)} ADD
                requested_revision bigint NOT NULL DEFAULT 0,
                claimed_revision bigint,
                converged_revision bigint NOT NULL DEFAULT 0,
                first_requested_at datetime2(7),
                not_before datetime2(7),
                attempted_generation bigint NOT NULL DEFAULT 0,
                converged_generation bigint NOT NULL DEFAULT 0;
            """, cancellationToken);
        await ExecuteAsync($"CREATE INDEX {table.Name}_generation ON {Name(table)} (attempted_generation);", cancellationToken);
        return table;
    }

    public async Task InsertReconcileAsync(StoreLocation table, TestKey key, DueState due, long revision, CancellationToken cancellationToken)
    {
        var dueAt = due switch
        {
            DueState.None => "NULL",
            _ => "SYSUTCDATETIME()",
        };
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = KeyCommand(connection, null,
            $"INSERT INTO {Name(table)} (tenant, id, due_at, requested_revision, claimed_revision, converged_revision) VALUES (@tenant, @id, {dueAt}, @revision, @revision, @revision)",
            key);
        command.Parameters.Add(new SqlParameter("@revision", SqlDbType.BigInt) { Value = revision });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<ReconcileState> ReadReconcileAsync(StoreLocation table, TestKey key, CancellationToken cancellationToken)
    {
        var lease = await ReadAsync(table, key, "", cancellationToken);
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = KeyCommand(connection, null, $"""
            SELECT requested_revision, claimed_revision, converged_revision, CAST(CASE WHEN first_requested_at IS NOT NULL THEN 1 ELSE 0 END AS bit),
                   CASE WHEN due_at IS NULL OR due_at = '9999-12-31T23:59:59.9999999' THEN NULL ELSE DATEDIFF_BIG(microsecond, SYSUTCDATETIME(), due_at) END,
                   DATEDIFF_BIG(microsecond, SYSUTCDATETIME(), not_before),
                   attempted_generation, converged_generation
            FROM {Name(table)} WHERE tenant = @tenant AND id = @id
            """, key);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException($"No record {key}.");
        }
        return new ReconcileState(
            lease,
            reader.GetInt64(0),
            reader.IsDBNull(1) ? null : reader.GetInt64(1),
            reader.GetInt64(2),
            reader.GetBoolean(3),
            Micros(reader, 4),
            Micros(reader, 5),
            reader.GetInt64(6),
            reader.GetInt64(7));
    }

    /// <summary>
    /// The table's index write counters, which count every row write including one that leaves its values as they were,
    /// with a checksum of its rows.
    /// </summary>
    public async Task<string> RecordVersionsAsync(StoreLocation table, CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = new SqlCommand($"""
            SELECT CONCAT(
                (SELECT SUM(leaf_insert_count + leaf_update_count + leaf_delete_count + leaf_ghost_count)
                 FROM sys.dm_db_index_operational_stats(DB_ID(), OBJECT_ID(N'{Name(table)}'), NULL, NULL)),
                N':',
                (SELECT CHECKSUM_AGG(BINARY_CHECKSUM(*)) FROM {Name(table)}))
            """, connection);
        return (string)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    public async Task InsertAsync(StoreLocation table, TestKey key, TimeSpan? dueIn, CancellationToken cancellationToken)
    {
        var prefixes = await PrefixesAsync(table, cancellationToken);
        var dueColumns = string.Join(", ", prefixes.Select(p => p + "due_at"));
        var dueValues = string.Join(", ", prefixes.Select(_ => "@due"));
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = KeyCommand(connection, null, $"""
            DECLARE @due datetime2(7) = CASE WHEN @delay IS NULL THEN NULL
                ELSE DATEADD(nanosecond, CAST(@delay % 10000000 AS int) * 100, DATEADD(second, CAST(@delay / 10000000 AS int), SYSUTCDATETIME())) END;
            INSERT INTO {Name(table)} (tenant, id, {dueColumns}) VALUES (@tenant, @id, {dueValues});
            """, key);
        command.Parameters.Add(new SqlParameter("@delay", SqlDbType.BigInt) { Value = dueIn is { } d ? d.Ticks : DBNull.Value });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<RecordState> ReadAsync(StoreLocation table, TestKey key, string prefix, CancellationToken cancellationToken)
    {
        var p = prefix;
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = KeyCommand(connection, null, $"""
            SELECT CASE WHEN {p}due_at IS NULL THEN 0 WHEN {p}due_at <= SYSUTCDATETIME() THEN 1 ELSE 2 END,
                   {p}lease_owner, {p}lease_epoch, CAST(CASE WHEN {p}lease_expires_at IS NOT NULL THEN 1 ELSE 0 END AS bit),
                   CAST(CASE WHEN {p}lease_expires_at > SYSUTCDATETIME() THEN 1 ELSE 0 END AS bit),
                   {p}takeovers, {p}failures, {p}settled_epoch, {p}settle_failures,
                   CAST(CASE WHEN {p}cancel_requested_at IS NOT NULL THEN 1 ELSE 0 END AS bit), {p}execution_id, progress
            FROM {Name(table)} WHERE tenant = @tenant AND id = @id
            """, key);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException($"No record {key}.");
        }
        return new RecordState(
            (DueState)reader.GetInt32(0),
            reader.IsDBNull(1) ? null : new OwnerId(reader.GetGuid(1)),
            reader.GetInt64(2),
            reader.GetBoolean(3),
            reader.GetBoolean(4),
            reader.GetInt32(5),
            reader.GetInt32(6),
            reader.GetInt64(7),
            reader.GetInt32(8),
            reader.GetBoolean(9),
            reader.IsDBNull(10) ? null : reader.GetGuid(10),
            reader.IsDBNull(11) ? null : reader.GetString(11));
    }

    public async Task<IConformanceTransaction> BeginAsync(CancellationToken cancellationToken)
    {
        var connection = await OpenAsync(cancellationToken);
        return new Transaction(connection, (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken));
    }

    public async Task JournalAsync(WorkTransaction transaction, StoreLocation table, string entry, CancellationToken cancellationToken)
    {
        var sql = transaction.SqlServer();
        await using var command = new SqlCommand($"INSERT INTO {Name(table)}_journal (entry) VALUES (@entry)", sql.Connection, sql);
        command.Parameters.Add(new SqlParameter("@entry", SqlDbType.NVarChar, -1) { Value = entry });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task InsertInTransactionAsync(WorkTransaction transaction, StoreLocation table, TestKey key, CancellationToken cancellationToken)
    {
        var sql = transaction.SqlServer();
        await using var command = KeyCommand(sql.Connection, sql, $"INSERT INTO {Name(table)} (tenant, id) VALUES (@tenant, @id)", key);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<string>> ReadJournalAsync(StoreLocation table, CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = new SqlCommand($"SELECT entry FROM {Name(table)}_journal ORDER BY n", connection);
        var entries = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            entries.Add(reader.GetString(0));
        }
        return entries;
    }

    private async Task<string[]> PrefixesAsync(StoreLocation table, CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = new SqlCommand(
            "SELECT LEFT(column_name, LEN(column_name) - LEN('due_at')) FROM information_schema.columns "
            + "WHERE table_schema = @schema AND table_name = @table AND column_name LIKE '%due_at' ORDER BY ordinal_position",
            connection);
        command.Parameters.Add(new SqlParameter("@schema", SqlDbType.NVarChar, 128) { Value = table.Schema });
        command.Parameters.Add(new SqlParameter("@table", SqlDbType.NVarChar, 128) { Value = table.Name });
        var prefixes = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            prefixes.Add(reader.GetString(0));
        }
        return [.. prefixes];
    }

    public async Task<JobState> ReadJobAsync(JobId job, CancellationToken cancellationToken) =>
        (await ReadJobsAsync("id = @value", new SqlParameter("@value", SqlDbType.UniqueIdentifier) { Value = job.Value }, cancellationToken)).SingleOrDefault()
        ?? throw new InvalidOperationException($"No job {job}.");

    public Task<IReadOnlyList<JobState>> ReadJobsAsync(string definition, CancellationToken cancellationToken) =>
        ReadJobsAsync("definition = @value", new SqlParameter("@value", SqlDbType.NVarChar, 200) { Value = definition }, cancellationToken);

    private async Task<IReadOnlyList<JobState>> ReadJobsAsync(string filter, SqlParameter parameter, CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = new SqlCommand($"""
            SELECT id, definition, CAST(NULLIF(state, 0) AS int), CAST(outcome AS int), reason, args, args_version, started, attempts,
                   pending_args,
                   CASE WHEN due_at IS NULL THEN NULL ELSE DATEDIFF_BIG(microsecond, SYSUTCDATETIME(), due_at) END,
                   DATEDIFF_BIG(microsecond, SYSUTCDATETIME(), pending_due),
                   CAST(CASE WHEN lease_expires_at > SYSUTCDATETIME() THEN 1 ELSE 0 END AS bit),
                   CAST(CASE WHEN cancel_requested_at IS NOT NULL THEN 1 ELSE 0 END AS bit), execution_id, partition_key,
                   occurrence, unconsumed, skip_pending, paused, ended_at, CAST(CASE WHEN merge_key IS NOT NULL THEN 1 ELSE 0 END AS bit),
                   calendar_unavailable, waited
            FROM work.jobs WHERE {filter} ORDER BY created_at, id
            """, connection);
        command.Parameters.Add(parameter);
        var jobs = new List<JobState>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            jobs.Add(new JobState(
                new JobId(reader.GetGuid(0)),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : (JobOutcome)reader.GetInt32(2),
                reader.IsDBNull(3) ? null : (JobOutcome)reader.GetInt32(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                ArgsValue(reader.GetFieldValue<byte[]>(5)),
                reader.GetInt32(6),
                reader.GetBoolean(7),
                reader.GetInt32(8),
                reader.IsDBNull(9) ? null : ArgsValue(reader.GetFieldValue<byte[]>(9)),
                Micros(reader, 10),
                Micros(reader, 11),
                reader.GetBoolean(12),
                reader.GetBoolean(13),
                reader.GetGuid(14),
                reader.GetString(15),
                reader.IsDBNull(16) ? null : reader.GetString(16),
                reader.GetBoolean(17),
                reader.GetBoolean(18),
                reader.GetBoolean(19),
                reader.IsDBNull(20) ? null : new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(20), DateTimeKind.Utc)),
                reader.GetBoolean(21),
                reader.GetBoolean(22),
                reader.GetBoolean(23)));
        }
        return jobs;
    }

    public async Task<bool> ReadRecurrencePausedAsync(string definition, Subject subject, CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = SubjectCommand(connection, null, "SELECT paused FROM work.job_recurrences WHERE definition = @definition AND subject_kind = @kind AND subject_id = @id", definition, subject);
        return await command.ExecuteScalarAsync(cancellationToken) as bool? ?? throw new InvalidOperationException($"No recurrence of {definition} for {subject}.");
    }

    public async Task LockSubjectAsync(WorkTransaction transaction, string definition, Subject subject, CancellationToken cancellationToken)
    {
        var sql = transaction.SqlServer();
        // An update that changes nothing locks the rows through their key, as a writer does; a locking read can be
        // served by a covering secondary index and then leaves the rows themselves unlocked.
        foreach (var table in new[] { "work.job_recurrences", "work.jobs" })
        {
            await using var command = SubjectCommand(sql.Connection, sql,
                $"UPDATE {table} SET subject_id = subject_id WHERE definition = @definition AND subject_kind = @kind AND subject_id = @id", definition, subject);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    /// <summary>
    /// Reads the store's clock on a connection kept open for it, one caller at a time: a new connection may take a
    /// login, which costs hundreds of milliseconds on SQL Server 2025 under the suite's load and would count as lag in
    /// the scenarios that measure it.
    /// </summary>
    public async Task<DateTimeOffset> NowAsync(CancellationToken cancellationToken)
    {
        await clockGate.WaitAsync(cancellationToken);
        try
        {
            await using var command = new SqlCommand("SELECT SYSUTCDATETIME()", clock);
            return new DateTimeOffset(DateTime.SpecifyKind((DateTime)(await command.ExecuteScalarAsync(cancellationToken))!, DateTimeKind.Utc));
        }
        finally
        {
            clockGate.Release();
        }
    }

    /// <summary>The database's <c>Transactions/sec</c> counter, a running total of the transactions it started.</summary>
    public async Task<long> CommittedTransactionsAsync(CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = new SqlCommand(
            "SELECT cntr_value FROM sys.dm_os_performance_counters WHERE object_name LIKE N'%:Databases%' AND counter_name = N'Transactions/sec' AND instance_name = DB_NAME()",
            connection);
        return (long)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    public async Task<ProcessRecord?> ReadProcessAsync(OwnerId owner, CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = new SqlCommand("""
            SELECT p.type, p.release, DATEDIFF_BIG(microsecond, p.heartbeat_at, SYSUTCDATETIME()), CAST(CASE WHEN p.stopped_at IS NOT NULL THEN 1 ELSE 0 END AS bit),
                   d.definition, d.revision, d.runs, d.declares_recurrence
            FROM work.processes AS p LEFT JOIN work.process_definitions AS d ON d.owner = p.owner
            WHERE p.owner = @owner ORDER BY d.definition;
            SELECT definition, args_version FROM work.process_definition_args WHERE owner = @owner ORDER BY definition, args_version;
            """, connection);
        command.Parameters.Add(new SqlParameter("@owner", SqlDbType.UniqueIdentifier) { Value = owner.Value });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        (string Type, string? Release, TimeSpan Age, bool Stopped)? process = null;
        var rows = new List<(string Definition, long Revision, bool Runs, bool Declares)>();
        while (await reader.ReadAsync(cancellationToken))
        {
            process ??= (reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1), TimeSpan.FromTicks(reader.GetInt64(2) * 10), reader.GetBoolean(3));
            if (!reader.IsDBNull(4))
            {
                rows.Add((reader.GetString(4), reader.GetInt64(5), reader.GetBoolean(6), reader.GetBoolean(7)));
            }
        }
        await reader.NextResultAsync(cancellationToken);
        var args = new Dictionary<string, List<int>>(StringComparer.Ordinal);
        while (await reader.ReadAsync(cancellationToken))
        {
            var definition = reader.GetString(0);
            if (!args.TryGetValue(definition, out var versions))
            {
                args[definition] = versions = [];
            }
            versions.Add(reader.GetInt32(1));
        }
        if (process is not { } p)
        {
            return null;
        }
        var definitions = rows.Select(r => new ProcessDefinition(r.Definition, r.Revision, r.Runs, args.GetValueOrDefault(r.Definition)?.ToArray() ?? [], r.Declares)).ToList();
        return new ProcessRecord(p.Type, p.Release, p.Age, p.Stopped, definitions);
    }

    public async Task AgeProcessAsync(OwnerId owner, TimeSpan by, CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = new SqlCommand("""
            UPDATE work.processes SET
                started_at = DATEADD(nanosecond, -CAST(@by % 10000000 AS int) * 100, DATEADD(second, -CAST(@by / 10000000 AS int), started_at)),
                heartbeat_at = DATEADD(nanosecond, -CAST(@by % 10000000 AS int) * 100, DATEADD(second, -CAST(@by / 10000000 AS int), heartbeat_at)),
                stopped_at = DATEADD(nanosecond, -CAST(@by % 10000000 AS int) * 100, DATEADD(second, -CAST(@by / 10000000 AS int), stopped_at))
            WHERE owner = @owner
            """, connection);
        command.Parameters.Add(new SqlParameter("@owner", SqlDbType.UniqueIdentifier) { Value = owner.Value });
        command.Parameters.Add(new SqlParameter("@by", SqlDbType.BigInt) { Value = by.Ticks });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task DeleteProcessAsync(OwnerId owner, CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = new SqlCommand("DELETE FROM work.processes WHERE owner = @owner", connection);
        command.Parameters.Add(new SqlParameter("@owner", SqlDbType.UniqueIdentifier) { Value = owner.Value });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>The value of conformance arguments, stored as <c>{"Value":"..."}</c>; empty for other jobs, such as retention.</summary>
    private static string ArgsValue(byte[] json) =>
        System.Text.Json.JsonDocument.Parse(Encoding.UTF8.GetString(json)).RootElement.TryGetProperty("Value", out var value) ? value.GetString()! : "";

    private static TimeSpan? Micros(SqlDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : TimeSpan.FromTicks(reader.GetInt64(ordinal) * 10);

    private static string Name(StoreLocation table) => $"{table.Schema}.{table.Name}";

    private async Task<SqlConnection> OpenAsync(CancellationToken cancellationToken)
    {
        var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }

    private async Task ExecuteAsync(string text, CancellationToken cancellationToken)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = new SqlCommand(text, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static SqlCommand KeyCommand(SqlConnection connection, SqlTransaction? transaction, string text, TestKey key)
    {
        var command = new SqlCommand(text, connection, transaction);
        command.Parameters.Add(new SqlParameter("@tenant", SqlDbType.NVarChar, 100) { Value = key.Tenant });
        command.Parameters.Add(new SqlParameter("@id", SqlDbType.BigInt) { Value = key.Id });
        return command;
    }

    private static SqlCommand SubjectCommand(SqlConnection connection, SqlTransaction? transaction, string text, string definition, Subject subject)
    {
        var command = new SqlCommand(text, connection, transaction);
        command.Parameters.Add(new SqlParameter("@definition", SqlDbType.NVarChar, 200) { Value = definition });
        command.Parameters.Add(new SqlParameter("@kind", SqlDbType.NVarChar, 100) { Value = subject.Kind });
        command.Parameters.Add(new SqlParameter("@id", SqlDbType.NVarChar, 400) { Value = subject.Id });
        return command;
    }

    private sealed class Transaction(SqlConnection connection, SqlTransaction transaction) : IConformanceTransaction
    {
        public WorkTransaction Work { get; } = transaction;

        public async Task WriteProgressAsync(StoreLocation table, TestKey key, string progress, CancellationToken cancellationToken)
        {
            await using var command = KeyCommand(connection, transaction, $"UPDATE {Name(table)} SET progress = @progress WHERE tenant = @tenant AND id = @id", key);
            command.Parameters.Add(new SqlParameter("@progress", SqlDbType.NVarChar, -1) { Value = progress });
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        public async Task<string?> ReadProgressAsync(StoreLocation table, TestKey key, CancellationToken cancellationToken)
        {
            await using var command = KeyCommand(connection, transaction, $"SELECT progress FROM {Name(table)} WHERE tenant = @tenant AND id = @id", key);
            return await command.ExecuteScalarAsync(cancellationToken) as string;
        }

        public async Task LockAsync(StoreLocation table, TestKey key, CancellationToken cancellationToken)
        {
            await using var command = KeyCommand(connection, transaction, $"SELECT 1 FROM {Name(table)} WITH (UPDLOCK, ROWLOCK) WHERE tenant = @tenant AND id = @id", key);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        public Task SavepointAsync(CancellationToken cancellationToken) => transaction.SaveAsync("conformance", cancellationToken);

        public Task RollbackToSavepointAsync(CancellationToken cancellationToken) => transaction.RollbackAsync("conformance", cancellationToken);

        public Task CommitAsync(CancellationToken cancellationToken) => transaction.CommitAsync(cancellationToken);

        public Task RollbackAsync(CancellationToken cancellationToken) => transaction.RollbackAsync(cancellationToken);

        public async ValueTask DisposeAsync()
        {
            await transaction.DisposeAsync();
            await connection.DisposeAsync();
        }
    }
}

public sealed class SqlServerLeaseClaimScenarios(SqlServerConformanceProvider provider)
    : LeaseClaimScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerLeaseHolderScenarios(SqlServerConformanceProvider provider)
    : LeaseHolderScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerLeaseTakeoverScenarios(SqlServerConformanceProvider provider)
    : LeaseTakeoverScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerLeaseLimitScenarios(SqlServerConformanceProvider provider)
    : LeaseLimitScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerRemoteHolderScenarios(SqlServerConformanceProvider provider)
    : RemoteHolderScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerReconcileRequestScenarios(SqlServerConformanceProvider provider)
    : ReconcileRequestScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerReconcileResultScenarios(SqlServerConformanceProvider provider)
    : ReconcileResultScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerReconcileInvalidationScenarios(SqlServerConformanceProvider provider)
    : ReconcileInvalidationScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerReconcileSupersessionScenarios(SqlServerConformanceProvider provider)
    : ReconcileSupersessionScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerReconcilePoolScenarios(SqlServerConformanceProvider provider)
    : ReconcilePoolScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerJobRequestScenarios(SqlServerConformanceProvider provider)
    : JobRequestScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerJobAttemptScenarios(SqlServerConformanceProvider provider)
    : JobAttemptScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerJobDefinitionScenarios(SqlServerConformanceProvider provider)
    : JobDefinitionScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerJobOutcomeScenarios(SqlServerConformanceProvider provider)
    : JobOutcomeScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerJobRecurrenceScenarios(SqlServerConformanceProvider provider)
    : JobRecurrenceScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerJobLifecycleScenarios(SqlServerConformanceProvider provider)
    : JobLifecycleScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerJobOverlapScenarios(SqlServerConformanceProvider provider)
    : JobOverlapScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerJobPauseScenarios(SqlServerConformanceProvider provider)
    : JobPauseScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerJobResultScenarios(SqlServerConformanceProvider provider)
    : JobResultScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerJobStatusScenarios(SqlServerConformanceProvider provider)
    : JobStatusScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerJobRunHistoryScenarios(SqlServerConformanceProvider provider)
    : JobRunHistoryScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerJobActionScenarios(SqlServerConformanceProvider provider)
    : JobActionScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerJobRetentionScenarios(SqlServerConformanceProvider provider)
    : JobRetentionScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerProcessRegistryScenarios(SqlServerConformanceProvider provider)
    : ProcessRegistryScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerJobCoverageScenarios(SqlServerConformanceProvider provider)
    : JobCoverageScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerJobCalendarScenarios(SqlServerConformanceProvider provider)
    : JobCalendarScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerJobChainScenarios(SqlServerConformanceProvider provider)
    : JobChainScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerJobMonitoringScenarios(SqlServerConformanceProvider provider)
    : JobMonitoringScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerSignalScenarios(SqlServerConformanceProvider provider)
    : SignalScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerJobTelemetryScenarios(SqlServerConformanceProvider provider)
    : JobTelemetryScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerHostStartScenarios(SqlServerConformanceProvider provider)
    : HostStartScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerProcessRoleScenarios(SqlServerConformanceProvider provider)
    : ProcessRoleScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerApplicationIsolationScenarios(SqlServerConformanceProvider provider)
    : ApplicationIsolationScenarios(provider), IClassFixture<SqlServerConformanceProvider>;

public sealed class SqlServerControlledClockScenarios(SqlServerConformanceProvider provider)
    : ControlledClockScenarios(provider), IClassFixture<SqlServerConformanceProvider>;
