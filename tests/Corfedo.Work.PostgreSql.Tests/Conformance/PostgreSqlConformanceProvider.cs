using Corfedo.Work.Conformance;
using Corfedo.Work.PostgreSql.Tests.Provisioning;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;

namespace Corfedo.Work.PostgreSql.Tests.Conformance;

/// <summary>
/// Runs the conformance scenarios against a fresh PostgreSQL database per test class, as a deployment with a separate
/// migrate step runs: the database's owner migrates the framework schemas and creates the application's tables, and
/// stores and application transactions use a runtime login that holds only the runtime privileges on the framework
/// schema and DML on the application's tables. Stores verify their schema, so a statement that needs more fails.
/// </summary>
public sealed class PostgreSqlConformanceProvider(PostgreSqlServer server) : IReconcileConformanceProvider, IJobConformanceProvider, IAsyncLifetime
{
    private const string Schema = "app";
    private NpgsqlDataSource dataSource = null!;
    private NpgsqlDataSource runtime = null!;
    private string role = "";
    private int tables;

    public bool HasSignals => true;

    public bool ConcurrentTransactions => true;

    public async ValueTask InitializeAsync()
    {
        var connectionString = await server.CreateDatabaseAsync();
        dataSource = NpgsqlDataSource.Create(connectionString);
        role = "runtime_" + Guid.NewGuid().ToString("N")[..12];
        var password = Guid.NewGuid().ToString("N");
        await using (var command = dataSource.CreateCommand($"""
            CREATE ROLE {role} LOGIN PASSWORD '{password}';
            CREATE SCHEMA {Schema};
            GRANT USAGE ON SCHEMA {Schema} TO {role};
            ALTER DEFAULT PRIVILEGES IN SCHEMA {Schema} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {role};
            ALTER DEFAULT PRIVILEGES IN SCHEMA {Schema} GRANT USAGE, SELECT ON SEQUENCES TO {role};
            """))
        {
            await command.ExecuteNonQueryAsync();
        }
        await PostgreSqlWorkSchema.MigrateAsync(dataSource, "work", [role]);
        runtime = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(connectionString) { Username = role, Password = password }.ConnectionString);
    }

    public async ValueTask DisposeAsync()
    {
        await runtime.DisposeAsync();
        await dataSource.DisposeAsync();
    }

    public void AddStore(IServiceCollection services) => services.AddPostgreSqlWorkStore(runtime, schemaMode: WorkSchemaMode.Verify);

    /// <summary>Each application has its own framework schema in the shared database, migrated before its store starts.</summary>
    public void AddApplicationStore(IServiceCollection services, string application)
    {
        PostgreSqlWorkSchema.MigrateAsync(dataSource, "work_" + application, [role]).GetAwaiter().GetResult();
        services.AddPostgreSqlWorkStore(runtime, "work_" + application, WorkSchemaMode.Verify);
    }

    public IWorkStore CreateStore() => new PostgreSqlWorkStore(runtime, "work", NullLoggerFactory.Instance, TimeProvider.System, WorkSchemaMode.Verify);

    public async Task<StoreLocation> CreateTableAsync(IReadOnlyList<string> prefixes, CancellationToken cancellationToken)
    {
        var name = "records_" + Interlocked.Increment(ref tables);
        var columns = string.Join(",\n", prefixes.Select(p => $"""
            {p}due_at timestamptz,
            {p}lease_owner uuid,
            {p}lease_epoch bigint NOT NULL DEFAULT 0,
            {p}lease_expires_at timestamptz,
            {p}takeovers integer NOT NULL DEFAULT 0,
            {p}failures integer NOT NULL DEFAULT 0,
            {p}settled_epoch bigint NOT NULL DEFAULT 0,
            {p}lease_outstanding integer GENERATED ALWAYS AS (CASE WHEN {p}settled_epoch < {p}lease_epoch THEN 1 END) STORED,
            {p}settle_failures integer NOT NULL DEFAULT 0,
            {p}settle_retry_at timestamptz,
            {p}cancel_requested_at timestamptz,
            {p}execution_id uuid,
            {p}ended_completion_id uuid,
            {p}ended_digest bytea
            """));
        var indexes = string.Join("\n", prefixes.Select(p => $"""
            CREATE INDEX ON {Schema}.{name} ({p}due_at) WHERE {p}lease_expires_at IS NULL;
            CREATE INDEX ON {Schema}.{name} ({p}lease_expires_at) WHERE {p}lease_expires_at IS NOT NULL;
            CREATE INDEX ON {Schema}.{name} ({p}lease_outstanding, {p}lease_expires_at) WHERE {p}lease_expires_at IS NOT NULL;
            CREATE INDEX ON {Schema}.{name} ({p}lease_outstanding, tenant, {p}lease_owner) WHERE {p}lease_expires_at IS NOT NULL;
            CREATE INDEX ON {Schema}.{name} ({p}lease_owner) WHERE {p}lease_expires_at IS NOT NULL;
            """));
        await using var command = dataSource.CreateCommand($"""
            CREATE TABLE {Schema}.{name} (
                tenant text NOT NULL,
                id bigint NOT NULL,
                progress text,
                {columns},
                PRIMARY KEY (tenant, id)
            );
            {indexes}
            CREATE TABLE {Schema}.{name}_journal (n bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, entry text NOT NULL);
            """);
        await command.ExecuteNonQueryAsync(cancellationToken);
        return StoreLocation.Table(Schema, name);
    }

    public async Task<StoreLocation> CreateReconcileTableAsync(CancellationToken cancellationToken)
    {
        var table = await CreateTableAsync([""], cancellationToken);
        await using var command = dataSource.CreateCommand($"""
            ALTER TABLE {Name(table)}
                ADD COLUMN requested_revision bigint NOT NULL DEFAULT 0,
                ADD COLUMN claimed_revision bigint,
                ADD COLUMN converged_revision bigint NOT NULL DEFAULT 0,
                ADD COLUMN first_requested_at timestamptz,
                ADD COLUMN not_before timestamptz,
                ADD COLUMN attempted_generation bigint NOT NULL DEFAULT 0,
                ADD COLUMN converged_generation bigint NOT NULL DEFAULT 0;
            CREATE INDEX ON {Name(table)} (attempted_generation);
            """);
        await command.ExecuteNonQueryAsync(cancellationToken);
        return table;
    }

    public async Task InsertReconcileAsync(StoreLocation table, TestKey key, DueState due, long revision, CancellationToken cancellationToken)
    {
        var dueAt = due switch
        {
            DueState.None => "NULL",
            _ => "clock_timestamp()",
        };
        await using var command = dataSource.CreateCommand(
            $"INSERT INTO {Name(table)} (tenant, id, due_at, requested_revision, claimed_revision, converged_revision) VALUES ($1, $2, {dueAt}, $3, $3, $3)");
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = key.Tenant });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = key.Id });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = revision });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<ReconcileState> ReadReconcileAsync(StoreLocation table, TestKey key, CancellationToken cancellationToken)
    {
        var lease = await ReadAsync(table, key, "", cancellationToken);
        await using var command = dataSource.CreateCommand($"""
            SELECT requested_revision, claimed_revision, converged_revision, first_requested_at IS NOT NULL,
                   CASE WHEN due_at IS NULL THEN NULL ELSE extract(epoch FROM due_at - clock_timestamp())::float8 END,
                   extract(epoch FROM not_before - clock_timestamp())::float8,
                   attempted_generation, converged_generation
            FROM {Name(table)} WHERE tenant = $1 AND id = $2
            """);
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = key.Tenant });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = key.Id });
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
            reader.IsDBNull(4) ? null : TimeSpan.FromSeconds(reader.GetDouble(4)),
            reader.IsDBNull(5) ? null : TimeSpan.FromSeconds(reader.GetDouble(5)),
            reader.GetInt64(6),
            reader.GetInt64(7));
    }

    public async Task<string> RecordVersionsAsync(StoreLocation table, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand($"SELECT coalesce(string_agg(xmin::text || ':' || ctid::text, ',' ORDER BY tenant, id), '') FROM {Name(table)}");
        return (string)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    public async Task InsertAsync(StoreLocation table, TestKey key, TimeSpan? dueIn, CancellationToken cancellationToken)
    {
        var prefixes = await PrefixesAsync(table, cancellationToken);
        var dueColumns = string.Join(", ", prefixes.Select(p => p + "due_at"));
        var dueValues = string.Join(", ", prefixes.Select(_ => "CASE WHEN $3::interval IS NULL THEN NULL ELSE clock_timestamp() + $3 END"));
        await using var command = dataSource.CreateCommand($"INSERT INTO {Name(table)} (tenant, id, {dueColumns}) VALUES ($1, $2, {dueValues})");
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = key.Tenant });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = key.Id });
        command.Parameters.Add(new NpgsqlParameter { Value = dueIn is { } d ? d : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Interval });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<RecordState> ReadAsync(StoreLocation table, TestKey key, string prefix, CancellationToken cancellationToken)
    {
        var p = prefix;
        await using var command = dataSource.CreateCommand($"""
            SELECT CASE WHEN {p}due_at IS NULL THEN 0 WHEN {p}due_at <= clock_timestamp() THEN 1 ELSE 2 END,
                   {p}lease_owner, {p}lease_epoch, {p}lease_expires_at IS NOT NULL, coalesce({p}lease_expires_at > clock_timestamp(), false),
                   {p}takeovers, {p}failures, {p}settled_epoch, {p}settle_failures, {p}cancel_requested_at IS NOT NULL, {p}execution_id, progress
            FROM {Name(table)} WHERE tenant = $1 AND id = $2
            """);
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = key.Tenant });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = key.Id });
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
        var connection = await runtime.OpenConnectionAsync(cancellationToken);
        return new Transaction(connection, await connection.BeginTransactionAsync(cancellationToken));
    }

    public async Task JournalAsync(WorkTransaction transaction, StoreLocation table, string entry, CancellationToken cancellationToken)
    {
        var npgsql = transaction.Npgsql();
        await using var command = new NpgsqlCommand($"INSERT INTO {Name(table)}_journal (entry) VALUES ($1)", npgsql.Connection, npgsql);
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = entry });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task LockWithoutWaitingAsync(WorkTransaction transaction, StoreLocation table, TestKey key, CancellationToken cancellationToken)
    {
        var npgsql = transaction.Npgsql();
        await using var command = new NpgsqlCommand($"SELECT 1 FROM {Name(table)} WHERE tenant = $1 AND id = $2 FOR UPDATE NOWAIT", npgsql.Connection, npgsql);
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = key.Tenant });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = key.Id });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task InsertInTransactionAsync(WorkTransaction transaction, StoreLocation table, TestKey key, CancellationToken cancellationToken)
    {
        var npgsql = transaction.Npgsql();
        await using var command = new NpgsqlCommand($"INSERT INTO {Name(table)} (tenant, id) VALUES ($1, $2)", npgsql.Connection, npgsql);
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = key.Tenant });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = key.Id });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<string>> ReadJournalAsync(StoreLocation table, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand($"SELECT entry FROM {Name(table)}_journal ORDER BY n");
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
        await using var command = dataSource.CreateCommand(
            "SELECT left(column_name, length(column_name) - length('due_at')) FROM information_schema.columns "
            + "WHERE table_schema = $1 AND table_name = $2 AND column_name LIKE '%due_at' ORDER BY ordinal_position");
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = table.Schema });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = table.Name });
        var prefixes = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            prefixes.Add(reader.GetString(0));
        }
        return [.. prefixes];
    }

    public async Task<JobState> ReadJobAsync(JobId job, CancellationToken cancellationToken) =>
        (await ReadJobsAsync("id = $1", new NpgsqlParameter<Guid> { TypedValue = job.Value }, cancellationToken)).SingleOrDefault()
        ?? throw new InvalidOperationException($"No job {job}.");

    public Task<IReadOnlyList<JobState>> ReadJobsAsync(string definition, CancellationToken cancellationToken) =>
        ReadJobsAsync("definition = $1", new NpgsqlParameter<string> { TypedValue = definition }, cancellationToken);

    private async Task<IReadOnlyList<JobState>> ReadJobsAsync(string filter, NpgsqlParameter parameter, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand($"""
            SELECT id, definition, NULLIF(state, 0)::int4, outcome::int4, reason, convert_from(args, 'UTF8'), args_version, started, attempts,
                   convert_from(pending_args, 'UTF8'),
                   CASE WHEN due_at IS NULL THEN NULL ELSE extract(epoch FROM due_at - clock_timestamp())::float8 END,
                   extract(epoch FROM pending_due - clock_timestamp())::float8,
                   coalesce(lease_expires_at > clock_timestamp(), false), cancel_requested_at IS NOT NULL, execution_id, partition_key,
                   occurrence, unconsumed, skip_pending, paused, ended_at, merge_key IS NOT NULL, calendar_unavailable, waited
            FROM work.jobs WHERE {filter} ORDER BY created_at, id
            """);
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
                ArgsValue(reader.GetString(5)),
                reader.GetInt32(6),
                reader.GetBoolean(7),
                reader.GetInt32(8),
                reader.IsDBNull(9) ? null : ArgsValue(reader.GetString(9)),
                reader.IsDBNull(10) ? null : TimeSpan.FromSeconds(reader.GetDouble(10)),
                reader.IsDBNull(11) ? null : TimeSpan.FromSeconds(reader.GetDouble(11)),
                reader.GetBoolean(12),
                reader.GetBoolean(13),
                reader.GetGuid(14),
                reader.GetString(15),
                reader.IsDBNull(16) ? null : reader.GetString(16),
                reader.GetBoolean(17),
                reader.GetBoolean(18),
                reader.GetBoolean(19),
                reader.IsDBNull(20) ? null : reader.GetFieldValue<DateTimeOffset>(20),
                reader.GetBoolean(21),
                reader.GetBoolean(22),
                reader.GetBoolean(23)));
        }
        return jobs;
    }

    public async Task<bool> ReadRecurrencePausedAsync(string definition, Subject subject, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("SELECT paused FROM work.job_recurrences WHERE definition = $1 AND subject_kind = $2 AND subject_id = $3");
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = definition });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = subject.Kind });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = subject.Id });
        return await command.ExecuteScalarAsync(cancellationToken) as bool? ?? throw new InvalidOperationException($"No recurrence of {definition} for {subject}.");
    }

    public async Task LockSubjectAsync(WorkTransaction transaction, string definition, Subject subject, CancellationToken cancellationToken)
    {
        var npgsql = transaction.Npgsql();
        foreach (var table in new[] { "work.job_recurrences", "work.jobs" })
        {
            await using var command = new NpgsqlCommand($"SELECT 1 FROM {table} WHERE definition = $1 AND subject_kind = $2 AND subject_id = $3 FOR UPDATE", npgsql.Connection, npgsql);
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = definition });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = subject.Kind });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = subject.Id });
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    public async Task<DateTimeOffset> NowAsync(CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("SELECT clock_timestamp()");
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return reader.GetFieldValue<DateTimeOffset>(0);
    }

    public async Task<long> CommittedTransactionsAsync(CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()");
        return (long)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    public async Task<ProcessRecord?> ReadProcessAsync(OwnerId owner, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("""
            SELECT p.type, p.release, extract(epoch FROM clock_timestamp() - p.heartbeat_at)::float8, p.stopped_at IS NOT NULL,
                   d.definition, d.revision, d.runs, d.accepted_args, d.declares_recurrence
            FROM work.processes AS p LEFT JOIN work.process_definitions AS d ON d.owner = p.owner
            WHERE p.owner = $1 ORDER BY d.definition
            """);
        command.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = owner.Value });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        ProcessRecord? state = null;
        var definitions = new List<ProcessDefinition>();
        while (await reader.ReadAsync(cancellationToken))
        {
            state ??= new ProcessRecord(reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1), TimeSpan.FromSeconds(reader.GetDouble(2)), reader.GetBoolean(3), definitions);
            if (!reader.IsDBNull(4))
            {
                definitions.Add(new ProcessDefinition(reader.GetString(4), reader.GetInt64(5), reader.GetBoolean(6), reader.GetFieldValue<int[]>(7), reader.GetBoolean(8)));
            }
        }
        return state;
    }

    public async Task AgeProcessAsync(OwnerId owner, TimeSpan by, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(
            "UPDATE work.processes SET started_at = started_at - $2, heartbeat_at = heartbeat_at - $2, stopped_at = stopped_at - $2 WHERE owner = $1");
        command.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = owner.Value });
        command.Parameters.Add(new NpgsqlParameter<TimeSpan> { TypedValue = by, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Interval });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task DeleteProcessAsync(OwnerId owner, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand("DELETE FROM work.processes WHERE owner = $1");
        command.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = owner.Value });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>The value of conformance arguments, stored as <c>{"Value":"..."}</c>; empty for other jobs, such as retention.</summary>
    private static string ArgsValue(string json) =>
        System.Text.Json.JsonDocument.Parse(json).RootElement.TryGetProperty("Value", out var value) ? value.GetString()! : "";

    private static string Name(StoreLocation table) => $"{table.Schema}.{table.Name}";

    private sealed class Transaction(NpgsqlConnection connection, NpgsqlTransaction transaction) : IConformanceTransaction
    {
        public WorkTransaction Work { get; } = transaction;

        public async Task WriteProgressAsync(StoreLocation table, TestKey key, string progress, CancellationToken cancellationToken)
        {
            await using var command = Command($"UPDATE {Name(table)} SET progress = $3 WHERE tenant = $1 AND id = $2", key);
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = progress });
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        public async Task<string?> ReadProgressAsync(StoreLocation table, TestKey key, CancellationToken cancellationToken)
        {
            await using var command = Command($"SELECT progress FROM {Name(table)} WHERE tenant = $1 AND id = $2", key);
            return await command.ExecuteScalarAsync(cancellationToken) as string;
        }

        public async Task LockAsync(StoreLocation table, TestKey key, CancellationToken cancellationToken)
        {
            await using var command = Command($"SELECT 1 FROM {Name(table)} WHERE tenant = $1 AND id = $2 FOR UPDATE", key);
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

        private NpgsqlCommand Command(string sql, TestKey key)
        {
            var command = new NpgsqlCommand(sql, connection, transaction);
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = key.Tenant });
            command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = key.Id });
            return command;
        }
    }
}

public sealed class PostgreSqlSignalScenarios(PostgreSqlConformanceProvider provider)
    : SignalScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlLeaseClaimScenarios(PostgreSqlConformanceProvider provider)
    : LeaseClaimScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlLeaseHolderScenarios(PostgreSqlConformanceProvider provider)
    : LeaseHolderScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlLeaseTakeoverScenarios(PostgreSqlConformanceProvider provider)
    : LeaseTakeoverScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlLeaseLimitScenarios(PostgreSqlConformanceProvider provider)
    : LeaseLimitScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlRemoteHolderScenarios(PostgreSqlConformanceProvider provider)
    : RemoteHolderScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlReconcileRequestScenarios(PostgreSqlConformanceProvider provider)
    : ReconcileRequestScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlReconcileResultScenarios(PostgreSqlConformanceProvider provider)
    : ReconcileResultScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlReconcileInvalidationScenarios(PostgreSqlConformanceProvider provider)
    : ReconcileInvalidationScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlReconcileSupersessionScenarios(PostgreSqlConformanceProvider provider)
    : ReconcileSupersessionScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlReconcilePoolScenarios(PostgreSqlConformanceProvider provider)
    : ReconcilePoolScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlJobRequestScenarios(PostgreSqlConformanceProvider provider)
    : JobRequestScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlJobAttemptScenarios(PostgreSqlConformanceProvider provider)
    : JobAttemptScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlJobDefinitionScenarios(PostgreSqlConformanceProvider provider)
    : JobDefinitionScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlJobOutcomeScenarios(PostgreSqlConformanceProvider provider)
    : JobOutcomeScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlJobRecurrenceScenarios(PostgreSqlConformanceProvider provider)
    : JobRecurrenceScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlJobLifecycleScenarios(PostgreSqlConformanceProvider provider)
    : JobLifecycleScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlJobOverlapScenarios(PostgreSqlConformanceProvider provider)
    : JobOverlapScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlJobPauseScenarios(PostgreSqlConformanceProvider provider)
    : JobPauseScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlJobResultScenarios(PostgreSqlConformanceProvider provider)
    : JobResultScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlJobStatusScenarios(PostgreSqlConformanceProvider provider)
    : JobStatusScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlJobRunHistoryScenarios(PostgreSqlConformanceProvider provider)
    : JobRunHistoryScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlJobActionScenarios(PostgreSqlConformanceProvider provider)
    : JobActionScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlJobRetentionScenarios(PostgreSqlConformanceProvider provider)
    : JobRetentionScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlProcessRegistryScenarios(PostgreSqlConformanceProvider provider)
    : ProcessRegistryScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlJobCoverageScenarios(PostgreSqlConformanceProvider provider)
    : JobCoverageScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlJobCalendarScenarios(PostgreSqlConformanceProvider provider)
    : JobCalendarScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlJobChainScenarios(PostgreSqlConformanceProvider provider)
    : JobChainScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlJobMonitoringScenarios(PostgreSqlConformanceProvider provider)
    : JobMonitoringScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlJobTelemetryScenarios(PostgreSqlConformanceProvider provider)
    : JobTelemetryScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlHostStartScenarios(PostgreSqlConformanceProvider provider)
    : HostStartScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlManySourcesScenarios(PostgreSqlConformanceProvider provider)
    : ManySourcesScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlProcessRoleScenarios(PostgreSqlConformanceProvider provider)
    : ProcessRoleScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlApplicationIsolationScenarios(PostgreSqlConformanceProvider provider)
    : ApplicationIsolationScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;

public sealed class PostgreSqlControlledClockScenarios(PostgreSqlConformanceProvider provider)
    : ControlledClockScenarios(provider), IClassFixture<PostgreSqlConformanceProvider>;
