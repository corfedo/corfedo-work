using Corfedo.Work.PostgreSql.Leases;
using Npgsql;
using NpgsqlTypes;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Jobs;

internal sealed partial class PostgreSqlJobStore
{
    private const string DefinitionColumns =
        "key, revision, digest, subject_kind, attempts, backoff_kind, backoff_initial, backoff_maximum, partition_limit, "
        + "definition_limit, priority, attempt_timeout, retry_timeouts, late_grace, minimum_args_version, paused, run_history, job_retention, run_history_retention, "
        + "expect_time, expect_zone, expect_calendar, expect_running, expect_failures";

    /// <summary>
    /// Takes the definition fence exclusively, so claims in flight finish first and claims that start meanwhile queue
    /// behind it, then stores the definition if its revision is newer than the stored one, with the policy
    /// resolved with the host's defaults. The pause state is kept.
    /// </summary>
    public async ValueTask<StoredJobDefinition> StoreDefinitionAsync(JobDefinition definition, JobPolicy policy, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(policy.RunHistory, nameof(policy));
        ArgumentNullException.ThrowIfNull(policy.Retention, nameof(policy));
        ArgumentNullException.ThrowIfNull(policy.RunHistoryRetention, nameof(policy));
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        StoredJobDefinition? stored;
        await using (var batch = new NpgsqlBatch(connection, transaction))
        {
            batch.BatchCommands.Add(Command("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", Text(LeaseSql.DefinitionLockKey(schema, definition.Key))));
            batch.BatchCommands.Add(Command($"SELECT {DefinitionColumns} FROM {definitions} WHERE key = $1 FOR UPDATE", Text(definition.Key)));
            await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
            await reader.NextResultAsync(cancellationToken);
            stored = await reader.ReadAsync(cancellationToken) ? ReadDefinition(reader) : null;
        }
        if (stored is not null && stored.Revision > definition.Revision)
        {
            return stored;
        }
        if (stored is not null && stored.Revision == definition.Revision)
        {
            if (!stored.Digest.AsSpan().SequenceEqual(definition.Digest))
            {
                throw new JobDefinitionConflictException(
                    $"Job definition {definition.Key} revision {definition.Revision} is stored with different content; increase the revision when the shared policy changes.");
            }
            return stored;
        }
        var write = stored is null
            ? $"INSERT INTO {definitions} ({DefinitionColumns}) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, false, $16, $17, $18, $19, $20, $21, $22, $23)"
            : $"UPDATE {definitions} SET revision = $2, digest = $3, subject_kind = $4, attempts = $5, backoff_kind = $6, backoff_initial = $7, "
                + "backoff_maximum = $8, partition_limit = $9, definition_limit = $10, priority = $11, attempt_timeout = $12, retry_timeouts = $13, "
                + "late_grace = $14, minimum_args_version = $15, run_history = $16, job_retention = $17, run_history_retention = $18, "
                + "expect_time = $19, expect_zone = $20, expect_calendar = $21, expect_running = $22, expect_failures = $23, stored_at = clock_timestamp() WHERE key = $1";
        await using (var command = new NpgsqlCommand(write, connection, transaction))
        {
            command.Parameters.Add(Text(definition.Key));
            command.Parameters.Add(Int64(definition.Revision));
            command.Parameters.Add(new NpgsqlParameter<byte[]> { TypedValue = definition.Digest, NpgsqlDbType = NpgsqlDbType.Bytea });
            command.Parameters.Add(Nullable(definition.Subject, NpgsqlDbType.Text));
            command.Parameters.Add(Int32(policy.Attempts));
            command.Parameters.Add(new NpgsqlParameter<short> { TypedValue = (short)policy.Backoff.Kind });
            command.Parameters.Add(Interval(policy.Backoff.Initial));
            command.Parameters.Add(Interval(policy.Backoff.Maximum));
            command.Parameters.Add(Nullable(policy.PartitionLimit, NpgsqlDbType.Integer));
            command.Parameters.Add(Nullable(policy.DefinitionLimit, NpgsqlDbType.Integer));
            command.Parameters.Add(new NpgsqlParameter<short> { TypedValue = (short)policy.Priority });
            command.Parameters.Add(Nullable(policy.Timeout, NpgsqlDbType.Interval));
            command.Parameters.Add(Bool(policy.RetryTimeouts));
            command.Parameters.Add(Nullable(policy.Late.Grace, NpgsqlDbType.Interval));
            command.Parameters.Add(Int32(policy.MinimumArgsVersion));
            command.Parameters.Add(Bool(policy.RunHistory.Value));
            command.Parameters.Add(Interval(policy.Retention.Value));
            command.Parameters.Add(Interval(policy.RunHistoryRetention.Value));
            var deadline = policy.Expect?.Deadline;
            command.Parameters.Add(Nullable(deadline?.Time, NpgsqlDbType.Time));
            command.Parameters.Add(Nullable(deadline?.TimeZone, NpgsqlDbType.Text));
            command.Parameters.Add(Nullable(deadline?.Calendar, NpgsqlDbType.Text));
            command.Parameters.Add(Nullable(policy.Expect?.RunningLimit, NpgsqlDbType.Interval));
            command.Parameters.Add(Nullable(policy.Expect?.FailureLimit, NpgsqlDbType.Integer));
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        return new StoredJobDefinition(definition.Key, definition.Revision, definition.Subject, policy, definition.Digest, stored?.Paused ?? false);
    }

    public async ValueTask<StoredJobDefinition?> GetDefinitionAsync(string key, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(key);
        await using var command = dataSource.CreateCommand($"SELECT {DefinitionColumns} FROM {definitions} WHERE key = $1");
        command.Parameters.Add(Text(key));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadDefinition(reader) : null;
    }

    private static StoredJobDefinition ReadDefinition(NpgsqlDataReader reader)
    {
        var initial = reader.GetFieldValue<TimeSpan>(6);
        var policy = new JobPolicy
        {
            Attempts = reader.GetInt32(4),
            Backoff = (BackoffKind)reader.GetInt16(5) switch
            {
                BackoffKind.None => Backoff.None,
                BackoffKind.Constant => Backoff.Constant(initial),
                _ => Backoff.Exponential(initial, reader.GetFieldValue<TimeSpan>(7)),
            },
            PartitionLimit = reader.IsDBNull(8) ? null : reader.GetInt32(8),
            DefinitionLimit = reader.IsDBNull(9) ? null : reader.GetInt32(9),
            Priority = (Priority)reader.GetInt16(10),
            Timeout = reader.IsDBNull(11) ? null : reader.GetFieldValue<TimeSpan>(11),
            RetryTimeouts = reader.GetBoolean(12),
            Late = reader.IsDBNull(13) ? Late.Run : Late.SkipAfter(reader.GetFieldValue<TimeSpan>(13)),
            MinimumArgsVersion = reader.GetInt32(14),
            RunHistory = reader.GetBoolean(16),
            Retention = reader.GetFieldValue<TimeSpan>(17),
            RunHistoryRetention = reader.GetFieldValue<TimeSpan>(18),
            Expect = ReadExpectation(reader, 19),
        };
        return new StoredJobDefinition(
            reader.GetString(0),
            reader.GetInt64(1),
            reader.IsDBNull(3) ? null : reader.GetString(3),
            policy,
            reader.GetFieldValue<byte[]>(2),
            reader.GetBoolean(15));
    }

    /// <summary>
    /// Reads the expectations stored from <paramref name="first"/> on: deadline time, zone and calendar, running limit and
    /// failure limit; null when none is set.
    /// </summary>
    internal static JobExpectation? ReadExpectation(NpgsqlDataReader reader, int first)
    {
        JobExpectation? expectation = null;
        if (!reader.IsDBNull(first))
        {
            expectation = Expect.CompletesBy(reader.GetFieldValue<TimeOnly>(first), reader.GetString(first + 1), reader.IsDBNull(first + 2) ? null : reader.GetString(first + 2));
        }
        if (!reader.IsDBNull(first + 3))
        {
            var limit = reader.GetFieldValue<TimeSpan>(first + 3);
            expectation = expectation is null ? Expect.RunsAtMost(limit) : expectation.RunsAtMost(limit);
        }
        if (!reader.IsDBNull(first + 4))
        {
            var failures = reader.GetInt32(first + 4);
            expectation = expectation is null ? Expect.FailsAtMostInARow(failures) : expectation.FailsAtMostInARow(failures);
        }
        return expectation;
    }

    private static NpgsqlParameter Nullable(object? value, NpgsqlDbType type) => new() { Value = value ?? DBNull.Value, NpgsqlDbType = type };
}
