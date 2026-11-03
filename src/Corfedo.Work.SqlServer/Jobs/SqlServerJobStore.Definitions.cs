using System.Data;
using Corfedo.Work.SqlServer.Leases;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Jobs;

internal sealed partial class SqlServerJobStore
{
    private const string DefinitionColumns =
        "[key], revision, digest, subject_kind, attempts, backoff_kind, backoff_initial, backoff_maximum, partition_limit, "
        + "definition_limit, priority, attempt_timeout, retry_timeouts, late_grace, minimum_args_version, paused, run_history, job_retention, run_history_retention, "
        + "expect_time, expect_zone, expect_calendar, expect_running, expect_failures";

    /// <summary>
    /// Takes the definition fence exclusively, so claims in flight finish first and claims that start meanwhile queue
    /// behind it, locks the stored row, then stores the definition if its revision is newer than the stored one, with
    /// the host's retention durations where its policy leaves them unset. The pause state is kept.
    /// </summary>
    public async ValueTask<StoredJobDefinition> StoreDefinitionAsync(JobDefinition definition, JobRetention retention, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(retention);
        return await InTransactionAsync(async work =>
        {
            StoredJobDefinition? stored;
            await using (var read = Tsql.Command(work,
                LeaseSql.AppLock("@fence", "Exclusive")
                + $"SELECT {DefinitionColumns} FROM {definitions} WITH (UPDLOCK, ROWLOCK) WHERE [key] = @key;",
                Tsql.Text("@fence", LeaseSql.DefinitionLockName(schema, definition.Key), 255),
                Tsql.Text("@key", definition.Key, 200)))
            await using (var reader = await read.ReaderAsync(cancellationToken))
            {
                stored = await reader.NextRowAsync(cancellationToken) ? ReadDefinition(reader) : null;
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
            var policy = definition.Policy with
            {
                Retention = definition.Policy.Retention ?? retention.Jobs,
                RunHistoryRetention = definition.Policy.RunHistoryRetention ?? retention.RunHistory,
            };
            var write = stored is null
                ? $"INSERT INTO {definitions} ({DefinitionColumns}) VALUES (@key, @revision, @digest, @subject, @attempts, @backoff_kind, @backoff_initial, @backoff_maximum, "
                    + "@partition_limit, @definition_limit, @priority, @timeout, @retry_timeouts, @late_grace, @minimum_args, 0, @run_history, @job_retention, @run_history_retention, "
                    + "@expect_time, @expect_zone, @expect_calendar, @expect_running, @expect_failures);"
                : $"UPDATE {definitions} SET revision = @revision, digest = @digest, subject_kind = @subject, attempts = @attempts, backoff_kind = @backoff_kind, "
                    + "backoff_initial = @backoff_initial, backoff_maximum = @backoff_maximum, partition_limit = @partition_limit, definition_limit = @definition_limit, "
                    + "priority = @priority, attempt_timeout = @timeout, retry_timeouts = @retry_timeouts, late_grace = @late_grace, minimum_args_version = @minimum_args, "
                    + "run_history = @run_history, job_retention = @job_retention, run_history_retention = @run_history_retention, expect_time = @expect_time, "
                    + "expect_zone = @expect_zone, expect_calendar = @expect_calendar, expect_running = @expect_running, expect_failures = @expect_failures, "
                    + "stored_at = SYSUTCDATETIME() WHERE [key] = @key;";
            var deadline = policy.Expect?.Deadline;
            await using (var command = Tsql.Command(work, write,
                Tsql.Text("@key", definition.Key, 200),
                Tsql.Int64("@revision", definition.Revision),
                Tsql.Bytes("@digest", definition.Digest),
                Tsql.Text("@subject", definition.Subject, 100),
                Tsql.Int32("@attempts", policy.Attempts),
                Tsql.Int16("@backoff_kind", (short)policy.Backoff.Kind),
                Tsql.Ticks("@backoff_initial", policy.Backoff.Initial),
                Tsql.Ticks("@backoff_maximum", policy.Backoff.Maximum),
                Tsql.Int32("@partition_limit", policy.PartitionLimit),
                Tsql.Int32("@definition_limit", policy.DefinitionLimit),
                Tsql.Int16("@priority", (short)policy.Priority),
                Tsql.Ticks("@timeout", policy.Timeout),
                Tsql.Bool("@retry_timeouts", policy.RetryTimeouts),
                Tsql.Ticks("@late_grace", policy.Late.Grace),
                Tsql.Int32("@minimum_args", policy.MinimumArgsVersion),
                Tsql.Bool("@run_history", policy.RunHistory),
                Tsql.Ticks("@job_retention", policy.Retention.Value),
                Tsql.Ticks("@run_history_retention", policy.RunHistoryRetention.Value),
                new SqlParameter("@expect_time", SqlDbType.Time) { Scale = 7, Value = deadline is { } d ? d.Time.ToTimeSpan() : DBNull.Value },
                Tsql.Text("@expect_zone", deadline?.TimeZone, 100),
                Tsql.Text("@expect_calendar", deadline?.Calendar, 128),
                Tsql.Ticks("@expect_running", policy.Expect?.RunningLimit),
                Tsql.Int32("@expect_failures", policy.Expect?.FailureLimit)))
            {
                await command.NonQueryAsync(cancellationToken);
            }
            return new StoredJobDefinition(definition.Key, definition.Revision, definition.Subject, policy, definition.Digest, stored?.Paused ?? false);
        }, cancellationToken);
    }

    public async ValueTask<StoredJobDefinition?> GetDefinitionAsync(string key, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(key);
        await using var connection = await leases.OpenAsync(cancellationToken);
        await using var command = Tsql.Command(connection, null, $"SELECT {DefinitionColumns} FROM {definitions} WHERE [key] = @key;", Tsql.Text("@key", key, 200));
        await using var reader = await command.ReaderAsync(cancellationToken);
        return await reader.NextRowAsync(cancellationToken) ? ReadDefinition(reader) : null;
    }

    private static StoredJobDefinition ReadDefinition(SqlDataReader reader)
    {
        var initial = TimeSpan.FromTicks(reader.GetInt64(6));
        var policy = new JobPolicy
        {
            Attempts = reader.GetInt32(4),
            Backoff = (BackoffKind)reader.GetInt16(5) switch
            {
                BackoffKind.None => Backoff.None,
                BackoffKind.Constant => Backoff.Constant(initial),
                _ => Backoff.Exponential(initial, TimeSpan.FromTicks(reader.GetInt64(7))),
            },
            PartitionLimit = reader.IsDBNull(8) ? null : reader.GetInt32(8),
            DefinitionLimit = reader.IsDBNull(9) ? null : reader.GetInt32(9),
            Priority = (Priority)reader.GetInt16(10),
            Timeout = Ticks(reader, 11),
            RetryTimeouts = reader.GetBoolean(12),
            Late = reader.IsDBNull(13) ? Late.Run : Late.SkipAfter(TimeSpan.FromTicks(reader.GetInt64(13))),
            MinimumArgsVersion = reader.GetInt32(14),
            RunHistory = reader.GetBoolean(16),
            Retention = TimeSpan.FromTicks(reader.GetInt64(17)),
            RunHistoryRetention = TimeSpan.FromTicks(reader.GetInt64(18)),
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
    internal static JobExpectation? ReadExpectation(SqlDataReader reader, int first)
    {
        JobExpectation? expectation = null;
        if (!reader.IsDBNull(first))
        {
            expectation = Expect.CompletesBy(TimeOnly.FromTimeSpan(reader.GetTimeSpan(first)), reader.GetString(first + 1), reader.IsDBNull(first + 2) ? null : reader.GetString(first + 2));
        }
        if (Ticks(reader, first + 3) is { } limit)
        {
            expectation = expectation is null ? Expect.RunsAtMost(limit) : expectation.RunsAtMost(limit);
        }
        if (!reader.IsDBNull(first + 4))
        {
            var failures = reader.GetInt32(first + 4);
            expectation = expectation is null ? Expect.FailsAtMostInARow(failures) : expectation.FailsAtMostInARow(failures);
        }
        return expectation;
    }

    /// <summary>A duration stored as bigint ticks, or null.</summary>
    internal static TimeSpan? Ticks(SqlDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : TimeSpan.FromTicks(reader.GetInt64(ordinal));
}
