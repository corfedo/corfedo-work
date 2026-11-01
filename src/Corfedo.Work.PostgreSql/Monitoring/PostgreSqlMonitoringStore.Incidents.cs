using Npgsql;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Monitoring;

internal sealed partial class PostgreSqlMonitoringStore
{
    /// <summary>
    /// Reads the definition's recurring-chain occurrences that started more than the limit ago from the
    /// <c>jobs_owned_chain</c> range bounded by one clock reading, locked in shared mode without waiting, so a
    /// completion committing meanwhile waits and then resolves the incident; a row an ending holds is left to the next
    /// check. Opens one incident per key naming its earliest occurrence. A second statement, without locks, finds the
    /// earliest start among occurrences still within the limit from the rest of the range.
    /// </summary>
    public async ValueTask<RunningCheck> CheckRunningAsync(string definition, TimeSpan limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        List<WorkIncident> opened;
        DateTimeOffset? next;
        await using (var batch = new NpgsqlBatch(connection, transaction))
        {
            batch.BatchCommands.Add(Command(
                $"WITH over AS (SELECT DISTINCT ON (o.subject_kind, o.subject_id) o.* FROM (SELECT r.id, r.subject_kind, r.subject_id, r.attempt_started_at FROM {jobs} AS r "
                + "WHERE r.definition = $1 AND r.owned_chain IS NOT NULL AND r.state = 0 AND r.attempt_started_at <= (SELECT clock_timestamp()) - $2 FOR SHARE OF r SKIP LOCKED) AS o "
                + "ORDER BY o.subject_kind, o.subject_id, o.attempt_started_at) "
                + $"INSERT INTO {incidents} AS i (id, kind, definition, subject_kind, subject_id, job, opened_at, due_at) "
                + $"SELECT gen_random_uuid(), {(int)WorkIncidentKind.RunsAtMost}, $1, o.subject_kind, o.subject_id, o.id, clock_timestamp(), clock_timestamp() FROM over AS o "
                + "ON CONFLICT (definition, subject_kind, subject_id, kind, date_set) WHERE resolved_at IS NULL DO NOTHING "
                + $"RETURNING {IncidentColumns}",
                Text(definition),
                Interval(limit)));
            batch.BatchCommands.Add(Command(
                $"SELECT min(r.attempt_started_at) + $2 FROM {jobs} AS r WHERE r.definition = $1 AND r.owned_chain IS NOT NULL AND r.state = 0 "
                + "AND r.attempt_started_at > (SELECT clock_timestamp()) - $2",
                Text(definition),
                Interval(limit)));
            await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
            opened = await ReadIncidentsAsync(reader, cancellationToken);
            await reader.NextResultAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            next = reader.IsDBNull(0) ? null : reader.GetFieldValue<DateTimeOffset>(0);
        }
        if (opened.Count > 0)
        {
            await NotifyAsync(connection, transaction, cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        WorkTelemetry.Count(new IncidentChanges(opened, []));
        return new RunningCheck(opened, next);
    }

    /// <summary>
    /// One statement locks the job in shared mode while the lease is valid and its attempt open, reads the stored running
    /// limit with it and, once the attempt reached it, opens the incident; a renewal or an ending holding the job is
    /// waited for and the job read again. The remaining time is computed with the store's clock.
    /// </summary>
    public async ValueTask<TimeSpan?> CheckAttemptAsync(Lease<JobId> lease, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        var sql = leases.Sql(lease.Source);
        var k = sql.KeyCount;
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        TimeSpan? remaining = null;
        List<WorkIncident> opened = [];
        await using (var batch = new NpgsqlBatch(connection, transaction))
        {
            batch.BatchCommands.Add(KeyCommand(
                $"WITH a AS (SELECT r.id, r.definition, r.subject_kind, r.subject_id, r.attempt_started_at + d.expect_running - clock_timestamp() AS remaining "
                + $"FROM {sql.Table} AS r JOIN {definitions} AS d ON d.key = r.definition "
                + $"WHERE {sql.KeyMatch(1)} AND {sql.Valid(k + 1, k + 2)} AND r.attempt_started_at IS NOT NULL AND d.expect_running IS NOT NULL FOR SHARE OF r), "
                + $"opened AS (INSERT INTO {incidents} AS i (id, kind, definition, subject_kind, subject_id, job, opened_at, due_at) "
                + $"SELECT gen_random_uuid(), {(int)WorkIncidentKind.RunsAtMost}, a.definition, a.subject_kind, a.subject_id, a.id, clock_timestamp(), clock_timestamp() "
                + "FROM a WHERE a.remaining <= interval '0' "
                + "ON CONFLICT (definition, subject_kind, subject_id, kind, date_set) WHERE resolved_at IS NULL DO NOTHING "
                + $"RETURNING {IncidentColumns}) "
                + $"SELECT a.remaining, {IncidentColumns} FROM a LEFT JOIN opened AS i ON true",
                lease.Source,
                lease.Key,
                Uuid(lease.Owner.Value),
                Int64(lease.Epoch)));
            await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                var left = reader.GetFieldValue<TimeSpan>(0);
                if (left > TimeSpan.Zero)
                {
                    remaining = left;
                }
                else if (!reader.IsDBNull(1))
                {
                    opened.Add(ReadIncident(reader, 1));
                }
            }
        }
        if (opened.Count > 0)
        {
            await NotifyAsync(connection, transaction, cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        WorkTelemetry.Count(new IncidentChanges(opened, []));
        return remaining;
    }

    /// <summary>Resolves the open incidents of the removed kinds; resets failures of rows it can lock without waiting.</summary>
    public async ValueTask<IncidentChanges> ResolveRemovedAsync(string definition, IReadOnlyCollection<WorkIncidentKind> kinds, bool resetFailures, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(kinds);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        List<WorkIncident> resolved;
        await using (var batch = new NpgsqlBatch(connection, transaction))
        {
            batch.BatchCommands.Add(Command(
                $"WITH open AS (SELECT i.id FROM {incidents} AS i WHERE i.definition = $1 AND i.resolved_at IS NULL AND i.kind = ANY($2) FOR UPDATE SKIP LOCKED) "
                + $"UPDATE {incidents} AS i SET resolved_at = clock_timestamp(), due_at = coalesce(i.due_at, clock_timestamp()) FROM open WHERE i.id = open.id "
                + $"RETURNING {IncidentColumns}",
                Text(definition),
                new NpgsqlParameter<short[]> { TypedValue = [.. kinds.Select(k => (short)k)] }));
            if (resetFailures)
            {
                batch.BatchCommands.Add(Command(
                    $"WITH failed AS (SELECT s.ctid AS t FROM {states} AS s WHERE s.definition = $1 AND s.failures > 0 FOR UPDATE SKIP LOCKED) "
                    + $"UPDATE {states} AS s SET failures = 0 FROM failed WHERE s.ctid = failed.t",
                    Text(definition)));
            }
            await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
            resolved = await ReadIncidentsAsync(reader, cancellationToken);
        }
        if (resolved.Count > 0)
        {
            await NotifyAsync(connection, transaction, cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        var changes = resolved.Count == 0 ? IncidentChanges.None : new IncidentChanges([], resolved);
        WorkTelemetry.Count(changes);
        return changes;
    }

    /// <summary>
    /// One statement opens the wanted date-set incidents that are not open and resolves the open ones no longer wanted,
    /// including those of sets no longer in the report.
    /// </summary>
    public async ValueTask<IncidentChanges> SyncDateSetsAsync(IReadOnlyCollection<string> runningOut, IReadOnlyCollection<string> waiting, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(runningOut);
        ArgumentNullException.ThrowIfNull(waiting);
        const int RunningOut = (int)WorkIncidentKind.DateSetRunningOut;
        const int Waiting = (int)WorkIncidentKind.DateSetWorkWaiting;
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        var opened = new List<WorkIncident>();
        var resolved = new List<WorkIncident>();
        await using (var command = new NpgsqlCommand(
            $"WITH want AS (SELECT {RunningOut}::int2 AS kind, s AS date_set FROM unnest($1::text[]) AS s UNION ALL SELECT {Waiting}::int2, s FROM unnest($2::text[]) AS s), "
            + $"resolved AS (UPDATE {incidents} AS i SET resolved_at = clock_timestamp(), due_at = coalesce(i.due_at, clock_timestamp()) "
            + $"WHERE i.definition IS NULL AND i.subject_kind IS NULL AND i.kind IN ({RunningOut}, {Waiting}) AND i.resolved_at IS NULL "
            + $"AND NOT EXISTS (SELECT 1 FROM want AS w WHERE w.kind = i.kind AND w.date_set = i.date_set) RETURNING {IncidentColumns}), "
            + $"opened AS (INSERT INTO {incidents} AS i (id, kind, date_set, opened_at, due_at) "
            + "SELECT gen_random_uuid(), w.kind, w.date_set, clock_timestamp(), clock_timestamp() FROM want AS w "
            + $"ON CONFLICT (definition, subject_kind, subject_id, kind, date_set) WHERE resolved_at IS NULL DO NOTHING RETURNING {IncidentColumns}) "
            + "SELECT true, * FROM opened UNION ALL SELECT false, * FROM resolved",
            connection,
            transaction))
        {
            command.Parameters.Add(Texts(runningOut));
            command.Parameters.Add(Texts(waiting));
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                (reader.GetBoolean(0) ? opened : resolved).Add(ReadIncident(reader, 1));
            }
        }
        var changes = new IncidentChanges(opened, resolved);
        if (changes.Any)
        {
            await NotifyAsync(connection, transaction, cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        WorkTelemetry.Count(changes);
        return changes;
    }

    /// <summary>
    /// Deletes resolved incidents by their resolution time, expectation rows without a deadline or failures whose key
    /// has no open incident, and queued key changes of definitions that expect no completions, which only the
    /// monitoring loop deletes otherwise; each statement locks its rows without waiting.
    /// </summary>
    public async ValueTask<int> RetainAsync(TimeSpan retention, int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        await using var batch = dataSource.CreateBatch();
        batch.BatchCommands.Add(Command(
            $"DELETE FROM {incidents} AS i USING (SELECT r.id FROM {incidents} AS r WHERE r.resolved_at < clock_timestamp() - $1 "
            + "ORDER BY r.resolved_at LIMIT $2 FOR UPDATE SKIP LOCKED) AS v WHERE i.id = v.id",
            Interval(retention),
            Int32(limit)));
        batch.BatchCommands.Add(Command(
            $"DELETE FROM {states} AS s USING (SELECT e.ctid AS t FROM {states} AS e WHERE e.deadline_at IS NULL AND e.failures = 0 "
            + $"AND NOT EXISTS (SELECT 1 FROM {incidents} AS i WHERE i.definition = e.definition AND i.resolved_at IS NULL "
            + "AND i.subject_kind IS NOT DISTINCT FROM e.subject_kind AND i.subject_id IS NOT DISTINCT FROM e.subject_id) "
            + "LIMIT $1 FOR UPDATE SKIP LOCKED) AS v WHERE s.ctid = v.t",
            Int32(limit)));
        batch.BatchCommands.Add(Command(
            $"DELETE FROM {keyChanges} AS c WHERE c.ctid = ANY (ARRAY (SELECT q.ctid FROM {keyChanges} AS q JOIN {definitions} AS d ON d.key = q.definition "
            + "WHERE d.expect_time IS NULL LIMIT $1 FOR UPDATE OF q SKIP LOCKED))",
            Int32(limit)));
        await batch.ExecuteNonQueryAsync(cancellationToken);
        return batch.BatchCommands.Max(static (NpgsqlBatchCommand c) => c.RecordsAffected);
    }
}
