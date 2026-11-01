using Corfedo.Work.PostgreSql.Leases;
using Npgsql;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Jobs;

internal sealed partial class PostgreSqlJobStore
{
    /// <summary>
    /// Takes the definition fence exclusively, waiting for claims in flight, then stores the pause state that every
    /// later claim and planning pass reads after its fence. Resuming signals claimers and planners.
    /// </summary>
    public async ValueTask PauseDefinitionAsync(IWorkTransaction transaction, LeaseSource<JobId> source, bool paused, CancellationToken cancellationToken)
    {
        var work = PostgreSqlTransaction.From(transaction);
        var definition = Sql(source).Job!.Definition;
        await using (var batch = new NpgsqlBatch(work.Connection, work.Transaction))
        {
            batch.BatchCommands.Add(Command("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", Text(LeaseSql.DefinitionLockKey(schema, definition))));
            batch.BatchCommands.Add(Command($"UPDATE {definitions} SET paused = $2 WHERE key = $1", Text(definition), Bool(paused)));
            await batch.ExecuteNonQueryAsync(cancellationToken);
        }
        if (!paused)
        {
            await NotifyDueAsync(work, source, cancellationToken);
            await NotifyPlanAsync(work, source, cancellationToken);
        }
    }

    /// <summary>
    /// Takes the subject's pause lock exclusively, waiting for claims that hold it in shared mode, then stores or deletes
    /// the subject's pause, the authority claims and planning check after taking the lock, and copies it to the flags of
    /// the subject's recurrences and then its outstanding jobs, the lock order of recurrence operations, which keeps
    /// paused work out of the due indexes; when a recurrence's flag changes, the key is queued for the monitoring loop,
    /// which is signalled.
    /// Requests and schedules take no pause lock: a row stored concurrently may carry a stale flag, which claims and
    /// planning ignore in favour of the stored pause and maintenance repairs.
    /// </summary>
    public async ValueTask PauseSubjectAsync(IWorkTransaction transaction, LeaseSource<JobId> source, Subject subject, bool paused, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(subject);
        var work = PostgreSqlTransaction.From(transaction);
        var sql = Sql(source);
        var definition = sql.Job!.Definition;
        await using (var batch = new NpgsqlBatch(work.Connection, work.Transaction))
        {
            batch.BatchCommands.Add(Command(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                Text(LeaseSql.SubjectLockPrefix(schema, definition) + subject.Kind + "\n" + subject.Id)));
            batch.BatchCommands.Add(paused
                ? Command($"INSERT INTO {pauses} (definition, subject_kind, subject_id, paused_at) VALUES ($1, $2, $3, clock_timestamp()) ON CONFLICT DO NOTHING", Text(definition), Text(subject.Kind), Text(subject.Id))
                : Command($"DELETE FROM {pauses} WHERE definition = $1 AND subject_kind = $2 AND subject_id = $3", Text(definition), Text(subject.Kind), Text(subject.Id)));
            batch.BatchCommands.Add(Command(
                $"WITH flagged AS (UPDATE {recurrences} SET paused = $4 WHERE definition = $1 AND subject_kind = $2 AND subject_id = $3 AND paused <> $4 RETURNING 1), "
                + $"queued AS (INSERT INTO {changes} (definition, subject_kind, subject_id) SELECT $1, $2, $3 WHERE EXISTS (SELECT 1 FROM flagged) RETURNING definition) "
                + NotifyMonitor("queued", 5),
                [Text(definition), Text(subject.Kind), Text(subject.Id), Bool(paused), .. MonitorSignal(source)]));
            batch.BatchCommands.Add(Command(
                $"UPDATE {sql.Table} SET paused = $4 WHERE definition = $1 AND subject_kind = $2 AND subject_id = $3 AND state = 0 AND paused <> $4",
                Text(definition),
                Text(subject.Kind),
                Text(subject.Id),
                Bool(paused)));
            await batch.ExecuteNonQueryAsync(cancellationToken);
        }
        if (!paused)
        {
            await NotifyDueAsync(work, source, cancellationToken);
            await NotifyPlanAsync(work, source, cancellationToken);
        }
    }

    /// <summary>
    /// Sets the copied pause flags of recurrences and then outstanding jobs to the stored subject pauses, in both
    /// directions, locking each row without waiting, and queues the keys of repaired recurrences for the monitoring
    /// loop, which is signalled. A flag left set by a schedule or request that raced a resume is cleared here, so its recurrence is planned
    /// and its job claimed again without further requests. Clearing signals planners and claimers.
    /// </summary>
    public async ValueTask<int> RepairPausesAsync(LeaseSource<JobId> source, int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var sql = Sql(source);
        var definition = sql.Job!.Definition;
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        int[] repaired;
        await using (var batch = new NpgsqlBatch(connection, transaction))
        {
            // Clearing finds stale flags among every flagged row, usually none. Under the LIMIT the planner probes
            // job_pauses once per flagged row; the materialized anti join is planned for all of them and hashes instead,
            // five times faster at 100 000 flagged jobs.
            batch.BatchCommands.Add(Command(
                $"WITH flagged AS (UPDATE {recurrences} SET paused = false WHERE id IN (WITH stale AS MATERIALIZED (SELECT c.id FROM {recurrences} AS c "
                + $"WHERE c.definition = $1 AND c.paused AND NOT {SubjectPaused}) "
                + $"SELECT c.id FROM {recurrences} AS c JOIN stale AS s ON s.id = c.id WHERE c.paused LIMIT $2 FOR UPDATE OF c SKIP LOCKED) "
                + $"RETURNING definition, subject_kind, subject_id) INSERT INTO {changes} (definition, subject_kind, subject_id) SELECT * FROM flagged",
                Text(definition),
                Int32(limit)));
            batch.BatchCommands.Add(Command(
                $"WITH flagged AS (UPDATE {recurrences} SET paused = true WHERE id IN (SELECT c.id FROM {pauses} AS p JOIN {recurrences} AS c ON c.definition = p.definition "
                + "AND c.subject_kind = p.subject_kind AND c.subject_id = p.subject_id WHERE p.definition = $1 AND NOT c.paused LIMIT $2 FOR UPDATE OF c SKIP LOCKED) "
                + $"RETURNING definition, subject_kind, subject_id) INSERT INTO {changes} (definition, subject_kind, subject_id) SELECT * FROM flagged",
                Text(definition),
                Int32(limit)));
            batch.BatchCommands.Add(Command(
                $"UPDATE {sql.Table} SET paused = false WHERE id IN (WITH stale AS MATERIALIZED (SELECT r.id FROM {sql.Table} AS r "
                + $"WHERE r.definition = $1 AND r.paused AND r.state = 0 AND NOT {sql.SubjectPaused}) "
                + $"SELECT r.id FROM {sql.Table} AS r JOIN stale AS s ON s.id = r.id WHERE r.paused AND r.state = 0 LIMIT $2 FOR UPDATE OF r SKIP LOCKED)",
                Text(definition),
                Int32(limit)));
            batch.BatchCommands.Add(Command(
                $"UPDATE {sql.Table} SET paused = true WHERE id IN (SELECT r.id FROM {pauses} AS p JOIN {sql.Table} AS r ON r.definition = p.definition "
                + "AND r.subject_kind = p.subject_kind AND r.subject_id = p.subject_id AND r.state = 0 WHERE p.definition = $1 AND NOT r.paused LIMIT $2 FOR UPDATE OF r SKIP LOCKED)",
                Text(definition),
                Int32(limit)));
            await batch.ExecuteNonQueryAsync(cancellationToken);
            repaired = [.. batch.BatchCommands.Select(static (NpgsqlBatchCommand c) => c.RecordsAffected)];
        }
        if (repaired[0] > 0)
        {
            await NotifyPlanAsync(new PostgreSqlTransaction(transaction), source, cancellationToken);
        }
        if (repaired[0] + repaired[1] > 0)
        {
            await using var notify = new NpgsqlCommand($"WITH queued AS (SELECT $1::text AS definition) {NotifyMonitor("queued", 2)}", connection, transaction);
            notify.Parameters.Add(Text(definition));
            notify.Parameters.AddRange(MonitorSignal(source));
            await notify.ExecuteNonQueryAsync(cancellationToken);
        }
        if (repaired[2] > 0)
        {
            await NotifyDueAsync(new PostgreSqlTransaction(transaction), source, cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        return repaired.Max();
    }
}
