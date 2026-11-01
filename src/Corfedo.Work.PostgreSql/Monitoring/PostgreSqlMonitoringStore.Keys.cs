using Npgsql;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Monitoring;

/// <summary>
/// Tracking of expected keys. Transactions that change a recurrence or its copied pause flag queue its key in
/// <c>job_expectation_changes</c>; each pass applies the queued keys, so a steady pass reads what changed. A full
/// reconciliation, which reads every recurrence and key of a definition, runs only while the definition's revision or
/// pause differs from the ones it last reconciled with, and is recorded once a read without locks finds nothing left.
/// </summary>
internal sealed partial class PostgreSqlMonitoringStore
{
    /// <summary>Sets the window of a row inserted or taken up for a deadline; parameters from <c>$3</c>: deadline, window start, revision.</summary>
    private const string StartWindow =
        "deadline_at = coalesce($3, 'infinity'), window_start = $4, met_at = NULL, first_after = NULL, last_after = NULL, revision = $5";

    /// <summary>Whether the key of a row aliased <c>s</c> has an unpaused recurrence of the definition <c>$1</c>.</summary>
    private string HasUnpausedRecurrence(string row) =>
        $"CASE WHEN {row}.subject_kind IS NULL "
        + $"THEN EXISTS (SELECT 1 FROM {recurrences} AS r WHERE r.definition = $1 AND r.subject_kind IS NULL AND NOT r.paused) "
        + $"ELSE EXISTS (SELECT 1 FROM {recurrences} AS r WHERE r.definition = $1 AND r.subject_kind = {row}.subject_kind AND r.subject_id = {row}.subject_id AND NOT r.paused) END";

    /// <summary>
    /// The start of a statement that clears the windows of the rows the query <paramref name="gone"/> names by
    /// <c>ctid</c> as <c>t</c> and resolves their keys' open <see cref="WorkIncidentKind.CompletesBy"/> incidents, as the
    /// CTEs <c>cleared</c> and <c>resolved</c>; the definition is <c>$1</c>.
    /// </summary>
    private string ClearWindows(string gone) =>
        $"WITH gone AS ({gone}), "
        + $"cleared AS (UPDATE {states} AS s SET deadline_at = NULL, window_start = NULL, met_at = NULL, first_after = NULL, last_after = NULL, whole = false, revision = NULL "
        + "FROM gone WHERE s.ctid = gone.t RETURNING s.subject_kind, s.subject_id), "
        + $"resolved AS (UPDATE {incidents} AS i SET resolved_at = clock_timestamp(), due_at = coalesce(i.due_at, clock_timestamp()) FROM cleared AS c "
        + $"WHERE i.definition = $1 AND i.kind = {(int)WorkIncidentKind.CompletesBy} AND i.resolved_at IS NULL "
        + "AND i.subject_kind IS NOT DISTINCT FROM c.subject_kind AND i.subject_id IS NOT DISTINCT FROM c.subject_id "
        + $"RETURNING {IncidentColumns}) ";

    /// <summary>
    /// Deletes the queued changes locked without waiting and, in the same transaction, reads each key in a new statement,
    /// so it sees every change committed before the deletion: whether it has an unpaused recurrence, and its row locked
    /// without waiting. Then starts, clears or queues again the keys that need it.
    /// </summary>
    public async ValueTask<int> ApplyChangesAsync(string definition, long revision, ExpectationWindow? window, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        var deleted = 0;
        var keys = new HashSet<(string? Kind, string? Id)>();
        await using (var delete = new NpgsqlCommand(
            $"DELETE FROM {keyChanges} AS c WHERE c.ctid = ANY (ARRAY (SELECT q.ctid FROM {keyChanges} AS q WHERE q.definition = $1 LIMIT $2 FOR UPDATE SKIP LOCKED)) "
            + "RETURNING c.subject_kind, c.subject_id",
            connection,
            transaction))
        {
            delete.Parameters.Add(Text(definition));
            delete.Parameters.Add(Int32(limit));
            await using var reader = await delete.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                deleted++;
                keys.Add((reader.IsDBNull(0) ? null : reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1)));
            }
        }
        var changes = IncidentChanges.None;
        if (window is not null && keys.Count > 0)
        {
            changes = await ApplyKeysAsync(connection, transaction, definition, revision, window, keys, cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        WorkTelemetry.Count(changes);
        return deleted;
    }

    private async Task<IncidentChanges> ApplyKeysAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string definition, long revision, ExpectationWindow window, IReadOnlyCollection<(string? Kind, string? Id)> keys, CancellationToken cancellationToken)
    {
        // Subject keys and the definition-alone key are read in separate branches, so each matches its row by equality
        // or IS NULL on the key index.
        const string SubjectRow = "s.definition = $1 AND s.subject_kind = k.subject_kind AND s.subject_id = k.subject_id";
        const string AloneRow = "s.definition = $1 AND s.subject_kind IS NULL";
        string Lateral(string row) =>
            $"LEFT JOIN LATERAL (SELECT s.ctid AS t, s.deadline_at IS NOT NULL AS tracked, s.whole FROM {states} AS s WHERE {row} FOR UPDATE OF s SKIP LOCKED) AS l ON true";
        var requeue = new List<(string? Kind, string? Id)>();
        var start = new List<(string? Kind, string? Id)>();
        var clear = new List<string>();
        await using (var read = new NpgsqlCommand(
            "WITH k AS (SELECT * FROM unnest($2::text[], $3::text[]) AS k(subject_kind, subject_id)) "
            + $"SELECT k.subject_kind, k.subject_id, EXISTS (SELECT 1 FROM {recurrences} AS r WHERE r.definition = $1 AND r.subject_kind = k.subject_kind AND r.subject_id = k.subject_id AND NOT r.paused), "
            + $"l.t::text, l.tracked, l.whole, EXISTS (SELECT 1 FROM {states} AS s WHERE {SubjectRow}) FROM k {Lateral(SubjectRow)} "
            + $"UNION ALL SELECT NULL, NULL, EXISTS (SELECT 1 FROM {recurrences} AS r WHERE r.definition = $1 AND r.subject_kind IS NULL AND NOT r.paused), "
            + $"l.t::text, l.tracked, l.whole, EXISTS (SELECT 1 FROM {states} AS s WHERE {AloneRow}) FROM (SELECT 1 WHERE $4) AS k {Lateral(AloneRow)}",
            connection,
            transaction))
        {
            var subjects = keys.Where(k => k.Kind is not null).ToArray();
            read.Parameters.Add(Text(definition));
            read.Parameters.Add(Texts(subjects.Select(k => k.Kind)));
            read.Parameters.Add(Texts(subjects.Select(k => k.Id)));
            read.Parameters.Add(Bool(subjects.Length < keys.Count));
            await using var reader = await read.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var key = (reader.IsDBNull(0) ? null : reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1));
                var expected = reader.GetBoolean(2);
                var row = reader.IsDBNull(3) ? null : reader.GetString(3);
                if (row is null && reader.GetBoolean(6))
                {
                    // Another transaction holds the key's row.
                    requeue.Add(key);
                }
                else if (expected && (row is null || !reader.GetBoolean(4)))
                {
                    start.Add(key);
                }
                else if (!expected && row is not null && reader.GetBoolean(4) && !reader.GetBoolean(5))
                {
                    clear.Add(row);
                }
            }
        }
        if (requeue.Count > 0)
        {
            await using var command = new NpgsqlCommand($"INSERT INTO {keyChanges} (definition, subject_kind, subject_id) SELECT $1, * FROM unnest($2::text[], $3::text[])", connection, transaction);
            command.Parameters.Add(Text(definition));
            command.Parameters.Add(Texts(requeue.Select(k => k.Kind)));
            command.Parameters.Add(Texts(requeue.Select(k => k.Id)));
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        if (start.Count > 0)
        {
            await using var command = new NpgsqlCommand(
                $"INSERT INTO {states} AS s (definition, subject_kind, subject_id, deadline_at, window_start, revision) "
                + "SELECT $1, k.subject_kind, k.subject_id, coalesce($4, 'infinity'), $5, $6 FROM unnest($2::text[], $3::text[]) AS k(subject_kind, subject_id) "
                + "ON CONFLICT ON CONSTRAINT job_expectation_states_key DO UPDATE SET deadline_at = EXCLUDED.deadline_at, window_start = EXCLUDED.window_start, "
                + "met_at = NULL, first_after = NULL, last_after = NULL, revision = EXCLUDED.revision WHERE s.deadline_at IS NULL",
                connection,
                transaction);
            command.Parameters.Add(Text(definition));
            command.Parameters.Add(Texts(start.Select(k => k.Kind)));
            command.Parameters.Add(Texts(start.Select(k => k.Id)));
            command.Parameters.Add(NullableTime(Deadline(window.Deadline)));
            command.Parameters.Add(Time(window.WindowStart));
            command.Parameters.Add(Int64(revision));
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        if (clear.Count == 0)
        {
            return IncidentChanges.None;
        }
        List<WorkIncident> resolved;
        await using (var command = new NpgsqlCommand(ClearWindows("SELECT unnest($2::text[]::tid[]) AS t") + "SELECT * FROM resolved AS i", connection, transaction))
        {
            command.Parameters.Add(Text(definition));
            command.Parameters.Add(Texts(clear));
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            resolved = await ReadIncidentsAsync(reader, cancellationToken);
        }
        if (resolved.Count == 0)
        {
            return IncidentChanges.None;
        }
        await NotifyAsync(connection, transaction, cancellationToken);
        return new IncidentChanges([], resolved);
    }

    /// <summary>
    /// Without recurrences, inserts or takes up the definition-alone row as the whole definition unless it has a
    /// deadline, keeping a deadline it has. With them, locks the row if it is marked whole without waiting, then keeps its
    /// window when a recurrence without subject is unpaused and clears it otherwise.
    /// </summary>
    public async ValueTask TrackWholeAsync(string definition, long revision, bool whole, ExpectationWindow window, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(window);
        if (whole)
        {
            await using var insert = dataSource.CreateCommand(
                $"INSERT INTO {states} AS s (definition, whole, deadline_at, window_start, revision) SELECT $1, true, coalesce($2, 'infinity'), $3, $4 "
                + $"WHERE NOT EXISTS (SELECT 1 FROM {states} AS e WHERE e.definition = $1 AND e.subject_kind IS NULL AND e.whole AND e.deadline_at IS NOT NULL) "
                + "ON CONFLICT ON CONSTRAINT job_expectation_states_key DO UPDATE SET whole = true, "
                + "deadline_at = coalesce(s.deadline_at, EXCLUDED.deadline_at), window_start = CASE WHEN s.deadline_at IS NULL THEN EXCLUDED.window_start ELSE s.window_start END, "
                + "revision = CASE WHEN s.deadline_at IS NULL THEN EXCLUDED.revision ELSE s.revision END");
            insert.Parameters.Add(Text(definition));
            insert.Parameters.Add(NullableTime(Deadline(window.Deadline)));
            insert.Parameters.Add(Time(window.WindowStart));
            insert.Parameters.Add(Int64(revision));
            await insert.ExecuteNonQueryAsync(cancellationToken);
            return;
        }
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        string? row = null;
        var kept = false;
        await using (var read = new NpgsqlCommand(
            $"SELECT s.ctid::text, {HasUnpausedRecurrence("s")} FROM {states} AS s WHERE s.definition = $1 AND s.subject_kind IS NULL AND s.whole FOR UPDATE SKIP LOCKED",
            connection,
            transaction))
        {
            read.Parameters.Add(Text(definition));
            await using var reader = await read.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                (row, kept) = (reader.GetString(0), reader.GetBoolean(1));
            }
        }
        if (row is null)
        {
            return;
        }
        var changes = IncidentChanges.None;
        if (kept)
        {
            await using var update = new NpgsqlCommand($"UPDATE {states} SET whole = false WHERE ctid = $1::text::tid", connection, transaction);
            update.Parameters.Add(Text(row));
            await update.ExecuteNonQueryAsync(cancellationToken);
        }
        else
        {
            List<WorkIncident> resolved;
            await using (var clear = new NpgsqlCommand(ClearWindows("SELECT $2::text::tid AS t") + "SELECT * FROM resolved AS i", connection, transaction))
            {
                clear.Parameters.Add(Text(definition));
                clear.Parameters.Add(Text(row));
                await using var reader = await clear.ExecuteReaderAsync(cancellationToken);
                resolved = await ReadIncidentsAsync(reader, cancellationToken);
            }
            if (resolved.Count > 0)
            {
                await NotifyAsync(connection, transaction, cancellationToken);
                changes = new IncidentChanges([], resolved);
            }
        }
        await transaction.CommitAsync(cancellationToken);
        WorkTelemetry.Count(changes);
    }

    /// <summary>
    /// Keys are found by an anti-join of the definition's unpaused recurrences against its rows with a deadline, in the
    /// order of their subject index, at most <paramref name="limit"/> per statement, and the definition alone for a
    /// recurrence without subject. A row the ending transaction created for failures takes up the deadline.
    /// </summary>
    public async ValueTask<int> InsertExpectedAsync(string definition, long revision, ExpectationWindow window, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(window);
        await using var command = dataSource.CreateCommand(
            $"INSERT INTO {states} AS s (definition, subject_kind, subject_id, deadline_at, window_start, revision) "
            + "SELECT $1, k.subject_kind, k.subject_id, coalesce($3, 'infinity'), $4, $5 FROM ("
            + $"(SELECT DISTINCT r.subject_kind, r.subject_id FROM {recurrences} AS r WHERE r.definition = $1 AND r.subject_kind IS NOT NULL AND NOT r.paused "
            + $"AND NOT EXISTS (SELECT 1 FROM {states} AS e WHERE e.definition = $1 AND e.subject_kind = r.subject_kind AND e.subject_id = r.subject_id AND e.deadline_at IS NOT NULL) "
            + "ORDER BY r.subject_kind, r.subject_id LIMIT $2) "
            + "UNION ALL SELECT NULL, NULL "
            + $"WHERE EXISTS (SELECT 1 FROM {recurrences} AS r WHERE r.definition = $1 AND r.subject_kind IS NULL AND NOT r.paused) "
            + $"AND NOT EXISTS (SELECT 1 FROM {states} AS e WHERE e.definition = $1 AND e.subject_kind IS NULL AND e.deadline_at IS NOT NULL)) AS k "
            + $"ON CONFLICT ON CONSTRAINT job_expectation_states_key DO UPDATE SET whole = false, {StartWindow}");
        command.Parameters.Add(Text(definition));
        command.Parameters.Add(Int32(limit));
        command.Parameters.Add(NullableTime(Deadline(window.Deadline)));
        command.Parameters.Add(Time(window.WindowStart));
        command.Parameters.Add(Int64(revision));
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Locks up to <paramref name="limit"/> rows with a deadline that are not expected without waiting, clears their
    /// window and resolves their open completion incidents in one statement. While tracking, a row is expected when it
    /// is marked whole or a recurrence of its key is not paused.
    /// </summary>
    public async ValueTask<int> ClearUnexpectedAsync(string definition, bool tracking, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        var expectedRow = tracking ? $"s.whole OR {HasUnpausedRecurrence("s")}" : "false";
        await using var command = dataSource.CreateCommand(
            ClearWindows($"SELECT s.ctid AS t FROM {states} AS s WHERE s.definition = $1 AND s.deadline_at IS NOT NULL AND NOT ({expectedRow}) LIMIT $2 FOR UPDATE SKIP LOCKED")
            + "SELECT (SELECT count(*) FROM cleared)::int4, r.* FROM (SELECT 1) AS one LEFT JOIN resolved AS r ON true");
        command.Parameters.Add(Text(definition));
        command.Parameters.Add(Int32(limit));
        var count = 0;
        var resolved = new List<WorkIncident>();
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                count = reader.GetInt32(0);
                if (!reader.IsDBNull(1))
                {
                    resolved.Add(ReadIncident(reader, 1));
                }
            }
        }
        WorkTelemetry.Count(resolved.Count == 0 ? IncidentChanges.None : new IncidentChanges([], resolved));
        return count;
    }

    /// <summary>
    /// One update of the definition row, guarded by its revision and pause and by reads without locks of what a
    /// reconciliation leaves to do; a row another transaction held stays as it was committed, so it is found.
    /// </summary>
    public async ValueTask<bool> MarkReconciledAsync(StoredExpectation definition, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        var expectation = definition.Expectation;
        var tracking = expectation?.Deadline is not null && !definition.Paused;
        var left = new List<string>
        {
            $"EXISTS (SELECT 1 FROM {incidents} AS i WHERE i.definition = $1 AND i.resolved_at IS NULL AND i.kind = ANY($4))",
        };
        if (expectation?.FailureLimit is null)
        {
            left.Add($"EXISTS (SELECT 1 FROM {states} AS s WHERE s.definition = $1 AND s.failures > 0)");
        }
        if (tracking)
        {
            left.Add($"EXISTS (SELECT 1 FROM {states} AS s WHERE s.definition = $1 AND s.deadline_at IS NOT NULL AND (s.revision IS DISTINCT FROM $2 OR NOT (s.whole OR {HasUnpausedRecurrence("s")})))");
            left.Add($"EXISTS (SELECT 1 FROM {recurrences} AS r WHERE r.definition = $1 AND r.subject_kind IS NOT NULL AND NOT r.paused "
                + $"AND NOT EXISTS (SELECT 1 FROM {states} AS e WHERE e.definition = $1 AND e.subject_kind = r.subject_kind AND e.subject_id = r.subject_id AND e.deadline_at IS NOT NULL))");
            left.Add($"(EXISTS (SELECT 1 FROM {recurrences} AS r WHERE r.definition = $1 AND r.subject_kind IS NULL AND NOT r.paused) "
                + $"AND NOT EXISTS (SELECT 1 FROM {states} AS e WHERE e.definition = $1 AND e.subject_kind IS NULL AND e.deadline_at IS NOT NULL))");
        }
        else
        {
            left.Add($"EXISTS (SELECT 1 FROM {states} AS s WHERE s.definition = $1 AND s.deadline_at IS NOT NULL)");
        }
        await using var command = dataSource.CreateCommand(
            $"UPDATE {definitions} AS d SET monitored_revision = $2, monitored_paused = $3 WHERE d.key = $1 AND d.revision = $2 AND d.paused = $3 "
            + $"AND NOT ({string.Join(" OR ", left)})");
        command.Parameters.Add(Text(definition.Definition));
        command.Parameters.Add(Int64(definition.Revision));
        command.Parameters.Add(Bool(definition.Paused));
        command.Parameters.Add(new NpgsqlParameter<short[]> { TypedValue = [.. ExpectationKinds.Unexpected(expectation).Select(k => (short)k)] });
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }
}
