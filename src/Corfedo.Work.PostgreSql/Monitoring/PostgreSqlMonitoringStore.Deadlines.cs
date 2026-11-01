using Npgsql;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Monitoring;

internal sealed partial class PostgreSqlMonitoringStore
{
    /// <summary>
    /// Reads the distinct window starts of the rows to move, computes their deadlines and moves the rows locked without
    /// waiting. Recorded completions are placed against the new deadline: the first before it is the window's, the
    /// first and latest at or after it are completions after the deadline.
    /// </summary>
    public async ValueTask<int> RescheduleAsync(string definition, long revision, Func<DateTimeOffset, DateTimeOffset> next, bool olderRevisions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(next);
        var selection = olderRevisions
            ? "s.definition = $1 AND (s.deadline_at = 'infinity' OR (s.deadline_at IS NOT NULL AND s.revision IS DISTINCT FROM $2))"
            : "s.definition = $1 AND s.deadline_at = 'infinity'";
        var starts = new List<DateTimeOffset>();
        await using (var read = dataSource.CreateCommand($"SELECT DISTINCT s.window_start FROM {states} AS s WHERE {selection}"))
        {
            read.Parameters.Add(Text(definition));
            read.Parameters.Add(Int64(revision));
            await using var reader = await read.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                starts.Add(reader.GetFieldValue<DateTimeOffset>(0));
            }
        }
        var moves = starts.Select(w => (Start: w, Deadline: next(w)))
            .Where(m => olderRevisions || m.Deadline != ExpectationWindow.Uncovered)
            .ToArray();
        if (moves.Length == 0)
        {
            return 0;
        }
        const string Completions = "unnest(ARRAY[s.met_at, s.first_after, s.last_after]) AS c";
        await using var command = dataSource.CreateCommand(
            $"WITH v AS (SELECT * FROM unnest($3::timestamptz[], $4::timestamptz[]) AS v(w, n)), "
            + $"moved AS (SELECT s.ctid AS t, coalesce(v.n, 'infinity') AS n FROM {states} AS s JOIN v ON s.window_start = v.w WHERE {selection} FOR UPDATE OF s SKIP LOCKED) "
            + $"UPDATE {states} AS s SET deadline_at = moved.n, revision = $2, "
            + $"met_at = (SELECT min(c) FROM {Completions} WHERE c < moved.n), "
            + $"first_after = (SELECT min(c) FROM {Completions} WHERE c >= moved.n), "
            + $"last_after = (SELECT max(c) FROM {Completions} WHERE c >= moved.n) "
            + "FROM moved WHERE s.ctid = moved.t");
        command.Parameters.Add(Text(definition));
        command.Parameters.Add(Int64(revision));
        command.Parameters.Add(Times(moves.Select(m => (DateTimeOffset?)m.Start)));
        command.Parameters.Add(Times(moves.Select(m => Deadline(m.Deadline))));
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Locks the definition's rows whose deadline passed by the store's clock without waiting, the clock read once as
    /// the bound of the deadline index, applies
    /// <paramref name="process"/> to each, then stores the processed windows and inserts the incidents of their missed
    /// deadlines in one statement: an incident a late completion resolved is inserted resolved, an open one only when the
    /// key has no open completion incident. Then reads the definition's earliest deadline.
    /// </summary>
    public async ValueTask<DeadlinePass> ProcessDeadlinesAsync(string definition, Func<ExpectationWindow, DateTimeOffset, ExpectationStep> process, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(process);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        var rows = new List<(string Tid, Subject? Subject, ExpectationStep Step)>();
        await using (var select = new NpgsqlCommand(
            "SELECT s.ctid::text, s.subject_kind, s.subject_id, s.deadline_at, s.window_start, s.met_at, s.first_after, s.last_after, clock_timestamp() "
            + $"FROM {states} AS s WHERE s.definition = $1 AND s.deadline_at <= (SELECT clock_timestamp()) ORDER BY s.deadline_at LIMIT $2 FOR UPDATE SKIP LOCKED",
            connection,
            transaction))
        {
            select.Parameters.Add(Text(definition));
            select.Parameters.Add(Int32(limit));
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var window = new ExpectationWindow(
                    reader.GetFieldValue<DateTimeOffset>(3),
                    reader.GetFieldValue<DateTimeOffset>(4),
                    reader.IsDBNull(5) ? null : reader.GetFieldValue<DateTimeOffset>(5),
                    reader.IsDBNull(6) ? null : reader.GetFieldValue<DateTimeOffset>(6),
                    reader.IsDBNull(7) ? null : reader.GetFieldValue<DateTimeOffset>(7));
                var subject = reader.IsDBNull(1) ? null : Subject.Of(reader.GetString(1), reader.GetString(2));
                rows.Add((reader.GetString(0), subject, process(window, reader.GetFieldValue<DateTimeOffset>(8))));
            }
        }
        var changes = IncidentChanges.None;
        if (rows.Count > 0)
        {
            var missed = rows.SelectMany(r => r.Step.Missed.Select(m => (r.Subject, Missed: m))).ToArray();
            await using var command = new NpgsqlCommand(
                "WITH v AS (SELECT * FROM unnest($2::text[]::tid[], $3::timestamptz[], $4::timestamptz[], $5::timestamptz[], $6::timestamptz[], $7::timestamptz[]) "
                + "AS v(t, deadline_at, window_start, met_at, first_after, last_after)), "
                + $"moved AS (UPDATE {states} AS s SET deadline_at = coalesce(v.deadline_at, 'infinity'), window_start = v.window_start, "
                + "met_at = v.met_at, first_after = v.first_after, last_after = v.last_after FROM v WHERE s.ctid = v.t) "
                + $"INSERT INTO {incidents} AS i (id, kind, definition, subject_kind, subject_id, opened_at, resolved_at, due_at) "
                + $"SELECT gen_random_uuid(), {(int)WorkIncidentKind.CompletesBy}, $1, m.kind, m.id, m.opened, m.resolved, clock_timestamp() "
                + "FROM unnest($8::text[], $9::text[], $10::timestamptz[], $11::timestamptz[]) AS m(kind, id, opened, resolved) "
                + "ON CONFLICT (definition, subject_kind, subject_id, kind, date_set) WHERE resolved_at IS NULL DO NOTHING "
                + $"RETURNING {IncidentColumns}",
                connection,
                transaction);
            command.Parameters.Add(Text(definition));
            command.Parameters.Add(Texts(rows.Select(r => r.Tid)));
            command.Parameters.Add(Times(rows.Select(r => Deadline(r.Step.Window.Deadline))));
            command.Parameters.Add(Times(rows.Select(r => (DateTimeOffset?)r.Step.Window.WindowStart)));
            command.Parameters.Add(Times(rows.Select(r => r.Step.Window.MetAt)));
            command.Parameters.Add(Times(rows.Select(r => r.Step.Window.FirstAfter)));
            command.Parameters.Add(Times(rows.Select(r => r.Step.Window.LastAfter)));
            command.Parameters.Add(Texts(missed.Select(m => m.Subject?.Kind)));
            command.Parameters.Add(Texts(missed.Select(m => m.Subject?.Id)));
            command.Parameters.Add(Times(missed.Select(m => (DateTimeOffset?)m.Missed.At)));
            command.Parameters.Add(Times(missed.Select(m => m.Missed.ResolvedAt)));
            List<WorkIncident> inserted;
            await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                inserted = await ReadIncidentsAsync(reader, cancellationToken);
            }
            if (inserted.Count > 0)
            {
                await NotifyAsync(connection, transaction, cancellationToken);
                changes = new IncidentChanges(inserted, [.. inserted.Where(i => i.ResolvedAt is not null)]);
            }
        }
        DateTimeOffset? next;
        await using (var earliest = new NpgsqlCommand(
            $"SELECT min(s.deadline_at) FROM {states} AS s WHERE s.definition = $1 AND s.deadline_at IS NOT NULL AND s.deadline_at < 'infinity'",
            connection,
            transaction))
        {
            earliest.Parameters.Add(Text(definition));
            await using var reader = await earliest.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            next = reader.IsDBNull(0) ? null : reader.GetFieldValue<DateTimeOffset>(0);
        }
        await transaction.CommitAsync(cancellationToken);
        WorkTelemetry.Count(changes);
        return new DeadlinePass(rows.Count, changes, next);
    }
}
