using Npgsql;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Monitoring;

internal sealed partial class PostgreSqlMonitoringStore
{
    /// <summary>
    /// Records a job's end in the transaction that ends it, for a definition with expectations, after the job's own
    /// update. A failure (<see cref="JobOutcome.Failed"/> or <see cref="JobOutcome.TimedOut"/>) increments the key's
    /// failures when the definition has a failure limit and opens an incident once they exceed it. A completion
    /// updates the key's row, and the definition-alone row of a definition expected as a whole, only when it resets
    /// failures, meets the window's deadline first or follows a passed deadline not yet processed; then, in a separate
    /// statement that sees incidents committed while the update waited for the row, it resolves the key's open
    /// incidents; a resolved <see cref="WorkIncidentKind.CompletesBy"/> incident starts the key's window at the
    /// completion. Other outcomes change nothing. Signals the incident source when an incident opened or resolved.
    /// </summary>
    internal async Task<IncidentChanges> RecordEndAsync(PostgreSqlTransaction work, string definition, JobId job, Subject? subject, JobOutcome outcome, DateTimeOffset endedAt, int? failureLimit, CancellationToken cancellationToken)
    {
        var changes = outcome switch
        {
            JobOutcome.Failed or JobOutcome.TimedOut when failureLimit is { } limit => await RecordFailureAsync(work, definition, job, subject, endedAt, limit, cancellationToken),
            JobOutcome.Completed => await RecordSuccessAsync(work, definition, subject, endedAt, cancellationToken),
            _ => IncidentChanges.None,
        };
        if (changes.Any)
        {
            await NotifyAsync(work.Connection, work.Transaction, cancellationToken);
        }
        WorkTelemetry.Count(changes);
        return changes;
    }

    private async Task<IncidentChanges> RecordFailureAsync(PostgreSqlTransaction work, string definition, JobId job, Subject? subject, DateTimeOffset endedAt, int limit, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"WITH s AS (INSERT INTO {states} AS s (definition, subject_kind, subject_id, failures) VALUES ($1, $2, $3, 1) "
            + "ON CONFLICT ON CONSTRAINT job_expectation_states_key DO UPDATE SET failures = s.failures + 1 RETURNING s.failures) "
            + $"INSERT INTO {incidents} AS i (id, kind, definition, subject_kind, subject_id, job, opened_at, due_at) "
            + $"SELECT gen_random_uuid(), {(int)WorkIncidentKind.FailsAtMostInARow}, $1, $2, $3, $4, $5, clock_timestamp() FROM s WHERE s.failures > $6 "
            + "ON CONFLICT (definition, subject_kind, subject_id, kind, date_set) WHERE resolved_at IS NULL DO NOTHING "
            + $"RETURNING {IncidentColumns}",
            work.Connection,
            work.Transaction);
        command.Parameters.Add(Text(definition));
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)subject?.Kind ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)subject?.Id ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.Add(Uuid(job.Value));
        command.Parameters.Add(Time(endedAt));
        command.Parameters.Add(Int32(limit));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var opened = await ReadIncidentsAsync(reader, cancellationToken);
        return opened.Count == 0 ? IncidentChanges.None : new IncidentChanges(opened, []);
    }

    private async Task<IncidentChanges> RecordSuccessAsync(PostgreSqlTransaction work, string definition, Subject? subject, DateTimeOffset endedAt, CancellationToken cancellationToken)
    {
        // $1 definition, $2 completion time, then the subject.
        const string InWindow = "s.met_at IS NULL AND $2 >= s.window_start AND $2 < s.deadline_at";
        var whole = subject is null ? "" : " OR (s.subject_kind IS NULL AND s.whole)";
        List<WorkIncident> resolved;
        await using (var batch = new NpgsqlBatch(work.Connection, work.Transaction))
        {
            batch.BatchCommands.Add(Command(
                $"UPDATE {states} AS s SET failures = 0, "
                + $"met_at = CASE WHEN {InWindow} THEN $2 ELSE s.met_at END, "
                + "first_after = CASE WHEN s.deadline_at <= $2 THEN least(s.first_after, $2) ELSE s.first_after END, "
                + "last_after = CASE WHEN s.deadline_at <= $2 THEN greatest(s.last_after, $2) ELSE s.last_after END "
                + $"WHERE s.definition = $1 AND ({KeyMatch("s", subject, 3)}{whole}) AND (s.failures > 0 OR ({InWindow}) OR s.deadline_at <= $2)",
                [Text(definition), Time(endedAt), .. SubjectParameters(subject)]));
            var wholeIncident = subject is null
                ? ""
                : $" OR (i.subject_kind IS NULL AND i.kind = {(int)WorkIncidentKind.CompletesBy} "
                    + $"AND EXISTS (SELECT 1 FROM {states} AS s WHERE s.definition = $1 AND s.subject_kind IS NULL AND s.whole))";
            batch.BatchCommands.Add(Command(
                $"UPDATE {incidents} AS i SET resolved_at = greatest($2, i.opened_at), due_at = coalesce(i.due_at, clock_timestamp()) "
                + $"WHERE i.resolved_at IS NULL AND i.definition = $1 AND i.kind IN ({ExpectationKinds.Sql}) AND (({KeyMatch("i", subject, 3)}){wholeIncident}) "
                + $"RETURNING {IncidentColumns}",
                [Text(definition), Time(endedAt), .. SubjectParameters(subject)]));
            // The state update returns no rows, so the reader starts at the resolved incidents.
            await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
            resolved = await ReadIncidentsAsync(reader, cancellationToken);
        }
        if (resolved.Count == 0)
        {
            return IncidentChanges.None;
        }
        // A completion that resolves a missed deadline belongs to it: the key's next window starts at the completion.
        foreach (var incident in resolved.Where(i => i.Kind == WorkIncidentKind.CompletesBy))
        {
            await using var command = new NpgsqlCommand(
                $"UPDATE {states} AS s SET window_start = $2, met_at = NULL WHERE s.definition = $1 AND {KeyMatch("s", incident.Subject, 3)}",
                work.Connection,
                work.Transaction);
            command.Parameters.Add(Text(definition));
            command.Parameters.Add(Time(endedAt));
            AddSubject(command.Parameters, incident.Subject);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        return new IncidentChanges([], resolved);
    }

    private static NpgsqlParameter[] SubjectParameters(Subject? subject) => subject is null ? [] : [Text(subject.Kind), Text(subject.Id)];
}
