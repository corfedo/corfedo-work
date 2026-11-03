namespace Corfedo.Work.SqlServer.Monitoring;

internal sealed partial class SqlServerMonitoringStore
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
    internal async Task<IncidentChanges> RecordEndAsync(SqlServerTransaction work, string definition, JobId job, Subject? subject, JobOutcome outcome, DateTimeOffset endedAt, int? failureLimit, CancellationToken cancellationToken)
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

    /// <summary>
    /// Counts the failure on the key's row, creating it when missing; a creation that meets a row committed meanwhile
    /// counts on that one. An incident is opened once the failures exceed the limit, unless the key has one open.
    /// </summary>
    private async Task<IncidentChanges> RecordFailureAsync(SqlServerTransaction work, string definition, JobId job, Subject? subject, DateTimeOffset endedAt, int limit, CancellationToken cancellationToken)
    {
        await using var command = Tsql.Command(work, $"""
            DECLARE @failures int, @upsert_inserted bit;
            {Upsert(
                $"UPDATE s SET @failures = failures = s.failures + 1 FROM {states} AS s WHERE s.[definition] = @definition AND {KeyMatch("s", subject)};",
                $"INSERT INTO {states} ([definition], subject_kind, subject_id, failures) VALUES (@definition, @kind, @subject, 1);\nSET @failures = 1;")}
            IF @failures > @limit
            BEGIN
                {OpenIncident("id, kind, [definition], subject_kind, subject_id, job, opened_at, due_at",
                    $"VALUES (NEWID(), {(int)WorkIncidentKind.FailsAtMostInARow}, @definition, @kind, @subject, @job, @ended, SYSUTCDATETIME())")}
            END
            """,
            [
                Tsql.Text("@definition", definition, 200),
                .. SubjectParameters(subject),
                Tsql.Guid("@job", job.Value),
                Tsql.Time("@ended", endedAt),
                Tsql.Int32("@limit", limit),
            ]);
        await using var reader = await command.ReaderAsync(cancellationToken);
        var opened = await ReadIncidentsAsync(reader, cancellationToken);
        return opened.Count == 0 ? IncidentChanges.None : new IncidentChanges(opened, []);
    }

    /// <summary>
    /// Updates the definition-alone row of a definition expected as a whole and then the key's row, in key order, each
    /// through the key; then resolves the open incidents, found without locks and locked in primary-key order.
    /// </summary>
    private async Task<IncidentChanges> RecordSuccessAsync(SqlServerTransaction work, string definition, Subject? subject, DateTimeOffset endedAt, CancellationToken cancellationToken)
    {
        const string InWindow = "s.met_at IS NULL AND @ended >= s.window_start AND @ended < s.deadline_at";
        const string Update = $"""
            failures = 0,
                met_at = CASE WHEN {InWindow} THEN @ended ELSE s.met_at END,
                first_after = CASE WHEN s.deadline_at <= @ended THEN LEAST(s.first_after, @ended) ELSE s.first_after END,
                last_after = CASE WHEN s.deadline_at <= @ended THEN GREATEST(s.last_after, @ended) ELSE s.last_after END
            """;
        const string Applies = $"(s.failures > 0 OR ({InWindow}) OR s.deadline_at <= @ended)";
        var whole = subject is null
            ? ""
            : $"UPDATE s SET {Update} FROM {states} AS s WHERE s.[definition] = @definition AND s.subject_kind IS NULL AND s.subject_id IS NULL AND s.whole = 1 AND {Applies};\n";
        var wholeIncident = subject is null
            ? ""
            : $" OR (i.subject_kind IS NULL AND i.kind = {(int)WorkIncidentKind.CompletesBy} "
                + $"AND EXISTS (SELECT 1 FROM {states} AS s WHERE s.[definition] = @definition AND s.subject_kind IS NULL AND s.whole = 1))";
        var resolve = $"i.[definition] = @definition AND (({KeyMatch("i", subject)}){wholeIncident})";
        List<WorkIncident> resolved;
        await using (var command = Tsql.Command(work, $"""
            {whole}UPDATE s SET {Update} FROM {states} AS s WHERE s.[definition] = @definition AND {KeyMatch("s", subject)} AND {Applies};
            {ResolveIncidents(resolve, "GREATEST(@ended, i.opened_at)")}
            """,
            [Tsql.Text("@definition", definition, 200), Tsql.Time("@ended", endedAt), .. SubjectParameters(subject)]))
        await using (var reader = await command.ReaderAsync(cancellationToken))
        {
            resolved = await ReadIncidentsAsync(reader, cancellationToken);
        }
        if (resolved.Count == 0)
        {
            return IncidentChanges.None;
        }
        // A completion that resolves a missed deadline belongs to it: the key's next window starts at the completion.
        foreach (var incident in resolved.Where(i => i.Kind == WorkIncidentKind.CompletesBy))
        {
            await using var command = Tsql.Command(work,
                $"UPDATE s SET window_start = @ended, met_at = NULL FROM {states} AS s WHERE s.[definition] = @definition AND {KeyMatch("s", incident.Subject)};",
                [Tsql.Text("@definition", definition, 200), Tsql.Time("@ended", endedAt), .. SubjectParameters(incident.Subject)]);
            await command.NonQueryAsync(cancellationToken);
        }
        return new IncidentChanges([], resolved);
    }
}
