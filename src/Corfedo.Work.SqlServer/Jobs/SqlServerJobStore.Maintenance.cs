using Corfedo.Work.SqlServer.Leases;

namespace Corfedo.Work.SqlServer.Jobs;

internal sealed partial class SqlServerJobStore
{
    /// <summary>
    /// Reads every definition's maintenance in one statement outside a transaction: a row per definition, its key a
    /// literal, with the conditions the maintenance calls select rows by, shared with them, as <c>EXISTS</c> probes, which take no locks
    /// beyond the statement's read-committed reads. The statement text depends only on the sources, so a process sends
    /// the same one at every pass.
    /// </summary>
    public async ValueTask<IReadOnlyDictionary<string, JobMaintenanceDue>> MaintenanceDueAsync(IReadOnlyCollection<LeaseSource<JobId>> sources, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sources);
        var due = new Dictionary<string, JobMaintenanceDue>(StringComparer.Ordinal);
        if (sources.Count == 0)
        {
            return due;
        }
        var rows = new List<string>(sources.Count);
        foreach (var source in sources)
        {
            rows.Add(Probe(await SqlAsync(source, cancellationToken)));
        }
        await using var connection = await leases.OpenAsync(cancellationToken);
        await using var command = Tsql.Command(connection, null,
            "DECLARE @now datetime2(7) = SYSUTCDATETIME();\n" + string.Join("\nUNION ALL\n", rows) + ";");
        await using var reader = await command.ReaderAsync(cancellationToken);
        while (await reader.NextRowAsync(cancellationToken))
        {
            due[reader.GetString(0)] = new(
                reader.IsDBNull(1) ? null : reader.GetInt64(1),
                reader.GetBoolean(2),
                reader.GetBoolean(3),
                reader.GetBoolean(4),
                reader.GetBoolean(5),
                reader.GetBoolean(6),
                reader.IsDBNull(7) ? null : Tsql.FromMicros(reader.GetInt64(7)));
        }
        return due;
    }

    /// <summary>The probe row of one definition: key, revision, the five flags and the microseconds until its next future instant.</summary>
    private string Probe(LeaseSql sql)
    {
        var definition = Tsql.Literal(sql.Job!.Definition);
        const string active = "COALESCE(d.paused, 0) = 0";
        static string Exists(string from) => $"EXISTS (SELECT 1 FROM {from})";
        var planning = Exists($"{Jobs} AS j WHERE {Unconsumed("j", definition)}")
            + $" OR ({active} AND ({Exists($"{recurrences} AS c WHERE {DueToPlan("c", definition)}")} OR {Exists($"{recurrences} AS c WHERE {WaitingToPlan("c", definition)}")}))";
        var calendar = Exists($"{sql.Table} AS r WHERE {CalendarInstantCame("r", definition)}") + " OR " + Exists($"{sql.Table} AS r WHERE {WindowClosed("r", definition)}");
        var skipped = Exists($"{sql.Table} AS r WHERE {sql.Scope} AND {Skipping}");
        var retired = Exists($"{sql.Table} AS r WHERE {Retired(sql)}");
        var repairs = Exists($"{recurrences} AS c WHERE {RecurrencePauseCleared(definition)}")
            + " OR " + Exists($"{pauses} AS p INNER JOIN {recurrences} AS c ON {RecurrenceOfPause} WHERE {RecurrencePauseSet(definition)}")
            + " OR " + Exists($"{sql.Table} AS r WHERE {JobPauseCleared(definition)}")
            + " OR " + Exists($"{pauses} AS p INNER JOIN {sql.Table} AS r ON {JobOfPause} WHERE {JobPauseSet(definition)}");
        var next = "(SELECT MIN(n.t) FROM (VALUES "
            + $"(CASE WHEN {active} THEN (SELECT MIN(c.next_at) FROM {recurrences} AS c WHERE {NextCounts("c", definition)} AND c.next_at > @now) END), "
            + $"((SELECT MIN(r.calendar_at) FROM {sql.Table} AS r WHERE {CalendarInstantNext("r", definition)})), "
            + $"((SELECT MIN(r.window_end) FROM {sql.Table} AS r WHERE {WindowNext("r", definition)}))) AS n (t))";
        return $"SELECT CAST({definition} AS nvarchar(200)), d.revision, {Tsql.Bit(planning)}, {Tsql.Bit(calendar)}, {Tsql.Bit(skipped)}, {Tsql.Bit(retired)}, {Tsql.Bit(repairs)}, "
            + $"{Tsql.Micros("SYSUTCDATETIME()", next)} FROM (SELECT 1 AS one) AS single LEFT JOIN {definitions} AS d ON d.[key] = {definition}";
    }
}
