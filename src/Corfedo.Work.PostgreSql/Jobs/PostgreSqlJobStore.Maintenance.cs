using Corfedo.Work.PostgreSql.Leases;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Jobs;

internal sealed partial class PostgreSqlJobStore
{
    /// <summary>
    /// Reads every definition's maintenance in one statement outside a transaction: a row per definition, its key a
    /// literal, with the conditions of the maintenance calls as <c>EXISTS</c> probes, which lock nothing. The statement
    /// text depends only on the sources, so a process sends the same one at every pass.
    /// </summary>
    public async ValueTask<IReadOnlyDictionary<string, JobMaintenanceDue>> MaintenanceDueAsync(IReadOnlyCollection<LeaseSource<JobId>> sources, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sources);
        var due = new Dictionary<string, JobMaintenanceDue>(StringComparer.Ordinal);
        if (sources.Count == 0)
        {
            return due;
        }
        await using var command = dataSource.CreateCommand(string.Join(" UNION ALL ", sources.Select(Probe)));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            due[reader.GetString(0)] = new(
                reader.IsDBNull(1) ? null : reader.GetInt64(1),
                reader.GetBoolean(2),
                reader.GetBoolean(3),
                reader.GetBoolean(4),
                reader.GetBoolean(5),
                reader.GetBoolean(6),
                reader.IsDBNull(7) ? null : Seconds(reader.GetDouble(7)));
        }
        return due;
    }

    /// <summary>The probe row of one definition: key, revision, the five flags and the seconds until its next future instant.</summary>
    private string Probe(LeaseSource<JobId> source)
    {
        var sql = Sql(source);
        var definition = LeaseSql.Literal(sql.Job!.Definition);
        const string active = "NOT coalesce(d.paused, false)";
        string Exists(string from) => $"EXISTS (SELECT 1 FROM {from})";
        return $"(SELECT {definition}::text, d.revision, "
            + $"({Exists($"{sql.Table} AS j WHERE j.definition = {definition} AND j.unconsumed")} OR ({active} AND "
            + $"({Exists($"{recurrences} AS c WHERE {DueToPlan(sql, definition)}")} OR {Exists($"{recurrences} AS c WHERE {WaitingToPlan(sql, definition)}")}))), "
            + $"({Exists($"{sql.Table} AS r WHERE {CalendarInstantCame(definition)}")} OR {Exists($"{sql.Table} AS r WHERE {WindowClosed(definition)}")}), "
            + $"{Exists($"{sql.Table} AS r WHERE {sql.Scope} AND {Skipping}")}, "
            + $"{Exists($"{sql.Table} AS r WHERE {Retired(sql)}")}, "
            + $"({Exists($"{recurrences} AS c WHERE {RecurrencePauseCleared(definition)}")} OR {Exists(RecurrencePauseSet(definition))} "
            + $"OR {Exists($"{sql.Table} AS r WHERE {JobPauseCleared(sql, definition)}")} OR {Exists(JobPauseSet(sql, definition))}), "
            + "extract(epoch FROM least("
            + $"CASE WHEN {active} THEN (SELECT c.next_at FROM {recurrences} AS c WHERE {NextCounts(sql, definition)} AND c.next_at > now() ORDER BY c.next_at LIMIT 1) END, "
            + $"(SELECT min(t) FROM ({NextCalendarInstants(sql, definition)}) AS next)) - clock_timestamp())::float8 "
            + $"FROM (SELECT 1) AS one LEFT JOIN {definitions} AS d ON d.key = {definition})";
    }
}
