using Corfedo.Work.SqlServer.Leases;

namespace Corfedo.Work.SqlServer.Jobs;

internal sealed partial class SqlServerJobStore
{
    /// <summary>
    /// Reads every definition's maintenance in one statement outside a transaction: a row per definition, its key a
    /// literal, with the conditions the maintenance calls select rows by as <c>EXISTS</c> probes, which take no locks
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
        string Stored(string alias) =>
            $"EXISTS (SELECT 1 FROM {pauses} AS p WHERE p.[definition] = {alias}.[definition] AND p.subject_kind = {alias}.subject_kind AND p.subject_id = {alias}.subject_id)";
        var planning = Exists($"{Jobs} AS j WHERE j.[definition] = {definition} AND j.unconsumed = 1")
            + $" OR ({active} AND ("
            + Exists($"{recurrences} AS c WHERE c.[definition] = {definition} AND c.next_at IS NOT NULL AND c.next_at <= @now AND c.paused = 0 AND {SubjectUnpaused("c")} AND {DueCanPlan("c")}")
            + " OR " + Exists($"{recurrences} AS c WHERE c.[definition] = {definition} AND c.waiting_at IS NOT NULL AND c.paused = 0 AND {SubjectUnpaused("c")} AND NOT {OccurrenceOutstanding("c")}")
            + "))";
        var calendar = Exists($"{sql.Table} AS r WHERE r.[definition] = {definition} AND r.calendar_at IS NOT NULL AND r.calendar_at <= @now AND r.state = 0 AND r.calendar_unavailable = 0 AND r.lease_expires_at IS NULL")
            + " OR " + Exists($"{sql.Table} AS r WHERE r.[definition] = {definition} AND r.window_end IS NOT NULL AND r.window_end <= @now AND r.lease_expires_at IS NULL AND r.state = 0");
        var skipped = Exists($"{sql.Table} AS r WHERE {sql.Scope} AND r.state = 0 AND (r.skip_pending = 1 OR r.chain_outcome IS NOT NULL)");
        var retired = Exists($"{sql.Table} AS r WHERE {Retired(sql)}");
        var repairs = Exists($"{recurrences} AS c WHERE c.[definition] = {definition} AND c.paused = 1 AND NOT {Stored("c")}")
            + " OR " + Exists($"{pauses} AS p INNER JOIN {recurrences} AS c ON c.[definition] = p.[definition] AND c.subject_kind = p.subject_kind AND c.subject_id = p.subject_id "
                + $"WHERE p.[definition] = {definition} AND c.subject_kind IS NOT NULL AND c.paused = 0")
            + " OR " + Exists($"{sql.Table} AS r WHERE r.[definition] = {definition} AND r.paused = 1 AND r.state = 0 AND NOT {Stored("r")}")
            + " OR " + Exists($"{pauses} AS p INNER JOIN {sql.Table} AS r ON r.subject_kind = p.subject_kind AND r.subject_id = p.subject_id AND r.[definition] = p.[definition] "
                + $"WHERE p.[definition] = {definition} AND r.state = 0 AND r.paused = 0");
        var next = "(SELECT MIN(n.t) FROM (VALUES "
            + $"(CASE WHEN {active} THEN (SELECT MIN(c.next_at) FROM {recurrences} AS c WHERE c.[definition] = {definition} AND c.next_at IS NOT NULL AND c.next_at > @now "
            + $"AND c.paused = 0 AND {SubjectUnpaused("c")} AND {DueCanPlan("c")}) END), "
            + $"((SELECT MIN(r.calendar_at) FROM {sql.Table} AS r WHERE r.[definition] = {definition} AND r.calendar_at IS NOT NULL AND r.calendar_at > @now AND r.state = 0 "
            + "AND r.calendar_unavailable = 0 AND r.lease_expires_at IS NULL)), "
            + $"((SELECT MIN(r.window_end) FROM {sql.Table} AS r WHERE r.[definition] = {definition} AND r.window_end IS NOT NULL AND r.window_end > @now "
            + "AND r.lease_expires_at IS NULL AND r.state = 0))) AS n (t))";
        return $"SELECT CAST({definition} AS nvarchar(200)), d.revision, {Tsql.Bit(planning)}, {Tsql.Bit(calendar)}, {Tsql.Bit(skipped)}, {Tsql.Bit(retired)}, {Tsql.Bit(repairs)}, "
            + $"{Tsql.Micros("SYSUTCDATETIME()", next)} FROM (SELECT 1 AS one) AS single LEFT JOIN {definitions} AS d ON d.[key] = {definition}";
    }
}
