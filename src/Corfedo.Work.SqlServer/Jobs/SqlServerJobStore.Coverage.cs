using System.Buffers;
using System.Text;
using System.Text.Json;
using Corfedo.Work.SqlServer.Processes;

namespace Corfedo.Work.SqlServer.Jobs;

/// <summary>
/// The job definitions of registered processes, in <c>process_definitions</c> with the accepted argument versions in
/// <c>process_definition_args</c>, and the reports of which definitions live processes cover. A process is live while
/// it has not stopped and its heartbeat plus its own stale window is after the report's clock reading, taken once per
/// report.
/// </summary>
internal sealed partial class SqlServerJobStore
{
    private string Processes => Tsql.Quote(schema) + ".processes";

    private string ProcessDefinitions => Tsql.Quote(schema) + ".process_definitions";

    private string ProcessDefinitionArgs => Tsql.Quote(schema) + ".process_definition_args";

    private string Jobs => $"{Tsql.Quote(Table.Schema)}.{Tsql.Quote(Table.Name)}";

    /// <summary>Live processes <c>p</c> at <paramref name="now"/>.</summary>
    private static string Live(string now) => $"p.stopped_at IS NULL AND {Tsql.AddTicks("p.heartbeat_at", "p.stale_after")} > {now}";

    /// <summary>
    /// Records the process's definitions with their accepted argument versions in one transaction; definitions already
    /// recorded for the owner are left as they are.
    /// </summary>
    public async ValueTask RegisterProcessAsync(OwnerId owner, IReadOnlyList<ProcessDefinition> definitions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definitions);
        if (definitions.Count == 0)
        {
            return;
        }
        await using var connection = await Tsql.OpenAsync(connectionString, cancellationToken);
        await using var command = Tsql.Command(connection, null, Tsql.OwnTransaction($"""
            DECLARE @new nvarchar(max) = (SELECT d.definition FROM {DefinitionRows} AS d
                WHERE NOT EXISTS (SELECT 1 FROM {ProcessDefinitions} AS e WHERE e.owner = @owner AND e.definition = d.definition) FOR JSON PATH);
            INSERT INTO {ProcessDefinitions} (owner, definition, revision, runs, declares_recurrence)
            SELECT @owner, d.definition, d.revision, d.runs, d.declares FROM {DefinitionRows} AS d
            WHERE d.definition IN (SELECT n.definition FROM OPENJSON(@new) WITH (definition nvarchar(200) COLLATE {Tsql.Binary} '$.definition') AS n);
            INSERT INTO {ProcessDefinitionArgs} (owner, definition, args_version)
            SELECT DISTINCT @owner, d.definition, CAST(a.value AS int) FROM {DefinitionRows} AS d CROSS APPLY OPENJSON(d.accepted) AS a
            WHERE d.definition IN (SELECT n.definition FROM OPENJSON(@new) WITH (definition nvarchar(200) COLLATE {Tsql.Binary} '$.definition') AS n);
            """),
            Tsql.Guid("@owner", owner.Value),
            Tsql.Json("@definitions", DefinitionsJson(definitions)));
        await command.NonQueryAsync(cancellationToken);
    }

    /// <summary>The definitions of <see cref="DefinitionsJson"/> in <c>@definitions</c>, as rows.</summary>
    private const string DefinitionRows = $"OPENJSON(@definitions) WITH (definition nvarchar(200) COLLATE {Tsql.Binary} '$.d', revision bigint '$.r', runs bit '$.u', declares bit '$.c', accepted nvarchar(max) '$.a' AS JSON)";

    private static string DefinitionsJson(IReadOnlyList<ProcessDefinition> definitions)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            foreach (var definition in definitions)
            {
                writer.WriteStartObject();
                writer.WriteString("d", definition.Definition);
                writer.WriteNumber("r", definition.Revision);
                writer.WriteBoolean("u", definition.Runs);
                writer.WriteBoolean("c", definition.DeclaresRecurrence);
                writer.WriteStartArray("a");
                foreach (var version in definition.AcceptedArgs)
                {
                    writer.WriteNumberValue(version);
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    /// <summary>Whether no stored pause holds the subject of row <paramref name="alias"/>; the copied flags may be stale while no process repairs them.</summary>
    private string Unpaused(string alias) =>
        $"NOT EXISTS (SELECT 1 FROM {pauses} AS s WHERE s.[definition] = {alias}.[definition] AND s.subject_kind = {alias}.subject_kind AND s.subject_id = {alias}.subject_id)";

    /// <summary>A live process runs definition <paramref name="definition"/>, accepting <paramref name="version"/> when given.</summary>
    private string LiveRunner(string definition, string? version = null) =>
        $"EXISTS (SELECT 1 FROM {ProcessDefinitions} AS pd JOIN {Processes} AS p ON p.owner = pd.owner "
        + (version is null ? "" : $"JOIN {ProcessDefinitionArgs} AS a ON a.owner = pd.owner AND a.[definition] = pd.[definition] AND a.args_version = {version} ")
        + $"WHERE pd.[definition] = {definition} AND pd.runs = 1 AND {Live("@now")})";

    /// <summary>
    /// One batch with one clock reading. The argument versions of unheld jobs of every unpaused definition are found by a
    /// loose scan of <c>jobs_due</c>, one <c>TOP (1)</c> index probe per version and definition (a correlated
    /// <c>MIN</c> aggregates the whole index instead, looking up every row to recheck its filter), and joined with the
    /// versions of expired leases from <c>jobs_expiry</c>; a version has due work when a probe of that version finds an unheld job
    /// due now or an expired lease whose subject is not paused. A definition no live process runs is unserved when one
    /// of its versions has due work, a job's calendar instant needs planning, or a recurrence needs its planner (a due
    /// next instant, of an after-completion schedule only without an outstanding occurrence, or a waiting occurrence
    /// without one). Only when none holds does it look at its flagged due jobs and recurrences: their subjects, read in
    /// subject order from <c>jobs_paused</c> and <c>job_recurrences_paused</c>, minus the subjects in
    /// <c>job_pauses</c>, so a flag no stored pause backs does not hide work that no maintenance repairs. For a
    /// definition live processes run, a version with due work that no live runner accepts and that is not below the
    /// stored minimum is unserved; a flag left stale on its jobs is repaired by the runners' maintenance.
    /// </summary>
    public async ValueTask<UnservedReport> GetUnservedAsync(CancellationToken cancellationToken)
    {
        const string unheld = "j.lease_expires_at IS NULL AND j.due_at IS NOT NULL AND j.paused = 0";
        var outstanding = $"EXISTS (SELECT 1 FROM {Jobs} AS o WHERE o.recurrence_id IS NOT NULL AND o.recurrence_id = c.id AND o.state = 0 AND o.skip_pending = 0)";
        var recurrenceDue = $"((c.next_at <= @now AND (c.interval_kind IS NULL OR c.interval_kind <> {(short)Interval.AfterCompletion} OR NOT {outstanding})) "
            + $"OR (c.waiting_at IS NOT NULL AND NOT {outstanding}))";
        var pausedSubjects = $"SELECT s.subject_kind, s.subject_id FROM {pauses} AS s WHERE s.[definition] = d.[key]";
        const string DueVersions = $"OPENJSON(@due) WITH ([definition] nvarchar(200) COLLATE {Tsql.Binary} '$.definition', version int '$.version')";
        await using var connection = await Tsql.OpenAsync(connectionString, cancellationToken);
        await using var command = Tsql.Command(connection, null, $"""
            DECLARE @now datetime2(7) = SYSUTCDATETIME(), @versions nvarchar(max) = N'', @level nvarchar(max);
            SET @level = (SELECT d.[key] AS [definition], m.version FROM {definitions} AS d
                CROSS APPLY (SELECT TOP (1) j.args_version AS version FROM {Jobs} AS j WITH (INDEX(jobs_due)) WHERE j.[definition] = d.[key] AND {unheld} ORDER BY j.args_version) AS m
                WHERE d.paused = 0 AND m.version IS NOT NULL FOR JSON PATH);
            WHILE @level IS NOT NULL
            BEGIN
                SET @versions = @versions + N',' + SUBSTRING(@level, 2, LEN(@level) - 2);
                SET @level = (SELECT l.[definition], m.version FROM OPENJSON(@level) WITH ([definition] nvarchar(200) COLLATE {Tsql.Binary} '$.definition', version int '$.version') AS l
                    CROSS APPLY (SELECT TOP (1) j.args_version AS version FROM {Jobs} AS j WITH (INDEX(jobs_due)) WHERE j.[definition] = l.[definition] AND j.args_version > l.version AND {unheld} ORDER BY j.args_version) AS m
                    WHERE m.version IS NOT NULL FOR JSON PATH);
            END
            DECLARE @due nvarchar(max) = (SELECT c.[definition], c.version FROM (
                SELECT [definition], version FROM OPENJSON(N'[' + STUFF(@versions, 1, 1, N'') + N']') WITH ([definition] nvarchar(200) COLLATE {Tsql.Binary} '$.definition', version int '$.version')
                UNION
                SELECT j.[definition], j.args_version FROM {definitions} AS d JOIN {Jobs} AS j WITH (INDEX(jobs_expiry))
                ON j.[definition] = d.[key] AND j.lease_expires_at IS NOT NULL AND j.lease_expires_at < @now WHERE d.paused = 0) AS c
            WHERE EXISTS (SELECT 1 FROM {Jobs} AS j WITH (INDEX(jobs_due)) WHERE j.[definition] = c.[definition] AND j.args_version = c.version AND {unheld} AND j.due_at <= @now AND {Unpaused("j")})
            OR EXISTS (SELECT 1 FROM {Jobs} AS j WITH (INDEX(jobs_expiry)) WHERE j.[definition] = c.[definition] AND j.args_version = c.version AND j.lease_expires_at IS NOT NULL
                AND j.lease_expires_at < @now AND {Unpaused("j")})
            FOR JSON PATH);
            SELECT d.[key], CAST(NULL AS int) FROM {definitions} AS d WHERE d.paused = 0 AND NOT {LiveRunner("d.[key]")} AND (
                EXISTS (SELECT 1 FROM {DueVersions} AS due WHERE due.[definition] = d.[key])
                OR EXISTS (SELECT 1 FROM {Jobs} AS j WITH (INDEX(jobs_calendar_due)) WHERE j.[definition] = d.[key] AND j.calendar_at IS NOT NULL AND j.calendar_at <= @now AND j.state = 0
                    AND j.calendar_unavailable = 0 AND j.lease_expires_at IS NULL)
                OR EXISTS (SELECT 1 FROM {recurrences} AS c WHERE c.[definition] = d.[key] AND c.paused = 0 AND {Unpaused("c")} AND {recurrenceDue})
                OR EXISTS (SELECT j.subject_kind, j.subject_id FROM {Jobs} AS j WITH (INDEX(jobs_paused)) WHERE j.[definition] = d.[key] AND j.paused = 1 AND j.state = 0
                    AND j.lease_expires_at IS NULL AND j.due_at <= @now EXCEPT {pausedSubjects})
                OR EXISTS (SELECT 1 FROM (SELECT r.subject_kind, r.subject_id FROM {recurrences} AS r WITH (INDEX(job_recurrences_paused)) WHERE r.[definition] = d.[key] AND r.paused = 1
                    AND (r.next_at <= @now OR r.waiting_at IS NOT NULL) EXCEPT {pausedSubjects}) AS x
                    JOIN {recurrences} AS c ON c.[definition] = d.[key] AND c.subject_kind = x.subject_kind AND c.subject_id = x.subject_id AND c.paused = 1
                    WHERE {recurrenceDue}))
            UNION ALL
            SELECT due.[definition], due.version FROM {DueVersions} AS due JOIN {definitions} AS d ON d.[key] = due.[definition]
            WHERE due.version >= d.minimum_args_version AND {LiveRunner("due.[definition]")} AND NOT {LiveRunner("due.[definition]", "due.version")}
            ORDER BY 1, 2;
            """);
        var unrun = new List<UnservedDefinition>();
        var versions = new List<UnservedVersion>();
        await using var reader = await command.ReaderAsync(cancellationToken);
        while (await reader.NextRowAsync(cancellationToken))
        {
            if (reader.IsDBNull(1))
            {
                unrun.Add(new UnservedDefinition(reader.GetString(0)));
            }
            else
            {
                versions.Add(new UnservedVersion(reader.GetString(0), reader.GetInt32(1)));
            }
        }
        return new UnservedReport(unrun, versions);
    }

    /// <summary>
    /// Code-defined recurrences, found per stored definition by their unique identity, that were stored by a declaring
    /// revision and whose definition no live process declares.
    /// </summary>
    public async ValueTask<IReadOnlyList<UndeclaredRecurrence>> GetUndeclaredRecurrencesAsync(CancellationToken cancellationToken)
    {
        await using var connection = await Tsql.OpenAsync(connectionString, cancellationToken);
        await using var command = Tsql.Command(connection, null, $"""
            DECLARE @now datetime2(7) = SYSUTCDATETIME();
            SELECT c.definition, c.declared_revision, c.next_at FROM {definitions} AS d
            JOIN {recurrences} AS c ON c.definition = d.[key] AND c.merge_key = @identity
            WHERE c.declared_revision IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM {ProcessDefinitions} AS pd JOIN {Processes} AS p ON p.owner = pd.owner
                WHERE pd.definition = c.definition AND pd.declares_recurrence = 1 AND {Live("@now")})
            ORDER BY c.definition;
            """,
            Tsql.Text("@identity", JobRequestData.DefinitionIdentity, 512));
        var list = new List<UndeclaredRecurrence>();
        await using var reader = await command.ReaderAsync(cancellationToken);
        while (await reader.NextRowAsync(cancellationToken))
        {
            list.Add(new UndeclaredRecurrence(reader.GetString(0), reader.GetInt64(1), Tsql.NullableTime(reader, 2)));
        }
        return list;
    }

    public async ValueTask<IReadOnlyList<OutdatedProcess>> GetOutdatedProcessesAsync(CancellationToken cancellationToken)
    {
        await using var connection = await Tsql.OpenAsync(connectionString, cancellationToken);
        await using var command = Tsql.Command(connection, null, $"""
            DECLARE @now datetime2(7) = SYSUTCDATETIME();
            SELECT p.owner, p.type, p.release, {(int)ProcessState.Live}, p.started_at, p.heartbeat_at, p.stopped_at, pd.definition, pd.revision, d.revision
            FROM {Processes} AS p JOIN {ProcessDefinitions} AS pd ON pd.owner = p.owner JOIN {definitions} AS d ON d.[key] = pd.definition
            WHERE {Live("@now")} AND pd.revision < d.revision
            ORDER BY pd.definition, p.started_at, p.owner;
            """);
        var list = new List<OutdatedProcess>();
        await using var reader = await command.ReaderAsync(cancellationToken);
        while (await reader.NextRowAsync(cancellationToken))
        {
            list.Add(new OutdatedProcess(SqlServerProcessStore.ReadInstance(reader, 0), reader.GetString(7), reader.GetInt64(8), reader.GetInt64(9)));
        }
        return list;
    }
}
