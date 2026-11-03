using System.Buffers;
using System.Text;
using System.Text.Json;
using Corfedo.Work.SqlServer.Processes;

namespace Corfedo.Work.SqlServer.Jobs;

/// <summary>
/// The job definitions of registered processes, in <c>process_definitions</c> with the accepted argument versions in
/// <c>process_definition_args</c>, and the reports of which definitions live processes cover. A process is live while
/// it has not stopped and its heartbeat plus its own stale window is after the statement's clock reading; each report
/// is one statement, so it reads one snapshot at one instant.
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
            DECLARE @d TABLE (definition nvarchar(200) COLLATE {Tsql.Binary} NOT NULL PRIMARY KEY, revision bigint NOT NULL, runs bit NOT NULL, declares bit NOT NULL, accepted nvarchar(max) NOT NULL);
            INSERT INTO @d (definition, revision, runs, declares, accepted)
            SELECT definition, revision, runs, declares, accepted FROM OPENJSON(@definitions)
            WITH (definition nvarchar(200) '$.d', revision bigint '$.r', runs bit '$.u', declares bit '$.c', accepted nvarchar(max) '$.a' AS JSON);
            DECLARE @new TABLE (definition nvarchar(200) COLLATE {Tsql.Binary} NOT NULL PRIMARY KEY);
            INSERT INTO {ProcessDefinitions} (owner, definition, revision, runs, declares_recurrence)
            OUTPUT inserted.definition INTO @new
            SELECT @owner, d.definition, d.revision, d.runs, d.declares FROM @d AS d
            WHERE NOT EXISTS (SELECT 1 FROM {ProcessDefinitions} AS e WHERE e.owner = @owner AND e.definition = d.definition);
            INSERT INTO {ProcessDefinitionArgs} (owner, definition, args_version)
            SELECT DISTINCT @owner, d.definition, CAST(a.value AS int) FROM @d AS d JOIN @new AS n ON n.definition = d.definition CROSS APPLY OPENJSON(d.accepted) AS a;
            """),
            Tsql.Guid("@owner", owner.Value),
            Tsql.Json("@definitions", DefinitionsJson(definitions)));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

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
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
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
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            list.Add(new OutdatedProcess(SqlServerProcessStore.ReadInstance(reader, 0), reader.GetString(7), reader.GetInt64(8), reader.GetInt64(9)));
        }
        return list;
    }
}
