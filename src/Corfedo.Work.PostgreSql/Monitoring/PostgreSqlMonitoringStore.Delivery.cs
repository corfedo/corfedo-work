using Npgsql;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Monitoring;

internal sealed partial class PostgreSqlMonitoringStore
{
    /// <summary>The lease of <c>$1</c> held by owner <c>$2</c> at epoch <c>$3</c> is valid by a clock reading taken now.</summary>
    private const string HeldLease = "i.id = $1 AND i.lease_owner = $2 AND i.lease_epoch = $3 AND i.lease_expires_at > clock_timestamp()";

    public async ValueTask<IncidentDelivery?> ReadDeliveryAsync(Lease<Guid> lease, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        await using var command = dataSource.CreateCommand(
            $"SELECT {IncidentColumns}, i.opened_delivered_at IS NOT NULL, i.resolved_delivered_at IS NOT NULL FROM {incidents} AS i WHERE {HeldLease}");
        AddLease(command.Parameters, lease);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? new IncidentDelivery(ReadIncident(reader, 0), reader.GetBoolean(9), reader.GetBoolean(10)) : null;
    }

    public async ValueTask<bool> RecordDeliveryAsync(Lease<Guid> lease, bool resolution, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        await using var command = dataSource.CreateCommand(
            resolution
                ? $"UPDATE {incidents} AS i SET resolved_delivered_at = clock_timestamp() WHERE {HeldLease} AND i.resolved_at IS NOT NULL"
                : $"UPDATE {incidents} AS i SET opened_delivered_at = clock_timestamp() WHERE {HeldLease}");
        AddLease(command.Parameters, lease);
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    /// <summary>
    /// The update waits for a resolution committing meanwhile and then sees it, so a resolution is never left
    /// undelivered and not due.
    /// </summary>
    public async ValueTask<bool> FinishDeliveryAsync(Lease<Guid> lease, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        await using var command = dataSource.CreateCommand(
            $"UPDATE {incidents} AS i SET lease_expires_at = NULL, takeovers = 0, failures = 0, "
            + "due_at = CASE WHEN i.opened_delivered_at IS NULL OR (i.resolved_at IS NOT NULL AND i.resolved_delivered_at IS NULL) THEN clock_timestamp() END "
            + $"WHERE {HeldLease}");
        AddLease(command.Parameters, lease);
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    private static void AddLease(NpgsqlParameterCollection parameters, Lease<Guid> lease)
    {
        parameters.Add(Uuid(lease.Key));
        parameters.Add(Uuid(lease.Owner.Value));
        parameters.Add(Int64(lease.Epoch));
    }

    /// <summary>
    /// Counts open incidents by kind on the open-incident index, and finds each definition's oldest due job with one
    /// probe of the due index per argument version, walking the versions with a loose index scan.
    /// </summary>
    public async ValueTask<MonitoringGauges> GetGaugesAsync(CancellationToken cancellationToken)
    {
        const string Due = "j.lease_expires_at IS NULL AND j.due_at IS NOT NULL AND NOT j.paused";
        await using var batch = dataSource.CreateBatch();
        batch.BatchCommands.Add(Command($"SELECT i.kind, count(*) FROM {incidents} AS i WHERE i.resolved_at IS NULL GROUP BY i.kind"));
        batch.BatchCommands.Add(Command(
            $"SELECT d.key, extract(epoch FROM clock_timestamp() - o.due_at)::float8 FROM {definitions} AS d CROSS JOIN LATERAL ("
            + "WITH RECURSIVE v(version) AS ("
            + $"SELECT (SELECT min(j.args_version) FROM {jobs} AS j WHERE j.definition = d.key AND {Due}) "
            + $"UNION ALL SELECT (SELECT min(j.args_version) FROM {jobs} AS j WHERE j.definition = d.key AND j.args_version > v.version AND {Due}) FROM v WHERE v.version IS NOT NULL) "
            + $"SELECT min((SELECT j.due_at FROM {jobs} AS j WHERE j.definition = d.key AND j.args_version = v.version AND {Due} ORDER BY j.due_at LIMIT 1)) AS due_at "
            + "FROM v WHERE v.version IS NOT NULL) AS o"));
        await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
        var open = new Dictionary<WorkIncidentKind, long>();
        while (await reader.ReadAsync(cancellationToken))
        {
            open[(WorkIncidentKind)reader.GetInt16(0)] = reader.GetInt64(1);
        }
        await reader.NextResultAsync(cancellationToken);
        var due = new Dictionary<string, TimeSpan>(StringComparer.Ordinal);
        while (await reader.ReadAsync(cancellationToken))
        {
            due[reader.GetString(0)] = reader.IsDBNull(1) ? TimeSpan.Zero : Seconds(reader.GetDouble(1));
        }
        return new MonitoringGauges(open, due);
    }
}
