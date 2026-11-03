using System.Globalization;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Monitoring;

internal sealed partial class SqlServerMonitoringStore
{
    /// <summary>The lease of <c>@id</c> held by <c>@owner</c> at <c>@epoch</c> is valid by a clock reading taken in the statement.</summary>
    private const string HeldLease = "i.id = @id AND i.lease_owner = @owner AND i.lease_epoch = @epoch AND i.lease_expires_at > SYSUTCDATETIME()";

    /// <summary>Locks the incident through its key, waiting, so the next statement evaluates the lease after the lock.</summary>
    private string LockIncident => $"DECLARE @incident_held int;\nSELECT @incident_held = 1 FROM {incidents} WITH (UPDLOCK, ROWLOCK) WHERE id = @id;\n";

    public async ValueTask<IncidentDelivery?> ReadDeliveryAsync(Lease<Guid> lease, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        await using var connection = await leases.OpenAsync(cancellationToken);
        await using var command = Tsql.Command(connection, null,
            $"SELECT {IncidentColumns}, {Tsql.Bit("i.opened_delivered_at IS NOT NULL")}, {Tsql.Bit("i.resolved_delivered_at IS NOT NULL")} FROM {incidents} AS i WHERE {HeldLease};",
            LeaseParameters(lease));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? new IncidentDelivery(ReadIncident(reader, 0), reader.GetBoolean(9), reader.GetBoolean(10)) : null;
    }

    public async ValueTask<bool> RecordDeliveryAsync(Lease<Guid> lease, bool resolution, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        return await UpdateHeldAsync(lease,
            resolution
                ? $"UPDATE i SET resolved_delivered_at = SYSUTCDATETIME() FROM {incidents} AS i WHERE {HeldLease} AND i.resolved_at IS NOT NULL;"
                : $"UPDATE i SET opened_delivered_at = SYSUTCDATETIME() FROM {incidents} AS i WHERE {HeldLease};",
            cancellationToken);
    }

    /// <summary>
    /// The lock waits for a resolution committing meanwhile, and the update then sees it, so a resolution is never left
    /// undelivered and not due.
    /// </summary>
    public async ValueTask<bool> FinishDeliveryAsync(Lease<Guid> lease, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        return await UpdateHeldAsync(lease,
            $"UPDATE i SET lease_expires_at = NULL, takeovers = 0, failures = 0, "
            + "due_at = CASE WHEN i.opened_delivered_at IS NULL OR (i.resolved_at IS NOT NULL AND i.resolved_delivered_at IS NULL) THEN SYSUTCDATETIME() END "
            + $"FROM {incidents} AS i WHERE {HeldLease};",
            cancellationToken);
    }

    /// <summary>Locks the incident, then runs <paramref name="update"/>, in a transaction of its own; true when it changed the row.</summary>
    private async Task<bool> UpdateHeldAsync(Lease<Guid> lease, string update, CancellationToken cancellationToken)
    {
        await using var connection = await leases.OpenAsync(cancellationToken);
        await using var command = Tsql.Command(connection, null, Tsql.OwnTransaction(LockIncident + update + "\nSELECT @@ROWCOUNT;"), LeaseParameters(lease));
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture) == 1;
    }

    private static SqlParameter[] LeaseParameters(Lease<Guid> lease) =>
        [Tsql.Guid("@id", lease.Key), Tsql.Guid("@owner", lease.Owner.Value), Tsql.Int64("@epoch", lease.Epoch)];

    /// <summary>
    /// Counts open incidents by kind on the open-incident index, and finds each definition's oldest due job with one
    /// probe of the due index per argument version, walking the versions with a loose index scan.
    /// </summary>
    public async ValueTask<MonitoringGauges> GetGaugesAsync(CancellationToken cancellationToken)
    {
        const string Due = "j.lease_expires_at IS NULL AND j.due_at IS NOT NULL AND j.paused = 0";
        await using var connection = await leases.OpenAsync(cancellationToken);
        await using var command = Tsql.Command(connection, null, $"""
            SELECT i.kind, COUNT_BIG(*) FROM {incidents} AS i WHERE i.resolved_at IS NULL GROUP BY i.kind;
            DECLARE @level TABLE ([definition] nvarchar(200) COLLATE {Tsql.Binary} NOT NULL PRIMARY KEY, version int NOT NULL);
            DECLARE @next TABLE ([definition] nvarchar(200) COLLATE {Tsql.Binary} NOT NULL PRIMARY KEY, version int NOT NULL);
            DECLARE @oldest TABLE ([definition] nvarchar(200) COLLATE {Tsql.Binary} NOT NULL, due_at datetime2(7) NOT NULL);
            INSERT INTO @level ([definition], version)
            SELECT d.[key], m.version FROM {definitions} AS d
            CROSS APPLY (SELECT MIN(j.args_version) AS version FROM {jobs} AS j WITH (INDEX(jobs_due)) WHERE j.[definition] = d.[key] AND {Due}) AS m
            WHERE m.version IS NOT NULL;
            WHILE EXISTS (SELECT 1 FROM @level)
            BEGIN
                INSERT INTO @oldest ([definition], due_at)
                SELECT l.[definition], o.due_at FROM @level AS l
                CROSS APPLY (SELECT MIN(j.due_at) AS due_at FROM {jobs} AS j WITH (INDEX(jobs_due)) WHERE j.[definition] = l.[definition] AND j.args_version = l.version AND {Due}) AS o
                WHERE o.due_at IS NOT NULL;
                DELETE FROM @next;
                INSERT INTO @next ([definition], version)
                SELECT l.[definition], m.version FROM @level AS l
                CROSS APPLY (SELECT MIN(j.args_version) AS version FROM {jobs} AS j WITH (INDEX(jobs_due)) WHERE j.[definition] = l.[definition] AND j.args_version > l.version AND {Due}) AS m
                WHERE m.version IS NOT NULL;
                DELETE FROM @level;
                INSERT INTO @level ([definition], version) SELECT [definition], version FROM @next;
            END
            SELECT d.[key], {Tsql.Micros("o.due_at", "SYSUTCDATETIME()")} FROM {definitions} AS d
            LEFT JOIN (SELECT [definition], MIN(due_at) AS due_at FROM @oldest GROUP BY [definition]) AS o ON o.[definition] = d.[key];
            """);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var open = new Dictionary<WorkIncidentKind, long>();
        while (await reader.ReadAsync(cancellationToken))
        {
            open[(WorkIncidentKind)reader.GetInt16(0)] = reader.GetInt64(1);
        }
        await reader.NextResultAsync(cancellationToken);
        var due = new Dictionary<string, TimeSpan>(StringComparer.Ordinal);
        while (await reader.ReadAsync(cancellationToken))
        {
            due[reader.GetString(0)] = reader.IsDBNull(1) ? TimeSpan.Zero : Tsql.FromMicros(reader.GetInt64(1));
        }
        return new MonitoringGauges(open, due);
    }
}
