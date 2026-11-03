using System.Text.Json;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Processes;

/// <summary>
/// The process registry on the framework table <c>processes</c>. Every time is the store's clock: writes use
/// <c>SYSUTCDATETIME()</c>, reads judge staleness with one clock reading per statement.
/// </summary>
internal sealed class SqlServerProcessStore(string connectionString, string schema) : IProcessStore
{
    private readonly string processes = Tsql.Quote(schema) + ".processes";

    public async ValueTask RegisterAsync(ProcessRegistration registration, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(registration);
        await using var connection = await Tsql.OpenAsync(connectionString, cancellationToken);
        // A recorded owner is left as it is; a concurrent registration of the same owner meets the key.
        await using var command = Tsql.Command(connection, null, Tsql.IgnoreUnique(
            $"INSERT INTO {processes} (owner, type, release, stale_after, started_at, heartbeat_at) "
            + "SELECT @owner, @type, @release, @stale_after, SYSUTCDATETIME(), SYSUTCDATETIME() "
            + $"WHERE NOT EXISTS (SELECT 1 FROM {processes} WHERE owner = @owner);"),
            Tsql.Guid("@owner", registration.Owner.Value),
            Tsql.Text("@type", registration.Type, 200),
            Tsql.Text("@release", registration.Release, 200),
            Tsql.Ticks("@stale_after", registration.StaleAfter));
        await command.NonQueryAsync(cancellationToken);
    }

    public async ValueTask<bool> HeartbeatAsync(OwnerId owner, CancellationToken cancellationToken)
    {
        await using var connection = await Tsql.OpenAsync(connectionString, cancellationToken);
        await using var command = Tsql.Command(connection, null,
            $"UPDATE {processes} SET heartbeat_at = SYSUTCDATETIME() WHERE owner = @owner; SELECT @@ROWCOUNT;",
            Tsql.Guid("@owner", owner.Value));
        return (int)(await command.ScalarAsync(cancellationToken))! > 0;
    }

    public async ValueTask StopAsync(OwnerId owner, CancellationToken cancellationToken)
    {
        await using var connection = await Tsql.OpenAsync(connectionString, cancellationToken);
        await using var command = Tsql.Command(connection, null,
            $"UPDATE {processes} SET stopped_at = SYSUTCDATETIME() WHERE owner = @owner AND stopped_at IS NULL;",
            Tsql.Guid("@owner", owner.Value));
        await command.NonQueryAsync(cancellationToken);
    }

    public async ValueTask<IReadOnlyList<ProcessInstance>> ListAsync(string? type, IReadOnlyCollection<ProcessState> states, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(states);
        await using var connection = await Tsql.OpenAsync(connectionString, cancellationToken);
        await using var command = Tsql.Command(connection, null, $"""
            DECLARE @now datetime2(7) = SYSUTCDATETIME();
            SELECT owner, type, release, state, started_at, heartbeat_at, stopped_at FROM (
                SELECT p.owner, p.type, p.release, p.started_at, p.heartbeat_at, p.stopped_at,
                    {State("p", "@now")} AS state
                FROM {processes} AS p WHERE @type IS NULL OR p.type = @type) AS p
            WHERE state IN (SELECT CAST(value AS int) FROM OPENJSON(@states)) ORDER BY type, started_at, owner;
            """,
            Tsql.Text("@type", type, 200),
            Tsql.Json("@states", JsonSerializer.Serialize(states.Select(s => (int)s).ToArray(), SqlServerJson.Default.Int32Array)));
        var list = new List<ProcessInstance>();
        await using var reader = await command.ReaderAsync(cancellationToken);
        while (await reader.NextRowAsync(cancellationToken))
        {
            list.Add(ReadInstance(reader, 0));
        }
        return list;
    }

    public async ValueTask<int> RetainAsync(TimeSpan retention, int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        await using var connection = await Tsql.OpenAsync(connectionString, cancellationToken);
        // Rows are locked without waiting, so concurrent starts never wait for each other; deleting a process deletes
        // its definitions and calendars.
        await using var command = Tsql.Command(connection, null, Tsql.OwnTransaction($"""
            DECLARE @cutoff datetime2(7) = {Tsql.AddTicks("SYSUTCDATETIME()", "-@retention")};
            DECLARE @expired nvarchar(max) = (SELECT TOP (@limit) r.owner FROM {processes} AS r WITH (UPDLOCK, ROWLOCK, READPAST)
                WHERE r.stopped_at < @cutoff OR (r.stopped_at IS NULL AND {Tsql.AddTicks("r.heartbeat_at", "r.stale_after")} < @cutoff)
                ORDER BY r.owner FOR JSON PATH);
            DELETE p FROM OPENJSON(@expired) WITH (owner uniqueidentifier '$.owner') AS e INNER LOOP JOIN {processes} AS p WITH (FORCESEEK) ON p.owner = e.owner;
            DECLARE @deleted int = @@ROWCOUNT;
            """) + "SELECT @deleted;",
            Tsql.Ticks("@retention", retention),
            Tsql.Int32("@limit", limit));
        return (int)(await command.ScalarAsync(cancellationToken))!;
    }

    /// <summary>The <see cref="ProcessState"/> of the process row <paramref name="alias"/> at <paramref name="now"/>.</summary>
    internal static string State(string alias, string now) =>
        $"CASE WHEN {alias}.stopped_at IS NOT NULL THEN {(int)ProcessState.Stopped} "
        + $"WHEN {Tsql.AddTicks(alias + ".heartbeat_at", alias + ".stale_after")} > {now} THEN {(int)ProcessState.Live} ELSE {(int)ProcessState.Stale} END";

    /// <summary>
    /// Reads a process from <paramref name="first"/> on: owner, type, release, state as <see cref="ProcessState"/>,
    /// started, heartbeat and stopped times.
    /// </summary>
    internal static ProcessInstance ReadInstance(SqlDataReader reader, int first) => new(
        new OwnerId(reader.GetGuid(first)),
        reader.GetString(first + 1),
        reader.IsDBNull(first + 2) ? null : reader.GetString(first + 2),
        (ProcessState)reader.GetInt32(first + 3),
        Tsql.Time(reader, first + 4),
        Tsql.Time(reader, first + 5),
        Tsql.NullableTime(reader, first + 6));
}
