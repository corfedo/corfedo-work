using Corfedo.Work.PostgreSql.Leases;
using Npgsql;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Processes;

/// <summary>
/// The process registry on the framework table <c>processes</c>. Every time is the store's clock: writes use
/// <c>clock_timestamp()</c>, reads judge staleness with the statement's <c>now()</c>.
/// </summary>
internal sealed class PostgreSqlProcessStore(NpgsqlDataSource dataSource, string schema) : IProcessStore
{
    private readonly string processes = LeaseSql.Quote(schema) + ".processes";

    public async ValueTask RegisterAsync(ProcessRegistration registration, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(registration);
        await using var command = dataSource.CreateCommand(
            $"INSERT INTO {processes} (owner, type, release, stale_after, started_at, heartbeat_at) "
            + "VALUES ($1, $2, $3, $4, clock_timestamp(), clock_timestamp()) ON CONFLICT (owner) DO NOTHING");
        command.Parameters.Add(Uuid(registration.Owner.Value));
        command.Parameters.Add(Text(registration.Type));
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)registration.Release ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.Add(Interval(registration.StaleAfter));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async ValueTask<bool> HeartbeatAsync(OwnerId owner, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand($"UPDATE {processes} SET heartbeat_at = clock_timestamp() WHERE owner = $1");
        command.Parameters.Add(Uuid(owner.Value));
        return await command.ExecuteNonQueryAsync(cancellationToken) > 0;
    }

    public async ValueTask StopAsync(OwnerId owner, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand($"UPDATE {processes} SET stopped_at = clock_timestamp() WHERE owner = $1 AND stopped_at IS NULL");
        command.Parameters.Add(Uuid(owner.Value));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async ValueTask<IReadOnlyList<ProcessInstance>> ListAsync(string? type, IReadOnlyCollection<ProcessState> states, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(states);
        await using var command = dataSource.CreateCommand(
            "SELECT owner, type, release, state, started_at, heartbeat_at, stopped_at FROM ("
            + "SELECT p.*, CASE WHEN p.stopped_at IS NOT NULL THEN 2 WHEN p.heartbeat_at + p.stale_after > now() THEN 0 ELSE 1 END AS state "
            + $"FROM {processes} AS p WHERE $1::text IS NULL OR p.type = $1) AS p "
            + "WHERE state = ANY($2) ORDER BY type, started_at, owner");
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)type ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = [.. states.Select(s => (int)s)] });
        var list = new List<ProcessInstance>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            list.Add(ReadInstance(reader, 0));
        }
        return list;
    }

    public async ValueTask<int> RetainAsync(TimeSpan retention, int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        await using var command = dataSource.CreateCommand(
            $"DELETE FROM {processes} AS p USING (SELECT r.owner FROM {processes} AS r "
            + "WHERE r.stopped_at < now() - $1 OR (r.stopped_at IS NULL AND r.heartbeat_at + r.stale_after < now() - $1) "
            + "ORDER BY r.owner LIMIT $2 FOR UPDATE SKIP LOCKED) AS v WHERE p.owner = v.owner");
        command.Parameters.Add(Interval(retention));
        command.Parameters.Add(Int32(limit));
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Reads a process from <paramref name="first"/> on: owner, type, release, state as <see cref="ProcessState"/>,
    /// started, heartbeat and stopped times.
    /// </summary>
    internal static ProcessInstance ReadInstance(NpgsqlDataReader reader, int first) => new(
        new OwnerId(reader.GetGuid(first)),
        reader.GetString(first + 1),
        reader.IsDBNull(first + 2) ? null : reader.GetString(first + 2),
        (ProcessState)reader.GetInt32(first + 3),
        reader.GetFieldValue<DateTimeOffset>(first + 4),
        reader.GetFieldValue<DateTimeOffset>(first + 5),
        reader.IsDBNull(first + 6) ? null : reader.GetFieldValue<DateTimeOffset>(first + 6));
}
