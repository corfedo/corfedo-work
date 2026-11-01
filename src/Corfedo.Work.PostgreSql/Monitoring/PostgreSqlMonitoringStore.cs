using Corfedo.Work.PostgreSql.Leases;
using Npgsql;
using NpgsqlTypes;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Monitoring;

/// <summary>
/// Expectation state and incidents on the framework tables <c>job_expectation_states</c> and <c>work_incidents</c>.
/// A key is a definition with a subject, or alone with a null subject; statements match a null subject with
/// <c>IS NULL</c> so both forms use the key indexes.
/// </summary>
internal sealed partial class PostgreSqlMonitoringStore(NpgsqlDataSource dataSource, PostgreSqlLeaseStore leases, string schema) : IMonitoringStore
{
    /// <summary>The columns <see cref="ReadIncident"/> reads, of an incident aliased <c>i</c>.</summary>
    private const string IncidentColumns = "i.id, i.kind, i.definition, i.subject_kind, i.subject_id, i.date_set, i.job, i.opened_at, i.resolved_at";

    /// <summary>Whether a definition aliased <c>d</c> has any expectation.</summary>
    internal const string HasExpectations = "(d.expect_time IS NOT NULL OR d.expect_running IS NOT NULL OR d.expect_failures IS NOT NULL)";

    private readonly string states = LeaseSql.Quote(schema) + ".job_expectation_states";
    private readonly string incidents = LeaseSql.Quote(schema) + ".work_incidents";
    private readonly string definitions = LeaseSql.Quote(schema) + ".job_definitions";
    private readonly string recurrences = LeaseSql.Quote(schema) + ".job_recurrences";
    private readonly string keyChanges = LeaseSql.Quote(schema) + ".job_expectation_changes";
    private readonly string jobs = LeaseSql.Quote(schema) + ".jobs";
    private readonly string processes = LeaseSql.Quote(schema) + ".processes";

    public StoreLocation Incidents { get; } = StoreLocation.Table(schema, "work_incidents");

    public async ValueTask<StoredExpectations> GetExpectationsAsync(CancellationToken cancellationToken)
    {
        await using var batch = dataSource.CreateBatch();
        batch.BatchCommands.Add(Command("SELECT clock_timestamp()"));
        batch.BatchCommands.Add(Command(
            "SELECT d.key, d.revision, d.paused, d.expect_time, d.expect_zone, d.expect_calendar, d.expect_running, d.expect_failures, "
            + $"d.expect_time IS NOT NULL AND EXISTS (SELECT 1 FROM {recurrences} AS r WHERE r.definition = d.key), "
            + "d.monitored_revision IS NOT DISTINCT FROM d.revision AND d.monitored_paused IS NOT DISTINCT FROM d.paused, "
            + $"EXISTS (SELECT 1 FROM {keyChanges} AS c WHERE c.definition = d.key) FROM {definitions} AS d ORDER BY d.key"));
        await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        var now = reader.GetFieldValue<DateTimeOffset>(0);
        await reader.NextResultAsync(cancellationToken);
        var list = new List<StoredExpectation>();
        while (await reader.ReadAsync(cancellationToken))
        {
            list.Add(new StoredExpectation(reader.GetString(0), reader.GetInt64(1), Jobs.PostgreSqlJobStore.ReadExpectation(reader, 3), reader.GetBoolean(2), reader.GetBoolean(8), reader.GetBoolean(9), reader.GetBoolean(10)));
        }
        return new StoredExpectations(now, list);
    }

    /// <summary>
    /// Reads one page newest first in the order of the opening index, continuing after the cursor; one more incident
    /// than the limit is read to tell whether a next page exists.
    /// </summary>
    public async ValueTask<WorkIncidentPage> GetIncidentsAsync(bool openOnly, string? definition, int limit, WorkIncidentCursor? after, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var filters = new List<string>();
        await using var command = dataSource.CreateCommand();
        if (openOnly)
        {
            filters.Add("i.resolved_at IS NULL");
        }
        if (definition is not null)
        {
            command.Parameters.Add(Text(definition));
            filters.Add($"i.definition = ${command.Parameters.Count}");
        }
        if (after is not null)
        {
            command.Parameters.Add(Time(after.OpenedAt));
            command.Parameters.Add(Uuid(after.Id));
            filters.Add($"(i.opened_at, i.id) < (${command.Parameters.Count - 1}, ${command.Parameters.Count})");
        }
        command.Parameters.Add(Int32(limit + 1));
        command.CommandText = $"SELECT {IncidentColumns} FROM {incidents} AS i"
            + (filters.Count == 0 ? "" : " WHERE " + string.Join(" AND ", filters))
            + $" ORDER BY i.opened_at DESC, i.id DESC LIMIT ${command.Parameters.Count}";
        var list = new List<WorkIncident>(Math.Min(limit + 1, 1001));
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                list.Add(ReadIncident(reader, 0));
            }
        }
        if (list.Count <= limit)
        {
            return new WorkIncidentPage(list, null);
        }
        list.RemoveAt(limit);
        return new WorkIncidentPage(list, new WorkIncidentCursor(list[^1].OpenedAt, list[^1].Id));
    }

    public async ValueTask<IReadOnlyList<string>> GetUnmonitoredAsync(CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(
            $"SELECT d.key FROM {definitions} AS d WHERE {HasExpectations} "
            + $"AND NOT EXISTS (SELECT 1 FROM {processes} AS p WHERE p.monitors AND p.stopped_at IS NULL AND p.heartbeat_at + p.stale_after > now()) ORDER BY d.key");
        var keys = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            keys.Add(reader.GetString(0));
        }
        return keys;
    }

    public async ValueTask RegisterMonitorAsync(OwnerId owner, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand($"UPDATE {processes} SET monitors = true WHERE owner = $1 AND NOT monitors");
        command.Parameters.Add(Uuid(owner.Value));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>Reads an incident from <paramref name="first"/> on, in the order of <see cref="IncidentColumns"/>.</summary>
    private static WorkIncident ReadIncident(NpgsqlDataReader reader, int first) => new(
        reader.GetGuid(first),
        (WorkIncidentKind)reader.GetInt16(first + 1),
        reader.IsDBNull(first + 2) ? null : reader.GetString(first + 2),
        reader.IsDBNull(first + 3) ? null : Subject.Of(reader.GetString(first + 3), reader.GetString(first + 4)),
        reader.IsDBNull(first + 5) ? null : reader.GetString(first + 5),
        reader.IsDBNull(first + 6) ? null : new JobId(reader.GetGuid(first + 6)),
        reader.GetFieldValue<DateTimeOffset>(first + 7),
        reader.IsDBNull(first + 8) ? null : reader.GetFieldValue<DateTimeOffset>(first + 8));

    private static async Task<List<WorkIncident>> ReadIncidentsAsync(NpgsqlDataReader reader, CancellationToken cancellationToken)
    {
        var list = new List<WorkIncident>();
        while (await reader.ReadAsync(cancellationToken))
        {
            list.Add(ReadIncident(reader, 0));
        }
        return list;
    }

    /// <summary>The key's match on <paramref name="alias"/>: its subject from parameters <paramref name="kind"/> and the next, or no subject.</summary>
    private static string KeyMatch(string alias, Subject? subject, int kind) =>
        subject is null ? $"{alias}.subject_kind IS NULL" : $"{alias}.subject_kind = ${kind} AND {alias}.subject_id = ${kind + 1}";

    /// <summary>Adds the subject's kind and identifier when it has one.</summary>
    private static void AddSubject(NpgsqlParameterCollection parameters, Subject? subject)
    {
        if (subject is not null)
        {
            parameters.Add(Text(subject.Kind));
            parameters.Add(Text(subject.Id));
        }
    }

    private static NpgsqlParameter<DateTimeOffset> Time(DateTimeOffset value) => new() { TypedValue = value.ToUniversalTime(), NpgsqlDbType = NpgsqlDbType.TimestampTz };

    private static NpgsqlParameter NullableTime(DateTimeOffset? value) =>
        new() { Value = value is { } v ? v.ToUniversalTime() : DBNull.Value, NpgsqlDbType = NpgsqlDbType.TimestampTz };

    /// <summary>
    /// A deadline as a parameter: null for <see cref="ExpectationWindow.Uncovered"/>, which statements store as
    /// <c>'infinity'</c> with <c>coalesce</c>, and read back as null with <c>NULLIF</c>.
    /// </summary>
    private static DateTimeOffset? Deadline(DateTimeOffset value) => value == ExpectationWindow.Uncovered ? null : value;

    /// <summary>A deadline read with <c>NULLIF(deadline_at, 'infinity')</c>.</summary>
    private static DateTimeOffset ReadDeadline(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? ExpectationWindow.Uncovered : reader.GetFieldValue<DateTimeOffset>(ordinal);

    private static NpgsqlParameter<DateTimeOffset?[]> Times(IEnumerable<DateTimeOffset?> values) =>
        new() { TypedValue = [.. values.Select(v => v?.ToUniversalTime())], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.TimestampTz };

    private static NpgsqlParameter<string?[]> Texts(IEnumerable<string?> values) =>
        new() { TypedValue = [.. values], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text };

    /// <summary>Signals claimers of the incident source when the transaction commits.</summary>
    internal Task NotifyAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken) =>
        leases.NotifyAsync(connection, transaction, WorkTopics.Due, IncidentSource.Name, cancellationToken);
}
