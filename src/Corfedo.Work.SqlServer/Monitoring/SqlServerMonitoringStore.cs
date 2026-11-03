using Corfedo.Work.SqlServer.Jobs;
using Corfedo.Work.SqlServer.Leases;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Monitoring;

/// <summary>
/// Expectation state and incidents on the framework tables <c>job_expectation_states</c> and <c>work_incidents</c>.
/// A key is a definition with a subject, or alone with a null subject; statements match a null subject with
/// <c>IS NULL</c> so both forms use the key indexes.
/// </summary>
internal sealed partial class SqlServerMonitoringStore(SqlServerLeaseStore leases, string schema) : IMonitoringStore
{
    /// <summary>The columns <see cref="ReadIncident"/> reads, of an incident aliased <c>i</c>.</summary>
    private const string IncidentColumns = "i.id, i.kind, i.[definition], i.subject_kind, i.subject_id, i.date_set, i.job, i.opened_at, i.resolved_at";

    /// <summary>The <c>OUTPUT</c> list of <see cref="IncidentColumns"/>.</summary>
    private const string IncidentOutput =
        "inserted.id, inserted.kind, inserted.[definition], inserted.subject_kind, inserted.subject_id, inserted.date_set, inserted.job, inserted.opened_at, inserted.resolved_at";

    /// <summary>Whether a definition aliased <c>d</c> has any expectation.</summary>
    internal const string HasExpectations = "(d.expect_time IS NOT NULL OR d.expect_running IS NOT NULL OR d.expect_failures IS NOT NULL)";

    private readonly string states = Tsql.Quote(schema) + ".job_expectation_states";
    private readonly string incidents = Tsql.Quote(schema) + ".work_incidents";
    private readonly string definitions = Tsql.Quote(schema) + ".job_definitions";
    private readonly string recurrences = Tsql.Quote(schema) + ".job_recurrences";
    private readonly string keyChanges = Tsql.Quote(schema) + ".job_expectation_changes";
    private readonly string jobs = Tsql.Quote(schema) + ".jobs";
    private readonly string processes = Tsql.Quote(schema) + ".processes";

    public StoreLocation Incidents { get; } = StoreLocation.Table(schema, "work_incidents");

    public async ValueTask<StoredExpectations> GetExpectationsAsync(CancellationToken cancellationToken)
    {
        await using var connection = await leases.OpenAsync(cancellationToken);
        await using var command = Tsql.Command(connection, null, $"""
            SELECT SYSUTCDATETIME();
            SELECT d.[key], d.revision, d.paused, d.expect_time, d.expect_zone, d.expect_calendar, d.expect_running, d.expect_failures,
                {Tsql.Bit($"d.expect_time IS NOT NULL AND EXISTS (SELECT 1 FROM {recurrences} AS r WHERE r.[definition] = d.[key])")},
                {Tsql.Bit("d.monitored_revision = d.revision AND d.monitored_paused = d.paused")},
                {Tsql.Bit($"EXISTS (SELECT 1 FROM {keyChanges} AS c WHERE c.[definition] = d.[key])")}
            FROM {definitions} AS d ORDER BY d.[key];
            """);
        await using var reader = await command.ReaderAsync(cancellationToken);
        await reader.NextRowAsync(cancellationToken);
        var now = Tsql.Time(reader, 0);
        await reader.NextSetAsync(cancellationToken);
        var list = new List<StoredExpectation>();
        while (await reader.NextRowAsync(cancellationToken))
        {
            list.Add(new StoredExpectation(reader.GetString(0), reader.GetInt64(1), SqlServerJobStore.ReadExpectation(reader, 3), reader.GetBoolean(2), reader.GetBoolean(8), reader.GetBoolean(9), reader.GetBoolean(10)));
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
        var parameters = new List<SqlParameter> { Tsql.Int32("@limit", limit + 1) };
        if (openOnly)
        {
            filters.Add("i.resolved_at IS NULL");
        }
        if (definition is not null)
        {
            parameters.Add(Tsql.Text("@definition", definition, 200));
            filters.Add("i.[definition] = @definition");
        }
        if (after is not null)
        {
            parameters.Add(Tsql.Time("@after_opened", after.OpenedAt));
            parameters.Add(Tsql.Guid("@after_id", after.Id));
            filters.Add("(i.opened_at < @after_opened OR (i.opened_at = @after_opened AND i.id < @after_id))");
        }
        await using var connection = await leases.OpenAsync(cancellationToken);
        await using var command = Tsql.Command(connection, null,
            $"SELECT TOP (@limit) {IncidentColumns} FROM {incidents} AS i"
            + (filters.Count == 0 ? "" : " WHERE " + string.Join(" AND ", filters))
            + " ORDER BY i.opened_at DESC, i.id DESC;",
            [.. parameters]);
        var list = new List<WorkIncident>(Math.Min(limit + 1, 1001));
        await using (var reader = await command.ReaderAsync(cancellationToken))
        {
            while (await reader.NextRowAsync(cancellationToken))
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
        await using var connection = await leases.OpenAsync(cancellationToken);
        await using var command = Tsql.Command(connection, null, $"""
            DECLARE @now datetime2(7) = SYSUTCDATETIME();
            SELECT d.[key] FROM {definitions} AS d WHERE {HasExpectations}
            AND NOT EXISTS (SELECT 1 FROM {processes} AS p WHERE p.monitors = 1 AND p.stopped_at IS NULL AND {Tsql.AddTicks("p.heartbeat_at", "p.stale_after")} > @now)
            ORDER BY d.[key];
            """);
        var keys = new List<string>();
        await using var reader = await command.ReaderAsync(cancellationToken);
        while (await reader.NextRowAsync(cancellationToken))
        {
            keys.Add(reader.GetString(0));
        }
        return keys;
    }

    public async ValueTask RegisterMonitorAsync(OwnerId owner, CancellationToken cancellationToken)
    {
        await using var connection = await leases.OpenAsync(cancellationToken);
        await using var command = Tsql.Command(connection, null, $"UPDATE {processes} SET monitors = 1 WHERE owner = @owner AND monitors = 0;", Tsql.Guid("@owner", owner.Value));
        await command.NonQueryAsync(cancellationToken);
    }

    /// <summary>Reads an incident from <paramref name="first"/> on, in the order of <see cref="IncidentColumns"/>.</summary>
    private static WorkIncident ReadIncident(SqlDataReader reader, int first) => new(
        reader.GetGuid(first),
        (WorkIncidentKind)reader.GetInt16(first + 1),
        reader.IsDBNull(first + 2) ? null : reader.GetString(first + 2),
        reader.IsDBNull(first + 3) ? null : Subject.Of(reader.GetString(first + 3), reader.GetString(first + 4)),
        reader.IsDBNull(first + 5) ? null : reader.GetString(first + 5),
        reader.IsDBNull(first + 6) ? null : new JobId(reader.GetGuid(first + 6)),
        Tsql.Time(reader, first + 7),
        Tsql.NullableTime(reader, first + 8));

    private static async Task<List<WorkIncident>> ReadIncidentsAsync(SqlDataReader reader, CancellationToken cancellationToken)
    {
        var list = new List<WorkIncident>();
        while (await reader.NextRowAsync(cancellationToken))
        {
            list.Add(ReadIncident(reader, 0));
        }
        return list;
    }

    /// <summary>Reads the incidents of the current result set and of every result set after it.</summary>
    private static async Task<List<WorkIncident>> ReadIncidentSetsAsync(SqlDataReader reader, CancellationToken cancellationToken)
    {
        var list = new List<WorkIncident>();
        do
        {
            list.AddRange(await ReadIncidentsAsync(reader, cancellationToken));
        }
        while (await reader.NextSetAsync(cancellationToken));
        return list;
    }

    /// <summary>The key's match on <paramref name="alias"/>: its subject in <c>@kind</c> and <c>@subject</c>, or no subject.</summary>
    private static string KeyMatch(string alias, Subject? subject) =>
        subject is null ? $"{alias}.subject_kind IS NULL AND {alias}.subject_id IS NULL" : $"{alias}.subject_kind = @kind AND {alias}.subject_id = @subject";

    /// <summary>The parameters of <see cref="KeyMatch"/>.</summary>
    private static SqlParameter[] SubjectParameters(Subject? subject) =>
        [Tsql.Text("@kind", subject?.Kind, 100), Tsql.Text("@subject", subject?.Id, 400)];

    /// <summary>The key of <c>job_expectation_states</c>, whose subject columns are null for the definition-alone key.</summary>
    private static readonly (string Column, string Type)[] StateKey =
        [("[definition]", "nvarchar(200)"), ("subject_kind", "nvarchar(100)"), ("subject_id", "nvarchar(400)")];

    /// <summary>The key of <c>job_expectation_changes</c>.</summary>
    private static readonly (string Column, string Type)[] ChangeKey = [("n", "bigint")];

    /// <summary>The row of <paramref name="alias"/> has the subject of <paramref name="key"/>, a state row or a row of keys.</summary>
    private static string SameKey(string alias, string key) =>
        $"{alias}.subject_kind IS NOT DISTINCT FROM {key}.subject_kind AND {alias}.subject_id IS NOT DISTINCT FROM {key}.subject_id";

    /// <summary>
    /// Statements that resolve the open incidents (alias <c>i</c>) <paramref name="selection"/> selects and return them
    /// in one result set of <see cref="IncidentColumns"/>: they are found without locks, locked in primary-key order,
    /// waiting, and resolved at <paramref name="resolvedAt"/> when they still match after the locks are held.
    /// </summary>
    private string ResolveIncidents(string selection, string resolvedAt = "SYSUTCDATETIME()") => $"""
        DECLARE @open nvarchar(max) = (SELECT i.id FROM {incidents} AS i WHERE i.resolved_at IS NULL AND {selection} ORDER BY i.id FOR JSON PATH), @open_locked int;
        SELECT @open_locked = COUNT(*) FROM OPENJSON(@open) WITH (id uniqueidentifier '$.id') AS o INNER LOOP JOIN {incidents} AS i WITH (UPDLOCK, ROWLOCK, FORCESEEK) ON i.id = o.id;
        UPDATE i SET resolved_at = {resolvedAt}, due_at = COALESCE(i.due_at, SYSUTCDATETIME()) OUTPUT {IncidentOutput}
        FROM OPENJSON(@open) WITH (id uniqueidentifier '$.id') AS o INNER LOOP JOIN {incidents} AS i WITH (FORCESEEK) ON i.id = o.id WHERE i.resolved_at IS NULL AND {selection};

        """;

    /// <summary>
    /// A statement that opens an incident unless its key has one open of its kind and returns it in a result set of
    /// <see cref="IncidentColumns"/>, with the columns <paramref name="columns"/> and the <c>VALUES</c>
    /// <paramref name="values"/>. Run in a loop, it returns one result set per run; <see cref="ReadIncidentSetsAsync"/>
    /// reads them.
    /// </summary>
    private string OpenIncident(string columns, string values) =>
        Tsql.IgnoreUnique($"INSERT INTO {incidents} ({columns}) OUTPUT {IncidentOutput} {values};");

    /// <summary>
    /// Statements that run <paramref name="update"/> of one state row and, when it changed none, <paramref name="insert"/>;
    /// an insert that meets a row committed meanwhile runs the update again, which then waits for that row. The batch
    /// declares <c>@upsert_inserted bit</c>.
    /// </summary>
    private static string Upsert(string update, string insert) => $"""
        {update}
        IF @@ROWCOUNT = 0
        BEGIN
            SET @upsert_inserted = 0;
            {Tsql.IgnoreUnique(insert + "\nSET @upsert_inserted = 1;")}
            IF @upsert_inserted = 0 {update}
        END

        """;

    /// <summary>Signals claimers of the incident source when the transaction commits.</summary>
    internal Task NotifyAsync(SqlConnection connection, SqlTransaction transaction, CancellationToken cancellationToken) =>
        leases.NotifyAsync(connection, transaction, WorkTopics.Due, IncidentSource.Name, cancellationToken);
}
