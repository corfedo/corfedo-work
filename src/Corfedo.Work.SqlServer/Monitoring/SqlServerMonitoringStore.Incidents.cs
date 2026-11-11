using System.Globalization;
using Corfedo.Work.SqlServer.Leases;

namespace Corfedo.Work.SqlServer.Monitoring;

internal sealed partial class SqlServerMonitoringStore
{
    /// <summary>
    /// Finds the definition's recurring-chain occurrences that started more than the limit ago through
    /// <c>jobs_owned_chain</c>, bounded by one clock reading, and locks them through their key without waiting, so a
    /// completion committing meanwhile waits and then resolves the incident; a row an ending holds is left to the next
    /// check. Opens one incident per key naming its earliest occurrence. A first statement, without locks, finds the
    /// earliest start among occurrences still within the limit.
    /// </summary>
    public async ValueTask<RunningCheck> CheckRunningAsync(string definition, TimeSpan limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        var (opened, next) = await InTransactionAsync(async work =>
        {
            const string over = "[definition] = @definition AND owned_chain IS NOT NULL AND state = 0 AND attempt_started_at <= @bound";
            await using var command = Tsql.Command(work, $"""
                DECLARE @now datetime2(7) = SYSUTCDATETIME();
                DECLARE @bound datetime2(7) = {Tsql.AddTicks("@now", "-@limit")};
                SELECT {Tsql.AddTicks("MIN(r.attempt_started_at)", "@limit")} FROM {jobs} AS r WITH (INDEX(jobs_owned_chain))
                WHERE r.[definition] = @definition AND r.owned_chain IS NOT NULL AND r.state = 0 AND r.attempt_started_at > @bound;
                DECLARE @over nvarchar(max) = N'';
                {Tsql.LockEach("over_scan", "2147483647", $"SELECT r.id FROM {jobs} AS r WITH (INDEX(jobs_owned_chain)) WHERE r.{over.Replace(" AND ", " AND r.", StringComparison.Ordinal)}",
                    jobs, Tsql.IdKey, "r." + over.Replace(" AND ", " AND r.", StringComparison.Ordinal), "@over")}
                DECLARE @first nvarchar(max) = (SELECT f.id, f.subject_kind, f.subject_id FROM (
                    SELECT r.id, r.subject_kind, r.subject_id, ROW_NUMBER() OVER (PARTITION BY r.subject_kind, r.subject_id ORDER BY r.attempt_started_at) AS rn
                    FROM {Tsql.KeyRows("@over", Tsql.IdKey)} AS o INNER LOOP JOIN {jobs} AS r WITH (FORCESEEK) ON r.id = o.id) AS f
                WHERE f.rn = 1 ORDER BY f.subject_kind, f.subject_id FOR JSON PATH);
                DECLARE @job uniqueidentifier, @kind nvarchar(100), @subject nvarchar(400);
                {Tsql.EachRow("first", "@first", [("@job", "uniqueidentifier", "id"), ("@kind", "nvarchar(100)", "subject_kind"), ("@subject", "nvarchar(400)", "subject_id")],
                    OpenIncident("id, kind, [definition], subject_kind, subject_id, job, opened_at, due_at",
                        $"VALUES (NEWID(), {(int)WorkIncidentKind.RunsAtMost}, @definition, @kind, @subject, @job, SYSUTCDATETIME(), SYSUTCDATETIME())"))}
                """,
                Tsql.Text("@definition", definition, 200),
                Tsql.Ticks("@limit", limit));
            await using var reader = await command.ReaderAsync(cancellationToken);
            await reader.NextRowAsync(cancellationToken);
            var next = Tsql.NullableTime(reader, 0);
            var opened = await reader.NextSetAsync(cancellationToken) ? await ReadIncidentSetsAsync(reader, cancellationToken) : [];
            await reader.DisposeAsync();
            if (opened.Count > 0)
            {
                await NotifyAsync(work.Connection, work.Transaction, cancellationToken);
            }
            return (opened, next);
        }, cancellationToken);
        WorkTelemetry.Count(new IncidentChanges(opened, []));
        return new RunningCheck(opened, next);
    }

    /// <summary>
    /// Locks the job in shared mode, waiting for a renewal or an ending that holds it, then reads, while the lease is
    /// valid and its attempt open, the stored running limit with it and, once the attempt reached it, opens the
    /// incident. The remaining time is computed with the store's clock read after the lock.
    /// </summary>
    public async ValueTask<TimeSpan?> CheckAttemptAsync(Lease<JobId> lease, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        var sql = await leases.SqlAsync(lease.Source, cancellationToken);
        var (remaining, opened) = await InTransactionAsync(async work =>
        {
            await using var command = SqlServerLeaseStore.KeyCommand(work.Connection, work.Transaction, sql, lease.Source, lease.Key, $"""
                DECLARE @held int;
                SELECT @held = 1 FROM {sql.Table} AS r WITH (REPEATABLEREAD, ROWLOCK) WHERE {sql.KeyMatch};
                DECLARE @a_id uniqueidentifier, @a_definition nvarchar(200), @a_kind nvarchar(100), @a_subject nvarchar(400), @a_remaining bigint;
                SELECT @a_id = r.id, @a_definition = r.[definition], @a_kind = r.subject_kind, @a_subject = r.subject_id,
                    @a_remaining = {Tsql.Micros("SYSUTCDATETIME()", Tsql.AddTicks("r.attempt_started_at", "d.expect_running"))}
                FROM {sql.Table} AS r JOIN {definitions} AS d ON d.[key] = r.[definition]
                WHERE {sql.KeyMatch} AND {sql.Valid} AND r.attempt_started_at IS NOT NULL AND d.expect_running IS NOT NULL;
                SELECT @a_remaining WHERE @a_id IS NOT NULL;
                IF @a_remaining <= 0
                BEGIN
                    {OpenIncident("id, kind, [definition], subject_kind, subject_id, job, opened_at, due_at",
                        $"VALUES (NEWID(), {(int)WorkIncidentKind.RunsAtMost}, @a_definition, @a_kind, @a_subject, @a_id, SYSUTCDATETIME(), SYSUTCDATETIME())")}
                END
                """,
                Tsql.Guid("@owner", lease.Owner.Value),
                Tsql.Int64("@epoch", lease.Epoch));
            TimeSpan? left = null;
            List<WorkIncident> opened = [];
            await using (var reader = await command.ReaderAsync(cancellationToken))
            {
                if (await reader.NextRowAsync(cancellationToken))
                {
                    var micros = reader.GetInt64(0);
                    if (micros > 0)
                    {
                        left = Tsql.FromMicros(micros);
                    }
                    else if (await reader.NextSetAsync(cancellationToken))
                    {
                        opened = await ReadIncidentsAsync(reader, cancellationToken);
                    }
                }
            }
            if (opened.Count > 0)
            {
                await NotifyAsync(work.Connection, work.Transaction, cancellationToken);
            }
            return (left, opened);
        }, cancellationToken);
        WorkTelemetry.Count(new IncidentChanges(opened, []));
        return remaining;
    }

    /// <summary>Resolves the open incidents of the removed kinds; resets failures of rows it can lock without waiting.</summary>
    public async ValueTask<IncidentChanges> ResolveRemovedAsync(string definition, IReadOnlyCollection<WorkIncidentKind> kinds, bool resetFailures, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(kinds);
        var list = kinds.Count == 0 ? "NULL" : string.Join(", ", kinds.Select(k => ((short)k).ToString(CultureInfo.InvariantCulture)));
        var resolved = await InTransactionAsync(async work =>
        {
            await using var command = Tsql.Command(work, $"""
                {ResolveIncidents($"i.[definition] = @definition AND i.kind IN ({list})")}
                {(resetFailures ? $"""
                    DECLARE @failed nvarchar(max) = N'';
                    {Tsql.LockEach("failed_scan", "2147483647", $"SELECT s.[definition], s.subject_kind, s.subject_id FROM {states} AS s WHERE s.[definition] = @definition AND s.failures > 0",
                        states, StateKey, "r.failures > 0", "@failed", nullable: true)}
                    UPDATE s SET failures = 0 FROM {Tsql.KeyRows("@failed", StateKey)} AS f INNER LOOP JOIN {states} AS s WITH (FORCESEEK) ON s.[definition] = f.[definition] AND {SameKey("s", "f")};
                    """ : "")}
                """,
                Tsql.Text("@definition", definition, 200));
            List<WorkIncident> resolved;
            await using (var reader = await command.ReaderAsync(cancellationToken))
            {
                resolved = await ReadIncidentsAsync(reader, cancellationToken);
            }
            if (resolved.Count > 0)
            {
                await NotifyAsync(work.Connection, work.Transaction, cancellationToken);
            }
            return resolved;
        }, cancellationToken);
        var changes = resolved.Count == 0 ? IncidentChanges.None : new IncidentChanges([], resolved);
        WorkTelemetry.Count(changes);
        return changes;
    }

    /// <summary>
    /// Resolves the open date-set incidents no longer wanted, including those of sets no longer in the report, then
    /// opens the wanted ones that are not open.
    /// </summary>
    public async ValueTask<IncidentChanges> SyncDateSetsAsync(IReadOnlyCollection<string> runningOut, IReadOnlyCollection<string> waiting, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(runningOut);
        ArgumentNullException.ThrowIfNull(waiting);
        const int RunningOut = (int)WorkIncidentKind.DateSetRunningOut;
        const int Waiting = (int)WorkIncidentKind.DateSetWorkWaiting;
        var want = $"(SELECT {RunningOut} AS kind, value AS date_set FROM OPENJSON(@running_out) WITH (value nvarchar(128) COLLATE {Tsql.Binary} '$') "
            + $"UNION ALL SELECT {Waiting}, value FROM OPENJSON(@waiting) WITH (value nvarchar(128) COLLATE {Tsql.Binary} '$'))";
        var changes = await InTransactionAsync(async work =>
        {
            await using var command = Tsql.Command(work, $"""
                {ResolveIncidents($"i.[definition] IS NULL AND i.subject_kind IS NULL AND i.kind IN ({RunningOut}, {Waiting}) "
                    + $"AND NOT EXISTS (SELECT 1 FROM {want} AS w WHERE w.kind = i.kind AND w.date_set = i.date_set)")}
                DECLARE @want nvarchar(max) = (SELECT w.kind, w.date_set FROM {want} AS w FOR JSON PATH), @kind smallint, @set nvarchar(128);
                {Tsql.EachRow("want", "@want", [("@kind", "smallint", "kind"), ("@set", "nvarchar(128)", "date_set")], $"""
                    IF NOT EXISTS (SELECT 1 FROM {incidents} AS i WHERE i.[definition] IS NULL AND i.subject_kind IS NULL AND i.subject_id IS NULL AND i.kind = @kind AND i.date_set = @set AND i.resolved_at IS NULL)
                    BEGIN
                        {OpenIncident("id, kind, date_set, opened_at, due_at", "VALUES (NEWID(), @kind, @set, SYSUTCDATETIME(), SYSUTCDATETIME())")}
                    END
                    """)}
                """,
                Tsql.Strings("@running_out", runningOut),
                Tsql.Strings("@waiting", waiting));
            List<WorkIncident> opened;
            List<WorkIncident> resolved;
            await using (var reader = await command.ReaderAsync(cancellationToken))
            {
                resolved = await ReadIncidentsAsync(reader, cancellationToken);
                opened = await reader.NextSetAsync(cancellationToken) ? await ReadIncidentSetsAsync(reader, cancellationToken) : [];
            }
            var changes = new IncidentChanges(opened, resolved);
            if (changes.Any)
            {
                await NotifyAsync(work.Connection, work.Transaction, cancellationToken);
            }
            return changes;
        }, cancellationToken);
        WorkTelemetry.Count(changes);
        return changes;
    }

    public async ValueTask<IncidentChanges> SyncUnservedAsync(IReadOnlyCollection<string> definitions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definitions);
        const int Unserved = (int)WorkIncidentKind.Unserved;
        const string Want = $"OPENJSON(@definitions) WITH (value nvarchar(200) COLLATE {Tsql.Binary} '$')";
        var changes = await InTransactionAsync(async work =>
        {
            await using var command = Tsql.Command(work, $"""
                {ResolveIncidents($"i.kind = {Unserved} AND NOT EXISTS (SELECT 1 FROM {Want} AS w WHERE w.value = i.[definition])")}
                DECLARE @want nvarchar(max) = (SELECT w.value AS [definition] FROM {Want} AS w FOR JSON PATH), @definition nvarchar(200);
                {Tsql.EachRow("want", "@want", [("@definition", "nvarchar(200)", "definition")], $"""
                    IF NOT EXISTS (SELECT 1 FROM {incidents} AS i WHERE i.[definition] = @definition AND i.subject_kind IS NULL AND i.subject_id IS NULL AND i.kind = {Unserved} AND i.date_set IS NULL AND i.resolved_at IS NULL)
                    BEGIN
                        {OpenIncident("id, kind, [definition], opened_at, due_at", $"VALUES (NEWID(), {Unserved}, @definition, SYSUTCDATETIME(), SYSUTCDATETIME())")}
                    END
                    """)}
                """,
                Tsql.Strings("@definitions", definitions));
            List<WorkIncident> opened;
            List<WorkIncident> resolved;
            await using (var reader = await command.ReaderAsync(cancellationToken))
            {
                resolved = await ReadIncidentsAsync(reader, cancellationToken);
                opened = await reader.NextSetAsync(cancellationToken) ? await ReadIncidentSetsAsync(reader, cancellationToken) : [];
            }
            var changes = new IncidentChanges(opened, resolved);
            if (changes.Any)
            {
                await NotifyAsync(work.Connection, work.Transaction, cancellationToken);
            }
            return changes;
        }, cancellationToken);
        WorkTelemetry.Count(changes);
        return changes;
    }

    /// <summary>
    /// Deletes resolved incidents by their resolution time, expectation rows without a deadline or failures whose key
    /// has no open incident, and queued key changes of definitions that expect no completions, which only the
    /// monitoring loop deletes otherwise; each statement runs in a transaction of its own and locks its rows through
    /// their key without waiting.
    /// </summary>
    public async ValueTask<int> RetainAsync(TimeSpan retention, int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        DateTimeOffset now;
        await using (var connection = await leases.OpenAsync(cancellationToken))
        await using (var clock = Tsql.Command(connection, null, "SELECT SYSUTCDATETIME();"))
        await using (var reader = await clock.ReaderAsync(cancellationToken))
        {
            await reader.NextRowAsync(cancellationToken);
            now = Tsql.Time(reader, 0);
        }
        var bound = retention.Ticks >= (now - DateTimeOffset.MinValue).Ticks ? DateTimeOffset.MinValue : now - retention;
        var resolved = await DeleteAsync($"""
            DECLARE @doomed nvarchar(max) = N'';
            {Tsql.LockEach("resolved_scan", "@limit",
                $"SELECT i.id FROM {incidents} AS i WITH (INDEX(work_incidents_retention)) WHERE i.resolved_at IS NOT NULL AND i.resolved_at < @bound ORDER BY i.resolved_at",
                incidents, Tsql.IdKey, "r.resolved_at < @bound", "@doomed")}
            DELETE i FROM {Tsql.KeyRows("@doomed", Tsql.IdKey)} AS d INNER LOOP JOIN {incidents} AS i WITH (FORCESEEK) ON i.id = d.id;
            SELECT @@ROWCOUNT;
            """, Tsql.Time("@bound", bound));
        var idle = $"deadline_at IS NULL AND failures = 0 AND NOT EXISTS (SELECT 1 FROM {incidents} AS i WHERE i.[definition] = x.[definition] AND i.resolved_at IS NULL AND {SameKey("i", "x")})";
        var states = await DeleteAsync($"""
            DECLARE @doomed nvarchar(max) = N'';
            {Tsql.LockEach("idle_scan", "@limit",
                $"SELECT x.[definition], x.subject_kind, x.subject_id FROM {this.states} AS x WITH (INDEX(job_expectation_states_idle)) WHERE x.{idle}",
                this.states, StateKey, "r." + idle.Replace("x.", "r.", StringComparison.Ordinal), "@doomed", nullable: true)}
            DELETE s FROM {Tsql.KeyRows("@doomed", StateKey)} AS d INNER LOOP JOIN {this.states} AS s WITH (FORCESEEK) ON s.[definition] = d.[definition] AND {SameKey("s", "d")};
            SELECT @@ROWCOUNT;
            """);
        var queued = await DeleteAsync($"""
            DECLARE @doomed nvarchar(max) = N'';
            {Tsql.LockEach("queued_scan", "@limit",
                $"SELECT q.n FROM {keyChanges} AS q JOIN {definitions} AS d ON d.[key] = q.[definition] WHERE d.expect_time IS NULL",
                keyChanges, ChangeKey, "1 = 1", "@doomed")}
            DELETE c FROM {Tsql.KeyRows("@doomed", ChangeKey)} AS d INNER LOOP JOIN {keyChanges} AS c WITH (FORCESEEK) ON c.n = d.n;
            SELECT @@ROWCOUNT;
            """);
        return Math.Max(resolved, Math.Max(states, queued));

        async Task<int> DeleteAsync(string text, params Microsoft.Data.SqlClient.SqlParameter[] parameters) =>
            await SqlServerLeaseStore.RetryDeadlocksAsync(async () =>
            {
                await using var connection = await leases.OpenAsync(cancellationToken);
                await using var command = Tsql.Command(connection, null, Tsql.OwnTransaction(text),
                    [.. parameters.Select(p => (Microsoft.Data.SqlClient.SqlParameter)((ICloneable)p).Clone()), Tsql.Int32("@limit", limit)]);
                return Convert.ToInt32(await command.ScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
            });
    }
}
