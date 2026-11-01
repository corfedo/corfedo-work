using System.Globalization;
using Corfedo.Work.PostgreSql.Leases;
using Corfedo.Work.PostgreSql.Processes;
using Npgsql;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Jobs;

/// <summary>
/// The job definitions of registered processes, in <c>process_definitions</c>, and the reports of which definitions
/// live processes cover. A process is live while it has not stopped and its heartbeat plus its own stale window is
/// after the statement's <c>now()</c>; each report is one statement, so it reads one snapshot at one instant.
/// </summary>
internal sealed partial class PostgreSqlJobStore
{
    private string Processes => LeaseSql.Quote(schema) + ".processes";

    private string ProcessDefinitions => LeaseSql.Quote(schema) + ".process_definitions";

    private string Jobs => $"{LeaseSql.Quote(Table.Schema)}.{LeaseSql.Quote(Table.Name)}";

    /// <summary>The definitions of live processes as rows <c>definition, accepted_args, runs, declares_recurrence, revision, owner</c>.</summary>
    private string LiveDefinitions =>
        $"SELECT pd.definition, pd.accepted_args, pd.runs, pd.declares_recurrence, pd.revision, pd.owner FROM {ProcessDefinitions} AS pd "
        + $"JOIN {Processes} AS p ON p.owner = pd.owner WHERE p.stopped_at IS NULL AND p.heartbeat_at + p.stale_after > now()";

    /// <summary>Whether no stored pause holds the subject of row <paramref name="alias"/>; the copied flags may be stale while no process repairs them.</summary>
    private string Unpaused(string alias) =>
        $"NOT EXISTS (SELECT 1 FROM {pauses} AS s WHERE s.definition = {alias}.definition AND s.subject_kind = {alias}.subject_kind AND s.subject_id = {alias}.subject_id)";

    /// <summary>The subjects of definition <c>d</c> that a stored pause holds.</summary>
    private string PausedSubjects => $"SELECT s.subject_kind, s.subject_id FROM {pauses} AS s WHERE s.definition = d.key";

    public async ValueTask RegisterProcessAsync(OwnerId owner, IReadOnlyList<ProcessDefinition> definitions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definitions);
        if (definitions.Count == 0)
        {
            return;
        }
        await using var command = dataSource.CreateCommand(
            $"INSERT INTO {ProcessDefinitions} (owner, definition, revision, runs, accepted_args, declares_recurrence) "
            + "SELECT $1, d.definition, d.revision, d.runs, d.accepted::integer[], d.declares "
            + "FROM unnest($2::text[], $3::bigint[], $4::boolean[], $5::text[], $6::boolean[]) AS d(definition, revision, runs, accepted, declares) "
            + "ON CONFLICT (owner, definition) DO NOTHING");
        command.Parameters.Add(Uuid(owner.Value));
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = [.. definitions.Select(d => d.Definition)] });
        command.Parameters.Add(new NpgsqlParameter<long[]> { TypedValue = [.. definitions.Select(d => d.Revision)] });
        command.Parameters.Add(new NpgsqlParameter<bool[]> { TypedValue = [.. definitions.Select(d => d.Runs)] });
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = [.. definitions.Select(d => "{" + string.Join(',', d.AcceptedArgs.Select(v => v.ToString(CultureInfo.InvariantCulture))) + "}")] });
        command.Parameters.Add(new NpgsqlParameter<bool[]> { TypedValue = [.. definitions.Select(d => d.DeclaresRecurrence)] });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// One statement. The argument versions of unheld jobs of every unpaused definition are found by a loose scan of
    /// <c>jobs_due</c>, one index probe per version, and joined with the versions of expired leases from
    /// <c>jobs_expiry</c>; a version has due work when a probe of that version finds an unheld job due now or an expired
    /// lease whose subject is not paused. A definition no live process runs is unserved when one of its versions has due
    /// work, a job's calendar instant needs planning, or a recurrence needs its planner (a due next instant, of an
    /// after-completion schedule only without an outstanding occurrence, or a waiting occurrence without one). Only when
    /// none holds does it look at its flagged due jobs and recurrences: their subjects, read in subject order from
    /// <c>jobs_paused</c> and <c>job_recurrences_paused</c>, minus the subjects in <c>job_pauses</c>, so a flag no stored
    /// pause backs does not hide work that no maintenance repairs. For a definition live processes run, a version with due work that no live
    /// runner accepts and that is not below the stored minimum is unserved; a flag left stale on its jobs is repaired by
    /// the runners' maintenance.
    /// </summary>
    public async ValueTask<UnservedReport> GetUnservedAsync(CancellationToken cancellationToken)
    {
        var unheld = "j.lease_expires_at IS NULL AND j.due_at IS NOT NULL AND NOT j.paused";
        var outstanding = $"EXISTS (SELECT 1 FROM {Jobs} AS o WHERE o.recurrence_id = c.id AND o.state = 0 AND NOT o.skip_pending)";
        await using var batch = dataSource.CreateBatch();
        // The cost estimate charges every definition no live process runs with the scans of its flagged rows, which run
        // only for one without other due work; past jit_above_cost, compiling would take longer than the report runs.
        // The batch is one implicit transaction, so the setting ends with it.
        batch.BatchCommands.Add(Command("SELECT set_config('jit', 'off', true)"));
        batch.BatchCommands.Add(Command(
            $"WITH RECURSIVE live AS MATERIALIZED (SELECT l.definition, l.accepted_args FROM ({LiveDefinitions}) AS l WHERE l.runs), "
            + "versions (definition, version) AS ("
            + $"SELECT d.key, (SELECT min(j.args_version) FROM {Jobs} AS j WHERE j.definition = d.key AND {unheld}) FROM {definitions} AS d WHERE NOT d.paused "
            + "UNION ALL "
            + $"SELECT v.definition, (SELECT min(j.args_version) FROM {Jobs} AS j WHERE j.definition = v.definition AND j.args_version > v.version AND {unheld}) "
            + "FROM versions AS v WHERE v.version IS NOT NULL), "
            + "unrun AS MATERIALIZED ("
            + $"SELECT d.key FROM {definitions} AS d WHERE NOT d.paused AND NOT EXISTS (SELECT 1 FROM live WHERE live.definition = d.key)), "
            + "due (definition, version) AS MATERIALIZED (SELECT c.definition, c.version FROM ("
            + "SELECT v.definition, v.version FROM versions AS v WHERE v.version IS NOT NULL "
            + "UNION "
            + $"SELECT j.definition, j.args_version FROM {definitions} AS d JOIN {Jobs} AS j ON j.definition = d.key AND j.lease_expires_at < now() WHERE NOT d.paused) AS c "
            + $"WHERE EXISTS (SELECT 1 FROM {Jobs} AS j WHERE j.definition = c.definition AND j.args_version = c.version AND {unheld} AND j.due_at <= now() AND {Unpaused("j")}) "
            + $"OR EXISTS (SELECT 1 FROM {Jobs} AS j WHERE j.definition = c.definition AND j.args_version = c.version AND j.lease_expires_at < now() AND {Unpaused("j")})) "
            + "SELECT d.key, NULL::integer FROM unrun AS d WHERE ("
            + "EXISTS (SELECT 1 FROM due WHERE due.definition = d.key) "
            + $"OR EXISTS (SELECT 1 FROM {Jobs} AS j WHERE j.definition = d.key AND j.calendar_at <= now() AND j.state = 0 AND NOT j.calendar_unavailable AND j.lease_expires_at IS NULL) "
            + $"OR EXISTS (SELECT 1 FROM {recurrences} AS c WHERE c.definition = d.key AND NOT c.paused AND {Unpaused("c")} AND ("
            + $"(c.next_at <= now() AND (c.interval_kind IS DISTINCT FROM {(short)Interval.AfterCompletion} OR NOT {outstanding})) "
            + $"OR (c.waiting_at IS NOT NULL AND NOT {outstanding}))) "
            + $"OR EXISTS (SELECT j.subject_kind, j.subject_id FROM {Jobs} AS j WHERE j.definition = d.key AND j.paused AND j.state = 0 "
            + $"AND j.lease_expires_at IS NULL AND j.due_at <= now() EXCEPT {PausedSubjects}) "
            + $"OR EXISTS (SELECT 1 FROM (SELECT r.subject_kind, r.subject_id FROM {recurrences} AS r WHERE r.definition = d.key AND r.paused "
            + $"AND (r.next_at <= now() OR r.waiting_at IS NOT NULL) EXCEPT {PausedSubjects}) AS x "
            + $"JOIN {recurrences} AS c ON c.definition = d.key AND c.subject_kind = x.subject_kind AND c.subject_id = x.subject_id AND c.paused WHERE "
            + $"(c.next_at <= now() AND (c.interval_kind IS DISTINCT FROM {(short)Interval.AfterCompletion} OR NOT {outstanding})) "
            + $"OR (c.waiting_at IS NOT NULL AND NOT {outstanding}))) "
            + "UNION ALL "
            + $"SELECT due.definition, due.version FROM due JOIN {definitions} AS d ON d.key = due.definition "
            + "WHERE due.version >= d.minimum_args_version AND EXISTS (SELECT 1 FROM live WHERE live.definition = due.definition) "
            + "AND NOT EXISTS (SELECT 1 FROM live WHERE live.definition = due.definition AND due.version = ANY (live.accepted_args)) "
            + "ORDER BY 1, 2"));
        var unrun = new List<UnservedDefinition>();
        var versions = new List<UnservedVersion>();
        await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
        await reader.NextResultAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
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
    /// revision and whose definition is not among those live processes declare, which are read once.
    /// </summary>
    public async ValueTask<IReadOnlyList<UndeclaredRecurrence>> GetUndeclaredRecurrencesAsync(CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(
            $"WITH declared AS MATERIALIZED (SELECT DISTINCT l.definition FROM ({LiveDefinitions}) AS l WHERE l.declares_recurrence) "
            + $"SELECT c.definition, c.declared_revision, c.next_at FROM {definitions} AS d "
            + $"JOIN {recurrences} AS c ON c.definition = d.key AND c.merge_key = $1 "
            + "WHERE c.declared_revision IS NOT NULL AND NOT EXISTS (SELECT 1 FROM declared WHERE declared.definition = c.definition) "
            + "ORDER BY c.definition");
        command.Parameters.Add(Text(JobRequestData.DefinitionIdentity));
        var list = new List<UndeclaredRecurrence>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            list.Add(new UndeclaredRecurrence(reader.GetString(0), reader.GetInt64(1), reader.IsDBNull(2) ? null : reader.GetFieldValue<DateTimeOffset>(2)));
        }
        return list;
    }

    public async ValueTask<IReadOnlyList<OutdatedProcess>> GetOutdatedProcessesAsync(CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(
            $"SELECT p.owner, p.type, p.release, {(int)ProcessState.Live}, p.started_at, p.heartbeat_at, p.stopped_at, pd.definition, pd.revision, d.revision "
            + $"FROM {Processes} AS p JOIN {ProcessDefinitions} AS pd ON pd.owner = p.owner JOIN {definitions} AS d ON d.key = pd.definition "
            + "WHERE p.stopped_at IS NULL AND p.heartbeat_at + p.stale_after > now() AND pd.revision < d.revision "
            + "ORDER BY pd.definition, p.started_at, p.owner");
        var list = new List<OutdatedProcess>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            list.Add(new OutdatedProcess(PostgreSqlProcessStore.ReadInstance(reader, 0), reader.GetString(7), reader.GetInt64(8), reader.GetInt64(9)));
        }
        return list;
    }
}
