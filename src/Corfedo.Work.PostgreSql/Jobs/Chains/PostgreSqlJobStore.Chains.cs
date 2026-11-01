using Corfedo.Work.PostgreSql.Leases;
using Npgsql;
using NpgsqlTypes;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Jobs;

/// <summary>
/// Chains on the job table: every step is a job row from the request on, and <c>job_chains</c> and
/// <c>job_chain_groups</c> keep each chain's top level and groups with one pending count and the worst outcome among
/// their steps that ran. A step's terminal transition decides its children, settles empty groups that follow it and
/// decrements its enclosing group, in its own transaction. It holds its own row before the group row, so siblings
/// serialize on the group without deadlocking.
/// </summary>
internal sealed partial class PostgreSqlJobStore : IChainStore
{
    private readonly string chains = LeaseSql.Quote(schema) + ".job_chains";
    private readonly string chainGroups = LeaseSql.Quote(schema) + ".job_chain_groups";

    public async ValueTask RequestChainAsync(IWorkTransaction transaction, ChainData chain, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(chain);
        var work = PostgreSqlTransaction.From(transaction);
        await CheckCalendarsAsync(work, [chain.Steps[0].Request], cancellationToken);
        await InsertChainAsync(work, chain, null, null, cancellationToken);
    }

    /// <summary>
    /// Locks the occurrence and checks its lease, and that no cancellation was requested, after the lock is held, gives
    /// the lease up and leaves the occurrence
    /// outstanding without a due time, pointing at its chain, then creates the chain. The attempt stays open, held by the
    /// chain: the occurrence is running until its definition's maintenance ends it with the chain's outcome, which
    /// records the attempt in the run history.
    /// </summary>
    public async ValueTask<bool> StartOwnedChainAsync(Lease<JobId> lease, ChainData chain, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(chain);
        var sql = Sql(lease.Source);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        var work = new PostgreSqlTransaction(transaction);
        await using (var batch = new NpgsqlBatch(connection, transaction))
        {
            batch.BatchCommands.Add(KeyCommand($"SELECT 1 FROM {sql.Table} AS r WHERE {sql.KeyMatch(1)} FOR UPDATE OF r", lease.Source, lease.Key));
            batch.BatchCommands.Add(KeyCommand(
                $"UPDATE {sql.Table} AS r SET lease_expires_at = NULL, due_at = NULL, window_end = NULL, owned_chain = $4 "
                + $"WHERE {sql.KeyMatch(1)} AND r.state = 0 AND {sql.Valid(2, 3)} AND r.cancel_requested_at IS NULL",
                lease.Source,
                lease.Key,
                Uuid(lease.Owner.Value),
                Int64(lease.Epoch),
                Uuid(chain.Id.Value)));
            await batch.ExecuteNonQueryAsync(cancellationToken);
            if (batch.BatchCommands[1].RecordsAffected != 1)
            {
                return false;
            }
        }
        await InsertChainAsync(work, chain, lease.Key, sql.Job!.Definition, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    /// <summary>
    /// Inserts the chain, its groups and its steps, each group and the chain's top level with the number of its nodes
    /// pending, and signals the root's definition: claimers, or its planners when the root has a calendar.
    /// </summary>
    private async Task InsertChainAsync(PostgreSqlTransaction work, ChainData chain, JobId? owner, string? ownerDefinition, CancellationToken cancellationToken)
    {
        var root = chain.Steps[0];
        await using (var insert = new NpgsqlCommand(
            $"INSERT INTO {chains} (id, root_job, owner_job, owner_definition, definitions, created_at, pending) VALUES ($1, $2, $3, $4, $5, clock_timestamp(), $6)",
            work.Connection,
            work.Transaction))
        {
            insert.Parameters.Add(Uuid(chain.Id.Value));
            insert.Parameters.Add(Uuid(root.Id.Value));
            insert.Parameters.Add(new NpgsqlParameter { Value = owner?.Value ?? (object)DBNull.Value, NpgsqlDbType = NpgsqlDbType.Uuid });
            insert.Parameters.Add(new NpgsqlParameter { Value = ownerDefinition ?? (object)DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
            insert.Parameters.Add(Texts([.. chain.Steps.Select(s => s.Definition).Distinct(StringComparer.Ordinal)]));
            insert.Parameters.Add(Int32(Pending(chain.Steps, chain.Groups, null)));
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }
        await InsertGroupsAsync(work, chain.Id, 1, chain.Steps, chain.Groups, false, cancellationToken);
        await InsertStepsAsync(work, chain.Id, 1, chain.Steps, root, cancellationToken);
        var source = JobDefinition.SourceNameOf(root.Definition);
        await leases.NotifyAsync(work.Connection, work.Transaction, root.Request.Schedule.Calendar is null ? WorkTopics.Due : WorkTopics.Plan, source, cancellationToken);
    }

    /// <summary>The nodes, steps and groups, whose enclosing group is <paramref name="group"/>.</summary>
    private static int Pending(IReadOnlyList<ChainStepData> steps, IReadOnlyList<ChainGroupData> groups, Guid? group) =>
        steps.Count(s => s.Group == group) + groups.Count(g => g.Group == group);

    private async Task InsertGroupsAsync(PostgreSqlTransaction work, ChainId chain, long generation, IReadOnlyList<ChainStepData> steps, IReadOnlyList<ChainGroupData> groups, bool dynamic, CancellationToken cancellationToken)
    {
        if (groups.Count == 0)
        {
            return;
        }
        await using var insert = new NpgsqlCommand(
            $"INSERT INTO {chainGroups} (id, chain_id, generation, parent, enclosing_group, pending, dynamic) "
            + "SELECT u.id, $1, $2, u.parent, u.enclosing, u.pending, $7 FROM unnest($3::uuid[], $4::uuid[], $5::uuid[], $6::int4[]) AS u(id, parent, enclosing, pending)",
            work.Connection,
            work.Transaction);
        insert.Parameters.Add(Uuid(chain.Value));
        insert.Parameters.Add(Int64(generation));
        insert.Parameters.Add(Uuids([.. groups.Select(g => g.Id)]));
        insert.Parameters.Add(Uuids([.. groups.Select(g => g.Parent)]));
        insert.Parameters.Add(NullableUuids(groups.Select(g => g.Group)));
        insert.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = [.. groups.Select(g => Pending(steps, groups, g.Id))], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer });
        insert.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = dynamic });
        await insert.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Inserts the steps in one statement: <paramref name="root"/>, when given, due by its schedule, or with a calendar
    /// at its instant for its definition's planner; every other step waiting for its parent without a due time. The
    /// pause flag is copied from the stored subject pauses.
    /// </summary>
    private async Task InsertStepsAsync(PostgreSqlTransaction work, ChainId chain, long generation, IReadOnlyList<ChainStepData> steps, ChainStepData? root, CancellationToken cancellationToken)
    {
        var parameters = new List<NpgsqlParameter>
        {
            Uuid(chain.Value),
            Int64(generation),
            Uuids([.. steps.Select(s => s.Id.Value)]),
            Texts([.. steps.Select(s => s.Definition)]),
            NullableTexts(steps.Select(s => s.Request.Subject?.Kind)),
            NullableTexts(steps.Select(s => s.Request.Subject?.Id)),
            Texts([.. steps.Select(s => s.Request.Partition ?? "")]),
            new NpgsqlParameter<byte[][]> { TypedValue = [.. steps.Select(s => s.Request.Args)], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bytea },
            new NpgsqlParameter<int[]> { TypedValue = [.. steps.Select(s => s.Request.ArgsVersion)], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer },
            NullableUuids(steps.Select(s => s.Parent)),
            NullableUuids(steps.Select(s => s.Group)),
            new NpgsqlParameter<short[]> { TypedValue = [.. steps.Select(s => (short)s.Condition)], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Smallint },
            Uuid(root?.Id.Value ?? Guid.Empty),
            NullableTexts(steps.Select(s => s.Request.TraceParent)),
        };
        string Parameter(NpgsqlParameter parameter)
        {
            parameters.Add(parameter);
            return "$" + parameters.Count;
        }
        var (due, calendarAt, calendar, blocked, closed) = ("NULL::timestamptz", "NULL::timestamptz", "NULL::text", "NULL::int2", "NULL::int2");
        if (root is not null)
        {
            var schedule = root.Request.Schedule;
            var (instant, _) = Timing(schedule, Parameter);
            if (schedule.Calendar is null)
            {
                due = instant;
            }
            else
            {
                calendarAt = instant;
                calendar = Parameter(Text(schedule.Calendar));
                blocked = Parameter(new NpgsqlParameter<short> { TypedValue = (short)schedule.Blocked });
                closed = Parameter(new NpgsqlParameter<short> { TypedValue = (short)schedule.Closed });
            }
        }
        const string isRoot = "u.id = $13";
        await using var insert = new NpgsqlCommand(
            $"INSERT INTO {jobTable} (id, definition, subject_kind, subject_id, partition_key, args, args_version, first_requested_at, created_at, "
            + "due_at, calendar_at, calendar, calendar_blocked, calendar_closed, execution_id, paused, "
            + "chain_id, chain_generation, chain_parent, chain_group, chain_condition, chain_waiting, trace_parent) "
            + "SELECT u.id, u.definition, u.subject_kind, u.subject_id, u.partition_key, u.args, u.args_version, clock_timestamp(), clock_timestamp(), "
            + $"CASE WHEN {isRoot} THEN {due} END, CASE WHEN {isRoot} THEN {calendarAt} END, CASE WHEN {isRoot} THEN {calendar} END, "
            + $"CASE WHEN {isRoot} THEN {blocked} END, CASE WHEN {isRoot} THEN {closed} END, gen_random_uuid(), "
            + $"u.subject_kind IS NOT NULL AND {PausedCopy("u.definition", "u.subject_kind", "u.subject_id")}, "
            + $"$1, $2, u.parent, u.grp, u.condition, NOT {isRoot}, u.trace_parent "
            + "FROM unnest($3::uuid[], $4::text[], $5::text[], $6::text[], $7::text[], $8::bytea[], $9::int4[], $10::uuid[], $11::uuid[], $12::int2[], $14::text[]) "
            + "AS u(id, definition, subject_kind, subject_id, partition_key, args, args_version, parent, grp, condition, trace_parent)",
            work.Connection,
            work.Transaction);
        insert.Parameters.AddRange(parameters.ToArray());
        await insert.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Locks the running step and checks its lease after the lock is held, then adds the groups, marked dynamic, and the
    /// steps that follow it under the chain's current generation, counts the new nodes of the step's own enclosing group into its pending
    /// count, and records the definitions of the new steps with the chain.
    /// </summary>
    public async ValueTask AddGroupAsync(IWorkTransaction transaction, Lease<JobId> lease, IReadOnlyList<ChainStepData> steps, IReadOnlyList<ChainGroupData> groups, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(steps);
        ArgumentNullException.ThrowIfNull(groups);
        var work = PostgreSqlTransaction.From(transaction);
        var sql = Sql(lease.Source);
        Guid chain;
        Guid? scope;
        long generation;
        await using (var batch = new NpgsqlBatch(work.Connection, work.Transaction))
        {
            batch.BatchCommands.Add(KeyCommand($"SELECT 1 FROM {sql.Table} AS r WHERE {sql.KeyMatch(1)} FOR UPDATE OF r", lease.Source, lease.Key));
            batch.BatchCommands.Add(KeyCommand(
                $"SELECT r.chain_id, r.chain_group, r.chain_generation FROM {sql.Table} AS r WHERE {sql.KeyMatch(1)} AND r.state = 0 AND {sql.Valid(2, 3)}",
                lease.Source,
                lease.Key,
                Uuid(lease.Owner.Value),
                Int64(lease.Epoch)));
            await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
            await reader.NextResultAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                throw new LeaseLostException($"The lease on {lease.Source.Name} record {lease.Key} at epoch {lease.Epoch} is no longer valid.");
            }
            if (reader.IsDBNull(0))
            {
                throw new InvalidOperationException($"Job {lease.Key} of {sql.Job!.Definition} is not a chain step; only a chain step adds a group.");
            }
            chain = reader.GetGuid(0);
            scope = reader.IsDBNull(1) ? null : reader.GetGuid(1);
            generation = reader.GetInt64(2);
        }
        var added = groups.Select(g => g with { Group = g.Group ?? scope }).ToList();
        var addedSteps = steps.Select(s => s with { Group = s.Group ?? scope }).ToList();
        await InsertGroupsAsync(work, new ChainId(chain), generation, addedSteps, added, true, cancellationToken);
        if (addedSteps.Count > 0)
        {
            await InsertStepsAsync(work, new ChainId(chain), generation, addedSteps, null, cancellationToken);
        }
        var pending = Int32(Pending(addedSteps, added, scope));
        await using var count = new NpgsqlBatch(work.Connection, work.Transaction);
        count.BatchCommands.Add(scope is { } enclosing
            ? Command($"UPDATE {chainGroups} SET pending = pending + $2 WHERE id = $1", Uuid(enclosing), pending)
            : Command($"UPDATE {chains} SET pending = pending + $2 WHERE id = $1", Uuid(chain), pending));
        count.BatchCommands.Add(Command(
            $"UPDATE {chains} SET definitions = ARRAY(SELECT DISTINCT d FROM unnest(definitions || $2) AS d) WHERE id = $1",
            Uuid(chain),
            Texts([.. addedSteps.Select(s => s.Definition).Distinct(StringComparer.Ordinal)])));
        await count.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Settles a chain step that has just ended, in its terminal transaction: a skipped root records every waiting step
    /// of the chain to be skipped; otherwise the step's children are decided by its outcome and the empty groups that
    /// follow it settle as succeeded. Then its enclosing group, or the chain's top level, counts it settled.
    /// </summary>
    private async Task SettleStepAsync(PostgreSqlTransaction work, Guid chain, Guid step, Guid? group, bool root, JobOutcome outcome, CancellationToken cancellationToken)
    {
        if (root && outcome == JobOutcome.Skipped)
        {
            await using var skip = new NpgsqlCommand(
                $"UPDATE {jobTable} AS r SET chain_waiting = false, skip_pending = true, skip_reason = $2 WHERE r.chain_id = $1 AND r.state = 0 AND r.chain_waiting RETURNING r.definition",
                work.Connection,
                work.Transaction);
            skip.Parameters.Add(Uuid(chain));
            skip.Parameters.Add(Text(JobReasons.RootSkipped));
            var definitions = new HashSet<string>(StringComparer.Ordinal);
            await using (var reader = await skip.ExecuteReaderAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    definitions.Add(reader.GetString(0));
                }
            }
            foreach (var definition in definitions)
            {
                await leases.NotifyAsync(work.Connection, work.Transaction, WorkTopics.Plan, JobDefinition.SourceNameOf(definition), cancellationToken);
            }
        }
        else
        {
            await DecideAsync(work, chain, step, Class(outcome), cancellationToken);
            await using var empty = new NpgsqlCommand(
                $"UPDATE {chainGroups} SET settled_at = clock_timestamp(), outcome = $3 WHERE chain_id = $1 AND parent = $2 AND pending = 0 AND settled_at IS NULL RETURNING id, enclosing_group",
                work.Connection,
                work.Transaction);
            empty.Parameters.Add(Uuid(chain));
            empty.Parameters.Add(Uuid(step));
            empty.Parameters.Add(new NpgsqlParameter<short> { TypedValue = (short)JobOutcome.Completed });
            var settled = new List<(Guid Id, Guid? Group)>();
            await using (var reader = await empty.ExecuteReaderAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    settled.Add((reader.GetGuid(0), reader.IsDBNull(1) ? null : reader.GetGuid(1)));
                }
            }
            foreach (var (id, enclosing) in settled)
            {
                await DecideAsync(work, chain, id, ChainOutcomes.Succeeded, cancellationToken);
                await CountSettledAsync(work, chain, enclosing, Rank(JobOutcome.Completed), cancellationToken);
            }
        }
        await CountSettledAsync(work, chain, group, Rank(outcome), cancellationToken);
    }

    /// <summary>
    /// Decides the waiting children of a settled step or group: those whose condition holds for <paramref name="settled"/>
    /// become due now, the others are recorded to be skipped by their definition's maintenance, which runs their outcome
    /// hook. Signals the claimers and planners of their definitions.
    /// </summary>
    private async Task DecideAsync(PostgreSqlTransaction work, Guid chain, Guid node, ChainOutcomes settled, CancellationToken cancellationToken)
    {
        await using var decide = new NpgsqlCommand(
            $"UPDATE {jobTable} AS r SET chain_waiting = false, due_at = CASE WHEN r.chain_condition & $3 <> 0 THEN clock_timestamp() END, "
            + "skip_pending = r.chain_condition & $3 = 0, skip_reason = CASE WHEN r.chain_condition & $3 = 0 THEN $4 END "
            + "WHERE r.chain_id = $1 AND r.chain_parent = $2 AND r.state = 0 AND r.chain_waiting "
            + "RETURNING r.definition, r.chain_condition & $3 <> 0",
            work.Connection,
            work.Transaction);
        decide.Parameters.Add(Uuid(chain));
        decide.Parameters.Add(Uuid(node));
        decide.Parameters.Add(new NpgsqlParameter<short> { TypedValue = (short)settled });
        decide.Parameters.Add(Text(JobReasons.ChainCondition));
        var signals = new HashSet<(string, SignalTopic)>();
        await using (var reader = await decide.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                signals.Add((reader.GetString(0), reader.GetBoolean(1) ? WorkTopics.Due : WorkTopics.Plan));
            }
        }
        foreach (var (definition, kind) in signals)
        {
            await leases.NotifyAsync(work.Connection, work.Transaction, kind, JobDefinition.SourceNameOf(definition), cancellationToken);
        }
    }

    /// <summary>
    /// Counts one settled node of rank <paramref name="rank"/> in <paramref name="group"/>, or in the chain's top level,
    /// in one statement that also settles it when nothing is pending any longer. A settled group decides its
    /// continuations and is counted in its own enclosing group with the rank of its aggregate. A settled chain records
    /// the outcome its owning occurrence ends with and signals that occurrence's planners, and a chain whose deletion
    /// was requested is deleted.
    /// </summary>
    private async Task CountSettledAsync(PostgreSqlTransaction work, Guid chain, Guid? group, short rank, CancellationToken cancellationToken)
    {
        const string settles = "pending = 1";
        var aggregate = $"CASE greatest(worst, $2) WHEN 0 THEN {(short)JobOutcome.Skipped} WHEN 1 THEN {(short)JobOutcome.Completed} "
            + $"WHEN 2 THEN {(short)JobOutcome.Cancelled} ELSE {(short)JobOutcome.Failed} END";
        var update = $"SET pending = pending - 1, worst = greatest(worst, $2), settled_at = CASE WHEN {settles} THEN clock_timestamp() END, "
            + $"outcome = CASE WHEN {settles} THEN {aggregate} END WHERE id = $1 ";
        while (group is { } current)
        {
            await using var command = new NpgsqlCommand(
                $"UPDATE {chainGroups} {update}RETURNING pending, outcome, enclosing_group",
                work.Connection,
                work.Transaction);
            command.Parameters.Add(Uuid(current));
            command.Parameters.Add(new NpgsqlParameter<short> { TypedValue = rank });
            JobOutcome outcome;
            await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                await reader.ReadAsync(cancellationToken);
                if (reader.GetInt32(0) > 0)
                {
                    return;
                }
                outcome = (JobOutcome)reader.GetInt16(1);
                group = reader.IsDBNull(2) ? null : reader.GetGuid(2);
            }
            await DecideAsync(work, chain, current, Class(outcome), cancellationToken);
            rank = Rank(outcome);
        }
        await using (var command = new NpgsqlCommand(
            $"UPDATE {chains} {update}RETURNING pending, outcome, owner_job, owner_definition, delete_requested",
            work.Connection,
            work.Transaction))
        {
            command.Parameters.Add(Uuid(chain));
            command.Parameters.Add(new NpgsqlParameter<short> { TypedValue = rank });
            JobOutcome outcome;
            Guid? owner;
            string? ownerDefinition;
            bool delete;
            await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                await reader.ReadAsync(cancellationToken);
                if (reader.GetInt32(0) > 0)
                {
                    return;
                }
                outcome = (JobOutcome)reader.GetInt16(1);
                owner = reader.IsDBNull(2) ? null : reader.GetGuid(2);
                ownerDefinition = reader.IsDBNull(3) ? null : reader.GetString(3);
                delete = reader.GetBoolean(4);
            }
            if (owner is { } occurrence)
            {
                await using var record = new NpgsqlCommand(
                    $"UPDATE {jobTable} SET chain_outcome = $3 WHERE id = $1 AND state = 0 AND owned_chain = $2",
                    work.Connection,
                    work.Transaction);
                record.Parameters.Add(Uuid(occurrence));
                record.Parameters.Add(Uuid(chain));
                record.Parameters.Add(new NpgsqlParameter<short> { TypedValue = (short)outcome });
                if (await record.ExecuteNonQueryAsync(cancellationToken) == 1)
                {
                    await leases.NotifyAsync(work.Connection, work.Transaction, WorkTopics.Plan, JobDefinition.SourceNameOf(ownerDefinition!), cancellationToken);
                }
            }
            if (delete)
            {
                await DeleteChainRowsAsync(work, chain, cancellationToken);
            }
        }
    }

    /// <summary>The outcomes a condition is evaluated against for a settled node.</summary>
    private static ChainOutcomes Class(JobOutcome outcome) => outcome switch
    {
        JobOutcome.Completed => ChainOutcomes.Succeeded,
        JobOutcome.Failed or JobOutcome.TimedOut => ChainOutcomes.Failed,
        JobOutcome.Cancelled => ChainOutcomes.Cancelled,
        _ => ChainOutcomes.Skipped,
    };

    /// <summary>A settled node's weight in its group's aggregate: skipped nodes do not count, then succeeded, cancelled and failed.</summary>
    private static short Rank(JobOutcome outcome) => outcome switch
    {
        JobOutcome.Completed => 1,
        JobOutcome.Cancelled => 2,
        JobOutcome.Failed or JobOutcome.TimedOut => 3,
        _ => 0,
    };

    /// <summary>The reason an occurrence ends with once its chain settled with <paramref name="outcome"/>.</summary>
    private static string? OwnerReason(JobOutcome outcome) => outcome switch
    {
        JobOutcome.Completed => null,
        JobOutcome.Cancelled => JobReasons.ChainCancelled,
        JobOutcome.Skipped => JobReasons.ChainSkipped,
        _ => JobReasons.ChainFailed,
    };

    /// <summary>Deletes a chain's steps and the chain with its groups.</summary>
    private async Task DeleteChainRowsAsync(PostgreSqlTransaction work, Guid chain, CancellationToken cancellationToken)
    {
        await using var batch = new NpgsqlBatch(work.Connection, work.Transaction);
        batch.BatchCommands.Add(Command($"DELETE FROM {jobTable} WHERE chain_id = $1", Uuid(chain)));
        batch.BatchCommands.Add(Command($"DELETE FROM {chains} WHERE id = $1", Uuid(chain)));
        await batch.ExecuteNonQueryAsync(cancellationToken);
    }

    private static NpgsqlParameter<Guid?[]> NullableUuids(IEnumerable<Guid?> values) =>
        new() { TypedValue = [.. values], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Uuid };

    private static NpgsqlParameter<string?[]> NullableTexts(IEnumerable<string?> values) =>
        new() { TypedValue = [.. values], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text };
}
