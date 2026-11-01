using Npgsql;
using NpgsqlTypes;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Jobs;

internal sealed partial class PostgreSqlJobStore
{
    private const string OutcomeColumns = "j.id, j.state <> 0, j.ended_execution_id, j.outcome, j.reason, j.ended_at, j.result, j.result_version";

    public async ValueTask<StoredJobOutcome?> GetParentOutcomeAsync(JobId step, string definition, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        await using var command = dataSource.CreateCommand(
            $"SELECT {OutcomeColumns} FROM {jobTable} AS s JOIN {jobTable} AS j ON j.id = s.chain_parent WHERE s.id = $1 AND j.definition = $2");
        command.Parameters.Add(Uuid(step.Value));
        command.Parameters.Add(Text(definition));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? StoredOutcome(reader) : null;
    }

    /// <summary>The steps of the group the step follows that no step or group of the group follows, from the chain's steps.</summary>
    public async ValueTask<IReadOnlyList<StoredJobOutcome>> GetGroupOutcomesAsync(JobId step, string definition, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        await using var command = dataSource.CreateCommand(
            $"SELECT {OutcomeColumns} FROM {jobTable} AS s JOIN {jobTable} AS j ON j.chain_id = s.chain_id AND j.chain_group = s.chain_parent "
            + $"WHERE s.id = $1 AND j.definition = $2 AND NOT EXISTS (SELECT 1 FROM {jobTable} AS n WHERE n.chain_id = j.chain_id AND n.chain_parent = j.id AND n.chain_group = j.chain_group) "
            + $"AND NOT EXISTS (SELECT 1 FROM {chainGroups} AS g WHERE g.chain_id = j.chain_id AND g.parent = j.id) "
            + "ORDER BY j.created_at, j.id");
        command.Parameters.Add(Uuid(step.Value));
        command.Parameters.Add(Text(definition));
        var outcomes = new List<StoredJobOutcome>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            outcomes.Add(StoredOutcome(reader));
        }
        return outcomes;
    }

    /// <summary>Reads the outcome columns of <see cref="OutcomeColumns"/>.</summary>
    private static StoredJobOutcome StoredOutcome(NpgsqlDataReader reader) => new(
        new JobId(reader.GetGuid(0)),
        reader.GetBoolean(1),
        reader.IsDBNull(2) ? null : reader.GetGuid(2),
        reader.IsDBNull(3) ? null : (JobOutcome)reader.GetInt16(3),
        reader.IsDBNull(4) ? null : reader.GetString(4),
        reader.IsDBNull(5) ? null : reader.GetFieldValue<DateTimeOffset>(5),
        reader.IsDBNull(6) ? null : reader.GetFieldValue<byte[]>(6),
        reader.IsDBNull(7) ? 0 : reader.GetInt32(7));

    /// <summary>The chain row, its steps through <c>jobs_chain</c> and its groups, in one snapshot.</summary>
    public async ValueTask<ChainStatus?> GetChainAsync(ChainId chain, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken);
        await using var batch = new NpgsqlBatch(connection, transaction);
        batch.BatchCommands.Add(Command($"SELECT generation, root_job, owner_job, settled_at, outcome, created_at FROM {chains} WHERE id = $1", Uuid(chain.Value)));
        batch.BatchCommands.Add(Command(
            $"SELECT id, definition, chain_generation, chain_parent, chain_group, chain_waiting, state <> 0, outcome, reason FROM {jobTable} WHERE chain_id = $1 ORDER BY created_at, id",
            Uuid(chain.Value)));
        batch.BatchCommands.Add(Command(
            $"SELECT id, generation, parent, enclosing_group, pending, settled_at IS NOT NULL, outcome FROM {chainGroups} WHERE chain_id = $1 ORDER BY id",
            Uuid(chain.Value)));
        await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }
        var (generation, root, owner, settledAt, outcome, createdAt) = (
            reader.GetInt64(0),
            new JobId(reader.GetGuid(1)),
            reader.IsDBNull(2) ? (JobId?)null : new JobId(reader.GetGuid(2)),
            reader.IsDBNull(3) ? (DateTimeOffset?)null : reader.GetFieldValue<DateTimeOffset>(3),
            reader.IsDBNull(4) ? (JobOutcome?)null : (JobOutcome)reader.GetInt16(4),
            reader.GetFieldValue<DateTimeOffset>(5));
        await reader.NextResultAsync(cancellationToken);
        var steps = new List<ChainStepStatus>();
        while (await reader.ReadAsync(cancellationToken))
        {
            steps.Add(new ChainStepStatus(
                new JobId(reader.GetGuid(0)),
                reader.GetString(1),
                reader.GetInt64(2),
                reader.IsDBNull(3) ? null : reader.GetGuid(3),
                reader.IsDBNull(4) ? null : reader.GetGuid(4),
                reader.GetBoolean(5),
                reader.GetBoolean(6),
                reader.IsDBNull(7) ? null : (JobOutcome)reader.GetInt16(7),
                reader.IsDBNull(8) ? null : reader.GetString(8)));
        }
        await reader.NextResultAsync(cancellationToken);
        var groups = new List<ChainGroupStatus>();
        while (await reader.ReadAsync(cancellationToken))
        {
            groups.Add(new ChainGroupStatus(
                reader.GetGuid(0),
                reader.GetInt64(1),
                reader.GetGuid(2),
                reader.IsDBNull(3) ? null : reader.GetGuid(3),
                reader.GetInt32(4),
                reader.GetBoolean(5),
                reader.IsDBNull(6) ? null : (JobOutcome)reader.GetInt16(6)));
        }
        return new ChainStatus(chain, generation, root, owner, settledAt is not null, outcome, createdAt, settledAt, steps, groups);
    }

    /// <summary>
    /// Locks the chain row, which serializes retries, and requires it settled. The reset set is the node with its
    /// dependants, a group with its branches; every group enclosing a reset node is reopened, which resets its
    /// continuations as well, up to the chain's top level. Reset steps are outstanding again under the new generation
    /// with new execution ids and fresh attempts, waiting for their parents, keeping their ended execution's outcome;
    /// the chosen step is due now, or the branches of a chosen group are decided by the outcome of the step it follows.
    /// A dynamic group added by a reset step is deleted with its steps and groups instead, since the step adds it again
    /// when it runs. Reopened groups count their reset nodes as pending and keep the worst outcome of the others.
    /// </summary>
    public async ValueTask<RetryFromOutcome> RetryFromAsync(ChainId chain, Guid node, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        var work = new PostgreSqlTransaction(transaction);
        long generation;
        var nodes = new Dictionary<Guid, ChainNode>();
        await using (var batch = new NpgsqlBatch(connection, transaction))
        {
            batch.BatchCommands.Add(Command($"SELECT generation, settled_at IS NOT NULL FROM {chains} WHERE id = $1 FOR UPDATE", Uuid(chain.Value)));
            batch.BatchCommands.Add(Command(
                $"SELECT id, chain_parent, chain_group, outcome, chain_condition, definition FROM {jobTable} WHERE chain_id = $1",
                Uuid(chain.Value)));
            batch.BatchCommands.Add(Command($"SELECT id, parent, enclosing_group, outcome, dynamic FROM {chainGroups} WHERE chain_id = $1", Uuid(chain.Value)));
            await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return RetryFromOutcome.NotFound;
            }
            if (!reader.GetBoolean(1))
            {
                return RetryFromOutcome.NotSettled;
            }
            generation = reader.GetInt64(0) + 1;
            await reader.NextResultAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var id = reader.GetGuid(0);
                nodes[id] = new ChainNode(
                    id,
                    false,
                    reader.IsDBNull(1) ? null : reader.GetGuid(1),
                    reader.IsDBNull(2) ? null : reader.GetGuid(2),
                    reader.IsDBNull(3) ? null : (JobOutcome)reader.GetInt16(3),
                    (ChainOutcomes)reader.GetInt16(4),
                    reader.GetString(5),
                    false);
            }
            await reader.NextResultAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var id = reader.GetGuid(0);
                nodes[id] = new ChainNode(id, true, reader.GetGuid(1), reader.IsDBNull(2) ? null : reader.GetGuid(2), reader.IsDBNull(3) ? null : (JobOutcome)reader.GetInt16(3), ChainOutcomes.None, null, reader.GetBoolean(4));
            }
        }
        if (!nodes.TryGetValue(node, out var chosen))
        {
            return RetryFromOutcome.NotFound;
        }
        var children = nodes.Values.Where(n => n.Parent is not null).ToLookup(n => n.Parent!.Value);
        var members = nodes.Values.Where(n => n.Group is not null).ToLookup(n => n.Group!.Value);
        var reset = new HashSet<Guid>();
        void Reset(ChainNode n)
        {
            if (!reset.Add(n.Id))
            {
                return;
            }
            foreach (var child in children[n.Id])
            {
                Reset(child);
            }
            foreach (var member in n.IsGroup ? members[n.Id] : [])
            {
                Reset(member);
            }
        }
        Reset(chosen);
        var reopened = new HashSet<Guid>();
        for (var changed = true; changed;)
        {
            changed = false;
            foreach (var id in reset.Concat(reopened).ToList())
            {
                if (nodes[id].Group is { } scope && !reset.Contains(scope) && reopened.Add(scope))
                {
                    changed = true;
                    foreach (var continuation in children[scope])
                    {
                        Reset(continuation);
                    }
                }
            }
        }
        // A dynamic group whose step runs again is discarded with its branches and continuations: the step adds it again.
        var discarded = new HashSet<Guid>();
        void Discard(ChainNode n)
        {
            if (!discarded.Add(n.Id))
            {
                return;
            }
            foreach (var dependant in children[n.Id].Concat(n.IsGroup ? members[n.Id] : []))
            {
                Discard(dependant);
            }
        }
        foreach (var group in nodes.Values.Where(n => n.Dynamic && reset.Contains(n.Parent!.Value)).ToList())
        {
            Discard(group);
        }
        reset.ExceptWith(discarded);
        foreach (var id in discarded)
        {
            nodes.Remove(id);
        }
        var due = new List<Guid>();
        var skipped = new List<Guid>();
        if (!chosen.IsGroup)
        {
            due.Add(chosen.Id);
        }
        else
        {
            var settled = Class(nodes[chosen.Parent!.Value].Outcome ?? JobOutcome.Skipped);
            foreach (var head in members[chosen.Id].Where(m => !m.IsGroup && m.Parent == chosen.Parent))
            {
                ((head.Condition & settled) != 0 ? due : skipped).Add(head.Id);
            }
        }
        (int Pending, short Worst) Count(Guid? scope)
        {
            var scoped = nodes.Values.Where(n => n.Group == scope).ToList();
            var open = scoped.Count(n => reset.Contains(n.Id) || reopened.Contains(n.Id));
            var worst = scoped.Where(n => !reset.Contains(n.Id) && !reopened.Contains(n.Id) && n.Outcome is not null).Select(n => Rank(n.Outcome!.Value)).DefaultIfEmpty((short)0).Max();
            return (open, worst);
        }
        var groups = reset.Concat(reopened).Where(id => nodes[id].IsGroup).ToArray();
        var counts = groups.Select(id => Count(id)).ToArray();
        var top = Count(null);
        var steps = reset.Where(id => !nodes[id].IsGroup).ToArray();
        await using (var batch = new NpgsqlBatch(connection, transaction))
        {
            batch.BatchCommands.Add(Command($"DELETE FROM {jobTable} WHERE chain_id = $1 AND id = ANY($2)", Uuid(chain.Value), Uuids([.. discarded])));
            batch.BatchCommands.Add(Command($"DELETE FROM {chainGroups} WHERE chain_id = $1 AND id = ANY($2)", Uuid(chain.Value), Uuids([.. discarded])));
            batch.BatchCommands.Add(Command(
                $"UPDATE {jobTable} AS r SET state = 0, chain_generation = $2, chain_waiting = NOT (r.id = ANY($4) OR r.id = ANY($5)), "
                + "due_at = CASE WHEN r.id = ANY($4) AND r.ended_calendar IS NULL THEN clock_timestamp() END, "
                + "calendar_at = CASE WHEN r.id = ANY($4) AND r.ended_calendar IS NOT NULL THEN clock_timestamp() END, "
                + $"calendar_blocked = CASE WHEN r.ended_calendar IS NOT NULL THEN {(short)Blocked.MoveForward} END, "
                + "calendar = r.ended_calendar, ended_calendar = NULL, window_end = NULL, calendar_unavailable = false, "
                + "skip_pending = r.id = ANY($5), skip_reason = CASE WHEN r.id = ANY($5) THEN $6 END, "
                + "execution_id = gen_random_uuid(), first_requested_at = clock_timestamp(), attempts = 0, started = false, takeovers = 0, failures = 0, "
                + "cancel_requested_at = NULL, progress_percent = NULL, progress_message = NULL, "
                + $"paused = r.subject_kind IS NOT NULL AND {PausedCopy("r.definition", "r.subject_kind", "r.subject_id")} "
                + "WHERE r.chain_id = $1 AND r.id = ANY($3)",
                Uuid(chain.Value),
                Int64(generation),
                Uuids(steps),
                Uuids([.. due]),
                Uuids([.. skipped]),
                Text(JobReasons.ChainCondition)));
            batch.BatchCommands.Add(Command(
                $"UPDATE {chainGroups} AS g SET generation = $1, settled_at = NULL, outcome = NULL, pending = u.pending, worst = u.worst "
                + "FROM unnest($2::uuid[], $3::int4[], $4::int2[]) AS u(id, pending, worst) WHERE g.id = u.id",
                Int64(generation),
                Uuids(groups),
                new NpgsqlParameter<int[]> { TypedValue = [.. counts.Select(c => c.Pending)], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer },
                new NpgsqlParameter<short[]> { TypedValue = [.. counts.Select(c => c.Worst)], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Smallint }));
            batch.BatchCommands.Add(Command(
                $"UPDATE {chains} SET generation = $2, settled_at = NULL, outcome = NULL, pending = $3, worst = $4 WHERE id = $1",
                Uuid(chain.Value),
                Int64(generation),
                Int32(top.Pending),
                new NpgsqlParameter<short> { TypedValue = top.Worst }));
            await batch.ExecuteNonQueryAsync(cancellationToken);
        }
        if (chosen.IsGroup && !members[chosen.Id].Any())
        {
            // A group without branches settles as succeeded at once.
            await using (var settle = new NpgsqlCommand($"UPDATE {chainGroups} SET settled_at = clock_timestamp(), outcome = $2 WHERE id = $1", connection, transaction))
            {
                settle.Parameters.Add(Uuid(chosen.Id));
                settle.Parameters.Add(new NpgsqlParameter<short> { TypedValue = (short)JobOutcome.Completed });
                await settle.ExecuteNonQueryAsync(cancellationToken);
            }
            await DecideAsync(work, chain.Value, chosen.Id, ChainOutcomes.Succeeded, cancellationToken);
            await CountSettledAsync(work, chain.Value, chosen.Group, Rank(JobOutcome.Completed), cancellationToken);
        }
        foreach (var definition in due.Select(id => nodes[id].Definition!).Distinct(StringComparer.Ordinal))
        {
            await leases.NotifyAsync(connection, transaction, WorkTopics.Due, JobDefinition.SourceNameOf(definition), cancellationToken);
            await leases.NotifyAsync(connection, transaction, WorkTopics.Plan, JobDefinition.SourceNameOf(definition), cancellationToken);
        }
        foreach (var definition in skipped.Select(id => nodes[id].Definition!).Distinct(StringComparer.Ordinal))
        {
            await leases.NotifyAsync(connection, transaction, WorkTopics.Plan, JobDefinition.SourceNameOf(definition), cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        return RetryFromOutcome.Retried;
    }

    /// <summary>
    /// Locks the chain's outstanding steps as <see cref="CancelLockOrder"/> describes, waiting, refuses the cancellation when one belongs to a
    /// definition <paramref name="definitions"/> does not resolve, then cancels them per definition. Steps recorded to be
    /// skipped are left to finalization.
    /// </summary>
    public async ValueTask<int> CancelChainAsync(WorkTransaction transaction, ChainId chain, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definitions);
        return await CancelStepsAsync(PostgreSqlTransaction.From(transaction), chain.Value, definitions, cancellationToken);
    }

    private async Task<int> CancelStepsAsync(PostgreSqlTransaction work, Guid chain, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT r.id, r.definition FROM {jobTable} AS r WHERE r.chain_id = $1 AND r.state = 0 AND NOT r.skip_pending ORDER BY {CancelLockOrder} FOR UPDATE OF r",
            work.Connection,
            work.Transaction);
        command.Parameters.Add(Uuid(chain));
        var jobs = new List<(Guid Id, string Definition)>();
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                jobs.Add((reader.GetGuid(0), reader.GetString(1)));
            }
        }
        var resolved = new List<((LeaseSource<JobId> Source, JobEndedCallback? Ended) Definition, Guid[] Jobs)>();
        foreach (var group in jobs.GroupBy(j => j.Definition, StringComparer.Ordinal))
        {
            var definition = definitions(group.Key)
                ?? throw new InvalidOperationException($"Job definition {group.Key} has steps in chain {chain} and is not registered in this process, so its outcome hook cannot run; register it with AddJob or AddJobDefinition.");
            resolved.Add((definition, [.. group.Select(j => j.Id)]));
        }
        foreach (var (definition, ids) in resolved)
        {
            await CancelLockedAsync(work, definition.Source, ids, definition.Ended, cancellationToken);
        }
        return jobs.Count;
    }

    /// <summary>
    /// A settled chain is locked and deleted. With a cascade, an unsettled chain's steps are cancelled first, holding
    /// their locks before the chain row as their terminal transitions do, and the chain is marked to be deleted by the
    /// transition that settles it, or deleted now when the cancellations settled it.
    /// </summary>
    public async ValueTask<ChainDeleteOutcome> DeleteChainAsync(ChainId chain, ChainDelete mode, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definitions);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        var work = new PostgreSqlTransaction(transaction);
        bool? settled;
        await using (var read = new NpgsqlCommand($"SELECT settled_at IS NOT NULL FROM {chains} WHERE id = $1", connection, transaction))
        {
            read.Parameters.Add(Uuid(chain.Value));
            settled = (bool?)await read.ExecuteScalarAsync(cancellationToken);
        }
        if (settled is null)
        {
            return ChainDeleteOutcome.NotFound;
        }
        if (settled == false && mode == ChainDelete.Cascade)
        {
            await CancelStepsAsync(work, chain.Value, definitions, cancellationToken);
            await using var mark = new NpgsqlCommand($"UPDATE {chains} SET delete_requested = true WHERE id = $1 AND settled_at IS NULL", connection, transaction);
            mark.Parameters.Add(Uuid(chain.Value));
            if (await mark.ExecuteNonQueryAsync(cancellationToken) == 1)
            {
                await transaction.CommitAsync(cancellationToken);
                return ChainDeleteOutcome.Deleting;
            }
        }
        await using (var locked = new NpgsqlCommand($"SELECT settled_at IS NOT NULL FROM {chains} WHERE id = $1 FOR UPDATE", connection, transaction))
        {
            locked.Parameters.Add(Uuid(chain.Value));
            switch ((bool?)await locked.ExecuteScalarAsync(cancellationToken))
            {
                case null:
                    return ChainDeleteOutcome.NotFound;
                case false:
                    return ChainDeleteOutcome.Unsettled;
            }
        }
        await DeleteChainRowsAsync(work, chain.Value, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return ChainDeleteOutcome.Deleted;
    }

    /// <summary>A step or group of a chain as a retry reads it; a group has no condition or definition.</summary>
    private sealed record ChainNode(Guid Id, bool IsGroup, Guid? Parent, Guid? Group, JobOutcome? Outcome, ChainOutcomes Condition, string? Definition, bool Dynamic);
}
