using System.Globalization;
using System.Text.Json;
using Corfedo.Work.SqlServer.Leases;

namespace Corfedo.Work.SqlServer.Jobs;

internal sealed partial class SqlServerJobStore
{
    public async ValueTask<StoredJobOutcome?> GetParentOutcomeAsync(JobId step, string definition, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        await using var connection = await leases.OpenAsync(cancellationToken);
        await using var command = Tsql.Command(connection, null,
            $"SELECT {OutcomeColumns} FROM {Jobs} AS s INNER JOIN {Jobs} AS j ON j.id = s.chain_parent WHERE s.id = @step AND j.[definition] = @definition;",
            Tsql.Guid("@step", step.Value),
            Tsql.Text("@definition", definition, 200));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? StoredOutcome(reader) : null;
    }

    /// <summary>The steps of the group the step follows that no step or group of the group follows, from the chain's steps.</summary>
    public async ValueTask<IReadOnlyList<StoredJobOutcome>> GetGroupOutcomesAsync(JobId step, string definition, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        await using var connection = await leases.OpenAsync(cancellationToken);
        await using var command = Tsql.Command(connection, null, $"""
            SELECT {OutcomeColumns} FROM {Jobs} AS s INNER JOIN {Jobs} AS j WITH (INDEX(jobs_chain)) ON j.chain_id IS NOT NULL AND j.chain_id = s.chain_id
            WHERE s.id = @step AND j.chain_group = s.chain_parent AND j.[definition] = @definition
                AND NOT EXISTS (SELECT 1 FROM {Jobs} AS n WITH (INDEX(jobs_chain)) WHERE n.chain_id IS NOT NULL AND n.chain_id = j.chain_id AND n.chain_parent = j.id AND n.chain_group = j.chain_group)
                AND NOT EXISTS (SELECT 1 FROM {chainGroups} AS g WHERE g.chain_id = j.chain_id AND g.parent = j.id)
            ORDER BY j.created_at, j.id;
            """,
            Tsql.Guid("@step", step.Value),
            Tsql.Text("@definition", definition, 200));
        var outcomes = new List<StoredJobOutcome>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            outcomes.Add(StoredOutcome(reader));
        }
        return outcomes;
    }

    /// <summary>
    /// The chain row, its steps through <c>jobs_chain</c> and its groups, in one statement, so they are read from one
    /// snapshot: the steps and groups come as JSON arrays.
    /// </summary>
    public async ValueTask<ChainStatus?> GetChainAsync(ChainId chain, CancellationToken cancellationToken)
    {
        await using var connection = await leases.OpenAsync(cancellationToken);
        await using var command = Tsql.Command(connection, null, $"""
            SELECT c.generation, c.root_job, c.owner_job, c.settled_at, c.outcome, c.created_at,
                (SELECT j.id AS i, j.[definition] AS d, j.chain_generation AS n, j.chain_parent AS p, j.chain_group AS g, j.chain_waiting AS w,
                    {Tsql.Bit("j.state <> 0")} AS e, j.outcome AS o, j.reason AS r
                 FROM {Jobs} AS j WITH (INDEX(jobs_chain)) WHERE j.chain_id IS NOT NULL AND j.chain_id = c.id ORDER BY j.created_at, j.id FOR JSON PATH),
                (SELECT g.id AS i, g.generation AS n, g.parent AS p, g.enclosing_group AS g, g.pending AS c, {Tsql.Bit("g.settled_at IS NOT NULL")} AS s, g.outcome AS o
                 FROM {chainGroups} AS g WHERE g.chain_id = c.id ORDER BY g.id FOR JSON PATH)
            FROM {chains} AS c WHERE c.id = @chain;
            """, Tsql.Guid("@chain", chain.Value));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }
        var settledAt = Tsql.NullableTime(reader, 3);
        var steps = new List<ChainStepStatus>();
        if (!reader.IsDBNull(6))
        {
            using var document = JsonDocument.Parse(reader.GetString(6));
            foreach (var step in document.RootElement.EnumerateArray())
            {
                steps.Add(new ChainStepStatus(
                    new JobId(step.GetProperty("i").GetGuid()),
                    step.GetProperty("d").GetString()!,
                    step.GetProperty("n").GetInt64(),
                    OptionalGuid(step, "p"),
                    OptionalGuid(step, "g"),
                    step.GetProperty("w").GetBoolean(),
                    step.GetProperty("e").GetBoolean(),
                    step.TryGetProperty("o", out var outcome) ? (JobOutcome)outcome.GetInt16() : null,
                    step.TryGetProperty("r", out var reason) ? reason.GetString() : null));
            }
        }
        var groups = new List<ChainGroupStatus>();
        if (!reader.IsDBNull(7))
        {
            using var document = JsonDocument.Parse(reader.GetString(7));
            foreach (var group in document.RootElement.EnumerateArray())
            {
                groups.Add(new ChainGroupStatus(
                    group.GetProperty("i").GetGuid(),
                    group.GetProperty("n").GetInt64(),
                    group.GetProperty("p").GetGuid(),
                    OptionalGuid(group, "g"),
                    group.GetProperty("c").GetInt32(),
                    group.GetProperty("s").GetBoolean(),
                    group.TryGetProperty("o", out var outcome) ? (JobOutcome)outcome.GetInt16() : null));
            }
        }
        return new ChainStatus(
            chain,
            reader.GetInt64(0),
            new JobId(reader.GetGuid(1)),
            reader.IsDBNull(2) ? null : new JobId(reader.GetGuid(2)),
            settledAt is not null,
            reader.IsDBNull(4) ? null : (JobOutcome)reader.GetInt16(4),
            Tsql.Time(reader, 5),
            settledAt,
            steps,
            groups);
    }

    /// <summary>A uniqueidentifier property of a <c>FOR JSON</c> row, which omits null columns.</summary>
    private static Guid? OptionalGuid(JsonElement element, string name) => element.TryGetProperty(name, out var value) ? value.GetGuid() : null;

    /// <summary>
    /// Locks the chain row, which serializes retries, and requires it settled. The reset set is the node with its
    /// dependants, a group with its branches; every group enclosing a reset node is reopened, which resets its
    /// continuations as well, up to the chain's top level. Reset steps are outstanding again under the new generation
    /// with new execution ids and fresh attempts, waiting for their parents, keeping their ended execution's outcome;
    /// the chosen step is due now, or the branches of a chosen group are decided by the outcome of the step it follows.
    /// A dynamic group added by a reset step is deleted with its steps and groups instead, since the step adds it again
    /// when it runs. Reopened groups count their reset nodes as pending and keep the worst outcome of the others. Steps
    /// and groups are written in primary-key order.
    /// </summary>
    public async ValueTask<RetryFromOutcome> RetryFromAsync(ChainId chain, Guid node, CancellationToken cancellationToken) =>
        await InTransactionAsync(async work =>
        {
            long generation;
            var nodes = new Dictionary<Guid, ChainNode>();
            await using (var read = Tsql.Command(work, $"""
                DECLARE @held int;
                SELECT @held = 1 FROM {chains} WITH (UPDLOCK, ROWLOCK) WHERE id = @chain;
                SELECT generation, {Tsql.Bit("settled_at IS NOT NULL")} FROM {chains} WHERE id = @chain;
                SELECT id, chain_parent, chain_group, outcome, chain_condition, [definition] FROM {Jobs} WITH (INDEX(jobs_chain)) WHERE chain_id IS NOT NULL AND chain_id = @chain;
                SELECT id, parent, enclosing_group, outcome, dynamic FROM {chainGroups} WHERE chain_id = @chain;
                """, Tsql.Guid("@chain", chain.Value)))
            await using (var reader = await read.ExecuteReaderAsync(cancellationToken))
            {
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
            var top = Count(null);
            var steps = reset.Where(id => !nodes[id].IsGroup).ToArray();
            await using (var write = Tsql.Command(work, $"""
                DECLARE @discarded TABLE (id uniqueidentifier NOT NULL PRIMARY KEY);
                INSERT INTO @discarded (id) SELECT DISTINCT value FROM OPENJSON(@discarded_ids) WITH (value uniqueidentifier '$');
                DELETE r FROM @discarded AS d INNER LOOP JOIN {Jobs} AS r WITH (INDEX(jobs_key), FORCESEEK) ON r.id = d.id WHERE r.chain_id = @chain;
                DELETE g FROM @discarded AS d INNER LOOP JOIN {chainGroups} AS g WITH (FORCESEEK) ON g.id = d.id WHERE g.chain_id = @chain;
                DECLARE @steps TABLE (id uniqueidentifier NOT NULL PRIMARY KEY, due bit NOT NULL, skip bit NOT NULL);
                INSERT INTO @steps (id, due, skip) SELECT id, due, skip FROM OPENJSON(@step_ids) WITH (id uniqueidentifier '$.i', due bit '$.d', skip bit '$.s');
                UPDATE r SET state = 0, chain_generation = @generation, chain_waiting = {Tsql.Bit("s.due = 0 AND s.skip = 0")},
                    due_at = CASE WHEN s.due = 1 AND r.ended_calendar IS NULL THEN SYSUTCDATETIME() END,
                    calendar_at = CASE WHEN s.due = 1 AND r.ended_calendar IS NOT NULL THEN SYSUTCDATETIME() END,
                    calendar_blocked = CASE WHEN r.ended_calendar IS NOT NULL THEN {(short)Blocked.MoveForward} END,
                    calendar = r.ended_calendar, ended_calendar = NULL, window_end = NULL, calendar_unavailable = 0,
                    skip_pending = s.skip, skip_reason = CASE WHEN s.skip = 1 THEN @reason END,
                    execution_id = NEWID(), first_requested_at = SYSUTCDATETIME(), attempts = 0, started = 0, takeovers = 0, failures = 0,
                    cancel_requested_at = NULL, progress_percent = NULL, progress_message = NULL,
                    paused = {PausedCopy("r.[definition]", "r.subject_kind", "r.subject_id")}
                FROM @steps AS s INNER LOOP JOIN {Jobs} AS r WITH (INDEX(jobs_key), FORCESEEK) ON r.id = s.id WHERE r.chain_id = @chain;
                DECLARE @groups TABLE (id uniqueidentifier NOT NULL PRIMARY KEY, pending int NOT NULL, worst smallint NOT NULL);
                INSERT INTO @groups (id, pending, worst) SELECT id, pending, worst FROM OPENJSON(@group_counts) WITH (id uniqueidentifier '$.i', pending int '$.n', worst smallint '$.w');
                UPDATE g SET generation = @generation, settled_at = NULL, outcome = NULL, pending = u.pending, worst = u.worst
                FROM @groups AS u INNER LOOP JOIN {chainGroups} AS g WITH (FORCESEEK) ON g.id = u.id;
                UPDATE {chains} SET generation = @generation, settled_at = NULL, outcome = NULL, pending = @pending, worst = @worst WHERE id = @chain;
                """,
                Tsql.Guid("@chain", chain.Value),
                Tsql.Int64("@generation", generation),
                Tsql.Guids("@discarded_ids", discarded),
                Tsql.Json("@step_ids", Json(writer =>
                {
                    foreach (var id in steps)
                    {
                        writer.WriteStartObject();
                        writer.WriteString("i", id);
                        writer.WriteBoolean("d", due.Contains(id));
                        writer.WriteBoolean("s", skipped.Contains(id));
                        writer.WriteEndObject();
                    }
                })),
                Tsql.Json("@group_counts", Json(writer =>
                {
                    foreach (var id in groups)
                    {
                        var (pending, worst) = Count(id);
                        writer.WriteStartObject();
                        writer.WriteString("i", id);
                        writer.WriteNumber("n", pending);
                        writer.WriteNumber("w", worst);
                        writer.WriteEndObject();
                    }
                })),
                Tsql.Int32("@pending", top.Pending),
                Tsql.Int16("@worst", top.Worst),
                Tsql.Text("@reason", JobReasons.ChainCondition)))
            {
                await write.ExecuteNonQueryAsync(cancellationToken);
            }
            if (chosen.IsGroup && !members[chosen.Id].Any())
            {
                // A group without branches settles as succeeded at once.
                await using (var settle = Tsql.Command(work, $"UPDATE {chainGroups} SET settled_at = SYSUTCDATETIME(), outcome = @outcome WHERE id = @id;",
                    Tsql.Guid("@id", chosen.Id),
                    Tsql.Int16("@outcome", (short)JobOutcome.Completed)))
                {
                    await settle.ExecuteNonQueryAsync(cancellationToken);
                }
                await DecideAsync(work, chain.Value, chosen.Id, ChainOutcomes.Succeeded, cancellationToken);
                await CountSettledAsync(work, chain.Value, chosen.Group, Rank(JobOutcome.Completed), cancellationToken);
            }
            foreach (var definition in due.Select(id => nodes[id].Definition!).Distinct(StringComparer.Ordinal))
            {
                await leases.NotifyAsync(work.Connection, work.Transaction, WorkTopics.Due, JobDefinition.SourceNameOf(definition), cancellationToken);
                await leases.NotifyAsync(work.Connection, work.Transaction, WorkTopics.Plan, JobDefinition.SourceNameOf(definition), cancellationToken);
            }
            foreach (var definition in skipped.Select(id => nodes[id].Definition!).Distinct(StringComparer.Ordinal))
            {
                await leases.NotifyAsync(work.Connection, work.Transaction, WorkTopics.Plan, JobDefinition.SourceNameOf(definition), cancellationToken);
            }
            return RetryFromOutcome.Retried;
        }, cancellationToken);

    /// <summary>
    /// Locks the chain's outstanding steps in the cancellation order, waiting, refuses the cancellation when one belongs
    /// to a definition <paramref name="definitions"/> does not resolve, then cancels them per definition. Steps recorded
    /// to be skipped are left to finalization.
    /// </summary>
    public async ValueTask<int> CancelChainAsync(WorkTransaction transaction, ChainId chain, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definitions);
        return await CancelStepsAsync(SqlServerTransaction.From(transaction), chain.Value, definitions, cancellationToken);
    }

    private async Task<int> CancelStepsAsync(SqlServerTransaction work, Guid chain, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken)
    {
        var locked = await LockForCancelAsync(
            work,
            "r.chain_id IS NOT NULL AND r.chain_id = @chain AND r.state = 0 AND r.skip_pending = 0",
            [Tsql.Guid("@chain", chain)],
            cancellationToken);
        await CancelSelectedAsync(work, locked, definitions, cancellationToken);
        return locked.Selected.Count;
    }

    /// <summary>
    /// A settled chain is locked and deleted. With a cascade, an unsettled chain's steps are cancelled first, holding
    /// their locks before the chain row as their terminal transitions do, and the chain is marked to be deleted by the
    /// transition that settles it, or deleted now when the cancellations settled it.
    /// </summary>
    public async ValueTask<ChainDeleteOutcome> DeleteChainAsync(ChainId chain, ChainDelete mode, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definitions);
        return await InTransactionAsync(async work =>
        {
            bool? settled;
            await using (var read = Tsql.Command(work, $"SELECT {Tsql.Bit("settled_at IS NOT NULL")} FROM {chains} WHERE id = @chain;", Tsql.Guid("@chain", chain.Value)))
            {
                settled = (bool?)await read.ExecuteScalarAsync(cancellationToken);
            }
            if (settled is null)
            {
                return ChainDeleteOutcome.NotFound;
            }
            if (settled == false && mode == ChainDelete.Cascade)
            {
                await CancelStepsAsync(work, chain.Value, definitions, cancellationToken);
                await using var mark = Tsql.Command(work,
                    $"UPDATE {chains} SET delete_requested = 1 WHERE id = @chain AND settled_at IS NULL;\nSELECT @@ROWCOUNT;",
                    Tsql.Guid("@chain", chain.Value));
                if (Convert.ToInt32(await mark.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture) == 1)
                {
                    return ChainDeleteOutcome.Deleting;
                }
            }
            await using (var locked = Tsql.Command(work, $"""
                DECLARE @held int;
                SELECT @held = 1 FROM {chains} WITH (UPDLOCK, ROWLOCK) WHERE id = @chain;
                SELECT {Tsql.Bit("settled_at IS NOT NULL")} FROM {chains} WHERE id = @chain;
                """, Tsql.Guid("@chain", chain.Value)))
            {
                switch ((bool?)await locked.ExecuteScalarAsync(cancellationToken))
                {
                    case null:
                        return ChainDeleteOutcome.NotFound;
                    case false:
                        return ChainDeleteOutcome.Unsettled;
                }
            }
            await DeleteChainRowsAsync(work, chain.Value, cancellationToken);
            return ChainDeleteOutcome.Deleted;
        }, cancellationToken);
    }

    /// <summary>A step or group of a chain as a retry reads it; a group has no condition or definition.</summary>
    private sealed record ChainNode(Guid Id, bool IsGroup, Guid? Parent, Guid? Group, JobOutcome? Outcome, ChainOutcomes Condition, string? Definition, bool Dynamic);
}
