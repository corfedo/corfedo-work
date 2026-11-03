using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Corfedo.Work.SqlServer.Leases;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Jobs;

/// <summary>
/// Chains on the job table: every step is a job row from the request on, and <c>job_chains</c> and
/// <c>job_chain_groups</c> keep each chain's top level and groups with one pending count and the worst outcome among
/// their steps that ran; <c>job_chain_definitions</c> holds the definitions of each chain's steps. A step's terminal
/// transition decides its children, settles empty groups that follow it and decrements its enclosing group, in its own
/// transaction. It holds its own row before the group rows, inner groups before the groups enclosing them and those
/// before the chain row, so siblings serialize on the group without deadlocking. Several steps or groups are locked in
/// primary-key order.
/// </summary>
internal sealed partial class SqlServerJobStore : IChainStore
{
    private readonly string chains = Tsql.Quote(schema) + ".job_chains";
    private readonly string chainGroups = Tsql.Quote(schema) + ".job_chain_groups";
    private readonly string chainDefinitions = Tsql.Quote(schema) + ".job_chain_definitions";

    public async ValueTask RequestChainAsync(WorkTransaction transaction, ChainData chain, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(chain);
        var work = SqlServerTransaction.From(transaction);
        await CheckCalendarsAsync(work, [chain.Steps[0].Request], cancellationToken);
        await InsertChainAsync(work, chain, null, null, cancellationToken);
    }

    /// <summary>
    /// Locks the occurrence, then checks its lease, and that no cancellation was requested, by a clock reading taken
    /// after the lock is held, gives the lease up and leaves the occurrence outstanding without a due time, pointing at
    /// its chain, then creates the chain. The attempt stays open, held by the chain: the occurrence is running until its
    /// definition's maintenance ends it with the chain's outcome, which records the attempt in the run history.
    /// </summary>
    public async ValueTask<bool> StartOwnedChainAsync(Lease<JobId> lease, ChainData chain, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(chain);
        var sql = await SqlAsync(lease.Source, cancellationToken);
        return await SqlServerLeaseStore.RetryDeadlocksAsync(async () =>
        {
            await using var connection = await leases.OpenAsync(cancellationToken);
            await using var transaction = await SqlServerLeaseStore.BeginAsync(connection, cancellationToken);
            var work = new SqlServerTransaction(transaction);
            await using (var start = SqlServerLeaseStore.KeyCommand(connection, transaction, sql, lease.Source, lease.Key, $"""
                DECLARE @held int;
                SELECT @held = 1 FROM {sql.Table} AS r WITH (UPDLOCK, ROWLOCK) WHERE {sql.KeyMatch};
                UPDATE r SET lease_expires_at = NULL, due_at = NULL, window_end = NULL, owned_chain = @chain
                FROM {sql.Table} AS r WHERE {sql.KeyMatch} AND r.state = 0 AND {sql.Valid} AND r.cancel_requested_at IS NULL;
                SELECT @@ROWCOUNT;
                """,
                Tsql.Guid("@owner", lease.Owner.Value),
                Tsql.Int64("@epoch", lease.Epoch),
                Tsql.Guid("@chain", chain.Id.Value)))
            {
                if (Convert.ToInt32(await start.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture) != 1)
                {
                    return false;
                }
            }
            await InsertChainAsync(work, chain, lease.Key, sql.Job!.Definition, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return true;
        });
    }

    /// <summary>
    /// Inserts the chain with the definitions of its steps, its groups and its steps, each group and the chain's top
    /// level with the number of its nodes pending, and signals the root's definition: claimers, or its planners when the
    /// root has a calendar.
    /// </summary>
    private async Task InsertChainAsync(SqlServerTransaction work, ChainData chain, JobId? owner, string? ownerDefinition, CancellationToken cancellationToken)
    {
        var root = chain.Steps[0];
        await using (var insert = Tsql.Command(work, $"""
            INSERT INTO {chains} (id, root_job, owner_job, owner_definition, created_at, pending) VALUES (@chain, @root, @owner, @owner_definition, SYSUTCDATETIME(), @pending);
            INSERT INTO {chainDefinitions} (chain_id, [definition]) SELECT DISTINCT @chain, value FROM OPENJSON(@definitions) WITH (value nvarchar(200) '$');
            """,
            Tsql.Guid("@chain", chain.Id.Value),
            Tsql.Guid("@root", root.Id.Value),
            Tsql.Guid("@owner", owner?.Value),
            Tsql.Text("@owner_definition", ownerDefinition, 200),
            Tsql.Int32("@pending", Pending(chain.Steps, chain.Groups, null)),
            Tsql.Strings("@definitions", chain.Steps.Select(s => s.Definition).Distinct(StringComparer.Ordinal))))
        {
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

    private async Task InsertGroupsAsync(SqlServerTransaction work, ChainId chain, long generation, IReadOnlyList<ChainStepData> steps, IReadOnlyList<ChainGroupData> groups, bool dynamic, CancellationToken cancellationToken)
    {
        if (groups.Count == 0)
        {
            return;
        }
        await using var insert = Tsql.Command(work, $"""
            INSERT INTO {chainGroups} (id, chain_id, generation, parent, enclosing_group, pending, dynamic)
            SELECT u.id, @chain, @generation, u.parent, u.enclosing, u.pending, @dynamic
            FROM OPENJSON(@groups) WITH (id uniqueidentifier '$.i', parent uniqueidentifier '$.p', enclosing uniqueidentifier '$.g', pending int '$.n') AS u
            ORDER BY u.id;
            """,
            Tsql.Guid("@chain", chain.Value),
            Tsql.Int64("@generation", generation),
            Tsql.Bool("@dynamic", dynamic),
            Tsql.Json("@groups", Json(writer =>
            {
                foreach (var group in groups)
                {
                    writer.WriteStartObject();
                    writer.WriteString("i", group.Id);
                    writer.WriteString("p", group.Parent);
                    if (group.Group is { } enclosing)
                    {
                        writer.WriteString("g", enclosing);
                    }
                    writer.WriteNumber("n", Pending(steps, groups, group.Id));
                    writer.WriteEndObject();
                }
            })));
        await insert.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Inserts the steps in one statement: <paramref name="root"/>, when given, due by its schedule, or with a calendar
    /// at its instant for its definition's planner; every other step waiting for its parent without a due time. The
    /// pause flag is copied from the stored subject pauses.
    /// </summary>
    private async Task InsertStepsAsync(SqlServerTransaction work, ChainId chain, long generation, IReadOnlyList<ChainStepData> steps, ChainStepData? root, CancellationToken cancellationToken)
    {
        var parameters = new List<SqlParameter>
        {
            Tsql.Guid("@chain", chain.Value),
            Tsql.Int64("@generation", generation),
            Tsql.Guid("@root", root?.Id.Value ?? Guid.Empty),
            Tsql.Json("@steps", Json(writer =>
            {
                foreach (var step in steps)
                {
                    var request = step.Request;
                    writer.WriteStartObject();
                    writer.WriteString("i", step.Id.Value);
                    writer.WriteString("d", step.Definition);
                    if (request.Subject is { } subject)
                    {
                        writer.WriteString("sk", subject.Kind);
                        writer.WriteString("si", subject.Id);
                    }
                    writer.WriteString("p", request.Partition ?? "");
                    writer.WriteBase64String("a", request.Args);
                    writer.WriteNumber("v", request.ArgsVersion);
                    if (step.Parent is { } parent)
                    {
                        writer.WriteString("pa", parent);
                    }
                    if (step.Group is { } group)
                    {
                        writer.WriteString("g", group);
                    }
                    writer.WriteNumber("c", (short)step.Condition);
                    if (request.TraceParent is { } trace)
                    {
                        writer.WriteString("t", trace);
                    }
                    writer.WriteEndObject();
                }
            })),
        };
        var (due, calendarAt, calendar, blocked, closed) = ("NULL", "NULL", "NULL", "NULL", "NULL");
        if (root is not null)
        {
            var schedule = root.Request.Schedule;
            var instant = Timing(schedule, parameters);
            if (schedule.Calendar is null)
            {
                due = instant;
            }
            else
            {
                calendarAt = instant;
                parameters.Add(Tsql.Text("@calendar", schedule.Calendar, 128));
                parameters.Add(Tsql.Int16("@blocked", (short)schedule.Blocked));
                parameters.Add(Tsql.Int16("@closed", (short)schedule.Closed));
                (calendar, blocked, closed) = ("@calendar", "@blocked", "@closed");
            }
        }
        // T-SQL refuses a CASE whose results are all the NULL constant.
        static string ForRoot(string value) => value == "NULL" ? value : $"CASE WHEN u.id = @root THEN {value} END";
        await using var insert = Tsql.Command(work, $"""
            INSERT INTO {Jobs} (id, [definition], subject_kind, subject_id, partition_key, args, args_version, first_requested_at, created_at,
                due_at, calendar_at, calendar, calendar_blocked, calendar_closed, execution_id, paused,
                chain_id, chain_generation, chain_parent, chain_group, chain_condition, chain_waiting, trace_parent)
            SELECT u.id, u.[definition], u.subject_kind, u.subject_id, u.partition_key, u.args, u.args_version, SYSUTCDATETIME(), SYSUTCDATETIME(),
                {ForRoot(due)}, {ForRoot(calendarAt)}, {ForRoot(calendar)}, {ForRoot(blocked)}, {ForRoot(closed)}, NEWID(),
                {PausedCopy("u.[definition]", "u.subject_kind", "u.subject_id")},
                @chain, @generation, u.parent, u.grp, u.condition, CASE WHEN u.id = @root THEN 0 ELSE 1 END, u.trace_parent
            FROM OPENJSON(@steps) WITH (
                id uniqueidentifier '$.i', [definition] nvarchar(200) '$.d', subject_kind nvarchar(100) '$.sk', subject_id nvarchar(400) '$.si',
                partition_key nvarchar(400) '$.p', args varbinary(max) '$.a', args_version int '$.v', parent uniqueidentifier '$.pa',
                grp uniqueidentifier '$.g', condition smallint '$.c', trace_parent nvarchar(100) '$.t') AS u
            ORDER BY u.id;
            """, [.. parameters]);
        await insert.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>The due expression of a one-shot schedule, adding the parameters it reads.</summary>
    private static string Timing(Schedule schedule, List<SqlParameter> parameters)
    {
        switch (schedule.Kind)
        {
            case ScheduleKind.Now:
                return "SYSUTCDATETIME()";
            case ScheduleKind.At:
                parameters.Add(Tsql.Time("@at", schedule.Time));
                return "@at";
            case ScheduleKind.After or ScheduleKind.Debounce:
                parameters.Add(Tsql.Ticks("@delay", schedule.Delay));
                return Tsql.AddTicks("SYSUTCDATETIME()", "@delay");
            default:
                throw new ArgumentException($"A request takes a one-shot schedule, not {schedule.Kind}.", nameof(schedule));
        }
    }

    /// <summary>
    /// Locks the running step, then checks its lease by a clock reading taken after the lock is held, then adds the
    /// groups, marked dynamic, and the steps that follow it under the chain's current generation, counts the new nodes
    /// of the step's own enclosing group into its pending count, and records the definitions of the new steps with the
    /// chain under the chain row's lock.
    /// </summary>
    public async ValueTask AddGroupAsync(WorkTransaction transaction, Lease<JobId> lease, IReadOnlyList<ChainStepData> steps, IReadOnlyList<ChainGroupData> groups, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(steps);
        ArgumentNullException.ThrowIfNull(groups);
        var work = SqlServerTransaction.From(transaction);
        var sql = await SqlAsync(lease.Source, cancellationToken);
        Guid chain;
        Guid? scope;
        long generation;
        await using (var read = SqlServerLeaseStore.KeyCommand(work.Connection, work.Transaction, sql, lease.Source, lease.Key, $"""
            DECLARE @held int;
            SELECT @held = 1 FROM {sql.Table} AS r WITH (UPDLOCK, ROWLOCK) WHERE {sql.KeyMatch};
            SELECT r.chain_id, r.chain_group, r.chain_generation FROM {sql.Table} AS r WHERE {sql.KeyMatch} AND r.state = 0 AND {sql.Valid};
            """,
            Tsql.Guid("@owner", lease.Owner.Value),
            Tsql.Int64("@epoch", lease.Epoch)))
        await using (var reader = await read.ExecuteReaderAsync(cancellationToken))
        {
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
        await using var count = Tsql.Command(work, $"""
            {(scope is null ? "" : $"UPDATE {chainGroups} SET pending = pending + @pending WHERE id = @group;")}
            DECLARE @chain_held int;
            SELECT @chain_held = 1 FROM {chains} WITH (UPDLOCK, ROWLOCK) WHERE id = @chain;
            {(scope is null ? $"UPDATE {chains} SET pending = pending + @pending WHERE id = @chain;" : "")}
            INSERT INTO {chainDefinitions} (chain_id, [definition]) SELECT DISTINCT @chain, d.value FROM OPENJSON(@definitions) WITH (value nvarchar(200) '$') AS d
            WHERE NOT EXISTS (SELECT 1 FROM {chainDefinitions} AS e WHERE e.chain_id = @chain AND e.[definition] = d.value);
            """,
            Tsql.Guid("@chain", chain),
            Tsql.Guid("@group", scope),
            Tsql.Int32("@pending", Pending(addedSteps, added, scope)),
            Tsql.Strings("@definitions", addedSteps.Select(s => s.Definition).Distinct(StringComparer.Ordinal)));
        await count.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Settles a chain step that has just ended, in its terminal transaction: a skipped root records every waiting step
    /// of the chain to be skipped; otherwise the step's children are decided by its outcome and the empty groups that
    /// follow it settle as succeeded. Then its enclosing group, or the chain's top level, counts it settled.
    /// </summary>
    private async Task SettleStepAsync(SqlServerTransaction work, Guid chain, Guid step, Guid? group, bool root, JobOutcome outcome, CancellationToken cancellationToken)
    {
        if (root && outcome == JobOutcome.Skipped)
        {
            await using var skip = Tsql.Command(work, $"""
                DECLARE @waiting TABLE (id uniqueidentifier NOT NULL PRIMARY KEY);
                INSERT INTO @waiting (id) SELECT j.id FROM {Jobs} AS j WITH (INDEX(jobs_chain)) WHERE j.chain_id IS NOT NULL AND j.chain_id = @chain AND j.state = 0 AND j.chain_waiting = 1;
                DECLARE @skipped TABLE ([definition] nvarchar(200) COLLATE {Tsql.Binary} NOT NULL);
                UPDATE r SET chain_waiting = 0, skip_pending = 1, skip_reason = @reason OUTPUT inserted.[definition] INTO @skipped
                FROM @waiting AS w INNER LOOP JOIN {Jobs} AS r WITH (INDEX(jobs_key), FORCESEEK) ON r.id = w.id
                WHERE r.chain_id = @chain AND r.state = 0 AND r.chain_waiting = 1;
                SELECT DISTINCT [definition] FROM @skipped;
                """,
                Tsql.Guid("@chain", chain),
                Tsql.Text("@reason", JobReasons.RootSkipped));
            var definitions = new List<string>();
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
            await using var empty = Tsql.Command(work, $"""
                DECLARE @empty TABLE (id uniqueidentifier NOT NULL PRIMARY KEY);
                INSERT INTO @empty (id) SELECT g.id FROM {chainGroups} AS g WHERE g.chain_id = @chain AND g.parent = @step AND g.pending = 0 AND g.settled_at IS NULL;
                DECLARE @locked int;
                SELECT @locked = COUNT(*) FROM @empty AS e INNER LOOP JOIN {chainGroups} AS g WITH (UPDLOCK, ROWLOCK, FORCESEEK) ON g.id = e.id;
                DECLARE @settled TABLE (id uniqueidentifier NOT NULL, enclosing_group uniqueidentifier);
                UPDATE g SET settled_at = SYSUTCDATETIME(), outcome = @outcome OUTPUT inserted.id, inserted.enclosing_group INTO @settled
                FROM @empty AS e INNER LOOP JOIN {chainGroups} AS g WITH (FORCESEEK) ON g.id = e.id WHERE g.pending = 0 AND g.settled_at IS NULL;
                SELECT id, enclosing_group FROM @settled ORDER BY id;
                """,
                Tsql.Guid("@chain", chain),
                Tsql.Guid("@step", step),
                Tsql.Int16("@outcome", (short)JobOutcome.Completed));
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
    /// Decides the waiting children of a settled step or group: those whose condition holds for
    /// <paramref name="settled"/> become due now, the others are recorded to be skipped by their definition's
    /// maintenance, which runs their outcome hook. They are found without locks, locked in primary-key order and decided
    /// when they still wait. Signals the claimers and planners of their definitions.
    /// </summary>
    private async Task DecideAsync(SqlServerTransaction work, Guid chain, Guid node, ChainOutcomes settled, CancellationToken cancellationToken)
    {
        await using var decide = Tsql.Command(work, $"""
            DECLARE @children TABLE (id uniqueidentifier NOT NULL PRIMARY KEY);
            INSERT INTO @children (id) SELECT j.id FROM {Jobs} AS j WITH (INDEX(jobs_chain)) WHERE j.chain_id IS NOT NULL AND j.chain_id = @chain AND j.chain_parent = @node
                AND j.state = 0 AND j.chain_waiting = 1;
            DECLARE @locked int;
            SELECT @locked = COUNT(*) FROM @children AS c INNER LOOP JOIN {Jobs} AS r WITH (UPDLOCK, ROWLOCK, INDEX(jobs_key), FORCESEEK) ON r.id = c.id;
            DECLARE @decided TABLE ([definition] nvarchar(200) COLLATE {Tsql.Binary} NOT NULL, runs bit NOT NULL);
            UPDATE r SET chain_waiting = 0, due_at = CASE WHEN r.chain_condition & @settled <> 0 THEN SYSUTCDATETIME() END,
                skip_pending = CASE WHEN r.chain_condition & @settled = 0 THEN 1 ELSE 0 END, skip_reason = CASE WHEN r.chain_condition & @settled = 0 THEN @reason END
            OUTPUT inserted.[definition], {Tsql.Bit("inserted.chain_condition & @settled <> 0")} INTO @decided
            FROM @children AS c INNER LOOP JOIN {Jobs} AS r WITH (INDEX(jobs_key), FORCESEEK) ON r.id = c.id
            WHERE r.chain_id = @chain AND r.chain_parent = @node AND r.state = 0 AND r.chain_waiting = 1;
            SELECT DISTINCT [definition], runs FROM @decided;
            """,
            Tsql.Guid("@chain", chain),
            Tsql.Guid("@node", node),
            Tsql.Int16("@settled", (short)settled),
            Tsql.Text("@reason", JobReasons.ChainCondition));
        var signals = new List<(string, SignalTopic)>();
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
    /// Counts one settled node of rank <paramref name="rank"/> in <paramref name="group"/>, or in the chain's top level:
    /// locks the row, then counts and, when nothing is pending any longer, settles it with a clock reading taken after
    /// the lock is held. A settled group decides its continuations and is counted in its own enclosing group with the
    /// rank of its aggregate. A settled chain records the outcome its owning occurrence ends with and signals that
    /// occurrence's planners, and a chain whose deletion was requested is deleted.
    /// </summary>
    private async Task CountSettledAsync(SqlServerTransaction work, Guid chain, Guid? group, short rank, CancellationToken cancellationToken)
    {
        const string settles = "pending = 1";
        var aggregate = $"CASE GREATEST(worst, @rank) WHEN 0 THEN {(short)JobOutcome.Skipped} WHEN 1 THEN {(short)JobOutcome.Completed} "
            + $"WHEN 2 THEN {(short)JobOutcome.Cancelled} ELSE {(short)JobOutcome.Failed} END";
        string Count(string table, string columns) => $"""
            DECLARE @counted_held int;
            SELECT @counted_held = 1 FROM {table} WITH (UPDLOCK, ROWLOCK) WHERE id = @id;
            DECLARE @counted TABLE (pending int NOT NULL, outcome smallint, {columns});
            UPDATE {table} SET pending = pending - 1, worst = GREATEST(worst, @rank), settled_at = CASE WHEN {settles} THEN SYSUTCDATETIME() END,
                outcome = CASE WHEN {settles} THEN {aggregate} END
            OUTPUT inserted.pending, inserted.outcome, {string.Join(", ", columns.Split(", ").Select(c => "inserted." + c.Split(' ')[0]))} INTO @counted
            WHERE id = @id;
            SELECT * FROM @counted;
            """;
        while (group is { } current)
        {
            await using var command = Tsql.Command(work, Count(chainGroups, "enclosing_group uniqueidentifier"),
                Tsql.Guid("@id", current),
                Tsql.Int16("@rank", rank));
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
        JobOutcome settledOutcome;
        Guid? owner;
        string? ownerDefinition;
        bool delete;
        await using (var command = Tsql.Command(work, Count(chains, "owner_job uniqueidentifier, owner_definition nvarchar(200), delete_requested bit"),
            Tsql.Guid("@id", chain),
            Tsql.Int16("@rank", rank)))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            await reader.ReadAsync(cancellationToken);
            if (reader.GetInt32(0) > 0)
            {
                return;
            }
            settledOutcome = (JobOutcome)reader.GetInt16(1);
            owner = reader.IsDBNull(2) ? null : reader.GetGuid(2);
            ownerDefinition = reader.IsDBNull(3) ? null : reader.GetString(3);
            delete = reader.GetBoolean(4);
        }
        if (owner is { } occurrence)
        {
            await using var record = Tsql.Command(work,
                $"UPDATE {Jobs} SET chain_outcome = @outcome WHERE id = @id AND state = 0 AND owned_chain = @chain;\nSELECT @@ROWCOUNT;",
                Tsql.Guid("@id", occurrence),
                Tsql.Guid("@chain", chain),
                Tsql.Int16("@outcome", (short)settledOutcome));
            if (Convert.ToInt32(await record.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture) == 1)
            {
                await leases.NotifyAsync(work.Connection, work.Transaction, WorkTopics.Plan, JobDefinition.SourceNameOf(ownerDefinition!), cancellationToken);
            }
        }
        if (delete)
        {
            await DeleteChainRowsAsync(work, chain, cancellationToken);
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

    /// <summary>Deletes a chain's steps, in primary-key order, and the chain with its groups and definitions.</summary>
    private async Task DeleteChainRowsAsync(SqlServerTransaction work, Guid chain, CancellationToken cancellationToken)
    {
        await using var command = Tsql.Command(work, $"""
            DECLARE @steps TABLE (id uniqueidentifier NOT NULL PRIMARY KEY);
            INSERT INTO @steps (id) SELECT j.id FROM {Jobs} AS j WITH (INDEX(jobs_chain)) WHERE j.chain_id IS NOT NULL AND j.chain_id = @chain;
            DELETE r FROM @steps AS s INNER LOOP JOIN {Jobs} AS r WITH (INDEX(jobs_key), FORCESEEK) ON r.id = s.id WHERE r.chain_id = @chain;
            DELETE FROM {chains} WHERE id = @chain;
            """, Tsql.Guid("@chain", chain));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>A JSON array whose elements <paramref name="write"/> writes.</summary>
    private static string Json(Action<Utf8JsonWriter> write)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            write(writer);
            writer.WriteEndArray();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }
}
