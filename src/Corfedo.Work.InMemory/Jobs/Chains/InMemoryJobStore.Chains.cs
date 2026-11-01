using System.Runtime.CompilerServices;
using Corfedo.Work.InMemory.Database;
using static Corfedo.Work.InMemory.Leases.InMemoryLeaseStore;

namespace Corfedo.Work.InMemory.Jobs;

/// <summary>
/// Chains on the job table: every step is a job record from the request on, and the application's chain and group
/// rows keep each chain's top level and groups with one pending count and the worst outcome among their steps that
/// ran. A step's terminal transition decides its children, settles empty groups that follow it and counts itself in its
/// enclosing group, in its own transaction.
/// </summary>
internal sealed partial class InMemoryJobStore
{
    private readonly ConditionalWeakTable<IReadOnlyList<Guid>, ILookup<Guid, Guid>> childIndex = new();

    public ValueTask RequestChainAsync(IWorkTransaction transaction, ChainData chain, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(chain);
        var work = Join(transaction);
        CheckCalendars(work, [chain.Steps[0].Request]);
        InsertChain(work, chain, null, null);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Checks the occurrence's lease and that no cancellation was requested, gives the lease up and leaves the
    /// occurrence outstanding without a due time, pointing at its chain, then creates the chain. The attempt stays open,
    /// held by the chain: the occurrence is running until its definition's maintenance ends it with the chain's outcome,
    /// which records the attempt in the run history.
    /// </summary>
    public async ValueTask<bool> StartOwnedChainAsync(Lease<JobId> lease, ChainData chain, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(chain);
        var rules = Rules(lease.Source);
        using var work = await database.BeginAsync(cancellationToken);
        if (rules.Find(work, Key(lease.Key.Value)) is not { } r || !r.Outstanding() || !rules.Valid(r, lease.Owner, lease.Epoch, Now, 0)
            || r.Time(LeaseFields.Default.CancelRequestedAt) is not null)
        {
            return false;
        }
        Write(work, r, [
            Set(LeaseFields.Default.LeaseExpiresAt, null),
            Set(LeaseFields.Default.DueAt, null),
            Set(JobColumns.WindowEnd, null),
            Set(JobColumns.OwnedChain, chain.Id.Value),
        ]);
        InsertChain(work, chain, lease.Key, rules.Job!.Definition);
        work.Commit();
        return true;
    }

    /// <summary>
    /// Inserts the chain, its groups and its steps, each group and the chain's top level with the number of its nodes
    /// pending, and signals the root's definition: claimers, or its planners when the root has a calendar.
    /// </summary>
    private void InsertChain(InMemoryTransaction work, ChainData chain, JobId? owner, string? ownerDefinition)
    {
        var root = chain.Steps[0];
        var now = Now;
        work.Put(application.Chains, chain.Id.Value, new ChainRow(
            1,
            root.Id.Value,
            owner?.Value,
            ownerDefinition,
            [.. chain.Steps.Select(s => s.Definition).Distinct(StringComparer.Ordinal)],
            [.. chain.Steps.Select(s => s.Id.Value)],
            [.. chain.Groups.Select(g => g.Id)],
            now,
            null,
            null,
            Pending(chain.Steps, chain.Groups, null),
            0,
            false));
        InsertGroups(work, chain.Id.Value, 1, chain.Steps, chain.Groups, false);
        InsertSteps(work, chain.Id.Value, 1, chain.Steps, root, now);
        leases.Signal(work, root.Request.Schedule.Calendar is null ? WorkTopics.Due : WorkTopics.Plan, JobDefinition.SourceNameOf(root.Definition));
    }

    /// <summary>The nodes, steps and groups, whose enclosing group is <paramref name="group"/>.</summary>
    private static int Pending(IReadOnlyList<ChainStepData> steps, IReadOnlyList<ChainGroupData> groups, Guid? group) =>
        steps.Count(s => s.Group == group) + groups.Count(g => g.Group == group);

    private void InsertGroups(InMemoryTransaction work, Guid chain, long generation, IReadOnlyList<ChainStepData> steps, IReadOnlyList<ChainGroupData> groups, bool dynamic)
    {
        foreach (var group in groups)
        {
            work.Put(application.ChainGroups, group.Id, new ChainGroupRow(chain, generation, group.Parent, group.Group, Pending(steps, groups, group.Id), 0, null, null, dynamic));
        }
    }

    /// <summary>
    /// Inserts the steps: <paramref name="root"/>, when given, due by its schedule, or with a calendar at its instant for
    /// its definition's planner; every other step waiting for its parent without a due time. The pause flag is copied
    /// from the stored subject pauses.
    /// </summary>
    private void InsertSteps(InMemoryTransaction work, Guid chain, long generation, IReadOnlyList<ChainStepData> steps, ChainStepData? root, DateTimeOffset now)
    {
        foreach (var step in steps)
        {
            var request = step.Request;
            var isRoot = root is not null && step.Id == root.Id;
            var schedule = request.Schedule;
            var calendar = isRoot ? schedule.Calendar : null;
            DateTimeOffset? instant = isRoot ? Timing(schedule, now).Requested : null;
            Insert(work, new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                [JobColumns.Id] = step.Id.Value,
                [JobColumns.Definition] = step.Definition,
                [JobColumns.SubjectKind] = request.Subject?.Kind,
                [JobColumns.SubjectId] = request.Subject?.Id,
                [JobColumns.Partition] = request.Partition ?? "",
                [JobColumns.Args] = request.Args,
                [JobColumns.ArgsVersion] = request.ArgsVersion,
                [JobColumns.FirstRequestedAt] = now,
                [JobColumns.CreatedAt] = now,
                [LeaseFields.Default.DueAt] = calendar is null ? instant : null,
                [JobColumns.CalendarAt] = calendar is null ? null : instant,
                [JobColumns.Calendar] = calendar,
                [JobColumns.CalendarBlocked] = calendar is null ? null : (int)schedule.Blocked,
                [JobColumns.CalendarClosed] = calendar is null ? null : (int)schedule.Closed,
                [LeaseFields.Default.ExecutionId] = Guid.NewGuid(),
                [JobColumns.Paused] = SubjectPaused(work, step.Definition, request.Subject),
                [JobColumns.ChainId] = chain,
                [JobColumns.ChainGeneration] = generation,
                [JobColumns.ChainParent] = step.Parent,
                [JobColumns.ChainGroup] = step.Group,
                [JobColumns.ChainCondition] = (int)step.Condition,
                [JobColumns.ChainWaiting] = !isRoot,
                [JobColumns.TraceParent] = request.TraceParent,
            });
        }
    }

    /// <summary>
    /// Checks the running step's lease, then adds the groups, marked dynamic, and the steps that follow it under the
    /// chain's current generation, counts the new nodes of the step's own enclosing group into its pending count, and
    /// records the definitions of the new steps with the chain.
    /// </summary>
    public ValueTask AddGroupAsync(IWorkTransaction transaction, Lease<JobId> lease, IReadOnlyList<ChainStepData> steps, IReadOnlyList<ChainGroupData> groups, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(steps);
        ArgumentNullException.ThrowIfNull(groups);
        var work = Join(transaction);
        var rules = Rules(lease.Source);
        if (rules.Find(work, Key(lease.Key.Value)) is not { } r || !r.Outstanding() || !rules.Valid(r, lease.Owner, lease.Epoch, Now, 0))
        {
            throw new LeaseLostException($"The lease on {lease.Source.Name} record {lease.Key} at epoch {lease.Epoch} is no longer valid.");
        }
        if (r.Uuid(JobColumns.ChainId) is not { } chain)
        {
            throw new InvalidOperationException($"Job {lease.Key} of {rules.Job!.Definition} is not a chain step; only a chain step adds a group.");
        }
        var scope = r.Uuid(JobColumns.ChainGroup);
        var generation = r.Int64(JobColumns.ChainGeneration);
        var added = groups.Select(g => g with { Group = g.Group ?? scope }).ToList();
        var addedSteps = steps.Select(s => s with { Group = s.Group ?? scope }).ToList();
        InsertGroups(work, chain, generation, addedSteps, added, true);
        InsertSteps(work, chain, generation, addedSteps, null, Now);
        var pending = Pending(addedSteps, added, scope);
        if (scope is { } enclosing)
        {
            var group = work.Get(application.ChainGroups, enclosing)!;
            work.Put(application.ChainGroups, enclosing, group with { Pending = group.Pending + pending });
        }
        var row = work.Get(application.Chains, chain)!;
        work.Put(application.Chains, chain, row with
        {
            Pending = scope is null ? row.Pending + pending : row.Pending,
            Definitions = [.. row.Definitions.Concat(addedSteps.Select(s => s.Definition)).Distinct(StringComparer.Ordinal)],
            Steps = [.. row.Steps, .. addedSteps.Select(s => s.Id.Value)],
            Groups = [.. row.Groups, .. added.Select(g => g.Id)],
        });
        return ValueTask.CompletedTask;
    }

    /// <summary>The chain's steps as the transaction sees them, in creation order.</summary>
    private IEnumerable<Record> ChainSteps(InMemoryTransaction work, Guid chain) =>
        (work.Get(application.Chains, chain)?.Steps ?? []).Select(id => Job(work, id)).OfType<Record>();

    /// <summary>
    /// The waiting children of a step or group: the chain's steps that follow it, as the transaction sees them. A step's
    /// parent never changes, so the steps of a chain are indexed by parent once per version of its step list.
    /// </summary>
    private IEnumerable<Record> Children(InMemoryTransaction work, Guid chain, Guid node)
    {
        if (work.Get(application.Chains, chain) is not { } row)
        {
            return [];
        }
        var index = childIndex.GetValue(row.Steps, steps => steps
            .Select(id => Job(work, id))
            .OfType<Record>()
            .Where(r => r.Uuid(JobColumns.ChainParent) is not null)
            .ToLookup(r => r.Uuid(JobColumns.ChainParent)!.Value, r => r.Uuid(JobColumns.Id)!.Value));
        return index[node].Select(id => Job(work, id)).OfType<Record>();
    }

    /// <summary>The chain's groups as the transaction sees them.</summary>
    private IEnumerable<(Guid Id, ChainGroupRow Row)> ChainGroups(InMemoryTransaction work, Guid chain) =>
        (work.Get(application.Chains, chain)?.Groups ?? []).Select(id => (id, work.Get(application.ChainGroups, id))).Where(g => g.Item2 is not null)!;

    /// <summary>
    /// Settles a chain step that has just ended, in its terminal transition: a skipped root records every waiting step
    /// of the chain to be skipped; otherwise the step's children are decided by its outcome and the empty groups that
    /// follow it settle as succeeded. Then its enclosing group, or the chain's top level, counts it settled.
    /// </summary>
    private void SettleStep(InMemoryTransaction work, Guid chain, Guid step, Guid? group, bool root, JobOutcome outcome)
    {
        if (root && outcome == JobOutcome.Skipped)
        {
            var definitions = new HashSet<string>(StringComparer.Ordinal);
            foreach (var r in ChainSteps(work, chain).Where(r => r.Outstanding() && r.Bool(JobColumns.ChainWaiting)).ToList())
            {
                Write(work, r, [Set(JobColumns.ChainWaiting, false), Set(JobColumns.SkipPending, true), Set(JobColumns.SkipReason, JobReasons.RootSkipped)]);
                definitions.Add(r.Text(JobColumns.Definition)!);
            }
            foreach (var definition in definitions)
            {
                leases.Signal(work, WorkTopics.Plan, JobDefinition.SourceNameOf(definition));
            }
        }
        else
        {
            Decide(work, chain, step, Class(outcome));
            var now = Now;
            var settled = ChainGroups(work, chain).Where(g => g.Row.Parent == step && g.Row.Pending == 0 && g.Row.SettledAt is null).ToList();
            foreach (var (id, row) in settled)
            {
                work.Put(application.ChainGroups, id, row with { SettledAt = now, Outcome = JobOutcome.Completed });
            }
            foreach (var (id, row) in settled)
            {
                Decide(work, chain, id, ChainOutcomes.Succeeded);
                CountSettled(work, chain, row.EnclosingGroup, Rank(JobOutcome.Completed));
            }
        }
        CountSettled(work, chain, group, Rank(outcome));
    }

    /// <summary>
    /// Decides the waiting children of a settled step or group: those whose condition holds for <paramref name="settled"/>
    /// become due now, the others are recorded to be skipped by their definition's maintenance, which runs their outcome
    /// hook. Signals the claimers and planners of their definitions.
    /// </summary>
    private void Decide(InMemoryTransaction work, Guid chain, Guid node, ChainOutcomes settled)
    {
        var now = Now;
        var signals = new HashSet<(string, SignalTopic)>();
        foreach (var r in Children(work, chain, node).Where(r => r.Outstanding() && r.Bool(JobColumns.ChainWaiting)).ToList())
        {
            var runs = ((ChainOutcomes)r.Int32(JobColumns.ChainCondition) & settled) != 0;
            Write(work, r, [
                Set(JobColumns.ChainWaiting, false),
                Set(LeaseFields.Default.DueAt, runs ? now : null),
                Set(JobColumns.SkipPending, !runs),
                Set(JobColumns.SkipReason, runs ? null : JobReasons.ChainCondition),
            ]);
            signals.Add((r.Text(JobColumns.Definition)!, runs ? WorkTopics.Due : WorkTopics.Plan));
        }
        foreach (var (definition, kind) in signals)
        {
            leases.Signal(work, kind, JobDefinition.SourceNameOf(definition));
        }
    }

    /// <summary>
    /// Counts one settled node of rank <paramref name="rank"/> in <paramref name="group"/>, or in the chain's top level,
    /// and settles it when nothing is pending any longer. A settled group decides its continuations and is counted in
    /// its own enclosing group with the rank of its aggregate. A settled chain records the outcome its owning occurrence
    /// ends with and signals that occurrence's planners, and a chain whose deletion was requested is deleted.
    /// </summary>
    private void CountSettled(InMemoryTransaction work, Guid chain, Guid? group, int rank)
    {
        var now = Now;
        while (group is { } current)
        {
            var row = work.Get(application.ChainGroups, current)!;
            var worst = Math.Max(row.Worst, rank);
            var settles = row.Pending == 1;
            var outcome = settles ? Aggregate(worst) : (JobOutcome?)null;
            work.Put(application.ChainGroups, current, row with { Pending = row.Pending - 1, Worst = worst, SettledAt = settles ? now : null, Outcome = outcome });
            if (!settles)
            {
                return;
            }
            Decide(work, chain, current, Class(outcome!.Value));
            rank = Rank(outcome.Value);
            group = row.EnclosingGroup;
        }
        var top = work.Get(application.Chains, chain)!;
        var topWorst = Math.Max(top.Worst, rank);
        var topSettles = top.Pending == 1;
        var settled = topSettles ? Aggregate(topWorst) : (JobOutcome?)null;
        work.Put(application.Chains, chain, top with { Pending = top.Pending - 1, Worst = topWorst, SettledAt = topSettles ? now : null, Outcome = settled });
        if (!topSettles)
        {
            return;
        }
        if (top.OwnerJob is { } occurrence
            && Job(work, occurrence) is { } owner && owner.Outstanding() && owner.Uuid(JobColumns.OwnedChain) == chain)
        {
            Write(work, owner, [Set(JobColumns.ChainOutcome, (int)settled!.Value)]);
            leases.Signal(work, WorkTopics.Plan, JobDefinition.SourceNameOf(top.OwnerDefinition!));
        }
        if (top.DeleteRequested)
        {
            DeleteChainRows(work, chain);
        }
    }

    /// <summary>A group's or chain's outcome from the worst rank among its settled nodes.</summary>
    private static JobOutcome Aggregate(int worst) => worst switch
    {
        0 => JobOutcome.Skipped,
        1 => JobOutcome.Completed,
        2 => JobOutcome.Cancelled,
        _ => JobOutcome.Failed,
    };

    /// <summary>The outcomes a condition is evaluated against for a settled node.</summary>
    private static ChainOutcomes Class(JobOutcome outcome) => outcome switch
    {
        JobOutcome.Completed => ChainOutcomes.Succeeded,
        JobOutcome.Failed or JobOutcome.TimedOut => ChainOutcomes.Failed,
        JobOutcome.Cancelled => ChainOutcomes.Cancelled,
        _ => ChainOutcomes.Skipped,
    };

    /// <summary>A settled node's weight in its group's aggregate: skipped nodes do not count, then succeeded, cancelled and failed.</summary>
    private static int Rank(JobOutcome outcome) => outcome switch
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
    private void DeleteChainRows(InMemoryTransaction work, Guid chain)
    {
        if (work.Get(application.Chains, chain) is not { } row)
        {
            return;
        }
        foreach (var step in row.Steps)
        {
            work.Remove(JobRows, Key(step));
        }
        foreach (var group in row.Groups)
        {
            work.Remove(application.ChainGroups, group);
        }
        work.Remove(application.Chains, chain);
    }

    /// <summary>
    /// Deletes up to <paramref name="limit"/> settled chains as units, oldest settled first, whose window has passed: the
    /// job retention of the owner definition, or the longest of the definitions of its steps, as stored now. Chains
    /// none of whose definitions is stored are kept.
    /// </summary>
    private int RetainChains(InMemoryTransaction work, int limit)
    {
        var retention = work.Scan(application.Definitions)
            .Where(d => d.Row.Stored.Policy.Retention is not null)
            .ToDictionary(d => d.Key, d => d.Row.Stored.Policy.Retention!.Value, StringComparer.Ordinal);
        if (retention.Count == 0)
        {
            return 0;
        }
        var now = Now;
        var expired = work.Scan(application.Chains)
            .Where(c => c.Row.SettledAt is { } settled
                && (c.Row.OwnerDefinition is { } owner ? [owner] : c.Row.Definitions).Where(retention.ContainsKey).Select(d => retention[d]).DefaultIfEmpty(TimeSpan.MaxValue).Max() is var window
                && window != TimeSpan.MaxValue
                && settled < now - window)
            .OrderBy(c => c.Row.SettledAt)
            .Take(limit)
            .Select(c => c.Key)
            .ToList();
        foreach (var chain in expired)
        {
            DeleteChainRows(work, chain);
        }
        return expired.Count;
    }
}
