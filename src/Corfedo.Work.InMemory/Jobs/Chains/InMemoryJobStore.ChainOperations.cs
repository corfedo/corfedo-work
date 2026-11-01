using Corfedo.Work.InMemory.Database;
using static Corfedo.Work.InMemory.Leases.InMemoryLeaseStore;

namespace Corfedo.Work.InMemory.Jobs;

internal sealed partial class InMemoryJobStore
{
    public ValueTask<StoredJobOutcome?> GetParentOutcomeAsync(JobId step, string definition, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        return ValueTask.FromResult(database.ReadCommitted(() =>
            Committed(step.Value)?.Uuid(JobColumns.ChainParent) is { } parent && Committed(parent) is { } r && r.Text(JobColumns.Definition) == definition
                ? StoredOutcome(r)
                : null));
    }

    /// <summary>The steps of the group the step follows that no step or group of the group follows, from the chain's steps.</summary>
    public ValueTask<IReadOnlyList<StoredJobOutcome>> GetGroupOutcomesAsync(JobId step, string definition, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        return ValueTask.FromResult<IReadOnlyList<StoredJobOutcome>>(database.ReadCommitted(() =>
        {
            if (Committed(step.Value) is not { } s || s.Uuid(JobColumns.ChainId) is not { } chain || s.Uuid(JobColumns.ChainParent) is not { } group
                || application.Chains.Committed.GetValueOrDefault(chain) is not { } row)
            {
                return [];
            }
            var steps = row.Steps.Select(Committed).OfType<Record>().ToList();
            var groupParents = row.Groups.Select(application.ChainGroups.Committed.GetValueOrDefault).OfType<ChainGroupRow>().Select(g => g.Parent).ToHashSet();
            return steps
                .Where(j => j.Uuid(JobColumns.ChainGroup) == group && j.Text(JobColumns.Definition) == definition
                    && !steps.Any(n => n.Uuid(JobColumns.ChainParent) == j.Uuid(JobColumns.Id) && n.Uuid(JobColumns.ChainGroup) == group)
                    && !groupParents.Contains(j.Uuid(JobColumns.Id)!.Value))
                .Select(StoredOutcome)
                .ToList();
        }));
    }

    /// <summary>The chain row, its steps in creation order and its groups, in one snapshot.</summary>
    public ValueTask<ChainStatus?> GetChainAsync(ChainId chain, CancellationToken cancellationToken) =>
        ValueTask.FromResult(database.ReadCommitted(() =>
        {
            if (application.Chains.Committed.GetValueOrDefault(chain.Value) is not { } row)
            {
                return null;
            }
            var steps = row.Steps.Select(Committed).OfType<Record>().Select(r => new ChainStepStatus(
                r.JobId(),
                r.Text(JobColumns.Definition)!,
                r.Int64(JobColumns.ChainGeneration),
                r.Uuid(JobColumns.ChainParent),
                r.Uuid(JobColumns.ChainGroup),
                r.Bool(JobColumns.ChainWaiting),
                !r.Outstanding(),
                r[JobColumns.Outcome] is int outcome ? (JobOutcome)outcome : null,
                r.Text(JobColumns.Reason))).ToList();
            var groups = row.Groups
                .Select(id => (Id: id, Row: application.ChainGroups.Committed.GetValueOrDefault(id)))
                .Where(g => g.Row is not null)
                .OrderBy(g => g.Id)
                .Select(g => new ChainGroupStatus(g.Id, g.Row!.Generation, g.Row.Parent, g.Row.EnclosingGroup, g.Row.Pending, g.Row.SettledAt is not null, g.Row.Outcome))
                .ToList();
            return new ChainStatus(chain, row.Generation, new JobId(row.RootJob), row.OwnerJob is { } owner ? new JobId(owner) : null, row.SettledAt is not null, row.Outcome, row.CreatedAt, row.SettledAt, steps, groups);
        }));

    /// <summary>The committed job with the id; call under the database's lock.</summary>
    private Record? Committed(Guid job) => JobRows.Committed.GetValueOrDefault(Key(job));

    /// <summary>
    /// Requires the chain settled. The reset set is the node with its dependants, a group with its branches; every group
    /// enclosing a reset node is reopened, which resets its continuations as well, up to the chain's top level. Reset
    /// steps are outstanding again under the new generation with new execution ids and fresh attempts, waiting for their
    /// parents, keeping their ended execution's outcome; the chosen step is due now, or the branches of a chosen group are
    /// decided by the outcome of the step it follows. A dynamic group added by a reset step is deleted with its steps and
    /// groups instead, since the step adds it again when it runs. Reopened groups count their reset nodes as pending and
    /// keep the worst outcome of the others.
    /// </summary>
    public async ValueTask<RetryFromOutcome> RetryFromAsync(ChainId chain, Guid node, CancellationToken cancellationToken)
    {
        using var work = await database.BeginAsync(cancellationToken);
        if (work.Get(application.Chains, chain.Value) is not { } row)
        {
            return RetryFromOutcome.NotFound;
        }
        if (row.SettledAt is null)
        {
            return RetryFromOutcome.NotSettled;
        }
        var generation = row.Generation + 1;
        var nodes = new Dictionary<Guid, ChainNode>();
        foreach (var r in ChainSteps(work, chain.Value))
        {
            var id = r.Uuid(JobColumns.Id)!.Value;
            nodes[id] = new ChainNode(
                id,
                false,
                r.Uuid(JobColumns.ChainParent),
                r.Uuid(JobColumns.ChainGroup),
                r[JobColumns.Outcome] is int outcome ? (JobOutcome)outcome : null,
                (ChainOutcomes)r.Int32(JobColumns.ChainCondition),
                r.Text(JobColumns.Definition),
                false);
        }
        foreach (var (id, group) in ChainGroups(work, chain.Value))
        {
            nodes[id] = new ChainNode(id, true, group.Parent, group.EnclosingGroup, group.Outcome, ChainOutcomes.None, null, group.Dynamic);
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
        var due = new HashSet<Guid>();
        var skipped = new HashSet<Guid>();
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
        (int Pending, int Worst) Count(Guid? scope)
        {
            var scoped = nodes.Values.Where(n => n.Group == scope).ToList();
            var open = scoped.Count(n => reset.Contains(n.Id) || reopened.Contains(n.Id));
            var worst = scoped.Where(n => !reset.Contains(n.Id) && !reopened.Contains(n.Id) && n.Outcome is not null).Select(n => Rank(n.Outcome!.Value)).DefaultIfEmpty(0).Max();
            return (open, worst);
        }
        foreach (var id in discarded)
        {
            work.Remove(JobRows, Key(id));
            work.Remove(application.ChainGroups, id);
        }
        var now = Now;
        foreach (var id in reset.Where(id => !nodes[id].IsGroup))
        {
            var r = Job(work, id)!;
            var calendar = r.Text(JobColumns.EndedCalendar);
            var isDue = due.Contains(id);
            var isSkipped = skipped.Contains(id);
            Write(work, r, [
                Set(JobColumns.State, 0),
                Set(JobColumns.ChainGeneration, generation),
                Set(JobColumns.ChainWaiting, !(isDue || isSkipped)),
                Set(LeaseFields.Default.DueAt, isDue && calendar is null ? now : null),
                Set(JobColumns.CalendarAt, isDue && calendar is not null ? now : null),
                Set(JobColumns.CalendarBlocked, calendar is not null ? (int)Blocked.MoveForward : null),
                Set(JobColumns.Calendar, calendar),
                Set(JobColumns.EndedCalendar, null),
                Set(JobColumns.WindowEnd, null),
                Set(JobColumns.CalendarUnavailable, false),
                Set(JobColumns.SkipPending, isSkipped),
                Set(JobColumns.SkipReason, isSkipped ? JobReasons.ChainCondition : null),
                Set(LeaseFields.Default.ExecutionId, Guid.NewGuid()),
                Set(JobColumns.FirstRequestedAt, now),
                Set(JobColumns.Attempts, 0),
                Set(JobColumns.Started, false),
                Set(LeaseFields.Default.Takeovers, 0),
                Set(LeaseFields.Default.Failures, 0),
                Set(LeaseFields.Default.CancelRequestedAt, null),
                Set(JobColumns.ProgressPercent, null),
                Set(JobColumns.ProgressMessage, null),
                Set(JobColumns.Paused, SubjectPaused(work, r)),
            ]);
        }
        foreach (var id in reset.Concat(reopened).Where(id => nodes[id].IsGroup))
        {
            var (pending, worst) = Count(id);
            var group = work.Get(application.ChainGroups, id)!;
            work.Put(application.ChainGroups, id, group with { Generation = generation, SettledAt = null, Outcome = null, Pending = pending, Worst = worst });
        }
        var top = Count(null);
        work.Put(application.Chains, chain.Value, row with
        {
            Generation = generation,
            SettledAt = null,
            Outcome = null,
            Pending = top.Pending,
            Worst = top.Worst,
            Steps = [.. row.Steps.Where(id => !discarded.Contains(id))],
            Groups = [.. row.Groups.Where(id => !discarded.Contains(id))],
        });
        if (chosen.IsGroup && !members[chosen.Id].Any())
        {
            // A group without branches settles as succeeded at once.
            var group = work.Get(application.ChainGroups, chosen.Id)!;
            work.Put(application.ChainGroups, chosen.Id, group with { SettledAt = now, Outcome = JobOutcome.Completed });
            Decide(work, chain.Value, chosen.Id, ChainOutcomes.Succeeded);
            CountSettled(work, chain.Value, chosen.Group, Rank(JobOutcome.Completed));
        }
        foreach (var definition in due.Select(id => nodes[id].Definition!).Distinct(StringComparer.Ordinal))
        {
            leases.Signal(work, WorkTopics.Due, JobDefinition.SourceNameOf(definition));
            leases.Signal(work, WorkTopics.Plan, JobDefinition.SourceNameOf(definition));
        }
        foreach (var definition in skipped.Select(id => nodes[id].Definition!).Distinct(StringComparer.Ordinal))
        {
            leases.Signal(work, WorkTopics.Plan, JobDefinition.SourceNameOf(definition));
        }
        work.Commit();
        return RetryFromOutcome.Retried;
    }

    /// <summary>
    /// Cancels the chain's outstanding steps that are not to be skipped, in the order subject cancellation ends them,
    /// after refusing the cancellation when one belongs to a definition <paramref name="definitions"/> does not resolve.
    /// Steps recorded to be skipped are left to finalization.
    /// </summary>
    public async ValueTask<int> CancelChainAsync(IWorkTransaction transaction, ChainId chain, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definitions);
        return await CancelStepsAsync(Join(transaction), chain.Value, definitions, cancellationToken);
    }

    private async Task<int> CancelStepsAsync(InMemoryTransaction work, Guid chain, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken)
    {
        var locked = SelectForCancel(work, ChainSteps(work, chain).ToList());
        await CancelSelectedAsync(work, locked, definitions, cancellationToken);
        return locked.Selected.Count;
    }

    /// <summary>
    /// A settled chain is deleted. With a cascade, an unsettled chain's steps are cancelled first and the chain is marked
    /// to be deleted by the transition that settles it, or deleted now when the cancellations settled it.
    /// </summary>
    public async ValueTask<ChainDeleteOutcome> DeleteChainAsync(ChainId chain, ChainDelete mode, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definitions);
        using var work = await database.BeginAsync(cancellationToken);
        if (work.Get(application.Chains, chain.Value) is not { } row)
        {
            return ChainDeleteOutcome.NotFound;
        }
        if (row.SettledAt is null && mode == ChainDelete.Cascade)
        {
            await CancelStepsAsync(work, chain.Value, definitions, cancellationToken);
            if (work.Get(application.Chains, chain.Value) is { SettledAt: null } unsettled)
            {
                work.Put(application.Chains, chain.Value, unsettled with { DeleteRequested = true });
                work.Commit();
                return ChainDeleteOutcome.Deleting;
            }
        }
        switch (work.Get(application.Chains, chain.Value))
        {
            case null:
                return ChainDeleteOutcome.NotFound;
            case { SettledAt: null }:
                return ChainDeleteOutcome.Unsettled;
        }
        DeleteChainRows(work, chain.Value);
        work.Commit();
        return ChainDeleteOutcome.Deleted;
    }

    /// <summary>A step or group of a chain as a retry reads it; a group has no condition or definition.</summary>
    private sealed record ChainNode(Guid Id, bool IsGroup, Guid? Parent, Guid? Group, JobOutcome? Outcome, ChainOutcomes Condition, string? Definition, bool Dynamic);
}
