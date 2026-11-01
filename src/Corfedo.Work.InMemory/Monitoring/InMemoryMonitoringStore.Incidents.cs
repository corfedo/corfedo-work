using Corfedo.Work.InMemory.Database;
using Corfedo.Work.InMemory.Jobs;
using static Corfedo.Work.InMemory.Leases.InMemoryLeaseStore;

namespace Corfedo.Work.InMemory.Monitoring;

internal sealed partial class InMemoryMonitoringStore
{
    /// <summary>
    /// Opens one incident per key with a recurring-chain occurrence of the definition that started more than the limit
    /// ago, naming its earliest such occurrence, and finds the earliest start among occurrences still within the limit.
    /// </summary>
    public async ValueTask<RunningCheck> CheckRunningAsync(string definition, TimeSpan limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        using var work = await database.BeginAsync(cancellationToken);
        var now = Now;
        var running = work.Scan(application.Jobs.Rows)
            .Select(r => r.Row)
            .Where(r => r.Text(JobColumns.Definition) == definition && r.Uuid(JobColumns.OwnedChain) is not null && r.Outstanding()
                && r.Time(JobColumns.AttemptStartedAt) is not null)
            .ToList();
        var opened = new List<WorkIncident>();
        foreach (var earliest in running
            .Where(r => r.Time(JobColumns.AttemptStartedAt) <= now - limit)
            .GroupBy(r => (r.Text(JobColumns.SubjectKind), r.Text(JobColumns.SubjectId)))
            .Select(g => g.MinBy(r => r.Time(JobColumns.AttemptStartedAt))!))
        {
            if (Insert(work, WorkIncidentKind.RunsAtMost, definition, earliest.Subject(), null, earliest.Uuid(JobColumns.Id), now) is { } incident)
            {
                opened.Add(incident);
            }
        }
        var next = running.Select(r => r.Time(JobColumns.AttemptStartedAt)).Where(t => t > now - limit).Min() + limit;
        if (opened.Count > 0)
        {
            Notify(work);
        }
        work.Commit();
        WorkTelemetry.Count(new IncidentChanges(opened, []));
        return new RunningCheck(opened, next);
    }

    /// <summary>
    /// While the lease is valid and its attempt open, reads the stored running limit and, once the attempt reached it,
    /// opens the incident. The remaining time is computed with the store's clock.
    /// </summary>
    public async ValueTask<TimeSpan?> CheckAttemptAsync(Lease<JobId> lease, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        var rules = leases.Rules(lease.Source);
        using var work = await database.BeginAsync(cancellationToken);
        var now = Now;
        TimeSpan? remaining = null;
        var opened = new List<WorkIncident>();
        if (rules.Find(work, new RowKey([lease.Key.Value])) is { } r && rules.Valid(r, lease.Owner, lease.Epoch, now, 0)
            && r.Time(JobColumns.AttemptStartedAt) is { } started
            && work.Get(application.Definitions, r.Text(JobColumns.Definition)!)?.Stored is { } stored
            && Expectation(stored)?.RunningLimit is { } limit)
        {
            var left = started + limit - now;
            if (left > TimeSpan.Zero)
            {
                remaining = left;
            }
            else if (Insert(work, WorkIncidentKind.RunsAtMost, stored.Key, r.Subject(), null, lease.Key.Value, now) is { } incident)
            {
                opened.Add(incident);
                Notify(work);
            }
        }
        work.Commit();
        WorkTelemetry.Count(new IncidentChanges(opened, []));
        return remaining;
    }

    /// <summary>Resolves the open incidents of the removed kinds; with <paramref name="resetFailures"/> resets the definition's failures.</summary>
    public async ValueTask<IncidentChanges> ResolveRemovedAsync(string definition, IReadOnlyCollection<WorkIncidentKind> kinds, bool resetFailures, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(kinds);
        using var work = await database.BeginAsync(cancellationToken);
        var now = Now;
        var removed = kinds.Select(k => (int)k).ToHashSet();
        var resolved = AllIncidents(work)
            .Where(i => i.Open() && (string?)i[IncidentColumns.Definition] == definition && removed.Contains(i.Int32(IncidentColumns.Kind)))
            .ToList()
            .Select(i => Resolve(work, i, now))
            .ToList();
        if (resetFailures)
        {
            foreach (var (key, s) in Rows(work, definition).Where(s => s.Row.Failures > 0).ToList())
            {
                work.Put(application.Expectations, key, s with { Failures = 0 });
            }
        }
        if (resolved.Count > 0)
        {
            Notify(work);
        }
        work.Commit();
        var changes = resolved.Count == 0 ? IncidentChanges.None : new IncidentChanges([], resolved);
        WorkTelemetry.Count(changes);
        return changes;
    }

    /// <summary>Opens the wanted date-set incidents that are not open and resolves the open ones no longer wanted.</summary>
    public async ValueTask<IncidentChanges> SyncDateSetsAsync(IReadOnlyCollection<string> runningOut, IReadOnlyCollection<string> waiting, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(runningOut);
        ArgumentNullException.ThrowIfNull(waiting);
        using var work = await database.BeginAsync(cancellationToken);
        var now = Now;
        var want = runningOut.Select(s => (Kind: WorkIncidentKind.DateSetRunningOut, Set: s))
            .Concat(waiting.Select(s => (Kind: WorkIncidentKind.DateSetWorkWaiting, Set: s)))
            .ToHashSet();
        var resolved = AllIncidents(work)
            .Where(i => i.Open() && i[IncidentColumns.Definition] is null && i[IncidentColumns.SubjectKind] is null
                && (WorkIncidentKind)i.Int32(IncidentColumns.Kind) is WorkIncidentKind.DateSetRunningOut or WorkIncidentKind.DateSetWorkWaiting
                && !want.Contains(((WorkIncidentKind)i.Int32(IncidentColumns.Kind), (string)i[IncidentColumns.DateSet]!)))
            .ToList()
            .Select(i => Resolve(work, i, now))
            .ToList();
        var opened = want.Select(w => Insert(work, w.Kind, null, null, w.Set, null, now)).OfType<WorkIncident>().ToList();
        var changes = new IncidentChanges(opened, resolved);
        if (changes.Any)
        {
            Notify(work);
        }
        work.Commit();
        WorkTelemetry.Count(changes);
        return changes;
    }

    /// <summary>
    /// Deletes resolved incidents by their resolution time, expectation rows without a deadline or failures whose key
    /// has no open incident, and queued key changes of stored definitions that expect no completions, which only the
    /// monitoring loop deletes otherwise; each in its own transaction.
    /// </summary>
    public async ValueTask<int> RetainAsync(TimeSpan retention, int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var incidents = await DeleteAsync(work =>
        {
            var horizon = Now - retention;
            var expired = AllIncidents(work).Where(i => i.Time(IncidentColumns.ResolvedAt) < horizon).OrderBy(i => i.Time(IncidentColumns.ResolvedAt)).Take(limit).ToList();
            foreach (var i in expired)
            {
                work.Remove(IncidentRows, new RowKey([i.Uuid(IncidentColumns.Id)!.Value]));
            }
            return expired.Count;
        });
        var rows = await DeleteAsync(work =>
        {
            var open = AllIncidents(work).Where(i => i.Open()).ToList();
            var idle = work.Scan(application.Expectations)
                .Where(s => s.Row.DeadlineAt is null && s.Row.Failures == 0
                    && !open.Any(i => OfKey(i, s.Key.Definition, s.Key.SubjectKind, s.Key.SubjectId)))
                .Take(limit)
                .Select(s => s.Key)
                .ToList();
            foreach (var key in idle)
            {
                work.Remove(application.Expectations, key);
            }
            return idle.Count;
        });
        var changes = await DeleteAsync(work =>
        {
            var queued = work.Scan(application.ExpectationChanges)
                .Where(c => work.Get(application.Definitions, c.Row.Definition)?.Stored is { } stored && Expectation(stored)?.Deadline is null)
                .Take(limit)
                .Select(c => c.Key)
                .ToList();
            foreach (var key in queued)
            {
                work.Remove(application.ExpectationChanges, key);
            }
            return queued.Count;
        });
        return Math.Max(incidents, Math.Max(rows, changes));

        async Task<int> DeleteAsync(Func<InMemoryTransaction, int> delete)
        {
            using var work = await database.BeginAsync(cancellationToken);
            var count = delete(work);
            work.Commit();
            return count;
        }
    }

    /// <summary>Open incidents by kind, and per stored definition the time since its oldest due job became due, zero without one.</summary>
    public ValueTask<MonitoringGauges> GetGaugesAsync(CancellationToken cancellationToken)
    {
        var now = Now;
        return ValueTask.FromResult(database.ReadCommitted(() =>
        {
            var open = IncidentRows.Committed.Values
                .Where(i => i.Open())
                .GroupBy(i => (WorkIncidentKind)i.Int32(IncidentColumns.Kind))
                .ToDictionary(g => g.Key, g => (long)g.Count());
            var oldest = application.Jobs.Rows.Committed.Values
                .Where(j => j.Time(LeaseFields.Default.LeaseExpiresAt) is null && j.Time(LeaseFields.Default.DueAt) is not null && !j.Bool(JobColumns.Paused))
                .GroupBy(j => j.Text(JobColumns.Definition)!, StringComparer.Ordinal)
                .ToDictionary(g => g.Key, g => g.Min(j => j.Time(LeaseFields.Default.DueAt)!.Value), StringComparer.Ordinal);
            var due = application.Definitions.Committed.Keys.ToDictionary(
                key => key,
                key => oldest.TryGetValue(key, out var at) && at < now ? now - at : TimeSpan.Zero,
                StringComparer.Ordinal);
            return new MonitoringGauges(open, due);
        }));
    }
}
