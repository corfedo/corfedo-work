using Corfedo.Work.InMemory.Database;

namespace Corfedo.Work.InMemory.Jobs;

/// <summary>
/// The job definitions of registered processes and the reports of which definitions live processes cover. A process is
/// live while it has not stopped and its heartbeat plus its own stale window is after the store's clock; each report
/// reads committed state at one instant.
/// </summary>
internal sealed partial class InMemoryJobStore
{
    /// <summary>Records the definitions a registered process registers; definitions it already recorded are left as they are.</summary>
    public async ValueTask RegisterProcessAsync(OwnerId owner, IReadOnlyList<ProcessDefinition> definitions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definitions);
        if (definitions.Count == 0)
        {
            return;
        }
        using var transaction = await database.BeginAsync(cancellationToken);
        var process = transaction.Get(application.Processes, owner) ?? throw new InvalidOperationException($"Process {owner} is not registered.");
        var recorded = process.Definitions.Select(d => d.Definition).ToHashSet(StringComparer.Ordinal);
        transaction.Put(application.Processes, owner, process with { Definitions = [.. process.Definitions, .. definitions.Where(d => !recorded.Contains(d.Definition))] });
        transaction.Commit();
    }

    /// <summary>The definitions of live processes; call under the database's lock.</summary>
    private List<ProcessDefinition> LiveDefinitions(DateTimeOffset now) =>
        [.. application.Processes.Committed.Values.Where(p => p.LiveAt(now)).SelectMany(p => p.Definitions)];

    /// <summary>
    /// As of the clock less the grace, for every stored definition that is not paused, with the processes live at some
    /// instant since then: an argument version has due work when an unheld job of it with no copied pause is due or one
    /// of its leases expired, and its subject has no stored pause. A definition no live
    /// process runs is unserved when a version has due work, a job's calendar instant needs planning, a job whose copied
    /// pause no stored pause backs is due, or a recurrence whose subject has no stored pause needs its planner (a due
    /// next instant, of an after-completion schedule only without an outstanding occurrence, or a waiting occurrence
    /// without one). For a definition live processes run, a version with due work that no live runner accepts and that
    /// is not below the stored minimum is unserved.
    /// </summary>
    public ValueTask<UnservedReport> GetUnservedAsync(TimeSpan grace, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(grace, TimeSpan.Zero);
        // Every instant the report compares with is the clock less the grace.
        var now = Now - grace;
        var report = database.ReadCommitted(() =>
        {
            var live = application.Processes.Committed.Values.Where(p => p.LiveSince(now)).SelectMany(p => p.Definitions).Where(d => d.Runs).ToList();
            var pauses = application.Pauses.Committed;
            bool StoredPause(Record r) => r.PauseKey() is { } key && pauses.ContainsKey(key);
            var jobs = JobRows.Committed.Values.ToLookup(r => r.Text(JobColumns.Definition), StringComparer.Ordinal);
            var recurrences = application.Recurrences.Committed.Values.ToLookup(c => c.Definition, StringComparer.Ordinal);
            var outstanding = JobRows.Committed.Values
                .Where(r => r.Uuid(JobColumns.RecurrenceId) is not null && r.Outstanding() && !r.Bool(JobColumns.SkipPending))
                .Select(r => r.Uuid(JobColumns.RecurrenceId)!.Value)
                .ToHashSet();
            var unrun = new List<UnservedDefinition>();
            var versions = new List<UnservedVersion>();
            foreach (var stored in application.Definitions.Committed.Values.Select(d => d.Stored).Where(d => !d.Paused).OrderBy(d => d.Key, StringComparer.Ordinal))
            {
                var own = jobs[stored.Key].ToList();
                var lease = LeaseFields.Default;
                var due = own
                    .Where(r => !StoredPause(r)
                        && ((r.Time(lease.LeaseExpiresAt) is null && !r.Bool(JobColumns.Paused) && r.Time(lease.DueAt) <= now)
                            || r.Time(lease.LeaseExpiresAt) < now))
                    .Select(r => r.Int32(JobColumns.ArgsVersion))
                    .ToHashSet();
                var runners = live.Where(d => d.Definition == stored.Key).ToList();
                if (runners.Count == 0)
                {
                    var unserved = due.Count > 0
                        || own.Any(r => r.Time(JobColumns.CalendarAt) <= now && r.Outstanding() && !r.Bool(JobColumns.CalendarUnavailable) && r.Time(lease.LeaseExpiresAt) is null)
                        || own.Any(r => r.Bool(JobColumns.Paused) && r.Outstanding() && r.Time(lease.LeaseExpiresAt) is null && r.Time(lease.DueAt) <= now && !StoredPause(r))
                        || recurrences[stored.Key].Any(c =>
                            !(c.Subject is { } subject && pauses.ContainsKey(PauseKey.Of(c.Definition, subject)))
                            && ((c.NextAt <= now && (!AfterCompletion(c) || !outstanding.Contains(c.Id))) || (c.WaitingAt is not null && !outstanding.Contains(c.Id))));
                    if (unserved)
                    {
                        unrun.Add(new UnservedDefinition(stored.Key));
                    }
                    continue;
                }
                foreach (var version in due.Order())
                {
                    if (version >= stored.Policy.MinimumArgsVersion && !runners.Any(d => d.AcceptedArgs.Contains(version)))
                    {
                        versions.Add(new UnservedVersion(stored.Key, version));
                    }
                }
            }
            return new UnservedReport(unrun, versions);
        });
        return ValueTask.FromResult(report);
    }

    /// <summary>Code-defined recurrences stored by a declaring revision whose definition no live process declares.</summary>
    public ValueTask<IReadOnlyList<UndeclaredRecurrence>> GetUndeclaredRecurrencesAsync(CancellationToken cancellationToken)
    {
        var now = Now;
        IReadOnlyList<UndeclaredRecurrence> list = database.ReadCommitted(() =>
        {
            var declared = LiveDefinitions(now).Where(d => d.DeclaresRecurrence).Select(d => d.Definition).ToHashSet(StringComparer.Ordinal);
            return application.Recurrences.Committed.Values
                .Where(c => c.MergeKey == JobRequestData.DefinitionIdentity
                    && c.DeclaredRevision is not null
                    && application.Definitions.Committed.ContainsKey(c.Definition)
                    && !declared.Contains(c.Definition))
                .OrderBy(c => c.Definition, StringComparer.Ordinal)
                .Select(c => new UndeclaredRecurrence(c.Definition, c.DeclaredRevision!.Value, c.NextAt))
                .ToList();
        });
        return ValueTask.FromResult(list);
    }

    public ValueTask<IReadOnlyList<OutdatedProcess>> GetOutdatedProcessesAsync(CancellationToken cancellationToken)
    {
        var now = Now;
        IReadOnlyList<OutdatedProcess> list = database.ReadCommitted(() => application.Processes.Committed.Values
            .Where(p => p.LiveAt(now))
            .SelectMany(p => p.Definitions.Select(d => (Process: p, Definition: d, Stored: application.Definitions.Committed.GetValueOrDefault(d.Definition)?.Stored)))
            .Where(x => x.Stored is not null && x.Definition.Revision < x.Stored.Revision)
            .OrderBy(x => x.Definition.Definition, StringComparer.Ordinal)
            .ThenBy(x => x.Process.StartedAt)
            .ThenBy(x => x.Process.Owner.Value)
            .Select(x => new OutdatedProcess(
                new ProcessInstance(x.Process.Owner, x.Process.Type, x.Process.Release, ProcessState.Live, x.Process.StartedAt, x.Process.HeartbeatAt, x.Process.StoppedAt),
                x.Definition.Definition,
                x.Definition.Revision,
                x.Stored!.Revision))
            .ToList());
        return ValueTask.FromResult(list);
    }
}
