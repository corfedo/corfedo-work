using Corfedo.Work.InMemory.Database;
using static Corfedo.Work.InMemory.Leases.InMemoryLeaseStore;

namespace Corfedo.Work.InMemory.Jobs;

internal sealed partial class InMemoryJobStore
{
    /// <summary>Stores the pause state that every later claim and planning pass reads. Resuming signals claimers and planners.</summary>
    public ValueTask PauseDefinitionAsync(WorkTransaction transaction, LeaseSource<JobId> source, bool paused, CancellationToken cancellationToken)
    {
        var work = Join(transaction);
        var definition = Rules(source).Job!.Definition;
        if (work.Get(application.Definitions, definition) is { } row)
        {
            work.Put(application.Definitions, definition, row with { Stored = row.Stored with { Paused = paused } });
        }
        if (!paused)
        {
            NotifyDue(work, source);
            NotifyPlan(work, source);
        }
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Stores or deletes the subject's pause, the authority claims and planning check, and copies it to the flags of the
    /// subject's recurrences and outstanding jobs; when a recurrence's flag changes, the key is queued for the monitoring
    /// loop.
    /// </summary>
    public ValueTask PauseSubjectAsync(WorkTransaction transaction, LeaseSource<JobId> source, Subject subject, bool paused, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(subject);
        var work = Join(transaction);
        var definition = Rules(source).Job!.Definition;
        var key = PauseKey.Of(definition, subject);
        if (paused)
        {
            if (work.Get(application.Pauses, key) is null)
            {
                work.Put(application.Pauses, key, new PauseRow(Now));
            }
        }
        else
        {
            work.Remove(application.Pauses, key);
        }
        var flagged = work.Scan(application.Recurrences).Select(c => c.Row).Where(c => c.Definition == definition && c.Subject == subject && c.Paused != paused).ToList();
        foreach (var recurrence in flagged)
        {
            work.Put(application.Recurrences, recurrence.Id, recurrence with { Paused = paused });
        }
        if (flagged.Count > 0)
        {
            QueueExpectationChange(work, source, definition, subject);
        }
        foreach (var r in Jobs(work, definition).Where(r => r.PauseKey() == key && r.Outstanding() && r.Bool(JobColumns.Paused) != paused).ToList())
        {
            Write(work, r, [Set(JobColumns.Paused, paused)]);
        }
        if (!paused)
        {
            NotifyDue(work, source);
            NotifyPlan(work, source);
        }
        return ValueTask.CompletedTask;
    }

    /// <summary>Whether the recurrence's copied pause flag disagrees with the stored subject pauses.</summary>
    private bool PauseStale(InMemoryTransaction work, RecurrenceRow c) => c.Paused != SubjectPaused(work, c);

    /// <summary>Whether the outstanding job's copied pause flag disagrees with the stored subject pauses.</summary>
    private bool PauseStale(InMemoryTransaction work, Record r) => r.Outstanding() && r.Bool(JobColumns.Paused) != SubjectPaused(work, r);

    /// <summary>
    /// Sets the copied pause flags of recurrences and then outstanding jobs to the stored subject pauses, in both
    /// directions, up to <paramref name="limit"/> rows each, and queues the keys of repaired recurrences for the
    /// monitoring loop. Clearing signals planners and claimers.
    /// </summary>
    public async ValueTask<int> RepairPausesAsync(LeaseSource<JobId> source, int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var definition = Rules(source).Job!.Definition;
        using var work = await database.BeginAsync(cancellationToken);
        var repaired = new int[4];
        foreach (var (index, flag) in new[] { (0, false), (1, true) })
        {
            var stale = work.Scan(application.Recurrences).Select(c => c.Row)
                .Where(c => c.Definition == definition && c.Paused != flag && PauseStale(work, c))
                .Take(limit)
                .ToList();
            foreach (var recurrence in stale)
            {
                work.Put(application.Recurrences, recurrence.Id, recurrence with { Paused = flag });
                QueueChange(work, definition, recurrence.Subject);
            }
            repaired[index] = stale.Count;
        }
        foreach (var (index, flag) in new[] { (2, false), (3, true) })
        {
            var stale = Jobs(work, definition)
                .Where(r => r.Bool(JobColumns.Paused) != flag && PauseStale(work, r))
                .Take(limit)
                .ToList();
            foreach (var r in stale)
            {
                Write(work, r, [Set(JobColumns.Paused, flag)]);
            }
            repaired[index] = stale.Count;
        }
        if (repaired[0] > 0)
        {
            NotifyPlan(work, source);
        }
        if (repaired[0] + repaired[1] > 0)
        {
            NotifyMonitor(work, source, definition);
        }
        if (repaired[2] > 0)
        {
            NotifyDue(work, source);
        }
        work.Commit();
        return repaired.Max();
    }
}
