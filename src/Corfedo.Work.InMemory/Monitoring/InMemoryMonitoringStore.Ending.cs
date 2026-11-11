using Corfedo.Work.InMemory.Database;

namespace Corfedo.Work.InMemory.Monitoring;

internal sealed partial class InMemoryMonitoringStore
{
    /// <summary>
    /// Records a job's end in the transaction that ends it, for a definition with expectations, after the job's own
    /// update. A failure (<see cref="JobOutcome.Failed"/> or <see cref="JobOutcome.TimedOut"/>) increments the key's
    /// failures when the definition has a failure limit and opens an incident once they exceed it. A completion
    /// updates the key's row, and the definition-alone row of a definition expected as a whole, only when it resets
    /// failures, meets the window's deadline first or follows a passed deadline not yet processed; then it resolves the
    /// key's open incidents; a resolved <see cref="WorkIncidentKind.CompletesBy"/> incident starts the key's window at
    /// the completion. Other outcomes change nothing. Signals the incident source when an incident opened or resolved.
    /// </summary>
    internal void RecordEnd(InMemoryTransaction work, string definition, JobId job, Subject? subject, JobOutcome outcome, DateTimeOffset endedAt, int? failureLimit)
    {
        var changes = outcome switch
        {
            JobOutcome.Failed or JobOutcome.TimedOut when failureLimit is { } limit => RecordFailure(work, definition, job, subject, endedAt, limit),
            JobOutcome.Completed => RecordSuccess(work, definition, subject, endedAt),
            _ => IncidentChanges.None,
        };
        if (changes.Any)
        {
            Notify(work);
        }
        WorkTelemetry.Count(changes);
    }

    private IncidentChanges RecordFailure(InMemoryTransaction work, string definition, JobId job, Subject? subject, DateTimeOffset endedAt, int limit)
    {
        var key = ExpectationKey.Of(definition, subject);
        var row = work.Get(application.Expectations, key) ?? new ExpectationRow();
        var failures = row.Failures + 1;
        work.Put(application.Expectations, key, row with { Failures = failures });
        if (failures <= limit || Insert(work, WorkIncidentKind.FailsAtMostInARow, definition, subject, null, job.Value, endedAt) is not { } opened)
        {
            return IncidentChanges.None;
        }
        return new IncidentChanges([opened], []);
    }

    private IncidentChanges RecordSuccess(InMemoryTransaction work, string definition, Subject? subject, DateTimeOffset endedAt)
    {
        bool InWindow(ExpectationRow s) => s.MetAt is null && endedAt >= s.WindowStart && endedAt < s.DeadlineAt;
        var own = ExpectationKey.Of(definition, subject);
        var alone = ExpectationKey.Of(definition, null);
        var wholeKey = subject is not null && work.Get(application.Expectations, alone) is { Whole: true };
        foreach (var key in wholeKey ? [own, alone] : new[] { own })
        {
            if (work.Get(application.Expectations, key) is not { } s || !(s.Failures > 0 || InWindow(s) || s.DeadlineAt <= endedAt))
            {
                continue;
            }
            work.Put(application.Expectations, key, s with
            {
                Failures = 0,
                MetAt = InWindow(s) ? endedAt : s.MetAt,
                FirstAfter = s.DeadlineAt <= endedAt ? Min(s.FirstAfter, endedAt) : s.FirstAfter,
                LastAfter = s.DeadlineAt <= endedAt ? Max(s.LastAfter, endedAt) : s.LastAfter,
            });
        }
        var resolved = AllIncidents(work)
            .Where(i => i.Open() && (string?)i[IncidentColumns.Definition] == definition
                && ExpectationKinds.Of((WorkIncidentKind)i.Int32(IncidentColumns.Kind))
                && (OfKey(i, definition, subject?.Kind, subject?.Id)
                    || (wholeKey && i[IncidentColumns.SubjectKind] is null && i.Int32(IncidentColumns.Kind) == (int)WorkIncidentKind.CompletesBy)))
            .ToList()
            .Select(i => Resolve(work, i, endedAt))
            .ToList();
        if (resolved.Count == 0)
        {
            return IncidentChanges.None;
        }
        // A completion that resolves a missed deadline belongs to it: the key's next window starts at the completion.
        foreach (var incident in resolved.Where(i => i.Kind == WorkIncidentKind.CompletesBy))
        {
            var key = ExpectationKey.Of(definition, incident.Subject);
            if (work.Get(application.Expectations, key) is { } s)
            {
                work.Put(application.Expectations, key, s with { WindowStart = endedAt, MetAt = null });
            }
        }
        return new IncidentChanges([], resolved);
    }

    /// <summary>The earlier of the values, as SQL's <c>least</c>, which ignores nulls.</summary>
    private static DateTimeOffset Min(DateTimeOffset? a, DateTimeOffset b) => a is { } value && value < b ? value : b;

    /// <summary>The later of the values, as SQL's <c>greatest</c>, which ignores nulls.</summary>
    private static DateTimeOffset Max(DateTimeOffset? a, DateTimeOffset b) => a is { } value && value > b ? value : b;
}
