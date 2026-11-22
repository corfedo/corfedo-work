using Corfedo.Work.InMemory.Database;

namespace Corfedo.Work.InMemory.Jobs;

internal sealed partial class InMemoryJobStore
{
    /// <summary>
    /// Reads every definition's maintenance in one transaction that writes nothing, with the conditions of the
    /// maintenance calls.
    /// </summary>
    public async ValueTask<IReadOnlyDictionary<string, JobMaintenanceDue>> MaintenanceDueAsync(IReadOnlyCollection<LeaseSource<JobId>> sources, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sources);
        var due = new Dictionary<string, JobMaintenanceDue>(StringComparer.Ordinal);
        if (sources.Count == 0)
        {
            return due;
        }
        using var work = await database.BeginAsync(cancellationToken);
        var now = Now;
        foreach (var source in sources)
        {
            var rules = Rules(source);
            var definition = rules.Job!.Definition;
            var stored = Stored(work, definition);
            var definitionPaused = stored?.Paused ?? false;
            var recurrences = work.Scan(application.Recurrences).Select(c => c.Row).Where(c => c.Definition == definition).ToList();
            var jobs = Jobs(work, definition).ToList();
            var scoped = rules.Scan(work).Select(r => r.Row).ToList();
            var planning = jobs.Any(r => r.Bool(JobColumns.Unconsumed))
                || (!definitionPaused && recurrences.Any(c => DueToPlan(work, c, now) || WaitingToPlan(work, c)));
            var calendar = scoped.Any(r => CalendarUnheld(r) && (CalendarInstantCame(r, now) || WindowClosed(r, now)));
            var pauses = recurrences.Any(c => PauseStale(work, c)) || jobs.Any(r => PauseStale(work, r));
            var nextPlanned = definitionPaused ? null : recurrences.Where(c => c.NextAt > now && NextCounts(work, c)).Min(c => c.NextAt);
            var nextCalendar = NextCalendarInstant(scoped, now);
            var next = nextPlanned is { } a && nextCalendar is { } b ? (a < b ? a : b) : nextPlanned ?? nextCalendar;
            due[definition] = new(
                stored?.Revision,
                planning,
                calendar,
                scoped.Any(Skipping),
                scoped.Any(r => IsRetired(work, definition, r, now)),
                pauses,
                Until(next, now));
        }
        return due;
    }
}
