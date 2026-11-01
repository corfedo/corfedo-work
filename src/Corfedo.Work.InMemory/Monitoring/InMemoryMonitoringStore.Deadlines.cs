using Corfedo.Work.InMemory.Database;

namespace Corfedo.Work.InMemory.Monitoring;

internal sealed partial class InMemoryMonitoringStore
{
    /// <summary>
    /// Reads the distinct window starts of the rows to move, computes their deadlines outside the transaction and moves
    /// the rows in another, those still selected. Recorded completions are placed against the new deadline: the first
    /// before it is the window's, the first and latest at or after it are completions after the deadline.
    /// </summary>
    public async ValueTask<int> RescheduleAsync(string definition, long revision, Func<DateTimeOffset, DateTimeOffset> next, bool olderRevisions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(next);
        bool Selected(ExpectationRow s) =>
            s.DeadlineAt == ExpectationWindow.Uncovered || (olderRevisions && s.DeadlineAt is not null && s.Revision != revision);
        var starts = database.ReadCommitted(() => application.Expectations.Committed
            .Where(s => s.Key.Definition == definition && Selected(s.Value))
            .Select(s => s.Value.WindowStart!.Value)
            .Distinct()
            .ToList());
        var moves = starts.Select(w => (Start: w, Deadline: next(w)))
            .Where(m => olderRevisions || m.Deadline != ExpectationWindow.Uncovered)
            .ToDictionary(m => m.Start, m => m.Deadline);
        if (moves.Count == 0)
        {
            return 0;
        }
        using var work = await database.BeginAsync(cancellationToken);
        var moved = 0;
        foreach (var (key, s) in Rows(work, definition).Where(s => Selected(s.Row) && moves.ContainsKey(s.Row.WindowStart!.Value)).ToList())
        {
            var deadline = moves[s.WindowStart!.Value];
            DateTimeOffset?[] completions = [s.MetAt, s.FirstAfter, s.LastAfter];
            var before = completions.OfType<DateTimeOffset>().Where(c => c < deadline).ToList();
            var after = completions.OfType<DateTimeOffset>().Where(c => c >= deadline).ToList();
            work.Put(application.Expectations, key, s with
            {
                DeadlineAt = deadline,
                Revision = revision,
                MetAt = before.Count == 0 ? null : before.Min(),
                FirstAfter = after.Count == 0 ? null : after.Min(),
                LastAfter = after.Count == 0 ? null : after.Max(),
            });
            moved++;
        }
        work.Commit();
        return moved;
    }

    /// <summary>
    /// Takes the definition's rows whose deadline passed by the store's clock, earliest first, applies
    /// <paramref name="process"/> to each, stores the processed windows and inserts the incidents of their missed
    /// deadlines: an incident a late completion resolved is inserted resolved, an open one only when the key has no
    /// open completion incident. Then reads the definition's earliest deadline.
    /// </summary>
    public async ValueTask<DeadlinePass> ProcessDeadlinesAsync(string definition, Func<ExpectationWindow, DateTimeOffset, ExpectationStep> process, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(process);
        using var work = await database.BeginAsync(cancellationToken);
        var now = Now;
        var rows = Rows(work, definition).Where(s => s.Row.DeadlineAt <= now).OrderBy(s => s.Row.DeadlineAt).Take(limit).ToList();
        var inserted = new List<WorkIncident>();
        foreach (var (key, s) in rows)
        {
            var step = process(new ExpectationWindow(s.DeadlineAt!.Value, s.WindowStart!.Value, s.MetAt, s.FirstAfter, s.LastAfter), now);
            var window = step.Window;
            work.Put(application.Expectations, key, s with
            {
                DeadlineAt = window.Deadline,
                WindowStart = window.WindowStart,
                MetAt = window.MetAt,
                FirstAfter = window.FirstAfter,
                LastAfter = window.LastAfter,
            });
            foreach (var missed in step.Missed)
            {
                if (Insert(work, WorkIncidentKind.CompletesBy, definition, key.Subject, null, null, missed.At, missed.ResolvedAt) is { } incident)
                {
                    inserted.Add(incident);
                }
            }
        }
        var changes = IncidentChanges.None;
        if (inserted.Count > 0)
        {
            Notify(work);
            changes = new IncidentChanges(inserted, [.. inserted.Where(i => i.ResolvedAt is not null)]);
        }
        var next = Rows(work, definition).Select(s => s.Row.DeadlineAt).Where(d => d is not null && d < ExpectationWindow.Uncovered).Min();
        work.Commit();
        WorkTelemetry.Count(changes);
        return new DeadlinePass(rows.Count, changes, next);
    }
}
