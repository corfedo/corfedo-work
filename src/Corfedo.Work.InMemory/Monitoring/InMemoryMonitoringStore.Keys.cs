using Corfedo.Work.InMemory.Database;

namespace Corfedo.Work.InMemory.Monitoring;

/// <summary>
/// Tracking of expected keys. Transactions that change a recurrence or its copied pause flag queue its key; each pass
/// applies the queued keys, so a steady pass reads what changed. A full reconciliation, which reads every recurrence and
/// key of a definition, runs only while the definition's revision or pause differs from the ones it last reconciled
/// with, and is recorded once nothing is left.
/// </summary>
internal sealed partial class InMemoryMonitoringStore
{
    /// <summary>Whether the key has an unpaused recurrence of the definition.</summary>
    private static bool HasUnpausedRecurrence(InMemoryTransaction work, ApplicationState application, ExpectationKey key) =>
        work.Scan(application.Recurrences).Any(c => c.Row.Definition == key.Definition && !c.Row.Paused
            && c.Row.Subject?.Kind == key.SubjectKind && c.Row.Subject?.Id == key.SubjectId);

    /// <summary>The rows of a definition as the transaction sees them.</summary>
    private IEnumerable<(ExpectationKey Key, ExpectationRow Row)> Rows(InMemoryTransaction work, string definition) =>
        work.Scan(application.Expectations).Where(s => s.Key.Definition == definition);

    /// <summary>Starts the window of a row inserted or taken up for a deadline.</summary>
    private static ExpectationRow StartWindow(ExpectationRow row, ExpectationWindow window, long revision) =>
        row with { DeadlineAt = window.Deadline, WindowStart = window.WindowStart, MetAt = null, FirstAfter = null, LastAfter = null, Revision = revision };

    /// <summary>Clears the windows of the keys and resolves their open <see cref="WorkIncidentKind.CompletesBy"/> incidents.</summary>
    private List<WorkIncident> ClearWindows(InMemoryTransaction work, string definition, IReadOnlyCollection<ExpectationKey> keys)
    {
        foreach (var key in keys)
        {
            var row = work.Get(application.Expectations, key)!;
            work.Put(application.Expectations, key, row with { DeadlineAt = null, WindowStart = null, MetAt = null, FirstAfter = null, LastAfter = null, Whole = false, Revision = null });
        }
        var now = Now;
        return
        [
            .. AllIncidents(work)
                .Where(i => i.Open() && i.Int32(IncidentColumns.Kind) == (int)WorkIncidentKind.CompletesBy
                    && keys.Any(k => OfKey(i, definition, k.SubjectKind, k.SubjectId)))
                .ToList()
                .Select(i => Resolve(work, i, now)),
        ];
    }

    /// <summary>
    /// Deletes the queued changes of the definition, oldest first, then starts the keys that have an unpaused recurrence
    /// and no deadline and clears the windows of tracked keys that have none, unless they are the definition alone
    /// expected as a whole.
    /// </summary>
    public async ValueTask<int> ApplyChangesAsync(string definition, long revision, ExpectationWindow? window, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        using var work = await database.BeginAsync(cancellationToken);
        var queued = work.Scan(application.ExpectationChanges).Where(c => c.Row.Definition == definition).OrderBy(c => c.Key).Take(limit).ToList();
        foreach (var (key, _) in queued)
        {
            work.Remove(application.ExpectationChanges, key);
        }
        var changes = IncidentChanges.None;
        var keys = queued.Select(c => ExpectationKey.Of(definition, c.Row.Subject)).Distinct().ToList();
        if (window is not null && keys.Count > 0)
        {
            var clear = new List<ExpectationKey>();
            foreach (var key in keys)
            {
                var expected = HasUnpausedRecurrence(work, application, key);
                var row = work.Get(application.Expectations, key);
                if (expected && row?.DeadlineAt is null)
                {
                    work.Put(application.Expectations, key, StartWindow(row ?? new ExpectationRow(), window, revision));
                }
                else if (!expected && row is { DeadlineAt: not null, Whole: false })
                {
                    clear.Add(key);
                }
            }
            if (clear.Count > 0 && ClearWindows(work, definition, clear) is { Count: > 0 } resolved)
            {
                Notify(work);
                changes = new IncidentChanges([], resolved);
            }
        }
        work.Commit();
        WorkTelemetry.Count(changes);
        return queued.Count;
    }

    /// <summary>
    /// Without recurrences, inserts or takes up the definition-alone row as the whole definition unless it has a
    /// deadline, keeping a deadline it has. With them, keeps the window of a row marked whole when a recurrence without
    /// subject is unpaused and clears it otherwise.
    /// </summary>
    public async ValueTask TrackWholeAsync(string definition, long revision, bool whole, ExpectationWindow window, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(window);
        using var work = await database.BeginAsync(cancellationToken);
        var key = ExpectationKey.Of(definition, null);
        var row = work.Get(application.Expectations, key);
        var changes = IncidentChanges.None;
        if (whole)
        {
            if (row is { Whole: true, DeadlineAt: not null })
            {
                return;
            }
            work.Put(application.Expectations, key, row is { DeadlineAt: not null }
                ? row with { Whole = true }
                : StartWindow(row ?? new ExpectationRow(), window, revision) with { Whole = true, MetAt = row?.MetAt, FirstAfter = row?.FirstAfter, LastAfter = row?.LastAfter });
        }
        else
        {
            if (row is not { Whole: true })
            {
                return;
            }
            if (HasUnpausedRecurrence(work, application, key))
            {
                work.Put(application.Expectations, key, row with { Whole = false });
            }
            else if (ClearWindows(work, definition, [key]) is { Count: > 0 } resolved)
            {
                Notify(work);
                changes = new IncidentChanges([], resolved);
            }
        }
        work.Commit();
        WorkTelemetry.Count(changes);
    }

    /// <summary>
    /// Starts up to <paramref name="limit"/> keys of the definition's unpaused recurrences with a subject that have no
    /// deadline, in subject order, and the definition alone for an unpaused recurrence without subject. A row the ending
    /// transaction created for failures takes up the deadline.
    /// </summary>
    public async ValueTask<int> InsertExpectedAsync(string definition, long revision, ExpectationWindow window, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(window);
        using var work = await database.BeginAsync(cancellationToken);
        bool Untracked(ExpectationKey key) => work.Get(application.Expectations, key)?.DeadlineAt is null;
        var recurrences = work.Scan(application.Recurrences).Select(c => c.Row).Where(c => c.Definition == definition && !c.Paused).ToList();
        var keys = recurrences
            .Where(c => c.Subject is not null)
            .Select(c => ExpectationKey.Of(definition, c.Subject))
            .Distinct()
            .Where(Untracked)
            .OrderBy(k => k.SubjectKind, StringComparer.Ordinal)
            .ThenBy(k => k.SubjectId, StringComparer.Ordinal)
            .Take(limit)
            .ToList();
        var alone = ExpectationKey.Of(definition, null);
        if (recurrences.Any(c => c.Subject is null) && Untracked(alone))
        {
            keys.Add(alone);
        }
        foreach (var key in keys)
        {
            work.Put(application.Expectations, key, StartWindow(work.Get(application.Expectations, key) ?? new ExpectationRow(), window, revision) with { Whole = false });
        }
        work.Commit();
        return keys.Count;
    }

    /// <summary>
    /// Clears the window of up to <paramref name="limit"/> rows with a deadline that are not expected and resolves their
    /// open completion incidents. While tracking, a row is expected when it is marked whole or a recurrence of its key is
    /// not paused.
    /// </summary>
    public async ValueTask<int> ClearUnexpectedAsync(string definition, bool tracking, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        using var work = await database.BeginAsync(cancellationToken);
        var keys = Rows(work, definition)
            .Where(s => s.Row.DeadlineAt is not null && !(tracking && (s.Row.Whole || HasUnpausedRecurrence(work, application, s.Key))))
            .Take(limit)
            .Select(s => s.Key)
            .ToList();
        var resolved = keys.Count == 0 ? [] : ClearWindows(work, definition, keys);
        if (resolved.Count > 0)
        {
            Notify(work);
        }
        work.Commit();
        WorkTelemetry.Count(resolved.Count == 0 ? IncidentChanges.None : new IncidentChanges([], resolved));
        return keys.Count;
    }

    /// <summary>
    /// Records the definition reconciled with its stored revision and pause when both are unchanged and nothing is left
    /// to do: no open incident of a kind it does not expect, no failures without a failure limit, and no deadline it does
    /// not expect or computed from an older revision.
    /// </summary>
    public async ValueTask<bool> MarkReconciledAsync(StoredExpectation definition, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        var expectation = definition.Expectation;
        var tracking = expectation?.Deadline is not null && !definition.Paused;
        var unexpected = ExpectationKinds.Unexpected(expectation).Select(k => (int)k).ToHashSet();
        using var work = await database.BeginAsync(cancellationToken);
        if (work.Get(application.Definitions, definition.Definition) is not { } stored
            || stored.Stored.Revision != definition.Revision || stored.Stored.Paused != definition.Paused)
        {
            return false;
        }
        var rows = Rows(work, definition.Definition).ToList();
        bool Tracked(ExpectationKey key) => work.Get(application.Expectations, key)?.DeadlineAt is not null;
        var recurrences = work.Scan(application.Recurrences).Select(c => c.Row).Where(c => c.Definition == definition.Definition && !c.Paused).ToList();
        var left = AllIncidents(work).Any(i => i.Open() && (string?)i[IncidentColumns.Definition] == definition.Definition && unexpected.Contains(i.Int32(IncidentColumns.Kind)))
            || (expectation?.FailureLimit is null && rows.Any(s => s.Row.Failures > 0))
            || (tracking
                ? rows.Any(s => s.Row.DeadlineAt is not null && (s.Row.Revision != definition.Revision || !(s.Row.Whole || HasUnpausedRecurrence(work, application, s.Key))))
                    || recurrences.Any(c => c.Subject is not null && !Tracked(ExpectationKey.Of(definition.Definition, c.Subject)))
                    || (recurrences.Any(c => c.Subject is null) && !Tracked(ExpectationKey.Of(definition.Definition, null)))
                : rows.Any(s => s.Row.DeadlineAt is not null));
        if (left)
        {
            return false;
        }
        work.Put(application.Definitions, definition.Definition, stored with { MonitoredRevision = definition.Revision, MonitoredPaused = definition.Paused });
        work.Commit();
        return true;
    }
}
