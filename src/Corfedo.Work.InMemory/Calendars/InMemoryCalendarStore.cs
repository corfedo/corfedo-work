using System.Globalization;
using Corfedo.Work.InMemory.Database;
using Corfedo.Work.InMemory.Jobs;
using Corfedo.Work.InMemory.Leases;
using static Corfedo.Work.InMemory.Leases.InMemoryLeaseStore;

namespace Corfedo.Work.InMemory.Calendars;

/// <summary>
/// Calendars by revision and published date sets of one application. Evaluators are built from the stored calendars
/// and date sets as the caller's transaction sees them.
/// </summary>
internal sealed class InMemoryCalendarStore(InMemoryWorkDatabase database, ApplicationState application, InMemoryLeaseStore leases) : ICalendarStore
{
    private DateTimeOffset Now => database.Clock.GetUtcNow();

    public async ValueTask<WorkCalendar> StoreCalendarAsync(WorkCalendar calendar, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(calendar);
        using var transaction = await database.BeginAsync(cancellationToken);
        if (transaction.Get(application.Calendars, calendar.Name)?.Calendar is { } stored && stored.Revision >= calendar.Revision)
        {
            if (stored.Revision == calendar.Revision && !stored.Digest.AsSpan().SequenceEqual(calendar.Digest))
            {
                throw new CalendarConflictException(
                    $"Calendar {calendar.Name} revision {calendar.Revision} is stored with a different rule; increase the revision when the rule changes.");
            }
            return stored;
        }
        transaction.Put(application.Calendars, calendar.Name, new CalendarRow(calendar, Now));
        transaction.Commit();
        return calendar;
    }

    public ValueTask<IReadOnlyList<WorkCalendar>> GetCalendarsAsync(CancellationToken cancellationToken) =>
        ValueTask.FromResult<IReadOnlyList<WorkCalendar>>(database.ReadCommitted(() =>
            application.Calendars.Committed.Values.Select(c => c.Calendar).OrderBy(c => c.Name, StringComparer.Ordinal).ToList()));

    public ValueTask<IReadOnlyList<CalendarDates>> GetDateSetsAsync(CancellationToken cancellationToken) =>
        ValueTask.FromResult<IReadOnlyList<CalendarDates>>(database.ReadCommitted(() =>
            application.DateSets.Committed.Values.OrderBy(d => d.Name, StringComparer.Ordinal).ToList()));

    /// <summary>
    /// Deletes the calendar unless live work uses it: outstanding jobs, recurrences or another stored calendar composing
    /// with it. The refusal names them, with the outstanding jobs counted per definition. Ended jobs keep the name
    /// without a reference and do not block.
    /// </summary>
    public async ValueTask<bool> RemoveCalendarAsync(string name, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(name);
        using var transaction = await database.BeginAsync(cancellationToken);
        if (transaction.Get(application.Calendars, name) is null)
        {
            return false;
        }
        var composing = transaction.Scan(application.Calendars)
            .Select(c => c.Row.Calendar)
            .Where(c => c.Name != name && c.References.Contains(name, StringComparer.Ordinal))
            .Select(c => c.Name)
            .Order(StringComparer.Ordinal)
            .ToList();
        var jobCounts = transaction.Scan(application.Jobs.Rows)
            .Select(r => r.Row)
            .Where(r => r.Text(JobColumns.Calendar) == name)
            .GroupBy(r => r.Text(JobColumns.Definition)!, StringComparer.Ordinal)
            .OrderBy(g => g.Key, StringComparer.Ordinal)
            .Select(g => string.Create(CultureInfo.InvariantCulture, $"{g.Key} ({g.Count()})"))
            .ToList();
        var recurrenceCount = transaction.Scan(application.Recurrences).Count(c => c.Row.Calendar == name);
        var blockers = new List<string>();
        if (jobCounts.Count > 0)
        {
            blockers.Add("outstanding jobs of " + string.Join(", ", jobCounts));
        }
        if (recurrenceCount > 0)
        {
            blockers.Add(string.Create(CultureInfo.InvariantCulture, $"{recurrenceCount} recurrences"));
        }
        if (composing.Count > 0)
        {
            blockers.Add("calendars " + string.Join(", ", composing) + " composing with it");
        }
        if (blockers.Count > 0)
        {
            throw new InvalidOperationException($"Calendar {name} cannot be removed while live work uses it: {string.Join("; ", blockers)}.");
        }
        transaction.Remove(application.Calendars, name);
        transaction.Commit();
        return true;
    }

    /// <summary>Records the calendars a registered process registers; calendars it already recorded are left as they are.</summary>
    public async ValueTask RegisterProcessAsync(OwnerId owner, IReadOnlyList<WorkCalendar> calendars, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(calendars);
        if (calendars.Count == 0)
        {
            return;
        }
        using var transaction = await database.BeginAsync(cancellationToken);
        var process = transaction.Get(application.Processes, owner) ?? throw new InvalidOperationException($"Process {owner} is not registered.");
        var recorded = process.Calendars.Select(c => c.Calendar).ToHashSet(StringComparer.Ordinal);
        transaction.Put(application.Processes, owner, process with { Calendars = [.. process.Calendars, .. calendars.Where(c => !recorded.Contains(c.Name)).Select(c => (c.Name, c.Revision))] });
        transaction.Commit();
    }

    /// <summary>Stored calendars that no live process registers: a process is live while it has not stopped and its heartbeat is within its stale window.</summary>
    public ValueTask<IReadOnlyList<UnregisteredCalendar>> GetUnregisteredCalendarsAsync(CancellationToken cancellationToken)
    {
        var now = Now;
        return ValueTask.FromResult<IReadOnlyList<UnregisteredCalendar>>(database.ReadCommitted(() =>
        {
            var registered = application.Processes.Committed.Values
                .Where(p => p.LiveAt(now))
                .SelectMany(p => p.Calendars.Select(c => c.Calendar))
                .ToHashSet(StringComparer.Ordinal);
            return application.Calendars.Committed.Values
                .Where(c => !registered.Contains(c.Calendar.Name))
                .OrderBy(c => c.Calendar.Name, StringComparer.Ordinal)
                .Select(c => new UnregisteredCalendar(c.Calendar.Name, c.Calendar.Revision, c.StoredAt))
                .ToList();
        }));
    }

    /// <summary>
    /// Every published date set, and every one a stored calendar uses, with the calendars that use it, directly or
    /// through composition, in one snapshot. Work waits for a set when an outstanding job of such a calendar waits for
    /// dates and the set is unpublished or, among the sets that calendar uses, covers the fewest dates. A set runs out when
    /// its coverage ends within <paramref name="margin"/> of now, of an instant still to be planned of an outstanding job
    /// of such a calendar, or of a next instant of such a recurrence within the lookahead; an unpublished set always does.
    /// </summary>
    public async ValueTask<IReadOnlyList<DateSetCoverage>> GetDateSetCoverageAsync(TimeSpan margin, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(margin, TimeSpan.Zero);
        using var transaction = await database.BeginAsync(cancellationToken);
        var calendars = transaction.Scan(application.Calendars).Select(c => c.Row.Calendar).ToList();
        var dates = transaction.Scan(application.DateSets).ToDictionary(d => d.Key, d => d.Row, StringComparer.Ordinal);
        var evaluator = new CalendarEvaluator(calendars, dates.Values);
        var names = dates.Keys.Concat(calendars.SelectMany(c => c.DateSets)).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal).ToArray();
        if (names.Length == 0)
        {
            return [];
        }
        var users = names.ToDictionary(n => n, n => evaluator.DependingOn(n).Order(StringComparer.Ordinal).ToArray(), StringComparer.Ordinal);
        var now = Now;
        // The outstanding jobs and the recurrences of calendars using a set, by calendar.
        var used = users.Values.SelectMany(u => u).ToHashSet(StringComparer.Ordinal);
        var jobs = transaction.Scan(application.Jobs.Rows)
            .Select(r => r.Row)
            .Where(r => r.Outstanding() && r.Text(JobColumns.Calendar) is { } c && used.Contains(c))
            .ToLookup(r => r.Text(JobColumns.Calendar)!, StringComparer.Ordinal);
        var recurrences = transaction.Scan(application.Recurrences)
            .Select(c => c.Row)
            .Where(c => c.Calendar is { } name && used.Contains(name))
            .ToLookup(c => c.Calendar!, StringComparer.Ordinal);
        var waiting = used.Where(c => jobs[c].Any(r => r.Bool(JobColumns.CalendarUnavailable))).ToHashSet(StringComparer.Ordinal);
        var needed = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in names.Where(dates.ContainsKey))
        {
            // The instant the set's coverage ends, by UTC days, brought forward by the margin.
            var threshold = new DateTimeOffset(dates[name].CoversThrough.AddDays(1).ToDateTime(TimeOnly.MinValue), TimeSpan.Zero) - margin;
            if (threshold <= now
                || users[name].Any(c => jobs[c].Any(r => r.Time(JobColumns.CalendarAt) >= threshold))
                || users[name].Any(c => recurrences[c].Any(r => r.NextAt >= threshold && r.NextAt <= now + CalendarEvaluator.Lookahead)))
            {
                needed.Add(name);
            }
        }
        transaction.Commit();
        // The sets a waiting calendar waits for: those it uses that are unpublished, otherwise those covering the fewest dates.
        var waitedFor = new HashSet<string>(StringComparer.Ordinal);
        foreach (var calendar in waiting)
        {
            var sets = names.Where(n => users[n].Contains(calendar, StringComparer.Ordinal)).ToArray();
            var unpublished = sets.Where(n => !dates.ContainsKey(n)).ToArray();
            if (unpublished.Length > 0)
            {
                waitedFor.UnionWith(unpublished);
            }
            else if (sets.Length > 0)
            {
                var least = sets.Min(n => dates[n].CoversThrough);
                waitedFor.UnionWith(sets.Where(n => dates[n].CoversThrough == least));
            }
        }
        return
        [
            .. names.Select(name =>
            {
                var set = dates.GetValueOrDefault(name);
                var state = waitedFor.Contains(name) ? DateSetCoverageState.WorkWaiting
                    : set is null || needed.Contains(name) ? DateSetCoverageState.RunningOut
                    : DateSetCoverageState.Covered;
                return new DateSetCoverage(name, set?.Revision, set?.CoversThrough, users[name], state);
            }),
        ];
    }

    /// <summary>
    /// Replaces the date set only with a newer revision. Then the jobs of every calendar that uses the set, directly or
    /// through composition, are planned again: outstanding ones that are not held, are not to be skipped and have a due
    /// time their calendar planned, not one an operator made due ignoring it, get it back as their calendar instant, and
    /// ones waiting for uncovered dates become plannable. Each definition with such jobs is signalled to plan on commit.
    /// </summary>
    public ValueTask<bool> PublishDatesAsync(WorkTransaction transaction, CalendarDates set, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(set);
        ArgumentException.ThrowIfNullOrWhiteSpace(set.Name, nameof(set));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(set.Revision);
        if (set.Dates.Any(d => d > set.CoversThrough))
        {
            throw new ArgumentException("A date set holds only dates it covers.", nameof(set));
        }
        var work = leases.Join(transaction);
        if (work.Get(application.DateSets, set.Name) is { } stored && stored.Revision >= set.Revision)
        {
            if (stored.Revision == set.Revision && (stored.CoversThrough != set.CoversThrough || !stored.Dates.SetEquals(set.Dates)))
            {
                throw new CalendarConflictException(
                    $"Date set {set.Name} revision {set.Revision} is stored with other dates; publish changes with a newer revision.");
            }
            return ValueTask.FromResult(false);
        }
        work.Put(application.DateSets, set.Name, new CalendarDates(set.Name, set.Revision, set.Dates.ToHashSet(), set.CoversThrough));
        var depending = Evaluator(work, application).DependingOn(set.Name);
        var definitions = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (key, r) in work.Scan(application.Jobs.Rows).ToList())
        {
            if (r.Text(JobColumns.Calendar) is not { } calendar || !depending.Contains(calendar)
                || !r.Outstanding() || r.Time(LeaseFields.Default.LeaseExpiresAt) is not null || r.Bool(JobColumns.SkipPending))
            {
                continue;
            }
            var unavailable = r.Bool(JobColumns.CalendarUnavailable);
            var due = r.Time(LeaseFields.Default.DueAt);
            if (!unavailable && (due is null || due == DateTimeOffset.MaxValue || r.Time(JobColumns.WindowEnd) == DateTimeOffset.MaxValue))
            {
                continue;
            }
            work.Put(application.Jobs.Rows, key, r.With(
                [
                    Set(JobColumns.CalendarAt, unavailable ? r.Time(JobColumns.CalendarAt) : due),
                    Set(LeaseFields.Default.DueAt, null),
                    Set(JobColumns.WindowEnd, null),
                    Set(JobColumns.CalendarUnavailable, false),
                ],
                database.NextVersion()));
            definitions.Add(r.Text(JobColumns.Definition)!);
        }
        foreach (var definition in definitions)
        {
            leases.Signal(work, WorkTopics.Plan, JobDefinition.SourceNameOf(definition));
        }
        return ValueTask.FromResult(true);
    }

    /// <summary>An evaluator of the calendars and date sets stored as the transaction sees them.</summary>
    internal static CalendarEvaluator Evaluator(InMemoryTransaction work, ApplicationState application) =>
        new(work.Scan(application.Calendars).Select(c => c.Row.Calendar), work.Scan(application.DateSets).Select(d => d.Row));
}
