namespace Corfedo.Work;

/// <summary>Publishing application-provided dates, removing calendars and the reports on both (J16).</summary>
public sealed class CalendarOperations(IWorkStore store, CalendarCoverageOptions? coverage = null)
{
    /// <summary>The margin within which a date set's coverage end counts as running out.</summary>
    public TimeSpan RunningOutMargin { get; } = (coverage ?? CalendarCoverageOptions.Default).RunningOutMargin;

    /// <summary>
    /// Publishes the date set <paramref name="name"/> at <paramref name="revision"/> in the caller's transaction: it
    /// replaces the stored set atomically when the revision is newer, and plans again the jobs whose calendars use it
    /// and that have not started: planned and waiting ones from their due time, and ones waiting for dates the set did
    /// not cover. Every process then plans with the same dates. False when a newer or equal revision is stored.
    /// </summary>
    /// <param name="dates">The dates the set excludes, none after <paramref name="coversThrough"/>.</param>
    /// <param name="coversThrough">The last date the set covers; a date after it is unknown, so work that needs it waits.</param>
    /// <exception cref="CalendarConflictException">The stored revision is equal and its dates or coverage differ.</exception>
    public ValueTask<bool> PublishDatesAsync(IWorkTransaction transaction, string name, long revision, IEnumerable<DateOnly> dates, DateOnly coversThrough, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(revision);
        ArgumentNullException.ThrowIfNull(dates);
        var set = dates.ToHashSet();
        if (set.Any(d => d > coversThrough))
        {
            throw new ArgumentException("A date set holds only dates it covers.", nameof(dates));
        }
        return store.Calendars.PublishDatesAsync(transaction, new CalendarDates(name, revision, set, coversThrough), cancellationToken);
    }

    /// <summary>
    /// Deletes the stored calendar <paramref name="name"/>; false when there is none. Ended jobs that ran within it do
    /// not hold it, and a rerun of one is then refused with <see cref="RerunOutcome.CalendarRemoved"/>. A process that
    /// still registers it stores it again at its next start; <see cref="GetUnregisteredCalendarsAsync"/> lists the
    /// calendars no live process registers.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Live work uses it: outstanding jobs, recurrences or another stored calendar composing with it, which the message
    /// names with the outstanding jobs counted per definition.
    /// </exception>
    public ValueTask<bool> RemoveAsync(string name, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(name);
        return store.Calendars.RemoveCalendarAsync(name, cancellationToken);
    }

    /// <summary>
    /// Stored calendars that no live process of the process registry registers, like undeclared recurrences: candidates
    /// for <see cref="RemoveAsync"/>. The report changes no record.
    /// </summary>
    public ValueTask<IReadOnlyList<UnregisteredCalendar>> GetUnregisteredCalendarsAsync(CancellationToken cancellationToken = default) =>
        store.Calendars.GetUnregisteredCalendarsAsync(cancellationToken);

    /// <summary>
    /// Every published date set, and every one a stored calendar uses, with its coverage, the calendars using it and
    /// whether it covers what planning needs: <see cref="DateSetCoverageState.WorkWaiting"/> while jobs wait for dates it
    /// does not cover, <see cref="DateSetCoverageState.RunningOut"/> when its coverage ends within
    /// <see cref="RunningOutMargin"/> of now or of the instants still to be planned, otherwise
    /// <see cref="DateSetCoverageState.Covered"/>. Existence checks only; the report changes no record.
    /// </summary>
    public ValueTask<IReadOnlyList<DateSetCoverage>> GetDateSetCoverageAsync(CancellationToken cancellationToken = default) =>
        store.Calendars.GetDateSetCoverageAsync(RunningOutMargin, cancellationToken);
}

/// <summary>How far ahead of need date sets should be covered (J16): by default 30 days.</summary>
public sealed record CalendarCoverageOptions(TimeSpan RunningOutMargin)
{
    public static CalendarCoverageOptions Default { get; } = new(TimeSpan.FromDays(30));
}
