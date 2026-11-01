namespace Corfedo.Work;

/// <summary>Calendars by revision and published date sets of one store (J16).</summary>
public interface ICalendarStore
{
    /// <summary>
    /// Stores <paramref name="calendar"/> if its revision is newer than the stored one and returns the newest stored
    /// calendar.
    /// </summary>
    /// <exception cref="CalendarConflictException">The stored revision is equal and its rule differs.</exception>
    ValueTask<WorkCalendar> StoreCalendarAsync(WorkCalendar calendar, CancellationToken cancellationToken);

    /// <summary>Every stored calendar at its newest revision.</summary>
    ValueTask<IReadOnlyList<WorkCalendar>> GetCalendarsAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Deletes the stored calendar; false when there is none. Ended jobs that ran within it do not hold it.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Live work uses it: outstanding jobs, recurrences or another stored calendar composing with it, which the message
    /// names; nothing was deleted.
    /// </exception>
    ValueTask<bool> RemoveCalendarAsync(string name, CancellationToken cancellationToken);

    /// <summary>Records the calendars a registered process registers; calendars it already recorded are left as they are.</summary>
    ValueTask RegisterProcessAsync(OwnerId owner, IReadOnlyList<WorkCalendar> calendars, CancellationToken cancellationToken);

    /// <summary>Stored calendars that no live process registers, in one snapshot.</summary>
    ValueTask<IReadOnlyList<UnregisteredCalendar>> GetUnregisteredCalendarsAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Every published date set and every one a stored calendar uses, with the calendars using it and its state, in one
    /// snapshot; a set runs out when its coverage ends within <paramref name="margin"/> of the instants planning needs.
    /// </summary>
    ValueTask<IReadOnlyList<DateSetCoverage>> GetDateSetCoverageAsync(TimeSpan margin, CancellationToken cancellationToken);

    /// <summary>
    /// In the caller's transaction, replaces the date set with <paramref name="revision"/> if it is newer than the stored
    /// one, and plans again the outstanding jobs that are not running and whose calendars use the set: planned and
    /// waiting ones are planned again from their due time, and jobs waiting for dates the set did not cover become
    /// plannable. False when a newer or equal revision is stored; nothing changed.
    /// </summary>
    /// <exception cref="CalendarConflictException">The stored revision is equal and its dates or coverage differ.</exception>
    ValueTask<bool> PublishDatesAsync(WorkTransaction transaction, CalendarDates dates, CancellationToken cancellationToken);

    /// <summary>The newest published revision of every date set.</summary>
    ValueTask<IReadOnlyList<CalendarDates>> GetDateSetsAsync(CancellationToken cancellationToken);
}

/// <summary>A calendar or date set is stored with the same revision and different content.</summary>
public sealed class CalendarConflictException(string message) : Exception(message);

/// <summary>A stored calendar that no live process registers; it stays stored until removed.</summary>
public sealed record UnregisteredCalendar(string Name, long Revision, DateTimeOffset StoredAt);

/// <summary>Whether a date set covers the dates planning needs (J16).</summary>
public enum DateSetCoverageState
{
    /// <summary>Its coverage reaches beyond the instants planning needs by more than the margin.</summary>
    Covered,

    /// <summary>
    /// It is unpublished, or its coverage ends within the margin of now, of an instant still to be planned or of a
    /// recurrence's next instant within the lookahead.
    /// </summary>
    RunningOut,

    /// <summary>Outstanding jobs of a calendar using it wait for dates it does not cover yet.</summary>
    WorkWaiting,
}

/// <summary>
/// A date set with the newest published revision and the last date it covers, both null while unpublished, the stored
/// calendars using it directly or through composition, and its state.
/// </summary>
public sealed record DateSetCoverage(string Name, long? Revision, DateOnly? CoversThrough, IReadOnlyList<string> Calendars, DateSetCoverageState State);
