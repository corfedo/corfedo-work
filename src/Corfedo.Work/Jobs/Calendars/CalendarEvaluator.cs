using System.Collections.Concurrent;

namespace Corfedo.Work;

/// <summary>
/// A published date set: application-provided dates with a data revision and the last date it covers. A date after
/// <see cref="CoversThrough"/> is unknown, never assumed to be allowed or excluded.
/// </summary>
public sealed record CalendarDates(string Name, long Revision, IReadOnlySet<DateOnly> Dates, DateOnly CoversThrough);

/// <summary>What a calendar says about the instants from a given one.</summary>
public enum CalendarSlotKind
{
    /// <summary>An allowed instant was found.</summary>
    Allowed,

    /// <summary>Deciding needs a date that a date set does not cover yet; nothing is planned until it does.</summary>
    Unavailable,

    /// <summary>No instant within the lookahead is allowed.</summary>
    None,
}

/// <summary>
/// The first allowed instant at or after the one asked for, and the end of the allowed interval it starts, null when
/// the interval lasts beyond the lookahead. A date set's coverage ending inside the interval ends it there.
/// </summary>
public readonly record struct CalendarSlot(CalendarSlotKind Kind, DateTimeOffset Start, DateTimeOffset? End)
{
    public static CalendarSlot Unavailable { get; } = new(CalendarSlotKind.Unavailable, default, null);

    public static CalendarSlot None { get; } = new(CalendarSlotKind.None, default, null);
}

/// <summary>
/// Evaluates stored calendars with the published date sets they use, the same way in every process. Planning never
/// looks further ahead than <see cref="Lookahead"/>. An evaluator is immutable and safe to share between threads.
/// </summary>
public sealed class CalendarEvaluator
{
    /// <summary>How far planning looks for an allowed instant.</summary>
    public static readonly TimeSpan Lookahead = TimeSpan.FromDays(366);

    private static readonly long FirstChunk = TimeSpan.FromDays(8).Ticks;

    private readonly Dictionary<string, WorkCalendar> calendars;
    private readonly Dictionary<string, CalendarDates> dateSets;
    private readonly ConcurrentDictionary<string, TimeZoneInfo> zones = new(StringComparer.Ordinal);

    public CalendarEvaluator(IEnumerable<WorkCalendar> calendars, IEnumerable<CalendarDates> dateSets)
    {
        ArgumentNullException.ThrowIfNull(calendars);
        ArgumentNullException.ThrowIfNull(dateSets);
        this.calendars = calendars.ToDictionary(c => c.Name, StringComparer.Ordinal);
        this.dateSets = dateSets.ToDictionary(d => d.Name, StringComparer.Ordinal);
    }

    /// <summary>Whether a calendar of this name is known.</summary>
    public bool Contains(string calendar) => calendars.ContainsKey(calendar);

    /// <summary>
    /// The first allowed instant at or after <paramref name="from"/> and the end of its allowed interval, or why there
    /// is none.
    /// </summary>
    /// <exception cref="InvalidOperationException">The calendar, or one it composes with, is unknown or part of a cycle.</exception>
    public CalendarSlot Next(string calendar, DateTimeOffset from)
    {
        ArgumentNullException.ThrowIfNull(calendar);
        var start = from.UtcTicks;
        var horizon = Math.Min(start + Lookahead.Ticks, DateTimeOffset.MaxValue.UtcTicks);
        for (var length = FirstChunk; ; length = Math.Min(length * 4, Lookahead.Ticks))
        {
            var to = Math.Min(start + length, horizon);
            var (intervals, known) = Intervals(calendar, start, to, []);
            if (intervals.Count > 0)
            {
                var first = intervals[0];
                if (first.End < to)
                {
                    return new(CalendarSlotKind.Allowed, Instant(first.Start), Instant(first.End));
                }
                if (to == horizon)
                {
                    return new(CalendarSlotKind.Allowed, Instant(first.Start), null);
                }
            }
            else if (known < to)
            {
                return CalendarSlot.Unavailable;
            }
            else if (to == horizon)
            {
                return CalendarSlot.None;
            }
        }
    }

    /// <summary>
    /// Whether the calendar allows any instant of [<paramref name="from"/>, <paramref name="to"/>):
    /// <see cref="CalendarSlotKind.Allowed"/>, <see cref="CalendarSlotKind.None"/>, or
    /// <see cref="CalendarSlotKind.Unavailable"/> when no allowed instant is known and a date set does not cover the rest.
    /// </summary>
    /// <exception cref="InvalidOperationException">The calendar, or one it composes with, is unknown or part of a cycle.</exception>
    public CalendarSlotKind AllowsAny(string calendar, DateTimeOffset from, DateTimeOffset to)
    {
        ArgumentNullException.ThrowIfNull(calendar);
        var (intervals, known) = Intervals(calendar, from.UtcTicks, to.UtcTicks, []);
        return intervals.Count > 0 ? CalendarSlotKind.Allowed : known < to.UtcTicks ? CalendarSlotKind.Unavailable : CalendarSlotKind.None;
    }

    /// <summary>
    /// The instant of <paramref name="time"/> on <paramref name="date"/> in <paramref name="zone"/>, as calendars read
    /// local times: a time in a spring gap is the end of the gap, one in an autumn overlap its first instance.
    /// </summary>
    public static DateTimeOffset At(TimeZoneInfo zone, DateOnly date, TimeOnly time)
    {
        ArgumentNullException.ThrowIfNull(zone);
        return Instant(Utc(zone, date, time));
    }

    /// <summary>The date in <paramref name="zone"/> at <paramref name="instant"/>.</summary>
    public static DateOnly DateAt(TimeZoneInfo zone, DateTimeOffset instant)
    {
        ArgumentNullException.ThrowIfNull(zone);
        return LocalDate(zone, instant.UtcTicks);
    }

    /// <summary>The calendars that use the date set <paramref name="dateSet"/>, directly or through the calendars they compose with.</summary>
    public IReadOnlySet<string> DependingOn(string dateSet)
    {
        var depending = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in calendars.Keys)
        {
            if (Uses(name, dateSet, []))
            {
                depending.Add(name);
            }
        }
        return depending;
    }

    /// <summary>Refuses calendars that compose with an unknown calendar or form a cycle.</summary>
    /// <exception cref="InvalidOperationException">A referenced calendar is unknown, or calendars compose in a cycle.</exception>
    public static void Validate(IEnumerable<WorkCalendar> calendars)
    {
        var byName = calendars.ToDictionary(c => c.Name, StringComparer.Ordinal);
        var done = new HashSet<string>(StringComparer.Ordinal);
        void Visit(string name, List<string> path)
        {
            if (done.Contains(name))
            {
                return;
            }
            if (path.Contains(name, StringComparer.Ordinal))
            {
                throw new InvalidOperationException($"Calendars compose in a cycle: {string.Join(" -> ", path.SkipWhile(p => p != name).Append(name))}.");
            }
            if (!byName.TryGetValue(name, out var calendar))
            {
                throw new InvalidOperationException($"Calendar {path[^1]} composes with calendar {name}, which is neither registered nor stored.");
            }
            path.Add(name);
            foreach (var reference in calendar.References)
            {
                Visit(reference, path);
            }
            path.RemoveAt(path.Count - 1);
            done.Add(name);
        }
        foreach (var name in byName.Keys)
        {
            Visit(name, []);
        }
    }

    private bool Uses(string name, string dateSet, HashSet<string> path)
    {
        if (!calendars.TryGetValue(name, out var calendar) || !path.Add(name))
        {
            return false;
        }
        return calendar.DateSets.Contains(dateSet, StringComparer.Ordinal) || calendar.References.Any(r => Uses(r, dateSet, path));
    }

    /// <summary>
    /// The allowed intervals of the calendar within [<paramref name="from"/>, <paramref name="to"/>), sorted and merged,
    /// and the instant up to which they are known; intervals end there.
    /// </summary>
    private (List<Interval> Intervals, long Known) Intervals(string name, long from, long to, HashSet<string> path)
    {
        if (!calendars.TryGetValue(name, out var calendar))
        {
            throw new InvalidOperationException($"Calendar {name} is not stored.");
        }
        if (!path.Add(name))
        {
            throw new InvalidOperationException($"Calendar {name} composes with itself.");
        }
        var (result, known) = Own(calendar, from, to);
        foreach (var excepted in calendar.Excepted)
        {
            var (other, otherKnown) = Intervals(excepted, from, to, path);
            result = Subtract(result, other);
            known = Math.Min(known, otherKnown);
        }
        foreach (var added in calendar.Added)
        {
            var (other, otherKnown) = Intervals(added, from, to, path);
            result = Union(result, other);
            known = Math.Min(known, otherKnown);
        }
        path.Remove(name);
        return (Clip(result, from, known), known);
    }

    /// <summary>The calendar's own windows on its allowed days, before composition.</summary>
    private (List<Interval> Intervals, long Known) Own(WorkCalendar calendar, long from, long to)
    {
        var zone = Zone(calendar.TimeZone);
        var known = to;
        var intervals = new List<Interval>();
        // A window crossing midnight belongs to the day it starts on, so the day before the first one counts too.
        var last = LocalDate(zone, to);
        for (var day = LocalDate(zone, from).AddDays(-1); day <= last; day = day.AddDays(1))
        {
            if (!calendar.Weekdays.Contains(day.DayOfWeek)
                || calendar.AnnualExclusions.Contains((day.Month, day.Day))
                || calendar.ExcludedDates.Contains(day))
            {
                continue;
            }
            var unknown = false;
            var excluded = false;
            foreach (var name in calendar.DateSets)
            {
                if (!dateSets.TryGetValue(name, out var set) || day > set.CoversThrough)
                {
                    unknown = true;
                }
                else if (set.Dates.Contains(day))
                {
                    excluded = true;
                }
            }
            if (excluded)
            {
                continue;
            }
            if (unknown)
            {
                known = Math.Min(known, Utc(zone, day, TimeOnly.MinValue));
                break;
            }
            if (calendar.Windows.Count == 0)
            {
                intervals.Add(new(Utc(zone, day, TimeOnly.MinValue), Utc(zone, day.AddDays(1), TimeOnly.MinValue)));
                continue;
            }
            foreach (var window in calendar.Windows)
            {
                var start = Utc(zone, day, window.Start);
                var end = window.End > window.Start ? Utc(zone, day, window.End)
                    : window.End < window.Start ? Utc(zone, day.AddDays(1), window.End)
                    : Utc(zone, day.AddDays(1), window.Start);
                if (end > start)
                {
                    intervals.Add(new(start, end));
                }
            }
        }
        return (Normalize(intervals), known);
    }

    private TimeZoneInfo Zone(string id) => zones.GetOrAdd(id, TimeZoneInfo.FindSystemTimeZoneById);

    private static DateOnly LocalDate(TimeZoneInfo zone, long utcTicks) =>
        DateOnly.FromDateTime(TimeZoneInfo.ConvertTimeFromUtc(new DateTime(utcTicks, DateTimeKind.Utc), zone));

    /// <summary>
    /// The instant of a local date and time: a time in a spring gap maps to the end of the gap, a repeated time in an
    /// autumn overlap to its first instance.
    /// </summary>
    private static long Utc(TimeZoneInfo zone, DateOnly date, TimeOnly time)
    {
        var local = DateTime.SpecifyKind(date.ToDateTime(time), DateTimeKind.Unspecified);
        if (zone.IsAmbiguousTime(local))
        {
            return (local - zone.GetAmbiguousTimeOffsets(local).Max()).Ticks;
        }
        if (!zone.IsInvalidTime(local))
        {
            return (local - zone.GetUtcOffset(local)).Ticks;
        }
        // The offset after the gap applies from the transition, which lies between the two readings of the local time.
        var before = zone.GetUtcOffset(local.AddDays(-1));
        var after = zone.GetUtcOffset(local.AddDays(1));
        long low = (local - after).Ticks, high = (local - before).Ticks;
        while (low < high)
        {
            var middle = low + ((high - low) / 2);
            if (zone.GetUtcOffset(new DateTime(middle, DateTimeKind.Utc)) == after)
            {
                high = middle;
            }
            else
            {
                low = middle + 1;
            }
        }
        return low;
    }

    private static DateTimeOffset Instant(long utcTicks) => new(utcTicks, TimeSpan.Zero);

    private readonly record struct Interval(long Start, long End);

    private static List<Interval> Normalize(List<Interval> intervals)
    {
        intervals.Sort((a, b) => a.Start.CompareTo(b.Start));
        var merged = new List<Interval>(intervals.Count);
        foreach (var interval in intervals)
        {
            if (merged.Count > 0 && interval.Start <= merged[^1].End)
            {
                merged[^1] = merged[^1] with { End = Math.Max(merged[^1].End, interval.End) };
            }
            else
            {
                merged.Add(interval);
            }
        }
        return merged;
    }

    private static List<Interval> Union(List<Interval> a, List<Interval> b) => Normalize([.. a, .. b]);

    private static List<Interval> Subtract(List<Interval> a, List<Interval> b)
    {
        var result = new List<Interval>();
        var j = 0;
        foreach (var interval in a)
        {
            var start = interval.Start;
            while (j < b.Count && b[j].End <= start)
            {
                j++;
            }
            for (var k = j; k < b.Count && b[k].Start < interval.End; k++)
            {
                if (b[k].Start > start)
                {
                    result.Add(new(start, b[k].Start));
                }
                start = Math.Max(start, b[k].End);
            }
            if (start < interval.End)
            {
                result.Add(new(start, interval.End));
            }
        }
        return result;
    }

    private static List<Interval> Clip(List<Interval> intervals, long from, long to)
    {
        var clipped = new List<Interval>(intervals.Count);
        foreach (var interval in intervals)
        {
            var start = Math.Max(interval.Start, from);
            var end = Math.Min(interval.End, to);
            if (start < end)
            {
                clipped.Add(new(start, end));
            }
        }
        return clipped;
    }
}
