using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Corfedo.Work;

/// <summary>
/// A named rule for when work may start, in local time (J16): allowed weekdays and daily windows in a time zone, minus
/// annual and single excluded dates and the dates of published date sets, minus the calendars it excepts, plus the
/// calendars it adds. Stored by revision like a job definition (R18); every process plans with the newest stored one.
/// </summary>
/// <remarks>
/// A window's start is inclusive and its end exclusive. A window whose end is before its start crosses midnight and
/// belongs to the day it starts on; equal start and end allow the whole day. Without windows an allowed day is
/// available all day. A local time that does not exist (a spring gap) maps to the end of the gap; a repeated local time
/// (an autumn overlap) maps to its first instance.
/// </remarks>
public sealed class WorkCalendar
{
    private static readonly DayOfWeek[] AllDays =
        [DayOfWeek.Sunday, DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday, DayOfWeek.Saturday];

    private WorkCalendar(string name, long revision)
    {
        Name = name;
        Revision = revision;
    }

    private WorkCalendar(WorkCalendar other)
        : this(other.Name, other.Revision)
    {
        TimeZone = other.TimeZone;
        Weekdays = other.Weekdays;
        Windows = other.Windows;
        AnnualExclusions = other.AnnualExclusions;
        ExcludedDates = other.ExcludedDates;
        DateSets = other.DateSets;
        Excepted = other.Excepted;
        Added = other.Added;
    }

    /// <summary>The stable registered name, never a CLR name.</summary>
    public string Name { get; }

    /// <summary>Increases whenever the rule changes.</summary>
    public long Revision { get; }

    /// <summary>The time zone id the rule is evaluated in; <c>UTC</c> unless set.</summary>
    public string TimeZone { get; private init; } = "UTC";

    /// <summary>The allowed weekdays; every day unless set.</summary>
    public IReadOnlyList<DayOfWeek> Weekdays { get; private init; } = AllDays;

    /// <summary>The daily windows of an allowed day; none allows the whole day.</summary>
    public IReadOnlyList<CalendarWindow> Windows { get; private init; } = [];

    /// <summary>Month and day excluded every year.</summary>
    public IReadOnlyList<(int Month, int Day)> AnnualExclusions { get; private init; } = [];

    /// <summary>Single excluded dates.</summary>
    public IReadOnlyList<DateOnly> ExcludedDates { get; private init; } = [];

    /// <summary>Names of published date sets whose dates are excluded.</summary>
    public IReadOnlyList<string> DateSets { get; private init; } = [];

    /// <summary>Names of calendars whose allowed instants are subtracted.</summary>
    public IReadOnlyList<string> Excepted { get; private init; } = [];

    /// <summary>Names of calendars whose allowed instants are added after the subtraction.</summary>
    public IReadOnlyList<string> Added { get; private init; } = [];

    /// <summary>The names of the calendars this one composes with.</summary>
    public IEnumerable<string> References => Excepted.Concat(Added);

    /// <summary>A digest of the rule; equal revisions must have equal digests.</summary>
    public byte[] Digest => SHA256.HashData(Encoding.UTF8.GetBytes(Rule));

    /// <summary>The canonical text of the rule, independent of the order of declaration; <see cref="Parse"/> reads it back.</summary>
    public string Rule
    {
        get
        {
            var lines = new List<string> { "tz:" + TimeZone, "weekdays:" + string.Join(",", Weekdays.Select(d => ((int)d).ToString(CultureInfo.InvariantCulture))) };
            lines.AddRange(Windows.Select(w => "window:" + Format(w.Start) + "-" + Format(w.End)));
            lines.AddRange(AnnualExclusions.Select(a => "annual:" + a.Month.ToString("00", CultureInfo.InvariantCulture) + "-" + a.Day.ToString("00", CultureInfo.InvariantCulture)));
            lines.AddRange(ExcludedDates.Select(d => "date:" + d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)));
            lines.AddRange(DateSets.Select(s => "dates:" + s));
            lines.AddRange(Excepted.Select(s => "except:" + s));
            lines.AddRange(Added.Select(s => "also:" + s));
            return string.Join("\n", lines);
        }
    }

    /// <summary>A calendar that allows every instant in UTC until its rules restrict it.</summary>
    public static WorkCalendar Create(string name, long revision)
    {
        CheckName(name, nameof(name));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(revision);
        return new WorkCalendar(name, revision);
    }

    /// <summary>Evaluates the rule in the time zone <paramref name="id"/>, such as <c>Europe/Warsaw</c>.</summary>
    /// <exception cref="TimeZoneNotFoundException">The time zone is not known on this system.</exception>
    public WorkCalendar InTimeZone(string id)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        TimeZoneInfo.FindSystemTimeZoneById(id);
        return new(this) { TimeZone = id };
    }

    /// <summary>Allows only <paramref name="days"/>; replaces earlier weekdays.</summary>
    public WorkCalendar AllowWeekdays(params DayOfWeek[] days)
    {
        ArgumentNullException.ThrowIfNull(days);
        if (days.Length == 0 || days.Any(d => !Enum.IsDefined(d)))
        {
            throw new ArgumentException("At least one weekday, each a defined day.", nameof(days));
        }
        return new(this) { Weekdays = [.. days.Distinct().Order()] };
    }

    /// <summary>
    /// Adds a daily window from <paramref name="start"/>, inclusive, to <paramref name="end"/>, exclusive; an end before
    /// the start crosses midnight, an equal one allows the whole day.
    /// </summary>
    public WorkCalendar AllowBetween(TimeOnly start, TimeOnly end) =>
        new(this) { Windows = [.. Windows.Append(new CalendarWindow(start, end)).Distinct().Order()] };

    /// <summary>Excludes <paramref name="month"/>/<paramref name="day"/> every year; February 29 only in leap years.</summary>
    public WorkCalendar ExcludeAnnual(int month, int day)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(month, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(month, 12);
        ArgumentOutOfRangeException.ThrowIfLessThan(day, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(day, DateTime.DaysInMonth(2000, month));
        return new(this) { AnnualExclusions = [.. AnnualExclusions.Append((month, day)).Distinct().Order()] };
    }

    /// <summary>Excludes one date.</summary>
    public WorkCalendar ExcludeDate(DateOnly date) => new(this) { ExcludedDates = [.. ExcludedDates.Append(date).Distinct().Order()] };

    /// <summary>
    /// Excludes the dates of the published date set <paramref name="dateSet"/>. Dates beyond the set's coverage are
    /// unknown: work that needs one waits until a covering revision is published.
    /// </summary>
    public WorkCalendar ExcludeDates(string dateSet)
    {
        CheckName(dateSet, nameof(dateSet));
        return new(this) { DateSets = [.. DateSets.Append(dateSet).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal)] };
    }

    /// <summary>Subtracts the allowed instants of the calendar <paramref name="calendar"/>.</summary>
    public WorkCalendar Except(string calendar)
    {
        CheckName(calendar, nameof(calendar));
        return new(this) { Excepted = [.. Excepted.Append(calendar).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal)] };
    }

    /// <summary>Adds the allowed instants of the calendar <paramref name="calendar"/>, after the subtractions.</summary>
    public WorkCalendar Also(string calendar)
    {
        CheckName(calendar, nameof(calendar));
        return new(this) { Added = [.. Added.Append(calendar).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal)] };
    }

    /// <summary>Reads a calendar from the canonical <paramref name="rule"/> a store keeps.</summary>
    /// <exception cref="FormatException">The rule is not in the canonical form.</exception>
    public static WorkCalendar Parse(string name, long revision, string rule)
    {
        ArgumentNullException.ThrowIfNull(rule);
        var timeZone = "UTC";
        IReadOnlyList<DayOfWeek> weekdays = AllDays;
        var windows = new List<CalendarWindow>();
        var annual = new List<(int, int)>();
        var dates = new List<DateOnly>();
        var sets = new List<string>();
        var excepted = new List<string>();
        var added = new List<string>();
        foreach (var line in rule.Split('\n'))
        {
            var colon = line.IndexOf(':', StringComparison.Ordinal);
            if (colon < 0)
            {
                throw new FormatException($"Calendar {name} has a malformed rule line.");
            }
            var value = line[(colon + 1)..];
            switch (line[..colon])
            {
                case "tz":
                    timeZone = value;
                    break;
                case "weekdays":
                    weekdays = [.. value.Split(',').Select(d => (DayOfWeek)int.Parse(d, CultureInfo.InvariantCulture))];
                    break;
                case "window":
                    var dash = value.IndexOf('-', StringComparison.Ordinal);
                    windows.Add(new CalendarWindow(ParseTime(value[..dash]), ParseTime(value[(dash + 1)..])));
                    break;
                case "annual":
                    annual.Add((int.Parse(value[..2], CultureInfo.InvariantCulture), int.Parse(value[3..], CultureInfo.InvariantCulture)));
                    break;
                case "date":
                    dates.Add(DateOnly.ParseExact(value, "yyyy-MM-dd", CultureInfo.InvariantCulture));
                    break;
                case "dates":
                    sets.Add(value);
                    break;
                case "except":
                    excepted.Add(value);
                    break;
                case "also":
                    added.Add(value);
                    break;
                default:
                    throw new FormatException($"Calendar {name} has an unknown rule line.");
            }
        }
        return new WorkCalendar(Create(name, revision))
        {
            TimeZone = timeZone,
            Weekdays = weekdays,
            Windows = windows,
            AnnualExclusions = annual,
            ExcludedDates = dates,
            DateSets = sets,
            Excepted = excepted,
            Added = added,
        };
    }

    public override string ToString() => Name;

    internal static void CheckName(string name, string parameter)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name, parameter);
        if (name.Any(char.IsControl))
        {
            throw new ArgumentException("A calendar or date set name cannot contain control characters.", parameter);
        }
    }

    private static string Format(TimeOnly time) => time.ToString("HH:mm:ss.fffffff", CultureInfo.InvariantCulture);

    private static TimeOnly ParseTime(string text) => TimeOnly.ParseExact(text, "HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
}

/// <summary>A daily window of a calendar: from <see cref="Start"/>, inclusive, to <see cref="End"/>, exclusive.</summary>
public readonly record struct CalendarWindow(TimeOnly Start, TimeOnly End) : IComparable<CalendarWindow>
{
    public int CompareTo(CalendarWindow other)
    {
        var start = Start.CompareTo(other.Start);
        return start != 0 ? start : End.CompareTo(other.End);
    }
}
