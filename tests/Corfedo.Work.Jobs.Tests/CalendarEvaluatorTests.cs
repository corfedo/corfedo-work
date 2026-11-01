namespace Corfedo.Work.Jobs.Tests;

/// <summary>
/// Allowed instants of calendars across days, windows crossing midnight, composition, date sets and daylight-saving
/// changes (CAL-1, CAL-3, CAL-6), evaluated without a store.
/// </summary>
public sealed class CalendarEvaluatorTests
{
    /// <summary>CAL-1: business days skip the weekend, an annual and a single excluded date.</summary>
    [Fact]
    public void BusinessDaysSkipTheWeekendAndExcludedDates()
    {
        var evaluator = Evaluator(WorkCalendar.Create("business", 1)
            .InTimeZone("Europe/Warsaw")
            .AllowWeekdays(DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday)
            .AllowBetween(new TimeOnly(8, 0), new TimeOnly(18, 0))
            .ExcludeAnnual(12, 24)
            .ExcludeDate(new DateOnly(2027, 12, 27)));

        Assert.Equal(Allowed(Utc(2027, 12, 23, 16), Utc(2027, 12, 23, 17)), evaluator.Next("business", Utc(2027, 12, 23, 16)));
        Assert.Equal(Allowed(Utc(2027, 12, 28, 7), Utc(2027, 12, 28, 17)), evaluator.Next("business", Utc(2027, 12, 23, 17)));
    }

    /// <summary>CAL-3: a window crossing midnight belongs to the day it starts on.</summary>
    [Fact]
    public void WindowCrossingMidnightBelongsToItsStartDay()
    {
        var evaluator = Evaluator(WorkCalendar.Create("nights", 1).AllowWeekdays(DayOfWeek.Friday).AllowBetween(new TimeOnly(22, 0), new TimeOnly(6, 0)));

        Assert.Equal(Allowed(Utc(2027, 1, 2, 3), Utc(2027, 1, 2, 6)), evaluator.Next("nights", Utc(2027, 1, 2, 3)));
        Assert.Equal(Allowed(Utc(2027, 1, 8, 22), Utc(2027, 1, 9, 6)), evaluator.Next("nights", Utc(2027, 1, 2, 7)));
        Assert.Equal(Allowed(Utc(2027, 1, 1, 22), Utc(2027, 1, 2, 6)), evaluator.Next("nights", Utc(2026, 12, 31, 23)));
    }

    /// <summary>Equal start and end allow a full day from the start; adjacent days form one window.</summary>
    [Fact]
    public void EqualStartAndEndAllowAFullDay()
    {
        var evaluator = Evaluator(
            WorkCalendar.Create("mondays", 1).AllowWeekdays(DayOfWeek.Monday).AllowBetween(new TimeOnly(9, 0), new TimeOnly(9, 0)),
            WorkCalendar.Create("weekend", 1).AllowWeekdays(DayOfWeek.Saturday, DayOfWeek.Sunday));

        Assert.Equal(Allowed(Utc(2027, 1, 4, 15), Utc(2027, 1, 5, 9)), evaluator.Next("mondays", Utc(2027, 1, 4, 15)));
        Assert.Equal(Allowed(Utc(2027, 1, 2, 0), Utc(2027, 1, 4, 0)), evaluator.Next("weekend", Utc(2027, 1, 1, 12)));
    }

    /// <summary>CAL-3: <c>Except</c> subtracts another calendar's instants, <c>Also</c> adds them after.</summary>
    [Fact]
    public void CompositionSubtractsThenAdds()
    {
        var evaluator = Evaluator(
            WorkCalendar.Create("lunch", 1).AllowBetween(new TimeOnly(12, 0), new TimeOnly(13, 0)),
            WorkCalendar.Create("office", 1).AllowBetween(new TimeOnly(8, 0), new TimeOnly(18, 0)).Except("lunch"),
            WorkCalendar.Create("evenings", 1).AllowWeekdays(DayOfWeek.Monday).AllowBetween(new TimeOnly(20, 0), new TimeOnly(21, 0)).Also("lunch"),
            WorkCalendar.Create("always", 1),
            WorkCalendar.Create("never", 1).AllowBetween(new TimeOnly(10, 0), new TimeOnly(11, 0)).Except("always"));

        Assert.Equal(Allowed(Utc(2027, 1, 5, 11), Utc(2027, 1, 5, 12)), evaluator.Next("office", Utc(2027, 1, 5, 11)));
        Assert.Equal(Allowed(Utc(2027, 1, 5, 13), Utc(2027, 1, 5, 18)), evaluator.Next("office", Utc(2027, 1, 5, 12, 30)));
        Assert.Equal(Allowed(Utc(2027, 1, 5, 12), Utc(2027, 1, 5, 13)), evaluator.Next("evenings", Utc(2027, 1, 5, 9)));
        Assert.Equal(CalendarSlot.None, evaluator.Next("never", Utc(2027, 1, 5, 9)));
    }

    /// <summary>CAL-3: calendars composing in a cycle or with an unknown one are refused.</summary>
    [Fact]
    public void CyclesAndUnknownReferencesAreRefused()
    {
        var cycle = Assert.Throws<InvalidOperationException>(() => CalendarEvaluator.Validate(
        [
            WorkCalendar.Create("a", 1).Except("b"),
            WorkCalendar.Create("b", 1).Also("c"),
            WorkCalendar.Create("c", 1).Except("a"),
        ]));
        Assert.Contains("cycle", cycle.Message, StringComparison.Ordinal);
        Assert.Throws<InvalidOperationException>(() => CalendarEvaluator.Validate([WorkCalendar.Create("a", 1).Except("missing")]));
        CalendarEvaluator.Validate([WorkCalendar.Create("a", 1).Except("b"), WorkCalendar.Create("b", 1)]);
    }

    /// <summary>CAL-6: dates of a set are excluded; a date beyond its coverage, or a set never published, is unavailable.</summary>
    [Fact]
    public void DateSetsExcludeTheirDatesAndEndAtTheirCoverage()
    {
        var holidays = new CalendarDates("holidays", 3, new HashSet<DateOnly> { new(2027, 1, 6) }, new DateOnly(2027, 1, 10));
        var evaluator = new CalendarEvaluator(
            [
                WorkCalendar.Create("public", 1).ExcludeDates("holidays"),
                WorkCalendar.Create("office", 1).AllowBetween(new TimeOnly(8, 0), new TimeOnly(18, 0)).Except("closed"),
                WorkCalendar.Create("closed", 1).Except("public"),
                WorkCalendar.Create("other", 1).ExcludeDates("unpublished"),
            ],
            [holidays]);

        Assert.Equal(Allowed(Utc(2027, 1, 5, 10), Utc(2027, 1, 6, 0)), evaluator.Next("public", Utc(2027, 1, 5, 10)));
        Assert.Equal(Allowed(Utc(2027, 1, 7, 0), Utc(2027, 1, 11, 0)), evaluator.Next("public", Utc(2027, 1, 6, 10)));
        Assert.Equal(CalendarSlot.Unavailable, evaluator.Next("public", Utc(2027, 1, 11, 10)));
        Assert.Equal(Allowed(Utc(2027, 1, 7, 8), Utc(2027, 1, 7, 18)), evaluator.Next("office", Utc(2027, 1, 6, 9)));
        Assert.Equal(CalendarSlot.Unavailable, evaluator.Next("other", Utc(2027, 1, 6, 9)));
        Assert.Equal(new HashSet<string> { "public", "closed", "office" }, evaluator.DependingOn("holidays"));
    }

    /// <summary>A window start in the spring gap maps to the end of the gap; one in the autumn overlap to its first instance.</summary>
    [Fact]
    public void WindowsAcrossDaylightSavingChanges()
    {
        var evaluator = Evaluator(WorkCalendar.Create("night", 1).InTimeZone("Europe/Warsaw").AllowBetween(new TimeOnly(2, 30), new TimeOnly(4, 0)));

        Assert.Equal(Allowed(Utc(2027, 3, 28, 1), Utc(2027, 3, 28, 2)), evaluator.Next("night", Utc(2027, 3, 28, 0)));
        Assert.Equal(Allowed(Utc(2027, 10, 31, 0, 30), Utc(2027, 10, 31, 3)), evaluator.Next("night", Utc(2027, 10, 31, 0)));
    }

    /// <summary>The stored rule reads back as the same calendar and does not depend on the order of declaration.</summary>
    [Fact]
    public void RuleRoundTripsIndependentOfOrder()
    {
        var first = WorkCalendar.Create("c", 2).InTimeZone("Europe/Warsaw").ExcludeDate(new DateOnly(2027, 1, 2)).ExcludeAnnual(12, 25)
            .AllowBetween(new TimeOnly(13, 0), new TimeOnly(18, 0)).AllowBetween(new TimeOnly(8, 0), new TimeOnly(12, 0, 0, 500)).Except("b").Except("a");
        var second = WorkCalendar.Create("c", 2).Except("a").Except("b").AllowBetween(new TimeOnly(8, 0), new TimeOnly(12, 0, 0, 500))
            .AllowBetween(new TimeOnly(13, 0), new TimeOnly(18, 0)).ExcludeAnnual(12, 25).ExcludeDate(new DateOnly(2027, 1, 2)).InTimeZone("Europe/Warsaw");

        Assert.Equal(first.Digest, second.Digest);
        var parsed = WorkCalendar.Parse("c", 2, first.Rule);
        Assert.Equal(first.Rule, parsed.Rule);
        Assert.Equal(new TimeOnly(12, 0, 0, 500), parsed.Windows[0].End);
    }

    private static CalendarEvaluator Evaluator(params WorkCalendar[] calendars) => new(calendars, []);

    private static CalendarSlot Allowed(DateTimeOffset start, DateTimeOffset end) => new(CalendarSlotKind.Allowed, start, end);

    private static DateTimeOffset Utc(int year, int month, int day, int hour, int minute = 0) => new(year, month, day, hour, minute, 0, TimeSpan.Zero);
}
