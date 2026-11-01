namespace Corfedo.Work.Jobs.Tests;

/// <summary>
/// Completion deadlines and their windows (M-1) with explicit instants: allowed and disallowed days, holidays,
/// daylight-saving changes, windows crossing midnight, uncovered dates and deadlines processed late.
/// </summary>
public sealed class DeadlineCalculatorTests
{
    private static readonly CompletionDeadline EightInWarsaw = new(new TimeOnly(8, 0), "Europe/Warsaw", null);

    [Fact]
    public void WithoutACalendarEveryDayHasADeadline()
    {
        var deadlines = new DeadlineCalculator(EightInWarsaw, null);

        Assert.Equal(Utc(2027, 1, 4, 7), deadlines.Next(Utc(2027, 1, 4, 6)));
        Assert.Equal(Utc(2027, 1, 5, 7), deadlines.Next(Utc(2027, 1, 4, 7)));
        Assert.Equal(Utc(2027, 1, 4, 7), deadlines.Previous(Utc(2027, 1, 4, 8)));
        Assert.Equal(Utc(2027, 1, 3, 7), deadlines.Previous(Utc(2027, 1, 4, 7)));
    }

    [Fact]
    public void DaysTheCalendarDoesNotAllowAndHolidaysHaveNoDeadline()
    {
        var business = WorkCalendar.Create("business", 1)
            .InTimeZone("Europe/Warsaw")
            .AllowWeekdays(DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday)
            .AllowBetween(new TimeOnly(9, 0), new TimeOnly(17, 0))
            .ExcludeAnnual(12, 24)
            .ExcludeDate(new DateOnly(2027, 12, 27));
        var deadlines = new DeadlineCalculator(EightInWarsaw with { Calendar = "business" }, new CalendarEvaluator([business], []));

        // A day counts when the calendar allows any instant of it, here 09:00-17:00, after the 08:00 deadline.
        Assert.Equal(Utc(2027, 12, 23, 7), deadlines.Next(Utc(2027, 12, 22, 7)));
        Assert.Equal(Utc(2027, 12, 28, 7), deadlines.Next(Utc(2027, 12, 23, 7)));
        Assert.Equal(Utc(2027, 12, 23, 7), deadlines.Previous(Utc(2027, 12, 28, 7)));
    }

    [Fact]
    public void DeadlinesKeepTheirLocalTimeAcrossDaylightSavingChanges()
    {
        var deadlines = new DeadlineCalculator(EightInWarsaw, null);

        Assert.Equal(Utc(2027, 3, 27, 7), deadlines.Next(Utc(2027, 3, 27, 0)));
        Assert.Equal(Utc(2027, 3, 28, 6), deadlines.Next(Utc(2027, 3, 27, 7)));
        Assert.Equal(Utc(2027, 10, 30, 6), deadlines.Next(Utc(2027, 10, 30, 0)));
        Assert.Equal(Utc(2027, 10, 31, 7), deadlines.Next(Utc(2027, 10, 30, 6)));

        // A time in the spring gap is the end of the gap, one in the autumn overlap its first instance.
        var night = new DeadlineCalculator(new CompletionDeadline(new TimeOnly(2, 30), "Europe/Warsaw", null), null);
        Assert.Equal(Utc(2027, 3, 28, 1), night.Next(Utc(2027, 3, 27, 12)));
        Assert.Equal(Utc(2027, 10, 31, 0, 30), night.Next(Utc(2027, 10, 30, 12)));
    }

    [Fact]
    public void AWindowCrossingMidnightAllowsTheDayItEndsOn()
    {
        var nights = WorkCalendar.Create("nights", 1).AllowWeekdays(DayOfWeek.Monday).AllowBetween(new TimeOnly(22, 0), new TimeOnly(2, 0));
        var deadlines = new DeadlineCalculator(new CompletionDeadline(new TimeOnly(6, 0), "UTC", "nights"), new CalendarEvaluator([nights], []));

        Assert.Equal(Utc(2027, 1, 4, 6), deadlines.Next(Utc(2027, 1, 3, 6)));
        Assert.Equal(Utc(2027, 1, 5, 6), deadlines.Next(Utc(2027, 1, 4, 6)));
        Assert.Equal(Utc(2027, 1, 11, 6), deadlines.Next(Utc(2027, 1, 5, 6)));
    }

    [Fact]
    public void DaysADateSetDoesNotCoverHoldTheDeadline()
    {
        var calendar = WorkCalendar.Create("holidays", 1).InTimeZone("Europe/Warsaw").ExcludeDates("public");
        var dates = new CalendarDates("public", 1, new HashSet<DateOnly> { new(2027, 1, 6) }, new DateOnly(2027, 1, 7));
        var deadlines = new DeadlineCalculator(EightInWarsaw with { Calendar = "holidays" }, new CalendarEvaluator([calendar], [dates]));

        Assert.Equal(Utc(2027, 1, 5, 7), deadlines.Next(Utc(2027, 1, 5, 6)));
        Assert.Equal(Utc(2027, 1, 7, 7), deadlines.Next(Utc(2027, 1, 5, 7)));
        Assert.Equal(ExpectationWindow.Uncovered, deadlines.Next(Utc(2027, 1, 7, 7)));

        var step = deadlines.Process(new ExpectationWindow(Utc(2027, 1, 7, 7), Utc(2027, 1, 5, 7), MetAt: Utc(2027, 1, 7, 6)), Utc(2027, 1, 7, 8));
        Assert.Equal(new ExpectationWindow(ExpectationWindow.Uncovered, Utc(2027, 1, 7, 7)), step.Window);
        Assert.Empty(step.Missed);
    }

    [Fact]
    public void ARunCompletingTheEveningBeforeMeetsTheMorningDeadline()
    {
        var deadlines = new DeadlineCalculator(EightInWarsaw, null);

        var step = deadlines.Process(new ExpectationWindow(Utc(2027, 1, 5, 7), Utc(2027, 1, 4, 7), MetAt: Utc(2027, 1, 4, 22)), Utc(2027, 1, 5, 7, 1));

        Assert.Empty(step.Missed);
        Assert.Equal(new ExpectationWindow(Utc(2027, 1, 6, 7), Utc(2027, 1, 5, 7)), step.Window);
    }

    [Fact]
    public void ALateCompletionResolvesTheMissedDeadlineAndStartsTheNextWindow()
    {
        var deadlines = new DeadlineCalculator(EightInWarsaw, null);
        var late = Utc(2027, 1, 5, 7, 12);

        var step = deadlines.Process(new ExpectationWindow(Utc(2027, 1, 5, 7), Utc(2027, 1, 4, 7), FirstAfter: late, LastAfter: late), Utc(2027, 1, 5, 7, 30));

        Assert.Equal([new MissedDeadline(Utc(2027, 1, 5, 7), late)], step.Missed);
        Assert.Equal(new ExpectationWindow(Utc(2027, 1, 6, 7), late), step.Window);
    }

    [Fact]
    public void ACompletionThatResolvedTheIncidentAlreadyStartedTheWindow()
    {
        var deadlines = new DeadlineCalculator(EightInWarsaw, null);
        var resolving = Utc(2027, 1, 5, 7, 12);

        // The completion that resolved an earlier incident started the window after this deadline: it belongs to
        // that incident, which was open at the deadline, so nothing opens.
        var step = deadlines.Process(new ExpectationWindow(Utc(2027, 1, 5, 7), resolving, FirstAfter: resolving, LastAfter: Utc(2027, 1, 5, 9)), Utc(2027, 1, 5, 10));

        Assert.Empty(step.Missed);
        Assert.Equal(new ExpectationWindow(Utc(2027, 1, 6, 7), resolving, MetAt: Utc(2027, 1, 5, 9)), step.Window);
    }

    [Fact]
    public void AMissedDeadlineWithoutCompletionsStaysOneOpenIncident()
    {
        var deadlines = new DeadlineCalculator(EightInWarsaw, null);

        var step = deadlines.Process(new ExpectationWindow(Utc(2027, 1, 5, 7), Utc(2027, 1, 4, 7)), Utc(2027, 1, 8, 8));

        Assert.Equal([new MissedDeadline(Utc(2027, 1, 5, 7), null)], step.Missed);
        Assert.Equal(new ExpectationWindow(Utc(2027, 1, 9, 7), Utc(2027, 1, 8, 7)), step.Window);
    }

    [Fact]
    public void DeadlinesPassedWhileNothingMonitoredAreProcessedInOrder()
    {
        var deadlines = new DeadlineCalculator(EightInWarsaw, null);

        // Completions after the 5th's deadline on the 6th and the 7th; nothing processed until the 9th.
        var step = deadlines.Process(
            new ExpectationWindow(Utc(2027, 1, 5, 7), Utc(2027, 1, 4, 7), FirstAfter: Utc(2027, 1, 6, 10), LastAfter: Utc(2027, 1, 7, 10)),
            Utc(2027, 1, 9, 8));

        Assert.Equal(
            [
                new MissedDeadline(Utc(2027, 1, 5, 7), Utc(2027, 1, 6, 10)),
                new MissedDeadline(Utc(2027, 1, 7, 7), Utc(2027, 1, 7, 10)),
                new MissedDeadline(Utc(2027, 1, 8, 7), null),
            ],
            step.Missed);
        Assert.Equal(new ExpectationWindow(Utc(2027, 1, 10, 7), Utc(2027, 1, 9, 7)), step.Window);
    }

    [Fact]
    public void AMetDeadlinePlacesLaterCompletionsAgainstTheNextOnes()
    {
        var deadlines = new DeadlineCalculator(EightInWarsaw, null);

        var step = deadlines.Process(
            new ExpectationWindow(Utc(2027, 1, 5, 7), Utc(2027, 1, 4, 7), MetAt: Utc(2027, 1, 4, 20), FirstAfter: Utc(2027, 1, 5, 9), LastAfter: Utc(2027, 1, 6, 9)),
            Utc(2027, 1, 6, 10));

        Assert.Empty(step.Missed);
        Assert.Equal(new ExpectationWindow(Utc(2027, 1, 7, 7), Utc(2027, 1, 6, 7), MetAt: Utc(2027, 1, 6, 9)), step.Window);
    }

    private static DateTimeOffset Utc(int year, int month, int day, int hour, int minute = 0) => new(year, month, day, hour, minute, 0, TimeSpan.Zero);
}
