namespace Corfedo.Work.Jobs.Tests;

/// <summary>Cron occurrences across daylight-saving changes (J-5, J-34) and refused expressions (J-23).</summary>
public sealed class CronSemanticsTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>J-5: a daily job at 02:30 in Europe/Warsaw runs once per day across both changes.</summary>
    [Fact]
    public void DailyJobInTheGapAndTheOverlapRunsOncePerDay()
    {
        var schedule = Cron("30 2 * * *", "Europe/Warsaw");

        Assert.Equal(
            [Utc(3, 27, 1, 30), Utc(3, 28, 1, 30), Utc(3, 29, 1, 0), Utc(3, 30, 0, 30)],
            Occurrences(schedule, Utc(3, 27, 0, 0), 4));
        Assert.Equal(
            [Utc(10, 24, 0, 30), Utc(10, 25, 0, 30), Utc(10, 26, 1, 30)],
            Occurrences(schedule, Utc(10, 24, 0, 0), 3));
    }

    /// <summary>J-34: an interval expression loses nothing in spring and runs in both instances of the repeated hour.</summary>
    [Fact]
    public void IntervalExpressionAcrossBothChanges()
    {
        var schedule = Cron("*/30 * * * *", "America/New_York");

        Assert.Equal(
            [Utc(3, 8, 6, 0), Utc(3, 8, 6, 30), Utc(3, 8, 7, 0), Utc(3, 8, 7, 30)],
            Occurrences(schedule, Utc(3, 8, 5, 59), 4));
        Assert.Equal(
            [Utc(11, 1, 4, 30), Utc(11, 1, 5, 0), Utc(11, 1, 5, 30), Utc(11, 1, 6, 0), Utc(11, 1, 6, 30), Utc(11, 1, 7, 0)],
            Occurrences(schedule, Utc(11, 1, 4, 29), 6));
    }

    /// <summary>J-34: a daily expression in the repeated hour runs only in its first instance.</summary>
    [Fact]
    public void DailyExpressionRunsOnlyInTheFirstInstanceOfTheRepeatedHour()
    {
        var schedule = Cron("30 1 * * *", "America/New_York");

        Assert.Equal([Utc(3, 7, 6, 30), Utc(3, 8, 6, 30), Utc(3, 9, 5, 30)], Occurrences(schedule, Utc(3, 7, 0, 0), 3));
        Assert.Equal([Utc(10, 31, 5, 30), Utc(11, 1, 5, 30), Utc(11, 2, 6, 30)], Occurrences(schedule, Utc(10, 31, 0, 0), 3));
    }

    [Fact]
    public void SixFieldsIncludeSeconds()
    {
        var schedule = Cron("*/15 * * * * *", "UTC");

        Assert.Equal([Utc(1, 1, 0, 0).AddSeconds(15), Utc(1, 1, 0, 0).AddSeconds(30)], Occurrences(schedule, Utc(1, 1, 0, 0), 2));
    }

    /// <summary>J-23: an expression with no future occurrence is refused.</summary>
    [Theory]
    [InlineData("0 0 30 2 *", "UTC")]
    [InlineData("not cron", "UTC")]
    [InlineData("H 2 * * *", "UTC")]
    [InlineData("0 2 * * *", "Not/AZone")]
    public void ExpressionsWithoutFutureOccurrencesOrInvalidOnesAreRefused(string expression, string zone) =>
        Assert.Throws<ArgumentException>(() => RecurrenceSchedule.Create(Schedule.Cron(expression, zone), Now));

    private static RecurrenceSchedule Cron(string expression, string zone) => RecurrenceSchedule.Create(Schedule.Cron(expression, zone), Now);

    private static DateTimeOffset[] Occurrences(RecurrenceSchedule schedule, DateTimeOffset after, int count)
    {
        var result = new DateTimeOffset[count];
        for (var i = 0; i < count; i++)
        {
            after = schedule.Next(after) ?? throw new InvalidOperationException("No further occurrence.");
            result[i] = after;
        }
        return result;
    }

    private static DateTimeOffset Utc(int month, int day, int hour, int minute) => new(2026, month, day, hour, minute, 0, TimeSpan.Zero);
}
