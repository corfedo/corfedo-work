namespace Corfedo.Work.Jobs.Tests;

/// <summary>The first instant of a new or replaced schedule, without a store.</summary>
public sealed class FirstInstantTests
{
    private static readonly DateTimeOffset T0 = new(2026, 5, 4, 7, 0, 0, TimeSpan.Zero);
    private static readonly Schedule Hourly = Schedule.Every(TimeSpan.FromHours(1), Interval.AfterCompletion);

    [Fact]
    public void AnAfterCompletionScheduleWithoutAnEndIsDueAtOnce() =>
        Assert.Equal(T0, RecurrenceCalculator.Instance.First(Hourly, T0, null));

    [Fact]
    public void AnAfterCompletionScheduleIsDueTheIntervalAfterTheLastEnd() =>
        Assert.Equal(T0.AddMinutes(40), RecurrenceCalculator.Instance.First(Hourly, T0, T0.AddMinutes(-20)));

    [Fact]
    public void AnAfterCompletionScheduleWhoseIntervalPassedIsDueAtOnce() =>
        Assert.Equal(T0, RecurrenceCalculator.Instance.First(Hourly, T0, T0.AddHours(-2)));

    [Fact]
    public void CronAndFixedRateSchedulesIgnoreTheLastEnd()
    {
        Assert.Equal(T0.AddHours(1), RecurrenceCalculator.Instance.First(Schedule.Cron("0 * * * *", "UTC"), T0, T0.AddMinutes(-1)));
        Assert.Equal(T0.AddHours(1), RecurrenceCalculator.Instance.First(Schedule.Every(TimeSpan.FromHours(1), Interval.FixedRate), T0, T0.AddMinutes(-1)));
    }
}
