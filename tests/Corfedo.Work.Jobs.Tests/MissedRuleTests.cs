namespace Corfedo.Work.Jobs.Tests;

/// <summary>The missed rule and fixed-rate grids, without a store.</summary>
public sealed class MissedRuleTests
{
    private static readonly DateTimeOffset T0 = new(2026, 5, 4, 7, 0, 0, TimeSpan.Zero);
    private static readonly RecurrenceSchedule Hourly = RecurrenceSchedule.Create(Schedule.Cron("0 * * * *", "UTC"), T0);

    [Fact]
    public void NothingIsPlannedBeforeTheNextInstant() =>
        Assert.Equal(new PlannedInstants(null, T0.AddHours(1)), OccurrencePlanner.Plan(Hourly, T0.AddHours(1), T0.AddMinutes(59), Missed.Skip));

    /// <summary>One passed instant is not missed: it is planned and Late decides at its claim.</summary>
    [Theory]
    [InlineData(Missed.Skip)]
    [InlineData(Missed.RunLatest)]
    public void OnePassedInstantIsPlanned(Missed missed) =>
        Assert.Equal(new PlannedInstants(T0.AddHours(1), T0.AddHours(2)), OccurrencePlanner.Plan(Hourly, T0.AddHours(1), T0.AddHours(1).AddMinutes(50), missed));

    /// <summary>J-6 timeline: down from before t1 until between t3 and t4.</summary>
    [Fact]
    public void SeveralMissedInstantsUnderSkipPlanNone() =>
        Assert.Equal(new PlannedInstants(null, T0.AddHours(4)), OccurrencePlanner.Plan(Hourly, T0.AddHours(1), T0.AddHours(3).AddMinutes(10), Missed.Skip));

    [Fact]
    public void SeveralMissedInstantsUnderRunLatestPlanTheLatest() =>
        Assert.Equal(new PlannedInstants(T0.AddHours(3), T0.AddHours(4)), OccurrencePlanner.Plan(Hourly, T0.AddHours(1), T0.AddHours(3).AddMinutes(10), Missed.RunLatest));

    [Fact]
    public void LatestMissedInstantIsFoundAfterLongDowntime()
    {
        var everySecond = RecurrenceSchedule.Create(Schedule.Cron("* * * * * *", "UTC"), T0);
        var now = T0.AddDays(400).AddMilliseconds(500);

        Assert.Equal(new PlannedInstants(T0.AddDays(400), T0.AddDays(400).AddSeconds(1)), OccurrencePlanner.Plan(everySecond, T0.AddSeconds(1), now, Missed.RunLatest));
    }

    [Fact]
    public void FixedRateStaysOnTheGridFromItsAnchor()
    {
        var anchor = T0.AddMinutes(7);
        var grid = RecurrenceSchedule.Create(Schedule.Every(TimeSpan.FromMinutes(10), Interval.FixedRate, anchor), T0);

        Assert.Equal(anchor, grid.Next(T0));
        Assert.Equal(anchor.AddMinutes(10), grid.Next(anchor));
        Assert.Equal(anchor.AddMinutes(30), grid.Next(anchor.AddMinutes(25)));
        Assert.Equal(new PlannedInstants(anchor.AddMinutes(20), anchor.AddMinutes(30)), OccurrencePlanner.Plan(grid, anchor, anchor.AddMinutes(25), Missed.RunLatest));
    }

    /// <summary>After completion has one next instant at a time, so only Late applies to it.</summary>
    [Fact]
    public void AfterCompletionPlansItsOneInstantHoweverLate()
    {
        var afterCompletion = RecurrenceSchedule.Create(Schedule.Every(TimeSpan.FromMinutes(5), Interval.AfterCompletion), T0);

        Assert.True(afterCompletion.AfterCompletion);
        Assert.Equal(new PlannedInstants(T0, null), OccurrencePlanner.Plan(afterCompletion, T0, T0.AddHours(3), Missed.Skip));
    }
}
