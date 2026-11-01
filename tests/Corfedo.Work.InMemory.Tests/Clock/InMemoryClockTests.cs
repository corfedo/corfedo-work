namespace Corfedo.Work.InMemory.Tests.Clock;

public sealed class InMemoryClockTests
{
    private static readonly DateTimeOffset Start = new(2027, 1, 4, 6, 0, 0, TimeSpan.Zero);

    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task TimeMovesOnlyWhenTheClockIsAdvanced()
    {
        var clock = new InMemoryClock(Start);
        var timestamp = clock.GetTimestamp();

        await Task.Delay(50, Cancel);

        Assert.Equal(Start, clock.GetUtcNow());
        Assert.Equal(timestamp, clock.GetTimestamp());
        clock.Advance(TimeSpan.FromMinutes(90));
        Assert.Equal(Start.AddMinutes(90), clock.GetUtcNow());
        Assert.Equal(TimeSpan.FromMinutes(90), clock.GetElapsedTime(timestamp));
        Assert.Throws<ArgumentOutOfRangeException>(() => clock.Advance(TimeSpan.FromTicks(-1)));
        Assert.Equal(Start.AddMinutes(90), clock.GetUtcNow());
    }

    [Fact]
    public void TimersFireInDueOrderWithTheClockAtTheirDueTimeAndPeriodicTimersRepeatUntilDisposed()
    {
        var clock = new InMemoryClock(Start);
        var fired = new List<(string Timer, TimeSpan At)>();
        void Record(object? name) => fired.Add(((string)name!, clock.GetUtcNow() - Start));
        using var late = clock.CreateTimer(Record, "late", TimeSpan.FromMinutes(3), Timeout.InfiniteTimeSpan);
        using var early = clock.CreateTimer(Record, "early", TimeSpan.FromMinutes(1), Timeout.InfiniteTimeSpan);
        var periodic = clock.CreateTimer(Record, "periodic", TimeSpan.FromMinutes(2), TimeSpan.FromMinutes(2));

        clock.Advance(TimeSpan.FromSeconds(59));
        Assert.Empty(fired);
        clock.Advance(TimeSpan.FromMinutes(4) + TimeSpan.FromSeconds(1));
        Assert.Equal(
            [("early", TimeSpan.FromMinutes(1)), ("periodic", TimeSpan.FromMinutes(2)), ("late", TimeSpan.FromMinutes(3)), ("periodic", TimeSpan.FromMinutes(4))],
            fired);

        periodic.Dispose();
        clock.Advance(TimeSpan.FromMinutes(10));
        Assert.Equal(4, fired.Count);
    }

    [Fact]
    public async Task ADelayOnTheClockCompletesOnlyWhenTheClockReachesItsEnd()
    {
        var clock = new InMemoryClock(Start);
        var delay = Task.Delay(TimeSpan.FromMinutes(1), clock, Cancel);

        await Task.Delay(50, Cancel);
        Assert.False(delay.IsCompleted);
        clock.Advance(TimeSpan.FromSeconds(59));
        await Task.Delay(50, Cancel);
        Assert.False(delay.IsCompleted);

        clock.Advance(TimeSpan.FromSeconds(1));
        await delay.WaitAsync(TimeSpan.FromSeconds(5), Cancel);
    }
}
