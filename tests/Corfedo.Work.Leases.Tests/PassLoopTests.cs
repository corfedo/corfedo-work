using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Corfedo.Work.Leases.Tests;

public sealed class PassLoopTests
{
    private static readonly TimeSpan Polling = TimeSpan.FromSeconds(10);

    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task AWakeDuringAPassRunsItAgainAfterItEnds()
    {
        var runs = 0;
        var inFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var loop = Loop(out _);
        var entry = loop.Add("woken", async _ =>
        {
            if (Interlocked.Increment(ref runs) == 1)
            {
                inFirst.SetResult();
                await release.Task;
            }
            return Polling;
        });
        loop.Start();

        await inFirst.Task.WaitAsync(TimeSpan.FromSeconds(5), Cancel);
        entry.Wake();
        release.SetResult();

        await WaitForAsync(() => Volatile.Read(ref runs) == 2);
        await Task.Delay(200, Cancel);
        Assert.Equal(2, Volatile.Read(ref runs));
    }

    [Fact]
    public async Task APassThatIsAlwaysDueDoesNotKeepTheOthersWaiting()
    {
        var quiet = new ConcurrentDictionary<string, int>();
        await using var loop = Loop(out _, workers: 1);
        loop.Add("busy", async _ =>
        {
            await Task.Yield();
            return TimeSpan.Zero;
        });
        var entries = Enumerable.Range(0, 3).Select(i => loop.Add($"quiet-{i}", _ =>
        {
            quiet.AddOrUpdate($"quiet-{i}", 1, (_, count) => count + 1);
            return Task.FromResult(Polling);
        })).ToArray();
        loop.Start();

        await WaitForAsync(() => quiet.Count == 3);
        entries[1].Wake();
        await WaitForAsync(() => quiet.GetValueOrDefault("quiet-1") == 2);
    }

    [Fact]
    public async Task APassThatWaitsDoesNotStopTheOthers()
    {
        var blocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var others = 0;
        await using var loop = Loop(out _);
        loop.Add("waiting", async _ =>
        {
            await blocked.Task;
            return Polling;
        });
        loop.Add("other", _ =>
        {
            Interlocked.Increment(ref others);
            return Task.FromResult(TimeSpan.FromMilliseconds(20));
        });
        loop.Start();

        await WaitForAsync(() => Volatile.Read(ref others) >= 5);
        blocked.SetResult();
    }

    [Fact]
    public async Task APassNeverRunsOnTwoWorkersAtOnce()
    {
        var active = 0;
        var most = 0;
        var runs = 0;
        await using var loop = Loop(out _, workers: 4);
        PassLoop.Entry? entry = null;
        entry = loop.Add("contended", async _ =>
        {
            var now = Interlocked.Increment(ref active);
            InterlockedMax(ref most, now);
            entry!.Wake();
            await Task.Delay(2, Cancel);
            Interlocked.Decrement(ref active);
            Interlocked.Increment(ref runs);
            return TimeSpan.Zero;
        });
        loop.Start();

        await WaitForAsync(() => Volatile.Read(ref runs) >= 50);
        Assert.Equal(1, Volatile.Read(ref most));
    }

    [Fact]
    public async Task AFailingPassIsLoggedAndRunsAgainAfterThePollingIntervalWhileTheOthersRun()
    {
        var failures = 0;
        var others = 0;
        await using var loop = Loop(out var logs, polling: TimeSpan.FromMilliseconds(300));
        loop.Add("failing", _ =>
        {
            Interlocked.Increment(ref failures);
            throw new InvalidOperationException("failed");
        });
        loop.Add("other", _ =>
        {
            Interlocked.Increment(ref others);
            return Task.FromResult(TimeSpan.FromMilliseconds(20));
        });
        loop.Start();

        await WaitForAsync(() => Volatile.Read(ref failures) == 2);
        Assert.True(Volatile.Read(ref others) >= 5, $"other ran {others} times while the failing pass waited");
        Assert.Contains(logs, l => l.Level == LogLevel.Error && l.Message.Contains("failing", StringComparison.Ordinal));
    }

    [Fact]
    public async Task DueTimesFollowTheMonotonicClockAndNotTheWallClock()
    {
        var runs = 0;
        var time = new SteppedWallClock();
        await using var loop = Loop(out _, time: time);
        loop.Add("timed", _ =>
        {
            Interlocked.Increment(ref runs);
            return Task.FromResult(TimeSpan.FromMilliseconds(50));
        });
        loop.Start();

        await WaitForAsync(() => Volatile.Read(ref runs) >= 1);
        time.StepBack(TimeSpan.FromHours(1));
        var before = Volatile.Read(ref runs);
        await WaitForAsync(() => Volatile.Read(ref runs) >= before + 3);
    }

    [Fact]
    public async Task StoppingEndsAPassInFlightWithoutLoggingAFailure()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var loop = Loop(out var logs);
        loop.Add("in-flight", async token =>
        {
            started.SetResult();
            try
            {
                await Task.Delay(Timeout.Infinite, token);
            }
            catch (OperationCanceledException)
            {
                // A pass whose store call fails while the process stops surfaces another exception.
                throw new InvalidOperationException("connection closed");
            }
            return Polling;
        });
        loop.Start();

        await started.Task.WaitAsync(TimeSpan.FromSeconds(5), Cancel);
        await loop.StopAsync().WaitAsync(TimeSpan.FromSeconds(5), Cancel);
        Assert.Empty(logs);
    }

    [Fact]
    public async Task PassesAreAddedBeforeTheLoopStarts()
    {
        await using var loop = Loop(out _);
        loop.Start();

        Assert.Throws<InvalidOperationException>(() => loop.Add("late", _ => Task.FromResult(Polling)));
    }

    private static StoppingLoop Loop(out ConcurrentQueue<(LogLevel Level, string Message)> logs, int workers = PassLoop.DefaultWorkers, TimeSpan? polling = null, TimeProvider? time = null)
    {
        logs = new ConcurrentQueue<(LogLevel, string)>();
        var services = new NoServices();
        var context = new LeaseDispatchContext(services, null!, null!, time ?? TimeProvider.System, new ListLoggerFactory(logs), polling ?? Polling, TimeSpan.FromMilliseconds(10));
        return new StoppingLoop(new PassLoop("test", context, workers));
    }

    private static void InterlockedMax(ref int target, int value)
    {
        var current = Volatile.Read(ref target);
        while (value > current)
        {
            var seen = Interlocked.CompareExchange(ref target, value, current);
            if (seen == current)
            {
                return;
            }
            current = seen;
        }
    }

    private static async Task WaitForAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "condition not reached within 5 s");
            await Task.Delay(10, Cancel);
        }
    }

    /// <summary>Stops the loop when the test ends.</summary>
    private sealed class StoppingLoop(PassLoop loop) : IAsyncDisposable
    {
        public PassLoop.Entry Add(string name, Func<CancellationToken, Task<TimeSpan>> pass) => loop.Add(name, pass);

        public void Start() => loop.Start();

        public Task StopAsync() => loop.StopAsync();

        public async ValueTask DisposeAsync() => await loop.StopAsync();
    }

    /// <summary>The system's timers and monotonic timestamps with a wall clock the test can step back.</summary>
    private sealed class SteppedWallClock : TimeProvider
    {
        private long offsetTicks;

        public override DateTimeOffset GetUtcNow() => System.GetUtcNow() + TimeSpan.FromTicks(Interlocked.Read(ref offsetTicks));

        public void StepBack(TimeSpan by) => Interlocked.Add(ref offsetTicks, -by.Ticks);
    }

    private sealed class NoServices : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }

    private sealed class ListLoggerFactory(ConcurrentQueue<(LogLevel Level, string Message)> logs) : ILoggerFactory, ILogger
    {
        public ILogger CreateLogger(string categoryName) => this;

        public void AddProvider(ILoggerProvider provider)
        {
        }

        public void Dispose()
        {
        }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            logs.Enqueue((logLevel, formatter(state, exception)));
    }
}
