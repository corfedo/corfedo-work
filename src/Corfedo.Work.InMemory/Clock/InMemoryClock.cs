namespace Corfedo.Work.InMemory;

/// <summary>
/// A clock that moves only when the test advances it. Timers, delays and timestamps follow it, so deadlines, timers,
/// backoffs, leases and recurrences fire on <see cref="Advance"/>. Give the same instance to the
/// <see cref="InMemoryWorkDatabase"/> and to the host's services as their <see cref="TimeProvider"/>.
/// </summary>
public sealed class InMemoryClock : TimeProvider
{
    private readonly Lock sync = new();
    private readonly List<Timer> timers = [];
    private readonly DateTimeOffset start;
    private DateTimeOffset now;
    private long sequence;

    /// <summary>A clock standing at <paramref name="start"/>.</summary>
    public InMemoryClock(DateTimeOffset start)
    {
        this.start = start.ToUniversalTime();
        now = this.start;
    }

    public override DateTimeOffset GetUtcNow()
    {
        lock (sync)
        {
            return now;
        }
    }

    public override TimeZoneInfo LocalTimeZone => TimeZoneInfo.Utc;

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public override long GetTimestamp() => (GetUtcNow() - start).Ticks;

    /// <summary>
    /// Moves the clock forward by <paramref name="by"/>. Timers due on the way fire in due order, each with the clock
    /// at its due time; work their callbacks start runs on its own and may observe a later time.
    /// </summary>
    public void Advance(TimeSpan by)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(by, TimeSpan.Zero);
        DateTimeOffset target;
        lock (sync)
        {
            target = now + by;
        }
        while (true)
        {
            Timer? next;
            lock (sync)
            {
                next = timers.Where(t => t.DueAt <= target).OrderBy(t => t.DueAt).ThenBy(t => t.Sequence).FirstOrDefault();
                // Concurrent advances never move the clock back.
                if (next is null)
                {
                    now = target > now ? target : now;
                    return;
                }
                now = next.DueAt > now ? next.DueAt : now;
                if (next.Period is { } period)
                {
                    next.DueAt = now + period;
                }
                else
                {
                    timers.Remove(next);
                }
            }
            next.Fire();
        }
    }

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        ArgumentNullException.ThrowIfNull(callback);
        var timer = new Timer(this, callback, state);
        timer.Change(dueTime, period);
        return timer;
    }

    private sealed class Timer(InMemoryClock clock, TimerCallback callback, object? state) : ITimer
    {
        public DateTimeOffset DueAt { get; set; }

        public TimeSpan? Period { get; private set; }

        public long Sequence { get; private set; }

        public void Fire() => callback(state);

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            lock (clock.sync)
            {
                clock.timers.Remove(this);
                if (dueTime == Timeout.InfiniteTimeSpan)
                {
                    return true;
                }
                ArgumentOutOfRangeException.ThrowIfLessThan(dueTime, TimeSpan.Zero);
                DueAt = clock.now + dueTime;
                Period = period == Timeout.InfiniteTimeSpan || period == TimeSpan.Zero ? null : period;
                Sequence = ++clock.sequence;
                clock.timers.Add(this);
            }
            if (dueTime == TimeSpan.Zero)
            {
                // A timer due now fires without waiting for the clock to move, after its creator has it.
                ThreadPool.QueueUserWorkItem(static c => c.Advance(TimeSpan.Zero), clock, preferLocal: false);
            }
            return true;
        }

        public void Dispose()
        {
            lock (clock.sync)
            {
                clock.timers.Remove(this);
            }
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
