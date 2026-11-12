using Microsoft.Extensions.Logging;

namespace Corfedo.Work;

/// <summary>
/// One loop that runs the passes of many sources, pools or definitions of one kind on a fixed number of workers. Each
/// pass keeps its own schedule: it is due when it is woken or when the wait it returned from its last pass has passed,
/// and a wait of zero makes it due again at once. A free worker takes the next due pass after the one taken last, so
/// due passes take turns and a pass that keeps returning zero cannot keep the others waiting for more than one turn of
/// each. A pass never runs on two workers at once, and a wake during a pass runs it again after it ends. The process
/// therefore holds at most <see cref="DefaultWorkers"/> connections per kind for these passes, however many sources it
/// registers, and one pass that waits slows the others of its kind without stopping them. Due times are measured on the
/// time provider's monotonic timestamp, so changes of the wall clock do not move them. Passes handle their own failures;
/// one that throws anyway is logged and runs again after the polling interval.
/// </summary>
internal sealed partial class PassLoop
{
    /// <summary>The workers of a loop: the connections a kind of pass holds at most, and the passes that may wait at once without stopping the others.</summary>
    public const int DefaultWorkers = 4;

    private readonly string kind;
    private readonly LeaseDispatchContext context;
    private readonly int workerCount;
    private readonly ILogger logger;
    private readonly WorkTelemetryOptions telemetry;
    private readonly List<Entry> entries = [];
    private readonly Lock gate = new();
    private readonly WakeSignal wake = new();
    private readonly CancellationTokenSource stopping = new();
    private Task[] workers = [];
    private int cursor;
    private bool started;

    public PassLoop(string kind, LeaseDispatchContext context, int workers = DefaultWorkers)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(workers);
        this.kind = kind;
        this.context = context;
        workerCount = workers;
        logger = context.Logging.CreateLogger("Corfedo.Work.Loops");
        telemetry = WorkTelemetryOptions.From(context.Services);
    }

    /// <summary>Adds a pass, due at once when the loop starts; passes are added before the loop starts.</summary>
    public Entry Add(string name, Func<CancellationToken, Task<TimeSpan>> pass)
    {
        lock (gate)
        {
            if (started)
            {
                throw new InvalidOperationException($"The {kind} loop has started; add its passes before it starts.");
            }
            var entry = new Entry(this, name, pass);
            entries.Add(entry);
            return entry;
        }
    }

    public void Start()
    {
        lock (gate)
        {
            started = true;
        }
        workers = [.. Enumerable.Range(0, workerCount).Select(_ => Task.Run(WorkAsync))];
    }

    private async Task WorkAsync()
    {
        var token = stopping.Token;
        var time = context.Time;
        while (!token.IsCancellationRequested)
        {
            var (entry, wait) = Take(time.GetTimestamp());
            if (entry is null)
            {
                try
                {
                    await wake.WaitAsync(wait, time, token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                continue;
            }
            TimeSpan delay;
            try
            {
                delay = await entry.Pass(token);
            }
            catch (Exception) when (token.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                LogPassFailed(logger, telemetry.Logged(exception), kind, entry.Name, telemetry.Describe(exception));
                delay = context.PollingInterval;
            }
            lock (gate)
            {
                entry.Next = time.GetTimestamp() + Ticks(delay > TimeSpan.Zero ? delay : TimeSpan.Zero, time);
                entry.Running = false;
            }
            // The pass may be due again, or woken while it ran; another worker may be waiting for it.
            wake.Set();
        }
    }

    /// <summary>The next due pass after the one taken last, marked running; or none and how long to wait for one.</summary>
    private (Entry? Entry, TimeSpan Wait) Take(long now)
    {
        lock (gate)
        {
            var wait = context.PollingInterval;
            for (var i = 0; i < entries.Count; i++)
            {
                var index = (cursor + i) % entries.Count;
                var entry = entries[index];
                if (entry.Running)
                {
                    continue;
                }
                if (entry.TakeWake() || entry.Next <= now)
                {
                    entry.Running = true;
                    cursor = (index + 1) % entries.Count;
                    return (entry, TimeSpan.Zero);
                }
                var until = context.Time.GetElapsedTime(now, entry.Next);
                if (until < wait)
                {
                    wait = until;
                }
            }
            return (null, wait);
        }
    }

    private static long Ticks(TimeSpan delay, TimeProvider time) => (long)Math.Ceiling(delay.TotalSeconds * time.TimestampFrequency);

    public async Task StopAsync()
    {
        await stopping.CancelAsync();
        await Task.WhenAll(workers).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        stopping.Dispose();
    }

    /// <summary>A pass's schedule in the loop; its fields other than the wake belong to the loop's lock.</summary>
    internal sealed class Entry(PassLoop owner, string name, Func<CancellationToken, Task<TimeSpan>> pass)
    {
        private int woken = 1;

        public string Name { get; } = name;

        public Func<CancellationToken, Task<TimeSpan>> Pass { get; } = pass;

        /// <summary>The timestamp from which the pass is due without a wake.</summary>
        public long Next { get; set; } = long.MinValue;

        public bool Running { get; set; }

        /// <summary>Makes the pass due: at once when it is idle, and again after its pass when it is running.</summary>
        public void Wake()
        {
            Volatile.Write(ref woken, 1);
            owner.wake.Set();
        }

        /// <summary>Clears the wake; true when the pass was woken.</summary>
        public bool TakeWake() => Interlocked.Exchange(ref woken, 0) == 1;
    }

    [LoggerMessage(EventId = 15, EventName = "loop.pass_failed", Level = LogLevel.Error, Message = "A {Kind} pass of {Name} failed: {Error}")]
    private static partial void LogPassFailed(ILogger logger, Exception? exception, string kind, string name, string error);
}
