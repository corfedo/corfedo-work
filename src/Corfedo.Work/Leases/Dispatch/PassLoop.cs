using Microsoft.Extensions.Logging;

namespace Corfedo.Work;

/// <summary>
/// One loop that runs the passes of many sources, pools or definitions of one kind on a fixed number of workers. Each
/// pass keeps its own schedule: it is due when it is woken or when the wait it returned from its last pass has passed,
/// a wait of zero makes it due again at once, and <see cref="UntilWoken"/> makes it due only when it is woken. A free worker takes the next due pass after the one taken last, so
/// due passes take turns and a pass that keeps returning zero cannot keep the others waiting for more than one turn of
/// each. A pass never runs on two workers at once, and a wake during a pass runs it again after it ends. The process
/// therefore holds at most <see cref="DefaultWorkers"/> connections per kind for these passes, however many sources it
/// registers, and one pass that waits slows the others of its kind without stopping them. The first due times of the
/// passes are spread over <see cref="StartSpread"/>, and each wait is shortened by a random part of up to a tenth, so
/// idle passes of one kind do not run at the same moment and need a connection each. Due times are measured on the
/// time provider's monotonic timestamp, so changes of the wall clock do not move them. Passes handle their own failures;
/// one that throws anyway is logged and runs again after the polling interval.
/// </summary>
internal sealed partial class PassLoop
{
    /// <summary>The workers of a loop: the connections a kind of pass holds at most, and the passes that may wait at once without stopping the others.</summary>
    public const int DefaultWorkers = 4;

    /// <summary>The wait a pass returns to run again only when it is woken; no pass computes it as a wait.</summary>
    public static readonly TimeSpan UntilWoken = TimeSpan.MaxValue;

    /// <summary>
    /// The window the first due times of a loop's passes are spread over evenly, at most the polling interval. Idle passes
    /// take milliseconds, so passes started this far apart run one after another; work waiting at startup is claimed
    /// within it, and a wake runs a pass at once.
    /// </summary>
    public static readonly TimeSpan StartSpread = TimeSpan.FromSeconds(1);

    /// <summary>
    /// The largest part of a wait a pass may run early, chosen at random each time, so passes that return the same waits,
    /// for example after a wake of them all, do not stay in step. Running early is harmless; a pass never runs late.
    /// </summary>
    private const double Jitter = 0.1;

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

    /// <summary>Adds a pass, due within <see cref="StartSpread"/> of the loop's start; passes are added before the loop starts.</summary>
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
            var now = context.Time.GetTimestamp();
            var spread = Ticks(StartSpread < context.PollingInterval ? StartSpread : context.PollingInterval, context.Time);
            for (var i = 0; i < entries.Count; i++)
            {
                entries[i].Next = now + (spread * i / entries.Count);
            }
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
                entry.Next = delay == UntilWoken ? long.MaxValue
                    : delay <= TimeSpan.Zero ? time.GetTimestamp()
                    : time.GetTimestamp() + Ticks(delay * (1 - (Jitter * Random.Shared.NextDouble())), time);
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
                if (entry.Next == long.MaxValue)
                {
                    continue;
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
        private int woken;

        public string Name { get; } = name;

        public Func<CancellationToken, Task<TimeSpan>> Pass { get; } = pass;

        /// <summary>The timestamp from which the pass is due without a wake; set when the loop starts.</summary>
        public long Next { get; set; }

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
