using Microsoft.Extensions.Logging;

namespace Corfedo.Work;

/// <summary>
/// One loop that runs the passes of many sources, pools or definitions of one kind, one pass at a time. Each pass keeps
/// its own schedule: it runs when it is woken or when the wait it returned from its last pass has passed, and a wait of
/// zero runs it again in the next round. A round runs every pass that is due once, in registration order, so a pass
/// that keeps returning zero cannot keep the others waiting for more than one round. The process therefore holds at
/// most one database connection per kind for these passes, however many sources it registers. Passes handle their own
/// failures; one that throws anyway is logged and run again after the polling interval.
/// </summary>
internal sealed partial class PassLoop(string kind, LeaseDispatchContext context)
{
    private readonly ILogger logger = context.Logging.CreateLogger("Corfedo.Work.Loops");
    private readonly WorkTelemetryOptions telemetry = WorkTelemetryOptions.From(context.Services);
    private readonly List<Entry> entries = [];
    private readonly WakeSignal wake = new();
    private readonly CancellationTokenSource stopping = new();
    private Task loop = Task.CompletedTask;
    private bool started;

    /// <summary>Adds a pass, due at once when the loop starts; passes are added before the loop starts.</summary>
    public Entry Add(string name, Func<CancellationToken, Task<TimeSpan>> pass)
    {
        if (started)
        {
            throw new InvalidOperationException($"The {kind} loop has started; add its passes before it starts.");
        }
        var entry = new Entry(this, name, pass);
        entries.Add(entry);
        return entry;
    }

    public void Start()
    {
        started = true;
        loop = Task.Run(LoopAsync);
    }

    private async Task LoopAsync()
    {
        var token = stopping.Token;
        var time = context.Time;
        while (!token.IsCancellationRequested)
        {
            foreach (var entry in entries)
            {
                if (!entry.TakeDue(time.GetUtcNow()))
                {
                    continue;
                }
                TimeSpan delay;
                try
                {
                    delay = await entry.Pass(token);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception exception)
                {
                    LogPassFailed(logger, telemetry.Logged(exception), kind, entry.Name, telemetry.Describe(exception));
                    delay = context.PollingInterval;
                }
                entry.Next = time.GetUtcNow() + (delay > TimeSpan.Zero ? delay : TimeSpan.Zero);
            }
            var now = time.GetUtcNow();
            var wait = context.PollingInterval;
            foreach (var entry in entries)
            {
                var until = entry.Woken ? TimeSpan.Zero : entry.Next - now;
                if (until < wait)
                {
                    wait = until;
                }
            }
            if (wait <= TimeSpan.Zero)
            {
                continue;
            }
            try
            {
                await wake.WaitAsync(wait, time, token);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    public async Task StopAsync()
    {
        await stopping.CancelAsync();
        await loop.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        stopping.Dispose();
    }

    /// <summary>A pass's schedule in the loop.</summary>
    internal sealed class Entry(PassLoop owner, string name, Func<CancellationToken, Task<TimeSpan>> pass)
    {
        private int woken = 1;

        public string Name { get; } = name;

        public Func<CancellationToken, Task<TimeSpan>> Pass { get; } = pass;

        public DateTimeOffset Next { get; set; } = DateTimeOffset.MinValue;

        public bool Woken => Volatile.Read(ref woken) == 1;

        /// <summary>Runs the pass in the loop's next round.</summary>
        public void Wake()
        {
            Volatile.Write(ref woken, 1);
            owner.wake.Set();
        }

        /// <summary>Whether the pass is due now; clears its wake, so a wake during the pass runs it again.</summary>
        public bool TakeDue(DateTimeOffset now) => Interlocked.Exchange(ref woken, 0) == 1 || Next <= now;
    }

    [LoggerMessage(EventId = 15, EventName = "loop.pass_failed", Level = LogLevel.Error, Message = "A {Kind} pass of {Name} failed: {Error}")]
    private static partial void LogPassFailed(ILogger logger, Exception? exception, string kind, string name, string error);
}
