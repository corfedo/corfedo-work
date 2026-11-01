using Microsoft.Extensions.Logging;

namespace Corfedo.Work;

/// <summary>
/// The running-limit timers of one definition's attempts this process holds. An attempt is tracked from the start of its
/// handler, and while the newest stored revision the process knows has a running limit, its timer fires when the
/// attempt reaches it; a newer revision re-arms every tracked attempt with its limit. A firing timer checks the attempt
/// in a statement fenced to the lease, which compares the attempt's stored start with the stored limit, so a raised
/// limit re-arms it with the time left and a removed one or a lost lease ends the checks. A failed check is retried.
/// </summary>
internal sealed partial class RunningLimitTimers
{
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(5);

    private readonly Lock gate = new();
    private readonly HashSet<Attempt> attempts = [];
    private TimeSpan? limit;

    /// <summary>Arms every tracked attempt with the running limit of a newer stored revision, or disarms them.</summary>
    public void Revise(TimeSpan? value)
    {
        lock (gate)
        {
            limit = value;
            foreach (var attempt in attempts)
            {
                attempt.Arm(value);
            }
        }
    }

    /// <summary>Tracks the attempt of <paramref name="run"/> until the returned handle is disposed.</summary>
    public IDisposable Track(LeaseRun<JobId> run, IWorkStore store, TimeProvider time, ILogger? logger, WorkTelemetryOptions telemetry)
    {
        var attempt = new Attempt(this, run.Lease, store, time, logger, telemetry);
        lock (gate)
        {
            attempts.Add(attempt);
            attempt.Arm(limit);
        }
        return attempt;
    }

    private void Remove(Attempt attempt)
    {
        lock (gate)
        {
            attempts.Remove(attempt);
        }
    }

    private sealed class Attempt(RunningLimitTimers owner, Lease<JobId> lease, IWorkStore store, TimeProvider time, ILogger? logger, WorkTelemetryOptions telemetry) : IDisposable
    {
        private readonly long started = time.GetTimestamp();
        private readonly CancellationTokenSource stopping = new();
        private readonly Lock sync = new();
        private ITimer? timer;
        private bool disposed;

        /// <summary>Fires when the attempt reaches <paramref name="value"/>, or never when it is null; called under the owner's lock.</summary>
        public void Arm(TimeSpan? value)
        {
            if (value is not { } after)
            {
                Schedule(null);
                return;
            }
            var due = after - time.GetElapsedTime(started);
            Schedule(due < TimeSpan.Zero ? TimeSpan.Zero : due);
        }

        /// <summary>Fires after <paramref name="due"/>, or never when it is null; the timer is created when first needed.</summary>
        private void Schedule(TimeSpan? due)
        {
            lock (sync)
            {
                if (disposed || (due is null && timer is null))
                {
                    return;
                }
                timer ??= time.CreateTimer(static state => _ = ((Attempt)state!).CheckAsync(), this, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                timer.Change(due ?? Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            }
        }

        private async Task CheckAsync()
        {
            try
            {
                if (await store.Monitoring.CheckAttemptAsync(lease, stopping.Token) is { } remaining)
                {
                    Schedule(remaining);
                }
            }
            catch (Exception) when (stopping.IsCancellationRequested)
            {
                // The attempt ended while its check ran.
            }
            catch (Exception exception)
            {
                if (logger is not null)
                {
                    LogCheckFailed(logger, telemetry.Logged(exception), lease.Source.Job!.Definition, lease.Key.ToString(), telemetry.Describe(exception));
                }
                Schedule(RetryDelay);
            }
        }

        public void Dispose()
        {
            owner.Remove(this);
            lock (sync)
            {
                disposed = true;
                timer?.Dispose();
            }
            stopping.Cancel();
            stopping.Dispose();
        }
    }

    [LoggerMessage(EventId = 46, EventName = "job.running_check_failed", Level = LogLevel.Warning, Message = "Checking the running limit of job {Definition} {Job} failed; retried: {Error}")]
    private static partial void LogCheckFailed(ILogger logger, Exception? exception, string definition, string job, string error);
}
