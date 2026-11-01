using Microsoft.Extensions.Logging;

namespace Corfedo.Work;

/// <summary>
/// Keeps one process registered: records it, deletes records that stopped or became stale longer than
/// <see cref="ProcessRegistry.Retention"/> ago, then heartbeats at its interval until stopped, and marks it stopped.
/// Records are only added when a process starts, so deleting expired ones at every start keeps the registry to about
/// the starts of one retention window without a schedule. A failed deletion is logged and left to the next start. A
/// failed heartbeat is logged and retried at the next interval; one that finds the process no longer recorded, deleted
/// by retention while the process could not heartbeat, records it again with the same owner and calls
/// <c>registered</c> again. Nothing here affects leases or running work.
/// </summary>
internal sealed partial class ProcessHeartbeat(
    IWorkStore store,
    ProcessRegistration registration,
    TimeSpan interval,
    TimeProvider time,
    ILogger logger,
    WorkTelemetryOptions telemetry,
    Func<CancellationToken, ValueTask>? registered = null)
{
    private CancellationTokenSource? stopping;
    private Task loop = Task.CompletedTask;

    /// <summary>The records one deletion statement removes at most.</summary>
    private const int RetentionBatch = 1000;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await RegisterAsync(cancellationToken);
        try
        {
            while (await store.Processes.RetainAsync(ProcessRegistry.Retention, RetentionBatch, cancellationToken) == RetentionBatch)
            {
            }
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            LogRetentionFailed(logger, telemetry.Logged(exception), telemetry.Describe(exception));
        }
        stopping = new CancellationTokenSource();
        loop = LoopAsync(stopping.Token);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (stopping is null)
        {
            return;
        }
        await stopping.CancelAsync();
        await loop.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        stopping.Dispose();
        stopping = null;
        try
        {
            await store.Processes.StopAsync(registration.Owner, cancellationToken);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            LogStopFailed(logger, telemetry.Logged(exception), registration.Owner.Value, telemetry.Describe(exception));
        }
    }

    private async Task RegisterAsync(CancellationToken cancellationToken)
    {
        await store.Processes.RegisterAsync(registration, cancellationToken);
        if (registered is not null)
        {
            await registered(cancellationToken);
        }
    }

    private async Task LoopAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            try
            {
                await Task.Delay(interval, time, cancellationToken);
                if (!await store.Processes.HeartbeatAsync(registration.Owner, cancellationToken))
                {
                    await RegisterAsync(cancellationToken);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                LogHeartbeatFailed(logger, telemetry.Logged(exception), registration.Owner.Value, telemetry.Describe(exception));
            }
        }
    }

    [LoggerMessage(EventId = 37, EventName = "process.heartbeat_failed", Level = LogLevel.Warning, Message = "Recording the heartbeat of process {Owner} failed; it is retried at the next interval: {Error}")]
    private static partial void LogHeartbeatFailed(ILogger logger, Exception? exception, Guid owner, string error);

    [LoggerMessage(EventId = 39, EventName = "process.retention_failed", Level = LogLevel.Warning, Message = "Deleting expired process records failed; the next starting process deletes them: {Error}")]
    private static partial void LogRetentionFailed(ILogger logger, Exception? exception, string error);

    [LoggerMessage(EventId = 38, EventName = "process.stop_failed", Level = LogLevel.Warning, Message = "Marking process {Owner} stopped failed; it counts as live until its heartbeat is stale: {Error}")]
    private static partial void LogStopFailed(ILogger logger, Exception? exception, Guid owner, string error);
}
