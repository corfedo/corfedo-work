using Microsoft.Extensions.Logging;

namespace Corfedo.Work;

/// <summary>
/// Runs a reconciler as the lease handler of its source and applies its result. A result returned by the reconciler
/// is applied in a guarded transaction of its own; a thrown exception is <c>Failed("handler_failed")</c>; a run that
/// returns <see cref="ReconcileRun{TKey}.Completed"/> without a committed completion is applied as
/// <c>Failed("uncompleted")</c>. Every application is fenced, so it changes nothing once the lease was lost or
/// superseded, and a failed result it applies is logged with its code. During shutdown an exception is left to the
/// dispatcher, which gives the record back as it is.
/// </summary>
internal sealed partial class ReconcilerHandler<TKey, TReconciler>(TReconciler reconciler, ReconcileOperations operations, ILoggerFactory? logging = null, IServiceProvider? services = null) : ILeaseHandler<TKey>
    where TKey : notnull
    where TReconciler : class, IReconciler<TKey>
{
    private static readonly ReconcileResult HandlerFailed = ReconcileResult.Failed("handler_failed");
    private static readonly ReconcileResult Uncompleted = ReconcileResult.Failed("uncompleted");

    private readonly ILogger? logger = logging?.CreateLogger("Corfedo.Work.Reconcile");
    private readonly WorkTelemetryOptions telemetry = services is null ? WorkTelemetryOptions.Default : WorkTelemetryOptions.From(services);

    public async ValueTask RunAsync(LeaseRun<TKey> run, CancellationToken cancellationToken)
    {
        var reconcileRun = new ReconcileRun<TKey>(run, operations);
        var source = run.Lease.Source;
        ReconcileResult? result;
        try
        {
            result = (await reconciler.ReconcileAsync(reconcileRun, cancellationToken)).Result;
        }
        catch (Exception exception) when (run.Hold.StopReason != LeaseStopReason.Shutdown)
        {
            if (run.Hold.StopReason is null && logger is not null)
            {
                LogFailed(logger, telemetry.Logged(exception), source.Name, source.Key.Format(run.Key), telemetry.Describe(exception));
            }
            result = HandlerFailed;
        }
        if (result is null)
        {
            if (await operations.CompleteAsync(run.Lease, Uncompleted, CancellationToken.None) && logger is not null)
            {
                LogUncompleted(logger, source.Name, source.Key.Format(run.Key));
            }
        }
        else if (await operations.CompleteAsync(run.Lease, result, CancellationToken.None)
            && result.Outcome == ReconcileOutcome.Failed && !ReferenceEquals(result, HandlerFailed) && logger is not null)
        {
            LogFailedResult(logger, source.Name, source.Key.Format(run.Key), result.Code!);
        }
        run.TryFinish();
    }

    public ValueTask ExhaustedAsync(LeaseRun<TKey> run, CancellationToken cancellationToken) =>
        throw new InvalidOperationException("Reconciliation sources have no maximum takeovers.");

    [LoggerMessage(EventId = 20, EventName = "reconcile.handler_failed", Level = LogLevel.Error, Message = "Reconciler of {Source} record {Key} failed; applied as failed: {Error}")]
    private static partial void LogFailed(ILogger logger, Exception? exception, string source, string key, string error);

    [LoggerMessage(EventId = 22, EventName = "reconcile.failed", Level = LogLevel.Warning, Message = "Reconciler of {Source} record {Key} failed with {Code}; retried after the failure backoff")]
    private static partial void LogFailedResult(ILogger logger, string source, string key, string code);

    [LoggerMessage(EventId = 21, EventName = "reconcile.uncompleted", Level = LogLevel.Warning, Message = "Reconciler of {Source} record {Key} returned Completed without a committed completion; applied as failed")]
    private static partial void LogUncompleted(ILogger logger, string source, string key);
}
