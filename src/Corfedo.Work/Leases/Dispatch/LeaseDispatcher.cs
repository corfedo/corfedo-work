using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Corfedo.Work;

/// <summary>
/// The runs of one source in a dispatch pool: claims due records when the pool asks, runs each with a
/// <see cref="LeaseHold{TKey}"/> in a new DI scope, and gives back what a handler leaves held. Each claim pass is a
/// <c>corfedo.work.claim</c> span and each run a <c>corfedo.work.run</c> span that starts a trace of its own, linked to
/// the trace that requested a job, with a logging scope naming the run; claims, takeovers, lost leases and
/// supersessions are counted per source, and the records running are reported as a gauge.
/// </summary>
internal sealed partial class LeaseDispatcher<TKey> : IDispatchLane
    where TKey : notnull
{
    private readonly LeaseSourceRegistration<TKey> registration;
    private readonly LeaseCallbacks<TKey> callbacks;
    private readonly LeaseDispatchContext context;
    private readonly Action finished;
    private readonly ILogger logger;
    private readonly CancellationToken stopping;
    private readonly ConcurrentDictionary<Guid, (LeaseRun<TKey> Run, Task Task)> running = new();
    private readonly WorkTelemetryOptions telemetry;
    private readonly IDisposable runningGauge;
    private CancellationToken shutdown;
    private DateTimeOffset? cutoff;

    public LeaseDispatcher(LeaseSourceRegistration<TKey> registration, LeaseCallbacks<TKey> callbacks, LeaseDispatchContext context, Action finished, CancellationToken stopping)
    {
        this.stopping = stopping;
        this.registration = registration;
        this.callbacks = callbacks;
        this.context = context;
        this.finished = finished;
        logger = context.Logging.CreateLogger("Corfedo.Work.Leases");
        telemetry = WorkTelemetryOptions.From(context.Services);
        runningGauge = WorkTelemetry.RunningReadings.Register(() => [new Measurement<long>(running.Count, new KeyValuePair<string, object?>(WorkTelemetry.SourceTag, Source))]);
    }

    public string Source => registration.Name;

    public int Running => running.Count;

    public int Priority => registration.Priority;

    private LeaseSource<TKey> LeaseSource => registration.Source;

    private ILeaseStore Store => context.Store.Leases;

    public void CheckCancellation()
    {
        foreach (var (run, _) in running.Values)
        {
            run.Hold.RenewNow();
        }
    }

    public async Task<int> ClaimAsync(int limit, CancellationToken cancellationToken)
    {
        var claimedAt = context.Time.GetTimestamp();
        // A claim that fails leaves no cutoff, so the pass does not ask for the next due time after it.
        cutoff = null;
        DueClaims<TKey> claims;
        // The span ends before the runs start, so they do not run under it.
        using (var span = WorkTelemetry.Source.StartActivity("corfedo.work.claim"))
        {
            span?.SetTag(WorkTelemetry.SourceTag, Source);
            try
            {
                try
                {
                    claims = await Store.ClaimDueAsync(LeaseSource, callbacks, context.Operations.Owner, limit, cancellationToken);
                }
                catch (JobDefinitionChangedException) when (registration.Refresh is { } refresh)
                {
                    // A newer definition revision was stored: claim again under statements built from it.
                    await refresh(context.Store, cancellationToken);
                    claimedAt = context.Time.GetTimestamp();
                    claims = await Store.ClaimDueAsync(LeaseSource, callbacks, context.Operations.Owner, limit, cancellationToken);
                }
            }
            catch (Exception exception)
            {
                WorkTelemetry.Fail(span, exception.GetType().FullName!, null);
                throw;
            }
        }
        cutoff = claims.Cutoff;
        foreach (var claim in claims.Claims)
        {
            var tags = Tags(claim);
            WorkTelemetry.Claims.Add(1, tags);
            if (claim.Reason == ClaimReason.Takeover)
            {
                WorkTelemetry.Takeovers.Add(1, tags);
            }
            StartRun(claim, claimedAt);
        }
        return claims.Claims.Count;
    }

    /// <summary>The source, and the claim's partition when the host adds it.</summary>
    private TagList Tags(LeaseClaim<TKey> claim)
    {
        var tags = new TagList { { WorkTelemetry.SourceTag, Source } };
        if (telemetry.PartitionOf(claim.Partition) is { } partition)
        {
            tags.Add(WorkTelemetry.PartitionTag, partition);
        }
        return tags;
    }

    public ValueTask<TimeSpan?> NextDueAsync(CancellationToken cancellationToken) =>
        cutoff is { } after ? Store.NextDueAsync(LeaseSource, after, cancellationToken) : ValueTask.FromResult<TimeSpan?>(null);

    public void LogClaimFailed(Exception exception) => LogDispatchFailed(logger, telemetry.Logged(exception), Source, telemetry.Describe(exception));

    private void StartRun(LeaseClaim<TKey> claim, long claimedAt)
    {
        var hold = new LeaseHold<TKey>(Store, context.Time, claim, claimedAt, registration.Timeout);
        var run = new LeaseRun<TKey>(claim, hold, context.Operations);
        // The run is counted before it starts, so a run that finishes at once is also removed.
        var start = new Task<Task>(() => RunAsync(run));
        running[claim.AttemptId] = (run, start.Unwrap());
        start.Start(TaskScheduler.Default);
    }

    private async Task RunAsync(LeaseRun<TKey> run)
    {
        var claim = run.Claim;
        var failed = false;
        // A run starts a trace of its own, linked to the one that requested its job.
        Activity.Current = null;
        using var span = StartSpan(claim);
        run.Span = span;
        using var logScope = logger.BeginScope(Scope(claim));
        // A claim whose validity ran out before the run started does not run its handler; it is still finished, which
        // gives the record back if the lease is valid after all and frees the run's capacity.
        if (!run.Hold.Lost)
        {
            try
            {
                await using var scope = context.Services.CreateAsyncScope();
                var handler = registration.Handler!(scope.ServiceProvider);
                if (claim.Exhausted)
                {
                    await handler.ExhaustedAsync(run, run.CancellationToken);
                }
                else
                {
                    await handler.RunAsync(run, run.CancellationToken);
                }
            }
            catch (Exception exception)
            {
                failed = true;
                WorkTelemetry.Fail(span, exception.GetType().FullName!, null);
                if (run.Hold.StopReason is null)
                {
                    LogHandlerFailed(logger, telemetry.Logged(exception), Source, LeaseSource.Key.Format(claim.Key), telemetry.Describe(exception));
                }
            }
        }
        try
        {
            await FinishAsync(run, failed);
        }
        finally
        {
            if (run.Hold.StopReason == LeaseStopReason.Superseded)
            {
                WorkTelemetry.Supersessions.Add(1, Tags(claim));
            }
            else if (run.Hold.Lost)
            {
                WorkTelemetry.LeasesLost.Add(1, Tags(claim));
            }
            running.TryRemove(claim.AttemptId, out _);
            await run.Hold.DisposeAsync();
            finished();
        }
    }

    private Activity? StartSpan(LeaseClaim<TKey> claim)
    {
        if (!WorkTelemetry.Source.HasListeners())
        {
            return null;
        }
        ActivityLink[]? links = claim.Job?.TraceParent is { } parent && ActivityContext.TryParse(parent, null, out var requested) ? [new ActivityLink(requested)] : null;
        var span = WorkTelemetry.Source.StartActivity("corfedo.work.run", ActivityKind.Internal, default(ActivityContext), links: links);
        if (span is null)
        {
            return null;
        }
        span.SetTag(WorkTelemetry.SourceTag, Source);
        if (LeaseSource.Job is { } job)
        {
            span.SetTag(WorkTelemetry.DefinitionTag, job.Definition);
            span.SetTag(WorkTelemetry.JobTag, claim.Key.ToString());
            span.SetTag(WorkTelemetry.AttemptTag, claim.Job?.Attempt);
        }
        span.SetTag(WorkTelemetry.ExecutionTag, claim.ExecutionId.ToString());
        if (telemetry.PartitionOf(claim.Partition) is { } partition)
        {
            span.SetTag(WorkTelemetry.PartitionTag, partition);
        }
        return span;
    }

    /// <summary>The run's logging scope: its source, and for a job its definition, job and attempt; its execution.</summary>
    private KeyValuePair<string, object?>[] Scope(LeaseClaim<TKey> claim) => LeaseSource.Job is { } job
        ?
        [
            new(WorkTelemetry.SourceTag, Source),
            new(WorkTelemetry.DefinitionTag, job.Definition),
            new(WorkTelemetry.JobTag, claim.Key.ToString()),
            new(WorkTelemetry.ExecutionTag, claim.ExecutionId),
            new(WorkTelemetry.AttemptTag, claim.Job?.Attempt),
        ]
        : [new(WorkTelemetry.SourceTag, Source), new(WorkTelemetry.ExecutionTag, claim.ExecutionId)];

    /// <summary>
    /// Gives back a record the handler did not release: as a failure with the source's backoff, unless the process is
    /// shutting down, in which case it is released as it is. A record the handler ended is released too, which
    /// changes nothing once the end committed and gives the record back if its transaction did not commit. A run whose
    /// hold was lost is released as failed as well: releases are fenced, so this changes nothing when the lease
    /// expired, and gives the record back at once when only the hold's conservative deadline passed, for example
    /// while a renewal reply was late, instead of leaving it to a takeover.
    /// </summary>
    private async Task FinishAsync(LeaseRun<TKey> run, bool failed)
    {
        if (!run.TryFinish())
        {
            return;
        }
        failed |= run.Hold.Lost;
        var claim = run.Claim;
        try
        {
            if (stopping.IsCancellationRequested)
            {
                await Store.ReleaseAsync(claim.Lease, Due.Unchanged, failed: false, shutdown);
                return;
            }
            var backoff = LeaseSource.Policy.FailureBackoff.Delay(claim.Failures);
            if (await Store.ReleaseAsync(claim.Lease, Due.After(backoff), failed: true, CancellationToken.None) && !failed)
            {
                LogUnended(logger, Source, LeaseSource.Key.Format(claim.Key));
            }
        }
        catch (Exception exception)
        {
            LogReleaseFailed(logger, telemetry.Logged(exception), Source, LeaseSource.Key.Format(claim.Key), telemetry.Describe(exception));
        }
    }

    /// <summary>Stops the runs of a pool that no longer claims; see <see cref="ILeaseDispatcher.StopAsync"/>.</summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        shutdown = cancellationToken;
        var runs = running.Values.ToArray();
        foreach (var (run, _) in runs)
        {
            run.Hold.Stop(LeaseStopReason.Shutdown);
        }
        var all = Task.WhenAll(runs.Select(r => r.Task));
        using (var grace = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
        {
            grace.CancelAfter(context.ShutdownGrace);
            await all.WaitAsync(grace.Token).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }
        // Records still held after the grace period go back at once, without a takeover or a failure. Their holds
        // stop renewing whether or not the release succeeds, so a handler that ignores its token cannot keep the
        // lease alive and an unreleased record becomes claimable when its lease expires.
        await Task.WhenAll(running.Values.Select(async entry =>
        {
            entry.Run.Hold.StopRenewing();
            if (!entry.Run.TryFinish())
            {
                return;
            }
            try
            {
                await Store.ReleaseAsync(entry.Run.Lease, Due.Unchanged, failed: false, cancellationToken);
            }
            catch (Exception exception)
            {
                LogReleaseFailed(logger, telemetry.Logged(exception), Source, LeaseSource.Key.Format(entry.Run.Key), telemetry.Describe(exception));
            }
        }));
        runningGauge.Dispose();
    }

    [LoggerMessage(EventId = 10, EventName = "lease.unended", Level = LogLevel.Warning, Message = "Handler of {Source} record {Key} returned without ending or releasing it; released as failed")]
    private static partial void LogUnended(ILogger logger, string source, string key);

    [LoggerMessage(EventId = 11, EventName = "lease.handler_failed", Level = LogLevel.Error, Message = "Handler of {Source} record {Key} failed; released as failed: {Error}")]
    private static partial void LogHandlerFailed(ILogger logger, Exception? exception, string source, string key, string error);

    [LoggerMessage(EventId = 12, EventName = "lease.release_failed", Level = LogLevel.Warning, Message = "Releasing {Source} record {Key} failed; it becomes claimable when its lease expires: {Error}")]
    private static partial void LogReleaseFailed(ILogger logger, Exception? exception, string source, string key, string error);

    [LoggerMessage(EventId = 13, EventName = "lease.dispatch_failed", Level = LogLevel.Error, Message = "Claiming records of {Source} failed: {Error}")]
    private static partial void LogDispatchFailed(ILogger logger, Exception? exception, string source, string error);
}

/// <summary>
/// Settles expired leases of one source at the earliest known expiry and at least every <see cref="MaximumWait"/>. A
/// lease can become settleable without a signal or a known deadline: its claim may commit after the lease expired, or
/// its record may stay locked past expiry. The short wait bounds how long such a lease waits once it is committed,
/// expired and unlocked.
/// </summary>
internal sealed partial class ExpiryProcessor<TKey> : IExpiryProcessor
    where TKey : notnull
{
    private const int BatchSize = 100;
    private static readonly TimeSpan MaximumWait = TimeSpan.FromMilliseconds(750);

    private readonly LeaseSourceRegistration<TKey> registration;
    private readonly LeaseCallbacks<TKey> callbacks;
    private readonly LeaseDispatchContext context;
    private readonly ILogger logger;
    private readonly WakeSignal wake = new();
    private readonly CancellationTokenSource stopping = new();
    private readonly WorkTelemetryOptions telemetry;
    private Task loop = Task.CompletedTask;

    public ExpiryProcessor(LeaseSourceRegistration<TKey> registration, LeaseCallbacks<TKey> callbacks, LeaseDispatchContext context)
    {
        this.registration = registration;
        this.callbacks = callbacks;
        this.context = context;
        logger = context.Logging.CreateLogger("Corfedo.Work.Leases");
        telemetry = WorkTelemetryOptions.From(context.Services);
    }

    public string Source => registration.Name;

    public void Start() => loop = Task.Run(LoopAsync);

    public void Wake() => wake.Set();

    private async Task LoopAsync()
    {
        var token = stopping.Token;
        var store = context.Store.Leases;
        while (!token.IsCancellationRequested)
        {
            TimeSpan delay;
            try
            {
                var settled = await store.SettleExpiredAsync(registration.Source, callbacks, BatchSize, token);
                if (settled == BatchSize)
                {
                    continue;
                }
                var next = await store.NextExpiryAsync(registration.Source, token);
                // A result at or before now is an expired lease left unsettled, for example because its record is locked.
                delay = next is { } due && due > TimeSpan.Zero && due < MaximumWait ? due : MaximumWait;
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                LogSettlementFailed(logger, telemetry.Logged(exception), Source, telemetry.Describe(exception));
                delay = context.PollingInterval;
            }
            try
            {
                await wake.WaitAsync(delay, context.Time, token);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    public async Task StopAsync()
    {
        stopping.Cancel();
        await loop.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        stopping.Dispose();
    }

    [LoggerMessage(EventId = 14, EventName = "lease.settlement_pass_failed", Level = LogLevel.Error, Message = "Settling expired leases of {Source} failed: {Error}")]
    private static partial void LogSettlementFailed(ILogger logger, Exception? exception, string source, string error);
}
