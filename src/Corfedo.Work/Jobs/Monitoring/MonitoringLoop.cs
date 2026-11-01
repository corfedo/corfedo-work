using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;

namespace Corfedo.Work;

/// <summary>
/// The monitoring loop of a process that adds monitoring: checks the stored expectations of every definition, also of
/// definitions no live process runs. Per definition it applies the queued changes of expected keys (its unpaused
/// recurrences, or the definition as a whole without recurrences, none while it is paused), reconciles every key while
/// the definition's revision or pause differs from the ones it was last reconciled with, moves deadlines a date set
/// held, processes passed deadlines and checks recurring-chain occurrences against the running limit, since their
/// chains hold their attempts without a lease holder to time them; then it opens and resolves the date-set incidents
/// from the coverage report. A reconciliation resolves the incidents of removed expectations, clears or starts windows
/// and moves deadlines an older revision computed, and is recorded only once nothing is left, so rows it skipped are
/// taken up by a later pass of any process. It wakes at the next deadline, when the next occurrence reaches its limit,
/// on a signal that expected keys changed, so a removed or paused key's incident resolves and a new key is seen at once,
/// and at the polling interval. Every decision is made over rows locked without waiting, so several processes may
/// monitor. While a listener enables them, each pass also reads the store-wide gauges, open incidents per kind and the
/// age of each definition's oldest due job, which the gauge callbacks report.
/// </summary>
internal sealed partial class MonitoringLoop(CalendarOperations calendars) : IWorkBackground, ISignalSubscriber
{
    private const int BatchSize = 1000;

    private readonly CancellationTokenSource stopping = new();
    private readonly SemaphoreSlim wake = new(0, 1);
    private Task loop = Task.CompletedTask;
    private WorkTelemetryOptions telemetry = WorkTelemetryOptions.Default;
    private MonitoringGauges? gauges;
    private IDisposable? openIncidents;
    private IDisposable? dueAge;
    private IDisposable? subscription;

    public void Start(LeaseDispatchContext context)
    {
        telemetry = WorkTelemetryOptions.From(context.Services);
        openIncidents = WorkTelemetry.OpenIncidentReadings.Register(ReadOpenIncidents);
        dueAge = WorkTelemetry.DueAgeReadings.Register(ReadDueAge);
        subscription = context.Signals?.Subscribe(WorkTopics.Monitor, this);
        loop = Task.Run(() => LoopAsync(context));
    }

    public void Received(string key) => Wake();

    public void Resync() => Wake();

    private void Wake()
    {
        try
        {
            if (wake.CurrentCount == 0)
            {
                wake.Release();
            }
        }
        catch (SemaphoreFullException)
        {
        }
    }

    private IEnumerable<Measurement<long>> ReadOpenIncidents() => Volatile.Read(ref gauges) is { } read
        ? Enum.GetValues<WorkIncidentKind>().Select(kind => new Measurement<long>(read.OpenIncidents.GetValueOrDefault(kind), new KeyValuePair<string, object?>(WorkTelemetry.KindTag, WorkTelemetry.Value(kind))))
        : [];

    private IEnumerable<Measurement<double>> ReadDueAge() => Volatile.Read(ref gauges) is { } read
        ? read.OldestDue.Select(due => new Measurement<double>(due.Value.TotalSeconds, new KeyValuePair<string, object?>(WorkTelemetry.DefinitionTag, due.Key)))
        : [];

    private async Task LoopAsync(LeaseDispatchContext context)
    {
        var token = stopping.Token;
        var logger = context.Logging.CreateLogger("Corfedo.Work.Monitoring");
        while (!token.IsCancellationRequested)
        {
            var delay = context.PollingInterval;
            try
            {
                var next = await PassAsync(context.Store, logger, token);
                if (next is { } wake && wake < delay)
                {
                    delay = wake < TimeSpan.Zero ? TimeSpan.Zero : wake;
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                LogPassFailed(logger, telemetry.Logged(exception), telemetry.Describe(exception));
            }
            try
            {
                using var waiting = CancellationTokenSource.CreateLinkedTokenSource(token);
                var signalled = wake.WaitAsync(waiting.Token);
                await Task.WhenAny(signalled, Task.Delay(delay, context.Time, waiting.Token));
                await waiting.CancelAsync();
                await signalled.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                token.ThrowIfCancellationRequested();
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    /// <summary>One pass; returns how long until the earliest deadline or running limit it knows of.</summary>
    private async Task<TimeSpan?> PassAsync(IWorkStore store, ILogger logger, CancellationToken cancellationToken)
    {
        var monitoring = store.Monitoring;
        var stored = await monitoring.GetExpectationsAsync(cancellationToken);
        CalendarEvaluator? evaluator = null;
        if (stored.Definitions.Any(d => d.Expectation?.Deadline?.Calendar is not null))
        {
            evaluator = new CalendarEvaluator(await store.Calendars.GetCalendarsAsync(cancellationToken), await store.Calendars.GetDateSetsAsync(cancellationToken));
        }
        DateTimeOffset? wake = null;
        void WakeAt(DateTimeOffset? at)
        {
            if (at is { } instant && (wake is null || instant < wake))
            {
                wake = instant;
            }
        }
        foreach (var definition in stored.Definitions)
        {
            try
            {
                var (deadline, running) = await CheckAsync(monitoring, definition, evaluator, stored.Now, cancellationToken);
                WakeAt(deadline);
                WakeAt(running);
            }
            catch (Exception exception) when (exception is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            {
                LogDefinitionFailed(logger, telemetry.Logged(exception), definition.Definition, telemetry.Describe(exception));
            }
        }
        if (WorkTelemetry.OpenIncidents.Enabled || WorkTelemetry.DueAge.Enabled)
        {
            Volatile.Write(ref gauges, await monitoring.GetGaugesAsync(cancellationToken));
        }
        var coverage = await calendars.GetDateSetCoverageAsync(cancellationToken);
        await monitoring.SyncDateSetsAsync(
            [.. coverage.Where(c => c.State is DateSetCoverageState.RunningOut or DateSetCoverageState.WorkWaiting).Select(c => c.Name)],
            [.. coverage.Where(c => c.State == DateSetCoverageState.WorkWaiting).Select(c => c.Name)],
            cancellationToken);
        return wake - stored.Now;
    }

    private static async Task<(DateTimeOffset? Deadline, DateTimeOffset? Running)> CheckAsync(IMonitoringStore monitoring, StoredExpectation definition, CalendarEvaluator? evaluator, DateTimeOffset now, CancellationToken cancellationToken)
    {
        var key = definition.Definition;
        var expectation = definition.Expectation;
        var calculator = expectation?.Deadline is { } completion ? new DeadlineCalculator(completion, evaluator) : null;
        var first = calculator is not null && !definition.Paused ? calculator.First(now) : null;
        if (first is not null)
        {
            await monitoring.TrackWholeAsync(key, definition.Revision, !definition.HasRecurrences, first, cancellationToken);
        }
        if (definition.HasChanges)
        {
            while (await monitoring.ApplyChangesAsync(key, definition.Revision, first, BatchSize, cancellationToken) == BatchSize)
            {
            }
        }
        if (!definition.Reconciled)
        {
            await ReconcileAsync(monitoring, definition, calculator, first, cancellationToken);
        }
        DateTimeOffset? deadline = null;
        if (calculator is not null && first is not null)
        {
            await monitoring.RescheduleAsync(key, definition.Revision, calculator.Next, olderRevisions: false, cancellationToken);
            DeadlinePass pass;
            do
            {
                pass = await monitoring.ProcessDeadlinesAsync(key, calculator.Process, BatchSize, cancellationToken);
            }
            while (pass.Processed == BatchSize);
            deadline = pass.Next;
        }
        DateTimeOffset? running = null;
        if (expectation?.RunningLimit is { } limit)
        {
            running = (await monitoring.CheckRunningAsync(key, limit, cancellationToken)).NextAt;
        }
        return (deadline, running);
    }

    /// <summary>
    /// Brings every key of the definition in line with its stored revision and pause; <paramref name="first"/> is the
    /// window a key starts with, null while the definition expects no completions.
    /// </summary>
    private static async Task ReconcileAsync(IMonitoringStore monitoring, StoredExpectation definition, DeadlineCalculator? calculator, ExpectationWindow? first, CancellationToken cancellationToken)
    {
        var key = definition.Definition;
        var removed = ExpectationKinds.Unexpected(definition.Expectation);
        if (removed.Length > 0)
        {
            await monitoring.ResolveRemovedAsync(key, removed, definition.Expectation?.FailureLimit is null, cancellationToken);
        }
        while (await monitoring.ClearUnexpectedAsync(key, tracking: first is not null, BatchSize, cancellationToken) == BatchSize)
        {
        }
        if (calculator is not null && first is not null)
        {
            while (await monitoring.InsertExpectedAsync(key, definition.Revision, first, BatchSize, cancellationToken) >= BatchSize)
            {
            }
            await monitoring.RescheduleAsync(key, definition.Revision, calculator.Next, olderRevisions: true, cancellationToken);
        }
        await monitoring.MarkReconciledAsync(definition, cancellationToken);
    }

    public async Task StopAsync()
    {
        subscription?.Dispose();
        await stopping.CancelAsync();
        await loop.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        openIncidents?.Dispose();
        dueAge?.Dispose();
        stopping.Dispose();
    }

    [LoggerMessage(EventId = 44, EventName = "monitoring.pass_failed", Level = LogLevel.Error, Message = "A monitoring pass failed: {Error}")]
    private static partial void LogPassFailed(ILogger logger, Exception? exception, string error);

    [LoggerMessage(EventId = 45, EventName = "monitoring.definition_failed", Level = LogLevel.Error, Message = "Checking the expectations of job definition {Definition} failed; it is checked again in the next pass: {Error}")]
    private static partial void LogDefinitionFailed(ILogger logger, Exception? exception, string definition, string error);
}
