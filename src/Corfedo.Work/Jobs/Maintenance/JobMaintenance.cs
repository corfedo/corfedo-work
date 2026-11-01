using Microsoft.Extensions.Logging;

namespace Corfedo.Work;

/// <summary>
/// The planner and lifecycle maintenance of one definition in a process that runs its handler, outside any pool's
/// capacity: plans due occurrences of the definition's recurrences and consumes ended ones, plans the calendar instants
/// of jobs and the jobs whose window closed, records jobs the overlap policy or a calendar skipped through the outcome
/// hook, ends jobs whose argument version is below the stored minimum, and repairs copied pause flags. A newer stored
/// revision the planning pass reads rebuilds the definition's source, so attempts this process holds take up its
/// running limit even while it claims nothing. It wakes on the definition's planning signals, at the next planned
/// instant, calendar instant or window end, and at the polling interval. It ignores eligibility, so it also reaches jobs
/// no live process may claim.
/// </summary>
internal sealed partial class JobMaintenance(JobDefinitionRegistration registration, JobRegistry registry) : IWorkBackground, ISignalSubscriber
{
    private const int BatchSize = 100;

    private readonly CancellationTokenSource stopping = new();
    private readonly SemaphoreSlim wake = new(0, 1);
    private Task loop = Task.CompletedTask;
    private IDisposable? subscription;

    public void Start(LeaseDispatchContext context)
    {
        subscription = context.Signals?.Subscribe(WorkTopics.Plan, this);
        loop = Task.Run(() => LoopAsync(context));
    }

    public void Received(string key)
    {
        if (key == registration.Definition.SourceName)
        {
            Wake();
        }
    }

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

    private async Task LoopAsync(LeaseDispatchContext context)
    {
        var token = stopping.Token;
        var logger = context.Logging.CreateLogger("Corfedo.Work.Jobs");
        var telemetry = WorkTelemetryOptions.From(context.Services);
        var ended = registry.Ended(registration);
        var jobs = context.Store.Jobs;
        while (!token.IsCancellationRequested)
        {
            var delay = context.PollingInterval;
            try
            {
                var source = await registration.SourceAsync(context.Store, token);
                var planned = await jobs.PlanAsync(source, RecurrenceCalculator.Instance, BatchSize, token);
                if (planned.Revision is { } revision && revision != source.Job!.Revision)
                {
                    await registration.RefreshAsync(context.Store, token);
                }
                var calendar = await jobs.PlanCalendarAsync(source, BatchSize, token);
                var finalized = await jobs.FinalizeSkippedAsync(source, ended, BatchSize, token);
                var retired = await jobs.RetireAsync(source, ended, BatchSize, token);
                var repaired = await jobs.RepairPausesAsync(source, BatchSize, token);
                if (planned.Recurrences >= BatchSize || calendar.Instants == BatchSize || calendar.Closed == BatchSize
                    || finalized == BatchSize || retired == BatchSize || repaired == BatchSize)
                {
                    continue;
                }
                if (planned.NextIn is { } next && next < delay)
                {
                    delay = next;
                }
                if (calendar.NextIn is { } instant && instant < delay)
                {
                    delay = instant;
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                LogMaintenanceFailed(logger, telemetry.Logged(exception), registration.Definition.Key, telemetry.Describe(exception));
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

    public async Task StopAsync()
    {
        subscription?.Dispose();
        await stopping.CancelAsync();
        await loop.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        stopping.Dispose();
    }

    [LoggerMessage(EventId = 34, EventName = "job.maintenance_failed", Level = LogLevel.Error, Message = "Maintenance of job definition {Definition} failed: {Error}")]
    private static partial void LogMaintenanceFailed(ILogger logger, Exception? exception, string definition, string error);
}
