using Microsoft.Extensions.Logging;

namespace Corfedo.Work;

/// <summary>
/// The planner and lifecycle maintenance of one definition in a process that runs its handler, as a pass of the
/// process's maintenance loop (<see cref="JobMaintenanceLoop"/>), outside any pool's
/// capacity: plans due occurrences of the definition's recurrences and consumes ended ones, plans the calendar instants
/// of jobs and the jobs whose window closed, records jobs the overlap policy or a calendar skipped through the outcome
/// hook, ends jobs whose argument version is below the stored minimum, and repairs copied pause flags. A newer stored
/// revision the planning pass reads rebuilds the definition's source, so attempts this process holds take up its
/// running limit even while it claims nothing. It wakes on the definition's planning signals, at the next planned
/// instant, calendar instant or window end, and at the polling interval. It ignores eligibility, so it also reaches jobs
/// no live process may claim.
/// </summary>
internal sealed partial class JobMaintenance(JobDefinitionRegistration registration, JobRegistry registry)
{
    private const int BatchSize = 100;
    private JobEndedCallback? ended;

    /// <summary>The source name of the definition, the key of its planning signals.</summary>
    public string Source => registration.Definition.SourceName;

    /// <summary>One maintenance pass; returns how long the definition waits before the next one unless it is woken.</summary>
    public async Task<TimeSpan> RunAsync(LeaseDispatchContext context, CancellationToken token)
    {
        var delay = context.PollingInterval;
        try
        {
            ended ??= registry.Ended(registration);
            var jobs = context.Store.Jobs;
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
                return TimeSpan.Zero;
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
        catch (Exception exception) when (!token.IsCancellationRequested)
        {
            var telemetry = WorkTelemetryOptions.From(context.Services);
            LogMaintenanceFailed(context.Logging.CreateLogger("Corfedo.Work.Jobs"), telemetry.Logged(exception), registration.Definition.Key, telemetry.Describe(exception));
        }
        return delay;
    }

    [LoggerMessage(EventId = 34, EventName = "job.maintenance_failed", Level = LogLevel.Error, Message = "Maintenance of job definition {Definition} failed: {Error}")]
    private static partial void LogMaintenanceFailed(ILogger logger, Exception? exception, string definition, string error);
}

/// <summary>
/// The process's maintenance loop: runs the <see cref="JobMaintenance"/> pass of every definition whose handler the
/// process runs, one at a time, and wakes a definition's pass on its planning signals and every pass on a resync.
/// </summary>
internal sealed class JobMaintenanceLoop(IEnumerable<JobMaintenance> definitions) : IWorkBackground, ISignalSubscriber
{
    private readonly Dictionary<string, PassLoop.Entry> entries = new(StringComparer.Ordinal);
    private PassLoop? loop;
    private IDisposable? subscription;

    public void Start(LeaseDispatchContext context)
    {
        loop = new PassLoop("maintenance", context);
        foreach (var definition in definitions)
        {
            entries[definition.Source] = loop.Add(definition.Source, token => definition.RunAsync(context, token));
        }
        subscription = context.Signals?.Subscribe(WorkTopics.Plan, this);
        loop.Start();
    }

    public void Received(string key)
    {
        if (entries.TryGetValue(key, out var entry))
        {
            entry.Wake();
        }
    }

    public void Resync()
    {
        foreach (var entry in entries.Values)
        {
            entry.Wake();
        }
    }

    public async Task StopAsync()
    {
        subscription?.Dispose();
        if (loop is not null)
        {
            await loop.StopAsync();
        }
    }
}
