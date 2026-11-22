using Microsoft.Extensions.Logging;

namespace Corfedo.Work;

/// <summary>
/// The planner and lifecycle maintenance of one definition in a process that runs its handler, as a pass of the
/// process's maintenance loop (<see cref="JobMaintenanceLoop"/>), outside any pool's capacity: plans due occurrences of
/// the definition's recurrences and consumes ended ones, plans the calendar instants of jobs and the jobs whose window
/// closed, records jobs the overlap policy or a calendar skipped through the outcome hook, ends jobs whose argument
/// version is below the stored minimum, and repairs copied pause flags. It runs only the calls the loop's probe found
/// work for (<see cref="JobMaintenanceDue"/>), and runs again at once while a call fills its batch. A newer stored
/// revision, read by the probe or the planning call, rebuilds the definition's source, so attempts this process holds
/// take up its running limit even while it claims nothing. It ignores eligibility, so it also reaches jobs no live
/// process may claim.
/// </summary>
internal sealed partial class JobMaintenance(JobDefinitionRegistration registration, JobRegistry registry)
{
    private const int BatchSize = 100;

    private readonly Lock gate = new();
    private JobEndedCallback? ended;
    private JobMaintenanceDue pending;

    // Records changed by passes since the probe was last woken; only passes read and write it, and they never overlap.
    private bool unreported;

    /// <summary>The source name of the definition, the key of its planning signals.</summary>
    public string Source => registration.Definition.SourceName;

    /// <summary>The definition's key, which <see cref="IJobStore.MaintenanceDueAsync"/> results are keyed by.</summary>
    public string Key => registration.Definition.Key;

    /// <summary>The definition's current source, which the probe reads.</summary>
    public ValueTask<LeaseSource<JobId>> SourceAsync(IWorkStore store, CancellationToken cancellationToken) => registration.SourceAsync(store, cancellationToken);

    /// <summary>
    /// Takes the probe's reading: rebuilds the source when the stored revision is newer and adds the calls with work to
    /// those the next pass runs. True when that pass has a call to run.
    /// </summary>
    public async ValueTask<bool> ObserveAsync(JobMaintenanceDue due, IWorkStore store, CancellationToken cancellationToken)
    {
        if (due.Revision is { } revision && revision != (await registration.SourceAsync(store, cancellationToken)).Job!.Revision)
        {
            await registration.RefreshAsync(store, cancellationToken);
        }
        lock (gate)
        {
            pending = Merge(pending, due);
            return pending.Any;
        }
    }

    /// <summary>
    /// One maintenance pass of the calls the probe found work for. Returns zero while a call filled its batch,
    /// <see cref="PassLoop.UntilWoken"/> otherwise; <paramref name="changed"/> runs when a call changed records, so the
    /// probe reads the definition's new state and next instant.
    /// </summary>
    public async Task<TimeSpan> RunAsync(LeaseDispatchContext context, Action changed, CancellationToken token)
    {
        JobMaintenanceDue work;
        lock (gate)
        {
            work = pending;
            pending = default;
        }
        if (!work.Any)
        {
            return PassLoop.UntilWoken;
        }
        var again = default(JobMaintenanceDue);
        TimeSpan? retry = null;
        var touched = false;
        try
        {
            ended ??= registry.Ended(registration);
            var jobs = context.Store.Jobs;
            var source = await registration.SourceAsync(context.Store, token);
            if (work.Planning)
            {
                var planned = await jobs.PlanAsync(source, RecurrenceCalculator.Instance, BatchSize, token);
                if (planned.Revision is { } revision)
                {
                    if (revision != source.Job!.Revision)
                    {
                        await registration.RefreshAsync(context.Store, token);
                    }
                    again = again with { Planning = planned.Recurrences >= BatchSize };
                    touched |= planned.Recurrences > 0;
                }
                else if (planned.NextIn is { } wait)
                {
                    // The definition is being stored: planning runs again after the wait the call returned.
                    again = again with { Planning = true };
                    retry = wait;
                }
            }
            if (work.Calendar)
            {
                var calendar = await jobs.PlanCalendarAsync(source, BatchSize, token);
                again = again with { Calendar = calendar.Instants == BatchSize || calendar.Closed == BatchSize };
                touched |= calendar.Instants + calendar.Closed > 0;
            }
            if (work.Skipped)
            {
                var finalized = await jobs.FinalizeSkippedAsync(source, ended, BatchSize, token);
                again = again with { Skipped = finalized == BatchSize };
                touched |= finalized > 0;
            }
            if (work.Retired)
            {
                var retired = await jobs.RetireAsync(source, ended, BatchSize, token);
                again = again with { Retired = retired == BatchSize };
                touched |= retired > 0;
            }
            if (work.Pauses)
            {
                var repaired = await jobs.RepairPausesAsync(source, BatchSize, token);
                again = again with { Pauses = repaired == BatchSize };
                touched |= repaired > 0;
            }
        }
        catch (Exception exception) when (!token.IsCancellationRequested)
        {
            // The probe finds the work of this pass again; it reads at once what the calls before the failure changed.
            Failed(context, exception);
            Report(touched, PassLoop.UntilWoken, changed);
            return PassLoop.UntilWoken;
        }
        if (again.Any)
        {
            lock (gate)
            {
                pending = Merge(pending, again);
            }
        }
        // A call that filled its batch runs again at once.
        var next = !again.Any ? PassLoop.UntilWoken
            : again with { Planning = again.Planning && retry is null } is { Any: true } ? TimeSpan.Zero
            : retry!.Value;
        Report(touched, next, changed);
        return next;
    }

    /// <summary>Logs a failure of this definition's maintenance or of reading its source for the probe.</summary>
    public void Failed(LeaseDispatchContext context, Exception exception)
    {
        var telemetry = WorkTelemetryOptions.From(context.Services);
        LogMaintenanceFailed(context.Logging.CreateLogger("Corfedo.Work.Jobs"), telemetry.Logged(exception), registration.Definition.Key, telemetry.Describe(exception));
    }

    /// <summary>
    /// Wakes the probe for the records passes changed once the pass does not run again at once, so draining a backlog in
    /// batches probes once, at its end.
    /// </summary>
    private void Report(bool touched, TimeSpan next, Action changed)
    {
        unreported |= touched;
        if (unreported && next != TimeSpan.Zero)
        {
            unreported = false;
            changed();
        }
    }

    private static JobMaintenanceDue Merge(JobMaintenanceDue a, JobMaintenanceDue b) => new(
        b.Revision ?? a.Revision,
        a.Planning || b.Planning,
        a.Calendar || b.Calendar,
        a.Skipped || b.Skipped,
        a.Retired || b.Retired,
        a.Pauses || b.Pauses,
        null);

    [LoggerMessage(EventId = 34, EventName = "job.maintenance_failed", Level = LogLevel.Error, Message = "Maintenance of job definition {Definition} failed: {Error}")]
    private static partial void LogMaintenanceFailed(ILogger logger, Exception? exception, string definition, string error);
}

/// <summary>
/// The process's maintenance loop. One probe pass reads, for every definition whose handler the process runs, which
/// maintenance calls have work (<see cref="IJobStore.MaintenanceDueAsync"/>), in one statement, and wakes the
/// <see cref="JobMaintenance"/> pass of each definition that has some; those passes run only when woken. The probe runs
/// at the polling interval, at the earliest next instant it read, on a definition's planning signal, on a resync and
/// after a definition's pass changed records.
/// </summary>
internal sealed partial class JobMaintenanceLoop(IEnumerable<JobMaintenance> definitions) : IWorkBackground, ISignalSubscriber
{
    /// <summary>The name of the probe pass in the maintenance loop.</summary>
    public const string ProbeName = "job maintenance";

    private readonly JobMaintenance[] definitions = [.. definitions];
    private readonly HashSet<string> sources = new(definitions.Select(d => d.Source), StringComparer.Ordinal);
    private readonly Dictionary<string, PassLoop.Entry> entries = new(StringComparer.Ordinal);
    private PassLoop? loop;
    private PassLoop.Entry? probe;
    private IDisposable? subscription;

    public void Start(LeaseDispatchContext context)
    {
        loop = new PassLoop("maintenance", context);
        if (definitions.Length > 0)
        {
            probe = loop.Add(ProbeName, token => ProbeAsync(context, token));
        }
        foreach (var definition in definitions)
        {
            entries[definition.Key] = loop.Add(definition.Source, token => definition.RunAsync(context, Probe, token));
        }
        subscription = context.Signals?.Subscribe(WorkTopics.Plan, this);
        loop.Start();
    }

    private void Probe() => probe?.Wake();

    private async Task<TimeSpan> ProbeAsync(LeaseDispatchContext context, CancellationToken token)
    {
        var delay = context.PollingInterval;
        // A definition whose source cannot be read or rebuilt is left out of this pass; the others are maintained.
        var probed = new List<(JobMaintenance Definition, LeaseSource<JobId> Source)>(definitions.Length);
        foreach (var definition in definitions)
        {
            try
            {
                probed.Add((definition, await definition.SourceAsync(context.Store, token)));
            }
            catch (Exception exception) when (!token.IsCancellationRequested)
            {
                definition.Failed(context, exception);
            }
        }
        if (probed.Count == 0)
        {
            return delay;
        }
        IReadOnlyDictionary<string, JobMaintenanceDue> due;
        try
        {
            due = await context.Store.Jobs.MaintenanceDueAsync([.. probed.Select(p => p.Source)], token);
        }
        catch (Exception exception) when (!token.IsCancellationRequested)
        {
            var telemetry = WorkTelemetryOptions.From(context.Services);
            LogProbeFailed(context.Logging.CreateLogger("Corfedo.Work.Jobs"), telemetry.Logged(exception), telemetry.Describe(exception));
            return delay;
        }
        foreach (var (definition, _) in probed)
        {
            if (!due.TryGetValue(definition.Key, out var found))
            {
                continue;
            }
            try
            {
                if (await definition.ObserveAsync(found, context.Store, token))
                {
                    entries[definition.Key].Wake();
                }
            }
            catch (Exception exception) when (!token.IsCancellationRequested)
            {
                definition.Failed(context, exception);
            }
            if (found.NextIn is { } next && next < delay)
            {
                delay = next;
            }
        }
        return delay;
    }

    /// <summary>A planning signal of a definition the process runs wakes the probe; others are ignored.</summary>
    public void Received(string key)
    {
        if (sources.Contains(key))
        {
            Probe();
        }
    }

    public void Resync() => Probe();

    public async Task StopAsync()
    {
        subscription?.Dispose();
        if (loop is not null)
        {
            await loop.StopAsync();
        }
    }

    [LoggerMessage(EventId = 49, EventName = "job.maintenance_probe_failed", Level = LogLevel.Error, Message = "The job maintenance probe failed: {Error}")]
    private static partial void LogProbeFailed(ILogger logger, Exception? exception, string error);
}
