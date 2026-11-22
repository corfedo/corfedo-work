using Microsoft.Extensions.Logging;

namespace Corfedo.Work;

/// <summary>
/// The planner and lifecycle maintenance of one definition in a process that runs its handler, as a pass of the
/// process's maintenance loop (<see cref="JobMaintenanceLoop"/>), outside any pool's capacity: plans due occurrences of
/// the definition's recurrences and consumes ended ones, plans the calendar instants of jobs and the jobs whose window
/// closed, records jobs the overlap policy or a calendar skipped through the outcome hook, ends jobs whose argument
/// version is below the stored minimum, and repairs copied pause flags. It runs only the calls the loop's probe found
/// work for (<see cref="JobMaintenanceDue"/>), runs again at once while a call fills its batch, and shortly after
/// planning found the definition being stored. A call that changed nothing, because the rows it found were locked,
/// failed or taken by another process, is held back from the probe's readings until the polling interval passes or the
/// definition's next instant comes, whichever is first; the definition's planning signal and a resync release it. So
/// work that keeps failing is retried at the polling interval, however often the probe runs for other definitions. A
/// newer stored revision, read by the probe or the planning call, rebuilds the definition's source, so attempts this
/// process holds take up its running limit even while it claims nothing. It ignores eligibility, so it also reaches
/// jobs no live process may claim.
/// </summary>
internal sealed partial class JobMaintenance(JobDefinitionRegistration registration, JobRegistry registry)
{
    private const int BatchSize = 100;

    /// <summary>How soon planning runs again after it found the definition being stored.</summary>
    private static readonly TimeSpan BusyRetry = TimeSpan.FromMilliseconds(100);

    private static readonly Call[] Calls = [Call.Planning, Call.Calendar, Call.Skipped, Call.Retired, Call.Pauses];

    private readonly Lock gate = new();

    // Under the gate: the calls the next pass runs, per call the timestamp before which a probe reading does not add it,
    // and the timestamp of the definition's next instant the probe last read.
    private readonly long[] heldUntil = new long[Calls.Length];
    private Call pending;
    private long nextInstant = long.MaxValue;

    private JobEndedCallback? ended;

    // Records changed by passes since the probe was last woken; only passes read and write it, and they never overlap.
    private bool unreported;

    /// <summary>The maintenance calls of a pass.</summary>
    [Flags]
    private enum Call
    {
        None = 0,
        Planning = 1,
        Calendar = 2,
        Skipped = 4,
        Retired = 8,
        Pauses = 16,
    }

    /// <summary>The source name of the definition, the key of its planning signals.</summary>
    public string Source => registration.Definition.SourceName;

    /// <summary>The definition's key, which <see cref="IJobStore.MaintenanceDueAsync"/> results are keyed by.</summary>
    public string Key => registration.Definition.Key;

    /// <summary>The definition's current source, which the probe reads.</summary>
    public ValueTask<LeaseSource<JobId>> SourceAsync(IWorkStore store, CancellationToken cancellationToken) => registration.SourceAsync(store, cancellationToken);

    /// <summary>
    /// Takes the probe's reading: rebuilds the source when the stored revision is newer and adds the calls with work that
    /// are not held back to those the next pass runs. True when that pass has a call to run.
    /// </summary>
    public async ValueTask<bool> ObserveAsync(JobMaintenanceDue due, LeaseDispatchContext context, CancellationToken cancellationToken)
    {
        if (due.Revision is { } revision && revision != (await registration.SourceAsync(context.Store, cancellationToken)).Job!.Revision)
        {
            await registration.RefreshAsync(context.Store, cancellationToken);
        }
        var now = context.Time.GetTimestamp();
        var found = (due.Planning ? Call.Planning : Call.None) | (due.Calendar ? Call.Calendar : Call.None)
            | (due.Skipped ? Call.Skipped : Call.None) | (due.Retired ? Call.Retired : Call.None) | (due.Pauses ? Call.Pauses : Call.None);
        lock (gate)
        {
            nextInstant = due.NextIn is { } next ? now + Ticks(next, context.Time) : long.MaxValue;
            for (var i = 0; i < Calls.Length; i++)
            {
                if ((found & Calls[i]) != 0 && heldUntil[i] <= now)
                {
                    pending |= Calls[i];
                }
            }
            return pending != Call.None;
        }
    }

    /// <summary>Releases the calls held back, on the definition's planning signal or a resync.</summary>
    public void Release()
    {
        lock (gate)
        {
            Array.Clear(heldUntil);
        }
    }

    /// <summary>
    /// One maintenance pass of the calls the probe found work for. Returns zero while a call filled its batch, a short
    /// wait while the definition is being stored, <see cref="PassLoop.UntilWoken"/> otherwise; <paramref name="changed"/>
    /// runs when calls changed records or one failed, so the probe reads the definition's new state and next instant.
    /// </summary>
    public async Task<TimeSpan> RunAsync(LeaseDispatchContext context, Action changed, CancellationToken token)
    {
        Call work;
        lock (gate)
        {
            work = pending;
            pending = Call.None;
        }
        if (work == Call.None)
        {
            return PassLoop.UntilWoken;
        }
        // The calls that ran, those of them that changed nothing, and those that filled their batch.
        var ran = Call.None;
        var quiet = Call.None;
        var again = Call.None;
        var busy = false;
        var touched = false;
        void Done(Call call, int count, bool full)
        {
            touched |= count > 0;
            quiet |= count > 0 ? Call.None : call;
            again |= full ? call : Call.None;
        }
        try
        {
            ended ??= registry.Ended(registration);
            var jobs = context.Store.Jobs;
            var source = await registration.SourceAsync(context.Store, token);
            if ((work & Call.Planning) != 0)
            {
                var planned = await jobs.PlanAsync(source, RecurrenceCalculator.Instance, BatchSize, token);
                if (planned.DefinitionBusy)
                {
                    busy = true;
                }
                else
                {
                    ran |= Call.Planning;
                    if (planned.Revision is { } revision && revision != source.Job!.Revision)
                    {
                        await registration.RefreshAsync(context.Store, token);
                    }
                    Done(Call.Planning, planned.Recurrences, planned.Recurrences >= BatchSize);
                }
            }
            if ((work & Call.Calendar) != 0)
            {
                ran |= Call.Calendar;
                var calendar = await jobs.PlanCalendarAsync(source, BatchSize, token);
                Done(Call.Calendar, calendar.Instants + calendar.Closed, calendar.Instants == BatchSize || calendar.Closed == BatchSize);
            }
            if ((work & Call.Skipped) != 0)
            {
                ran |= Call.Skipped;
                var finalized = await jobs.FinalizeSkippedAsync(source, ended, BatchSize, token);
                Done(Call.Skipped, finalized, finalized == BatchSize);
            }
            if ((work & Call.Retired) != 0)
            {
                ran |= Call.Retired;
                var retired = await jobs.RetireAsync(source, ended, BatchSize, token);
                Done(Call.Retired, retired, retired == BatchSize);
            }
            if ((work & Call.Pauses) != 0)
            {
                ran |= Call.Pauses;
                var repaired = await jobs.RepairPausesAsync(source, BatchSize, token);
                Done(Call.Pauses, repaired, repaired == BatchSize);
            }
        }
        catch (Exception exception) when (!token.IsCancellationRequested)
        {
            // The failed call is held back with those that changed nothing; the probe reads at once what the calls
            // before it changed and finds the calls after it again.
            Failed(context, exception);
            Hold(context, ran, quiet | LastOf(ran));
            Report(true, PassLoop.UntilWoken, changed);
            return PassLoop.UntilWoken;
        }
        Hold(context, ran, quiet);
        if (busy)
        {
            again |= Call.Planning;
        }
        if (again != Call.None)
        {
            lock (gate)
            {
                pending |= again;
            }
        }
        var next = (again & ~(busy ? Call.Planning : Call.None)) != Call.None ? TimeSpan.Zero
            : busy ? BusyRetry
            : PassLoop.UntilWoken;
        Report(touched, next, changed);
        return next;
    }

    /// <summary>Logs a failure of this definition's maintenance or of reading its source for the probe.</summary>
    public void Failed(LeaseDispatchContext context, Exception exception)
    {
        var telemetry = WorkTelemetryOptions.From(context.Services);
        LogMaintenanceFailed(context.Logging.CreateLogger("Corfedo.Work.Jobs"), telemetry.Logged(exception), registration.Definition.Key, telemetry.Describe(exception));
    }

    /// <summary>The call a pass ran last, the one that was running when it failed.</summary>
    private static Call LastOf(Call ran)
    {
        for (var i = Calls.Length - 1; i >= 0; i--)
        {
            if ((ran & Calls[i]) != 0)
            {
                return Calls[i];
            }
        }
        return Call.None;
    }

    /// <summary>
    /// Holds back the calls of <paramref name="ran"/> in <paramref name="quiet"/> until the polling interval passes or the
    /// definition's next instant comes, and releases the others of <paramref name="ran"/>.
    /// </summary>
    private void Hold(LeaseDispatchContext context, Call ran, Call quiet)
    {
        var now = context.Time.GetTimestamp();
        lock (gate)
        {
            var until = Math.Min(now + Ticks(context.PollingInterval, context.Time), nextInstant);
            for (var i = 0; i < Calls.Length; i++)
            {
                if ((ran & Calls[i]) != 0)
                {
                    heldUntil[i] = (quiet & Calls[i]) != 0 ? until : 0;
                }
            }
        }
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

    private static long Ticks(TimeSpan wait, TimeProvider time) => (long)(wait.TotalSeconds * time.TimestampFrequency);

    [LoggerMessage(EventId = 34, EventName = "job.maintenance_failed", Level = LogLevel.Error, Message = "Maintenance of job definition {Definition} failed: {Error}")]
    private static partial void LogMaintenanceFailed(ILogger logger, Exception? exception, string definition, string error);
}

/// <summary>
/// The process's maintenance loop. One probe pass reads, for every definition whose handler the process runs, which
/// maintenance calls have work (<see cref="IJobStore.MaintenanceDueAsync"/>), in one statement, and wakes the
/// <see cref="JobMaintenance"/> pass of each definition that has some; those passes run only when woken. The probe runs
/// at the polling interval (<see cref="PassLoop.Poll"/>), at the earliest next instant it read, on a definition's
/// planning signal, on a resync and after a definition's pass changed records or failed.
/// </summary>
internal sealed partial class JobMaintenanceLoop(IEnumerable<JobMaintenance> definitions) : IWorkBackground, ISignalSubscriber
{
    /// <summary>The name of the probe pass in the maintenance loop.</summary>
    public const string ProbeName = "job maintenance";

    private readonly JobMaintenance[] definitions = [.. definitions];
    private readonly Dictionary<string, JobMaintenance> bySource = definitions.ToDictionary(d => d.Source, StringComparer.Ordinal);
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
        var delay = PassLoop.Poll(context.PollingInterval);
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
                if (await definition.ObserveAsync(found, context, token))
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

    /// <summary>
    /// A planning signal of a definition the process runs releases its held calls and wakes the probe; others are
    /// ignored.
    /// </summary>
    public void Received(string key)
    {
        if (bySource.TryGetValue(key, out var definition))
        {
            definition.Release();
            Probe();
        }
    }

    public void Resync()
    {
        foreach (var definition in definitions)
        {
            definition.Release();
        }
        Probe();
    }

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
