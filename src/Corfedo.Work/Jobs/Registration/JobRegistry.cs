using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work;

/// <summary>
/// The job definitions and calendars this process registers, with their outcome hooks and accepted argument versions.
/// At startup it stores each calendar by revision, after checking that the newest calendars compose without unknown
/// names or cycles, then each definition by revision, with the host's <see cref="JobRetention"/> for durations its
/// policy leaves unset, and builds its source from the newest stored one, then stores the code-defined recurrences by
/// revision. A transactional operation on a definition the process has not registered is refused, so no process ends a
/// job without its hook. Building the registry resolves the JSON codec of every definition without one of its own from
/// the host's options, so a type they cannot serialize stops the host at startup.
/// </summary>
public sealed class JobRegistry : IWorkInitializer
{
    private readonly Dictionary<string, JobDefinitionRegistration> definitions;
    private readonly DeclaredRecurrence[] recurrences;
    private readonly WorkCalendar[] calendars;
    private readonly IServiceProvider services;
    private readonly JobJson json;

    public JobRegistry(IEnumerable<JobDefinitionRegistration> registrations, IEnumerable<DeclaredRecurrence> recurrences, IEnumerable<DeclaredCalendar> calendars, IServiceProvider services)
    {
        this.services = services;
        this.recurrences = [.. recurrences];
        this.calendars = [.. calendars.Select(c => c.Calendar)];
        if (this.calendars.GroupBy(c => c.Name, StringComparer.Ordinal).FirstOrDefault(g => g.Count() > 1) is { } twice)
        {
            throw new InvalidOperationException($"Calendar {twice.Key} is registered more than once.");
        }
        Telemetry = services.GetService<WorkTelemetryOptions>() ?? WorkTelemetryOptions.Default;
        Time = services.GetService<TimeProvider>() ?? TimeProvider.System;
        var retention = services.GetService<JobRetention>() ?? JobRetention.Default;
        json = services.GetService<JobJson>() ?? JobJson.Default;
        definitions = new Dictionary<string, JobDefinitionRegistration>(StringComparer.Ordinal);
        foreach (var registration in registrations)
        {
            registration.Retention = retention;
            if (!definitions.TryAdd(registration.Definition.Key, registration))
            {
                throw new InvalidOperationException($"Job definition {registration.Definition.Key} is registered more than once.");
            }
            registration.UseJson(json);
        }
        foreach (var recurrence in this.recurrences)
        {
            recurrence.Resolve(this);
        }
    }

    public IEnumerable<JobDefinitionRegistration> Definitions => definitions.Values;

    /// <summary>What the host adds to telemetry.</summary>
    internal WorkTelemetryOptions Telemetry { get; }

    internal TimeProvider Time { get; }

    public async ValueTask InitializeAsync(IWorkStore store, CancellationToken cancellationToken)
    {
        if (calendars.Length > 0)
        {
            // The newest of each calendar, registered here or stored, must compose without unknown names or cycles.
            var newest = (await store.Calendars.GetCalendarsAsync(cancellationToken)).ToDictionary(c => c.Name, StringComparer.Ordinal);
            foreach (var calendar in calendars)
            {
                if (!newest.TryGetValue(calendar.Name, out var stored) || stored.Revision <= calendar.Revision)
                {
                    newest[calendar.Name] = calendar;
                }
            }
            CalendarEvaluator.Validate(newest.Values);
            foreach (var calendar in calendars)
            {
                await store.Calendars.StoreCalendarAsync(calendar, cancellationToken);
            }
        }
        foreach (var registration in definitions.Values)
        {
            await registration.SourceAsync(store, cancellationToken);
        }
        foreach (var recurrence in recurrences)
        {
            var source = await Find(recurrence.Definition).SourceAsync(store, cancellationToken);
            await store.Jobs.DeclareRecurrenceAsync(source, recurrence.Data, recurrence.Definition.Revision, recurrence.Digest, RecurrenceCalculator.Instance, cancellationToken);
        }
    }

    /// <summary>Whether this process declares a code-defined recurrence.</summary>
    public bool DeclaresRecurrences => recurrences.Length > 0;

    /// <summary>The calendars this process registers, as it records them in the process registry.</summary>
    public IReadOnlyList<WorkCalendar> Calendars => calendars;

    /// <summary>
    /// The definitions as this process records them in the process registry: the revision of its own code, not the
    /// newer stored revision its source may apply, the argument versions of those it runs, and its declarations.
    /// </summary>
    public IReadOnlyList<ProcessDefinition> ProcessDefinitions() =>
    [
        .. definitions.Values.Select(r => new ProcessDefinition(
            r.Definition.Key,
            r.Definition.Revision,
            r.Leases is not null,
            r.Leases is not null ? r.AcceptedArgs : [],
            recurrences.Any(d => d.Definition.Key == r.Definition.Key))),
    ];

    /// <summary>The registration of <paramref name="definition"/>.</summary>
    /// <exception cref="InvalidOperationException">The process does not register the definition, or registers a different one under its key.</exception>
    public JobDefinitionRegistration Find(JobDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);
        if (!definitions.TryGetValue(definition.Key, out var registration))
        {
            throw new InvalidOperationException($"Job definition {definition.Key} is not registered in this process; register it with AddJob or AddJobDefinition.");
        }
        if (!ReferenceEquals(registration.Definition, definition))
        {
            throw new InvalidOperationException($"Job definition {definition.Key} is registered with a different definition.");
        }
        return registration;
    }

    /// <summary>The registration of the definition with <paramref name="key"/>, or null.</summary>
    public JobDefinitionRegistration? Find(string key) => definitions.GetValueOrDefault(key);

    /// <summary>The arguments codec of <paramref name="definition"/>, which this process registers.</summary>
    internal IJobArgs<TArgs> Args<TArgs>(JobDefinition<TArgs> definition) => ((JobDefinitionRegistration<TArgs>)Find(definition)).Args;

    /// <summary>The result codec of <paramref name="definition"/>, also when this process does not register it.</summary>
    internal IJobResults<TResult> Results<TArgs, TResult>(JobDefinition<TArgs, TResult> definition) =>
        definitions.TryGetValue(definition.Key, out var registration) && ReferenceEquals(registration.Definition, definition)
            ? (IJobResults<TResult>)registration.Results!
            : json.Results(definition);

    /// <summary>
    /// The definition's outcome callback: counts the outcome, then resolves its hook, if any, in a new scope for every
    /// call. It runs inside the ending transaction, so an end rolled back and applied again is counted again.
    /// </summary>
    public JobEndedCallback Ended(JobDefinitionRegistration registration) => registration.Ended(services, Telemetry);
}

/// <summary>One registered job definition.</summary>
public abstract class JobDefinitionRegistration
{
    private readonly SemaphoreSlim initializing = new(1, 1);
    private LeaseSource<JobId>? source;
    private JobEndedCallback? ended;

    private protected JobDefinitionRegistration(JobDefinition definition, IReadOnlyList<int> acceptedArgs, TimeSpan leaseDuration, Func<IServiceProvider, IJobEnded>? hook)
    {
        Definition = definition;
        AcceptedArgs = acceptedArgs;
        LeaseDuration = leaseDuration;
        Hook = hook;
    }

    public JobDefinition Definition { get; }

    /// <summary>The argument versions this process's code reads.</summary>
    public IReadOnlyList<int> AcceptedArgs { get; }

    public TimeSpan LeaseDuration { get; }

    internal Func<IServiceProvider, IJobEnded>? Hook { get; }

    /// <summary>The result codec, an <c>IJobResults&lt;TResult&gt;</c> of the definition's result type; null without one.</summary>
    internal object? Results { get; private protected set; }

    /// <summary>Resolves the definition's codecs, its own or from the host's JSON options.</summary>
    internal abstract void UseJson(JobJson json);

    /// <summary>The host's retention durations, stored for a revision whose policy leaves them unset.</summary>
    internal JobRetention Retention { get; set; } = JobRetention.Default;

    /// <summary>The lease registration of a definition this process runs; null when it only requests and cancels.</summary>
    internal LeaseSourceRegistration<JobId>? Leases { get; set; }

    /// <summary>The running-limit timers of the attempts this process holds, armed with the newest source's limit.</summary>
    internal RunningLimitTimers RunningLimits { get; } = new();

    /// <summary>
    /// The definition's source, built from the newest stored definition. The first call stores this process's
    /// definition by revision.
    /// </summary>
    public async ValueTask<LeaseSource<JobId>> SourceAsync(IWorkStore store, CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref source) is { } current)
        {
            return current;
        }
        await initializing.WaitAsync(cancellationToken);
        try
        {
            if (source is null)
            {
                Use(Build(store, await store.Jobs.StoreDefinitionAsync(Definition, Retention, cancellationToken)));
            }
            return source!;
        }
        finally
        {
            initializing.Release();
        }
    }

    /// <summary>
    /// Rebuilds the source from the newest stored definition, after a claim or a maintenance pass found a newer revision.
    /// </summary>
    internal async ValueTask RefreshAsync(IWorkStore store, CancellationToken cancellationToken)
    {
        var stored = await store.Jobs.GetDefinitionAsync(Definition.Key, cancellationToken)
            ?? throw new InvalidOperationException($"Job definition {Definition.Key} is no longer stored.");
        Use(Build(store, stored));
    }

    private LeaseSource<JobId> Build(IWorkStore store, StoredJobDefinition stored) =>
        JobSource.Define(store.Jobs.Table, stored, AcceptedArgs, LeaseDuration);

    private void Use(LeaseSource<JobId> built)
    {
        Volatile.Write(ref source, built);
        Leases?.Replace(built);
        RunningLimits.Revise(built.Job!.Policy.Expect?.RunningLimit);
    }

    internal JobEndedCallback Ended(IServiceProvider services, WorkTelemetryOptions telemetry) =>
        ended ??= Hook is { } hook
            ? async (end, transaction, cancellationToken) =>
            {
                Count(end, telemetry);
                await using var scope = services.CreateAsyncScope();
                await hook(scope.ServiceProvider).EndedAsync(end, transaction, cancellationToken);
            }
            : (end, _, _) =>
            {
                Count(end, telemetry);
                return ValueTask.CompletedTask;
            };

    private static void Count(JobEnd end, WorkTelemetryOptions telemetry)
    {
        var tags = new TagList { { WorkTelemetry.DefinitionTag, end.Definition }, { WorkTelemetry.OutcomeTag, WorkTelemetry.Value(end.Outcome) } };
        var skipped = end.Outcome == JobOutcome.Skipped && end.Occurrence is not null;
        var occurrence = new TagList { { WorkTelemetry.DefinitionTag, end.Definition } };
        if (end.Reason is { } reason)
        {
            tags.Add(WorkTelemetry.ReasonTag, reason);
            occurrence.Add(WorkTelemetry.ReasonTag, reason);
        }
        if (telemetry.PartitionOf(end.Partition) is { } partition)
        {
            tags.Add(WorkTelemetry.PartitionTag, partition);
            occurrence.Add(WorkTelemetry.PartitionTag, partition);
        }
        WorkTelemetry.JobOutcomes.Add(1, tags);
        if (skipped)
        {
            WorkTelemetry.OccurrencesSkipped.Add(1, occurrence);
        }
    }
}

/// <summary>A registered job definition with arguments of type <typeparamref name="TArgs"/>.</summary>
public sealed class JobDefinitionRegistration<TArgs> : JobDefinitionRegistration
{
    internal JobDefinitionRegistration(JobDefinition<TArgs> definition, IReadOnlyList<int> acceptedArgs, TimeSpan leaseDuration, Func<IServiceProvider, IJobEnded>? hook)
        : base(definition, acceptedArgs, leaseDuration, hook) => Definition = definition;

    public new JobDefinition<TArgs> Definition { get; }

    /// <summary>The arguments codec, resolved when the registry is built.</summary>
    internal IJobArgs<TArgs> Args { get; private set; } = null!;

    internal override void UseJson(JobJson json)
    {
        Args = json.Args(Definition);
        Results = Definition.ResultsWith(json);
    }
}
