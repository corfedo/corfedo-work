using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Corfedo.Work;

/// <summary>Registers what this process runs: lease and reconciliation sources, job definitions, their handlers, calendars, dispatch pools and how it appears in the process registry.</summary>
public sealed class WorkBuilder
{
    internal WorkBuilder(IServiceCollection services) => Services = services;

    public IServiceCollection Services { get; }

    internal TimeSpan PollingIntervalValue { get; private set; } = TimeSpan.FromSeconds(15);

    internal TimeSpan ShutdownGraceValue { get; private set; } = TimeSpan.FromSeconds(10);

    internal StartMode StartModeValue { get; private set; } = Work.StartMode.Automatic;

    internal JobRetention RetentionValue { get; private set; } = JobRetention.Default;

    internal CalendarCoverageOptions CalendarCoverageValue { get; private set; } = CalendarCoverageOptions.Default;

    internal Action<ProcessBuilder>? ProcessValue { get; private set; }

    internal WorkTelemetryOptions TelemetryValue { get; private set; } = WorkTelemetryOptions.Default;

    internal List<Action<JsonSerializerOptions>> JsonValue { get; } = [];

    /// <summary>
    /// Changes the JSON options that serialize job arguments and results of definitions without a codec of their own,
    /// such as their naming policy or converters. They start from <see cref="JsonSerializerDefaults.General"/>, with
    /// reflection-based serialization while it is enabled; calls apply in order.
    /// </summary>
    public WorkBuilder ConfigureJson(Action<JsonSerializerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        JsonValue.Add(configure);
        return this;
    }

    /// <summary>
    /// Serializes job arguments and results through <paramref name="resolver"/>, such as a source-generated
    /// <c>JsonSerializerContext</c>, ahead of the resolvers already configured; under Native AOT it is how the host
    /// supplies their types. The host checks every registered definition's types at startup.
    /// </summary>
    public WorkBuilder UseJson(IJsonTypeInfoResolver resolver)
    {
        ArgumentNullException.ThrowIfNull(resolver);
        return ConfigureJson(options => options.TypeInfoResolverChain.Insert(0, resolver));
    }

    /// <summary>
    /// Describes this process in the <see cref="ProcessRegistry">process registry</see>: its type, release, heartbeat
    /// interval and stale window. A process that describes itself is registered; otherwise it is registered when it
    /// runs a handler or declares a code-defined recurrence, and processes that only request work are not.
    /// </summary>
    public WorkBuilder Process(Action<ProcessBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        ProcessValue = configure;
        return this;
    }

    /// <summary>
    /// Extends what the <c>Corfedo.Work</c> activity source, meter and framework logs carry. By default they carry no
    /// partitions, and framework logs describe an exception by its type chain and stack frames without its message.
    /// </summary>
    public WorkBuilder Telemetry(Action<TelemetryBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new TelemetryBuilder();
        configure(builder);
        TelemetryValue = builder.Options;
        return this;
    }

    /// <summary>How often dispatchers look for due records without a signal or a known due time.</summary>
    public WorkBuilder PollingInterval(TimeSpan interval)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero);
        PollingIntervalValue = interval;
        return this;
    }

    /// <summary>
    /// <see cref="Work.StartMode.Manual"/> defers registering the process, dispatching and background loops until the
    /// application calls <see cref="IWorkHost.StartAsync"/>, for hosts that must finish their own initialization first.
    /// </summary>
    public WorkBuilder StartMode(StartMode mode)
    {
        StartModeValue = mode;
        return this;
    }

    /// <summary>How long running handlers get to observe shutdown before their records are released.</summary>
    public WorkBuilder ShutdownGrace(TimeSpan grace)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(grace, TimeSpan.Zero);
        ShutdownGraceValue = grace;
        return this;
    }

    /// <summary>
    /// How long terminal jobs and run history entries are kept for definitions whose policy leaves it unset (J12, D7):
    /// by default 7 and 30 days; and how long resolved incidents are kept, by default 90 days. A definition stores its
    /// durations with its revision, so a changed default applies to a definition from its next stored revision. It sets
    /// durations only; <see cref="AddRetention"/> runs retention.
    /// </summary>
    public WorkBuilder Retention(TimeSpan jobs, TimeSpan runHistory, TimeSpan? incidents = null)
    {
        RetentionValue = new JobRetention(jobs, runHistory, incidents);
        return this;
    }

    /// <summary>
    /// How far ahead of need date sets should be covered (J16): a set whose coverage ends within
    /// <paramref name="runningOutMargin"/> of now or of the instants planning needs is reported, and logged once, as
    /// running out. By default 30 days.
    /// </summary>
    public WorkBuilder CalendarCoverage(TimeSpan runningOutMargin)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(runningOutMargin, TimeSpan.Zero);
        CalendarCoverageValue = new CalendarCoverageOptions(runningOutMargin);
        return this;
    }

    /// <summary>
    /// Runs the framework's <see cref="WorkRetention">retention job</see> in this process: registers its definition with
    /// its handler and its hourly recurrence. At least one worker deployment per store and application adds it; several
    /// replicas may, coordinating through the job's lease. Processes that only request work leave it out. Adding it
    /// twice fails at startup like any definition registered twice.
    /// </summary>
    public WorkBuilder AddRetention()
    {
        Services.AddJob<WorkRetention.RetentionArgs, RetentionHandler>(WorkRetention.Definition);
        Services.AddRecurring(WorkRetention.Definition, new WorkRetention.RetentionArgs(), WorkRetention.Schedule);
        return this;
    }

    /// <inheritdoc cref="MonitoringServiceCollectionExtensions.AddMonitoring"/>
    public WorkBuilder AddMonitoring()
    {
        Services.AddMonitoring();
        return this;
    }

    /// <inheritdoc cref="IncidentServiceCollectionExtensions.OnIncident{THandler}"/>
    public WorkBuilder OnIncident<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>()
        where THandler : class, IWorkIncidentHandler
    {
        Services.OnIncident<THandler>();
        return this;
    }

    /// <summary>Claims and runs records of <paramref name="source"/> with <typeparamref name="THandler"/>.</summary>
    public WorkBuilder AddLeaseHandler<TKey, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(
        LeaseSource<TKey> source,
        Action<LeaseSourceBuilder<TKey>>? configure = null)
        where TKey : notnull
        where THandler : class, ILeaseHandler<TKey>
    {
        Services.AddLeaseHandler<TKey, THandler>(source, configure);
        return this;
    }

    /// <summary>
    /// Registers <paramref name="source"/> without a handler, for claims by key, in the caller's transaction or for
    /// remote holders: claims run its claim hook and this process settles its expired leases.
    /// </summary>
    public WorkBuilder AddLeaseSource<TKey>(LeaseSource<TKey> source, Action<LeaseSourceBuilder<TKey>>? configure = null)
        where TKey : notnull
    {
        Services.AddLeaseSource(source, configure);
        return this;
    }

    /// <summary>Claims and converges records of <paramref name="source"/> with <typeparamref name="TReconciler"/>.</summary>
    public WorkBuilder AddReconciler<TKey, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TReconciler>(
        ReconcileSource<TKey> source,
        Action<LeaseSourceBuilder<TKey>>? configure = null)
        where TKey : notnull
        where TReconciler : class, IReconciler<TKey>
    {
        Services.AddReconciler<TKey, TReconciler>(source, configure);
        return this;
    }

    /// <summary>
    /// Registers <paramref name="source"/> without a reconciler, for claims by key or for remote holders: claims run its
    /// claim hook and this process settles its expired leases. Requests and invalidation need no registration.
    /// </summary>
    public WorkBuilder AddReconcileSource<TKey>(ReconcileSource<TKey> source, Action<LeaseSourceBuilder<TKey>>? configure = null)
        where TKey : notnull
    {
        Services.AddReconcileSource(source, configure);
        return this;
    }

    /// <summary>Runs jobs of <paramref name="definition"/> with <typeparamref name="THandler"/>; the process also requests and cancels them.</summary>
    public WorkBuilder AddJob<TArgs, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(
        JobDefinition<TArgs> definition,
        Action<JobBuilder>? configure = null)
        where THandler : class, IJobHandler<TArgs>
    {
        Services.AddJob<TArgs, THandler>(definition, configure);
        return this;
    }

    /// <summary>Runs jobs of <paramref name="definition"/>, which has a result type, with <typeparamref name="THandler"/>.</summary>
    public WorkBuilder AddJob<TArgs, TResult, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(
        JobDefinition<TArgs, TResult> definition,
        Action<JobBuilder>? configure = null)
        where THandler : class, IJobHandler<TArgs, TResult>
    {
        Services.AddJob<TArgs, TResult, THandler>(definition, configure);
        return this;
    }

    /// <summary>Registers <paramref name="definition"/> for requests and cancellation without running its jobs here.</summary>
    public WorkBuilder AddJobDefinition<TArgs>(JobDefinition<TArgs> definition, Action<JobDefinitionBuilder>? configure = null)
    {
        Services.AddJobDefinition(definition, configure);
        return this;
    }

    /// <inheritdoc cref="JobServiceCollectionExtensions.AddRecurring{TArgs}"/>
    public WorkBuilder AddRecurring<TArgs>(JobDefinition<TArgs> definition, TArgs args, Schedule schedule, RecurrenceOptions? options = null)
    {
        Services.AddRecurring(definition, args, schedule, options);
        return this;
    }

    /// <inheritdoc cref="RecurringChainServiceCollectionExtensions.AddRecurringChain{TArgs}"/>
    public WorkBuilder AddRecurringChain<TArgs>(JobDefinition<TArgs> definition, TArgs args, Schedule schedule, Action<ChainBuilder> chain, RecurrenceOptions? options = null, Action<JobBuilder>? configure = null)
    {
        Services.AddRecurringChain(definition, args, schedule, chain, options, configure);
        return this;
    }

    /// <inheritdoc cref="CalendarServiceCollectionExtensions.AddCalendar"/>
    public WorkBuilder AddCalendar(WorkCalendar calendar)
    {
        Services.AddCalendar(calendar);
        return this;
    }

    /// <summary>
    /// Defines a dispatch pool: sources that join it with <c>Pool(name)</c> share its capacity, and claims rotate
    /// between them so that one source's backlog cannot starve the others.
    /// </summary>
    public WorkBuilder AddPool(string name, int capacity)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        Services.AddSingleton(new DispatchPoolDefinition(name, capacity));
        return this;
    }
}

public static class WorkServiceCollectionExtensions
{
    /// <summary>
    /// Adds durable work to the host; the process runs exactly what it registers, including the framework's
    /// <see cref="WorkRetention">retention job</see> only when it adds it with <see cref="WorkBuilder.AddRetention"/>.
    /// The store is registered by its provider, for example with <c>AddPostgreSqlWorkStore</c>.
    /// </summary>
    public static IServiceCollection AddWork(this IServiceCollection services, Action<WorkBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new WorkBuilder(services);
        configure(builder);
        if (!services.Any(d => d.ServiceType == typeof(IWorkStore)))
        {
            throw new InvalidOperationException("Work needs a store; register one with a provider, for example AddPostgreSqlWorkStore, before AddWork.");
        }
        ProcessServiceCollectionExtensions.AddProcessOptions(services, ProcessBuilder.Build(builder.ProcessValue));
        services.AddSingleton(builder.RetentionValue);
        services.AddSingleton(builder.CalendarCoverageValue);
        services.AddSingleton(builder.TelemetryValue);
        services.AddSingleton(new JobJson(builder.JsonValue));
        services.TryAddSingleton<LeaseRegistry>();
        services.TryAddSingleton<LeaseOperations>();
        services.TryAddSingleton<ReconcileOperations>();
        services.TryAddSingleton<JobRegistry>();
        services.TryAddSingleton<JobOperations>();
        services.TryAddSingleton<CalendarOperations>();
        services.TryAddSingleton<IncidentOperations>();
        services.AddSingleton(new WorkHostOptions(builder.PollingIntervalValue, builder.ShutdownGraceValue, builder.StartModeValue));
        services.AddSingleton<WorkHost>();
        services.AddSingleton<IWorkHost>(static services => services.GetRequiredService<WorkHost>());
        services.AddHostedService(static services => services.GetRequiredService<WorkHost>());
        return services;
    }
}

internal sealed record WorkHostOptions(TimeSpan PollingInterval, TimeSpan ShutdownGrace, StartMode StartMode);

/// <summary>What telemetry carries beyond the default; see <see cref="WorkBuilder.Telemetry"/>.</summary>
public sealed class TelemetryBuilder
{
    internal TelemetryBuilder()
    {
    }

    internal WorkTelemetryOptions Options { get; private set; } = WorkTelemetryOptions.Default;

    /// <summary>Adds <c>corfedo.work.partition</c> to spans and metrics of records with a partition.</summary>
    public TelemetryBuilder Partition()
    {
        Options = Options with { Partition = true };
        return this;
    }

    /// <summary>Passes the exception itself, with its message, to framework logs.</summary>
    public TelemetryBuilder ExceptionDetails()
    {
        Options = Options with { ExceptionDetails = true };
        return this;
    }
}
