using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Corfedo.Work;

/// <summary>Configures a job definition in this process: its outcome hook.</summary>
public class JobDefinitionBuilder
{
    private protected JobDefinitionBuilder(IServiceCollection services) => Services = services;

    private protected IServiceCollection Services { get; }

    internal Func<IServiceProvider, IJobEnded>? Hook { get; private set; }

    /// <summary>Runs <typeparamref name="THook"/> inside every terminal transition of the definition's jobs (D4).</summary>
    public JobDefinitionBuilder OnEnded<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THook>()
        where THook : class, IJobEnded
    {
        Services.TryAddScoped<THook>();
        Hook = static services => services.GetRequiredService<THook>();
        return this;
    }

    internal static JobDefinitionBuilder Create(IServiceCollection services) => new(services);
}

/// <summary>Configures a job definition this process runs: hook, capacity or pool, lease duration and accepted argument versions.</summary>
public sealed class JobBuilder : JobDefinitionBuilder
{
    internal JobBuilder(IServiceCollection services)
        : base(services)
    {
    }

    internal int CapacityValue { get; private set; } = 1;

    internal bool CapacitySet { get; private set; }

    internal string? PoolName { get; private set; }

    internal TimeSpan LeaseValue { get; private set; } = TimeSpan.FromMinutes(1);

    internal int[]? AcceptedArgs { get; private set; }

    /// <summary>Jobs of the definition this process runs at most at once.</summary>
    public JobBuilder Capacity(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        CapacityValue = capacity;
        CapacitySet = true;
        return this;
    }

    /// <summary>Runs the definition in the dispatch pool <paramref name="name"/>, sharing its capacity by priority class.</summary>
    public JobBuilder Pool(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        PoolName = name;
        return this;
    }

    /// <summary>How long a claim or renewal keeps an attempt's lease valid; renewed every third of it.</summary>
    public JobBuilder Lease(TimeSpan duration)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(duration, TimeSpan.Zero);
        LeaseValue = duration;
        return this;
    }

    /// <summary>The argument versions this process's codec reads; by default only the version it writes (R21).</summary>
    public JobBuilder AcceptsArgs(params int[] versions)
    {
        ArgumentNullException.ThrowIfNull(versions);
        if (versions.Length == 0 || versions.Any(v => v < 1))
        {
            throw new ArgumentException("At least one argument version, each at least 1.", nameof(versions));
        }
        AcceptedArgs = versions;
        return this;
    }

    /// <inheritdoc cref="JobDefinitionBuilder.OnEnded{THook}"/>
    public new JobBuilder OnEnded<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THook>()
        where THook : class, IJobEnded
    {
        base.OnEnded<THook>();
        return this;
    }
}

public static class JobServiceCollectionExtensions
{
    /// <summary>
    /// Registers a job definition this process runs with <typeparamref name="THandler"/>. The process also requests and
    /// cancels its jobs, and stores the definition by revision at startup.
    /// </summary>
    public static IServiceCollection AddJob<TArgs, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(
        this IServiceCollection services,
        JobDefinition<TArgs> definition,
        Action<JobBuilder>? configure = null)
        where THandler : class, IJobHandler<TArgs>
    {
        services.TryAddScoped<THandler>();
        services.TryAddScoped(static services => new JobHandler<TArgs, THandler>(
            services.GetRequiredService<THandler>(),
            services.GetRequiredService<JobRegistry>(),
            services.GetRequiredService<IWorkStore>(),
            services.GetService<ILoggerFactory>()));
        return services.AddJob(definition, configure, static services => services.GetRequiredService<JobHandler<TArgs, THandler>>());
    }

    /// <summary>
    /// Registers a job definition with a result type that this process runs with <typeparamref name="THandler"/>; the
    /// handler's returned value is stored with the outcome (J20).
    /// </summary>
    public static IServiceCollection AddJob<TArgs, TResult, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(
        this IServiceCollection services,
        JobDefinition<TArgs, TResult> definition,
        Action<JobBuilder>? configure = null)
        where THandler : class, IJobHandler<TArgs, TResult>
    {
        services.TryAddScoped<THandler>();
        services.TryAddScoped(static services => new JobHandler<TArgs, TResult, THandler>(
            services.GetRequiredService<THandler>(),
            services.GetRequiredService<JobRegistry>(),
            services.GetRequiredService<IWorkStore>(),
            services.GetService<ILoggerFactory>()));
        return services.AddJob(definition, configure, static services => services.GetRequiredService<JobHandler<TArgs, TResult, THandler>>());
    }

    internal static IServiceCollection AddJob<TArgs>(this IServiceCollection services, JobDefinition<TArgs> definition, Action<JobBuilder>? configure, Func<IServiceProvider, ILeaseHandler<JobId>> handler)
    {
        ArgumentNullException.ThrowIfNull(definition);
        var builder = new JobBuilder(services);
        configure?.Invoke(builder);
        if (builder.PoolName is not null && builder.CapacitySet)
        {
            throw new InvalidOperationException($"Job definition {definition.Key} runs in pool {builder.PoolName}, which sets its capacity; the definition cannot set its own.");
        }
        var registration = new JobDefinitionRegistration<TArgs>(definition, builder.AcceptedArgs ?? [definition.Args.Version], builder.LeaseValue, builder.Hook);
        // Dispatch starts only after the host stored the definition and replaced this source with the stored one; until
        // then it only names the definition's source.
        var declared = JobSource.Define(
            StoreLocation.Table("work", "jobs"),
            new StoredJobDefinition(definition.Key, definition.Revision, definition.Subject, definition.Policy, definition.Digest, Paused: false),
            registration.AcceptedArgs,
            builder.LeaseValue);
        var leases = new LeaseSourceRegistration<JobId>(
            declared,
            claimHook: null,
            expiryHandler: null,
            handler: handler,
            builder.CapacityValue,
            timeout: null,
            builder.PoolName)
        {
            Refresh = registration.RefreshAsync,
        };
        registration.Leases = leases;
        services.AddSingleton<LeaseSourceRegistration>(leases);
        services.AddSingleton<IWorkBackground>(services => new JobMaintenance(registration, services.GetRequiredService<JobRegistry>()));
        return services.AddJobRegistration(registration);
    }

    /// <summary>
    /// Registers a job definition this process requests and cancels but does not run; it stores the definition by
    /// revision at startup and runs its outcome hook in the transitions it causes.
    /// </summary>
    public static IServiceCollection AddJobDefinition<TArgs>(this IServiceCollection services, JobDefinition<TArgs> definition, Action<JobDefinitionBuilder>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(definition);
        var builder = JobDefinitionBuilder.Create(services);
        configure?.Invoke(builder);
        return services.AddJobRegistration(new JobDefinitionRegistration<TArgs>(definition, [definition.Args.Version], TimeSpan.FromMinutes(1), builder.Hook));
    }

    /// <summary>
    /// Declares the code-defined recurrence of <paramref name="definition"/>, which must be registered with
    /// <c>AddJob</c> or <c>AddJobDefinition</c> and have no subject kind. It is part of the definition's revision: at
    /// startup it is stored unless a newer revision's recurrence is (J10, R18).
    /// </summary>
    public static IServiceCollection AddRecurring<TArgs>(this IServiceCollection services, JobDefinition<TArgs> definition, TArgs args, Schedule schedule, RecurrenceOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(definition);
        options ??= RecurrenceOptions.Default;
        if (definition.Subject is not null || options.Subject is not null || options.MergeKey is not null)
        {
            throw new ArgumentException($"Job definition {definition.Key} has a subject kind or the options name an identity; per-subject recurrences are set with ScheduleAsync.", nameof(definition));
        }
        var data = JobOperations.Recurrence(definition, args, schedule, JobRequestData.DefinitionIdentity, null, options);
        services.AddSingleton(new DeclaredRecurrence(definition, data));
        return services;
    }

    private static IServiceCollection AddJobRegistration(this IServiceCollection services, JobDefinitionRegistration registration)
    {
        services.AddSingleton(registration);
        return services.AddJobServices();
    }

    /// <summary>The job registry, which stores calendars and definitions at startup, and the operations on them.</summary>
    internal static IServiceCollection AddJobServices(this IServiceCollection services)
    {
        services.TryAddSingleton<JobRegistry>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IWorkInitializer, JobRegistry>(static services => services.GetRequiredService<JobRegistry>()));
        services.TryAddSingleton<JobOperations>();
        services.TryAddSingleton<CalendarOperations>();
        services.TryAddSingleton<IncidentOperations>();
        return services;
    }
}
