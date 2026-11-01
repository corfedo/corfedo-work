using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Corfedo.Work;

/// <summary>Configures a lease source in this process: its claim hook and expiry handler, and with a handler its capacity or pool.</summary>
public sealed class LeaseSourceBuilder<TKey>
    where TKey : notnull
{
    private readonly IServiceCollection services;

    internal LeaseSourceBuilder(IServiceCollection services) => this.services = services;

    internal Func<IServiceProvider, ILeaseClaimHook<TKey>>? ClaimHook { get; private set; }

    internal Func<IServiceProvider, ILeaseExpiryHandler<TKey>>? ExpiryHandler { get; private set; }

    internal int CapacityValue { get; private set; } = 1;

    internal bool CapacitySet { get; private set; }

    internal string? PoolName { get; private set; }

    internal TimeSpan? TimeoutValue { get; private set; }

    /// <summary>Runs <typeparamref name="THook"/> inside every claim transaction of the source.</summary>
    public LeaseSourceBuilder<TKey> OnClaimed<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THook>()
        where THook : class, ILeaseClaimHook<TKey>
    {
        services.TryAddScoped<THook>();
        ClaimHook = static services => services.GetRequiredService<THook>();
        return this;
    }

    /// <summary>Settles expired leases of the source with <typeparamref name="THandler"/>.</summary>
    public LeaseSourceBuilder<TKey> OnExpired<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>()
        where THandler : class, ILeaseExpiryHandler<TKey>
    {
        services.TryAddScoped<THandler>();
        ExpiryHandler = static services => services.GetRequiredService<THandler>();
        return this;
    }

    /// <summary>Records of the source this process holds at most at once.</summary>
    public LeaseSourceBuilder<TKey> Capacity(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        CapacityValue = capacity;
        CapacitySet = true;
        return this;
    }

    /// <summary>
    /// Runs the source in the dispatch pool <paramref name="name"/>, defined with <c>AddPool</c>, sharing its capacity
    /// with the pool's other sources instead of having a capacity of its own.
    /// </summary>
    public LeaseSourceBuilder<TKey> Pool(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        PoolName = name;
        return this;
    }

    /// <summary>Cancels a run's token with the timeout reason after <paramref name="timeout"/>.</summary>
    public LeaseSourceBuilder<TKey> Timeout(TimeSpan timeout)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);
        TimeoutValue = timeout;
        return this;
    }
}

public static class LeaseServiceCollectionExtensions
{
    /// <summary>Registers a source whose records this process claims and runs with <typeparamref name="THandler"/>.</summary>
    public static IServiceCollection AddLeaseHandler<TKey, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(
        this IServiceCollection services,
        LeaseSource<TKey> source,
        Action<LeaseSourceBuilder<TKey>>? configure = null)
        where TKey : notnull
        where THandler : class, ILeaseHandler<TKey>
    {
        services.TryAddScoped<THandler>();
        return services.AddLeaseRegistration(source, configure, static services => services.GetRequiredService<THandler>());
    }

    /// <summary>
    /// Registers a source without a handler, for processes that claim on behalf of remote holders or by key: claims
    /// run its claim hook, and the process settles its expired leases.
    /// </summary>
    public static IServiceCollection AddLeaseSource<TKey>(this IServiceCollection services, LeaseSource<TKey> source, Action<LeaseSourceBuilder<TKey>>? configure = null)
        where TKey : notnull => services.AddLeaseRegistration(source, configure, null);

    private static IServiceCollection AddLeaseRegistration<TKey>(
        this IServiceCollection services,
        LeaseSource<TKey> source,
        Action<LeaseSourceBuilder<TKey>>? configure,
        Func<IServiceProvider, ILeaseHandler<TKey>>? handler)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        var builder = new LeaseSourceBuilder<TKey>(services);
        configure?.Invoke(builder);
        if (builder.ExpiryHandler is not null && !source.Policy.SettleExpiry)
        {
            throw new InvalidOperationException($"Lease source {source.Name} has an expiry handler but its policy does not set SettleExpiry.");
        }
        if (builder.PoolName is not null && builder.CapacitySet)
        {
            throw new InvalidOperationException($"Lease source {source.Name} runs in pool {builder.PoolName}, which sets its capacity; the source cannot set its own.");
        }
        services.AddSingleton<LeaseSourceRegistration>(new LeaseSourceRegistration<TKey>(
            source, builder.ClaimHook, builder.ExpiryHandler, handler, builder.CapacityValue, builder.TimeoutValue, builder.PoolName));
        services.TryAddSingleton<WorkProcess>();
        services.TryAddSingleton<LeaseRegistry>();
        services.TryAddSingleton<LeaseOperations>();
        return services;
    }
}
