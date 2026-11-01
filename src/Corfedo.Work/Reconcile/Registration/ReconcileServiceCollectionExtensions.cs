using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Corfedo.Work;

public static class ReconcileServiceCollectionExtensions
{
    /// <summary>
    /// Registers a reconciliation source whose records this process claims and converges with
    /// <typeparamref name="TReconciler"/>. The builder sets the capacity or pool, timeout, claim hook and expiry handler.
    /// </summary>
    public static IServiceCollection AddReconciler<TKey, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TReconciler>(
        this IServiceCollection services,
        ReconcileSource<TKey> source,
        Action<LeaseSourceBuilder<TKey>>? configure = null)
        where TKey : notnull
        where TReconciler : class, IReconciler<TKey>
    {
        ArgumentNullException.ThrowIfNull(source);
        services.TryAddScoped<TReconciler>();
        services.TryAddSingleton<ReconcileOperations>();
        return services.AddLeaseHandler<TKey, ReconcilerHandler<TKey, TReconciler>>(source.Leases, configure);
    }

    /// <summary>
    /// Registers a reconciliation source without a reconciler, for processes that request, invalidate, or claim by key:
    /// claims run its claim hook, and the process settles its expired leases.
    /// </summary>
    public static IServiceCollection AddReconcileSource<TKey>(this IServiceCollection services, ReconcileSource<TKey> source, Action<LeaseSourceBuilder<TKey>>? configure = null)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        services.TryAddSingleton<ReconcileOperations>();
        return services.AddLeaseSource(source.Leases, configure);
    }
}
