using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Corfedo.Work;

/// <summary>
/// Delivers one claimed incident to the process's incident handlers: its opening unless delivered, then its
/// resolution unless delivered, each recorded in its own commit fenced to the lease, then gives the incident back in
/// one locked update that leaves it due if an event still waits, such as a resolution committed meanwhile. A handler
/// that throws leaves the rest undelivered; the dispatcher releases the incident as failed, due again after the
/// source's backoff. A crash lets the lease expire, and another claim delivers again.
/// </summary>
internal sealed class IncidentDeliveryHandler(IEnumerable<IWorkIncidentHandler> handlers, IWorkStore store) : ILeaseHandler<Guid>
{
    public async ValueTask RunAsync(LeaseRun<Guid> run, CancellationToken cancellationToken)
    {
        var monitoring = store.Monitoring;
        if (await monitoring.ReadDeliveryAsync(run.Lease, cancellationToken) is not { } delivery)
        {
            return;
        }
        var incident = delivery.Incident;
        if (!delivery.OpenedDelivered)
        {
            foreach (var handler in handlers)
            {
                await handler.OpenedAsync(incident, cancellationToken);
            }
            if (!await monitoring.RecordDeliveryAsync(run.Lease, resolution: false, cancellationToken))
            {
                return;
            }
        }
        if (incident.ResolvedAt is not null && !delivery.ResolvedDelivered)
        {
            foreach (var handler in handlers)
            {
                await handler.ResolvedAsync(incident, cancellationToken);
            }
            if (!await monitoring.RecordDeliveryAsync(run.Lease, resolution: true, cancellationToken))
            {
                return;
            }
        }
        if (await monitoring.FinishDeliveryAsync(run.Lease, cancellationToken))
        {
            run.TryFinish();
        }
    }

    public ValueTask ExhaustedAsync(LeaseRun<Guid> run, CancellationToken cancellationToken) =>
        throw new InvalidOperationException("The incident source has no maximum takeovers; a delivery is never given up.");
}

/// <summary>Replaces the declared incident source with the store's incident table before sources are verified.</summary>
internal sealed class IncidentSourceInitializer(LeaseSourceRegistration<Guid> registration) : IWorkInitializer
{
    public ValueTask InitializeAsync(IWorkStore store, CancellationToken cancellationToken)
    {
        registration.Replace(IncidentSource.Define(store.Monitoring.Incidents));
        return ValueTask.CompletedTask;
    }
}

/// <summary>Marks a process that delivers incidents.</summary>
internal sealed class IncidentDeliveryRegistration;

public static class IncidentServiceCollectionExtensions
{
    /// <summary>
    /// Delivers incidents to <typeparamref name="THandler"/> in this process (M2): every process that registers a
    /// handler claims from the incident source, one incident at a time; each claim calls every registered handler with
    /// the incident's opening and then its resolution, at least once each.
    /// </summary>
    public static IServiceCollection OnIncident<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(this IServiceCollection services)
        where THandler : class, IWorkIncidentHandler
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddEnumerable(ServiceDescriptor.Scoped<IWorkIncidentHandler, THandler>());
        if (services.Any(d => d.ServiceType == typeof(IncidentDeliveryRegistration)))
        {
            return services;
        }
        // Dispatch starts only after the initializer replaced this source with one on the store's incident table.
        var registration = new LeaseSourceRegistration<Guid>(
            IncidentSource.Define(StoreLocation.Table("work", "work_incidents")),
            claimHook: null,
            expiryHandler: null,
            handler: static services => new IncidentDeliveryHandler(services.GetServices<IWorkIncidentHandler>(), services.GetRequiredService<IWorkStore>()),
            capacity: 1,
            timeout: null);
        services.AddSingleton(new IncidentDeliveryRegistration());
        services.AddSingleton<LeaseSourceRegistration>(registration);
        services.AddSingleton<IWorkInitializer>(new IncidentSourceInitializer(registration));
        return services;
    }
}
