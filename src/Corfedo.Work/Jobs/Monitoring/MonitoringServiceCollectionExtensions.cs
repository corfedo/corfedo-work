using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work;

public static class MonitoringServiceCollectionExtensions
{
    /// <summary>
    /// Runs the monitoring loop in this process (M1, M2): it checks the deadlines, running attempts and date-set
    /// coverage of every definition in the store, and the process registry records that the process monitors. At least
    /// one deployment per store adds it; several replicas may. Adding it twice adds one loop.
    /// </summary>
    public static IServiceCollection AddMonitoring(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (services.Any(d => d.ServiceType == typeof(MonitoringRegistration)))
        {
            return services;
        }
        services.AddSingleton<MonitoringRegistration>();
        services.AddJobServices();
        services.AddSingleton<IWorkBackground>(static services => new MonitoringLoop(services.GetRequiredService<CalendarOperations>()));
        return services;
    }
}

/// <summary>Marks a process that runs the monitoring loop.</summary>
internal sealed class MonitoringRegistration;
