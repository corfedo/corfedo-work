using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work;

public static class SignalServiceCollectionExtensions
{
    /// <summary>
    /// Registers the process's <see cref="IWorkSignals"/> over the registered <see cref="ISignalTransport"/>. Store
    /// providers call it when they register a store with signals.
    /// </summary>
    public static IServiceCollection AddWorkSignals(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddSingleton<IWorkSignals>(provider => new WorkSignals(
            provider.GetRequiredService<ISignalTransport>(),
            provider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance,
            WorkTelemetryOptions.From(provider)));
        return services;
    }

    /// <summary>
    /// Refuses a store after signals registered alone: the store sends its own signals, and a process whose hub listened
    /// to another transport would never hear them.
    /// </summary>
    internal static void EnsureNoSignals(IServiceCollection services)
    {
        if (services.Any(d => d.ServiceType == typeof(ISignalTransport)))
        {
            throw new InvalidOperationException(
                "Signals are already registered. A work store provides the process's signals: remove the AddPostgreSqlSignals or AddSqlServerSignals call.");
        }
    }

    /// <summary>Refuses signals registered alone after a store, which already provides them.</summary>
    internal static void EnsureNoStore(IServiceCollection services, string registration)
    {
        if (services.Any(d => d.ServiceType == typeof(IWorkStore)))
        {
            throw new InvalidOperationException(
                $"A work store is already registered and provides the process's signals: remove the {registration} call.");
        }
    }
}
