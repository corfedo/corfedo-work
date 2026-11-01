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
}
