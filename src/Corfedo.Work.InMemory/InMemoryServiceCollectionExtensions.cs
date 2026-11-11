using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work.InMemory;

public static class InMemoryServiceCollectionExtensions
{
    /// <summary>The application a store belongs to unless it names another.</summary>
    public const string DefaultApplication = "work";

    /// <summary>
    /// Stores work in <paramref name="database"/> as <paramref name="application"/>, with the store's signals as the
    /// process's <see cref="IWorkSignals"/>. Processes that register the same
    /// database and application share their work, as processes sharing a PostgreSQL database and framework schema do.
    /// </summary>
    public static IServiceCollection AddInMemoryWorkStore(this IServiceCollection services, InMemoryWorkDatabase database, string application = DefaultApplication)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(application);
        SignalServiceCollectionExtensions.EnsureNoSignals(services);
        services.AddSingleton<IWorkStore>(provider => new InMemoryWorkStore(
            database,
            application,
            provider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance,
            WorkTelemetryOptions.From(provider)));
        services.TryAddSingleton(provider => provider.GetRequiredService<IWorkStore>().Signals!);
        return services.AddWorkSignals();
    }
}
