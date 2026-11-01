using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Corfedo.Work;

public static class ProcessServiceCollectionExtensions
{
    /// <summary>
    /// Registers this process in the process registry without running other work: at startup it applies the framework
    /// schema and records itself, heartbeats while it runs and is marked stopped on graceful shutdown.
    /// <see cref="ProcessRegistry"/> lists the instances. A process that adds work with <c>AddWork</c> describes itself
    /// with <c>work.Process(...)</c> instead. The store is registered by its provider, for example with
    /// <c>AddPostgreSqlWorkStore</c>.
    /// </summary>
    public static IServiceCollection AddWorkProcess(this IServiceCollection services, Action<ProcessBuilder>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        AddProcessOptions(services, ProcessBuilder.Build(configure ?? (_ => { })));
        services.AddHostedService<ProcessHost>();
        return services;
    }

    /// <summary>Adds the process options and the services every registered process uses, once per container.</summary>
    internal static void AddProcessOptions(IServiceCollection services, ProcessOptions options)
    {
        if (services.Any(d => d.ServiceType == typeof(ProcessOptions)))
        {
            throw new InvalidOperationException("The process is described more than once; a process that adds work describes itself with work.Process inside AddWork, not with AddWorkProcess.");
        }
        if (!services.Any(d => d.ServiceType == typeof(IWorkStore)))
        {
            throw new InvalidOperationException("Work needs a store; register one with a provider, for example AddPostgreSqlWorkStore, first.");
        }
        services.AddSingleton(options);
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<WorkProcess>();
        services.TryAddSingleton<ProcessRegistry>();
    }
}
