using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work;

/// <summary>Registers a process that runs no other work: applies the framework schema, then keeps the process registered until it stops.</summary>
internal sealed class ProcessHost(IServiceProvider services, IWorkStore store, WorkProcess process, ProcessOptions options, TimeProvider time) : IHostedService
{
    private readonly ProcessHeartbeat heartbeat = new(
        store,
        options.Registration(process.Owner, services),
        options.Heartbeat,
        time,
        ((ILoggerFactory?)services.GetService(typeof(ILoggerFactory)) ?? NullLoggerFactory.Instance).CreateLogger("Corfedo.Work.Processes"),
        WorkTelemetryOptions.From(services));

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await store.MigrateAsync(cancellationToken);
        await heartbeat.StartAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken) => heartbeat.StopAsync(cancellationToken);
}
