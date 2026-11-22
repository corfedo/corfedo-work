using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work;

/// <summary>
/// The store connections a process's Work host uses, computed from its registrations, for sizing the store's data
/// source. <see cref="Listener"/>, <see cref="Passes"/> and <see cref="Housekeeping"/> count the background loops'
/// connections; <see cref="Runs"/> is an upper bound for brief statements of running work, each a single round trip. A
/// pool smaller than the sum queues those statements; if it stays exhausted past a lease's last confirmed expiry, the
/// lease is lost and its run is released as failed. Resolving it reads only registrations, never the store, so a data
/// source can be sized from it. Handlers and the application's own statements are not counted.
/// </summary>
/// <param name="Listener">The signal listener's session-level connection: one when the process has signals. A provider that takes the listener from a data source of its own, such as PostgreSQL with a listener data source, does not use the store's for it.</param>
/// <param name="Passes">The background passes that run at once: dispatch, expiry settlement and job maintenance, each up to its loop's four workers and no more than it has passes.</param>
/// <param name="Housekeeping">The process heartbeat, signals sent after commit, and the monitoring and calendar coverage loops, each one connection at a time.</param>
/// <param name="Runs">Work running at once, the sum of the dispatch pools' capacities; each run's renewals, release and end take a connection for one round trip each.</param>
public sealed record WorkConnections(int Listener, int Passes, int Housekeeping, int Runs)
{
    internal static WorkConnections From(IServiceProvider services)
    {
        var registry = services.GetRequiredService<LeaseRegistry>();
        var jobs = services.GetRequiredService<JobRegistry>();
        var process = services.GetRequiredService<ProcessOptions>();
        var pools = services.GetServices<DispatchPoolDefinition>().ToDictionary(p => p.Name, p => p.Capacity, StringComparer.Ordinal);
        var isService = services.GetService<IServiceProviderIsService>();
        var handled = registry.Sources.Where(r => r.HasHandler).ToArray();
        var pooled = handled.Select(r => r.Pool).OfType<string>().Distinct(StringComparer.Ordinal).ToArray();
        var standalone = handled.Where(r => r.Pool is null).ToArray();
        var settling = registry.Sources.Where(r => r.Settles).ToArray();
        static bool IsJob(LeaseSourceRegistration r) => r is LeaseSourceRegistration<JobId> { Source.Job: not null };
        var definitions = handled.Count(IsJob);
        var expiry = settling.Count(r => !IsJob(r)) + (settling.Any(IsJob) ? 1 : 0);
        var monitors = isService?.IsService(typeof(MonitoringRegistration)) == true;
        var calendars = jobs.Calendars.Count > 0;
        var signals = isService?.IsService(typeof(IWorkSignals)) == true;
        var heartbeat = process.Configured || handled.Length > 0 || jobs.DeclaresRecurrences || calendars || monitors;
        return new(
            signals ? 1 : 0,
            Loop(standalone.Length + pooled.Length) + Loop(expiry) + Loop(definitions == 0 ? 0 : definitions + 1),
            (heartbeat ? 1 : 0) + (signals ? 1 : 0) + (monitors ? 1 : 0) + (calendars ? 1 : 0),
            standalone.Sum(r => r.Capacity) + pooled.Sum(p => pools.GetValueOrDefault(p)));
    }

    private static int Loop(int passes) => Math.Min(PassLoop.DefaultWorkers, passes);
}
