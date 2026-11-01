using System.Data.Common;

namespace Corfedo.Work;

/// <summary>A store provider: framework schema, lease, reconciliation, job, calendar and chain operations, expectation state and incidents, the process registry and the signal transport.</summary>
public interface IWorkStore
{
    ILeaseStore Leases { get; }

    IReconcileStore Reconcile { get; }

    IJobStore Jobs { get; }

    ICalendarStore Calendars { get; }

    IChainStore Chains { get; }

    IProcessStore Processes { get; }

    IMonitoringStore Monitoring { get; }

    /// <summary>The signal transport, or null when the store has none and dispatchers rely on timers and polling.</summary>
    ISignalTransport? Signals { get; }

    /// <summary>Applies the framework's schema scripts, serialized across processes.</summary>
    ValueTask MigrateAsync(CancellationToken cancellationToken);
}
