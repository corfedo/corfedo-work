using System.Data.Common;
using Corfedo.Work.InMemory.Calendars;
using Corfedo.Work.InMemory.Jobs;
using Corfedo.Work.InMemory.Leases;
using Corfedo.Work.InMemory.Monitoring;
using Corfedo.Work.InMemory.Processes;
using Corfedo.Work.InMemory.Reconcile;
using Corfedo.Work.InMemory.Signals;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work.InMemory;

/// <summary>
/// The in-memory store of one application on an <see cref="InMemoryWorkDatabase"/>: lease operations on its tables,
/// the application's framework state, and in-process signals delivered on commit. For tests only: it keeps nothing
/// across processes or restarts.
/// </summary>
public sealed class InMemoryWorkStore : IWorkStore
{
    /// <summary>The store of <paramref name="application"/>; applications on one database are isolated from each other.</summary>
    public InMemoryWorkStore(InMemoryWorkDatabase database, string application = InMemoryServiceCollectionExtensions.DefaultApplication, ILoggerFactory? loggerFactory = null)
        : this(database, application, loggerFactory ?? NullLoggerFactory.Instance, WorkTelemetryOptions.Default)
    {
    }

    internal InMemoryWorkStore(InMemoryWorkDatabase database, string application, ILoggerFactory loggerFactory, WorkTelemetryOptions telemetry)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(application);
        var state = database.Application(application);
        var logger = loggerFactory.CreateLogger("Corfedo.Work.InMemory");
        var leases = new InMemoryLeaseStore(database, state, logger, telemetry);
        Leases = leases;
        Reconcile = new InMemoryReconcileStore(leases);
        var monitoring = new InMemoryMonitoringStore(database, state, leases);
        Monitoring = monitoring;
        var jobs = new InMemoryJobStore(database, state, leases, monitoring, logger, telemetry);
        Jobs = jobs;
        Chains = jobs;
        Calendars = new InMemoryCalendarStore(database, state, leases);
        Processes = new InMemoryProcessStore(database, state);
        Signals = new InMemorySignalTransport(database, state);
    }

    public ILeaseStore Leases { get; }

    public IReconcileStore Reconcile { get; }

    public IJobStore Jobs { get; }

    public ICalendarStore Calendars { get; }

    public IChainStore Chains { get; }

    public IProcessStore Processes { get; }

    public IMonitoringStore Monitoring { get; }

    public ISignalTransport Signals { get; }

    /// <summary>The in-memory store has no schema to apply.</summary>
    public ValueTask PrepareAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;
}
