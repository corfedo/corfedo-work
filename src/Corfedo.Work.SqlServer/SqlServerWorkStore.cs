using Corfedo.Work.SqlServer.Calendars;
using Corfedo.Work.SqlServer.Jobs;
using Corfedo.Work.SqlServer.Leases;
using Corfedo.Work.SqlServer.Monitoring;
using Corfedo.Work.SqlServer.Processes;
using Corfedo.Work.SqlServer.Reconcile;
using Corfedo.Work.SqlServer.Schema;
using Corfedo.Work.SqlServer.Signals;
using Microsoft.Extensions.Logging;

namespace Corfedo.Work.SqlServer;

/// <summary>
/// The SQL Server store: framework tables in their own schema, lease operations on application tables, job records,
/// calendars, expectation state and incidents, the process registry and the signals table. The database must read
/// committed snapshots (<c>READ_COMMITTED_SNAPSHOT ON</c>).
/// </summary>
public sealed class SqlServerWorkStore : IWorkStore
{
    private readonly string connectionString;
    private readonly string schema;
    private readonly WorkSchemaMode schemaMode;

    /// <summary>
    /// A store with its framework tables in <paramref name="schema"/>; <paramref name="schemaMode"/> says whether a
    /// starting process applies the schema or only verifies it.
    /// </summary>
    public SqlServerWorkStore(string connectionString, string schema, ILoggerFactory loggerFactory, TimeProvider time, WorkSchemaMode schemaMode = WorkSchemaMode.Migrate)
        : this(connectionString, schema, new SqlServerSignalOptions(), loggerFactory, time, WorkTelemetryOptions.Default, schemaMode)
    {
    }

    /// <summary>
    /// A store with its framework tables in <paramref name="schema"/> and its signals polled as
    /// <paramref name="signalOptions"/> say; <paramref name="schemaMode"/> says whether a starting process applies the
    /// schema or only verifies it.
    /// </summary>
    public SqlServerWorkStore(string connectionString, string schema, SqlServerSignalOptions signalOptions, ILoggerFactory loggerFactory, TimeProvider time, WorkSchemaMode schemaMode = WorkSchemaMode.Migrate)
        : this(connectionString, schema, signalOptions, loggerFactory, time, WorkTelemetryOptions.Default, schemaMode)
    {
    }

    internal SqlServerWorkStore(string connectionString, string schema, SqlServerSignalOptions signalOptions, ILoggerFactory loggerFactory, TimeProvider time, WorkTelemetryOptions telemetry, WorkSchemaMode schemaMode)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);
        ArgumentNullException.ThrowIfNull(signalOptions);
        signalOptions.Validate();
        this.connectionString = connectionString;
        this.schema = schema;
        this.schemaMode = schemaMode;
        var signals = new SqlServerSignalSql(schema);
        var logger = loggerFactory.CreateLogger("Corfedo.Work.SqlServer");
        var leases = new SqlServerLeaseStore(connectionString, schema, signals, logger, telemetry, alterTables: schemaMode == WorkSchemaMode.Migrate);
        Leases = leases;
        Reconcile = new SqlServerReconcileStore(leases, signals);
        var calendars = new SqlServerCalendarStore(connectionString, leases, schema);
        Calendars = calendars;
        var monitoring = new SqlServerMonitoringStore(leases, schema);
        Monitoring = monitoring;
        var jobs = new SqlServerJobStore(connectionString, leases, calendars, monitoring, schema, signals, logger, telemetry);
        Jobs = jobs;
        Chains = jobs;
        Processes = new SqlServerProcessStore(connectionString, schema);
        Signals = new SqlServerSignalTransport(connectionString, signals, signalOptions, time, logger, telemetry, ensureTable: schemaMode == WorkSchemaMode.Migrate);
    }

    public ILeaseStore Leases { get; }

    public IReconcileStore Reconcile { get; }

    public IJobStore Jobs { get; }

    public ICalendarStore Calendars { get; }

    public IChainStore Chains { get; }

    public IProcessStore Processes { get; }

    public IMonitoringStore Monitoring { get; }

    public ISignalTransport Signals { get; }

    public ValueTask PrepareAsync(CancellationToken cancellationToken) => new(schemaMode == WorkSchemaMode.Verify
        ? FrameworkSchema.VerifyAsync(connectionString, schema, cancellationToken)
        : FrameworkSchema.MigrateAsync(connectionString, schema, [], cancellationToken));
}
