using System.Data.Common;
using Corfedo.Work.PostgreSql.Calendars;
using Corfedo.Work.PostgreSql.Jobs;
using Corfedo.Work.PostgreSql.Leases;
using Corfedo.Work.PostgreSql.Monitoring;
using Corfedo.Work.PostgreSql.Processes;
using Corfedo.Work.PostgreSql.Reconcile;
using Corfedo.Work.PostgreSql.Schema;
using Corfedo.Work.PostgreSql.Signals;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Corfedo.Work.PostgreSql;

/// <summary>The PostgreSQL store: framework tables in their own schema, lease operations on application tables, job records, calendars, expectation state and incidents, and the process registry.</summary>
public sealed class PostgreSqlWorkStore : IWorkStore
{
    private readonly NpgsqlDataSource dataSource;
    private readonly string schema;

    public PostgreSqlWorkStore(NpgsqlDataSource dataSource, string schema, ILoggerFactory loggerFactory, TimeProvider time)
        : this(dataSource, schema, loggerFactory, time, WorkTelemetryOptions.Default)
    {
    }

    internal PostgreSqlWorkStore(NpgsqlDataSource dataSource, string schema, ILoggerFactory loggerFactory, TimeProvider time, WorkTelemetryOptions telemetry)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);
        this.dataSource = dataSource;
        this.schema = schema;
        var signals = PostgreSqlSignalSql.For(schema);
        var logger = loggerFactory.CreateLogger("Corfedo.Work.PostgreSql");
        var leases = new PostgreSqlLeaseStore(dataSource, schema, signals, logger, telemetry);
        Leases = leases;
        Reconcile = new PostgreSqlReconcileStore(dataSource, leases, signals);
        var calendars = new PostgreSqlCalendarStore(dataSource, leases, schema);
        Calendars = calendars;
        var monitoring = new PostgreSqlMonitoringStore(dataSource, leases, schema);
        Monitoring = monitoring;
        var jobs = new PostgreSqlJobStore(dataSource, leases, calendars, monitoring, schema, signals, logger, telemetry);
        Jobs = jobs;
        Chains = jobs;
        Processes = new PostgreSqlProcessStore(dataSource, schema);
        Signals = new PostgreSqlSignalTransport(dataSource, signals, time, logger, telemetry);
    }

    public ILeaseStore Leases { get; }

    public IReconcileStore Reconcile { get; }

    public IJobStore Jobs { get; }

    public ICalendarStore Calendars { get; }

    public IChainStore Chains { get; }

    public IProcessStore Processes { get; }

    public IMonitoringStore Monitoring { get; }

    public ISignalTransport Signals { get; }

    public ValueTask MigrateAsync(CancellationToken cancellationToken) => new(FrameworkSchema.MigrateAsync(dataSource, schema, cancellationToken));
}
