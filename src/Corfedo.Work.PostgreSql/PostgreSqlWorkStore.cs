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
    private readonly WorkSchemaMode schemaMode;

    /// <summary>
    /// A store with its framework tables in <paramref name="schema"/>; <paramref name="schemaMode"/> says whether a
    /// starting process applies the schema or only verifies it.
    /// </summary>
    public PostgreSqlWorkStore(NpgsqlDataSource dataSource, string schema, ILoggerFactory loggerFactory, TimeProvider time, WorkSchemaMode schemaMode = WorkSchemaMode.Migrate)
        : this(dataSource, schema, loggerFactory, time, WorkTelemetryOptions.Default, schemaMode)
    {
    }

    internal PostgreSqlWorkStore(NpgsqlDataSource dataSource, string schema, ILoggerFactory loggerFactory, TimeProvider time, WorkTelemetryOptions telemetry, WorkSchemaMode schemaMode)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);
        this.dataSource = dataSource;
        this.schema = schema;
        this.schemaMode = schemaMode;
        var logger = loggerFactory.CreateLogger("Corfedo.Work.PostgreSql");
        var signals = new PostgreSqlSignalSender(dataSource, PostgreSqlSignalSql.For(schema), logger, telemetry);
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

    public ValueTask PrepareAsync(CancellationToken cancellationToken) => new(schemaMode == WorkSchemaMode.Verify
        ? FrameworkSchema.VerifyAsync(dataSource, schema, cancellationToken)
        : FrameworkSchema.MigrateAsync(dataSource, schema, [], cancellationToken));
}
