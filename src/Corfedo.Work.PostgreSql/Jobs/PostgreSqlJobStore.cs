using Corfedo.Work.PostgreSql.Calendars;
using Corfedo.Work.PostgreSql.Leases;
using Corfedo.Work.PostgreSql.Monitoring;
using Corfedo.Work.PostgreSql.Signals;
using Microsoft.Extensions.Logging;
using Npgsql;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Jobs;

/// <summary>
/// Job definitions, requests, transitions and cancellation on the framework's job table. Statements of one definition
/// use the statements of its lease source, so they carry the same definition scope as claims.
/// </summary>
internal sealed partial class PostgreSqlJobStore(NpgsqlDataSource dataSource, PostgreSqlLeaseStore leases, PostgreSqlCalendarStore calendars, PostgreSqlMonitoringStore monitoring, string schema, PostgreSqlSignalSql signals, ILogger logger, WorkTelemetryOptions telemetry) : IJobStore
{
    private readonly string definitions = LeaseSql.Quote(schema) + ".job_definitions";
    private readonly string recurrences = LeaseSql.Quote(schema) + ".job_recurrences";
    private readonly string pauses = LeaseSql.Quote(schema) + ".job_pauses";
    private readonly string changes = LeaseSql.Quote(schema) + ".job_expectation_changes";

    public StoreLocation Table { get; } = StoreLocation.Table(schema, "jobs");

    /// <summary>The statements of a job definition's source.</summary>
    private LeaseSql Sql(LeaseSource<JobId> source)
    {
        if (source.Job is null)
        {
            throw new ArgumentException($"Lease source {source.Name} is not a job source.", nameof(source));
        }
        return leases.Sql(source);
    }

    /// <summary>Signals claimers of the source when the transaction commits.</summary>
    private Task NotifyDueAsync(PostgreSqlTransaction work, LeaseSource<JobId> source, CancellationToken cancellationToken) =>
        leases.NotifyAsync(work.Connection, work.Transaction, WorkTopics.Due, source.Name, cancellationToken);

    /// <summary>Signals the definition's planners when the transaction commits.</summary>
    private Task NotifyPlanAsync(PostgreSqlTransaction work, LeaseSource<JobId> source, CancellationToken cancellationToken) =>
        leases.NotifyAsync(work.Connection, work.Transaction, WorkTopics.Plan, source.Name, cancellationToken);

    /// <summary>
    /// The query that ends a statement queueing expected keys: it signals monitoring loops when the transaction commits,
    /// if <paramref name="queued"/>, a CTE of the queued rows' definitions, is not empty and the definition expects
    /// completions, the only expectation whose keys a recurrence decides. The channel and payload are parameters
    /// <paramref name="channel"/> and the next.
    /// </summary>
    private string NotifyMonitor(string queued, int channel) =>
        $"SELECT pg_notify(${channel}, ${channel + 1}) FROM {definitions} AS d WHERE d.key = (SELECT q.definition FROM {queued} AS q LIMIT 1) AND d.expect_time IS NOT NULL";

    /// <summary>The channel and payload parameters of <see cref="NotifyMonitor"/>.</summary>
    private NpgsqlParameter[] MonitorSignal(LeaseSource<JobId> source) => leases.Signal(WorkTopics.Monitor, source.Name);

    /// <summary>The copied pause flag of a new job: whether its subject has a stored pause.</summary>
    private string PausedCopy(string definition, string subjectKind, string subjectId) =>
        $"EXISTS (SELECT 1 FROM {pauses} AS p WHERE p.definition = {definition} AND p.subject_kind = {subjectKind} AND p.subject_id = {subjectId})";

    /// <summary>Signals the holder's process to renew its holds of the source at once and observe a cancellation request.</summary>
    private async Task NotifyCancelAsync(PostgreSqlTransaction work, LeaseSource<JobId> source, IEnumerable<Guid> owners, CancellationToken cancellationToken)
    {
        foreach (var owner in owners.Distinct())
        {
            await signals.NotifyAsync(work.Connection, work.Transaction, WorkTopics.Cancel, WorkTopics.CancelKey(new OwnerId(owner), source.Name), cancellationToken);
        }
    }

    private static NpgsqlParameter<Guid[]> Uuids(Guid[] values) => new() { TypedValue = values, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Uuid };

    [LoggerMessage(EventId = 36, EventName = "job.finalize_failed", Level = LogLevel.Warning, Message = "Ending job {Definition} {Job} recorded to be skipped or whose chain settled failed; it is retried in a later pass: {Error}")]
    private static partial void LogFinalizeFailed(ILogger logger, Exception? exception, string definition, Guid job, string error);

    [LoggerMessage(EventId = 35, EventName = "job.retire_failed", Level = LogLevel.Warning, Message = "Ending job {Definition} {Job} for its retired argument version failed; it is retried in a later pass: {Error}")]
    private static partial void LogRetireFailed(ILogger logger, Exception? exception, string definition, Guid job, string error);
}
