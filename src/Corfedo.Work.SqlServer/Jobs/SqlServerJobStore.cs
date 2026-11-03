using Corfedo.Work.SqlServer.Calendars;
using Corfedo.Work.SqlServer.Leases;
using Corfedo.Work.SqlServer.Monitoring;
using Corfedo.Work.SqlServer.Signals;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace Corfedo.Work.SqlServer.Jobs;

/// <summary>
/// Job definitions, requests, transitions and cancellation on the framework's job table. Statements of one definition
/// use the statements of its lease source, so they carry the same definition scope as claims. Rows are locked through
/// the primary key before any other index, several rows in primary-key order, and every condition that depends on a
/// lock is evaluated in a statement after the one that took it.
/// </summary>
internal sealed partial class SqlServerJobStore(
    string connectionString,
    SqlServerLeaseStore leases,
    SqlServerCalendarStore calendars,
    SqlServerMonitoringStore monitoring,
    string schema,
    SqlServerSignalSql signals,
    ILogger logger,
    WorkTelemetryOptions telemetry) : IJobStore
{
    private readonly string definitions = Tsql.Quote(schema) + ".job_definitions";
    private readonly string recurrences = Tsql.Quote(schema) + ".job_recurrences";
    private readonly string pauses = Tsql.Quote(schema) + ".job_pauses";
    private readonly string changes = Tsql.Quote(schema) + ".job_expectation_changes";
    private readonly string runs = Tsql.Quote(schema) + ".job_runs";

    public StoreLocation Table { get; } = StoreLocation.Table(schema, "jobs");

    /// <summary>The statements of a job definition's source.</summary>
    private async ValueTask<LeaseSql> SqlAsync(LeaseSource<JobId> source, CancellationToken cancellationToken)
    {
        if (source.Job is null)
        {
            throw new ArgumentException($"Lease source {source.Name} is not a job source.", nameof(source));
        }
        return await leases.SqlAsync(source, cancellationToken);
    }

    /// <summary>Signals claimers of the source when the transaction commits.</summary>
    private Task NotifyDueAsync(SqlServerTransaction work, LeaseSource<JobId> source, CancellationToken cancellationToken) =>
        leases.NotifyAsync(work.Connection, work.Transaction, WorkTopics.Due, source.Name, cancellationToken);

    /// <summary>Signals the definition's planners when the transaction commits.</summary>
    private Task NotifyPlanAsync(SqlServerTransaction work, LeaseSource<JobId> source, CancellationToken cancellationToken) =>
        leases.NotifyAsync(work.Connection, work.Transaction, WorkTopics.Plan, source.Name, cancellationToken);

    /// <summary>
    /// A statement that signals monitoring loops when the transaction commits, if <paramref name="queued"/>, a condition
    /// that keys were queued, holds and the definition in <c>@definition</c> expects completions, the only expectation
    /// whose keys a recurrence decides. The key is the parameter <c>@monitor_key</c> of <see cref="MonitorSignal"/>.
    /// </summary>
    private string NotifyMonitor(string queued) =>
        signals.InsertIf(WorkTopics.Monitor, "@monitor_key",
            $"({queued}) AND EXISTS (SELECT 1 FROM {definitions} AS d WHERE d.[key] = @definition AND d.expect_time IS NOT NULL)");

    /// <summary>The key parameter of <see cref="NotifyMonitor"/>.</summary>
    private static SqlParameter MonitorSignal(LeaseSource<JobId> source) => Tsql.Text("@monitor_key", source.Name, 1024);

    /// <summary>The copied pause flag of a new job, as a bit: whether its subject has a stored pause.</summary>
    private string PausedCopy(string definition, string subjectKind, string subjectId) =>
        Tsql.Bit($"EXISTS (SELECT 1 FROM {pauses} AS p WHERE p.[definition] = {definition} AND p.subject_kind = {subjectKind} AND p.subject_id = {subjectId})");

    /// <summary>Signals the holder's process to renew its holds of the source at once and observe a cancellation request.</summary>
    private async Task NotifyCancelAsync(SqlServerTransaction work, LeaseSource<JobId> source, IEnumerable<Guid> owners, CancellationToken cancellationToken)
    {
        foreach (var owner in owners.Distinct())
        {
            await signals.SendAsync(work.Connection, work.Transaction, WorkTopics.Cancel, WorkTopics.CancelKey(new OwnerId(owner), source.Name), cancellationToken);
        }
    }

    /// <summary>
    /// Runs <paramref name="operation"/> in a provider transaction of its own at read committed, committed when it
    /// returns, and again when SQL Server chose it as a deadlock victim.
    /// </summary>
    private Task<T> InTransactionAsync<T>(Func<SqlServerTransaction, Task<T>> operation, CancellationToken cancellationToken) =>
        SqlServerLeaseStore.RetryDeadlocksAsync(async () =>
        {
            await using var connection = await leases.OpenAsync(cancellationToken);
            await using var transaction = await SqlServerLeaseStore.BeginAsync(connection, cancellationToken);
            var result = await operation(new SqlServerTransaction(transaction));
            await transaction.CommitAsync(cancellationToken);
            return result;
        });

    [LoggerMessage(EventId = 36, EventName = "job.finalize_failed", Level = LogLevel.Warning, Message = "Ending job {Definition} {Job} recorded to be skipped or whose chain settled failed; it is retried in a later pass: {Error}")]
    private static partial void LogFinalizeFailed(ILogger logger, Exception? exception, string definition, Guid job, string error);

    [LoggerMessage(EventId = 35, EventName = "job.retire_failed", Level = LogLevel.Warning, Message = "Ending job {Definition} {Job} for its retired argument version failed; it is retried in a later pass: {Error}")]
    private static partial void LogRetireFailed(ILogger logger, Exception? exception, string definition, Guid job, string error);
}
