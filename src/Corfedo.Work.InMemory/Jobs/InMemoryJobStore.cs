using Corfedo.Work.InMemory.Database;
using Corfedo.Work.InMemory.Leases;
using Corfedo.Work.InMemory.Monitoring;
using Microsoft.Extensions.Logging;
using static Corfedo.Work.InMemory.Leases.InMemoryLeaseStore;

namespace Corfedo.Work.InMemory.Jobs;

/// <summary>
/// Job definitions, requests, transitions, cancellation, recurrences and their planning, calendar planning, chains,
/// pauses, actions, reads, retention and coverage reports of one application, on the application's job table. Operations of one definition use
/// the rules of its lease source, so they carry the same definition scope as claims. Every operation runs in one
/// serialized transaction, so the locks and lock orders of the PostgreSQL store hold by construction.
/// </summary>
internal sealed partial class InMemoryJobStore(
    InMemoryWorkDatabase database,
    ApplicationState application,
    InMemoryLeaseStore leases,
    InMemoryMonitoringStore monitoring,
    ILogger logger,
    WorkTelemetryOptions telemetry)
    : IJobStore, IChainStore
{
    public StoreLocation Table => application.Jobs.Location;

    private Table<RowKey, Record> JobRows => application.Jobs.Rows;

    private DateTimeOffset Now => database.Clock.GetUtcNow();

    /// <summary>The rules of a job definition's source.</summary>
    private LeaseRules<JobId> Rules(LeaseSource<JobId> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        if (source.Job is null)
        {
            throw new ArgumentException($"Lease source {source.Name} is not a job source.", nameof(source));
        }
        return leases.Rules(source);
    }

    private InMemoryTransaction Join(IWorkTransaction transaction) => leases.Join(transaction);

    private static RowKey Key(Guid job) => new([job]);

    /// <summary>The job with the id, of any definition, as the transaction sees it.</summary>
    private Record? Job(InMemoryTransaction transaction, Guid job) => transaction.Get(JobRows, Key(job));

    /// <summary>The job with the id if it belongs to <paramref name="definition"/>.</summary>
    private Record? Job(InMemoryTransaction transaction, string definition, Guid job) =>
        Job(transaction, job) is { } r && r.Text(JobColumns.Definition) == definition ? r : null;

    /// <summary>Every job record as the transaction sees it, in no particular order.</summary>
    private IEnumerable<Record> Jobs(InMemoryTransaction transaction) => transaction.Scan(JobRows).Select(r => r.Row);

    /// <summary>The jobs of a definition as the transaction sees them.</summary>
    private IEnumerable<Record> Jobs(InMemoryTransaction transaction, string definition) =>
        Jobs(transaction).Where(r => r.Text(JobColumns.Definition) == definition);

    /// <summary>Writes <paramref name="changes"/> to a job and returns the written record.</summary>
    private Record Write(InMemoryTransaction transaction, Record r, IEnumerable<KeyValuePair<string, object?>> changes)
    {
        var written = r.With(changes, database.NextVersion());
        transaction.Put(JobRows, Key(r.Uuid(JobColumns.Id)!.Value), written);
        return written;
    }

    /// <summary>Inserts a job with <paramref name="values"/>; every other column starts null, false or 0.</summary>
    private void Insert(InMemoryTransaction transaction, Dictionary<string, object?> values) =>
        transaction.Put(JobRows, Key((Guid)values[JobColumns.Id]!), new Record(values, database.NextVersion()));

    /// <summary>Whether a stored pause holds the subject of a definition.</summary>
    private bool SubjectPaused(InMemoryTransaction transaction, string definition, Subject? subject) =>
        subject is not null && transaction.Get(application.Pauses, PauseKey.Of(definition, subject)) is not null;

    /// <summary>Whether a stored pause holds the job's subject.</summary>
    private bool SubjectPaused(InMemoryTransaction transaction, Record r) =>
        r.PauseKey() is { } key && transaction.Get(application.Pauses, key) is not null;

    private StoredJobDefinition? Stored(InMemoryTransaction transaction, string definition) =>
        transaction.Get(application.Definitions, definition)?.Stored;

    /// <summary>Signals claimers of the source when the transaction commits.</summary>
    private void NotifyDue(InMemoryTransaction transaction, LeaseSource<JobId> source) => leases.Signal(transaction, WorkTopics.Due, source.Name);

    /// <summary>Signals the definition's planners when the transaction commits.</summary>
    private void NotifyPlan(InMemoryTransaction transaction, LeaseSource<JobId> source) => leases.Signal(transaction, WorkTopics.Plan, source.Name);

    /// <summary>
    /// Queues the key of a recurrence that changed for the monitoring loop, and signals the loop when the definition
    /// expects completions, the only expectation whose keys a recurrence decides.
    /// </summary>
    private void QueueExpectationChange(InMemoryTransaction transaction, LeaseSource<JobId> source, string definition, Subject? subject)
    {
        QueueChange(transaction, definition, subject);
        NotifyMonitor(transaction, source, definition);
    }

    /// <summary>Queues the key of a recurrence that changed for the monitoring loop.</summary>
    private void QueueChange(InMemoryTransaction transaction, string definition, Subject? subject) =>
        transaction.Put(application.ExpectationChanges, database.NextVersion(), new ExpectationChange(definition, subject));

    /// <summary>Signals monitoring loops when the definition expects completions.</summary>
    private void NotifyMonitor(InMemoryTransaction transaction, LeaseSource<JobId> source, string definition)
    {
        if (Stored(transaction, definition)?.Policy.Expect?.Deadline is not null)
        {
            leases.Signal(transaction, WorkTopics.Monitor, source.Name);
        }
    }

    /// <summary>A job is not held: no lease, or an expired one.</summary>
    private static bool NotHeld(Record r, DateTimeOffset now) => r.Time(LeaseFields.Default.LeaseExpiresAt) is not { } expires || expires <= now;

    /// <summary>
    /// Running: an attempt under a valid lease, or an occurrence of a recurring chain whose attempt its chain holds until
    /// the chain settled. Every read and operation that tells running work from waiting work uses this.
    /// </summary>
    private static bool Running(Record r, DateTimeOffset now) =>
        r.Time(LeaseFields.Default.LeaseExpiresAt) > now || r.Uuid(JobColumns.OwnedChain) is not null;

    /// <summary>The earliest of the values that are set, as SQL's <c>LEAST</c>; null when none is.</summary>
    private static DateTimeOffset? Least(DateTimeOffset? a, DateTimeOffset? b) => a is null ? b : b is null ? a : a < b ? a : b;

    private static TimeSpan? Until(DateTimeOffset? at, DateTimeOffset now) => at is { } value ? value > now ? value - now : TimeSpan.Zero : null;

    [LoggerMessage(EventId = 36, EventName = "job.finalize_failed", Level = LogLevel.Warning, Message = "Ending job {Definition} {Job} recorded to be skipped or whose chain settled failed; it is retried in a later pass: {Error}")]
    private static partial void LogFinalizeFailed(ILogger logger, Exception? exception, string definition, Guid job, string error);

    [LoggerMessage(EventId = 35, EventName = "job.retire_failed", Level = LogLevel.Warning, Message = "Ending job {Definition} {Job} for its retired argument version failed; it is retried in a later pass: {Error}")]
    private static partial void LogRetireFailed(ILogger logger, Exception? exception, string definition, Guid job, string error);
}
