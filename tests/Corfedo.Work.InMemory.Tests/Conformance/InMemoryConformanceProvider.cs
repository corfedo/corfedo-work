using System.Collections.Concurrent;
using System.Globalization;
using Corfedo.Work.Conformance;
using Corfedo.Work.InMemory.Database;
using Corfedo.Work.InMemory.Jobs;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.InMemory.Tests.Conformance;

/// <summary>Runs the conformance scenarios against a fresh in-memory database per test class, on the system clock.</summary>
public class InMemoryConformanceProvider : IReconcileConformanceProvider, IJobConformanceProvider
{
    private const string Schema = "app";
    private readonly InMemoryWorkDatabase database;
    private readonly ConcurrentDictionary<StoreLocation, string[]> prefixes = new();
    private int tables;
    private long journal;

    public InMemoryConformanceProvider()
        : this(TimeProvider.System)
    {
    }

    protected InMemoryConformanceProvider(TimeProvider clock) => database = new InMemoryWorkDatabase(clock);

    public bool HasSignals => true;

    public bool ConcurrentTransactions => false;

    public void AddStore(IServiceCollection services) => services.AddInMemoryWorkStore(database);

    public void AddApplicationStore(IServiceCollection services, string application) => services.AddInMemoryWorkStore(database, application);

    public IWorkStore CreateStore() => new InMemoryWorkStore(database);

    public Task<StoreLocation> CreateTableAsync(IReadOnlyList<string> prefixes, CancellationToken cancellationToken) =>
        Task.FromResult(CreateTable(prefixes, []));

    private StoreLocation CreateTable(IReadOnlyList<string> fieldSets, IEnumerable<string> extra)
    {
        var table = StoreLocation.Table(Schema, "records_" + Interlocked.Increment(ref tables));
        database.CreateTable(table, ["tenant", "id"], ["progress", .. fieldSets.SelectMany(LeaseFieldNames), .. extra]);
        database.CreateTable(Journal(table), ["n"], ["entry"]);
        prefixes[table] = [.. fieldSets];
        return table;
    }

    private static IEnumerable<string> LeaseFieldNames(string prefix)
    {
        var f = LeaseFields.Default;
        return new[]
        {
            f.DueAt, f.LeaseOwner, f.LeaseEpoch, f.LeaseExpiresAt, f.Takeovers, f.Failures, f.SettledEpoch, f.SettleFailures,
            f.SettleRetryAt, f.CancelRequestedAt, f.ExecutionId, f.EndedCompletionId, f.EndedDigest,
        }.Select(n => prefix + n);
    }

    public Task<StoreLocation> CreateReconcileTableAsync(CancellationToken cancellationToken)
    {
        var r = ReconcileFields.Default;
        return Task.FromResult(CreateTable([""], [r.RequestedRevision, r.ClaimedRevision, r.ConvergedRevision, r.FirstRequestedAt, r.NotBefore, r.AttemptedGeneration, r.ConvergedGeneration]));
    }

    public Task InsertReconcileAsync(StoreLocation table, TestKey key, DueState due, long revision, CancellationToken cancellationToken)
    {
        DateTimeOffset? dueAt = due switch
        {
            DueState.None => null,
            _ => Now,
        };
        var r = ReconcileFields.Default;
        return InsertAsync(table, new Dictionary<string, object?>
        {
            ["tenant"] = key.Tenant,
            ["id"] = key.Id,
            [LeaseFields.Default.DueAt] = dueAt,
            [r.RequestedRevision] = revision,
            [r.ClaimedRevision] = revision,
            [r.ConvergedRevision] = revision,
        }, cancellationToken);
    }

    public Task<ReconcileState> ReadReconcileAsync(StoreLocation table, TestKey key, CancellationToken cancellationToken)
    {
        var lease = Read(table, key, "");
        var record = Find(table, key);
        var r = ReconcileFields.Default;
        var now = Now;
        var due = (DateTimeOffset?)record[LeaseFields.Default.DueAt];
        return Task.FromResult(new ReconcileState(
            lease,
            Int64(record, r.RequestedRevision),
            record.GetValueOrDefault(r.ClaimedRevision) is { } claimed ? Convert.ToInt64(claimed, CultureInfo.InvariantCulture) : null,
            Int64(record, r.ConvergedRevision),
            record.GetValueOrDefault(r.FirstRequestedAt) is not null,
            due is { } value ? value - now : null,
            (DateTimeOffset?)record.GetValueOrDefault(r.NotBefore) - now,
            Int64(record, r.AttemptedGeneration),
            Int64(record, r.ConvergedGeneration)));
    }

    public Task<string> RecordVersionsAsync(StoreLocation table, CancellationToken cancellationToken)
    {
        var rows = database.Table(table).Rows;
        return Task.FromResult(database.ReadCommitted(() =>
            string.Join(",", rows.Committed.OrderBy(r => r.Key).Select(r => r.Value.Version.ToString(CultureInfo.InvariantCulture)))));
    }

    public Task InsertAsync(StoreLocation table, TestKey key, TimeSpan? dueIn, CancellationToken cancellationToken)
    {
        var values = new Dictionary<string, object?> { ["tenant"] = key.Tenant, ["id"] = key.Id };
        DateTimeOffset? due = dueIn is { } d ? Now + d : null;
        foreach (var prefix in prefixes[table])
        {
            values[prefix + LeaseFields.Default.DueAt] = due;
        }
        return InsertAsync(table, values, cancellationToken);
    }

    private async Task InsertAsync(StoreLocation table, Dictionary<string, object?> values, CancellationToken cancellationToken)
    {
        using var transaction = await database.BeginAsync(cancellationToken);
        transaction.Insert(table, values);
        transaction.Commit();
    }

    public Task<RecordState> ReadAsync(StoreLocation table, TestKey key, string prefix, CancellationToken cancellationToken) =>
        Task.FromResult(Read(table, key, prefix));

    private RecordState Read(StoreLocation table, TestKey key, string prefix)
    {
        var record = Find(table, key);
        var f = LeaseFields.Default;
        var now = Now;
        var due = (DateTimeOffset?)record.GetValueOrDefault(prefix + f.DueAt);
        var expires = (DateTimeOffset?)record.GetValueOrDefault(prefix + f.LeaseExpiresAt);
        return new RecordState(
            due is null ? DueState.None : due <= now ? DueState.Past : DueState.Future,
            record.GetValueOrDefault(prefix + f.LeaseOwner) is Guid owner ? new OwnerId(owner) : null,
            Int64(record, prefix + f.LeaseEpoch),
            expires is not null,
            expires > now,
            (int)Int64(record, prefix + f.Takeovers),
            (int)Int64(record, prefix + f.Failures),
            Int64(record, prefix + f.SettledEpoch),
            (int)Int64(record, prefix + f.SettleFailures),
            record.GetValueOrDefault(prefix + f.CancelRequestedAt) is not null,
            (Guid?)record.GetValueOrDefault(prefix + f.ExecutionId),
            (string?)record.GetValueOrDefault("progress"));
    }

    public async Task<IConformanceTransaction> BeginAsync(CancellationToken cancellationToken) =>
        new Transaction(await database.BeginAsync(cancellationToken));

    public Task JournalAsync(WorkTransaction transaction, StoreLocation table, string entry, CancellationToken cancellationToken)
    {
        transaction.InMemory().Insert(Journal(table), new Dictionary<string, object?> { ["n"] = Interlocked.Increment(ref journal), ["entry"] = entry });
        return Task.CompletedTask;
    }

    /// <summary>The in-memory store serializes transactions, so no record is ever held by another one.</summary>
    public Task LockWithoutWaitingAsync(WorkTransaction transaction, StoreLocation table, TestKey key, CancellationToken cancellationToken) => Task.CompletedTask;

    public Task InsertInTransactionAsync(WorkTransaction transaction, StoreLocation table, TestKey key, CancellationToken cancellationToken)
    {
        transaction.InMemory().Insert(table, new Dictionary<string, object?> { ["tenant"] = key.Tenant, ["id"] = key.Id });
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<string>> ReadJournalAsync(StoreLocation table, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<string>>([.. database.Read(Journal(table)).Select(r => (string)r["entry"]!)]);

    public Task<JobState> ReadJobAsync(JobId job, CancellationToken cancellationToken) =>
        Task.FromResult(ReadJobs(r => r.Uuid(JobColumns.Id) == job.Value).SingleOrDefault() ?? throw new InvalidOperationException($"No job {job}."));

    public Task<IReadOnlyList<JobState>> ReadJobsAsync(string definition, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<JobState>>(ReadJobs(r => r.Text(JobColumns.Definition) == definition));

    private List<JobState> ReadJobs(Func<Database.Record, bool> filter)
    {
        var now = Now;
        var rows = database.ReadCommitted(() => Work.Jobs.Rows.Committed.Values.Where(filter).ToList());
        var f = LeaseFields.Default;
        return [.. rows
            .OrderBy(r => r.Time(JobColumns.CreatedAt))
            .ThenBy(r => r.Uuid(JobColumns.Id))
            .Select(r => new JobState(
                r.JobId(),
                r.Text(JobColumns.Definition)!,
                r.Outstanding() ? null : (JobOutcome)r.Int32(JobColumns.State),
                r[JobColumns.Outcome] is int outcome ? (JobOutcome)outcome : null,
                r.Text(JobColumns.Reason),
                ArgsValue(r.Bytes(JobColumns.Args)!),
                r.Int32(JobColumns.ArgsVersion),
                r.Bool(JobColumns.Started),
                r.Int32(JobColumns.Attempts),
                r.Bytes(JobColumns.PendingArgs) is { } pending ? ArgsValue(pending) : null,
                r.Time(f.DueAt) is { } due ? due - now : null,
                r.Time(JobColumns.PendingDue) - now,
                r.Time(f.LeaseExpiresAt) > now,
                r.Time(f.CancelRequestedAt) is not null,
                r.Uuid(f.ExecutionId)!.Value,
                r.Text(JobColumns.Partition)!,
                r.Text(JobColumns.Occurrence),
                r.Bool(JobColumns.Unconsumed),
                r.Bool(JobColumns.SkipPending),
                r.Bool(JobColumns.Paused),
                r.Time(JobColumns.EndedAt),
                r.Text(JobColumns.MergeKey) is not null,
                r.Bool(JobColumns.CalendarUnavailable),
                r.Bool(JobColumns.Waited)))];
    }

    public Task<bool> ReadRecurrencePausedAsync(string definition, Subject subject, CancellationToken cancellationToken) =>
        Task.FromResult(database.ReadCommitted(() => Work.Recurrences.Committed.Values.FirstOrDefault(c => c.Definition == definition && c.Subject == subject))?.Paused
            ?? throw new InvalidOperationException($"No recurrence of {definition} for {subject}."));

    /// <summary>The transaction holds the whole store, so the subject's rows are locked already.</summary>
    public Task LockSubjectAsync(WorkTransaction transaction, string definition, Subject subject, CancellationToken cancellationToken) => Task.CompletedTask;

    public Task<DateTimeOffset> NowAsync(CancellationToken cancellationToken) => Task.FromResult(Now);

    public Task<long> CommittedTransactionsAsync(CancellationToken cancellationToken) => Task.FromResult(database.CommittedTransactions);

    public Task<ProcessRecord?> ReadProcessAsync(OwnerId owner, CancellationToken cancellationToken)
    {
        var process = database.ReadCommitted(() => Work.Processes.Committed.GetValueOrDefault(owner));
        return Task.FromResult(process is null
            ? null
            : new ProcessRecord(process.Type, process.Release, Now - process.HeartbeatAt, process.StoppedAt is not null, [.. process.Definitions.OrderBy(d => d.Definition, StringComparer.Ordinal)]));
    }

    public async Task AgeProcessAsync(OwnerId owner, TimeSpan by, CancellationToken cancellationToken)
    {
        using var transaction = await database.BeginAsync(cancellationToken);
        if (transaction.Get(Work.Processes, owner) is { } process)
        {
            transaction.Put(Work.Processes, owner, process with { StartedAt = process.StartedAt - by, HeartbeatAt = process.HeartbeatAt - by, StoppedAt = process.StoppedAt - by });
        }
        transaction.Commit();
    }

    public async Task DeleteProcessAsync(OwnerId owner, CancellationToken cancellationToken)
    {
        using var transaction = await database.BeginAsync(cancellationToken);
        transaction.Remove(Work.Processes, owner);
        transaction.Commit();
    }

    /// <summary>The framework state of the default application, which the scenarios' stores use.</summary>
    private ApplicationState Work => database.Application(InMemoryServiceCollectionExtensions.DefaultApplication);

    /// <summary>The value of conformance arguments, stored as <c>{"Value":"..."}</c>; empty for other jobs, such as retention.</summary>
    private static string ArgsValue(byte[] json) =>
        System.Text.Json.JsonDocument.Parse(json).RootElement.TryGetProperty("Value", out var value) ? value.GetString()! : "";

    private DateTimeOffset Now => database.Clock.GetUtcNow();

    private IReadOnlyDictionary<string, object?> Find(StoreLocation table, TestKey key) =>
        database.Find(table, [key.Tenant, key.Id]) ?? throw new InvalidOperationException($"No record {key}.");

    private static long Int64(IReadOnlyDictionary<string, object?> record, string field) =>
        record.GetValueOrDefault(field) is { } value ? Convert.ToInt64(value, CultureInfo.InvariantCulture) : 0;

    private static StoreLocation Journal(StoreLocation table) => StoreLocation.Table(table.Schema, table.Name + "_journal");

    private sealed class Transaction(InMemoryTransaction transaction) : IConformanceTransaction
    {
        private InMemoryTransaction.Savepoint? savepoint;

        public WorkTransaction Work => transaction;

        public Task WriteProgressAsync(StoreLocation table, TestKey key, string progress, CancellationToken cancellationToken)
        {
            transaction.Update(table, [key.Tenant, key.Id], new Dictionary<string, object?> { ["progress"] = progress });
            return Task.CompletedTask;
        }

        public Task<string?> ReadProgressAsync(StoreLocation table, TestKey key, CancellationToken cancellationToken) =>
            Task.FromResult((string?)transaction.Find(table, [key.Tenant, key.Id])?.GetValueOrDefault("progress"));

        /// <summary>The transaction holds the whole store, so every record is locked already.</summary>
        public Task LockAsync(StoreLocation table, TestKey key, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task SavepointAsync(CancellationToken cancellationToken)
        {
            savepoint = transaction.Save();
            return Task.CompletedTask;
        }

        public Task RollbackToSavepointAsync(CancellationToken cancellationToken)
        {
            transaction.RollbackTo(savepoint ?? throw new InvalidOperationException("No savepoint was taken."));
            return Task.CompletedTask;
        }

        public Task CommitAsync(CancellationToken cancellationToken)
        {
            transaction.Commit();
            return Task.CompletedTask;
        }

        public Task RollbackAsync(CancellationToken cancellationToken)
        {
            transaction.Rollback();
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            transaction.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}

/// <summary>The in-memory provider on an <see cref="InMemoryClock"/> standing on a Monday at 06:00 UTC, which scenarios advance.</summary>
public sealed class InMemoryClockConformanceProvider : InMemoryConformanceProvider, IClockConformanceProvider
{
    private readonly InMemoryClock clock;

    public InMemoryClockConformanceProvider()
        : this(new InMemoryClock(new DateTimeOffset(2027, 1, 4, 6, 0, 0, TimeSpan.Zero)))
    {
    }

    private InMemoryClockConformanceProvider(InMemoryClock clock)
        : base(clock) => this.clock = clock;

    public TimeProvider Clock => clock;

    public void Advance(TimeSpan by) => clock.Advance(by);
}

public sealed class InMemorySignalScenarios(InMemoryConformanceProvider provider)
    : SignalScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryLeaseClaimScenarios(InMemoryConformanceProvider provider)
    : LeaseClaimScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryLeaseHolderScenarios(InMemoryConformanceProvider provider)
    : LeaseHolderScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryLeaseTakeoverScenarios(InMemoryConformanceProvider provider)
    : LeaseTakeoverScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryLeaseLimitScenarios(InMemoryConformanceProvider provider)
    : LeaseLimitScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryRemoteHolderScenarios(InMemoryConformanceProvider provider)
    : RemoteHolderScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryReconcileRequestScenarios(InMemoryConformanceProvider provider)
    : ReconcileRequestScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryReconcileResultScenarios(InMemoryConformanceProvider provider)
    : ReconcileResultScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryReconcileInvalidationScenarios(InMemoryConformanceProvider provider)
    : ReconcileInvalidationScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryReconcileSupersessionScenarios(InMemoryConformanceProvider provider)
    : ReconcileSupersessionScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryReconcilePoolScenarios(InMemoryConformanceProvider provider)
    : ReconcilePoolScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryJobRequestScenarios(InMemoryConformanceProvider provider)
    : JobRequestScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryJobAttemptScenarios(InMemoryConformanceProvider provider)
    : JobAttemptScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryJobDefinitionScenarios(InMemoryConformanceProvider provider)
    : JobDefinitionScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryJobOutcomeScenarios(InMemoryConformanceProvider provider)
    : JobOutcomeScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryJobRecurrenceScenarios(InMemoryConformanceProvider provider)
    : JobRecurrenceScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryJobLifecycleScenarios(InMemoryConformanceProvider provider)
    : JobLifecycleScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryJobOverlapScenarios(InMemoryConformanceProvider provider)
    : JobOverlapScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryJobPauseScenarios(InMemoryConformanceProvider provider)
    : JobPauseScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryJobResultScenarios(InMemoryConformanceProvider provider)
    : JobResultScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryJobStatusScenarios(InMemoryConformanceProvider provider)
    : JobStatusScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryJobRunHistoryScenarios(InMemoryConformanceProvider provider)
    : JobRunHistoryScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryJobActionScenarios(InMemoryConformanceProvider provider)
    : JobActionScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryProcessRegistryScenarios(InMemoryConformanceProvider provider)
    : ProcessRegistryScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryJobCoverageScenarios(InMemoryConformanceProvider provider)
    : JobCoverageScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryJobRetentionScenarios(InMemoryConformanceProvider provider)
    : JobRetentionScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryJobCalendarScenarios(InMemoryConformanceProvider provider)
    : JobCalendarScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryJobChainScenarios(InMemoryConformanceProvider provider)
    : JobChainScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryJobMonitoringScenarios(InMemoryConformanceProvider provider)
    : JobMonitoringScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryJobTelemetryScenarios(InMemoryConformanceProvider provider)
    : JobTelemetryScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryHostStartScenarios(InMemoryConformanceProvider provider)
    : HostStartScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryManySourcesScenarios(InMemoryConformanceProvider provider)
    : ManySourcesScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryProcessRoleScenarios(InMemoryConformanceProvider provider)
    : ProcessRoleScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryApplicationIsolationScenarios(InMemoryConformanceProvider provider)
    : ApplicationIsolationScenarios(provider), IClassFixture<InMemoryConformanceProvider>;

public sealed class InMemoryControlledClockScenarios(InMemoryClockConformanceProvider provider)
    : ControlledClockScenarios(provider), IClassFixture<InMemoryClockConformanceProvider>;
