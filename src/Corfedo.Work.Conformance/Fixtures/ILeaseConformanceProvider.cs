using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>
/// What a provider supplies to run the lease scenarios: its store, and test tables keyed by <see cref="TestKey"/> with
/// one or more lease field sets, a <c>progress</c> domain field and a journal written in work transactions.
/// </summary>
public interface ILeaseConformanceProvider
{
    /// <summary>Whether the store delivers commit-time signals.</summary>
    bool HasSignals { get; }

    /// <summary>
    /// Whether transactions run concurrently, each locking the records it uses. A store that serializes transactions
    /// cannot keep one open while another runs, so scenarios that need that are skipped.
    /// </summary>
    bool ConcurrentTransactions { get; }

    /// <summary>Registers the provider's store in a process's services.</summary>
    void AddStore(IServiceCollection services);

    /// <summary>
    /// Registers the store of another application sharing the provider's database, isolated as the provider isolates
    /// applications (R19). Transactions from <see cref="BeginAsync"/> serve it as well.
    /// </summary>
    void AddApplicationStore(IServiceCollection services, string application);

    /// <summary>A store instance outside any host, for observing signals.</summary>
    IWorkStore CreateStore();

    /// <summary>
    /// Creates a table whose records carry one lease field set per prefix (field names are the defaults with the
    /// prefix), with the indexes the statements need, and an empty journal.
    /// </summary>
    Task<StoreLocation> CreateTableAsync(IReadOnlyList<string> prefixes, CancellationToken cancellationToken);

    /// <summary>Inserts a record due <paramref name="dueIn"/> from the store's now in every field set; null for not eligible.</summary>
    Task InsertAsync(StoreLocation table, TestKey key, TimeSpan? dueIn, CancellationToken cancellationToken);

    /// <summary>Reads one field set of a record, evaluating times with the store's clock.</summary>
    Task<RecordState> ReadAsync(StoreLocation table, TestKey key, string prefix, CancellationToken cancellationToken);

    Task<IConformanceTransaction> BeginAsync(CancellationToken cancellationToken);

    /// <summary>Appends a journal entry in a work transaction, as hooks and handlers write domain data.</summary>
    Task JournalAsync(IWorkTransaction transaction, StoreLocation table, string entry, CancellationToken cancellationToken);

    /// <summary>
    /// Inserts a record in a work transaction, as domain writes do; a key that exists fails with the store's unique-key
    /// violation, whose details name the key.
    /// </summary>
    Task InsertInTransactionAsync(IWorkTransaction transaction, StoreLocation table, TestKey key, CancellationToken cancellationToken);

    /// <summary>Committed journal entries in insertion order.</summary>
    Task<IReadOnlyList<string>> ReadJournalAsync(StoreLocation table, CancellationToken cancellationToken);
}

/// <summary>An application transaction against the store.</summary>
public interface IConformanceTransaction : IAsyncDisposable
{
    IWorkTransaction Work { get; }

    Task WriteProgressAsync(StoreLocation table, TestKey key, string progress, CancellationToken cancellationToken);

    Task<string?> ReadProgressAsync(StoreLocation table, TestKey key, CancellationToken cancellationToken);

    /// <summary>Locks the record for update without changing it, waiting for other holders of its lock.</summary>
    Task LockAsync(StoreLocation table, TestKey key, CancellationToken cancellationToken);

    /// <summary>Takes a savepoint that <see cref="RollbackToSavepointAsync"/> returns to.</summary>
    Task SavepointAsync(CancellationToken cancellationToken);

    /// <summary>Discards the writes and signals made after the savepoint; the transaction stays active.</summary>
    Task RollbackToSavepointAsync(CancellationToken cancellationToken);

    Task CommitAsync(CancellationToken cancellationToken);

    Task RollbackAsync(CancellationToken cancellationToken);
}

public enum DueState
{
    None,
    Past,
    Future,
}

/// <summary>One lease field set of a record. <see cref="Valid"/> is evaluated with the store's clock.</summary>
public sealed record RecordState(
    DueState Due,
    OwnerId? Owner,
    long Epoch,
    bool Held,
    bool Valid,
    int Takeovers,
    int Failures,
    long SettledEpoch,
    int SettleFailures,
    bool CancelRequested,
    Guid? ExecutionId,
    string? Progress);
