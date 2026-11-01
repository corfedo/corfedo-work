namespace Corfedo.Work.Conformance;

/// <summary>
/// What a provider supplies to run the reconciliation scenarios, in addition to the lease fixtures: tables whose
/// records carry the default lease and reconciliation fields with the indexes the statements need.
/// </summary>
public interface IReconcileConformanceProvider : ILeaseConformanceProvider
{
    /// <summary>Creates a table of reconciliation records with default field names, a <c>progress</c> domain field and an empty journal.</summary>
    Task<StoreLocation> CreateReconcileTableAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Inserts a reconciliation record converged at <paramref name="revision"/> under generation 0: due now for
    /// <see cref="DueState.Past"/>, idle for <see cref="DueState.None"/>.
    /// </summary>
    Task InsertReconcileAsync(StoreLocation table, TestKey key, DueState due, long revision, CancellationToken cancellationToken);

    /// <summary>Reads a record's lease and reconciliation fields, evaluating times with the store's clock.</summary>
    Task<ReconcileState> ReadReconcileAsync(StoreLocation table, TestKey key, CancellationToken cancellationToken);

    /// <summary>An opaque value that changes whenever any record of the table is written.</summary>
    Task<string> RecordVersionsAsync(StoreLocation table, CancellationToken cancellationToken);
}

/// <summary>
/// One reconciliation record. <see cref="DueIn"/> and <see cref="NotBeforeIn"/> are the times left by the store's
/// clock, negative when passed; <see cref="DueIn"/> is null when the due time is null or <c>Never</c>.
/// </summary>
public sealed record ReconcileState(
    RecordState Lease,
    long RequestedRevision,
    long? ClaimedRevision,
    long ConvergedRevision,
    bool FirstRequested,
    TimeSpan? DueIn,
    TimeSpan? NotBeforeIn,
    long AttemptedGeneration,
    long ConvergedGeneration);
