namespace Corfedo.Work;

/// <summary>
/// Atomic reconciliation operations of one store. Claims, renewals, guards and releases of reconciliation records are
/// lease operations on <see cref="ReconcileSource{TKey}.Leases"/>, which follow the reconciliation rules.
/// </summary>
public interface IReconcileStore
{
    /// <summary>
    /// In the caller's transaction, increments the record's requested revision and schedules it by the request's
    /// debounce and the source's request policy; signals claimers on commit. False if the record does not exist.
    /// </summary>
    ValueTask<bool> RequestAsync<TKey>(WorkTransaction transaction, ReconcileSource<TKey> source, TKey key, ReconcileRequest request, CancellationToken cancellationToken)
        where TKey : notnull;

    /// <summary>
    /// In the caller's transaction, requests each record of <paramref name="keys"/> as
    /// <see cref="RequestAsync{TKey}"/> does, locking the records in the order of <paramref name="keys"/>, and signals
    /// claimers once on commit. The keys are distinct and in ordinal order of their canonical form. Returns the keys of
    /// the records that exist, in the same order.
    /// </summary>
    ValueTask<IReadOnlyList<TKey>> RequestManyAsync<TKey>(WorkTransaction transaction, ReconcileSource<TKey> source, IReadOnlyList<TKey> keys, ReconcileRequest request, CancellationToken cancellationToken)
        where TKey : notnull;

    /// <summary>In the caller's transaction, applies a run's result to a validly held record and gives it back.</summary>
    /// <exception cref="LeaseLostException">The lease is not valid, or on a superseding source no longer current.</exception>
    ValueTask CompleteAsync<TKey>(WorkTransaction transaction, Lease<TKey> lease, ReconcileResult result, CancellationToken cancellationToken)
        where TKey : notnull;

    /// <summary>Applies a run's result in the store's own transaction; false if the lease was already lost.</summary>
    ValueTask<bool> CompleteAsync<TKey>(Lease<TKey> lease, ReconcileResult result, CancellationToken cancellationToken)
        where TKey : notnull;

    /// <summary>
    /// In the caller's transaction, takes the generation fences of the superseding sources among
    /// <paramref name="sources"/> in shared mode, in one fixed order, until the transaction ends. A transaction that
    /// commits holder output on superseding sources takes them before its other writes and row locks.
    /// </summary>
    /// <exception cref="InvalidOperationException">The transaction is not READ COMMITTED; it cannot commit.</exception>
    ValueTask FenceAsync(WorkTransaction transaction, IReadOnlyCollection<IReconcileSource> sources, CancellationToken cancellationToken);

    /// <summary>
    /// Reads the lease and reconciliation fields of the records of <paramref name="keys"/>, which are distinct, and the
    /// source's generation, as <see cref="ILeaseStore.ReadAsync{TKey}"/> reads lease fields.
    /// </summary>
    ValueTask<IReadOnlyList<KeyValuePair<TKey, ReconcileRecord>>> ReadAsync<TKey>(WorkTransaction? transaction, ReconcileSource<TKey> source, IReadOnlyList<TKey> keys, CancellationToken cancellationToken)
        where TKey : notnull;

    /// <summary>
    /// In the caller's transaction, increments the source's generation without writing any record, and returns it.
    /// Every eligible record becomes due once; on superseding sources running attempts become obsolete.
    /// </summary>
    ValueTask<long> InvalidateAllAsync<TKey>(WorkTransaction transaction, ReconcileSource<TKey> source, CancellationToken cancellationToken)
        where TKey : notnull;
}
