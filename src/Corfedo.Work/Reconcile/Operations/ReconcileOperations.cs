namespace Corfedo.Work;

/// <summary>
/// Reconciliation operations for application code. Claims, renewals and guards of reconciliation records are the
/// lease operations on <see cref="ReconcileSource{TKey}.Leases"/>.
/// </summary>
public sealed partial class ReconcileOperations(IWorkStore store)
{
    private IReconcileStore Store => store.Reconcile;

    /// <summary>Requests a run of the record's latest revision in the caller's transaction; false if the record does not exist.</summary>
    public ValueTask<bool> RequestAsync<TKey>(WorkTransaction transaction, ReconcileSource<TKey> source, TKey key, CancellationToken cancellationToken = default)
        where TKey : notnull => Store.RequestAsync(transaction, source, key, ReconcileRequest.Default, cancellationToken);

    /// <inheritdoc cref="IReconcileStore.RequestAsync{TKey}"/>
    public ValueTask<bool> RequestAsync<TKey>(WorkTransaction transaction, ReconcileSource<TKey> source, TKey key, ReconcileRequest request, CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(request);
        return Store.RequestAsync(transaction, source, key, request, cancellationToken);
    }

    /// <summary>
    /// Requests runs of the latest revisions of many records of one source in the caller's transaction, each as
    /// <see cref="RequestAsync{TKey}(WorkTransaction, ReconcileSource{TKey}, TKey, CancellationToken)"/> would; returns
    /// the keys of the records that exist.
    /// </summary>
    public ValueTask<IReadOnlyList<TKey>> RequestManyAsync<TKey>(WorkTransaction transaction, ReconcileSource<TKey> source, IEnumerable<TKey> keys, CancellationToken cancellationToken = default)
        where TKey : notnull => RequestManyAsync(transaction, source, keys, ReconcileRequest.Default, cancellationToken);

    /// <summary>
    /// Requests runs of the latest revisions of many records of one source in the caller's transaction, each as
    /// <see cref="RequestAsync{TKey}(WorkTransaction, ReconcileSource{TKey}, TKey, ReconcileRequest, CancellationToken)"/>
    /// would, and signals claimers once. Duplicate keys request a record once. Records are locked in ordinal order of
    /// their canonical keys, so bulk requests of overlapping records do not deadlock each other; a transaction that
    /// requests several records of one source requests them in one call. Returns the keys of the records that exist,
    /// in that order.
    /// </summary>
    public ValueTask<IReadOnlyList<TKey>> RequestManyAsync<TKey>(WorkTransaction transaction, ReconcileSource<TKey> source, IEnumerable<TKey> keys, ReconcileRequest request, CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentNullException.ThrowIfNull(request);
        TKey[] ordered = [.. keys
            .Select(key => (Key: key, Text: source.Key.Format(key)))
            .DistinctBy(k => k.Text, StringComparer.Ordinal)
            .OrderBy(k => k.Text, StringComparer.Ordinal)
            .Select(k => k.Key)];
        return ordered.Length == 0
            ? ValueTask.FromResult<IReadOnlyList<TKey>>([])
            : Store.RequestManyAsync(transaction, source, ordered, request, cancellationToken);
    }

    /// <inheritdoc cref="IReconcileStore.FenceAsync"/>
    public ValueTask FenceAsync(WorkTransaction transaction, IEnumerable<IReconcileSource> sources, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sources);
        return Store.FenceAsync(transaction, [.. sources], cancellationToken);
    }

    /// <inheritdoc cref="IReconcileStore.FenceAsync"/>
    public ValueTask FenceAsync(WorkTransaction transaction, IReconcileSource source, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        return Store.FenceAsync(transaction, [source], cancellationToken);
    }

    /// <inheritdoc cref="IReconcileStore.InvalidateAllAsync{TKey}"/>
    public ValueTask<long> InvalidateAllAsync<TKey>(WorkTransaction transaction, ReconcileSource<TKey> source, CancellationToken cancellationToken = default)
        where TKey : notnull => Store.InvalidateAllAsync(transaction, source, cancellationToken);

    /// <summary>
    /// Applies a run's result in the caller's transaction, guarded like every holder write, so the domain's outcome
    /// and the record's work state commit together.
    /// </summary>
    /// <exception cref="LeaseLostException">The lease is no longer valid or was superseded; the transaction must not commit.</exception>
    public ValueTask CompleteAsync<TKey>(Lease<TKey> lease, WorkTransaction transaction, ReconcileResult result, CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(result);
        return Store.CompleteAsync(transaction, Reconciled(lease), result, cancellationToken);
    }

    /// <summary>Applies a run's result in the store's own transaction; false if the lease was already lost.</summary>
    public ValueTask<bool> CompleteAsync<TKey>(Lease<TKey> lease, ReconcileResult result, CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(result);
        return Store.CompleteAsync(Reconciled(lease), result, cancellationToken);
    }

    private static Lease<TKey> Reconciled<TKey>(Lease<TKey> lease)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(lease);
        return lease.Source.Reconcile is null
            ? throw new ArgumentException($"Lease source {lease.Source.Name} is not a reconciliation source.", nameof(lease))
            : lease;
    }
}
