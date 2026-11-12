namespace Corfedo.Work;

/// <summary>
/// Reads of reconciliation records: their lease and reconciliation fields and the source's generation with the store's
/// clock, for diagnostics and for tests against a database whose clock they cannot move. Reads take no locks; a record
/// can change right after it was read.
/// </summary>
public sealed partial class ReconcileOperations
{
    /// <summary>The most records one <see cref="ReadManyAsync{TKey}(ReconcileSource{TKey}, IEnumerable{TKey}, CancellationToken)"/> reads.</summary>
    public const int MaximumKeys = RecordKeys.Maximum;

    /// <summary>The record's fields as committed, or null when the source has no record with the key.</summary>
    public async ValueTask<ReconcileRecord?> ReadAsync<TKey>(ReconcileSource<TKey> source, TKey key, CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        return Single(await Store.ReadAsync(null, source, [key], cancellationToken));
    }

    /// <summary>The record's fields as the caller's transaction sees them, or null when the source has no record with the key.</summary>
    public async ValueTask<ReconcileRecord?> ReadAsync<TKey>(WorkTransaction transaction, ReconcileSource<TKey> source, TKey key, CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(source);
        return Single(await Store.ReadAsync(transaction, source, [key], cancellationToken));
    }

    /// <summary>
    /// The fields of up to <see cref="MaximumKeys"/> records as committed, in one read with one clock reading; keys of
    /// no record are left out.
    /// </summary>
    public async ValueTask<IReadOnlyDictionary<TKey, ReconcileRecord>> ReadManyAsync<TKey>(ReconcileSource<TKey> source, IEnumerable<TKey> keys, CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        return RecordKeys.Dictionary(await Store.ReadAsync(null, source, RecordKeys.Distinct(source.Leases, keys), cancellationToken));
    }

    /// <summary>
    /// The fields of up to <see cref="MaximumKeys"/> records as the caller's transaction sees them, in one read with
    /// one clock reading; keys of no record are left out.
    /// </summary>
    public async ValueTask<IReadOnlyDictionary<TKey, ReconcileRecord>> ReadManyAsync<TKey>(WorkTransaction transaction, ReconcileSource<TKey> source, IEnumerable<TKey> keys, CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(source);
        return RecordKeys.Dictionary(await Store.ReadAsync(transaction, source, RecordKeys.Distinct(source.Leases, keys), cancellationToken));
    }

    private static ReconcileRecord? Single<TKey>(IReadOnlyList<KeyValuePair<TKey, ReconcileRecord>> records)
        where TKey : notnull => records.Count == 0 ? null : records[0].Value;
}
