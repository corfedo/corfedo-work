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
        return (await Store.ReadAsync(null, source, [key], cancellationToken))[0];
    }

    /// <summary>The record's fields as the caller's transaction sees them, or null when the source has no record with the key.</summary>
    public async ValueTask<ReconcileRecord?> ReadAsync<TKey>(WorkTransaction transaction, ReconcileSource<TKey> source, TKey key, CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(source);
        return (await Store.ReadAsync(transaction, source, [key], cancellationToken))[0];
    }

    /// <summary>
    /// The fields of up to <see cref="MaximumKeys"/> records as committed, in one read with one clock reading; keys of
    /// no record are left out.
    /// </summary>
    public async ValueTask<IReadOnlyDictionary<TKey, ReconcileRecord>> ReadManyAsync<TKey>(ReconcileSource<TKey> source, IEnumerable<TKey> keys, CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        var distinct = RecordKeys.Distinct(source.Leases, keys);
        return RecordKeys.Dictionary(distinct, await Store.ReadAsync(null, source, distinct, cancellationToken));
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
        var distinct = RecordKeys.Distinct(source.Leases, keys);
        return RecordKeys.Dictionary(distinct, await Store.ReadAsync(transaction, source, distinct, cancellationToken));
    }}
