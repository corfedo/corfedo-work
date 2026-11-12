namespace Corfedo.Work;

/// <summary>
/// Reads of lease records: their lease fields with the store's clock, for diagnostics and for tests against a database
/// whose clock they cannot move. Reads take no locks and run no hooks; a record can change right after it was read.
/// Jobs are read through <see cref="JobOperations"/>.
/// </summary>
public sealed partial class LeaseOperations
{
    /// <summary>The most records one <see cref="ReadManyAsync{TKey}(LeaseSource{TKey}, IEnumerable{TKey}, CancellationToken)"/> reads.</summary>
    public const int MaximumKeys = RecordKeys.Maximum;

    /// <summary>The record's lease fields as committed, or null when the source has no record with the key.</summary>
    public async ValueTask<LeaseRecord?> ReadAsync<TKey>(LeaseSource<TKey> source, TKey key, CancellationToken cancellationToken = default)
        where TKey : notnull => Single(await Store.ReadAsync(null, Readable(source), [key], cancellationToken));

    /// <summary>The record's lease fields as the caller's transaction sees them, or null when the source has no record with the key.</summary>
    public async ValueTask<LeaseRecord?> ReadAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, TKey key, CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(transaction);
        return Single(await Store.ReadAsync(transaction, Readable(source), [key], cancellationToken));
    }

    /// <summary>
    /// The lease fields of up to <see cref="MaximumKeys"/> records as committed, in one read with one clock reading;
    /// keys of no record are left out.
    /// </summary>
    public async ValueTask<IReadOnlyDictionary<TKey, LeaseRecord>> ReadManyAsync<TKey>(LeaseSource<TKey> source, IEnumerable<TKey> keys, CancellationToken cancellationToken = default)
        where TKey : notnull => RecordKeys.Dictionary(await Store.ReadAsync(null, Readable(source), RecordKeys.Distinct(source, keys), cancellationToken));

    /// <summary>
    /// The lease fields of up to <see cref="MaximumKeys"/> records as the caller's transaction sees them, in one read
    /// with one clock reading; keys of no record are left out.
    /// </summary>
    public async ValueTask<IReadOnlyDictionary<TKey, LeaseRecord>> ReadManyAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, IEnumerable<TKey> keys, CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(transaction);
        return RecordKeys.Dictionary(await Store.ReadAsync(transaction, Readable(source), RecordKeys.Distinct(source, keys), cancellationToken));
    }

    private static LeaseSource<TKey> Readable<TKey>(LeaseSource<TKey> source)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        return source.Job is { } job
            ? throw new ArgumentException($"Lease source {source.Name} holds jobs of definition {job.Definition}; jobs are read through JobOperations.", nameof(source))
            : source;
    }

    private static LeaseRecord? Single<TKey>(IReadOnlyList<KeyValuePair<TKey, LeaseRecord>> records)
        where TKey : notnull => records.Count == 0 ? null : records[0].Value;
}
