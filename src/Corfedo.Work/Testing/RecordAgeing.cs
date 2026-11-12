namespace Corfedo.Work.Testing;

/// <summary>
/// Ages one record for tests against a store that takes its time from the database's clock, the per-record counterpart
/// of advancing the in-memory store's clock: ageing a record by a span makes it look to every operation as if the span
/// had passed for it. Ageing by the lease duration plus the takeover backoff makes a held record claimable by a
/// takeover, ageing by the time left until its due time makes it due, and a negative span postpones it. Records of
/// other sources and the domain's own fields keep their times.
/// </summary>
/// <remarks>It writes fields the framework owns outside of any rule; production code must not use it.</remarks>
public sealed class RecordAgeing(IWorkStore store)
{
    private ILeaseAgeing Ageing => store.Leases as ILeaseAgeing
        ?? throw new NotSupportedException($"The store's lease store, {store.Leases.GetType().Name}, does not age records.");

    /// <inheritdoc cref="ILeaseAgeing.AgeAsync{TKey}"/>
    public ValueTask<bool> AgeAsync<TKey>(LeaseSource<TKey> source, TKey key, TimeSpan by, CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        return source.Job is { } job
            ? throw new ArgumentException($"Lease source {source.Name} holds jobs of definition {job.Definition}; jobs are not aged.", nameof(source))
            : Ageing.AgeAsync(source, key, by, cancellationToken);
    }

    /// <inheritdoc cref="ILeaseAgeing.AgeAsync{TKey}"/>
    public ValueTask<bool> AgeAsync<TKey>(ReconcileSource<TKey> source, TKey key, TimeSpan by, CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        return AgeAsync(source.Leases, key, by, cancellationToken);
    }
}
