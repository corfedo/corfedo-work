namespace Corfedo.Work.Testing;

/// <summary>
/// Ages records of a store whose clock tests cannot move, as <see cref="RecordAgeing"/> describes. A store's
/// <see cref="IWorkStore.Leases"/> implements it next to <see cref="ILeaseStore"/>; it is for tests only.
/// </summary>
public interface ILeaseAgeing
{
    /// <summary>
    /// In a transaction of its own, moves every time field of the record that is set back by <paramref name="by"/>:
    /// <c>DueAt</c>, <c>LeaseExpiresAt</c>, <c>CancelRequestedAt</c>, <c>SettleRetryAt</c> on sources that settle
    /// expiry, and <c>NotBefore</c> and <c>FirstRequestedAt</c> on reconciliation sources. Signals the source's
    /// claimers on commit when the record has a due time. False if the record does not exist.
    /// </summary>
    /// <exception cref="ArgumentException">The source holds jobs.</exception>
    ValueTask<bool> AgeAsync<TKey>(LeaseSource<TKey> source, TKey key, TimeSpan by, CancellationToken cancellationToken)
        where TKey : notnull;
}
