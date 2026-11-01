namespace Corfedo.Work;

/// <summary>Domain bookkeeping that commits with a claim, inside the claim transaction. No external calls.</summary>
public interface ILeaseClaimHook<TKey>
    where TKey : notnull
{
    ValueTask ClaimedAsync(LeaseClaim<TKey> claim, WorkTransaction transaction, CancellationToken cancellationToken);
}

/// <summary>Releases resources tied to an expired lease, inside the settlement transaction.</summary>
public interface ILeaseExpiryHandler<TKey>
    where TKey : notnull
{
    ValueTask ExpiredAsync(LeaseExpiry<TKey> expiry, WorkTransaction transaction, CancellationToken cancellationToken);
}

/// <summary>Runs claimed records of a source.</summary>
public interface ILeaseHandler<TKey>
    where TKey : notnull
{
    /// <summary>Runs one claim. The handler ends or releases the record before returning.</summary>
    ValueTask RunAsync(LeaseRun<TKey> run, CancellationToken cancellationToken);

    /// <summary>
    /// Decides a record whose claim exceeded the source's maximum takeovers: ends it, or releases it with a later due
    /// time, recording why in the domain's own fields.
    /// </summary>
    ValueTask ExhaustedAsync(LeaseRun<TKey> run, CancellationToken cancellationToken);
}
