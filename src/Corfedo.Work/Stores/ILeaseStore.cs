namespace Corfedo.Work;

/// <summary>Runs inside a claim transaction before the claim commits; a failure rolls the claim back.</summary>
public delegate ValueTask LeaseClaimCallback<TKey>(LeaseClaim<TKey> claim, WorkTransaction transaction, CancellationToken cancellationToken)
    where TKey : notnull;

/// <summary>Runs inside the transaction that settles an expired lease; a failure rolls the settlement back.</summary>
public delegate ValueTask LeaseExpiryCallback<TKey>(LeaseExpiry<TKey> expiry, WorkTransaction transaction, CancellationToken cancellationToken)
    where TKey : notnull;

/// <summary>The callbacks a store runs inside its own claim and settlement transactions for one source.</summary>
public sealed record LeaseCallbacks<TKey>(LeaseClaimCallback<TKey>? Claimed, LeaseExpiryCallback<TKey>? Expired)
    where TKey : notnull
{
    public static LeaseCallbacks<TKey> None { get; } = new(null, null);
}

/// <summary>
/// Atomic lease operations of one store. Each operation is one atomic step; every operation that depends on lease
/// validity or claimability locks the record first and then evaluates the condition with the store's clock.
/// </summary>
public interface ILeaseStore
{
    /// <summary>
    /// Claims up to <paramref name="limit"/> claimable records, earliest due first, skipping locked records. A call may
    /// commit claims in several steps; a failure, including cancellation, after a step committed ends the call with
    /// the claims committed so far, so that every committed claim reaches the caller. The result carries the cutoff of
    /// the call's last scan, also when nothing was claimed, for <see cref="NextDueAsync{TKey}"/>.
    /// </summary>
    ValueTask<DueClaims<TKey>> ClaimDueAsync<TKey>(LeaseSource<TKey> source, LeaseCallbacks<TKey> callbacks, OwnerId owner, int limit, CancellationToken cancellationToken)
        where TKey : notnull;

    /// <summary>Claims one record if it is claimable; null otherwise.</summary>
    ValueTask<LeaseClaim<TKey>?> ClaimAsync<TKey>(LeaseSource<TKey> source, LeaseCallbacks<TKey> callbacks, OwnerId owner, TKey key, CancellationToken cancellationToken)
        where TKey : notnull;

    /// <summary>In the caller's transaction, claims up to <paramref name="limit"/> of the candidates that are claimable.</summary>
    ValueTask<IReadOnlyList<LeaseClaim<TKey>>> ClaimAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, LeaseCallbacks<TKey> callbacks, OwnerId owner, IReadOnlyList<TKey> candidates, int limit, CancellationToken cancellationToken)
        where TKey : notnull;

    /// <summary>Extends a valid lease in its own transaction.</summary>
    ValueTask<RenewResult> RenewAsync<TKey>(Lease<TKey> lease, CancellationToken cancellationToken)
        where TKey : notnull;

    /// <summary>
    /// Extends a valid lease in the caller's transaction, which then holds the record's write lock. When
    /// <paramref name="progressed"/> is set, the renewal also resets <c>Takeovers</c>, which counts takeovers without
    /// progress.
    /// </summary>
    ValueTask<RenewResult> RenewAsync<TKey>(WorkTransaction transaction, Lease<TKey> lease, bool progressed, CancellationToken cancellationToken)
        where TKey : notnull;

    /// <summary>Gives a validly held record back, due as given; counts a failure when <paramref name="failed"/>.</summary>
    ValueTask<bool> ReleaseAsync<TKey>(Lease<TKey> lease, Due due, bool failed, CancellationToken cancellationToken)
        where TKey : notnull;

    /// <summary>
    /// In the caller's transaction, clears the lease, the due time and cancellation and records the completion.
    /// Returns <see cref="EndResult.Replayed"/> for a completion already recorded for this lease.
    /// </summary>
    /// <exception cref="LeaseLostException">The lease is not valid, or a different completion was recorded.</exception>
    ValueTask<EndResult> EndAsync<TKey>(WorkTransaction transaction, Lease<TKey> lease, LeaseCompletion? completion, CancellationToken cancellationToken)
        where TKey : notnull;

    /// <summary>
    /// Sets the due time in the caller's transaction and signals claimers on commit. False if the record does not exist.
    /// </summary>
    /// <exception cref="ArgumentException">The source holds reconciliation records, whose due time requests and results set.</exception>
    ValueTask<bool> ScheduleAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, TKey key, Due due, Guid? execution, CancellationToken cancellationToken)
        where TKey : notnull;

    /// <summary>Requests cancellation in the caller's transaction and signals the holder on commit.</summary>
    ValueTask<bool> RequestCancelAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, TKey key, CancellationToken cancellationToken)
        where TKey : notnull;

    /// <summary>Withdraws a cancellation request in the caller's transaction.</summary>
    ValueTask<bool> ClearCancelAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, TKey key, CancellationToken cancellationToken)
        where TKey : notnull;

    /// <summary>
    /// Reads the lease fields of the records of <paramref name="keys"/>, which are distinct, with one reading of the
    /// store's clock: in <paramref name="transaction"/>, or from committed records without one. Takes no locks. Returns
    /// the requested keys of the records that exist, each with its fields; keys of no record are left out.
    /// </summary>
    /// <exception cref="ArgumentException">The source holds jobs, which are read through their status.</exception>
    ValueTask<IReadOnlyList<KeyValuePair<TKey, LeaseRecord>>> ReadAsync<TKey>(WorkTransaction? transaction, LeaseSource<TKey> source, IReadOnlyList<TKey> keys, CancellationToken cancellationToken)
        where TKey : notnull;

    /// <summary>
    /// Settles up to <paramref name="limit"/> expired, unsettled leases, each in its own transaction, skipping locked
    /// records and records whose expiry handler failed earlier in the call.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// The source does not settle expiry, or it settles with an expiry handler and <paramref name="callbacks"/> has none.
    /// </exception>
    ValueTask<int> SettleExpiredAsync<TKey>(LeaseSource<TKey> source, LeaseCallbacks<TKey> callbacks, int limit, CancellationToken cancellationToken)
        where TKey : notnull;

    /// <summary>
    /// After a due claim with <paramref name="cutoff"/>, the time until a record of the source that claim could not
    /// consider may become claimable, no later than the earliest such time, or null if none is known. Never negative:
    /// zero means such a record may already be claimable, and the caller claims again. Records the claim considered but
    /// did not claim (locked, or held back by limits) are left out and found by signals and polling, so a caller that
    /// claims again with each new cutoff does not repeat passes over them.
    /// </summary>
    ValueTask<TimeSpan?> NextDueAsync<TKey>(LeaseSource<TKey> source, DateTimeOffset cutoff, CancellationToken cancellationToken)
        where TKey : notnull;

    /// <summary>Time until the earliest outstanding lease of the source expires unsettled, or null if none.</summary>
    ValueTask<TimeSpan?> NextExpiryAsync<TKey>(LeaseSource<TKey> source, CancellationToken cancellationToken)
        where TKey : notnull;

    /// <summary>Verifies that the source's records carry its fields and the indexes its statements need.</summary>
    /// <exception cref="InvalidOperationException">A field or index is missing or has the wrong type.</exception>
    ValueTask VerifyAsync<TKey>(LeaseSource<TKey> source, CancellationToken cancellationToken)
        where TKey : notnull;
}
