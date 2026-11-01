namespace Corfedo.Work;

/// <summary>The identity of one claim: the record, the holder and the epoch that fences its writes.</summary>
public sealed record Lease<TKey>(LeaseSource<TKey> Source, TKey Key, OwnerId Owner, long Epoch)
    where TKey : notnull
{
    /// <summary>
    /// An opaque string carrying the source, canonical key, owner and epoch, for holders outside the process. It is a
    /// fence, not a credential: the application authenticates the holder and the store still checks validity.
    /// </summary>
    public string Token => LeaseToken.Encode(Source.Name, Source.Key.Format(Key), Owner, Epoch);
}

/// <summary>Why a record was claimed.</summary>
public enum ClaimReason
{
    /// <summary>Claimed as a due record.</summary>
    Due,

    /// <summary>Claimed by its key or as a candidate in the caller's transaction.</summary>
    ByKey,

    /// <summary>The previous lease expired without being given back.</summary>
    Takeover,
}

/// <summary>A successful claim and the record's lease state at that moment.</summary>
public sealed record LeaseClaim<TKey>
    where TKey : notnull
{
    public required Lease<TKey> Lease { get; init; }

    public TKey Key => Lease.Key;

    public required ClaimReason Reason { get; init; }

    /// <summary>The previous holder, if the record was held before.</summary>
    public required OwnerId? PreviousOwner { get; init; }

    public required long PreviousEpoch { get; init; }

    /// <summary>Consecutive takeovers, including this claim if it is one.</summary>
    public required int Takeovers { get; init; }

    /// <summary>Consecutive releases after failed attempts.</summary>
    public required int Failures { get; init; }

    /// <summary>The claim exceeded the source's maximum takeovers; the exhaustion handler decides the outcome.</summary>
    public required bool Exhausted { get; init; }

    public required bool CancelRequested { get; init; }

    /// <summary>The current deliberate execution, stable across its attempts.</summary>
    public required Guid ExecutionId { get; init; }

    /// <summary>Unique for this claim.</summary>
    public required Guid AttemptId { get; init; }

    /// <summary>The partition value in its text form, or null.</summary>
    public required string? Partition { get; init; }

    /// <summary>How long the lease was valid for when the claim committed, by the store's clock.</summary>
    public required TimeSpan ValidFor { get; init; }

    /// <summary>On reconciliation sources, the revision and generation the claim started from; null otherwise.</summary>
    public ReconcileAttempt? Reconcile { get; init; }

    /// <summary>On job sources, how the claim found the job, its attempt and its arguments; null otherwise.</summary>
    public JobClaim? Job { get; init; }
}

/// <summary>An expired lease whose expiry is being settled.</summary>
public sealed record LeaseExpiry<TKey>(LeaseSource<TKey> Source, TKey Key, OwnerId Owner, long Epoch, string? Partition)
    where TKey : notnull;

/// <summary>The outcome of a renewal.</summary>
public readonly record struct RenewResult(bool Valid, bool CancelRequested, TimeSpan ValidFor)
{
    public static RenewResult Lost { get; } = new(false, false, TimeSpan.Zero);
}

/// <summary>Identity and payload digest of the transition that ends a record, for replayed completions.</summary>
public sealed record LeaseCompletion(Guid Id, byte[] Digest);

/// <summary>The outcome of ending a record.</summary>
public enum EndResult
{
    /// <summary>The lease was valid and the record ended.</summary>
    Ended,

    /// <summary>The same completion was already recorded for this lease; nothing changed.</summary>
    Replayed,
}

/// <summary>An operation that needs a valid lease found it lost, or a replayed completion did not match.</summary>
public sealed class LeaseLostException(string message) : Exception(message);

/// <summary>
/// The claims of one due claim and its cutoff: the store-clock instant that bounded its scans. A record due or
/// claimable by takeover at or before the cutoff was considered; one it did not claim is locked or held back by limits.
/// </summary>
public sealed record DueClaims<TKey>(IReadOnlyList<LeaseClaim<TKey>> Claims, DateTimeOffset Cutoff)
    where TKey : notnull;
