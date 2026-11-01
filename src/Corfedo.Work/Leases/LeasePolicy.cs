namespace Corfedo.Work;

/// <summary>How long leases of a source last, how takeovers and failures back off, and the store-enforced limits.</summary>
public sealed record LeasePolicy
{
    /// <summary>How long a claim or renewal keeps the lease valid.</summary>
    public required TimeSpan Duration { get; init; }

    /// <summary>Takeovers after which a claim is marked exhausted; null for no maximum.</summary>
    public int? MaximumTakeovers { get; init; }

    /// <summary>Delay after an expired lease before the record can be taken over, by the record's earlier takeovers.</summary>
    public Backoff TakeoverBackoff { get; init; } = Backoff.None;

    /// <summary>Delay before a record released after a failed attempt is due again, by the record's earlier failures.</summary>
    public Backoff FailureBackoff { get; init; } = Backoff.None;

    /// <summary>
    /// Whether expired leases hold resources that an expiry handler releases. Every process sees the same value, so a
    /// claimer without the handler leaves an unsettled expired lease to the processes that run it.
    /// </summary>
    public bool SettleExpiry { get; init; }

    /// <summary>
    /// Delay before an expiry whose handler failed is settled again, by the expired lease's earlier settlement
    /// failures. Until then neither the expiry processor nor a claim retries it.
    /// </summary>
    public Backoff SettlementBackoff { get; init; } = Backoff.Exponential(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(5));

    /// <summary>Outstanding leases of one partition across all holders; null for no limit.</summary>
    public int? PartitionLimit { get; init; }

    /// <summary>Outstanding leases of one partition held by one owner; null for no limit.</summary>
    public int? SpreadLimit { get; init; }

    /// <summary>Outstanding leases of the source held by one owner, local or remote; null for no limit.</summary>
    public int? OwnerLimit { get; init; }

    /// <summary>
    /// Outstanding leases of the whole source across all holders; null for no limit. Set on job sources from the
    /// stored definition limit.
    /// </summary>
    public int? SourceLimit { get; init; }

    /// <summary>Whether any limit is enforced by the store.</summary>
    public bool HasStoreLimits => PartitionLimit is not null || SpreadLimit is not null || OwnerLimit is not null || SourceLimit is not null;
}
