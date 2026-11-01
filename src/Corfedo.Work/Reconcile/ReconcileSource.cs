namespace Corfedo.Work;

/// <summary>What a request does to a record a run holds.</summary>
public enum RequestPolicy
{
    /// <summary>The run continues; its result schedules exactly one follow-up run.</summary>
    FollowUp,

    /// <summary>
    /// The request, and an invalidation, make the running attempt obsolete: its renewals, guards and results fail, and
    /// the record is claimable at once without takeover backoff.
    /// </summary>
    Supersede,
}

/// <summary>How runs of a reconciliation source are leased, how requests during a run behave, and when converged records run again.</summary>
public sealed record ReconcilePolicy
{
    /// <summary>
    /// Lease duration, backoffs and limits. <see cref="LeasePolicy.FailureBackoff"/> is the backoff of failed runs;
    /// <see cref="LeasePolicy.MaximumTakeovers"/> is not used, since reconciliation never gives up.
    /// </summary>
    public required LeasePolicy Lease { get; init; }

    public RequestPolicy Requests { get; init; } = RequestPolicy.FollowUp;

    /// <summary>How long after converging a record without pending requests it is evaluated again; null for never.</summary>
    public TimeSpan? ResyncInterval { get; init; }
}

/// <summary>The names of the reconciliation fields on a source's records.</summary>
public sealed record ReconcileFields
{
    public static ReconcileFields Default { get; } = new();

    public string RequestedRevision { get; init; } = "requested_revision";

    public string ClaimedRevision { get; init; } = "claimed_revision";

    public string ConvergedRevision { get; init; } = "converged_revision";

    public string FirstRequestedAt { get; init; } = "first_requested_at";

    public string NotBefore { get; init; } = "not_before";

    public string AttemptedGeneration { get; init; } = "attempted_generation";

    public string ConvergedGeneration { get; init; } = "converged_generation";
}

/// <summary>The reconciliation part of a lease source: its policy and fields.</summary>
public sealed record ReconcileSettings(ReconcilePolicy Policy, ReconcileFields Fields)
{
    public bool Supersedes => Policy.Requests == RequestPolicy.Supersede;
}

/// <summary>A reconciliation source without its key type, for operations over several sources.</summary>
public interface IReconcileSource
{
    string Name { get; }

    ReconcilePolicy Policy { get; }
}

/// <summary>
/// A kind of record that states what it should be: a request increments its requested revision and a run converges
/// it. Its records carry the lease fields and the reconciliation fields; <see cref="Leases"/> is the lease source
/// whose claims follow the reconciliation rules.
/// </summary>
public sealed class ReconcileSource<TKey> : IReconcileSource
    where TKey : notnull
{
    internal ReconcileSource(LeaseSource<TKey> leases) => Leases = leases;

    public LeaseSource<TKey> Leases { get; }

    public string Name => Leases.Name;

    public StoreLocation Store => Leases.Store;

    public IKeyCodec<TKey> Key => Leases.Key;

    public string? Partition => Leases.Partition;

    public ReconcilePolicy Policy => Leases.Reconcile!.Policy;

    public ReconcileFields Fields => Leases.Reconcile!.Fields;

    public override string ToString() => Name;
}

public static class ReconcileSource
{
    /// <summary>Defines a reconciliation source.</summary>
    public static ReconcileSource<TKey> Define<TKey>(
        string name,
        StoreLocation store,
        IKeyCodec<TKey> key,
        ReconcilePolicy policy,
        string? partition = null,
        LeaseFields? leaseFields = null,
        ReconcileFields? fields = null)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(policy.Lease);
        if (policy.Lease.MaximumTakeovers is not null)
        {
            throw new ArgumentException("Reconciliation never gives up, so its lease policy cannot set MaximumTakeovers.", nameof(policy));
        }
        if (policy.ResyncInterval is { } resync)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(resync, TimeSpan.Zero, nameof(policy));
        }
        var leases = LeaseSource.Define(name, store, key, policy.Lease, partition, leaseFields, new ReconcileSettings(policy, fields ?? ReconcileFields.Default), null);
        return new ReconcileSource<TKey>(leases);
    }
}
