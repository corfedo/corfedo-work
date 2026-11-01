namespace Corfedo.Work;

/// <summary>A kind of record that carries lease fields: where it is stored, how its key maps to fields, its policy.</summary>
public sealed class LeaseSource<TKey>
    where TKey : notnull
{
    internal LeaseSource(string name, StoreLocation store, IKeyCodec<TKey> key, string? partition, LeasePolicy policy, LeaseFields fields, ReconcileSettings? reconcile, JobSourceSettings? job)
    {
        Reconcile = reconcile;
        Job = job;
        Name = name;
        Store = store;
        Key = key;
        Partition = partition;
        Policy = policy;
        Fields = fields;
    }

    /// <summary>The source's identity; unique within an application.</summary>
    public string Name { get; }

    public StoreLocation Store { get; }

    public IKeyCodec<TKey> Key { get; }

    /// <summary>The partition field, or null when the source is not partitioned.</summary>
    public string? Partition { get; }

    public LeasePolicy Policy { get; }

    public LeaseFields Fields { get; }

    /// <summary>
    /// The reconciliation policy and fields when the records are reconciled, whose claims then follow the
    /// reconciliation rules; null for a plain lease source.
    /// </summary>
    public ReconcileSettings? Reconcile { get; }

    /// <summary>
    /// The definition, revision and accepted argument versions when the records are jobs of one definition on the
    /// store's job table, whose statements are then scoped to it; null otherwise.
    /// </summary>
    public JobSourceSettings? Job { get; }

    /// <summary>
    /// Whether expired leases are settled: for sources with <see cref="LeasePolicy.SettleExpiry"/> and for sources with
    /// a store-enforced limit, so that a crashed holder stops counting against limits once its expiry is settled, and
    /// for job sources, whose stored limits can change with a revision.
    /// </summary>
    public bool Settles => Policy.SettleExpiry || Policy.HasStoreLimits || Job is not null;

    /// <summary>Restores a lease from a token issued for this source.</summary>
    /// <exception cref="FormatException">The token is malformed or belongs to another source.</exception>
    public Lease<TKey> ParseToken(string token)
    {
        var (source, key, owner, epoch) = LeaseToken.Decode(token);
        if (source != Name)
        {
            throw new FormatException("The lease token belongs to another source.");
        }
        return new Lease<TKey>(this, Key.Parse(key), owner, epoch);
    }

    public override string ToString() => Name;
}

public static class LeaseSource
{
    /// <summary>Defines a lease source.</summary>
    public static LeaseSource<TKey> Define<TKey>(
        string name,
        StoreLocation store,
        IKeyCodec<TKey> key,
        LeasePolicy policy,
        string? partition = null,
        LeaseFields? fields = null)
        where TKey : notnull => Define(name, store, key, policy, partition, fields, null, null);

    internal static LeaseSource<TKey> Define<TKey>(
        string name,
        StoreLocation store,
        IKeyCodec<TKey> key,
        LeasePolicy policy,
        string? partition,
        LeaseFields? fields,
        ReconcileSettings? reconcile,
        JobSourceSettings? job)
        where TKey : notnull
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (name.Any(char.IsControl))
        {
            // Lease tokens separate their parts with a newline.
            throw new ArgumentException("A source name cannot contain control characters.", nameof(name));
        }
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(policy.Duration, TimeSpan.Zero);
        if (key.Fields.Count == 0)
        {
            throw new ArgumentException("A key needs at least one field.", nameof(key));
        }
        if (partition is null && (policy.PartitionLimit is not null || policy.SpreadLimit is not null))
        {
            throw new ArgumentException("Partition and spread limits need a partition field.", nameof(policy));
        }
        if (policy.SourceLimit is not null && job is null)
        {
            throw new ArgumentException("A source-wide limit is available on job sources only.", nameof(policy));
        }
        foreach (var limit in new[] { policy.PartitionLimit, policy.SpreadLimit, policy.OwnerLimit, policy.SourceLimit })
        {
            if (limit is < 1)
            {
                throw new ArgumentException("Limits must be at least 1.", nameof(policy));
            }
        }
        return new LeaseSource<TKey>(name, store, key, partition, policy, fields ?? LeaseFields.Default, reconcile, job);
    }
}
