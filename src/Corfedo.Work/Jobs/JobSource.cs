namespace Corfedo.Work;

/// <summary>
/// The job part of a definition's lease source: the stored revision its statements are built from, the argument
/// versions this process accepts and the stored policy the claims apply.
/// </summary>
public sealed record JobSourceSettings(string Definition, long Revision, IReadOnlyList<int> AcceptedArgs, JobPolicy Policy);

/// <summary>Lease sources of job definitions on the store's job table.</summary>
public static class JobSource
{
    /// <summary>
    /// The lease source of <paramref name="stored"/>: every statement is scoped to the definition, claims accept only
    /// <paramref name="acceptedArgs"/> not below the stored minimum, and the stored limits apply. Job sources always
    /// settle expiry.
    /// </summary>
    public static LeaseSource<JobId> Define(StoreLocation table, StoredJobDefinition stored, IReadOnlyList<int> acceptedArgs, TimeSpan leaseDuration)
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(stored);
        ArgumentNullException.ThrowIfNull(acceptedArgs);
        if (acceptedArgs.Count == 0 || acceptedArgs.Any(v => v < 1))
        {
            throw new ArgumentException("A registration accepts at least one argument version, each at least 1.", nameof(acceptedArgs));
        }
        var policy = new LeasePolicy
        {
            Duration = leaseDuration,
            PartitionLimit = stored.Policy.PartitionLimit,
            SourceLimit = stored.Policy.DefinitionLimit,
        };
        var settings = new JobSourceSettings(stored.Key, stored.Revision, [.. acceptedArgs.Distinct().Order()], stored.Policy);
        return LeaseSource.Define(JobDefinition.SourcePrefix + stored.Key, table, JobId.Codec, policy, "partition_key", null, null, settings);
    }
}
