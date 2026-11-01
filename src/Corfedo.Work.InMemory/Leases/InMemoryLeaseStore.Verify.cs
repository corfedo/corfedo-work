namespace Corfedo.Work.InMemory.Leases;

internal sealed partial class InMemoryLeaseStore
{
    /// <summary>
    /// Checks that the source's table exists, is keyed by the source's key fields in their order, and has the partition,
    /// lease and, on reconciliation sources, reconciliation fields.
    /// </summary>
    public ValueTask VerifyAsync<TKey>(LeaseSource<TKey> source, CancellationToken cancellationToken)
        where TKey : notnull
    {
        Rules(source);
        var table = database.FindTable(source.Store) ?? throw new InvalidOperationException($"Lease source {source.Name}: table {source.Store} does not exist.");
        var problems = new List<string>();
        var key = source.Key.Fields.Select(f => f.Name).ToArray();
        if (!table.Key.SequenceEqual(key, StringComparer.Ordinal))
        {
            problems.Add($"the table is keyed by ({string.Join(", ", table.Key)}), not by ({string.Join(", ", key)})");
        }
        var f = source.Fields;
        var fields = new List<string?>
        {
            source.Partition, f.DueAt, f.LeaseOwner, f.LeaseEpoch, f.LeaseExpiresAt, f.Takeovers, f.Failures, f.CancelRequestedAt,
            f.EndedCompletionId, f.EndedDigest,
        };
        if (source.Reconcile is null)
        {
            fields.Add(f.ExecutionId);
        }
        if (source.Settles)
        {
            fields.Add(f.SettledEpoch);
        }
        if (source.Policy.SettleExpiry)
        {
            fields.AddRange([f.SettleFailures, f.SettleRetryAt]);
        }
        if (source.Reconcile?.Fields is { } r)
        {
            fields.AddRange([r.RequestedRevision, r.ClaimedRevision, r.ConvergedRevision, r.FirstRequestedAt, r.NotBefore, r.AttemptedGeneration, r.ConvergedGeneration]);
        }
        problems.AddRange(fields.OfType<string>().Where(n => !table.Fields.Contains(n)).Select(n => $"field {n} is missing"));
        if (problems.Count > 0)
        {
            throw new InvalidOperationException($"Lease source {source.Name} on {source.Store}: {string.Join("; ", problems)}.");
        }
        return ValueTask.CompletedTask;
    }
}
