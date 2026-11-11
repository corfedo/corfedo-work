namespace Corfedo.Work.InMemory.Jobs;

internal sealed partial class InMemoryJobStore
{
    /// <summary>
    /// Stores the definition if its revision is newer than the stored one, with the policy resolved with the host's
    /// defaults. The pause state is kept. Its transaction serializes with every claim, so a claim
    /// commits under the revision it read.
    /// </summary>
    public async ValueTask<StoredJobDefinition> StoreDefinitionAsync(JobDefinition definition, JobPolicy policy, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(policy.RunHistory, nameof(policy));
        ArgumentNullException.ThrowIfNull(policy.Retention, nameof(policy));
        ArgumentNullException.ThrowIfNull(policy.RunHistoryRetention, nameof(policy));
        using var transaction = await database.BeginAsync(cancellationToken);
        var stored = Stored(transaction, definition.Key);
        if (stored is not null && stored.Revision > definition.Revision)
        {
            return stored;
        }
        if (stored is not null && stored.Revision == definition.Revision)
        {
            if (!stored.Digest.AsSpan().SequenceEqual(definition.Digest))
            {
                throw new JobDefinitionConflictException(
                    $"Job definition {definition.Key} revision {definition.Revision} is stored with different content; increase the revision when the shared policy changes.");
            }
            return stored;
        }
        var written = new StoredJobDefinition(definition.Key, definition.Revision, definition.Subject, policy, [.. definition.Digest], stored?.Paused ?? false);
        var row = transaction.Get(application.Definitions, definition.Key) is { } existing
            ? existing with { Stored = written, StoredAt = Now }
            : new Database.DefinitionRow(written, Now);
        transaction.Put(application.Definitions, definition.Key, row);
        transaction.Commit();
        return written;
    }

    public ValueTask<StoredJobDefinition?> GetDefinitionAsync(string key, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(key);
        return ValueTask.FromResult(database.ReadCommitted(() => application.Definitions.Committed.GetValueOrDefault(key)?.Stored));
    }
}
