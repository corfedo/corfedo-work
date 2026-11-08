using Corfedo.Work.InMemory.Database;
using Corfedo.Work.InMemory.Jobs;

namespace Corfedo.Work.InMemory.Leases;

internal sealed partial class InMemoryLeaseStore
{
    public async ValueTask<RenewResult> RenewAsync<TKey>(Lease<TKey> lease, CancellationToken cancellationToken)
        where TKey : notnull
    {
        using var transaction = await database.BeginAsync(cancellationToken);
        var result = Renew(transaction, lease, false);
        transaction.Commit();
        return result;
    }

    public ValueTask<RenewResult> RenewAsync<TKey>(WorkTransaction transaction, Lease<TKey> lease, bool progressed, CancellationToken cancellationToken)
        where TKey : notnull => ValueTask.FromResult(Renew(Join(transaction), lease, progressed));

    /// <summary>Extends the lease if it is still valid; a renewal that marks progress also resets the takeovers.</summary>
    private RenewResult Renew<TKey>(InMemoryTransaction transaction, Lease<TKey> lease, bool progressed)
        where TKey : notnull
    {
        var rules = Rules(lease.Source);
        var key = rules.RowKey(lease.Key);
        var now = Now;
        if (rules.Find(transaction, key) is not { } r || !rules.Valid(r, lease.Owner, lease.Epoch, now, rules.Generation(transaction, application)))
        {
            return RenewResult.Lost;
        }
        Write(transaction, rules, key, r, progressed
            ? [Set(rules.Fields.LeaseExpiresAt, now + rules.Policy.Duration), Set(rules.Fields.Takeovers, 0)]
            : [Set(rules.Fields.LeaseExpiresAt, now + rules.Policy.Duration)]);
        return new RenewResult(true, r.Time(rules.Fields.CancelRequestedAt) is not null, rules.Policy.Duration);
    }

    public async ValueTask<bool> ReleaseAsync<TKey>(Lease<TKey> lease, Due due, bool failed, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var rules = Rules(lease.Source);
        var f = rules.Fields;
        var key = rules.RowKey(lease.Key);
        using var transaction = await database.BeginAsync(cancellationToken);
        var now = Now;
        if (rules.Find(transaction, key) is not { } r || !rules.Valid(r, lease.Owner, lease.Epoch, now, rules.Generation(transaction, application)))
        {
            return false;
        }
        var dueAt = LeaseRules<TKey>.DueAt(due, rules.Due(r), now);
        var changes = new List<KeyValuePair<string, object?>>
        {
            Set(f.LeaseExpiresAt, null),
            Set(f.DueAt, dueAt),
            Set(f.Takeovers, 0),
            Set(f.Failures, failed ? r.Int32(f.Failures) + 1 : 0),
        };
        if (rules.Job is not null)
        {
            // The released attempt ends in the run history: a graceful release returns the attempt that claim used and is
            // Released, keeping the execution started; a failed one follows a lost hold and is LeaseLost.
            var ownAttempt = r.Int64(JobColumns.AttemptEpoch) == lease.Epoch;
            if (ownAttempt)
            {
                JobRuns.Record(transaction, application, r, failed ? JobRunOutcome.LeaseLost : JobRunOutcome.Released, null, now);
            }
            changes.Add(Set(JobColumns.Attempts, !failed && ownAttempt ? r.Int32(JobColumns.Attempts) - 1 : r.Int32(JobColumns.Attempts)));
            changes.Add(Set(JobColumns.AttemptStartedAt, null));
        }
        Write(transaction, rules, key, r, changes);
        if (rules.Policy.HasStoreLimits || LeaseRules<TKey>.Wakes(dueAt))
        {
            Signal(transaction, WorkTopics.Due, rules.Source.Name);
        }
        transaction.Commit();
        return true;
    }

    public ValueTask<EndResult> EndAsync<TKey>(WorkTransaction transaction, Lease<TKey> lease, LeaseCompletion? completion, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = Join(transaction);
        var source = lease.Source;
        var rules = Rules(source);
        var f = rules.Fields;
        var key = rules.RowKey(lease.Key);
        var r = rules.Find(work, key);
        if (r is not null && rules.Valid(r, lease.Owner, lease.Epoch, Now, rules.Generation(work, application)))
        {
            Write(work, rules, key, r, [
                Set(f.LeaseExpiresAt, null),
                Set(f.DueAt, null),
                Set(f.CancelRequestedAt, null),
                Set(f.Takeovers, 0),
                Set(f.Failures, 0),
                Set(f.EndedCompletionId, completion?.Id),
                Set(f.EndedDigest, completion?.Digest.ToArray()),
            ]);
            if (source.Policy.HasStoreLimits)
            {
                Signal(work, WorkTopics.Due, source.Name);
            }
            return ValueTask.FromResult(EndResult.Ended);
        }
        if (completion is not null
            && r is not null
            && r.Uuid(f.LeaseOwner) == lease.Owner.Value
            && r.Int64(f.LeaseEpoch) == lease.Epoch
            && r.Uuid(f.EndedCompletionId) == completion.Id)
        {
            if (r[f.EndedDigest] is byte[] digest && digest.AsSpan().SequenceEqual(completion.Digest))
            {
                return ValueTask.FromResult(EndResult.Replayed);
            }
            throw new LeaseLostException($"A different completion {completion.Id} was already recorded for {source.Name} record {source.Key.Format(lease.Key)}.");
        }
        throw new LeaseLostException($"The lease on {source.Name} record {source.Key.Format(lease.Key)} at epoch {lease.Epoch} is no longer valid.");
    }
}
