using Corfedo.Work.InMemory.Database;
using Corfedo.Work.InMemory.Leases;
using static Corfedo.Work.InMemory.Leases.InMemoryLeaseStore;

namespace Corfedo.Work.InMemory.Reconcile;

/// <summary>
/// Requests, results and invalidation of reconciliation sources, on the rules of their lease sources. Transactions are
/// serialized, so generation fences hold by construction: an invalidation never interleaves with a guarded transaction.
/// </summary>
internal sealed class InMemoryReconcileStore(InMemoryLeaseStore leases) : IReconcileStore
{
    /// <summary>
    /// Increments the requested revision and schedules the record. The due time is the trailing debounce from now,
    /// capped at the first request not yet claimed plus the maximum; a waiting record keeps an earlier due time; a
    /// backoff floor holds unless the request resets it. While a run holds the record, the
    /// due time is recorded for the run's result to schedule the follow-up; on superseding sources the request instead
    /// gives the run's lease back, and the holder's process is signalled to renew and observe it.
    /// </summary>
    public ValueTask<bool> RequestAsync<TKey>(IWorkTransaction transaction, ReconcileSource<TKey> source, TKey key, ReconcileRequest request, CancellationToken cancellationToken)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(request);
        var work = leases.Join(transaction);
        var rules = leases.Rules(source.Leases);
        var f = rules.Fields;
        var r = rules.Reconcile!;
        var rowKey = rules.RowKey(key);
        if (rules.Find(work, rowKey) is not { } record)
        {
            return ValueTask.FromResult(false);
        }
        var now = leases.Now;
        var held = rules.Expires(record) > now;
        var firstRequested = record.Time(r.FirstRequestedAt);
        var first = firstRequested ?? now;
        var debounce = request.Debounce;
        var debounced = now + (debounce?.Delay ?? TimeSpan.Zero);
        if (debounce?.Maximum is { } maximum && first + maximum < debounced)
        {
            debounced = first + maximum;
        }
        var due = rules.Due(record);
        var candidate = firstRequested is not null || held || due is not { } current || current > debounced ? debounced : current;
        var notBefore = record.Time(r.NotBefore);
        var dueAt = !request.ResetBackoff && notBefore is { } floor && floor > candidate ? floor
            : candidate;
        var changes = new List<KeyValuePair<string, object?>>
        {
            Set(r.RequestedRevision, record.Int64(r.RequestedRevision) + 1),
            Set(r.FirstRequestedAt, first),
            Set(f.DueAt, dueAt),
        };
        if (request.ResetBackoff)
        {
            changes.Add(Set(r.NotBefore, null));
            changes.Add(Set(f.Failures, 0));
        }
        var expires = rules.Expires(record);
        if (rules.Supersedes && held)
        {
            changes.Add(Set(f.LeaseExpiresAt, null));
            expires = null;
        }
        leases.Write(work, rules, rowKey, record, changes);
        if (expires is null && LeaseRules<TKey>.Wakes(dueAt))
        {
            leases.Signal(work, WorkTopics.Due, source.Name);
        }
        if (rules.Supersedes && held && record.Uuid(f.LeaseOwner) is { } owner)
        {
            leases.Signal(work, WorkTopics.Cancel, WorkTopics.CancelKey(new OwnerId(owner), source.Name));
        }
        return ValueTask.FromResult(true);
    }

    public ValueTask CompleteAsync<TKey>(IWorkTransaction transaction, Lease<TKey> lease, ReconcileResult result, CancellationToken cancellationToken)
        where TKey : notnull
    {
        if (!Complete(leases.Join(transaction), lease, result))
        {
            var source = lease.Source;
            throw new LeaseLostException($"The lease on {source.Name} record {source.Key.Format(lease.Key)} at epoch {lease.Epoch} is no longer valid.");
        }
        return ValueTask.CompletedTask;
    }

    public async ValueTask<bool> CompleteAsync<TKey>(Lease<TKey> lease, ReconcileResult result, CancellationToken cancellationToken)
        where TKey : notnull
    {
        using var transaction = await leases.Database.BeginAsync(cancellationToken);
        if (!Complete(transaction, lease, result))
        {
            return false;
        }
        transaction.Commit();
        return true;
    }

    /// <summary>
    /// Applies the result if the lease is still valid and gives the record back. Converged advances the converged
    /// revision and generation to what the run claimed and resets failures; the record is due at the time a request
    /// during the run recorded, after the resync interval, or not at all. Waiting is due at the retry time or the
    /// recorded request, whichever is first. Failed counts a failure and moves the backoff floor and the due time by the
    /// failure backoff.
    /// </summary>
    private bool Complete<TKey>(InMemoryTransaction transaction, Lease<TKey> lease, ReconcileResult result)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(result);
        var source = lease.Source;
        var rules = leases.Rules(source);
        var f = rules.Fields;
        var r = rules.Reconcile!;
        var rowKey = rules.RowKey(lease.Key);
        var now = leases.Now;
        if (rules.Find(transaction, rowKey) is not { } record
            || !rules.Valid(record, lease.Owner, lease.Epoch, now, rules.Generation(transaction, leases.Application)))
        {
            return false;
        }
        var due = rules.Due(record);
        var claimed = record[r.ClaimedRevision] is null ? (long?)null : record.Int64(r.ClaimedRevision);
        var pending = record.Int64(r.RequestedRevision) > claimed;
        var changes = new List<KeyValuePair<string, object?>> { Set(f.LeaseExpiresAt, null), Set(f.Takeovers, 0) };
        DateTimeOffset? dueAt;
        switch (result.Outcome)
        {
            case ReconcileOutcome.Converged:
                dueAt = pending ? due
                    : source.Reconcile!.Policy.ResyncInterval is { } resync ? now + resync
                    : null;
                changes.Add(Set(r.ConvergedRevision, claimed));
                changes.Add(Set(r.ConvergedGeneration, record.Int64(r.AttemptedGeneration)));
                changes.Add(Set(f.Failures, 0));
                changes.Add(Set(r.NotBefore, null));
                break;
            case ReconcileOutcome.Waiting:
                var retry = now + result.RetryAfter;
                dueAt = pending && due < retry ? due : retry;
                break;
            default:
                var floor = now + rules.FailureBackoff(record);
                dueAt = floor;
                changes.Add(Set(f.Failures, record.Int32(f.Failures) + 1));
                changes.Add(Set(r.NotBefore, floor));
                break;
        }
        changes.Add(Set(f.DueAt, dueAt));
        leases.Write(transaction, rules, rowKey, record, changes);
        // A completed record is claimable again or frees a slot of the source's limits.
        if (rules.Policy.HasStoreLimits || LeaseRules<TKey>.Wakes(dueAt))
        {
            leases.Signal(transaction, WorkTopics.Due, source.Name);
        }
        return true;
    }

    /// <summary>Validates the transaction; serialized transactions need no fence.</summary>
    public ValueTask FenceAsync(IWorkTransaction transaction, IReadOnlyCollection<IReconcileSource> sources, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sources);
        leases.Join(transaction);
        return ValueTask.CompletedTask;
    }

    /// <summary>Increments the source's generation in the caller's transaction and signals claimers on commit.</summary>
    public ValueTask<long> InvalidateAllAsync<TKey>(IWorkTransaction transaction, ReconcileSource<TKey> source, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = leases.Join(transaction);
        var generations = leases.Application.Generations;
        var generation = (work.Get(generations, source.Name)?.Value ?? 0) + 1;
        work.Put(generations, source.Name, new Generation(generation));
        leases.Signal(work, WorkTopics.Due, source.Name);
        return ValueTask.FromResult(generation);
    }
}
