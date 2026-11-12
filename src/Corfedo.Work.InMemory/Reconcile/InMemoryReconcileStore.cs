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
    public ValueTask<IReadOnlyList<ReconcileRecord?>> ReadAsync<TKey>(WorkTransaction? transaction, ReconcileSource<TKey> source, IReadOnlyList<TKey> keys, CancellationToken cancellationToken)
        where TKey : notnull =>
        ValueTask.FromResult(leases.Read(transaction, source.Leases, keys, static (lease, rules, r, generation) =>
        {
            var f = rules.Reconcile!;
            return new ReconcileRecord(
                lease,
                r.Int64(f.RequestedRevision),
                r[f.ClaimedRevision] is null ? null : r.Int64(f.ClaimedRevision),
                r.Int64(f.ConvergedRevision),
                r.Time(f.FirstRequestedAt),
                r.Time(f.NotBefore),
                r.Int64(f.AttemptedGeneration),
                r.Int64(f.ConvergedGeneration),
                generation);
        }));

    /// <summary>
    /// Increments the requested revision and schedules the record. The due time is the trailing debounce from now,
    /// capped at the first request not yet claimed plus the maximum; a waiting record keeps an earlier due time; a
    /// backoff floor holds unless the request resets it. While a run holds the record, the
    /// due time is recorded for the run's result to schedule the follow-up; on superseding sources the request instead
    /// gives the run's lease back, and the holder's process is signalled to renew and observe it.
    /// </summary>
    public ValueTask<bool> RequestAsync<TKey>(WorkTransaction transaction, ReconcileSource<TKey> source, TKey key, ReconcileRequest request, CancellationToken cancellationToken)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(request);
        var work = leases.Join(transaction);
        var rules = leases.Rules(source.Leases);
        if (Request(work, rules, key, request) is not { } outcome)
        {
            return ValueTask.FromResult(false);
        }
        Signal(work, source, outcome.Wakes, outcome.Superseded is { } owner ? [owner] : []);
        return ValueTask.FromResult(true);
    }

    /// <summary>Requests each record as <see cref="RequestAsync{TKey}"/> does, in the order of the keys, and signals claimers once.</summary>
    public ValueTask<IReadOnlyList<TKey>> RequestManyAsync<TKey>(WorkTransaction transaction, ReconcileSource<TKey> source, IReadOnlyList<TKey> keys, ReconcileRequest request, CancellationToken cancellationToken)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(request);
        var work = leases.Join(transaction);
        var rules = leases.Rules(source.Leases);
        var found = new List<TKey>(keys.Count);
        var wakes = false;
        var superseded = new HashSet<OwnerId>();
        foreach (var key in keys)
        {
            if (Request(work, rules, key, request) is { } outcome)
            {
                found.Add(key);
                wakes |= outcome.Wakes;
                if (outcome.Superseded is { } owner)
                {
                    superseded.Add(owner);
                }
            }
        }
        Signal(work, source, wakes, superseded);
        return ValueTask.FromResult<IReadOnlyList<TKey>>(found);
    }

    /// <summary>Signals claimers if a requested record is due and not held, and the holders of superseded runs.</summary>
    private void Signal<TKey>(InMemoryTransaction work, ReconcileSource<TKey> source, bool wakes, IEnumerable<OwnerId> superseded)
        where TKey : notnull
    {
        if (wakes)
        {
            leases.Signal(work, WorkTopics.Due, source.Name);
        }
        foreach (var owner in superseded)
        {
            leases.Signal(work, WorkTopics.Cancel, WorkTopics.CancelKey(owner, source.Name));
        }
    }

    /// <summary>
    /// Requests the record; null if it does not exist, otherwise whether claimers are to be signalled and the holder
    /// whose run the request superseded.
    /// </summary>
    private (bool Wakes, OwnerId? Superseded)? Request<TKey>(InMemoryTransaction work, LeaseRules<TKey> rules, TKey key, ReconcileRequest request)
        where TKey : notnull
    {
        var f = rules.Fields;
        var r = rules.Reconcile!;
        var rowKey = rules.RowKey(key);
        if (rules.Find(work, rowKey) is not { } record)
        {
            return null;
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
        var superseded = rules.Supersedes && held && record.Uuid(f.LeaseOwner) is { } owner ? new OwnerId(owner) : (OwnerId?)null;
        return (expires is null && LeaseRules<TKey>.Wakes(dueAt), superseded);
    }

    public ValueTask CompleteAsync<TKey>(WorkTransaction transaction, Lease<TKey> lease, ReconcileResult result, CancellationToken cancellationToken)
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
    public ValueTask FenceAsync(WorkTransaction transaction, IReadOnlyCollection<IReconcileSource> sources, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sources);
        leases.Join(transaction);
        return ValueTask.CompletedTask;
    }

    /// <summary>Increments the source's generation in the caller's transaction and signals claimers on commit.</summary>
    public ValueTask<long> InvalidateAllAsync<TKey>(WorkTransaction transaction, ReconcileSource<TKey> source, CancellationToken cancellationToken)
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
