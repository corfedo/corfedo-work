namespace Corfedo.Work.InMemory.Leases;

internal sealed partial class InMemoryLeaseStore
{
    public ValueTask<bool> ScheduleAsync<TKey>(IWorkTransaction transaction, LeaseSource<TKey> source, TKey key, Due due, Guid? execution, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = Join(transaction);
        var rules = Rules(source);
        if (rules.Reconcile is not null && execution is not null)
        {
            throw new ArgumentException($"Reconciliation source {source.Name} derives its executions from revisions; request the record instead.", nameof(execution));
        }
        var rowKey = rules.RowKey(key);
        if (rules.Find(work, rowKey) is not { } r)
        {
            return ValueTask.FromResult(false);
        }
        var dueAt = LeaseRules<TKey>.DueAt(due, rules.Due(r), Now);
        if (rules.Reconcile is { } rf)
        {
            // Reconciliation sources derive executions from revisions and keep no execution field. A due time is never
            // earlier than the backoff floor.
            if (dueAt is { } value && r.Time(rf.NotBefore) is { } floor && floor > value)
            {
                dueAt = floor;
            }
            Write(work, rules, rowKey, r, [Set(rules.Fields.DueAt, dueAt)]);
        }
        else
        {
            Write(work, rules, rowKey, r, [Set(rules.Fields.DueAt, dueAt), Set(rules.Fields.ExecutionId, execution ?? r.Uuid(rules.Fields.ExecutionId))]);
        }
        if (LeaseRules<TKey>.Wakes(dueAt))
        {
            Signal(work, WorkTopics.Due, source.Name);
        }
        return ValueTask.FromResult(true);
    }

    /// <summary>Sets <c>CancelRequestedAt</c> and signals the process of the record's holder, if it is held.</summary>
    public ValueTask<bool> RequestCancelAsync<TKey>(IWorkTransaction transaction, LeaseSource<TKey> source, TKey key, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = Join(transaction);
        var rules = Rules(source);
        var rowKey = rules.RowKey(key);
        if (rules.Find(work, rowKey) is not { } r)
        {
            return ValueTask.FromResult(false);
        }
        Write(work, rules, rowKey, r, [Set(rules.Fields.CancelRequestedAt, r.Time(rules.Fields.CancelRequestedAt) ?? Now)]);
        if (rules.Expires(r) is not null && r.Uuid(rules.Fields.LeaseOwner) is { } owner)
        {
            Signal(work, WorkTopics.Cancel, WorkTopics.CancelKey(new OwnerId(owner), source.Name));
        }
        return ValueTask.FromResult(true);
    }

    public ValueTask<bool> ClearCancelAsync<TKey>(IWorkTransaction transaction, LeaseSource<TKey> source, TKey key, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = Join(transaction);
        var rules = Rules(source);
        var rowKey = rules.RowKey(key);
        if (rules.Find(work, rowKey) is not { } r)
        {
            return ValueTask.FromResult(false);
        }
        Write(work, rules, rowKey, r, [Set(rules.Fields.CancelRequestedAt, null)]);
        Signal(work, WorkTopics.Due, source.Name);
        return ValueTask.FromResult(true);
    }

    /// <summary>
    /// From committed records: unheld records due after the cutoff, expired leases whose takeover backoff ends after it,
    /// and the next lease to expire, a lower bound for valid leases, whose holders usually renew them before then.
    /// Records the claim considered at or before its cutoff are left out.
    /// </summary>
    public ValueTask<TimeSpan?> NextDueAsync<TKey>(LeaseSource<TKey> source, DateTimeOffset cutoff, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var rules = Rules(source);
        var now = Now;
        var next = database.ReadCommitted(() =>
        {
            DateTimeOffset? earliest = null;
            void Consider(DateTimeOffset value) => earliest = earliest is null || value < earliest ? value : earliest;
            var paused = rules.CommittedPauses();
            foreach (var r in rules.Committed())
            {
                if (rules.Due(r) is not { } due || !rules.Eligible(r, paused))
                {
                    continue;
                }
                if (rules.Expires(r) is not { } expires)
                {
                    if (due > cutoff)
                    {
                        Consider(due);
                    }
                }
                else if (expires <= now)
                {
                    var takeover = expires + rules.TakeoverBackoff(r);
                    takeover = due > takeover ? due : takeover;
                    if (takeover > cutoff)
                    {
                        Consider(takeover);
                    }
                }
                else
                {
                    Consider(expires);
                }
            }
            return earliest;
        });
        return ValueTask.FromResult(Until(next, now));
    }

    /// <summary>The next unsettled lease to expire, and the earliest retry of expired leases whose settlement failed.</summary>
    public ValueTask<TimeSpan?> NextExpiryAsync<TKey>(LeaseSource<TKey> source, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var rules = Rules(source);
        var now = Now;
        var next = database.ReadCommitted(() =>
        {
            DateTimeOffset? earliest = null;
            foreach (var r in rules.Committed())
            {
                if (rules.Expires(r) is not { } expires || r.Int64(rules.Fields.SettledEpoch) >= r.Int64(rules.Fields.LeaseEpoch))
                {
                    continue;
                }
                var at = expires > now || !rules.Policy.SettleExpiry || r.Time(rules.Fields.SettleRetryAt) is not { } retry || retry <= expires ? expires : retry;
                earliest = earliest is null || at < earliest ? at : earliest;
            }
            return earliest;
        });
        return ValueTask.FromResult(Until(next, now));
    }

    private static TimeSpan? Until(DateTimeOffset? at, DateTimeOffset now) => at is { } value ? value > now ? value - now : TimeSpan.Zero : null;
}
