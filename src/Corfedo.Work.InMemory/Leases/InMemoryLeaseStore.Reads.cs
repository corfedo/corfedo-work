using Corfedo.Work.InMemory.Database;
using Corfedo.Work.Testing;

namespace Corfedo.Work.InMemory.Leases;

internal sealed partial class InMemoryLeaseStore : ILeaseAgeing
{
    public ValueTask<IReadOnlyList<KeyValuePair<TKey, LeaseRecord>>> ReadAsync<TKey>(WorkTransaction? transaction, LeaseSource<TKey> source, IReadOnlyList<TKey> keys, CancellationToken cancellationToken)
        where TKey : notnull => ValueTask.FromResult(Read(transaction, source, keys, static (lease, _, _, _) => lease));

    /// <summary>
    /// Reads the records of <paramref name="keys"/> as the transaction sees them, or the committed records under the
    /// database's lock without one, with one clock reading; <paramref name="map"/> adds what a caller reads beyond the
    /// lease fields, given the source's generation.
    /// </summary>
    internal IReadOnlyList<KeyValuePair<TKey, TRecord>> Read<TKey, TRecord>(
        WorkTransaction? transaction,
        LeaseSource<TKey> source,
        IReadOnlyList<TKey> keys,
        Func<LeaseRecord, LeaseRules<TKey>, Record, long, TRecord> map)
        where TKey : notnull
    {
        if (source.Job is not null)
        {
            throw new ArgumentException($"Lease source {source.Name} holds jobs; jobs are read through their status.", nameof(source));
        }
        var rules = Rules(source);
        if (transaction is not null)
        {
            var work = Join(transaction);
            return Collect(rules, keys, key => rules.Find(work, key), Now, rules.Generation(work, application), map);
        }
        return database.ReadCommitted(() => Collect(
            rules,
            keys,
            rules.Rows.Committed.GetValueOrDefault,
            Now,
            rules.Reconcile is null ? 0 : application.Generations.Committed.GetValueOrDefault(source.Name)?.Value ?? 0,
            map));
    }

    private static List<KeyValuePair<TKey, TRecord>> Collect<TKey, TRecord>(
        LeaseRules<TKey> rules,
        IReadOnlyList<TKey> keys,
        Func<RowKey, Record?> find,
        DateTimeOffset now,
        long generation,
        Func<LeaseRecord, LeaseRules<TKey>, Record, long, TRecord> map)
        where TKey : notnull
    {
        var records = new List<KeyValuePair<TKey, TRecord>>();
        foreach (var key in keys)
        {
            if (find(rules.RowKey(key)) is not { } r)
            {
                continue;
            }
            var f = rules.Fields;
            var lease = new LeaseRecord(
                now,
                rules.Due(r),
                r.Uuid(f.LeaseOwner) is { } owner ? new OwnerId(owner) : null,
                r.Int64(f.LeaseEpoch),
                rules.Expires(r),
                r.Int32(f.Takeovers),
                r.Int32(f.Failures),
                r.Time(f.CancelRequestedAt),
                rules.Reconcile is null ? r.Uuid(f.ExecutionId) : null);
            records.Add(new(key, map(lease, rules, r, generation)));
        }
        return records;
    }

    /// <summary>Moves the record's time fields back in a transaction of its own; null fields stay null.</summary>
    public async ValueTask<bool> AgeAsync<TKey>(LeaseSource<TKey> source, TKey key, TimeSpan by, CancellationToken cancellationToken)
        where TKey : notnull
    {
        if (source.Job is not null)
        {
            throw new ArgumentException($"Lease source {source.Name} holds jobs; jobs are not aged.", nameof(source));
        }
        var rules = Rules(source);
        var fields = new List<string> { rules.Fields.DueAt, rules.Fields.LeaseExpiresAt, rules.Fields.CancelRequestedAt };
        if (rules.Policy.SettleExpiry)
        {
            fields.Add(rules.Fields.SettleRetryAt);
        }
        if (rules.Reconcile is { } reconcile)
        {
            fields.Add(reconcile.NotBefore);
            fields.Add(reconcile.FirstRequestedAt);
        }
        using var transaction = await database.BeginAsync(cancellationToken);
        var rowKey = rules.RowKey(key);
        if (rules.Find(transaction, rowKey) is not { } r)
        {
            return false;
        }
        var written = Write(transaction, rules, rowKey, r, fields.Select(field => Set(field, r.Time(field) - by)));
        if (LeaseRules<TKey>.Wakes(rules.Due(written)))
        {
            Signal(transaction, WorkTopics.Due, source.Name);
        }
        transaction.Commit();
        return true;
    }
}
