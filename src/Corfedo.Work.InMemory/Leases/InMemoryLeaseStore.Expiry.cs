using Corfedo.Work.InMemory.Database;
using Corfedo.Work.InMemory.Jobs;
using Microsoft.Extensions.Logging;

namespace Corfedo.Work.InMemory.Leases;

internal sealed partial class InMemoryLeaseStore
{
    /// <summary>
    /// Settles expired leases one record per transaction, each time the earliest expired, unsettled record that is not
    /// waiting for a settlement retry and has not failed in this call. A failed settlement counts in
    /// <c>SettleFailures</c> and is retried after the source's settlement backoff.
    /// </summary>
    public async ValueTask<int> SettleExpiredAsync<TKey>(LeaseSource<TKey> source, LeaseCallbacks<TKey> callbacks, int limit, CancellationToken cancellationToken)
        where TKey : notnull
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        if (!source.Settles)
        {
            throw new ArgumentException($"Lease source {source.Name} does not settle expired leases.", nameof(source));
        }
        if (source.Policy.SettleExpiry && callbacks.Expired is null)
        {
            throw new ArgumentException($"Lease source {source.Name} settles expiry with its expiry handler, and none was given.", nameof(callbacks));
        }
        var rules = Rules(source);
        var failed = new HashSet<RowKey>();
        var settled = 0;
        while (settled < limit)
        {
            switch (await SettleNextAsync(rules, callbacks, failed, cancellationToken))
            {
                case Settlement.None:
                    return settled;
                case Settlement.Settled:
                    settled++;
                    break;
            }
        }
        return settled;
    }

    private enum Settlement
    {
        None,
        Settled,
        Failed,
    }

    /// <summary>Settles one expired epoch in its own transaction: run the handler, record.</summary>
    private async Task<Settlement> SettleNextAsync<TKey>(LeaseRules<TKey> rules, LeaseCallbacks<TKey> callbacks, HashSet<RowKey> failed, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var f = rules.Fields;
        using var transaction = await database.BeginAsync(cancellationToken);
        var now = Now;
        var next = rules.Scan(transaction)
            .Where(r => rules.Expires(r.Row) <= now
                && r.Row.Int64(f.SettledEpoch) < r.Row.Int64(f.LeaseEpoch)
                && !rules.SettlementWaiting(r.Row, now)
                && !failed.Contains(r.Key))
            .OrderBy(r => rules.Expires(r.Row))
            .ThenBy(r => r.Row.Version)
            .Select(r => ((RowKey, Record)?)r)
            .FirstOrDefault();
        if (next is not { } found)
        {
            return Settlement.None;
        }
        var (key, record) = found;
        var expiry = new LeaseExpiry<TKey>(rules.Source, rules.Key(key), new OwnerId(record.Uuid(f.LeaseOwner)!.Value), record.Int64(f.LeaseEpoch), rules.Partition(record));
        if (callbacks.Expired is { } handler)
        {
            var savepoint = transaction.Save();
            try
            {
                await handler(expiry, transaction, cancellationToken);
            }
            catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
            {
                transaction.RollbackTo(savepoint);
                CountSettlementFailure(transaction, rules, key, now);
                transaction.Commit();
                failed.Add(key);
                LogSettlementFailed(logger, telemetry.Logged(exception), rules.Source.Name, rules.Format(key), telemetry.Describe(exception));
                return Settlement.Failed;
            }
        }
        var current = rules.Find(transaction, key)!;
        var changes = new List<KeyValuePair<string, object?>> { Set(f.SettledEpoch, current.Int64(f.LeaseEpoch)) };
        if (rules.Job is not null)
        {
            // The attempt of the expired lease ends in the run history at the lease's expiry.
            if (current.Int64(JobColumns.AttemptEpoch) == current.Int64(f.LeaseEpoch))
            {
                JobRuns.Record(transaction, application, current, JobRunOutcome.LeaseLost, null, rules.Expires(current)!.Value);
            }
            changes.Add(Set(JobColumns.AttemptStartedAt, null));
        }
        if (rules.Policy.SettleExpiry)
        {
            changes.Add(Set(f.SettleFailures, 0));
            changes.Add(Set(f.SettleRetryAt, null));
        }
        Write(transaction, rules, key, current, changes);
        // A settlement frees a slot of the source's limits, and makes the record claimable for claimers without the
        // expiry handler.
        Signal(transaction, WorkTopics.Due, rules.Source.Name);
        transaction.Commit();
        return Settlement.Settled;
    }

    [LoggerMessage(EventId = 2, EventName = "lease.settlement_failed", Level = LogLevel.Warning, Message = "Settlement of the expired lease on {Source} record {Key} failed and will be retried: {Error}")]
    private static partial void LogSettlementFailed(ILogger logger, Exception? exception, string source, string key, string error);
}
