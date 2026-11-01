using Corfedo.Work.InMemory.Database;

namespace Corfedo.Work.InMemory.Jobs;

internal sealed partial class InMemoryJobStore
{
    /// <summary>
    /// Deletes, per stored definition, the oldest expired terminal jobs outside chains and then the oldest expired run
    /// history entries, each deletion in its own transaction. Occurrences the planner has not consumed and a recurrence's
    /// awaited occurrence are kept.
    /// </summary>
    public async ValueTask<RetentionPass> RetainAsync(int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var keys = database.ReadCommitted(() => application.Definitions.Committed.Keys.Order(StringComparer.Ordinal).ToList());
        int jobs = 0, entries = 0;
        var more = false;
        foreach (var key in keys)
        {
            var deleted = await DeleteAsync(work =>
            {
                if (Stored(work, key)?.Policy.Retention is not { } retention)
                {
                    return 0;
                }
                var horizon = Now - retention;
                var awaited = work.Scan(application.Recurrences).Select(c => c.Row.Awaited).OfType<Guid>().ToHashSet();
                var expired = Jobs(work, key)
                    .Where(r => !r.Outstanding() && r.Time(JobColumns.EndedAt) < horizon && !r.Bool(JobColumns.Unconsumed) && r.Uuid(JobColumns.ChainId) is null
                        && !(r.Uuid(JobColumns.RecurrenceId) is not null && awaited.Contains(r.Uuid(JobColumns.Id)!.Value)))
                    .OrderBy(r => r.Time(JobColumns.EndedAt))
                    .Take(limit)
                    .ToList();
                foreach (var r in expired)
                {
                    work.Remove(JobRows, Key(r.Uuid(JobColumns.Id)!.Value));
                }
                return expired.Count;
            });
            var deletedRuns = await DeleteAsync(work =>
            {
                if (Stored(work, key)?.Policy.RunHistoryRetention is not { } retention)
                {
                    return 0;
                }
                var horizon = Now - retention;
                var expired = work.Scan(application.Runs)
                    .Where(r => r.Row.Definition == key && r.Row.Entry.EndedAt < horizon)
                    .OrderBy(r => r.Row.Entry.EndedAt)
                    .Take(limit)
                    .Select(r => r.Key)
                    .ToList();
                foreach (var run in expired)
                {
                    work.Remove(application.Runs, run);
                }
                return expired.Count;
            });
            jobs += deleted;
            entries += deletedRuns;
            more |= deleted == limit || deletedRuns == limit;
        }
        var chainsDeleted = await DeleteAsync(work => RetainChains(work, limit));
        return new(jobs, entries, chainsDeleted, more || chainsDeleted == limit);

        async Task<int> DeleteAsync(Func<InMemoryTransaction, int> delete)
        {
            using var work = await database.BeginAsync(cancellationToken);
            var count = delete(work);
            work.Commit();
            return count;
        }
    }
}
