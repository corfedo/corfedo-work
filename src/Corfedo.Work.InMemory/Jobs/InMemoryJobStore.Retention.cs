using Corfedo.Work.InMemory.Database;

namespace Corfedo.Work.InMemory.Jobs;

internal sealed partial class InMemoryJobStore
{
    /// <summary>Whether the job ended failed or timed out.</summary>
    private static bool Failed(Record r) => r.Int32(JobColumns.State) is (int)JobOutcome.Failed or (int)JobOutcome.TimedOut;

    /// <summary>The occurrences recurrences await, as the transaction sees them.</summary>
    private HashSet<Guid> Awaited(InMemoryTransaction work) => work.Scan(application.Recurrences).Select(c => c.Row.Awaited).OfType<Guid>().ToHashSet();

    /// <summary>Whether the job is an ended occurrence the planner has not consumed or that its recurrence awaits.</summary>
    private static bool HeldByRecurrence(Record r, HashSet<Guid> awaited) =>
        r.Bool(JobColumns.Unconsumed) || (r.Uuid(JobColumns.RecurrenceId) is not null && awaited.Contains(r.Uuid(JobColumns.Id)!.Value));

    /// <summary>
    /// Whether retention and discard may delete the job: terminal, not held by its recurrence and not a chain step, which
    /// is deleted with its chain.
    /// </summary>
    private static bool Deletable(Record r, HashSet<Guid> awaited) => !r.Outstanding() && r.Uuid(JobColumns.ChainId) is null && !HeldByRecurrence(r, awaited);

    /// <summary>
    /// Deletes, per stored definition, the oldest expired failed jobs, the oldest expired other terminal jobs outside
    /// chains and the oldest expired run history entries, each deletion in its own transaction. Occurrences the planner has not consumed and a recurrence's
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
            // Failed retention is never subtracted from the clock when it keeps failed jobs until they are handled.
            var deletedFailed = await DeleteAsync(work => Stored(work, key)?.Policy.FailedRetention is { } window && window != Timeout.InfiniteTimeSpan
                ? DeleteExpired(work, key, window, failed: true)
                : 0);
            var deleted = await DeleteAsync(work => Stored(work, key)?.Policy.Retention is { } window ? DeleteExpired(work, key, window, failed: false) : 0);
            jobs += deletedFailed;
            more |= deletedFailed == limit;
            var deletedRuns = await DeleteAsync(work =>
            {
                if (Stored(work, key)?.Policy.RunHistoryRetention is not { } retention)
                {
                    return 0;
                }
                var horizon = JobRetention.Before(Now, retention);
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

        int DeleteExpired(InMemoryTransaction work, string key, TimeSpan window, bool failed)
        {
            var horizon = JobRetention.Before(Now, window);
            var awaited = Awaited(work);
            var expired = Jobs(work, key)
                .Where(r => Failed(r) == failed && Deletable(r, awaited) && r.Time(JobColumns.EndedAt) < horizon)
                .OrderBy(r => r.Time(JobColumns.EndedAt))
                .Take(limit)
                .ToList();
            foreach (var r in expired)
            {
                work.Remove(JobRows, Key(r.Uuid(JobColumns.Id)!.Value));
            }
            return expired.Count;
        }

        async Task<int> DeleteAsync(Func<InMemoryTransaction, int> delete)
        {
            using var work = await database.BeginAsync(cancellationToken);
            var count = delete(work);
            work.Commit();
            return count;
        }
    }
}
