using Npgsql;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Jobs;

internal sealed partial class PostgreSqlJobStore
{
    /// <summary>
    /// The condition on the job row <paramref name="r"/> under which retention and discard may delete it: terminal, not an
    /// occurrence its recurrence's planner has not consumed or that its recurrence awaits, and not a chain step, which
    /// is deleted with its chain.
    /// </summary>
    private string Deletable(string r) =>
        $"{r}.state <> 0 AND NOT {r}.unconsumed AND {r}.chain_id IS NULL "
        + $"AND ({r}.recurrence_id IS NULL OR NOT EXISTS (SELECT 1 FROM {recurrences} AS c WHERE c.id = {r}.recurrence_id AND c.awaited_job = {r}.id))";

    /// <summary>
    /// Deletes, per stored definition, the oldest expired failed jobs, the oldest expired other terminal jobs outside
    /// chains and the oldest expired run history entries, then the oldest expired settled chains as units, each statement
    /// in its own transaction.
    /// Candidates are locked with <c>SKIP LOCKED</c>, so retention never waits for a transition, a rerun, a retry or a
    /// planner, and a job a concurrent rerun made outstanding again fails the recheck of its state. The bounds use
    /// <c>now()</c>, so the end-time indexes serve them. A chain's window is its owner definition's job retention, or
    /// the longest of the definitions of its steps, as stored when retention runs; the shortest stored job retention
    /// bounds the scan of settled chains.
    /// </summary>
    public async ValueTask<RetentionPass> RetainAsync(int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var keys = new List<string>();
        await using (var command = dataSource.CreateCommand($"SELECT key FROM {definitions} ORDER BY key"))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                keys.Add(reader.GetString(0));
            }
        }
        var table = $"{Leases.LeaseSql.Quote(Table.Schema)}.{Leases.LeaseSql.Quote(Table.Name)}";
        var runs = Leases.LeaseSql.Quote(schema) + ".job_runs";
        // Failed jobs and the other terminal jobs are read on indexes of their own, jobs_failed and jobs_retention, whose
        // predicates the state conditions repeat. A null failed retention keeps failed jobs: the bound is null and
        // selects none.
        var deleteJobs = DeleteExpired(table, "r.state <> 2 AND r.state <> 4", "job_retention");
        var deleteFailed = DeleteExpired(table, "r.state IN (2, 4)", "failed_retention");
        var deleteRuns =
            $"DELETE FROM {runs} AS h USING (SELECT r.job_id, r.epoch FROM {runs} AS r WHERE r.definition = $1 "
            + $"AND r.ended_at < now() - (SELECT d.run_history_retention FROM {definitions} AS d WHERE d.key = $1) "
            + "ORDER BY r.ended_at LIMIT $2 FOR UPDATE OF r SKIP LOCKED) AS v WHERE h.job_id = v.job_id AND h.epoch = v.epoch";
        int jobs = 0, entries = 0;
        var more = false;
        foreach (var key in keys)
        {
            var deletedFailed = await DeleteAsync(deleteFailed, key);
            jobs += deletedFailed;
            more |= deletedFailed == limit;
            var deleted = await DeleteAsync(deleteJobs, key);
            var deletedRuns = await DeleteAsync(deleteRuns, key);
            jobs += deleted;
            entries += deletedRuns;
            more |= deleted == limit || deletedRuns == limit;
        }
        // The chosen chains are collected into an array, so their steps and rows are deleted by key lookups; joined to
        // the selection, the planner merged it with the whole step index.
        var chainsDeleted = await DeleteAsync(
            $"WITH v AS (SELECT array_agg(c.id) AS ids FROM (SELECT c.id FROM {chains} AS c WHERE c.settled_at < now() - (SELECT min(d.job_retention) FROM {definitions} AS d) "
            + $"AND c.settled_at < now() - (SELECT max(d.job_retention) FROM {definitions} AS d "
            + "WHERE d.key = ANY(CASE WHEN c.owner_definition IS NULL THEN c.definitions ELSE ARRAY[c.owner_definition] END)) "
            + "ORDER BY c.settled_at LIMIT $2 FOR UPDATE OF c SKIP LOCKED) AS c), "
            + $"s AS (DELETE FROM {table} AS j WHERE j.chain_id = ANY((SELECT ids FROM v)::uuid[])) "
            + $"DELETE FROM {chains} AS c WHERE c.id = ANY((SELECT ids FROM v)::uuid[])",
            "");
        return new(jobs, entries, chainsDeleted, more || chainsDeleted == limit);

        string DeleteExpired(string jobs, string state, string window) =>
            $"DELETE FROM {jobs} AS j USING (SELECT r.id FROM {jobs} AS r WHERE r.definition = $1 AND {state} AND {Deletable("r")} "
            + $"AND r.ended_at < now() - (SELECT d.{window} FROM {definitions} AS d WHERE d.key = $1) "
            + "ORDER BY r.ended_at LIMIT $2 FOR UPDATE OF r SKIP LOCKED) AS v WHERE j.id = v.id";

        async Task<int> DeleteAsync(string text, string key)
        {
            await using var command = dataSource.CreateCommand(text);
            command.Parameters.Add(Text(key));
            command.Parameters.Add(Int32(limit));
            return await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }
}
