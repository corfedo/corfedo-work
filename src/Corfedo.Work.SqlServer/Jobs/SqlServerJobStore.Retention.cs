using System.Globalization;
using Corfedo.Work.SqlServer.Leases;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Jobs;

internal sealed partial class SqlServerJobStore
{
    /// <summary>
    /// Deletes, per stored definition, the oldest expired terminal jobs outside chains and then the oldest expired run
    /// history entries, then the oldest expired settled chains as units, each statement in its own transaction.
    /// Candidates are found through the end-time indexes without locks and locked through their key without waiting, so
    /// retention never waits for a transition, a rerun, a retry or a planner, and a job a concurrent rerun made
    /// outstanding again fails the recheck of its state. The bounds are the store's clock, read with the stored
    /// retentions, minus each retention. A chain's window is its owner definition's job retention, or the longest of the
    /// definitions of its steps, as stored when retention runs; the shortest stored job retention bounds the scan of
    /// settled chains.
    /// </summary>
    public async ValueTask<RetentionPass> RetainAsync(int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var keys = new List<(string Key, TimeSpan Jobs, TimeSpan Runs)>();
        DateTimeOffset now;
        await using (var connection = await leases.OpenAsync(cancellationToken))
        {
            await using (var clock = Tsql.Command(connection, null, "SELECT SYSUTCDATETIME();"))
            await using (var reader = await clock.ReaderAsync(cancellationToken))
            {
                await reader.NextRowAsync(cancellationToken);
                now = Tsql.Time(reader, 0);
            }
            await using (var command = Tsql.Command(connection, null, $"SELECT [key], job_retention, run_history_retention FROM {definitions} ORDER BY [key];"))
            await using (var reader = await command.ReaderAsync(cancellationToken))
            {
                while (await reader.NextRowAsync(cancellationToken))
                {
                    keys.Add((reader.GetString(0), TimeSpan.FromTicks(reader.GetInt64(1)), TimeSpan.FromTicks(reader.GetInt64(2))));
                }
            }
        }
        var expired = $"j.[definition] = @definition AND j.ended_at IS NOT NULL AND j.ended_at < @bound AND j.state <> 0 AND j.chain_id IS NULL AND j.unconsumed = 0 "
            + $"AND (j.recurrence_id IS NULL OR NOT EXISTS (SELECT 1 FROM {recurrences} AS c WHERE c.id = j.recurrence_id AND c.awaited_job = j.id))";
        var deleteJobs = $"""
            DECLARE @doomed TABLE (id uniqueidentifier NOT NULL PRIMARY KEY);
            {Tsql.LockEach("job_scan", "@limit", $"SELECT j.id FROM {Jobs} AS j WITH (INDEX(jobs_retention)) WHERE {expired} ORDER BY j.ended_at",
                Jobs, Tsql.IdKey, expired.Replace("j.", "r.", StringComparison.Ordinal), "@doomed")}
            DELETE r FROM @doomed AS d INNER LOOP JOIN {Jobs} AS r WITH (INDEX(jobs_key), FORCESEEK) ON r.id = d.id;
            SELECT COUNT(*) FROM @doomed;
            """;
        var deleteRuns = $"""
            DECLARE @doomed TABLE (job_id uniqueidentifier NOT NULL, epoch bigint NOT NULL, PRIMARY KEY (job_id, epoch));
            {Tsql.LockEach("run_scan", "@limit",
                $"SELECT h.job_id, h.epoch FROM {runs} AS h WITH (INDEX(job_runs_retention)) WHERE h.[definition] = @definition AND h.ended_at < @bound ORDER BY h.ended_at",
                runs, [("job_id", "uniqueidentifier"), ("epoch", "bigint")], "r.ended_at < @bound", "@doomed")}
            DELETE r FROM @doomed AS d INNER LOOP JOIN {runs} AS r WITH (FORCESEEK) ON r.job_id = d.job_id AND r.epoch = d.epoch;
            SELECT COUNT(*) FROM @doomed;
            """;
        int jobs = 0, entries = 0;
        var more = false;
        foreach (var (key, jobRetention, runRetention) in keys)
        {
            var deleted = await DeleteAsync(deleteJobs, Tsql.Text("@definition", key, 200), Tsql.Time("@bound", Before(now, jobRetention)));
            var deletedRuns = await DeleteAsync(deleteRuns, Tsql.Text("@definition", key, 200), Tsql.Time("@bound", Before(now, runRetention)));
            jobs += deleted;
            entries += deletedRuns;
            more |= deleted == limit || deletedRuns == limit;
        }
        var chainsDeleted = 0;
        if (keys.Count > 0)
        {
            // A chain is expired once the longest retention of the definitions that keep it has passed since it settled.
            var settled = $"c.settled_at IS NOT NULL AND c.settled_at < @bound AND {Tsql.Micros("c.settled_at", "@now")} > (SELECT MAX(d.job_retention) / 10 FROM {definitions} AS d "
                + $"WHERE d.[key] = c.owner_definition OR (c.owner_definition IS NULL AND d.[key] IN (SELECT cd.[definition] FROM {chainDefinitions} AS cd WHERE cd.chain_id = c.id)))";
            chainsDeleted = await DeleteAsync($"""
                DECLARE @doomed TABLE (id uniqueidentifier NOT NULL PRIMARY KEY);
                {Tsql.LockEach("chain_scan", "@limit", $"SELECT c.id FROM {chains} AS c WITH (INDEX(job_chains_settled)) WHERE {settled} ORDER BY c.settled_at",
                    chains, Tsql.IdKey, settled.Replace("c.settled_at", "r.settled_at", StringComparison.Ordinal).Replace("c.owner_definition", "r.owner_definition", StringComparison.Ordinal)
                        .Replace("cd.chain_id = c.id", "cd.chain_id = r.id", StringComparison.Ordinal), "@doomed")}
                DECLARE @steps TABLE (id uniqueidentifier NOT NULL PRIMARY KEY);
                INSERT INTO @steps (id) SELECT j.id FROM @doomed AS d INNER LOOP JOIN {Jobs} AS j WITH (INDEX(jobs_chain), FORCESEEK) ON j.chain_id = d.id AND j.chain_id IS NOT NULL;
                DELETE r FROM @steps AS s INNER LOOP JOIN {Jobs} AS r WITH (INDEX(jobs_key), FORCESEEK) ON r.id = s.id WHERE r.chain_id IN (SELECT id FROM @doomed);
                DELETE c FROM @doomed AS d INNER LOOP JOIN {chains} AS c WITH (FORCESEEK) ON c.id = d.id;
                SELECT COUNT(*) FROM @doomed;
                """,
                Tsql.Time("@bound", Before(now, keys.Min(k => k.Jobs))),
                Tsql.Time("@now", now));
        }
        return new(jobs, entries, chainsDeleted, more || chainsDeleted == limit);

        async Task<int> DeleteAsync(string text, params SqlParameter[] parameters)
        {
            return await SqlServerLeaseStore.RetryDeadlocksAsync(async () =>
            {
                await using var connection = await leases.OpenAsync(cancellationToken);
                await using var command = Tsql.Command(connection, null, Tsql.OwnTransaction(text), [.. parameters.Select(p => (SqlParameter)((ICloneable)p).Clone()), Tsql.Int32("@limit", limit)]);
                return Convert.ToInt32(await command.ScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
            });
        }
    }

    /// <summary><paramref name="now"/> moved back by <paramref name="retention"/>, or the earliest time when that is before it.</summary>
    private static DateTimeOffset Before(DateTimeOffset now, TimeSpan retention) =>
        retention.Ticks >= (now - DateTimeOffset.MinValue).Ticks ? DateTimeOffset.MinValue : now - retention;
}
