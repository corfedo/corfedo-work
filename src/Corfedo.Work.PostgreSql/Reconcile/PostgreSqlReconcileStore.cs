using Corfedo.Work.PostgreSql.Leases;
using Corfedo.Work.PostgreSql.Signals;
using Npgsql;
using NpgsqlTypes;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Reconcile;

/// <summary>Requests, results and invalidation of reconciliation sources, on the statements of their lease sources.</summary>
internal sealed class PostgreSqlReconcileStore(NpgsqlDataSource dataSource, PostgreSqlLeaseStore leases, PostgreSqlSignalSender signals) : IReconcileStore
{
    /// <summary>
    /// Locks the record, then increments the requested revision and schedules it, with a clock reading taken after the
    /// lock is held. The due time is the trailing debounce from now, capped at the first request not yet claimed plus
    /// the maximum; a waiting record keeps an earlier due time; a backoff floor holds unless the request resets it.
    /// While a run holds the record, the due time is recorded for the run's
    /// result to schedule the follow-up; on superseding sources the request instead gives the run's lease back, and
    /// the holder's process is signalled to renew and observe it.
    /// </summary>
    public async ValueTask<bool> RequestAsync<TKey>(WorkTransaction transaction, ReconcileSource<TKey> source, TKey key, ReconcileRequest request, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = PostgreSqlTransaction.From(transaction);
        var sql = leases.Sql(source.Leases);
        var k = sql.KeyCount;
        await using var batch = new NpgsqlBatch(work.Connection, work.Transaction);
        batch.BatchCommands.Add(KeyCommand($"SELECT 1 FROM {sql.Table} AS r WHERE {sql.KeyMatch(1)} FOR UPDATE OF r", source.Leases, key));
        var schedule = KeyCommand(
            $"WITH old AS (SELECT {sql.Expires} > clock_timestamp() AS held, {sql.Owner} AS owner FROM {sql.Table} AS r WHERE {sql.KeyMatch(1)}) "
            + $"UPDATE {sql.Table} AS r SET {Schedule(sql, request, "old")} "
            + $"FROM old WHERE {sql.KeyMatch(1)} "
            + $"RETURNING {(sql.Supersedes ? "coalesce(old.held, false)" : "false")}, old.owner",
            source.Leases,
            key);
        AddDebounce(schedule.Parameters, request);
        batch.BatchCommands.Add(schedule);
        batch.BatchCommands.Add(KeyCommand(
            $"{PostgreSqlSignalSql.Rows($"${k + 1}")} FROM {sql.Table} AS r WHERE {sql.KeyMatch(1)} AND {sql.Expires} IS NULL AND {sql.Due} IS NOT NULL",
            source.Leases,
            key,
            Signal(WorkTopics.Due, source.Name)));
        var found = false;
        OwnerId? superseded = null;
        await using (var reader = await batch.ExecuteReaderAsync(cancellationToken))
        {
            await reader.NextResultAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                found = true;
                if (reader.GetBoolean(0) && !reader.IsDBNull(1))
                {
                    superseded = new OwnerId(reader.GetGuid(1));
                }
            }
            await signals.ReadAsync(reader, cancellationToken);
        }
        if (superseded is { } owner)
        {
            await signals.SendAsync(work.Connection, work.Transaction, WorkTopics.Cancel, WorkTopics.CancelKey(owner, source.Name), cancellationToken);
        }
        return found;
    }

    /// <summary>
    /// In one batch: locks the records in the order of the keys and returns the keys found, schedules them as
    /// <see cref="RequestAsync{TKey}"/> does with a clock reading per record taken after the locks are held, signals the
    /// holders of superseded runs once each, and signals claimers once if any record is due and not held.
    /// </summary>
    public async ValueTask<IReadOnlyList<TKey>> RequestManyAsync<TKey>(WorkTransaction transaction, ReconcileSource<TKey> source, IReadOnlyList<TKey> keys, ReconcileRequest request, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = PostgreSqlTransaction.From(transaction);
        var sql = leases.Sql(source.Leases);
        var k = sql.KeyCount;
        await using var batch = new NpgsqlBatch(work.Connection, work.Transaction);
        // Returns the requested key values, which can differ from the stored ones under the column's collation.
        var requested = string.Join(", ", Enumerable.Range(0, k).Select(i => $"u.k{i}"));
        var locks = new NpgsqlBatchCommand($"SELECT {requested} FROM {sql.Table} AS r JOIN {sql.OrderedUnnest} ON {sql.KeyJoin} ORDER BY u.ord FOR UPDATE OF r");
        AddKeys(locks.Parameters, source.Leases, keys);
        batch.BatchCommands.Add(locks);
        // The CTE is named u so that the key join of the update reads its key columns.
        var held = $"WITH u AS (SELECT u.*, {sql.Expires} > clock_timestamp() AS held, {sql.Owner} AS owner FROM {sql.Table} AS r JOIN {sql.Unnest} ON {sql.KeyJoin})";
        var update = $"UPDATE {sql.Table} AS r SET {Schedule(sql, request, "u")} FROM u WHERE {sql.KeyJoin}";
        var schedule = new NpgsqlBatchCommand();
        AddKeys(schedule.Parameters, source.Leases, keys);
        AddDebounce(schedule.Parameters, request);
        if (sql.Supersedes)
        {
            schedule.Parameters.Add(Text(source.Name));
            var cancel = PostgreSqlSignalSql.CancelExpression("c.owner", $"${schedule.Parameters.Count}");
            schedule.CommandText = $"{held}, s AS ({update} RETURNING u.held, u.owner) "
                + $"{PostgreSqlSignalSql.Rows(cancel)} FROM (SELECT DISTINCT owner FROM s WHERE s.held AND s.owner IS NOT NULL) AS c";
        }
        else
        {
            schedule.CommandText = $"{held} {update}";
        }
        batch.BatchCommands.Add(schedule);
        var due = new NpgsqlBatchCommand(
            $"{PostgreSqlSignalSql.Rows($"${k + 1}")} WHERE EXISTS "
            + $"(SELECT 1 FROM {sql.Table} AS r JOIN {sql.Unnest} ON {sql.KeyJoin} WHERE {sql.Expires} IS NULL AND {sql.Due} IS NOT NULL)");
        AddKeys(due.Parameters, source.Leases, keys);
        due.Parameters.Add(Signal(WorkTopics.Due, source.Name));
        batch.BatchCommands.Add(due);
        var found = new List<TKey>(keys.Count);
        await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            found.Add(ReadKey(source.Leases, reader));
        }
        await reader.NextResultAsync(cancellationToken);
        await signals.ReadAsync(reader, cancellationToken);
        return found;
    }

    /// <summary>
    /// The assignments of a request to the record <c>r</c>, with whether a run held it in <c>{old}.held</c>; the
    /// debounce delay and maximum are the parameters after the keys, as <see cref="AddDebounce"/> adds them.
    /// </summary>
    private static string Schedule(LeaseSql sql, ReconcileRequest request, string old)
    {
        var k = sql.KeyCount;
        var first = $"COALESCE({sql.FirstRequestedAt}, clock_timestamp())";
        var debounced = request.Debounce?.Maximum is null
            ? $"clock_timestamp() + ${k + 1}"
            : $"LEAST(clock_timestamp() + ${k + 1}, {first} + ${k + 2})";
        var candidate = $"CASE WHEN {sql.FirstRequestedAt} IS NOT NULL OR {old}.held THEN {debounced} ELSE LEAST({sql.Due}, {debounced}) END";
        // LEAST and GREATEST ignore nulls: an idle record has no due time, a record without a floor no floor.
        var due = request.ResetBackoff ? candidate : $"GREATEST({candidate}, {sql.NotBefore})";
        var reset = request.ResetBackoff ? $", {LeaseSql.Bare(sql.NotBefore)} = NULL, {LeaseSql.Bare(sql.Failures)} = 0" : "";
        var supersede = sql.Supersedes ? $", {LeaseSql.Bare(sql.Expires)} = CASE WHEN {old}.held THEN NULL ELSE {sql.Expires} END" : "";
        return $"{LeaseSql.Bare(sql.RequestedRevision)} = {sql.RequestedRevision} + 1, "
            + $"{LeaseSql.Bare(sql.FirstRequestedAt)} = {first}, {LeaseSql.Bare(sql.Due)} = {due}{reset}{supersede}";
    }

    private static void AddDebounce(NpgsqlParameterCollection parameters, ReconcileRequest request)
    {
        parameters.Add(Interval(request.Debounce?.Delay ?? TimeSpan.Zero));
        if (request.Debounce?.Maximum is { } maximum)
        {
            parameters.Add(Interval(maximum));
        }
    }

    public async ValueTask CompleteAsync<TKey>(WorkTransaction transaction, Lease<TKey> lease, ReconcileResult result, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = PostgreSqlTransaction.From(transaction);
        if (!await CompleteAsync(work.Connection, work.Transaction, lease, result, cancellationToken))
        {
            var source = lease.Source;
            throw new LeaseLostException($"The lease on {source.Name} record {source.Key.Format(lease.Key)} at epoch {lease.Epoch} is no longer valid.");
        }
    }

    public async ValueTask<bool> CompleteAsync<TKey>(Lease<TKey> lease, ReconcileResult result, CancellationToken cancellationToken)
        where TKey : notnull
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        if (!await CompleteAsync(connection, transaction, lease, result, cancellationToken))
        {
            return false;
        }
        await transaction.CommitAsync(cancellationToken);
        return true;
    }

    /// <summary>
    /// On superseding sources takes the generation fence, which requires READ COMMITTED, then locks the record and
    /// applies the result if the lease is still valid by a clock reading taken after the locks are held, and gives the
    /// record back. Converged advances the converged revision and generation to what the run claimed and resets failures; the
    /// record is due at the time a request during the run recorded, after the resync interval, or not at all. Waiting
    /// is due at the retry time or the recorded request, whichever is first. Failed counts a failure and moves the
    /// backoff floor and the due time by the failure backoff.
    /// </summary>
    private async Task<bool> CompleteAsync<TKey>(NpgsqlConnection connection, NpgsqlTransaction transaction, Lease<TKey> lease, ReconcileResult result, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var source = lease.Source;
        var sql = leases.Sql(source);
        var resync = source.Reconcile!.Policy.ResyncInterval;
        var k = sql.KeyCount;
        var pending = $"{sql.RequestedRevision} > {sql.ClaimedRevision}";
        var set = result.Outcome switch
        {
            ReconcileOutcome.Converged =>
                $"{LeaseSql.Bare(sql.ConvergedRevision)} = {sql.ClaimedRevision}, {LeaseSql.Bare(sql.ConvergedGeneration)} = {sql.AttemptedGeneration}, "
                + $"{LeaseSql.Bare(sql.Failures)} = 0, {LeaseSql.Bare(sql.NotBefore)} = NULL, "
                + $"{LeaseSql.Bare(sql.Due)} = CASE WHEN {pending} THEN {sql.Due} "
                + (resync is null ? "ELSE NULL END" : $"ELSE clock_timestamp() + ${k + 3} END"),
            ReconcileOutcome.Waiting =>
                $"{LeaseSql.Bare(sql.Due)} = CASE WHEN {pending} THEN LEAST({sql.Due}, clock_timestamp() + ${k + 3}) "
                + $"ELSE clock_timestamp() + ${k + 3} END",
            _ =>
                $"{LeaseSql.Bare(sql.Failures)} = {sql.Failures} + 1, ({LeaseSql.Bare(sql.NotBefore)}, {LeaseSql.Bare(sql.Due)}) = "
                + $"(SELECT t, t FROM (SELECT clock_timestamp() + {sql.FailureBackoff} AS t) AS b)",
        };
        var delay = result.Outcome switch
        {
            ReconcileOutcome.Converged when resync is { } interval => interval,
            ReconcileOutcome.Waiting => result.RetryAfter,
            _ => (TimeSpan?)null,
        };
        await using var batch = new NpgsqlBatch(connection, transaction);
        if (sql.GenerationFence is { } fence)
        {
            batch.BatchCommands.Add(new NpgsqlBatchCommand(fence));
        }
        batch.BatchCommands.Add(KeyCommand($"SELECT 1 FROM {sql.Table} AS r WHERE {sql.KeyMatch(1)} FOR UPDATE OF r", source, lease.Key));
        var update = KeyCommand(
            $"UPDATE {sql.Table} AS r SET {LeaseSql.Bare(sql.Expires)} = NULL, {LeaseSql.Bare(sql.Takeovers)} = 0, {set} "
            + $"WHERE {sql.KeyMatch(1)} AND {sql.Valid(k + 1, k + 2)}",
            source,
            lease.Key,
            Uuid(lease.Owner.Value),
            PostgreSqlLeaseStore.Int64(lease.Epoch));
        if (delay is { } value)
        {
            update.Parameters.Add(PostgreSqlLeaseStore.Interval(value));
        }
        batch.BatchCommands.Add(update);
        // A completed record is claimable again or frees a slot of the source's limits.
        batch.BatchCommands.Add(KeyCommand(
            $"{PostgreSqlSignalSql.Rows($"${k + 3}")} FROM {sql.Table} AS r WHERE {sql.KeyMatch(1)} AND {sql.Expires} IS NULL "
            + $"AND {sql.Owner} = ${k + 1} AND {sql.Epoch} = ${k + 2}{(sql.Policy.HasStoreLimits ? "" : $" AND {sql.Due} IS NOT NULL")}",
            source,
            lease.Key,
            Uuid(lease.Owner.Value),
            PostgreSqlLeaseStore.Int64(lease.Epoch),
            Signal(WorkTopics.Due, source.Name)));
        await using (var reader = await ExecuteFencedAsync(batch, cancellationToken))
        {
            await signals.ReadAsync(reader, cancellationToken);
        }
        return update.RecordsAffected == 1;
    }

    /// <summary>
    /// Takes the shared generation fences of the superseding sources among <paramref name="sources"/> in the
    /// caller's transaction, in their fixed order, in one statement.
    /// </summary>
    public async ValueTask FenceAsync(WorkTransaction transaction, IReadOnlyCollection<IReconcileSource> sources, CancellationToken cancellationToken)
    {
        var work = PostgreSqlTransaction.From(transaction);
        var keys = sources.Where(s => s.Policy.Requests == RequestPolicy.Supersede).Select(s => LeaseSql.GenerationLockKey(leases.Schema, s.Name)).ToArray();
        if (keys.Length == 0)
        {
            return;
        }
        await using var batch = new NpgsqlBatch(work.Connection, work.Transaction);
        batch.BatchCommands.Add(Command(
            $"SELECT {LeaseSql.Quote(leases.Schema)}.generation_fence($1, false)",
            new NpgsqlParameter<string[]> { TypedValue = keys, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text }));
        await (await ExecuteFencedAsync(batch, cancellationToken)).DisposeAsync();
    }

    /// <summary>
    /// Increments the source's generation row in the caller's transaction and signals claimers on commit. On
    /// superseding sources it first takes the generation lock exclusively, waiting for open guarded transactions,
    /// which hold it shared; guarded transactions that start meanwhile wait for the invalidation to end.
    /// </summary>
    public async ValueTask<long> InvalidateAllAsync<TKey>(WorkTransaction transaction, ReconcileSource<TKey> source, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = PostgreSqlTransaction.From(transaction);
        var sql = leases.Sql(source.Leases);
        await using var batch = new NpgsqlBatch(work.Connection, work.Transaction);
        if (sql.Supersedes)
        {
            batch.BatchCommands.Add(new NpgsqlBatchCommand(sql.InvalidationFence));
        }
        batch.BatchCommands.Add(new NpgsqlBatchCommand(
            $"INSERT INTO {sql.Generations} AS g (source, generation) VALUES ({sql.GenerationName}, 1) "
            + "ON CONFLICT (source) DO UPDATE SET generation = g.generation + 1 RETURNING g.generation"));
        batch.BatchCommands.Add(Command(PostgreSqlSignalSql.Rows("$1"), Signal(WorkTopics.Due, source.Name)));
        await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
        if (sql.Supersedes)
        {
            await reader.NextResultAsync(cancellationToken);
        }
        await reader.ReadAsync(cancellationToken);
        var generation = reader.GetInt64(0);
        await signals.ReadAsync(reader, cancellationToken);
        return generation;
    }
}
