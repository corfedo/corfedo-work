using Corfedo.Work.PostgreSql.Signals;
using Npgsql;
using NpgsqlTypes;

namespace Corfedo.Work.PostgreSql.Leases;

internal sealed partial class PostgreSqlLeaseStore
{
    public async ValueTask<RenewResult> RenewAsync<TKey>(Lease<TKey> lease, CancellationToken cancellationToken)
        where TKey : notnull
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        return await RenewAsync(connection, null, lease, false, cancellationToken);
    }

    public ValueTask<RenewResult> RenewAsync<TKey>(WorkTransaction transaction, Lease<TKey> lease, bool progressed, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = PostgreSqlTransaction.From(transaction);
        return RenewAsync(work.Connection, work.Transaction, lease, progressed, cancellationToken);
    }

    /// <summary>
    /// Locks the record, then extends the lease if it is still valid by a clock reading taken after the lock is held.
    /// Without a transaction both statements run in the batch's implicit transaction. In the caller's transaction on a
    /// superseding source, the generation fence is taken first, before the row lock, since the renewal authorizes
    /// what that transaction commits, and the transaction must be READ COMMITTED; a renewal of its own only compares
    /// the generation. A renewal that marks progress also resets the takeovers.
    /// </summary>
    private async ValueTask<RenewResult> RenewAsync<TKey>(NpgsqlConnection connection, NpgsqlTransaction? transaction, Lease<TKey> lease, bool progressed, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var source = lease.Source;
        var sql = Sql(source);
        var k = sql.KeyCount;
        await using var batch = new NpgsqlBatch(connection, transaction);
        if ((transaction is null ? null : sql.GenerationFence) is { } fence)
        {
            batch.BatchCommands.Add(new NpgsqlBatchCommand(fence));
        }
        batch.BatchCommands.Add(KeyCommand($"SELECT 1 FROM {sql.Table} AS r WHERE {sql.KeyMatch(1)} FOR UPDATE OF r", source, lease.Key));
        batch.BatchCommands.Add(KeyCommand(
            $"UPDATE {sql.Table} AS r SET {LeaseSql.Bare(sql.Expires)} = clock_timestamp() + ${k + 3}{(progressed ? $", {LeaseSql.Bare(sql.Takeovers)} = 0" : "")} "
            + $"WHERE {sql.KeyMatch(1)} AND {sql.Valid(k + 1, k + 2)} "
            + $"RETURNING {sql.Cancel} IS NOT NULL, extract(epoch FROM {sql.Expires} - clock_timestamp())::float8",
            source,
            lease.Key,
            Uuid(lease.Owner.Value),
            Int64(lease.Epoch),
            Interval(source.Policy.Duration)));
        await using var reader = await ExecuteFencedAsync(batch, cancellationToken);
        for (var i = 1; i < batch.BatchCommands.Count; i++)
        {
            await reader.NextResultAsync(cancellationToken);
        }
        return await reader.ReadAsync(cancellationToken)
            ? new RenewResult(true, reader.GetBoolean(0), Seconds(reader.GetDouble(1)))
            : RenewResult.Lost;
    }

    public async ValueTask<bool> ReleaseAsync<TKey>(Lease<TKey> lease, Due due, bool failed, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var source = lease.Source;
        var sql = Sql(source);
        var k = sql.KeyCount;
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var batch = new NpgsqlBatch(connection);
        batch.BatchCommands.Add(KeyCommand($"SELECT 1 FROM {sql.Table} AS r WHERE {sql.KeyMatch(1)} FOR UPDATE OF r", source, lease.Key));
        var update = batch.BatchCommands.Count;
        if (sql.Job is not null)
        {
            // The released attempt ends in the run history before the release changes the record, and only while the
            // lease is valid: a stale release records nothing, and settlement or a takeover records the loss. A graceful
            // release returns the attempt and is Released; a failed one follows a lost hold and is LeaseLost, ended now.
            var outcome = $"CASE WHEN ${k + 3} THEN {(int)JobRunOutcome.LeaseLost} ELSE {(int)JobRunOutcome.Released} END";
            batch.BatchCommands.Add(KeyCommand(
                sql.RecordRun($"{sql.Table} AS r", $"{sql.KeyMatch(1)} AND {sql.Valid(k + 1, k + 2)} AND r.\"attempt_epoch\" = {sql.Epoch}", outcome, "NULL", "clock_timestamp()"),
                source,
                lease.Key,
                Uuid(lease.Owner.Value),
                Int64(lease.Epoch),
                Bool(failed)));
            update++;
        }
        batch.BatchCommands.Add(KeyCommand(
            $"UPDATE {sql.Table} AS r SET {LeaseSql.Bare(sql.Expires)} = NULL, {LeaseSql.Bare(sql.Due)} = {sql.DueExpression(k + 3, k + 4)}, "
            + $"{LeaseSql.Bare(sql.Takeovers)} = 0, {LeaseSql.Bare(sql.Failures)} = CASE WHEN ${k + 5} THEN {sql.Failures} + 1 ELSE 0 END"
            // A graceful release of a job claim returns the attempt that claim used; the execution stays started.
            + (sql.Job is null ? " " : $", \"attempts\" = CASE WHEN NOT ${k + 5} AND r.\"attempt_epoch\" = {sql.Epoch} THEN r.\"attempts\" - 1 ELSE r.\"attempts\" END, \"attempt_started_at\" = NULL ")
            + $"WHERE {sql.KeyMatch(1)} AND {sql.Valid(k + 1, k + 2)}",
            source,
            lease.Key,
            Uuid(lease.Owner.Value),
            Int64(lease.Epoch),
            Int32((int)due.Kind),
            Interval(due.Delay),
            Bool(failed)));
        batch.BatchCommands.Add(KeyCommand(
            $"{PostgreSqlSignalSql.Rows($"${k + 3}")} FROM {sql.Table} AS r WHERE {sql.KeyMatch(1)} AND {sql.Expires} IS NULL "
            + $"AND {sql.Owner} = ${k + 1} AND {sql.Epoch} = ${k + 2}{(sql.Policy.HasStoreLimits ? "" : $" AND {sql.Due} IS NOT NULL")}",
            source,
            lease.Key,
            Uuid(lease.Owner.Value),
            Int64(lease.Epoch),
            Signal(WorkTopics.Due, source.Name)));
        await signals.ExecuteAsync(batch, cancellationToken);
        return batch.BatchCommands[update].RecordsAffected == 1;
    }

    public async ValueTask<EndResult> EndAsync<TKey>(WorkTransaction transaction, Lease<TKey> lease, LeaseCompletion? completion, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = PostgreSqlTransaction.From(transaction);
        var source = lease.Source;
        var sql = Sql(source);
        var k = sql.KeyCount;
        await using var batch = new NpgsqlBatch(work.Connection, work.Transaction);
        if (sql.GenerationFence is { } fence)
        {
            batch.BatchCommands.Add(new NpgsqlBatchCommand(fence));
        }
        batch.BatchCommands.Add(KeyCommand($"SELECT 1 FROM {sql.Table} AS r WHERE {sql.KeyMatch(1)} FOR UPDATE OF r", source, lease.Key));
        var end = batch.BatchCommands.Count;
        batch.BatchCommands.Add(KeyCommand(
            $"UPDATE {sql.Table} AS r SET {LeaseSql.Bare(sql.Expires)} = NULL, {LeaseSql.Bare(sql.Due)} = NULL, {LeaseSql.Bare(sql.Cancel)} = NULL, "
            + $"{LeaseSql.Bare(sql.Takeovers)} = 0, {LeaseSql.Bare(sql.Failures)} = 0, "
            + $"{LeaseSql.Bare(sql.EndedId)} = ${k + 3}, {LeaseSql.Bare(sql.EndedDigest)} = ${k + 4} "
            + $"WHERE {sql.KeyMatch(1)} AND {sql.Valid(k + 1, k + 2)} RETURNING 1",
            source,
            lease.Key,
            Uuid(lease.Owner.Value),
            Int64(lease.Epoch),
            new NpgsqlParameter { Value = completion is null ? DBNull.Value : completion.Id, NpgsqlDbType = NpgsqlDbType.Uuid },
            new NpgsqlParameter { Value = completion is null ? DBNull.Value : completion.Digest, NpgsqlDbType = NpgsqlDbType.Bytea }));
        batch.BatchCommands.Add(KeyCommand($"SELECT {sql.Owner}, {sql.Epoch}, {sql.EndedId}, {sql.EndedDigest} FROM {sql.Table} AS r WHERE {sql.KeyMatch(1)}", source, lease.Key));
        // Every statement returns a row set, so the reader visits them in order.
        await using var reader = await ExecuteFencedAsync(batch, cancellationToken);
        for (var i = 0; i <= end; i++)
        {
            await reader.NextResultAsync(cancellationToken);
        }
        if (batch.BatchCommands[end].RecordsAffected == 1)
        {
            await reader.DisposeAsync();
            if (source.Policy.HasStoreLimits)
            {
                await NotifyAsync(work.Connection, work.Transaction, WorkTopics.Due, source.Name, cancellationToken);
            }
            return EndResult.Ended;
        }
        if (completion is not null
            && await reader.ReadAsync(cancellationToken)
            && !reader.IsDBNull(0) && reader.GetGuid(0) == lease.Owner.Value
            && reader.GetInt64(1) == lease.Epoch
            && !reader.IsDBNull(2) && reader.GetGuid(2) == completion.Id)
        {
            if (!reader.IsDBNull(3) && reader.GetFieldValue<byte[]>(3).AsSpan().SequenceEqual(completion.Digest))
            {
                return EndResult.Replayed;
            }
            throw new LeaseLostException($"A different completion {completion.Id} was already recorded for {source.Name} record {source.Key.Format(lease.Key)}.");
        }
        throw new LeaseLostException($"The lease on {source.Name} record {source.Key.Format(lease.Key)} at epoch {lease.Epoch} is no longer valid.");
    }
}
