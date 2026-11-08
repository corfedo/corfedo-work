using Microsoft.Extensions.Logging;
using Npgsql;

namespace Corfedo.Work.PostgreSql.Leases;

internal sealed partial class PostgreSqlLeaseStore
{
    /// <summary>
    /// Settles expired leases one record per transaction, each time locking the earliest expired, unsettled record that
    /// no other transaction holds, that is not waiting for a settlement retry and that has not failed in this call.
    /// Locked and failing records therefore never keep later records from being settled. A failed settlement counts in
    /// <c>SettleFailures</c> and is retried after the source's settlement backoff; one whose handler found a row locked
    /// without waiting for it counts nothing and is left for a later call, as a locked record is.
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
        var sql = Sql(source);
        var failed = new List<TKey>();
        var settled = 0;
        while (settled < limit)
        {
            switch (await SettleNextAsync(sql, source, callbacks, failed, cancellationToken))
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

    /// <summary>Settles one expired epoch in its own transaction: lock without waiting, run the handler, record.</summary>
    private async Task<Settlement> SettleNextAsync<TKey>(LeaseSql sql, LeaseSource<TKey> source, LeaseCallbacks<TKey> callbacks, List<TKey> failed, CancellationToken cancellationToken)
        where TKey : notnull
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        LeaseExpiry<TKey> expiry;
        // The scan of the expiry index is bounded by now(), which is stable and so usable as an index bound; without it,
        // a pass that finds nothing read every valid lease. Each settlement runs in its own transaction, so now() is
        // recent. Skipping locked rows never waits, so the fresh clock reading is evaluated against the latest committed
        // version of a row this statement locks; it only becomes true with time, or changes by an update, which is
        // rechecked.
        await using (var select = new NpgsqlCommand(
            $"SELECT {sql.Select(sql.Owner, sql.Epoch, sql.PartitionText)} FROM {sql.Table} AS r "
            + $"WHERE {sql.Scope} AND {sql.Expires} <= now() AND {sql.Expires} <= clock_timestamp() AND {sql.Unsettled} AND NOT {sql.SettlementWaiting} "
            + $"AND NOT EXISTS (SELECT 1 FROM {sql.Unnest} WHERE {sql.KeyJoin}) "
            + $"ORDER BY {sql.Expires} LIMIT 1 FOR UPDATE OF r SKIP LOCKED",
            connection,
            transaction))
        {
            AddKeys(select.Parameters, source, failed);
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return Settlement.None;
            }
            var k = sql.KeyCount;
            expiry = new LeaseExpiry<TKey>(
                source,
                ReadKey(source, reader),
                new OwnerId(reader.GetGuid(k)),
                reader.GetInt64(k + 1),
                reader.IsDBNull(k + 2) ? null : reader.GetString(k + 2));
        }
        if (callbacks.Expired is { } handler)
        {
            const string savepoint = "corfedo_work_settlement";
            await transaction.SaveAsync(savepoint, cancellationToken);
            try
            {
                await handler(expiry, DbWorkTransaction.Of(transaction), cancellationToken);
            }
            catch (Exception exception) when (!cancellationToken.IsCancellationRequested && Busy(exception))
            {
                await transaction.RollbackAsync(CancellationToken.None);
                failed.Add(expiry.Key);
                LogCallbackBusy(logger, source.Name, source.Key.Format(expiry.Key));
                return Settlement.Failed;
            }
            catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
            {
                await transaction.RollbackAsync(savepoint, CancellationToken.None);
                await RecordSettlementFailureAsync(connection, transaction, sql, source, expiry.Key, cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                failed.Add(expiry.Key);
                LogSettlementFailed(logger, telemetry.Logged(exception), source.Name, source.Key.Format(expiry.Key), telemetry.Describe(exception));
                return Settlement.Failed;
            }
        }
        if (sql.Job is not null)
        {
            // The attempt of the expired lease ends in the run history at the lease's expiry.
            await using var lost = new NpgsqlCommand(
                sql.RecordRun($"{sql.Table} AS r", $"{sql.KeyMatch(1)} AND r.\"attempt_epoch\" = {sql.Epoch}", ((int)JobRunOutcome.LeaseLost).ToString(System.Globalization.CultureInfo.InvariantCulture), "NULL", sql.Expires),
                connection,
                transaction);
            AddKey(lost, source, expiry.Key);
            await lost.ExecuteNonQueryAsync(cancellationToken);
        }
        await using (var update = new NpgsqlCommand(
            $"UPDATE {sql.Table} AS r SET {sql.MarkSettled(sql.Epoch)}{(sql.Job is null ? "" : ", \"attempt_started_at\" = NULL")} WHERE {sql.KeyMatch(1)}",
            connection,
            transaction))
        {
            AddKey(update, source, expiry.Key);
            await update.ExecuteNonQueryAsync(cancellationToken);
        }
        // A settlement frees a slot of the source's limits, and makes the record claimable for claimers without the
        // expiry handler.
        await NotifyAsync(connection, transaction, WorkTopics.Due, source.Name, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return Settlement.Settled;
    }

    /// <summary>Counts a failed settlement of a locked record and sets when it may be retried.</summary>
    private static async Task RecordSettlementFailureAsync<TKey>(NpgsqlConnection connection, NpgsqlTransaction transaction, LeaseSql sql, LeaseSource<TKey> source, TKey key, CancellationToken cancellationToken)
        where TKey : notnull
    {
        await using var command = new NpgsqlCommand(
            $"UPDATE {sql.Table} AS r SET {LeaseSql.Bare(sql.SettleFailures)} = {sql.SettleFailures} + 1, "
            + $"{LeaseSql.Bare(sql.SettleRetryAt)} = clock_timestamp() + {sql.SettlementBackoff} WHERE {sql.KeyMatch(1)}",
            connection,
            transaction);
        AddKey(command, source, key);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    [LoggerMessage(EventId = 2, EventName = "lease.settlement_failed", Level = LogLevel.Warning, Message = "Settlement of the expired lease on {Source} record {Key} failed and will be retried: {Error}")]
    private static partial void LogSettlementFailed(ILogger logger, Exception? exception, string source, string key, string error);
}
