using System.Globalization;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace Corfedo.Work.SqlServer.Leases;

internal sealed partial class SqlServerLeaseStore
{
    /// <summary>
    /// Settles expired leases one record per transaction, each time locking the earliest expired, unsettled record that
    /// no other transaction holds, that is not waiting for a settlement retry and that has not failed in this call.
    /// Locked and failing records therefore never keep later records from being settled. A failed settlement counts in
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
        var sql = await SqlAsync(source, cancellationToken);
        var failed = new List<TKey>();
        var settled = 0;
        while (settled < limit)
        {
            switch (await RetryDeadlocksAsync(() => SettleNextAsync(sql, source, callbacks, failed, cancellationToken)))
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
        await using var connection = await OpenAsync(cancellationToken);
        await using var transaction = await BeginAsync(connection, cancellationToken);
        LeaseExpiry<TKey> expiry;
        // The scan reads the expiry index in its order from the earliest expiry, bounded by the clock, passes settled
        // leases by the epochs the index carries, and locks each row it finds through the key without waiting.
        // Skipping locked rows never waits, so the clock reading is evaluated against the latest committed version of
        // a row this statement locks; it only becomes true with time, or changes by an update, which is rechecked.
        string Expired(string a) =>
            $"{sql.ScopeOf(a)} AND {LeaseSql.On(a, sql.Expires)} IS NOT NULL AND {LeaseSql.On(a, sql.Expires)} <= SYSUTCDATETIME() AND {LeaseSql.On(a, sql.Settled)} < {LeaseSql.On(a, sql.Epoch)}";
        var into = $"DECLARE @expired TABLE ({string.Join(", ", sql.Shape.Keys.Select((k, i) => $"k{i} {k.Declaration} NOT NULL"))}, owner uniqueidentifier, epoch bigint, partition_key nvarchar(4000));\n";
        var notFailed = $"NOT EXISTS (SELECT 1 FROM @keys AS u WHERE {string.Join(" AND ", sql.Shape.Keys.Select((k, i) => $"x.{Tsql.Quote(k.Name)} = u.k{i}"))})";
        await using (var select = KeysCommand(connection, transaction, sql, source, failed,
            into + sql.ScanVariables
            + sql.LockScan("scan", "1", sql.Shape.ExpiryIndex, $"{Expired("x")} AND {notFailed}", $"{Expired("r")} AND NOT {sql.SettlementWaiting}", LeaseSql.On("x", sql.Expires),
                "@expired", sql.KeyColumns + ", owner, epoch, partition_key", sql.Select(sql.Owner, sql.Epoch, sql.PartitionText))
            + "SELECT * FROM @expired;"))
        {
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
        // The attempt of the expired lease ends in the run history at the lease's expiry. A settlement frees a slot of
        // the source's limits, and makes the record claimable for claimers without the expiry handler.
        var run = sql.Job is null
            ? ""
            : sql.RecordRun($"{sql.Table} AS r", $"{sql.KeyMatch} AND r.[attempt_epoch] = {sql.Epoch}", ((int)JobRunOutcome.LeaseLost).ToString(CultureInfo.InvariantCulture), "NULL", sql.Expires) + "\n";
        await using (var update = KeyCommand(connection, transaction, sql, source, expiry.Key,
            run
            + $"UPDATE r SET {sql.MarkSettled(sql.Epoch)}{(sql.Job is null ? "" : ", [attempt_started_at] = NULL")} FROM {sql.Table} AS r WHERE {sql.KeyMatch};\n"
            + signals.Insert,
            Corfedo.Work.SqlServer.Signals.SqlServerSignalSql.Parameters(WorkTopics.Due, source.Name)))
        {
            await update.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        return Settlement.Settled;
    }

    /// <summary>Counts a failed settlement of a locked record and sets when it may be retried.</summary>
    private static async Task RecordSettlementFailureAsync<TKey>(SqlConnection connection, SqlTransaction transaction, LeaseSql sql, LeaseSource<TKey> source, TKey key, CancellationToken cancellationToken)
        where TKey : notnull
    {
        await using var command = KeyCommand(connection, transaction, sql, source, key,
            $"UPDATE r SET {LeaseSql.Bare(sql.SettleFailures)} = {sql.SettleFailures} + 1, "
            + $"{LeaseSql.Bare(sql.SettleRetryAt)} = {Tsql.AddTicks("SYSUTCDATETIME()", sql.SettlementBackoff)} FROM {sql.Table} AS r WHERE {sql.KeyMatch};");
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    [LoggerMessage(EventId = 2, EventName = "lease.settlement_failed", Level = LogLevel.Warning, Message = "Settlement of the expired lease on {Source} record {Key} failed and will be retried: {Error}")]
    private static partial void LogSettlementFailed(ILogger logger, Exception? exception, string source, string key, string error);
}
