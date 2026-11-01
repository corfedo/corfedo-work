using Corfedo.Work.PostgreSql.Signals;
using Npgsql;
using NpgsqlTypes;

namespace Corfedo.Work.PostgreSql.Leases;

internal sealed partial class PostgreSqlLeaseStore
{
    public async ValueTask<bool> ScheduleAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, TKey key, Due due, Guid? execution, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = PostgreSqlTransaction.From(transaction);
        var sql = Sql(source);
        var k = sql.KeyCount;
        if (sql.Reconcile is not null)
        {
            throw new ArgumentException($"Lease source {source.Name} holds reconciliation records; they are made due by requests and invalidations, not schedules.", nameof(source));
        }
        await using var batch = new NpgsqlBatch(work.Connection, work.Transaction);
        batch.BatchCommands.Add(KeyCommand(
            $"UPDATE {sql.Table} AS r SET {LeaseSql.Bare(sql.Due)} = {sql.DueExpression(k + 1, k + 2)}, "
            + $"{LeaseSql.Bare(sql.Execution)} = COALESCE(${k + 3}, {sql.Execution}) WHERE {sql.KeyMatch(1)}",
            source,
            key,
            Int32((int)due.Kind),
            Interval(due.Delay),
            new NpgsqlParameter { Value = execution is { } id ? id : DBNull.Value, NpgsqlDbType = NpgsqlDbType.Uuid }));
        batch.BatchCommands.Add(KeyCommand(
            $"SELECT pg_notify(${k + 1}, ${k + 2}) FROM {sql.Table} AS r WHERE {sql.KeyMatch(1)} AND {sql.Due} IS NOT NULL",
            source,
            key,
            Signal(WorkTopics.Due, source.Name)));
        await batch.ExecuteNonQueryAsync(cancellationToken);
        return batch.BatchCommands[0].RecordsAffected == 1;
    }

    /// <summary>Sets <c>CancelRequestedAt</c> and signals the process of the record's holder, if it is held.</summary>
    public ValueTask<bool> RequestCancelAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, TKey key, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var sql = Sql(source);
        return SetCancelAsync(
            transaction,
            source,
            key,
            $"COALESCE({sql.Cancel}, clock_timestamp())",
            k => $"SELECT pg_notify(${k + 1}, {PostgreSqlSignalSql.CancelExpression(sql.Owner, $"${k + 2}")}) FROM {sql.Table} AS r WHERE {sql.KeyMatch(1)} AND {sql.Expires} IS NOT NULL",
            Text(source.Name),
            cancellationToken);
    }

    public ValueTask<bool> ClearCancelAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, TKey key, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var sql = Sql(source);
        return SetCancelAsync(
            transaction,
            source,
            key,
            "NULL",
            k => $"SELECT pg_notify(${k + 1}, ${k + 2}) FROM {sql.Table} AS r WHERE {sql.KeyMatch(1)}",
            PostgreSqlSignalSql.PayloadParameter(WorkTopics.Due, source.Name),
            cancellationToken);
    }

    private async ValueTask<bool> SetCancelAsync<TKey>(
        WorkTransaction transaction,
        LeaseSource<TKey> source,
        TKey key,
        string value,
        Func<int, string> notify,
        NpgsqlParameter payload,
        CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = PostgreSqlTransaction.From(transaction);
        var sql = Sql(source);
        var k = sql.KeyCount;
        await using var batch = new NpgsqlBatch(work.Connection, work.Transaction);
        batch.BatchCommands.Add(KeyCommand($"UPDATE {sql.Table} AS r SET {LeaseSql.Bare(sql.Cancel)} = {value} WHERE {sql.KeyMatch(1)}", source, key));
        batch.BatchCommands.Add(KeyCommand(notify(k), source, key, signals.ChannelParameter(), payload));
        await batch.ExecuteNonQueryAsync(cancellationToken);
        return batch.BatchCommands[0].RecordsAffected == 1;
    }

    public async ValueTask<TimeSpan?> NextDueAsync<TKey>(LeaseSource<TKey> source, DateTimeOffset cutoff, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var sql = Sql(source);
        // Bounded index lookups instead of a pass over every held record; the result only schedules a wake-up, and every
        // claim re-evaluates with clock_timestamp(). The claim scanned unheld records due and expired leases at or before
        // its cutoff ($1), so what it did not claim among them is locked or held back by limits and left out. Counted
        // are unheld records due after the cutoff, on job sources per accepted argument version; expired leases whose
        // takeover backoff ends after it; and the next lease to expire, a lower bound for valid leases, whose holders
        // usually renew them before then. A time at or before now, for a record that became claimable after the
        // cutoff, gives zero, so the caller claims again.
        var takeover = $"GREATEST({sql.Due}, {sql.Expires} + {sql.TakeoverBackoff})";
        var unheld = string.Concat(sql.DueRanges.Select(range =>
            $"(SELECT {sql.Due} AS t FROM {sql.Table} AS r WHERE {range} AND {sql.Eligible} AND {sql.Due} > $1 AND {sql.Expires} IS NULL "
            + $"ORDER BY {sql.Due} LIMIT 1) UNION ALL "));
        var text = $"SELECT extract(epoch FROM min(t) - clock_timestamp())::float8 FROM ("
            + unheld
            + $"(SELECT min({takeover}) FROM {sql.Table} AS r "
            + $"WHERE {sql.Eligible} AND {sql.Expires} <= now() AND {sql.Due} IS NOT NULL AND {takeover} > $1) "
            + $"UNION ALL (SELECT {sql.Expires} FROM {sql.Table} AS r WHERE {sql.Eligible} AND {sql.Expires} > now() AND {sql.Due} IS NOT NULL "
            + $"ORDER BY {sql.Expires} LIMIT 1)) AS next";
        return await ScalarSecondsAsync(text, cancellationToken, new NpgsqlParameter<DateTimeOffset> { TypedValue = cutoff.ToUniversalTime(), NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.TimestampTz });
    }

    public async ValueTask<TimeSpan?> NextExpiryAsync<TKey>(LeaseSource<TKey> source, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var sql = Sql(source);
        // The next unsettled lease to expire, from the expiry index, and the earliest retry of expired leases whose
        // settlement failed. A result at or before now means an expired lease is due but its record is locked.
        var retry = sql.Policy.SettleExpiry ? $"GREATEST({sql.Expires}, COALESCE({sql.SettleRetryAt}, {sql.Expires}))" : sql.Expires;
        var text = $"SELECT extract(epoch FROM min(t) - clock_timestamp())::float8 FROM ("
            + $"(SELECT {sql.Expires} AS t FROM {sql.Table} AS r WHERE {sql.Scope} AND {sql.Expires} > now() AND {sql.Settled} < {sql.Epoch} "
            + $"ORDER BY {sql.Expires} LIMIT 1) "
            + $"UNION ALL (SELECT min({retry}) FROM {sql.Table} AS r WHERE {sql.Scope} AND {sql.Expires} <= now() AND {sql.Settled} < {sql.Epoch})) AS next";
        return await ScalarSecondsAsync(text, cancellationToken);
    }

    private async Task<TimeSpan?> ScalarSecondsAsync(string text, CancellationToken cancellationToken, params NpgsqlParameter[] parameters)
    {
        await using var command = dataSource.CreateCommand(text);
        command.Parameters.AddRange(parameters);
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is double seconds ? Seconds(seconds) : null;
    }
}
