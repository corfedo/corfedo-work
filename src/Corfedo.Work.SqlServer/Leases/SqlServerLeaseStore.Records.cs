using System.Globalization;
using Corfedo.Work.SqlServer.Signals;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Leases;

internal sealed partial class SqlServerLeaseStore
{
    public async ValueTask<bool> ScheduleAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, TKey key, Due due, Guid? execution, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = SqlServerTransaction.From(transaction);
        var sql = await SqlAsync(source, cancellationToken);
        if (sql.Reconcile is not null)
        {
            throw new ArgumentException($"Lease source {source.Name} holds reconciliation records; they are made due by requests and invalidations, not schedules.", nameof(source));
        }
        var text = $"""
            UPDATE r SET {LeaseSql.Bare(sql.Due)} = {sql.DueExpression}, {LeaseSql.Bare(sql.Execution)} = COALESCE(@execution, {sql.Execution})
            FROM {sql.Table} AS r WHERE {sql.KeyMatch};
            DECLARE @scheduled int = @@ROWCOUNT;
            {signals.InsertFrom(WorkTopics.Due, "@source", $"{sql.Table} AS r WHERE {sql.KeyMatch} AND {sql.Due} IS NOT NULL")}SELECT @scheduled;
            """;
        await using var command = KeyCommand(work.Connection, work.Transaction, sql, source, key, text,
            Tsql.Int32("@due_kind", (int)due.Kind),
            Tsql.Ticks("@due_delay", due.Delay),
            Tsql.Guid("@execution", execution),
            Tsql.Text("@source", source.Name, 1024));
        return Convert.ToInt32(await command.ScalarAsync(cancellationToken), CultureInfo.InvariantCulture) == 1;
    }

    /// <summary>Sets <c>CancelRequestedAt</c> and signals the process of the record's holder, if it is held.</summary>
    public async ValueTask<bool> RequestCancelAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, TKey key, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var sql = await SqlAsync(source, cancellationToken);
        return await SetCancelAsync(
            transaction,
            sql,
            source,
            key,
            $"COALESCE({sql.Cancel}, SYSUTCDATETIME())",
            signals.InsertFrom(WorkTopics.Cancel, SqlServerSignalSql.CancelKey(sql.Owner, "@source"), $"{sql.Table} AS r WHERE {sql.KeyMatch} AND {sql.Expires} IS NOT NULL"),
            cancellationToken);
    }

    public async ValueTask<bool> ClearCancelAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, TKey key, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var sql = await SqlAsync(source, cancellationToken);
        return await SetCancelAsync(
            transaction,
            sql,
            source,
            key,
            "NULL",
            signals.InsertFrom(WorkTopics.Due, "@source", $"{sql.Table} AS r WHERE {sql.KeyMatch}"),
            cancellationToken);
    }

    private static async Task<bool> SetCancelAsync<TKey>(WorkTransaction transaction, LeaseSql sql, LeaseSource<TKey> source, TKey key, string value, string notify, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = SqlServerTransaction.From(transaction);
        var text = $"""
            UPDATE r SET {LeaseSql.Bare(sql.Cancel)} = {value} FROM {sql.Table} AS r WHERE {sql.KeyMatch};
            DECLARE @changed int = @@ROWCOUNT;
            {notify}SELECT @changed;
            """;
        await using var command = KeyCommand(work.Connection, work.Transaction, sql, source, key, text, Tsql.Text("@source", source.Name, 1024));
        return Convert.ToInt32(await command.ScalarAsync(cancellationToken), CultureInfo.InvariantCulture) == 1;
    }

    public async ValueTask<TimeSpan?> NextDueAsync<TKey>(LeaseSource<TKey> source, DateTimeOffset cutoff, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var sql = await SqlAsync(source, cancellationToken);
        // Bounded index lookups instead of a pass over every held record; the result only schedules a wake-up, and every
        // claim re-evaluates with a later clock reading. The claim scanned unheld records due and expired leases at or
        // before its cutoff (@cutoff), so what it did not claim among them is locked or held back by limits and left out.
        // Counted are unheld records due after the cutoff, on job sources per accepted argument version; expired leases
        // whose takeover backoff ends after it; and the next lease to expire, a lower bound for valid leases, whose
        // holders usually renew them before then. A time at or before now, for a record that became claimable after the
        // cutoff, gives zero, so the caller claims again.
        var takeover = $"GREATEST({sql.Due}, {Tsql.AddTicks(sql.Expires, sql.TakeoverBackoff)})";
        var unheld = sql.Job is null ? $"{sql.Expires} IS NULL" : $"{sql.Expires} IS NULL AND {sql.Due} IS NOT NULL";
        var due = string.Concat(sql.DueRanges.Select(range =>
            $"SELECT MIN({sql.Due}) FROM {sql.Table} AS r WITH (INDEX({Tsql.Quote(sql.Shape.DueIndex)})) WHERE {range} AND {sql.Eligible} AND {unheld} AND {sql.Due} > @cutoff UNION ALL "));
        var text = $"SELECT {Tsql.Micros("SYSUTCDATETIME()", "MIN(t)")} FROM ("
            + due
            + $"SELECT MIN({takeover}) FROM {sql.Table} AS r WITH (INDEX({Tsql.Quote(sql.Shape.ExpiryIndex)})) "
            + $"WHERE {sql.Eligible} AND {sql.Expires} IS NOT NULL AND {sql.Expires} <= SYSUTCDATETIME() AND {sql.Due} IS NOT NULL AND {takeover} > @cutoff "
            + $"UNION ALL SELECT MIN({sql.Expires}) FROM {sql.Table} AS r WITH (INDEX({Tsql.Quote(sql.Shape.ExpiryIndex)})) "
            + $"WHERE {sql.Eligible} AND {sql.Expires} IS NOT NULL AND {sql.Expires} > SYSUTCDATETIME() AND {sql.Due} IS NOT NULL) AS next (t);";
        return await ScalarMicrosAsync(text, cancellationToken, Tsql.Time("@cutoff", cutoff));
    }

    public async ValueTask<TimeSpan?> NextExpiryAsync<TKey>(LeaseSource<TKey> source, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var sql = await SqlAsync(source, cancellationToken);
        // The next unsettled lease to expire, from the expiry index, and the earliest retry of expired leases whose
        // settlement failed. A result at or before now means an expired lease is due but its record is locked.
        var retry = sql.Policy.SettleExpiry ? $"GREATEST({sql.Expires}, COALESCE({sql.SettleRetryAt}, {sql.Expires}))" : sql.Expires;
        var hint = $" WITH (INDEX({Tsql.Quote(sql.Shape.ExpiryIndex)}))";
        var text = $"SELECT {Tsql.Micros("SYSUTCDATETIME()", "MIN(t)")} FROM ("
            + $"SELECT MIN({sql.Expires}) FROM {sql.Table} AS r{hint} WHERE {sql.Scope} AND {sql.Expires} IS NOT NULL AND {sql.Expires} > SYSUTCDATETIME() AND {sql.Settled} < {sql.Epoch} "
            + $"UNION ALL SELECT MIN({retry}) FROM {sql.Table} AS r{hint} WHERE {sql.Scope} AND {sql.Expires} IS NOT NULL AND {sql.Expires} <= SYSUTCDATETIME() AND {sql.Settled} < {sql.Epoch}) AS next (t);";
        return await ScalarMicrosAsync(text, cancellationToken);
    }

    private async Task<TimeSpan?> ScalarMicrosAsync(string text, CancellationToken cancellationToken, params SqlParameter[] parameters)
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var command = Tsql.Command(connection, null, text, parameters);
        var value = await command.ScalarAsync(cancellationToken);
        return value is long micros ? Tsql.FromMicros(micros) : null;
    }
}
