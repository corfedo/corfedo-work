using System.Globalization;
using Corfedo.Work.Testing;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Leases;

internal sealed partial class SqlServerLeaseStore : ILeaseAgeing
{
    public async ValueTask<IReadOnlyList<LeaseRecord?>> ReadAsync<TKey>(WorkTransaction? transaction, LeaseSource<TKey> source, IReadOnlyList<TKey> keys, CancellationToken cancellationToken)
        where TKey : notnull => await ReadAsync(transaction, source, keys, _ => "", static (lease, _, _) => lease, static (lease, at) => lease with { ReadAt = at }, cancellationToken);

    /// <summary>
    /// Reads the lease fields of the records of <paramref name="keys"/> followed by the <paramref name="columns"/> of
    /// the source's statements, which <paramref name="map"/> reads from the ordinal it is given, in the order of the
    /// keys, null for a key of no record; keys longer than their columns match no record. Each key's position comes
    /// from the key set, whose keys <see cref="LeaseSql.KeyRows"/> reads from each one wrapped in an array of its own,
    /// so the caller's keys identify the records whatever form the columns return them in. The clock
    /// is read by a statement after the records', so no record read holds a time after it, and
    /// <paramref name="stamp"/> sets it.
    /// </summary>
    internal async ValueTask<IReadOnlyList<TRecord?>> ReadAsync<TKey, TRecord>(
        WorkTransaction? transaction,
        LeaseSource<TKey> source,
        IReadOnlyList<TKey> keys,
        Func<LeaseSql, string> columns,
        Func<LeaseRecord, SqlDataReader, int, TRecord> map,
        Func<TRecord, DateTimeOffset, TRecord> stamp,
        CancellationToken cancellationToken)
        where TKey : notnull
        where TRecord : class
    {
        if (source.Job is not null)
        {
            throw new ArgumentException($"Lease source {source.Name} holds jobs; jobs are read through their status.", nameof(source));
        }
        var records = new TRecord?[keys.Count];
        var sql = await SqlAsync(source, cancellationToken);
        var positions = Enumerable.Range(0, keys.Count).Where(i => Fits(sql, source, keys[i])).ToArray();
        if (positions.Length == 0)
        {
            return records;
        }
        var execution = sql.Reconcile is null ? sql.Execution : "CAST(NULL AS uniqueidentifier)";
        var text = $"""
            SELECT CAST(j.[key] AS int), {sql.Due}, {sql.Owner}, {sql.Epoch}, {sql.Expires}, {sql.Takeovers}, {sql.Failures}, {sql.Cancel}, {execution}{columns(sql)}
            FROM OPENJSON(@keyset) AS j CROSS APPLY {sql.KeyRows("CONCAT(N'[', j.[value], N']')")} AS u
                INNER LOOP JOIN {sql.Table} AS r WITH (INDEX({sql.KeyHint}), FORCESEEK) ON {sql.KeyJoin};
            SELECT SYSUTCDATETIME();
            """;
        var work = transaction is null ? null : SqlServerTransaction.From(transaction);
        await using var owned = work is null ? await OpenAsync(cancellationToken) : null;
        await using var command = KeysCommand(work?.Connection ?? owned!, work?.Transaction, sql, source, positions.Select(i => keys[i]), text);
        await using var reader = await command.ReaderAsync(cancellationToken);
        var read = new List<int>();
        while (await reader.NextRowAsync(cancellationToken))
        {
            var position = positions[reader.GetInt32(0)];
            var lease = new LeaseRecord(
                default,
                Time(reader, 1),
                reader.IsDBNull(2) ? null : new OwnerId(reader.GetGuid(2)),
                reader.GetInt64(3),
                Time(reader, 4),
                reader.GetInt32(5),
                reader.GetInt32(6),
                Time(reader, 7),
                reader.IsDBNull(8) ? null : reader.GetGuid(8));
            records[position] = map(lease, reader, 9);
            read.Add(position);
        }
        await reader.NextSetAsync(cancellationToken);
        await reader.NextRowAsync(cancellationToken);
        var at = Tsql.Time(reader, 0);
        foreach (var position in read)
        {
            records[position] = stamp(records[position]!, at);
        }
        return records;
    }

    internal static DateTimeOffset? Time(SqlDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : Tsql.Time(reader, ordinal);

    /// <summary>
    /// Moves the record's time fields back in one statement of a transaction of its own; null fields stay null. The
    /// signal row is written in the same transaction, so it commits with it.
    /// </summary>
    public async ValueTask<bool> AgeAsync<TKey>(LeaseSource<TKey> source, TKey key, TimeSpan by, CancellationToken cancellationToken)
        where TKey : notnull
    {
        if (source.Job is not null)
        {
            throw new ArgumentException($"Lease source {source.Name} holds jobs; jobs are not aged.", nameof(source));
        }
        var sql = await SqlAsync(source, cancellationToken);
        if (!Fits(sql, source, key))
        {
            return false;
        }
        var fields = new List<string> { sql.Due, sql.Expires, sql.Cancel };
        if (sql.Policy.SettleExpiry)
        {
            fields.Add(sql.SettleRetryAt);
        }
        if (sql.Reconcile is not null)
        {
            fields.Add(sql.NotBefore);
            fields.Add(sql.FirstRequestedAt);
        }
        var body = $"""
            DECLARE @aged int;
            UPDATE r SET {string.Join(", ", fields.Select(f => $"{LeaseSql.Bare(f)} = {Tsql.AddTicks(f, "@by")}"))}
            FROM {sql.Table} AS r WHERE {sql.KeyMatch};
            SET @aged = @@ROWCOUNT;
            {signals.InsertFrom(WorkTopics.Due, "@source", $"{sql.Table} AS r WHERE {sql.KeyMatch} AND {sql.Due} IS NOT NULL")}
            """;
        return await RetryDeadlocksAsync(async () =>
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var command = KeyCommand(connection, null, sql, source, key, Tsql.OwnTransaction(body) + "SELECT @aged;",
                Tsql.Int64("@by", -by.Ticks),
                Tsql.Text("@source", source.Name, 1024));
            return Convert.ToInt32(await command.ScalarAsync(cancellationToken), CultureInfo.InvariantCulture) == 1;
        });
    }
}
