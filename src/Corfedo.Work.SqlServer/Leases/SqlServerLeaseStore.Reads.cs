using System.Globalization;
using Corfedo.Work.Testing;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Leases;

internal sealed partial class SqlServerLeaseStore : ILeaseAgeing
{
    public async ValueTask<IReadOnlyList<KeyValuePair<TKey, LeaseRecord>>> ReadAsync<TKey>(WorkTransaction? transaction, LeaseSource<TKey> source, IReadOnlyList<TKey> keys, CancellationToken cancellationToken)
        where TKey : notnull => await ReadAsync(transaction, source, keys, _ => "", static (lease, _, _) => lease, cancellationToken);

    /// <summary>
    /// Reads the lease fields of the records of <paramref name="keys"/> followed by the <paramref name="columns"/> of
    /// the source's statements, which <paramref name="map"/> reads from the ordinal it is given, with one clock reading.
    /// Keys longer than their columns match no record. The keys returned are the requested values.
    /// </summary>
    internal async ValueTask<IReadOnlyList<KeyValuePair<TKey, TRecord>>> ReadAsync<TKey, TRecord>(
        WorkTransaction? transaction,
        LeaseSource<TKey> source,
        IReadOnlyList<TKey> keys,
        Func<LeaseSql, string> columns,
        Func<LeaseRecord, SqlDataReader, int, TRecord> map,
        CancellationToken cancellationToken)
        where TKey : notnull
    {
        if (source.Job is not null)
        {
            throw new ArgumentException($"Lease source {source.Name} holds jobs; jobs are read through their status.", nameof(source));
        }
        var sql = await SqlAsync(source, cancellationToken);
        List<TKey> fitting = [.. keys.Where(key => Fits(sql, source, key))];
        if (fitting.Count == 0)
        {
            return [];
        }
        var execution = sql.Reconcile is null ? sql.Execution : "CAST(NULL AS uniqueidentifier)";
        var text = $"""
            DECLARE @read_at datetime2(7) = SYSUTCDATETIME();
            SELECT {string.Join(", ", sql.KeyColumns.Split(", ").Select(c => "u." + c))}, @read_at,
                {sql.Due}, {sql.Owner}, {sql.Epoch}, {sql.Expires}, {sql.Takeovers}, {sql.Failures}, {sql.Cancel}, {execution}{columns(sql)}
            FROM {sql.KeyedRows()};
            """;
        var work = transaction is null ? null : SqlServerTransaction.From(transaction);
        await using var owned = work is null ? await OpenAsync(cancellationToken) : null;
        await using var command = KeysCommand(work?.Connection ?? owned!, work?.Transaction, sql, source, fitting, text);
        await using var reader = await command.ReaderAsync(cancellationToken);
        var records = new List<KeyValuePair<TKey, TRecord>>();
        var k = sql.KeyCount;
        while (await reader.NextRowAsync(cancellationToken))
        {
            var lease = new LeaseRecord(
                Tsql.Time(reader, k),
                Time(reader, k + 1),
                reader.IsDBNull(k + 2) ? null : new OwnerId(reader.GetGuid(k + 2)),
                reader.GetInt64(k + 3),
                Time(reader, k + 4),
                reader.GetInt32(k + 5),
                reader.GetInt32(k + 6),
                Time(reader, k + 7),
                reader.IsDBNull(k + 8) ? null : reader.GetGuid(k + 8));
            records.Add(new(ReadKey(source, reader), map(lease, reader, k + 9)));
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
