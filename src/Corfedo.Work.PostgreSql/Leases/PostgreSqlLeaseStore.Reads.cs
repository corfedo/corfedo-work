using Corfedo.Work.PostgreSql.Signals;
using Corfedo.Work.Testing;
using Npgsql;

namespace Corfedo.Work.PostgreSql.Leases;

internal sealed partial class PostgreSqlLeaseStore : ILeaseAgeing
{
    public async ValueTask<IReadOnlyList<LeaseRecord?>> ReadAsync<TKey>(WorkTransaction? transaction, LeaseSource<TKey> source, IReadOnlyList<TKey> keys, CancellationToken cancellationToken)
        where TKey : notnull => await ReadAsync(transaction, source, keys, "", static (lease, _, _) => lease, cancellationToken);

    /// <summary>
    /// Reads the lease fields of the records of <paramref name="keys"/> followed by <paramref name="columns"/>, which
    /// <paramref name="map"/> reads from the ordinal it is given, in the order of the keys, null for a key of no record.
    /// The clock is read once, by an uncorrelated subquery, which the statement evaluates after taking its snapshot.
    /// </summary>
    internal async ValueTask<IReadOnlyList<TRecord?>> ReadAsync<TKey, TRecord>(
        WorkTransaction? transaction,
        LeaseSource<TKey> source,
        IReadOnlyList<TKey> keys,
        string columns,
        Func<LeaseRecord, NpgsqlDataReader, int, TRecord> map,
        CancellationToken cancellationToken)
        where TKey : notnull
        where TRecord : class
    {
        if (source.Job is not null)
        {
            throw new ArgumentException($"Lease source {source.Name} holds jobs; jobs are read through their status.", nameof(source));
        }
        var records = new TRecord?[keys.Count];
        if (keys.Count == 0)
        {
            return records;
        }
        var sql = Sql(source);
        var execution = sql.Reconcile is null ? sql.Execution : "NULL::uuid";
        var text = $"SELECT u.ord, (SELECT clock_timestamp()), "
            + $"{sql.Due}, {sql.Owner}, {sql.Epoch}, {sql.Expires}, {sql.Takeovers}, {sql.Failures}, {sql.Cancel}, {execution}{columns} "
            + $"FROM {sql.OrderedUnnest} JOIN {sql.Table} AS r ON {sql.KeyJoin}";
        var work = transaction is null ? null : PostgreSqlTransaction.From(transaction);
        await using var owned = work is null ? await dataSource.OpenConnectionAsync(cancellationToken) : null;
        await using var command = new NpgsqlCommand(text, work?.Connection ?? owned, work?.Transaction);
        AddKeys(command.Parameters, source, keys);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var lease = new LeaseRecord(
                reader.GetFieldValue<DateTimeOffset>(1),
                Time(reader, 2),
                reader.IsDBNull(3) ? null : new OwnerId(reader.GetGuid(3)),
                reader.GetInt64(4),
                Time(reader, 5),
                reader.GetInt32(6),
                reader.GetInt32(7),
                Time(reader, 8),
                reader.IsDBNull(9) ? null : reader.GetGuid(9));
            records[reader.GetInt64(0) - 1] = map(lease, reader, 10);
        }
        return records;
    }

    internal static DateTimeOffset? Time(NpgsqlDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetFieldValue<DateTimeOffset>(ordinal);

    /// <summary>
    /// Moves the record's time fields back in one statement of its own transaction; null fields stay null. The signal
    /// rows are written in the same batch, so they commit with it.
    /// </summary>
    public async ValueTask<bool> AgeAsync<TKey>(LeaseSource<TKey> source, TKey key, TimeSpan by, CancellationToken cancellationToken)
        where TKey : notnull
    {
        if (source.Job is not null)
        {
            throw new ArgumentException($"Lease source {source.Name} holds jobs; jobs are not aged.", nameof(source));
        }
        var sql = Sql(source);
        var k = sql.KeyCount;
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
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var batch = new NpgsqlBatch(connection);
        batch.BatchCommands.Add(KeyCommand(
            $"UPDATE {sql.Table} AS r SET {string.Join(", ", fields.Select(f => $"{LeaseSql.Bare(f)} = {f} - ${k + 1}"))} WHERE {sql.KeyMatch(1)}",
            source,
            key,
            Interval(by)));
        batch.BatchCommands.Add(KeyCommand(
            $"{PostgreSqlSignalSql.Rows($"${k + 1}")} FROM {sql.Table} AS r WHERE {sql.KeyMatch(1)} AND {sql.Due} IS NOT NULL",
            source,
            key,
            Signal(WorkTopics.Due, source.Name)));
        await signals.ExecuteAsync(batch, cancellationToken);
        return batch.BatchCommands[0].RecordsAffected == 1;
    }
}
