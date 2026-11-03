using Npgsql;

namespace Corfedo.Work.PostgreSql.Leases;

internal sealed partial class PostgreSqlLeaseStore
{
    /// <summary>
    /// Checks the source's fields and types, and the indexes its statements use: a unique index on the key, an index
    /// led by the due time covering only unheld records (<c>WHERE LeaseExpiresAt IS NULL</c>), an index led by the
    /// lease expiry (claims of expired leases and next due time), and an index led by the owner (owner limit). Sources
    /// that settle have the stored generated column <c>LeaseOutstanding</c>, <c>CASE WHEN SettledEpoch &lt; LeaseEpoch
    /// THEN 1 END</c>, an index led by it and the lease expiry over held leases (<c>WHERE LeaseExpiresAt IS NOT
    /// NULL</c>, expiry settlement and next expiry), and where partition or spread limits are used an
    /// index led by it and the partition over held leases. Reconciliation sources also need their reconciliation fields
    /// and an index led by <c>AttemptedGeneration</c> over all records.
    /// </summary>
    public async ValueTask VerifyAsync<TKey>(LeaseSource<TKey> source, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var settles = source.Settles;
        var table = LeaseSql.Quote(source.Store.Schema) + "." + LeaseSql.Quote(source.Store.Name);
        var columns = new Dictionary<string, (string Type, bool NotNull, string? Generated)>(StringComparer.Ordinal);
        var indexes = new List<(bool Unique, bool Partial, bool UnheldOnly, bool HeldOnly, string[] Columns)>();
        await using (var batch = dataSource.CreateBatch())
        {
            batch.BatchCommands.Add(Command(
                "SELECT a.attname::text, t.typname::text, a.attnotnull, CASE WHEN a.attgenerated = 's' THEN pg_get_expr(d.adbin, d.adrelid) END "
                + "FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum "
                + "WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped",
                Text(table)));
            batch.BatchCommands.Add(Command(
                "SELECT i.indisunique, i.indpred IS NOT NULL, coalesce(pg_get_expr(i.indpred, i.indrelid) = '(' || quote_ident($2) || ' IS NULL)', false), "
                + "coalesce(pg_get_expr(i.indpred, i.indrelid) = '(' || quote_ident($2) || ' IS NOT NULL)', false), "
                + "ARRAY(SELECT coalesce(a.attname::text, '') FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, n) "
                + "LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum ORDER BY k.n) "
                + "FROM pg_index i WHERE i.indrelid = to_regclass($1) AND i.indisvalid",
                Text(table),
                Text(source.Fields.LeaseExpiresAt)));
            await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                columns[reader.GetString(0)] = (reader.GetString(1), reader.GetBoolean(2), reader.IsDBNull(3) ? null : reader.GetString(3));
            }
            await reader.NextResultAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                indexes.Add((reader.GetBoolean(0), reader.GetBoolean(1), reader.GetBoolean(2), reader.GetBoolean(3), reader.GetFieldValue<string[]>(4)));
            }
        }
        var problems = new List<string>();
        if (columns.Count == 0)
        {
            throw new InvalidOperationException($"Lease source {source.Name}: table {table} does not exist.");
        }
        void Require(string column, bool notNull, params string[] types)
        {
            if (!columns.TryGetValue(column, out var actual))
            {
                problems.Add($"field {column} is missing");
            }
            else if (!types.Contains(actual.Type))
            {
                problems.Add($"field {column} is {actual.Type}, expected {string.Join(" or ", types)}");
            }
            else if (notNull && !actual.NotNull)
            {
                problems.Add($"field {column} must be NOT NULL");
            }
        }
        foreach (var field in source.Key.Fields)
        {
            Require(field.Name, true, field.Type switch
            {
                KeyFieldType.String => ["text", "varchar"],
                KeyFieldType.Int32 => ["int4"],
                KeyFieldType.Int64 => ["int8"],
                _ => ["uuid"],
            });
        }
        var f = source.Fields;
        Require(f.DueAt, false, "timestamptz");
        Require(f.LeaseOwner, false, "uuid");
        Require(f.LeaseEpoch, true, "int8");
        Require(f.LeaseExpiresAt, false, "timestamptz");
        Require(f.Takeovers, true, "int4");
        Require(f.Failures, true, "int4");
        Require(f.CancelRequestedAt, false, "timestamptz");
        if (source.Reconcile is null)
        {
            Require(f.ExecutionId, false, "uuid");
        }
        Require(f.EndedCompletionId, false, "uuid");
        Require(f.EndedDigest, false, "bytea");
        if (settles)
        {
            Require(f.SettledEpoch, true, "int8");
            Require(f.LeaseOutstanding, false, "int4", "int2");
            if (columns.TryGetValue(f.LeaseOutstanding, out var outstanding) && !LeaseOutstanding.Matches(outstanding.Generated, f))
            {
                problems.Add($"field {f.LeaseOutstanding} must be a stored generated column, {LeaseOutstanding.Expression(f, LeaseSql.Quote)}");
            }
        }
        if (source.Policy.SettleExpiry)
        {
            Require(f.SettleFailures, true, "int4");
            Require(f.SettleRetryAt, false, "timestamptz");
        }
        var limitsPartition = source.Policy.PartitionLimit is not null || source.Policy.SpreadLimit is not null;
        if (source.Partition is { } partition)
        {
            if (!columns.TryGetValue(partition, out var actual))
            {
                problems.Add($"partition field {partition} is missing");
            }
            else if (limitsPartition && !actual.NotNull)
            {
                problems.Add($"partition field {partition} must be NOT NULL for partition and spread limits");
            }
        }
        var keyNames = source.Key.Fields.Select(k => k.Name).Order(StringComparer.Ordinal).ToArray();
        if (!indexes.Any(i => i.Unique && !i.Partial && i.Columns.Order(StringComparer.Ordinal).SequenceEqual(keyNames)))
        {
            problems.Add($"no unique index on the key ({string.Join(", ", source.Key.Fields.Select(k => k.Name))})");
        }
        void RequireLeading(string column, string purpose)
        {
            if (!indexes.Any(i => i.Columns.Length > 0 && i.Columns[0] == column))
            {
                problems.Add($"no index led by {column} ({purpose})");
            }
        }
        if (!indexes.Any(i => i.UnheldOnly && i.Columns.Length > 0 && i.Columns[0] == f.DueAt))
        {
            problems.Add($"no index led by {f.DueAt} WHERE {f.LeaseExpiresAt} IS NULL (claims and next due time of unheld records)");
        }
        RequireLeading(f.LeaseExpiresAt, "claims of expired leases and next due time");
        // Settlement and limit counts read unsettled leases only; led by the expiry alone they would pass every expired
        // lease already settled but not claimed again, so the cost of a burst would grow with its square.
        bool HeldLedBy(string second) => indexes.Any(i => i.HeldOnly && i.Columns.Length > 1 && i.Columns[0] == f.LeaseOutstanding && i.Columns[1] == second);
        if (settles && !HeldLedBy(f.LeaseExpiresAt))
        {
            problems.Add($"no index led by {f.LeaseOutstanding}, {f.LeaseExpiresAt} WHERE {f.LeaseExpiresAt} IS NOT NULL (expiry settlement and next expiry)");
        }
        if (limitsPartition && !HeldLedBy(source.Partition!))
        {
            problems.Add($"no index led by {f.LeaseOutstanding}, {source.Partition} WHERE {f.LeaseExpiresAt} IS NOT NULL (partition and spread limits)");
        }
        if (source.Policy.OwnerLimit is not null)
        {
            RequireLeading(f.LeaseOwner, "owner limit");
        }
        if (source.Reconcile is { Fields: var r })
        {
            Require(r.RequestedRevision, true, "int8");
            Require(r.ClaimedRevision, false, "int8");
            Require(r.ConvergedRevision, true, "int8");
            Require(r.FirstRequestedAt, false, "timestamptz");
            Require(r.NotBefore, false, "timestamptz");
            Require(r.AttemptedGeneration, true, "int8");
            Require(r.ConvergedGeneration, true, "int8");
            if (!indexes.Any(i => !i.Partial && i.Columns.Length > 0 && i.Columns[0] == r.AttemptedGeneration))
            {
                problems.Add($"no index led by {r.AttemptedGeneration} (claims after an invalidation)");
            }
        }
        if (problems.Count > 0)
        {
            throw new InvalidOperationException($"Lease source {source.Name} on {table}: {string.Join("; ", problems)}.");
        }
    }
}
