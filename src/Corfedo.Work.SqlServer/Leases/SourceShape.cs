using System.Data;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Leases;

/// <summary>A key field's column: its declaration for table variables and the parameter type that matches it.</summary>
internal sealed record KeyColumn(string Name, string Declaration, SqlDbType Type, int Size);

/// <summary>
/// What the statements of a source need from its table beyond the source definition: the key columns' exact types and
/// collations, so key parameters and table variables compare without converting the column, and the names of the
/// indexes the claim and settlement scans are bound to.
/// </summary>
internal sealed record SourceShape(
    IReadOnlyList<KeyColumn> Keys,
    string PartitionDeclaration,
    string DueIndex,
    string ExpiryIndex,
    string? PartitionIndex,
    string? OwnerIndex,
    string? GenerationIndex)
{
    /// <summary>The shape of the framework's job table.</summary>
    public static SourceShape Jobs { get; } = new(
        [new KeyColumn("id", "uniqueidentifier", SqlDbType.UniqueIdentifier, 0)],
        "nvarchar(400) COLLATE " + Tsql.Binary,
        "jobs_due",
        "jobs_expiry",
        "jobs_partition",
        null,
        null);
}

/// <summary>Reads and verifies a source's table in the catalog.</summary>
internal static class SourceCatalog
{
    private sealed record Column(string Type, short MaxLength, string? Collation, bool NotNull);

    private sealed record Index(string Name, bool Unique, string? Filter, string[] Keys, string[] Included);

    /// <summary>
    /// Checks the source's fields and types and the indexes its statements use, and returns its shape. Required are a
    /// unique index on the key; an index led by the due time covering only unheld records (<c>WHERE LeaseExpiresAt IS
    /// NULL</c>); an index led by the lease expiry, with no filter or <c>WHERE LeaseExpiresAt IS NOT NULL</c>, which on
    /// sources that settle expiry also carries SettledEpoch and LeaseEpoch, since SQL Server filtered indexes cannot
    /// compare two columns; where partition or spread limits are used an index led by the partition with
    /// <c>WHERE LeaseExpiresAt IS NOT NULL</c> carrying SettledEpoch and LeaseEpoch, and LeaseOwner for spread limits;
    /// an index led by the owner for owner limits; and on reconciliation sources an index led by AttemptedGeneration
    /// without a filter. The table must not escalate locks: a table lock would hold off every claim, which skips only
    /// locked rows.
    /// </summary>
    /// <exception cref="InvalidOperationException">The table, a field or an index is missing, has the wrong type, or the table escalates locks.</exception>
    public static async Task<SourceShape> ReadAsync<TKey>(string connectionString, LeaseSource<TKey> source, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var table = Tsql.Quote(source.Store.Schema) + "." + Tsql.Quote(source.Store.Name);
        var columns = new Dictionary<string, Column>(StringComparer.Ordinal);
        var indexes = new List<Index>();
        string? escalation;
        await using (var connection = new SqlConnection(connectionString))
        {
            await connection.OpenAsync(cancellationToken);
            await using var command = Tsql.Command(connection, null, """
                DECLARE @id int = OBJECT_ID(@table, N'U');
                SELECT lock_escalation_desc FROM sys.tables WHERE object_id = @id;
                SELECT c.name, t.name, c.max_length, c.collation_name, CAST(CASE WHEN c.is_nullable = 0 THEN 1 ELSE 0 END AS bit)
                FROM sys.columns AS c JOIN sys.types AS t ON t.user_type_id = c.user_type_id WHERE c.object_id = @id;
                SELECT i.index_id, i.name, i.is_unique, i.filter_definition FROM sys.indexes AS i
                WHERE i.object_id = @id AND i.type IN (1, 2) AND i.is_disabled = 0 AND i.is_hypothetical = 0;
                SELECT ic.index_id, c.name, ic.is_included_column FROM sys.index_columns AS ic
                JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                WHERE ic.object_id = @id ORDER BY ic.index_id, ic.is_included_column, ic.key_ordinal, ic.index_column_id;
                """,
                Tsql.Text("@table", table, 600));
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            escalation = await reader.ReadAsync(cancellationToken) ? reader.GetString(0) : null;
            await reader.NextResultAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                columns[reader.GetString(0)] = new Column(reader.GetString(1), reader.GetInt16(2), reader.IsDBNull(3) ? null : reader.GetString(3), reader.GetBoolean(4));
            }
            await reader.NextResultAsync(cancellationToken);
            var names = new Dictionary<int, (string Name, bool Unique, string? Filter)>();
            while (await reader.ReadAsync(cancellationToken))
            {
                names[reader.GetInt32(0)] = (reader.GetString(1), reader.GetBoolean(2), reader.IsDBNull(3) ? null : reader.GetString(3));
            }
            await reader.NextResultAsync(cancellationToken);
            var keys = new Dictionary<int, (List<string> Keys, List<string> Included)>();
            while (await reader.ReadAsync(cancellationToken))
            {
                var id = reader.GetInt32(0);
                if (!keys.TryGetValue(id, out var lists))
                {
                    keys[id] = lists = ([], []);
                }
                (reader.GetBoolean(2) ? lists.Included : lists.Keys).Add(reader.GetString(1));
            }
            foreach (var (id, index) in names)
            {
                var lists = keys.GetValueOrDefault(id, ([], []));
                indexes.Add(new Index(index.Name, index.Unique, index.Filter, [.. lists.Keys], [.. lists.Included]));
            }
        }
        if (escalation is null)
        {
            throw new InvalidOperationException($"Lease source {source.Name}: table {table} does not exist.");
        }
        var problems = new List<string>();
        if (escalation != "DISABLE")
        {
            problems.Add($"the table escalates locks ({escalation}); claims skip locked rows and cannot skip a table lock, so disable it with ALTER TABLE {table} SET (LOCK_ESCALATION = DISABLE)");
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
        string[] times = ["datetime2", "datetimeoffset"];
        foreach (var field in source.Key.Fields)
        {
            Require(field.Name, true, field.Type switch
            {
                KeyFieldType.String => ["nvarchar", "varchar", "nchar", "char"],
                KeyFieldType.Int32 => ["int"],
                KeyFieldType.Int64 => ["bigint"],
                _ => ["uniqueidentifier"],
            });
        }
        var f = source.Fields;
        Require(f.DueAt, false, times);
        Require(f.LeaseOwner, false, "uniqueidentifier");
        Require(f.LeaseEpoch, true, "bigint");
        Require(f.LeaseExpiresAt, false, times);
        Require(f.Takeovers, true, "int");
        Require(f.Failures, true, "int");
        Require(f.CancelRequestedAt, false, times);
        if (source.Reconcile is null)
        {
            Require(f.ExecutionId, false, "uniqueidentifier");
        }
        Require(f.EndedCompletionId, false, "uniqueidentifier");
        Require(f.EndedDigest, false, "varbinary");
        if (source.Settles)
        {
            Require(f.SettledEpoch, true, "bigint");
        }
        if (source.Policy.SettleExpiry)
        {
            Require(f.SettleFailures, true, "int");
            Require(f.SettleRetryAt, false, times);
        }
        var limitsPartition = source.Policy.PartitionLimit is not null || source.Policy.SpreadLimit is not null;
        Column? partitionColumn = null;
        if (source.Partition is { } partition)
        {
            if (!columns.TryGetValue(partition, out partitionColumn))
            {
                problems.Add($"partition field {partition} is missing");
            }
            else if (limitsPartition && !partitionColumn.NotNull)
            {
                problems.Add($"partition field {partition} must be NOT NULL for partition and spread limits");
            }
        }
        var keyNames = source.Key.Fields.Select(k => k.Name).Order(StringComparer.Ordinal).ToArray();
        if (!indexes.Any(i => i.Unique && i.Filter is null && i.Keys.Order(StringComparer.Ordinal).SequenceEqual(keyNames)))
        {
            problems.Add($"no unique index on the key ({string.Join(", ", source.Key.Fields.Select(k => k.Name))})");
        }
        bool Covers(Index index, params string[] fields) => fields.All(c => index.Keys.Contains(c) || index.Included.Contains(c));
        var unheld = Normalize($"{f.LeaseExpiresAt} IS NULL");
        var held = Normalize($"{f.LeaseExpiresAt} IS NOT NULL");
        var due = indexes.FirstOrDefault(i => i.Keys.FirstOrDefault() == f.DueAt && Normalize(i.Filter) == unheld);
        if (due is null)
        {
            problems.Add($"no index led by {f.DueAt} WHERE {f.LeaseExpiresAt} IS NULL (claims and next due time of unheld records)");
        }
        // Settlement reads unsettled expired leases only: the index carries the epochs, so settled ones are passed over
        // in the index without reading the table.
        string[] epochs = source.Settles ? [f.SettledEpoch, f.LeaseEpoch] : [];
        var expiry = indexes
            .Where(i => i.Keys.FirstOrDefault() == f.LeaseExpiresAt && (i.Filter is null || Normalize(i.Filter) == held))
            .OrderByDescending(i => Covers(i, epochs))
            .FirstOrDefault();
        if (expiry is null || !Covers(expiry, epochs))
        {
            problems.Add(source.Settles
                ? $"no index led by {f.LeaseExpiresAt} carrying {f.SettledEpoch} and {f.LeaseEpoch}, without a filter or WHERE {f.LeaseExpiresAt} IS NOT NULL (claims of expired leases, next due time and expiry settlement)"
                : $"no index led by {f.LeaseExpiresAt}, without a filter or WHERE {f.LeaseExpiresAt} IS NOT NULL (claims of expired leases and next due time)");
        }
        Index? partitionIndex = null;
        if (limitsPartition)
        {
            string[] counted = source.Policy.SpreadLimit is null ? epochs : [.. epochs, f.LeaseOwner];
            partitionIndex = indexes.FirstOrDefault(i => i.Keys.FirstOrDefault() == source.Partition && Normalize(i.Filter) == held && Covers(i, counted));
            if (partitionIndex is null)
            {
                problems.Add($"no index led by {source.Partition} WHERE {f.LeaseExpiresAt} IS NOT NULL carrying {string.Join(" and ", counted)} (partition and spread limits)");
            }
        }
        Index? ownerIndex = null;
        if (source.Policy.OwnerLimit is not null)
        {
            ownerIndex = indexes.FirstOrDefault(i => i.Keys.FirstOrDefault() == f.LeaseOwner);
            if (ownerIndex is null)
            {
                problems.Add($"no index led by {f.LeaseOwner} (owner limit)");
            }
        }
        Index? generationIndex = null;
        if (source.Reconcile is { Fields: var r })
        {
            Require(r.RequestedRevision, true, "bigint");
            Require(r.ClaimedRevision, false, "bigint");
            Require(r.ConvergedRevision, true, "bigint");
            Require(r.FirstRequestedAt, false, times);
            Require(r.NotBefore, false, times);
            Require(r.AttemptedGeneration, true, "bigint");
            Require(r.ConvergedGeneration, true, "bigint");
            generationIndex = indexes.FirstOrDefault(i => i.Filter is null && i.Keys.FirstOrDefault() == r.AttemptedGeneration);
            if (generationIndex is null)
            {
                problems.Add($"no index led by {r.AttemptedGeneration} (claims after an invalidation)");
            }
        }
        if (problems.Count > 0)
        {
            throw new InvalidOperationException($"Lease source {source.Name} on {table}: {string.Join("; ", problems)}.");
        }
        var keyColumns = source.Key.Fields.Select(k => KeyOf(k.Name, columns[k.Name])).ToArray();
        var partitionDeclaration = partitionColumn is null ? "nvarchar(4000)" : Declaration(partitionColumn);
        return new SourceShape(keyColumns, partitionDeclaration, due!.Name, expiry!.Name, partitionIndex?.Name, ownerIndex?.Name, generationIndex?.Name);
    }

    private static KeyColumn KeyOf(string name, Column column) => column.Type switch
    {
        "nvarchar" => new KeyColumn(name, Declaration(column), SqlDbType.NVarChar, column.MaxLength < 0 ? -1 : column.MaxLength / 2),
        "nchar" => new KeyColumn(name, Declaration(column), SqlDbType.NChar, column.MaxLength / 2),
        "varchar" => new KeyColumn(name, Declaration(column), SqlDbType.VarChar, column.MaxLength),
        "char" => new KeyColumn(name, Declaration(column), SqlDbType.Char, column.MaxLength),
        "int" => new KeyColumn(name, "int", SqlDbType.Int, 0),
        "bigint" => new KeyColumn(name, "bigint", SqlDbType.BigInt, 0),
        _ => new KeyColumn(name, "uniqueidentifier", SqlDbType.UniqueIdentifier, 0),
    };

    /// <summary>A column's type with its length and collation, for table variables of its values.</summary>
    private static string Declaration(Column column)
    {
        var length = column.Type switch
        {
            "nvarchar" or "nchar" => column.MaxLength < 0 ? "(max)" : $"({column.MaxLength / 2})",
            "varchar" or "char" or "varbinary" or "binary" => column.MaxLength < 0 ? "(max)" : $"({column.MaxLength})",
            _ => "",
        };
        return column.Type + length + (column.Collation is { } collation ? " COLLATE " + collation : "");
    }

    /// <summary>A filter definition without brackets, parentheses, spaces or case, for comparison.</summary>
    private static string? Normalize(string? filter) => filter is null
        ? null
        : new string(filter.Where(c => c is not ('[' or ']' or '(' or ')') && !char.IsWhiteSpace(c)).ToArray()).ToUpperInvariant();
}
