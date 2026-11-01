namespace Corfedo.Work.InMemory.Database;

/// <summary>
/// An immutable domain record: field values by name and a version that changes with every committed write. Fields
/// that were never written read as null.
/// </summary>
internal sealed class Record(IReadOnlyDictionary<string, object?> values, long version)
{
    public IReadOnlyDictionary<string, object?> Values { get; } = values;

    public long Version { get; } = version;

    public object? this[string field] => Values.TryGetValue(field, out var value) ? value : null;

    public DateTimeOffset? Time(string field) => (DateTimeOffset?)this[field];

    public Guid? Uuid(string field) => (Guid?)this[field];

    public long Int64(string field) => this[field] is { } value ? Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture) : 0;

    public int Int32(string field) => this[field] is { } value ? Convert.ToInt32(value, System.Globalization.CultureInfo.InvariantCulture) : 0;

    /// <summary>A copy with <paramref name="changes"/> applied.</summary>
    public Record With(IEnumerable<KeyValuePair<string, object?>> changes, long version)
    {
        var values = new Dictionary<string, object?>(Values, StringComparer.Ordinal);
        foreach (var (field, value) in changes)
        {
            values[field] = value;
        }
        return new Record(values, version);
    }
}

/// <summary>An application table: its location, key fields in declared order, its fields and its records.</summary>
internal sealed class DomainTable
{
    public DomainTable(StoreLocation location, IReadOnlyList<string> key, IEnumerable<string> fields)
    {
        Location = location;
        Key = [.. key];
        Fields = new HashSet<string>(key, StringComparer.Ordinal);
        Fields.UnionWith(fields);
        if (Key.Length == 0)
        {
            throw new ArgumentException("A table needs at least one key field.", nameof(key));
        }
    }

    public StoreLocation Location { get; }

    public string[] Key { get; }

    public HashSet<string> Fields { get; }

    public Table<RowKey, Record> Rows { get; } = new();

    /// <summary>The record's key from its key fields.</summary>
    public RowKey KeyOf(IReadOnlyDictionary<string, object?> values) =>
        new(Key.Select(f => values.TryGetValue(f, out var value) ? value : throw new ArgumentException($"Key field {f} of {Location} has no value.", nameof(values))).ToArray());

    /// <summary>Refuses writes of fields the table does not have.</summary>
    public void CheckFields(IEnumerable<string> fields)
    {
        foreach (var field in fields)
        {
            if (!Fields.Contains(field))
            {
                throw new ArgumentException($"Table {Location} has no field {field}.", nameof(fields));
            }
        }
    }
}
