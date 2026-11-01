namespace Corfedo.Work;

/// <summary>Where a source's records are stored.</summary>
public sealed record StoreLocation
{
    private StoreLocation(string schema, string name)
    {
        Schema = schema;
        Name = name;
    }

    /// <summary>The schema, or the database namespace a provider maps it to.</summary>
    public string Schema { get; }

    /// <summary>The table or collection name.</summary>
    public string Name { get; }

    /// <summary>A table in a schema.</summary>
    public static StoreLocation Table(string schema, string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        return new StoreLocation(schema, name);
    }

    public override string ToString() => $"{Schema}.{Name}";
}
