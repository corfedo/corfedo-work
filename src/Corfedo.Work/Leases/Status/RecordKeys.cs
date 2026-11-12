namespace Corfedo.Work;

/// <summary>The keys of one read of many records.</summary>
internal static class RecordKeys
{
    /// <summary>The most records one read of many returns.</summary>
    public const int Maximum = 1000;

    /// <summary>The distinct keys by their canonical form, at most <see cref="Maximum"/>.</summary>
    public static TKey[] Distinct<TKey>(LeaseSource<TKey> source, IEnumerable<TKey> keys)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(keys);
        TKey[] distinct = [.. keys.DistinctBy(source.Key.Format, StringComparer.Ordinal)];
        return distinct.Length > Maximum
            ? throw new ArgumentException($"At most {Maximum} records are read at once; {distinct.Length} keys were given.", nameof(keys))
            : distinct;
    }

    /// <summary>The records that exist, keyed by the caller's keys, from a store's read aligned with them.</summary>
    public static IReadOnlyDictionary<TKey, TRecord> Dictionary<TKey, TRecord>(IReadOnlyList<TKey> keys, IReadOnlyList<TRecord?> records)
        where TKey : notnull
        where TRecord : class
    {
        var found = new Dictionary<TKey, TRecord>();
        for (var i = 0; i < keys.Count; i++)
        {
            if (records[i] is { } record)
            {
                found[keys[i]] = record;
            }
        }
        return found;
    }
}
