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

    public static IReadOnlyDictionary<TKey, TRecord> Dictionary<TKey, TRecord>(IReadOnlyList<KeyValuePair<TKey, TRecord>> records)
        where TKey : notnull => records.ToDictionary(r => r.Key, r => r.Value);
}
