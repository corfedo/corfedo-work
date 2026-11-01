using System.Globalization;

namespace Corfedo.Work.InMemory.Database;

/// <summary>
/// The key of a record: its key field values in declared order. Integer values compare as 64-bit numbers, strings
/// ordinally and GUIDs in byte order, as the store's key order.
/// </summary>
internal readonly struct RowKey : IEquatable<RowKey>, IComparable<RowKey>
{
    private readonly object[] values;

    public RowKey(IReadOnlyList<object?> values)
    {
        this.values = new object[values.Count];
        for (var i = 0; i < values.Count; i++)
        {
            this.values[i] = values[i] switch
            {
                null => throw new ArgumentException("Key fields cannot be null.", nameof(values)),
                int number => (long)number,
                short number => (long)number,
                var value => value,
            };
        }
    }

    public IReadOnlyList<object> Values => values;

    public bool Equals(RowKey other)
    {
        if (values.Length != other.values.Length)
        {
            return false;
        }
        for (var i = 0; i < values.Length; i++)
        {
            if (!values[i].Equals(other.values[i]))
            {
                return false;
            }
        }
        return true;
    }

    public override bool Equals(object? obj) => obj is RowKey other && Equals(other);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        foreach (var value in values)
        {
            hash.Add(value);
        }
        return hash.ToHashCode();
    }

    public int CompareTo(RowKey other)
    {
        for (var i = 0; i < Math.Min(values.Length, other.values.Length); i++)
        {
            var compared = (values[i], other.values[i]) switch
            {
                (string a, string b) => string.CompareOrdinal(a, b),
                (long a, long b) => a.CompareTo(b),
                (Guid a, Guid b) => a.CompareTo(b),
                (var a, var b) => string.CompareOrdinal(Convert.ToString(a, CultureInfo.InvariantCulture), Convert.ToString(b, CultureInfo.InvariantCulture)),
            };
            if (compared != 0)
            {
                return compared;
            }
        }
        return values.Length.CompareTo(other.values.Length);
    }

    public override string ToString() => string.Join(", ", values.Select(v => Convert.ToString(v, CultureInfo.InvariantCulture)));
}
