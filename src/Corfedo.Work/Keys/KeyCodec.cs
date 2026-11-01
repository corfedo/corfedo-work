using System.Globalization;

namespace Corfedo.Work;

/// <summary>The store type of one key field.</summary>
public enum KeyFieldType
{
    String,
    Int32,
    Int64,
    Guid,
}

/// <summary>One field of a record key: the record's field name and its type.</summary>
public readonly record struct KeyField(string Name, KeyFieldType Type);

/// <summary>
/// Maps a key to the record's key fields and to a canonical string, and back without loss. Written by the
/// application; the framework never inspects key types.
/// </summary>
public interface IKeyCodec<TKey>
    where TKey : notnull
{
    /// <summary>The key fields, in declared order. Composite keys write and read their values in this order.</summary>
    IReadOnlyList<KeyField> Fields { get; }

    /// <summary>Writes the key's field values in declared order.</summary>
    void Write(TKey key, IKeyWriter writer);

    /// <summary>Reads a key from field values in declared order.</summary>
    TKey Read(IKeyReader reader);

    /// <summary>The canonical string form, used in tokens and telemetry.</summary>
    string Format(TKey key);

    /// <summary>Parses the canonical string form.</summary>
    TKey Parse(string value);
}

/// <summary>Receives key field values in declared order.</summary>
public interface IKeyWriter
{
    void Write(string value);

    void Write(int value);

    void Write(long value);

    void Write(Guid value);
}

/// <summary>Supplies key field values in declared order.</summary>
public interface IKeyReader
{
    string ReadString();

    int ReadInt32();

    long ReadInt64();

    Guid ReadGuid();
}

/// <summary>
/// Codecs for keys of one field. A key type that wraps the field value, such as <c>record RunKey(long Id)</c>, passes
/// how to read the value from the key and how to create the key from it. Composite keys implement
/// <see cref="IKeyCodec{TKey}"/>.
/// </summary>
public static class KeyCodec
{
    /// <summary>A 32-bit integer key in <paramref name="field"/>.</summary>
    public static IKeyCodec<int> Int32(string field) => Int32(field, key => key, value => value);

    /// <summary>A key wrapping a 32-bit integer in <paramref name="field"/>.</summary>
    public static IKeyCodec<TKey> Int32<TKey>(string field, Func<TKey, int> value, Func<int, TKey> create)
        where TKey : notnull =>
        new SingleField<TKey, int>(field, KeyFieldType.Int32, value, create, (w, v) => w.Write(v), r => r.ReadInt32(),
            v => v.ToString(CultureInfo.InvariantCulture), s => int.Parse(s, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture));

    /// <summary>A 64-bit integer key in <paramref name="field"/>.</summary>
    public static IKeyCodec<long> Int64(string field) => Int64(field, key => key, value => value);

    /// <summary>A key wrapping a 64-bit integer in <paramref name="field"/>.</summary>
    public static IKeyCodec<TKey> Int64<TKey>(string field, Func<TKey, long> value, Func<long, TKey> create)
        where TKey : notnull =>
        new SingleField<TKey, long>(field, KeyFieldType.Int64, value, create, (w, v) => w.Write(v), r => r.ReadInt64(),
            v => v.ToString(CultureInfo.InvariantCulture), s => long.Parse(s, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture));

    /// <summary>A <see cref="System.Guid"/> key in <paramref name="field"/>.</summary>
    public static IKeyCodec<Guid> Guid(string field) => Guid(field, key => key, value => value);

    /// <summary>A key wrapping a <see cref="System.Guid"/> in <paramref name="field"/>.</summary>
    public static IKeyCodec<TKey> Guid<TKey>(string field, Func<TKey, Guid> value, Func<Guid, TKey> create)
        where TKey : notnull =>
        new SingleField<TKey, Guid>(field, KeyFieldType.Guid, value, create, (w, v) => w.Write(v), r => r.ReadGuid(),
            v => v.ToString("D"), s => System.Guid.ParseExact(s, "D"));

    /// <summary>A string key in <paramref name="field"/>.</summary>
    public static IKeyCodec<string> String(string field) => String(field, key => key, value => value);

    /// <summary>A key wrapping a string in <paramref name="field"/>.</summary>
    public static IKeyCodec<TKey> String<TKey>(string field, Func<TKey, string> value, Func<string, TKey> create)
        where TKey : notnull =>
        new SingleField<TKey, string>(field, KeyFieldType.String, value, create, (w, v) => w.Write(v), r => r.ReadString(), v => v, s => s);

    private sealed class SingleField<TKey, TValue>(
        string field,
        KeyFieldType type,
        Func<TKey, TValue> value,
        Func<TValue, TKey> create,
        Action<IKeyWriter, TValue> write,
        Func<IKeyReader, TValue> read,
        Func<TValue, string> format,
        Func<string, TValue> parse) : IKeyCodec<TKey>
        where TKey : notnull
    {
        public IReadOnlyList<KeyField> Fields { get; } = [new(Validate(field), type)];

        public void Write(TKey key, IKeyWriter writer) => write(writer, value(key));

        public TKey Read(IKeyReader reader) => create(read(reader));

        public string Format(TKey key) => format(value(key));

        public TKey Parse(string text) => create(parse(text));

        private static string Validate(string field)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(field);
            return field;
        }
    }
}
