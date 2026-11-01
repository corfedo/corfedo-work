using Npgsql;
using NpgsqlTypes;

namespace Corfedo.Work.PostgreSql.Leases;

internal static class KeyTypes
{
    public static string SqlType(KeyFieldType type) => type switch
    {
        KeyFieldType.String => "text",
        KeyFieldType.Int32 => "int4",
        KeyFieldType.Int64 => "int8",
        _ => "uuid",
    };
}

/// <summary>Adds one key's field values as positional parameters.</summary>
internal sealed class ParameterKeyWriter(NpgsqlParameterCollection parameters) : IKeyWriter
{
    public void Write(string value) => parameters.Add(new NpgsqlParameter<string> { TypedValue = value, NpgsqlDbType = NpgsqlDbType.Text });

    public void Write(int value) => parameters.Add(new NpgsqlParameter<int> { TypedValue = value });

    public void Write(long value) => parameters.Add(new NpgsqlParameter<long> { TypedValue = value });

    public void Write(Guid value) => parameters.Add(new NpgsqlParameter<Guid> { TypedValue = value });
}

/// <summary>Collects many keys as one array parameter per key field.</summary>
internal sealed class ArrayKeyWriter : IKeyWriter
{
    private readonly IReadOnlyList<KeyField> fields;
    private readonly object[] columns;
    private int next;

    public ArrayKeyWriter(IReadOnlyList<KeyField> fields)
    {
        this.fields = fields;
        columns = new object[fields.Count];
        for (var i = 0; i < fields.Count; i++)
        {
            columns[i] = fields[i].Type switch
            {
                KeyFieldType.String => new List<string>(),
                KeyFieldType.Int32 => new List<int>(),
                KeyFieldType.Int64 => new List<long>(),
                _ => new List<Guid>(),
            };
        }
    }

    public void Write(string value) => Column<string>().Add(value);

    public void Write(int value) => Column<int>().Add(value);

    public void Write(long value) => Column<long>().Add(value);

    public void Write(Guid value) => Column<Guid>().Add(value);

    public void AddTo(NpgsqlParameterCollection parameters)
    {
        for (var i = 0; i < fields.Count; i++)
        {
            parameters.Add(columns[i] switch
            {
                List<string> values => new NpgsqlParameter<string[]> { TypedValue = [.. values], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text },
                List<int> values => new NpgsqlParameter<int[]> { TypedValue = [.. values] },
                List<long> values => new NpgsqlParameter<long[]> { TypedValue = [.. values] },
                List<Guid> values => new NpgsqlParameter<Guid[]> { TypedValue = [.. values] },
                _ => throw new InvalidOperationException(),
            });
        }
    }

    private List<T> Column<T>()
    {
        var index = next;
        next = (next + 1) % fields.Count;
        return columns[index] as List<T>
            ?? throw new InvalidOperationException($"Key field {fields[index].Name} is declared as {fields[index].Type}.");
    }
}

/// <summary>Reads one key from consecutive columns of the current row.</summary>
internal sealed class RowKeyReader(NpgsqlDataReader reader, int first) : IKeyReader
{
    private int ordinal = first;

    public string ReadString() => reader.GetString(ordinal++);

    public int ReadInt32() => reader.GetInt32(ordinal++);

    public long ReadInt64() => reader.GetInt64(ordinal++);

    public Guid ReadGuid() => reader.GetGuid(ordinal++);
}
