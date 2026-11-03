using System.Buffers;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Leases;

/// <summary>Adds one key's field values as the parameters <c>@k0</c>, <c>@k1</c>, ..., typed like the key columns.</summary>
internal sealed class ParameterKeyWriter(SqlParameterCollection parameters, IReadOnlyList<KeyColumn> columns) : IKeyWriter
{
    private int next;

    public void Write(string value) => Add(value);

    public void Write(int value) => Add(value);

    public void Write(long value) => Add(value);

    public void Write(Guid value) => Add(value);

    private void Add(object value)
    {
        var column = columns[next];
        var parameter = new SqlParameter("@k" + next, column.ParameterType) { Value = value };
        if (column.ParameterSize != 0)
        {
            parameter.Size = column.ParameterSize;
        }
        parameters.Add(parameter);
        next++;
    }
}

/// <summary>Checks that a key's string fields fit their columns.</summary>
internal sealed class FitsKeyWriter(IReadOnlyList<KeyColumn> columns) : IKeyWriter
{
    private int next;

    public bool Fits { get; private set; } = true;

    public void Write(string value) => Fits &= columns[next++].Fits(value);

    public void Write(int value) => next++;

    public void Write(long value) => next++;

    public void Write(Guid value) => next++;
}

/// <summary>Writes many keys as one JSON array of arrays, the <c>@keyset</c> parameter that <see cref="LeaseSql.KeyedRows"/> reads.</summary>
internal sealed class JsonKeyWriter : IKeyWriter, IDisposable
{
    private readonly ArrayBufferWriter<byte> buffer = new();
    private readonly Utf8JsonWriter writer;
    private readonly int fields;
    private int written;

    public JsonKeyWriter(int fields)
    {
        this.fields = fields;
        writer = new Utf8JsonWriter(buffer);
        writer.WriteStartArray();
    }

    public void Write(string value) => Next(w => w.WriteStringValue(value));

    public void Write(int value) => Next(w => w.WriteNumberValue(value));

    public void Write(long value) => Next(w => w.WriteNumberValue(value));

    public void Write(Guid value) => Next(w => w.WriteStringValue(value));

    public SqlParameter ToParameter(string name = "@keyset")
    {
        writer.WriteEndArray();
        writer.Flush();
        return Tsql.Json(name, Encoding.UTF8.GetString(buffer.WrittenSpan));
    }

    public void Dispose() => writer.Dispose();

    private void Next(Action<Utf8JsonWriter> value)
    {
        if (written % fields == 0)
        {
            writer.WriteStartArray();
        }
        value(writer);
        written++;
        if (written % fields == 0)
        {
            writer.WriteEndArray();
        }
    }
}

/// <summary>Reads one key from consecutive columns of the current row.</summary>
internal sealed class RowKeyReader(SqlDataReader reader, int first) : IKeyReader
{
    private int ordinal = first;

    public string ReadString() => reader.GetString(ordinal++);

    public int ReadInt32() => reader.GetInt32(ordinal++);

    public long ReadInt64() => reader.GetInt64(ordinal++);

    public Guid ReadGuid() => reader.GetGuid(ordinal++);
}
