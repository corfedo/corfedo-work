using System.Text.Json.Serialization;

namespace Corfedo.Work.SqlServer;

/// <summary>Source-generated JSON for the lists statements read with OPENJSON.</summary>
[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(string[]))]
[JsonSerializable(typeof(Guid[]))]
[JsonSerializable(typeof(int[]))]
[JsonSerializable(typeof(long[]))]
internal sealed partial class SqlServerJson : JsonSerializerContext;
