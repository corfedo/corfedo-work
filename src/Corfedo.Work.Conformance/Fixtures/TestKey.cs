using System.Globalization;

namespace Corfedo.Work.Conformance;

/// <summary>The composite key of conformance records: a tenant, which is also the partition, and a number.</summary>
public sealed record TestKey(string Tenant, long Id)
{
    /// <summary>The length of the tenant column in test tables of providers that limit it.</summary>
    public const int TenantLength = 100;

    public static IKeyCodec<TestKey> Codec { get; } = new KeyCodec();

    public override string ToString() => Codec.Format(this);

    private sealed class KeyCodec : IKeyCodec<TestKey>
    {
        public IReadOnlyList<KeyField> Fields { get; } = [new("tenant", KeyFieldType.String), new("id", KeyFieldType.Int64)];

        public void Write(TestKey key, IKeyWriter writer)
        {
            writer.Write(key.Tenant);
            writer.Write(key.Id);
        }

        public TestKey Read(IKeyReader reader) => new(reader.ReadString(), reader.ReadInt64());

        public string Format(TestKey key) => key.Tenant + "/" + key.Id.ToString(CultureInfo.InvariantCulture);

        public TestKey Parse(string value)
        {
            var slash = value.LastIndexOf('/');
            return new(value[..slash], long.Parse(value.AsSpan(slash + 1), NumberStyles.None, CultureInfo.InvariantCulture));
        }
    }
}
