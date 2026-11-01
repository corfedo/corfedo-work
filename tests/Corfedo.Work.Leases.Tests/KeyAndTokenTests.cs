using System.Globalization;

namespace Corfedo.Work.Leases.Tests;

public sealed class KeyAndTokenTests
{
    private static readonly LeaseSource<OrderKey> Orders = LeaseSource.Define(
        "orders", StoreLocation.Table("app", "orders"), OrderKey.Codec, new LeasePolicy { Duration = TimeSpan.FromMinutes(1) });

    [Fact]
    public void TokenRoundTripsSourceKeyOwnerAndEpoch()
    {
        var lease = new Lease<OrderKey>(Orders, new OrderKey("tenant/with\nnewline", 42), OwnerId.New(), 7);

        var parsed = Orders.ParseToken(lease.Token);

        Assert.Equal(lease, parsed);
    }

    [Fact]
    public void TokenOfAnotherSourceIsRejected()
    {
        var other = LeaseSource.Define("invoices", Orders.Store, OrderKey.Codec, Orders.Policy);
        var token = new Lease<OrderKey>(other, new OrderKey("t", 1), OwnerId.New(), 1).Token;

        Assert.Throws<FormatException>(() => Orders.ParseToken(token));
        Assert.Throws<FormatException>(() => Orders.ParseToken("not a token"));
    }

    [Theory]
    [InlineData(0, 100)]
    [InlineData(1, 200)]
    [InlineData(3, 800)]
    [InlineData(4, 1000)]
    [InlineData(1000, 1000)]
    public void ExponentialBackoffDoublesUpToItsMaximum(int previous, int milliseconds)
    {
        var backoff = Backoff.Exponential(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1));

        Assert.Equal(TimeSpan.FromMilliseconds(milliseconds), backoff.Delay(previous));
    }

    [Fact]
    public void LimitsNeedAPartitionAndArePositive()
    {
        var policy = new LeasePolicy { Duration = TimeSpan.FromMinutes(1), PartitionLimit = 1 };

        Assert.Throws<ArgumentException>(() => LeaseSource.Define("a", Orders.Store, OrderKey.Codec, policy));
        Assert.Throws<ArgumentException>(() => LeaseSource.Define("a", Orders.Store, OrderKey.Codec, policy with { PartitionLimit = 0 }, partition: "tenant"));
        Assert.True(LeaseSource.Define("a", Orders.Store, OrderKey.Codec, policy, partition: "tenant").Settles);
        Assert.False(Orders.Settles);
        Assert.True(LeaseSource.Define("a", Orders.Store, OrderKey.Codec, Orders.Policy with { SettleExpiry = true }).Settles);
    }

    [Fact]
    public void BuiltInCodecsRoundTripOneField()
    {
        var runs = KeyCodec.Int64<RunKey>("id", k => k.Id, id => new(id));
        var source = LeaseSource.Define("runs", Orders.Store, runs, Orders.Policy);
        var lease = new Lease<RunKey>(source, new RunKey(-42), OwnerId.New(), 3);

        Assert.Equal([new KeyField("id", KeyFieldType.Int64)], runs.Fields);
        Assert.Equal("-42", runs.Format(new RunKey(-42)));
        Assert.Equal(lease, source.ParseToken(lease.Token));
        Assert.Equal(new RunKey(7), RoundTrip(runs, new RunKey(7)));
        var id = Guid.NewGuid();
        Assert.Equal(id, KeyCodec.Guid("id").Parse(KeyCodec.Guid("id").Format(id)));
        Assert.Equal(id, RoundTrip(KeyCodec.Guid("id"), id));
        Assert.Equal(int.MinValue, RoundTrip(KeyCodec.Int32("id"), int.MinValue));
        Assert.Equal("a/b c", RoundTrip(KeyCodec.String("name"), "a/b c"));
        Assert.Throws<ArgumentException>(() => KeyCodec.Int64(" "));
    }

    private static TKey RoundTrip<TKey>(IKeyCodec<TKey> codec, TKey key)
        where TKey : notnull
    {
        var fields = new Fields();
        codec.Write(key, fields);
        Assert.Single(fields.Values);
        return codec.Read(fields);
    }

    private sealed record RunKey(long Id);

    private sealed class Fields : IKeyWriter, IKeyReader
    {
        private int next;

        public List<object> Values { get; } = [];

        public void Write(string value) => Values.Add(value);

        public void Write(int value) => Values.Add(value);

        public void Write(long value) => Values.Add(value);

        public void Write(Guid value) => Values.Add(value);

        public string ReadString() => (string)Values[next++];

        public int ReadInt32() => (int)Values[next++];

        public long ReadInt64() => (long)Values[next++];

        public Guid ReadGuid() => (Guid)Values[next++];
    }

    private sealed record OrderKey(string Tenant, long Id)
    {
        public static IKeyCodec<OrderKey> Codec { get; } = new KeyCodec();

        private sealed class KeyCodec : IKeyCodec<OrderKey>
        {
            public IReadOnlyList<KeyField> Fields { get; } = [new("tenant", KeyFieldType.String), new("id", KeyFieldType.Int64)];

            public void Write(OrderKey key, IKeyWriter writer)
            {
                writer.Write(key.Tenant);
                writer.Write(key.Id);
            }

            public OrderKey Read(IKeyReader reader) => new(reader.ReadString(), reader.ReadInt64());

            public string Format(OrderKey key) => key.Tenant + "/" + key.Id.ToString(CultureInfo.InvariantCulture);

            public OrderKey Parse(string value)
            {
                var slash = value.LastIndexOf('/');
                return new(value[..slash], long.Parse(value.AsSpan(slash + 1), CultureInfo.InvariantCulture));
            }
        }
    }
}
