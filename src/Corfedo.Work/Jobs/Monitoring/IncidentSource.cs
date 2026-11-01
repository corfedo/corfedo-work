namespace Corfedo.Work;

/// <summary>
/// The framework's lease source of incidents: an incident is due while one of its events waits for delivery, and
/// processes that register an incident handler claim one at a time. A failed delivery is due again after a backoff of
/// at most five minutes, and never given up.
/// </summary>
public static class IncidentSource
{
    /// <summary>The source's name.</summary>
    public const string Name = "work:incidents";

    /// <summary>The key codec of incidents: one <c>id</c> field.</summary>
    public static IKeyCodec<Guid> Codec { get; } = new KeyCodec();

    /// <summary>The incident source on the store's incident table.</summary>
    public static LeaseSource<Guid> Define(StoreLocation table) => LeaseSource.Define(
        Name,
        table,
        Codec,
        new LeasePolicy
        {
            Duration = TimeSpan.FromSeconds(30),
            FailureBackoff = Backoff.Exponential(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(5)),
        });

    private sealed class KeyCodec : IKeyCodec<Guid>
    {
        public IReadOnlyList<KeyField> Fields { get; } = [new("id", KeyFieldType.Guid)];

        public void Write(Guid key, IKeyWriter writer) => writer.Write(key);

        public Guid Read(IKeyReader reader) => reader.ReadGuid();

        public string Format(Guid key) => key.ToString("D");

        public Guid Parse(string value) => Guid.ParseExact(value, "D");
    }
}
