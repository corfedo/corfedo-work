namespace Corfedo.Work;

/// <summary>The identity of one job record; a rerun keeps it.</summary>
public readonly record struct JobId(Guid Value)
{
    /// <summary>The key codec of job records: one <c>id</c> field.</summary>
    public static IKeyCodec<JobId> Codec { get; } = new KeyCodec();

    public static JobId New() => new(Guid.CreateVersion7());

    public override string ToString() => Value.ToString("D");

    private sealed class KeyCodec : IKeyCodec<JobId>
    {
        public IReadOnlyList<KeyField> Fields { get; } = [new("id", KeyFieldType.Guid)];

        public void Write(JobId key, IKeyWriter writer) => writer.Write(key.Value);

        public JobId Read(IKeyReader reader) => new(reader.ReadGuid());

        public string Format(JobId key) => key.ToString();

        public JobId Parse(string value) => new(Guid.ParseExact(value, "D"));
    }
}

/// <summary>The domain record a job acts on: a kind and an identifier, such as <c>("feed", "npm")</c>.</summary>
public sealed record Subject
{
    private Subject(string kind, string id)
    {
        Kind = kind;
        Id = id;
    }

    public string Kind { get; }

    public string Id { get; }

    public static Subject Of(string kind, string id)
    {
        ArgumentException.ThrowIfNullOrEmpty(kind);
        ArgumentNullException.ThrowIfNull(id);
        if (kind.Any(char.IsControl))
        {
            // The merge identity joins kind and identifier with a newline.
            throw new ArgumentException("A subject kind cannot contain control characters.", nameof(kind));
        }
        return new Subject(kind, id);
    }

    public override string ToString() => Kind + ":" + Id;
}
