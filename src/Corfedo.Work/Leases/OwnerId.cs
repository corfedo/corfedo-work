namespace Corfedo.Work;

/// <summary>The identity of one process instance or remote holder. A process gets a new one on every start.</summary>
public readonly record struct OwnerId(Guid Value)
{
    public static OwnerId New() => new(Guid.CreateVersion7());

    public override string ToString() => Value.ToString("D");
}
