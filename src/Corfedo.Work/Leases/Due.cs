namespace Corfedo.Work;

/// <summary>A due time, always resolved with the store's clock.</summary>
public readonly record struct Due
{
    private Due(DueKind kind, TimeSpan delay)
    {
        Kind = kind;
        Delay = delay;
    }

    public DueKind Kind { get; }

    /// <summary>The delay after the store's current time, for <see cref="DueKind.After"/>.</summary>
    public TimeSpan Delay { get; }

    /// <summary>Due at the store's current time.</summary>
    public static Due Now { get; } = new(DueKind.After, TimeSpan.Zero);

    /// <summary>No run wanted: the record has no due time.</summary>
    public static Due None { get; } = new(DueKind.None, TimeSpan.Zero);

    /// <summary>Leaves the record's due time as it is.</summary>
    public static Due Unchanged { get; } = new(DueKind.Unchanged, TimeSpan.Zero);

    /// <summary>Due <paramref name="delay"/> after the store's current time.</summary>
    public static Due After(TimeSpan delay)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        return new(DueKind.After, delay);
    }
}

public enum DueKind
{
    Unchanged,
    After,
    None,
}
