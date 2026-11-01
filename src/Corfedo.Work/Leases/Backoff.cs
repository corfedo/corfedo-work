namespace Corfedo.Work;

/// <summary>A delay that depends on a count of consecutive events: none, constant, or exponential with a cap.</summary>
public sealed record Backoff
{
    private Backoff(BackoffKind kind, TimeSpan initial, TimeSpan maximum)
    {
        Kind = kind;
        Initial = initial;
        Maximum = maximum;
    }

    public BackoffKind Kind { get; }

    public TimeSpan Initial { get; }

    public TimeSpan Maximum { get; }

    public static Backoff None { get; } = new(BackoffKind.None, TimeSpan.Zero, TimeSpan.Zero);

    public static Backoff Constant(TimeSpan delay)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        return new(BackoffKind.Constant, delay, delay);
    }

    /// <summary><paramref name="initial"/> doubled for every previous event, never more than <paramref name="maximum"/>.</summary>
    public static Backoff Exponential(TimeSpan initial, TimeSpan maximum)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(initial, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(maximum, initial);
        return new(BackoffKind.Exponential, initial, maximum);
    }

    /// <summary>The delay after <paramref name="previous"/> consecutive earlier events.</summary>
    public TimeSpan Delay(int previous)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(previous);
        return Kind switch
        {
            BackoffKind.None => TimeSpan.Zero,
            BackoffKind.Constant => Initial,
            _ => TimeSpan.FromTicks((long)Math.Min(Initial.Ticks * Math.Pow(2, Math.Min(previous, 62)), Maximum.Ticks)),
        };
    }
}

public enum BackoffKind
{
    None,
    Constant,
    Exponential,
}
