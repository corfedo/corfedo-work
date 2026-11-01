namespace Corfedo.Work;

/// <summary>
/// The registered name of what a signal says may have changed: lowercase letters, digits, <c>.</c>, <c>-</c> and
/// <c>_</c>, at most <see cref="MaximumLength"/> characters. Topics starting with <c>work.</c> are the framework's.
/// </summary>
public readonly record struct SignalTopic
{
    public const int MaximumLength = 64;

    /// <exception cref="ArgumentException">The name is empty, too long or has other characters.</exception>
    public SignalTopic(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        if (!IsValidName(name))
        {
            throw new ArgumentException($"Signal topic '{name}' is not 1 to {MaximumLength} lowercase letters, digits, '.', '-' and '_'.", nameof(name));
        }
        Name = name;
    }

    /// <summary>The name, or null for <c>default</c>, which no operation accepts.</summary>
    public string Name { get; }

    public override string ToString() => Name ?? "";

    internal static bool IsValidName(ReadOnlySpan<char> name)
    {
        if (name.IsEmpty || name.Length > MaximumLength)
        {
            return false;
        }
        foreach (var c in name)
        {
            if (c is not ((>= 'a' and <= 'z') or (>= '0' and <= '9') or '.' or '-' or '_'))
            {
                return false;
            }
        }
        return true;
    }
}
