namespace Corfedo.Work;

/// <summary>
/// Describes this process in the process registry: its type, such as <c>control-plane</c>, by default the host's
/// application name; its release; how often it heartbeats, by default every 15 seconds; and how long after its last
/// heartbeat it counts as stale, by default 60 seconds.
/// </summary>
public sealed class ProcessBuilder
{
    internal ProcessBuilder()
    {
    }

    private string? TypeValue { get; set; }

    private string? ReleaseValue { get; set; }

    private TimeSpan HeartbeatValue { get; set; } = TimeSpan.FromSeconds(15);

    private TimeSpan StaleAfterValue { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>The process type: replicas of one service share it.</summary>
    public ProcessBuilder Type(string type)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(type);
        TypeValue = type;
        return this;
    }

    /// <summary>The release this process runs, such as its version.</summary>
    public ProcessBuilder Release(string release)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(release);
        ReleaseValue = release;
        return this;
    }

    /// <summary>How often the process records its heartbeat.</summary>
    public ProcessBuilder Heartbeat(TimeSpan interval)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero);
        HeartbeatValue = interval;
        return this;
    }

    /// <summary>How long after its last heartbeat the process counts as stale; longer than the heartbeat interval.</summary>
    public ProcessBuilder StaleAfter(TimeSpan duration)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(duration, TimeSpan.Zero);
        StaleAfterValue = duration;
        return this;
    }

    internal static ProcessOptions Build(Action<ProcessBuilder>? configure)
    {
        var builder = new ProcessBuilder();
        configure?.Invoke(builder);
        if (builder.StaleAfterValue <= builder.HeartbeatValue)
        {
            throw new InvalidOperationException($"The stale window ({builder.StaleAfterValue}) must be longer than the heartbeat interval ({builder.HeartbeatValue}).");
        }
        return new ProcessOptions(builder.TypeValue, builder.ReleaseValue, builder.HeartbeatValue, builder.StaleAfterValue, configure is not null);
    }
}

/// <summary>How this process registers; <see cref="Configured"/> says the application described it explicitly.</summary>
internal sealed record ProcessOptions(string? Type, string? Release, TimeSpan Heartbeat, TimeSpan StaleAfter, bool Configured)
{
    /// <summary>The registration of <paramref name="owner"/>, typed by the host's application name unless a type is set.</summary>
    public ProcessRegistration Registration(OwnerId owner, IServiceProvider services)
    {
        var type = Type
            ?? (services.GetService(typeof(Microsoft.Extensions.Hosting.IHostEnvironment)) as Microsoft.Extensions.Hosting.IHostEnvironment)?.ApplicationName
            ?? AppDomain.CurrentDomain.FriendlyName;
        return new ProcessRegistration(owner, type, Release, StaleAfter);
    }
}
