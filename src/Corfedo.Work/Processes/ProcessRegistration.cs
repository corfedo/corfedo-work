namespace Corfedo.Work;

/// <summary>
/// What a process records in the process registry: its owner id, which is also the owner of the leases it holds, its
/// type, such as <c>control-plane</c>, its release, and how long its heartbeat keeps it live.
/// </summary>
public sealed record ProcessRegistration(OwnerId Owner, string Type, string? Release, TimeSpan StaleAfter);

/// <summary>A process's state by the store's clock.</summary>
public enum ProcessState
{
    /// <summary>Running, with a heartbeat younger than its stale window. A recent heartbeat says nothing about readiness.</summary>
    Live,

    /// <summary>Not stopped, but its heartbeat is older than its stale window: it crashed, hangs or cannot reach the store.</summary>
    Stale,

    /// <summary>Marked stopped by a graceful shutdown.</summary>
    Stopped,
}

/// <summary>One process instance as the registry records it.</summary>
public sealed record ProcessInstance(
    OwnerId Owner,
    string Type,
    string? Release,
    ProcessState State,
    DateTimeOffset StartedAt,
    DateTimeOffset HeartbeatAt,
    DateTimeOffset? StoppedAt);
