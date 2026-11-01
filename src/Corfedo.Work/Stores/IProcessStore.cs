namespace Corfedo.Work;

/// <summary>
/// The process registry of one store: process instances and their heartbeats. A process is live while it has not
/// stopped and its last heartbeat is younger than its own stale window, both by the store's clock. Liveness is
/// information only; lease validity never depends on it.
/// </summary>
public interface IProcessStore
{
    /// <summary>Records the process with start and heartbeat at the store's clock; a recorded owner is left as it is.</summary>
    ValueTask RegisterAsync(ProcessRegistration registration, CancellationToken cancellationToken);

    /// <summary>Sets the process's heartbeat to the store's clock; false when the process is no longer recorded.</summary>
    ValueTask<bool> HeartbeatAsync(OwnerId owner, CancellationToken cancellationToken);

    /// <summary>Marks the process stopped at the store's clock.</summary>
    ValueTask StopAsync(OwnerId owner, CancellationToken cancellationToken);

    /// <summary>The recorded processes of <paramref name="type"/>, or of every type, in one of <paramref name="states"/>, by type and start.</summary>
    ValueTask<IReadOnlyList<ProcessInstance>> ListAsync(string? type, IReadOnlyCollection<ProcessState> states, CancellationToken cancellationToken);

    /// <summary>
    /// Deletes up to <paramref name="limit"/> processes that stopped longer than <paramref name="retention"/> ago, or
    /// whose heartbeat became stale longer ago without a stop, with what other layers recorded for them; returns how
    /// many it deleted.
    /// </summary>
    ValueTask<int> RetainAsync(TimeSpan retention, int limit, CancellationToken cancellationToken);
}
