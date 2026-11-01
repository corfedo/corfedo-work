namespace Corfedo.Work;

/// <summary>
/// Lists the processes recorded in the process registry: every process added with <c>AddWorkProcess</c>, and every
/// process that adds work and runs a handler, declares a code-defined recurrence or describes itself with
/// <c>work.Process(...)</c>. Processes that only request work are not recorded. States use the store's clock: a live
/// process has a recent heartbeat, which says nothing about its readiness. Every starting process deletes records that
/// stopped or became stale longer than <see cref="Retention"/> ago.
/// </summary>
public sealed class ProcessRegistry(IWorkStore store)
{
    private static readonly ProcessState[] LiveOnly = [ProcessState.Live];

    /// <summary>How long a process record is kept after the process stopped, or after its heartbeat became stale without a stop.</summary>
    public static TimeSpan Retention { get; } = TimeSpan.FromDays(7);

    /// <summary>
    /// The processes of <paramref name="type"/>, or of every type, in one of <paramref name="states"/>, by default
    /// live ones; ordered by type and start.
    /// </summary>
    public ValueTask<IReadOnlyList<ProcessInstance>> ListAsync(string? type = null, IReadOnlyCollection<ProcessState>? states = null, CancellationToken cancellationToken = default) =>
        store.Processes.ListAsync(type, states ?? LiveOnly, cancellationToken);
}
