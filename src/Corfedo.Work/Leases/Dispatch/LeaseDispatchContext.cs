using Microsoft.Extensions.Logging;

namespace Corfedo.Work;

/// <summary>
/// What dispatchers, expiry processors and background loops of one process share. <see cref="Signals"/> is null when
/// the store has no signals.
/// </summary>
public sealed record LeaseDispatchContext(
    IServiceProvider Services,
    IWorkStore Store,
    LeaseOperations Operations,
    TimeProvider Time,
    ILoggerFactory Logging,
    TimeSpan PollingInterval,
    TimeSpan ShutdownGrace,
    IWorkSignals? Signals = null);

/// <summary>Claims due records of its sources up to its capacity and runs them.</summary>
public interface ILeaseDispatcher
{
    /// <summary>The pool's name, or the source's name for a source with a capacity of its own.</summary>
    string Name { get; }

    /// <summary>The sources whose records the dispatcher claims.</summary>
    IReadOnlyList<string> Sources { get; }

    void Start();

    /// <summary>Wakes the dispatcher to claim now.</summary>
    void Wake();

    /// <summary>Renews the running holds of <paramref name="source"/> now, to observe a cancellation request.</summary>
    void CheckCancellation(string source);

    /// <summary>
    /// Stops claiming, cancels running handlers with the shutdown reason, waits for them up to the grace period, and
    /// releases records still held afterwards without counting a takeover or a failure.
    /// </summary>
    Task StopAsync(CancellationToken cancellationToken);
}

/// <summary>Settles expired leases of one source, as a pass of the process's expiry loop.</summary>
public interface IExpiryProcessor
{
    string Source { get; }

    /// <summary>Settles what has expired; returns how long the source waits before its next pass.</summary>
    Task<TimeSpan> SettleAsync(CancellationToken cancellationToken);
}

/// <summary>The runs of one source in a dispatcher: claims on request, runs the claims, and stops them on shutdown.</summary>
public interface IDispatchLane
{
    string Source { get; }

    /// <summary>Records of the source this lane holds now.</summary>
    int Running { get; }

    /// <summary>The source's priority class: a pool offers free capacity to higher classes first.</summary>
    int Priority { get; }

    /// <summary>Claims up to <paramref name="limit"/> due records and starts their runs; returns how many it claimed.</summary>
    Task<int> ClaimAsync(int limit, CancellationToken cancellationToken);

    /// <summary>
    /// The time until a record not considered by this lane's last claim may become claimable; zero to claim again, null
    /// when none is known or the last claim failed.
    /// </summary>
    ValueTask<TimeSpan?> NextDueAsync(CancellationToken cancellationToken);

    void CheckCancellation();

    void LogClaimFailed(Exception exception);

    /// <summary>Stops the runs once the dispatcher no longer claims; see <see cref="ILeaseDispatcher.StopAsync"/>.</summary>
    Task StopAsync(CancellationToken cancellationToken);
}

/// <summary>A dispatch pool shared by several sources: one capacity, offered by priority class, with claims rotating between the sources of a class.</summary>
public sealed record DispatchPoolDefinition(string Name, int Capacity);
