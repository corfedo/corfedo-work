namespace Corfedo.Work;

/// <summary>Why a hold's token was cancelled.</summary>
public enum LeaseStopReason
{
    /// <summary>The lease is no longer valid; output can no longer commit.</summary>
    Lost,

    /// <summary>Cancellation was requested for the record.</summary>
    CancelRequested,

    /// <summary>The run took longer than its registration allows.</summary>
    Timeout,

    /// <summary>The process is shutting down.</summary>
    Shutdown,

    /// <summary>
    /// On a reconciliation source that supersedes, a newer request or an invalidation made the run obsolete: a
    /// renewal failed while the lease had not expired.
    /// </summary>
    Superseded,
}

/// <summary>
/// Keeps a claimed lease alive while work runs: renews every third of the lease duration, tolerates failed renewals
/// until the last confirmed expiry would have passed, and cancels <see cref="Token"/> when the lease is lost,
/// cancellation is requested, the run times out or the process shuts down. Expiry is tracked with the local
/// monotonic clock from what the store reports, so process clock skew does not matter, and enforced by a timer of its
/// own, so a renewal that does not return cannot keep the token alive past the last confirmed expiry.
/// </summary>
public sealed class LeaseHold<TKey> : IAsyncDisposable
    where TKey : notnull
{
    private static readonly TimeSpan MinimumRetry = TimeSpan.FromMilliseconds(50);

    private readonly ILeaseStore store;
    private readonly TimeProvider time;
    private readonly CancellationTokenSource token = new();
    private readonly CancellationTokenSource stopRenewing = new();
    private readonly SemaphoreSlim renewNow = new(0, 1);
    private readonly TimeSpan interval;
    private readonly ITimer expiry;
    private readonly ITimer? timeout;
    private long deadline;
    private int reason = -1;
    private int lost;

    internal LeaseHold(ILeaseStore store, TimeProvider time, LeaseClaim<TKey> claim, long claimedAt, TimeSpan? timeout)
    {
        this.store = store;
        this.time = time;
        Lease = claim.Lease;
        interval = claim.Lease.Source.Policy.Duration / 3;
        deadline = claimedAt + Ticks(claim.ValidFor);
        if (claim.CancelRequested)
        {
            Stop(LeaseStopReason.CancelRequested);
        }
        var remaining = Remaining();
        if (remaining <= TimeSpan.Zero)
        {
            // The claim's validity was used up before the hold started, for example by a slow claim hook.
            MarkLost();
        }
        expiry = time.CreateTimer(static state => ((LeaseHold<TKey>)state!).Expire(), this, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        if (timeout is { } after)
        {
            this.timeout = time.CreateTimer(static state => ((LeaseHold<TKey>)state!).Stop(LeaseStopReason.Timeout), this, after, Timeout.InfiniteTimeSpan);
        }
        if (Lost)
        {
            return;
        }
        Arm(remaining);
        _ = Task.Run(() => RenewLoopAsync(NextRenewal(remaining)));
    }

    public Lease<TKey> Lease { get; }

    /// <summary>Cancelled when the holder should stop; <see cref="StopReason"/> says why.</summary>
    public CancellationToken Token => token.Token;

    /// <summary>The first reason the token was cancelled, or null while it is not.</summary>
    public LeaseStopReason? StopReason => Volatile.Read(ref reason) is var value and >= 0 ? (LeaseStopReason)value : null;

    /// <summary>Whether the lease was observed lost or its last confirmed expiry passed.</summary>
    public bool Lost => Volatile.Read(ref lost) == 1;

    /// <summary>Cancels the token with <paramref name="stopReason"/> unless it is already cancelled.</summary>
    public void Stop(LeaseStopReason stopReason)
    {
        if (Interlocked.CompareExchange(ref reason, (int)stopReason, -1) == -1)
        {
            token.Cancel();
        }
    }

    /// <summary>Renews at once instead of waiting for the next interval, for example after a cancellation signal.</summary>
    public void RenewNow()
    {
        try
        {
            renewNow.Release();
        }
        catch (SemaphoreFullException)
        {
        }
    }

    /// <summary>
    /// Stops renewing without waiting for a renewal in progress, so the lease expires unless it is given back. Used when
    /// the holder no longer owns the work, for example after the shutdown grace period.
    /// </summary>
    internal void StopRenewing() => stopRenewing.Cancel();

    private async Task RenewLoopAsync(TimeSpan wait)
    {
        while (!stopRenewing.IsCancellationRequested)
        {
            try
            {
                await renewNow.WaitAsync(wait, stopRenewing.Token);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            var started = time.GetTimestamp();
            try
            {
                var result = await store.RenewAsync(Lease, stopRenewing.Token);
                if (Lost || stopRenewing.IsCancellationRequested)
                {
                    return;
                }
                if (!result.Valid)
                {
                    // A lease that has not expired is taken away only by supersession; the local deadline is
                    // conservative, so a lease within it has not expired by the store's clock either.
                    if (Lease.Source.Reconcile is { Supersedes: true } && Remaining() > TimeSpan.Zero)
                    {
                        Stop(LeaseStopReason.Superseded);
                    }
                    MarkLost();
                    return;
                }
                Volatile.Write(ref deadline, started + Ticks(result.ValidFor));
                var remaining = Remaining();
                Arm(remaining);
                if (result.CancelRequested)
                {
                    Stop(LeaseStopReason.CancelRequested);
                }
                wait = NextRenewal(remaining);
            }
            catch (OperationCanceledException) when (stopRenewing.IsCancellationRequested)
            {
                return;
            }
            catch (Exception)
            {
                if (Lost)
                {
                    return;
                }
                var remaining = Remaining();
                wait = TimeSpan.FromTicks(Math.Min(interval.Ticks, Math.Max(remaining.Ticks / 2, MinimumRetry.Ticks)));
            }
        }
    }

    /// <summary>Renews at a third of the lease duration, or sooner when less validity remains.</summary>
    private TimeSpan NextRenewal(TimeSpan remaining) =>
        TimeSpan.FromTicks(Math.Max(Math.Min(interval.Ticks, remaining.Ticks / 3), 0));

    /// <summary>Runs at the last confirmed expiry; a renewal confirmed meanwhile moves the deadline and the timer.</summary>
    private void Expire()
    {
        var remaining = Remaining();
        if (remaining > TimeSpan.Zero)
        {
            Arm(remaining);
            return;
        }
        MarkLost();
    }

    private void Arm(TimeSpan remaining)
    {
        try
        {
            expiry.Change(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero, Timeout.InfiniteTimeSpan);
        }
        catch (ObjectDisposedException)
        {
        }
    }

    private TimeSpan Remaining() => time.GetElapsedTime(time.GetTimestamp(), Volatile.Read(ref deadline));

    private void MarkLost()
    {
        Volatile.Write(ref lost, 1);
        stopRenewing.Cancel();
        Stop(LeaseStopReason.Lost);
    }

    private long Ticks(TimeSpan span) => (long)(span.TotalSeconds * time.TimestampFrequency);

    /// <summary>
    /// Stops renewing. The lease stays as it is; the caller ends or releases the record. <see cref="Token"/> stays
    /// usable, since work that observed it may still be running. A renewal in progress is not awaited: it is cancelled
    /// and its result is ignored.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        stopRenewing.Cancel();
        await expiry.DisposeAsync();
        if (timeout is not null)
        {
            await timeout.DisposeAsync();
        }
    }
}
