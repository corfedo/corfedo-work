namespace Corfedo.Work;

/// <summary>An auto-reset wake-up that coalesces signals arriving while nobody waits.</summary>
internal sealed class WakeSignal
{
    private TaskCompletionSource next = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Set() => Volatile.Read(ref next).TrySetResult();

    /// <summary>
    /// Waits for a signal or the delay, whichever comes first. The delay is rounded up to whole milliseconds, the
    /// resolution of timers, so a wait for a due time does not end before it.
    /// </summary>
    public async Task WaitAsync(TimeSpan delay, TimeProvider time, CancellationToken cancellationToken)
    {
        var current = Volatile.Read(ref next);
        if (!current.Task.IsCompleted)
        {
            var rounded = TimeSpan.FromMilliseconds(Math.Ceiling(delay.TotalMilliseconds));
            await current.Task.WaitAsync(rounded, time, cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            cancellationToken.ThrowIfCancellationRequested();
        }
        Interlocked.CompareExchange(ref next, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously), current);
    }
}
