namespace Corfedo.Work;

/// <summary>
/// Wakes callers waiting for jobs to end. While anyone waits, one loop per process subscribes to
/// <see cref="WorkTopics.Ended"/> signals and rereads every waited job of a signalled definition in one statement
/// per definition; it also rereads all of them on <see cref="ISignalSubscriber.Resync"/>, when a waiter has marked its
/// job, and every <see cref="Fallback"/>, so signals lost while disconnected and stores without signals only delay the
/// wake. A waiter marks its job only after the first resync, and rereads it after the mark committed: a transition
/// that commits before the mark is seen by the reread, and one that commits after it sees the mark and signals.
/// </summary>
internal sealed class JobWaiter(IWorkStore store, IWorkSignals? signals)
{
    /// <summary>The longest time between rereads of the waited jobs.</summary>
    public static readonly TimeSpan Fallback = TimeSpan.FromSeconds(1);

    private readonly Lock gate = new();
    private readonly Dictionary<(string Definition, JobId Job), List<TaskCompletionSource>> waiting = [];
    private readonly HashSet<string> signalled = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> sources = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim wake = new(0, 1);
    private bool all;
    private CancellationTokenSource? running;
    private TaskCompletionSource listening = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Completes when the job of the definition has no outstanding execution, as far as a reread shows, or when
    /// <paramref name="timeout"/> passes.
    /// </summary>
    public async Task WaitAsync(JobDefinition definition, JobId job, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var key = (definition.Key, job);
        var ended = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task connected;
        lock (gate)
        {
            (waiting.TryGetValue(key, out var list) ? list : waiting[key] = []).Add(ended);
            sources[definition.SourceName] = definition.Key;
            if (running is null)
            {
                running = new CancellationTokenSource();
                listening = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                if (signals is null)
                {
                    listening.TrySetResult();
                }
                // Run outside the lock: a subscription made while the hub listens resyncs at once, which takes it.
                var stopping = running;
                var subscribed = listening;
                _ = Task.Run(() => RunAsync(stopping, subscribed));
            }
            connected = listening.Task;
        }
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        if (timeout != Timeout.InfiniteTimeSpan)
        {
            deadline.CancelAfter(timeout);
        }
        try
        {
            await connected.WaitAsync(deadline.Token);
            // Marking waits for a transition holding the job's lock; another waiter's mark is shared, and nothing
            // clears it except the transition that leaves the job without an outstanding execution.
            await store.Jobs.WatchAsync(definition.Key, job, deadline.Token);
            WakeAll();
            await ended.Task.WaitAsync(deadline.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
        }
        finally
        {
            lock (gate)
            {
                if (waiting.TryGetValue(key, out var list) && list.Remove(ended) && list.Count == 0)
                {
                    waiting.Remove(key);
                }
            }
        }
    }

    private async Task RunAsync(CancellationTokenSource stopping, TaskCompletionSource connected)
    {
        var stop = stopping.Token;
        var subscription = signals?.Subscribe(WorkTopics.Ended, new Subscriber(this, connected));
        try
        {
            while (true)
            {
                var woken = await wake.WaitAsync(Fallback, stop);
                (string Definition, JobId Job)[] keys;
                lock (gate)
                {
                    // Every waited job is reread at least every fallback interval, signalled or not.
                    all |= !woken;
                    if (waiting.Count == 0)
                    {
                        sources.Clear();
                        running = null;
                        stopping.Cancel();
                        break;
                    }
                    var definitions = all ? null : signalled.ToHashSet(StringComparer.Ordinal);
                    keys = [.. waiting.Keys.Where(k => definitions is null || definitions.Contains(k.Definition))];
                    all = false;
                    signalled.Clear();
                }
                await ReadAsync(keys, stop);
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested)
        {
        }
        finally
        {
            subscription?.Dispose();
            stopping.Dispose();
        }
    }

    /// <summary>Completes the waiters of jobs that ended. A failed read is retried at the next wake or fallback.</summary>
    private async Task ReadAsync((string Definition, JobId Job)[] keys, CancellationToken stop)
    {
        foreach (var group in keys.GroupBy(k => k.Definition, StringComparer.Ordinal))
        {
            IReadOnlyList<StoredJobOutcome> outcomes;
            try
            {
                outcomes = await store.Jobs.GetOutcomesAsync(group.Key, [.. group.Select(k => k.Job)], stop);
            }
            catch (Exception) when (!stop.IsCancellationRequested)
            {
                continue;
            }
            var found = outcomes.ToDictionary(o => o.Job);
            lock (gate)
            {
                foreach (var key in group)
                {
                    // A job that no longer exists ends the wait too; the caller's reread reports it.
                    if ((!found.TryGetValue(key.Job, out var outcome) || outcome.Ended) && waiting.TryGetValue(key, out var list))
                    {
                        foreach (var waiter in list)
                        {
                            waiter.TrySetResult();
                        }
                    }
                }
            }
        }
    }

    private void Received(string source)
    {
        lock (gate)
        {
            if (!sources.TryGetValue(source, out var definition))
            {
                return;
            }
            signalled.Add(definition);
        }
        Wake();
    }

    private void WakeAll()
    {
        lock (gate)
        {
            all = true;
        }
        Wake();
    }

    private void Wake()
    {
        try
        {
            if (wake.CurrentCount == 0)
            {
                wake.Release();
            }
        }
        catch (SemaphoreFullException)
        {
        }
    }

    private sealed class Subscriber(JobWaiter waiter, TaskCompletionSource connected) : ISignalSubscriber
    {
        public void Received(string key) => waiter.Received(key);

        public void Resync()
        {
            connected.TrySetResult();
            waiter.WakeAll();
        }
    }
}
