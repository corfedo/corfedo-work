namespace Corfedo.Work;

/// <summary>
/// Claims due records of its sources up to one capacity and runs them. Wakes on store signals, at the earliest known
/// due time and at the polling interval. Each pass offers the free capacity to the highest priority class first; lower
/// classes get what higher ones do not claim, and may starve while higher classes keep claimable work. Within a class
/// the pass starts with the next source in turn and offers every source an equal share of the free capacity before
/// offering what is left over, so a source with a large backlog cannot keep another source of its class waiting for
/// more than one rotation. Claims never preempt running work. A source with a capacity of its own is a pool of one.
/// </summary>
public sealed class DispatchPool : ILeaseDispatcher
{
    private static readonly TimeSpan ErrorDelay = TimeSpan.FromSeconds(1);

    private readonly int capacity;
    private readonly LeaseDispatchContext context;
    private readonly IDispatchLane[] lanes;
    private readonly WakeSignal wake = new();
    private readonly CancellationTokenSource stopping = new();
    private readonly Dictionary<int, int> turns = [];
    private Task loop = Task.CompletedTask;

    public DispatchPool(string name, int capacity, IEnumerable<LeaseSourceRegistration> registrations, LeaseDispatchContext context)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        Name = name;
        this.capacity = capacity;
        this.context = context;
        lanes = registrations.Select(r => r.CreateLane(context, wake.Set, stopping.Token)).ToArray();
        if (lanes.Length == 0)
        {
            throw new ArgumentException($"Dispatch pool {name} has no sources.", nameof(registrations));
        }
        Sources = lanes.Select(l => l.Source).ToArray();
    }

    public string Name { get; }

    public IReadOnlyList<string> Sources { get; }

    public void Start() => loop = Task.Run(LoopAsync);

    public void Wake() => wake.Set();

    public void CheckCancellation(string source)
    {
        foreach (var lane in lanes)
        {
            if (lane.Source == source)
            {
                lane.CheckCancellation();
            }
        }
    }

    private async Task LoopAsync()
    {
        var token = stopping.Token;
        while (!token.IsCancellationRequested)
        {
            TimeSpan delay;
            try
            {
                var free = capacity - lanes.Sum(l => l.Running);
                if (free <= 0)
                {
                    // Nothing can be claimed until a run finishes, which wakes the pool.
                    delay = context.PollingInterval;
                }
                else
                {
                    (var claimed, var failed) = await ClaimAsync(free, token);
                    if (claimed > 0)
                    {
                        continue;
                    }
                    delay = context.PollingInterval;
                    foreach (var lane in lanes)
                    {
                        try
                        {
                            if (await lane.NextDueAsync(token) is { } due && due < delay)
                            {
                                delay = due;
                            }
                        }
                        catch (Exception exception) when (!token.IsCancellationRequested)
                        {
                            lane.LogClaimFailed(exception);
                            failed = true;
                        }
                    }
                    if (failed && ErrorDelay < delay)
                    {
                        delay = ErrorDelay;
                    }
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return;
            }
            try
            {
                await wake.WaitAsync(delay, context.Time, token);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    /// <summary>
    /// One pass: the free capacity goes to the highest priority class first and what it leaves to the next. Within a
    /// class every source in turn gets an equal share, then sources that used their whole share get what is left. A
    /// failing source is logged and skipped, so it does not hold back the others.
    /// </summary>
    private async Task<(int Claimed, bool Failed)> ClaimAsync(int free, CancellationToken cancellationToken)
    {
        var remaining = free;
        var claimed = 0;
        var failed = false;
        // Priorities are read every pass: a job source's class follows its stored definition.
        foreach (var priorityClass in lanes.GroupBy(l => l.Priority).OrderByDescending(g => g.Key))
        {
            if (remaining <= 0)
            {
                break;
            }
            var (classClaimed, classFailed) = await ClaimClassAsync(priorityClass.Key, [.. priorityClass], remaining, cancellationToken);
            remaining -= classClaimed;
            claimed += classClaimed;
            failed |= classFailed;
        }
        return (claimed, failed);
    }

    /// <summary>One rotation within a priority class, starting with the class's next source in turn.</summary>
    private async Task<(int Claimed, bool Failed)> ClaimClassAsync(int priority, IDispatchLane[] members, int free, CancellationToken cancellationToken)
    {
        var next = turns.GetValueOrDefault(priority) % members.Length;
        turns[priority] = (next + 1) % members.Length;
        var order = new IDispatchLane[members.Length];
        for (var i = 0; i < members.Length; i++)
        {
            order[i] = members[(next + i) % members.Length];
        }
        var remaining = free;
        var claimed = 0;
        var failed = false;
        var full = new List<IDispatchLane>();
        for (var i = 0; i < order.Length && remaining > 0; i++)
        {
            var share = (remaining + order.Length - i - 1) / (order.Length - i);
            var count = await TryClaimAsync(order[i], share, cancellationToken);
            if (count < 0)
            {
                failed = true;
                continue;
            }
            remaining -= count;
            claimed += count;
            if (count == share)
            {
                full.Add(order[i]);
            }
        }
        foreach (var lane in full)
        {
            if (remaining <= 0 || order.Length == 1)
            {
                break;
            }
            var count = await TryClaimAsync(lane, remaining, cancellationToken);
            if (count > 0)
            {
                remaining -= count;
                claimed += count;
            }
        }
        return (claimed, failed);
    }

    /// <summary>Claims for one source; -1 if the claim failed.</summary>
    private static async Task<int> TryClaimAsync(IDispatchLane lane, int limit, CancellationToken cancellationToken)
    {
        try
        {
            return await lane.ClaimAsync(limit, cancellationToken);
        }
        catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
        {
            lane.LogClaimFailed(exception);
            return -1;
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        stopping.Cancel();
        await loop.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        await Task.WhenAll(lanes.Select(l => l.StopAsync(cancellationToken)));
        stopping.Dispose();
    }
}
