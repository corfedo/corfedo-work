using Microsoft.Extensions.DependencyInjection;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace Corfedo.Work.Conformance;

/// <summary>Store-enforced partition and spread limits, counted on outstanding leases.</summary>
public abstract class LeaseLimitScenarios(ILeaseConformanceProvider provider) : LeaseScenarioBase(provider)
{
    [Fact]
    public async Task L23_SpreadLimitDistributesAPartitionAcrossProcesses()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30), p => p with { SpreadLimit = 1 }), partitioned: true);
        await InsertManyAsync(source, 3, "busy");
        var holding = new ConcurrentDictionary<OwnerId, int>();
        var maximum = 0;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var script = new HandlerScript
        {
            Run = async (run, cancellationToken) =>
            {
                var now = holding.AddOrUpdate(run.Lease.Owner, 1, (_, n) => n + 1);
                InterlockedMax(ref maximum, now);
                await release.Task.WaitAsync(cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                holding.AddOrUpdate(run.Lease.Owner, 0, (_, n) => n - 1);
                await run.ReleaseAsync(Due.None, cancellationToken: CancellationToken.None);
            },
        };
        var workers = new List<TestProcess>();
        for (var i = 0; i < 3; i++)
        {
            workers.Add(await WorkerAsync(source, script, s => s.Capacity(3), pollingInterval: TimeSpan.FromMilliseconds(200)));
        }

        await EventuallyAsync(() => holding.Values.Sum() == 3, TimeSpan.FromSeconds(10), "all three records are held");
        Assert.Equal(1, maximum);
        Assert.All(workers, w => Assert.Equal(1, holding[w.Owner]));
        release.SetResult();
    }

    [Fact]
    public async Task L26_PartitionLimitRunsOneOfABusyPartitionWhileOthersRunImmediately()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30), p => p with { PartitionLimit = 1 }), partitioned: true);
        await InsertManyAsync(source, 5, "busy");
        var quiet = new TestKey("quiet", 1);
        await InsertAsync(source, quiet, TimeSpan.Zero);
        var running = new ConcurrentDictionary<string, int>();
        var maximum = new ConcurrentDictionary<string, int>();
        var started = new ConcurrentDictionary<TestKey, TimeSpan>();
        var stopwatch = Stopwatch.StartNew();
        var script = new HandlerScript
        {
            Run = async (run, cancellationToken) =>
            {
                started[run.Key] = stopwatch.Elapsed;
                var now = running.AddOrUpdate(run.Key.Tenant, 1, (_, n) => n + 1);
                maximum.AddOrUpdate(run.Key.Tenant, now, (_, m) => Math.Max(m, now));
                await Task.Delay(300, cancellationToken);
                running.AddOrUpdate(run.Key.Tenant, 0, (_, n) => n - 1);
                await EndAsync(run, cancellationToken);
            },
        };
        await WorkerAsync(source, script, s => s.Capacity(5), pollingInterval: TimeSpan.FromMilliseconds(200));
        await WorkerAsync(source, script, s => s.Capacity(5), pollingInterval: TimeSpan.FromMilliseconds(200));

        await EventuallyAsync(() => started.Count == 6, TimeSpan.FromSeconds(15), "every record runs");
        Assert.Equal(1, maximum["busy"]);
        Assert.True(started[quiet] < TimeSpan.FromSeconds(1), $"the quiet partition waited {started[quiet]}");
    }

    [Fact]
    public async Task L27_GuardedLeaseKeepsPartitionSlot()
    {
        RequireConcurrentTransactions();
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(2), p => p with { PartitionLimit = 1 }), partitioned: true);
        var guardedKey = new TestKey("p", 1);
        var waiting = new TestKey("p", 2);
        await InsertAsync(source, guardedKey, TimeSpan.Zero);
        var holder = await ClientAsync(source);
        var claimer = await ClientAsync(source);
        await ProcessAsync(work => work.AddLeaseSource(source), pollingInterval: TimeSpan.FromMilliseconds(100));
        var stopwatch = Stopwatch.StartNew();
        var claim = (await holder.Operations.ClaimAsync(source, guardedKey, Cancel))!;
        await InsertAsync(source, waiting, TimeSpan.Zero);
        await Task.Delay(1000, Cancel);

        await using (var guarded = await BeginAsync())
        {
            await holder.Operations.GuardAsync(claim.Lease, guarded.Work, Cancel);
            await guarded.WriteProgressAsync(source.Store, guardedKey, "output", Cancel);
            while (stopwatch.Elapsed < TimeSpan.FromSeconds(2.5))
            {
                Assert.Empty(await claimer.Operations.ClaimDueAsync(source, 10, Cancel));
                Assert.Null(await claimer.Operations.ClaimAsync(source, waiting, Cancel));
                await Task.Delay(100, Cancel);
            }
            await guarded.CommitAsync(Cancel);
        }

        Assert.Empty(await claimer.Operations.ClaimDueAsync(source, 10, Cancel));
        var guardedState = await ReadAsync(source, guardedKey);
        var waitingState = await ReadAsync(source, waiting);
        Assert.Equal((true, "output", 0L), (guardedState.Valid, guardedState.Progress, guardedState.SettledEpoch));
        Assert.Equal((false, 0L), (waitingState.Valid, waitingState.Epoch));
    }

    [Fact]
    public async Task L28_CrashedLeaseCountsUntilSettledThenTheNextRecordRuns()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(1), p => p with
        {
            PartitionLimit = 1,
            TakeoverBackoff = Backoff.Constant(TimeSpan.FromMinutes(5)),
        }), partitioned: true);
        var crashedKey = new TestKey("p", 1);
        var next = new TestKey("p", 2);
        await InsertAsync(source, crashedKey, TimeSpan.Zero);
        await InsertAsync(source, next, TimeSpan.Zero);
        var crashed = await ClientAsync(source);
        var stopwatch = Stopwatch.StartNew();
        var claim = (await crashed.Operations.ClaimAsync(source, crashedKey, Cancel))!;
        var ran = new TaskCompletionSource<(TimeSpan At, RecordState Crashed)>(TaskCreationOptions.RunContinuationsAsynchronously);
        var script = new HandlerScript
        {
            Run = async (run, cancellationToken) =>
            {
                ran.TrySetResult((stopwatch.Elapsed, await Provider.ReadAsync(source.Store, crashedKey, "", cancellationToken)));
                await EndAsync(run, cancellationToken);
            },
        };
        await WorkerAsync(source, script, pollingInterval: TimeSpan.FromMilliseconds(200));

        var (at, crashedState) = await ran.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);
        Assert.True(at >= TimeSpan.FromSeconds(0.9), $"the next record ran at {at}, before the crashed lease expired");
        Assert.Equal((claim.Lease.Epoch, claim.Lease.Epoch, false), (crashedState.Epoch, crashedState.SettledEpoch, crashedState.Valid));
    }

    [Fact]
    public async Task L26_ClaimWaitingForABusyPartitionHoldsNoRecordLocks()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30), p => p with { PartitionLimit = 1 }), partitioned: true);
        var claimed = new TestKey("p", 1);
        var sibling = new TestKey("p", 2);
        await InsertAsync(source, claimed, TimeSpan.Zero);
        await InsertAsync(source, sibling, TimeSpan.Zero);
        var api = await ClientAsync(source);
        var dispatcher = await ClientAsync(source);

        await using var transaction = await BeginAsync();
        Assert.Single(await api.Operations.ClaimAsync(transaction.Work, source, [claimed], 1, Cancel));
        var racing = dispatcher.Operations.ClaimDueAsync(source, 10, Cancel).AsTask();
        var byKey = dispatcher.Operations.ClaimAsync(source, sibling, Cancel).AsTask();
        await Task.Delay(300, Cancel);
        // The caller's transaction serializes the partition and then writes another record of it, which a claim racing
        // for the partition must not keep locked while it waits.
        await transaction.WriteProgressAsync(source.Store, sibling, "reserved", Cancel).WaitAsync(TimeSpan.FromSeconds(5), Cancel);
        await transaction.CommitAsync(Cancel);

        Assert.Empty(await racing.WaitAsync(TimeSpan.FromSeconds(5), Cancel));
        Assert.Null(await byKey.WaitAsync(TimeSpan.FromSeconds(5), Cancel));
        Assert.Equal(("reserved", false), ((await ReadAsync(source, sibling)).Progress, (await ReadAsync(source, sibling)).Held));
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int current;
        while (value > (current = Volatile.Read(ref target)) && Interlocked.CompareExchange(ref target, value, current) != current)
        {
        }
    }

    [Fact]
    public async Task L26_ClaimCancelledInALaterPassReturnsTheClaimsItCommitted()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30), p => p with { PartitionLimit = 1 }), partitioned: true);
        var first = new TestKey("p", 1);
        await InsertAsync(source, first, TimeSpan.FromSeconds(-3));
        await InsertAsync(source, new TestKey("p", 2), TimeSpan.FromSeconds(-2));
        var other = new TestKey("q", 1);
        await InsertAsync(source, other, TimeSpan.FromSeconds(-1));
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(Cancel);
        var hook = new ClaimHookScript
        {
            Claimed = (claim, _, cancellationToken) =>
            {
                if (claim.Key == other)
                {
                    cancel.Cancel();
                    cancellationToken.ThrowIfCancellationRequested();
                }
                return ValueTask.CompletedTask;
            },
        };
        var client = await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(hook);
                work.AddLeaseSource(source, s => s.OnClaimed<ScriptedClaimHook>());
            },
            start: false);

        var claims = await client.Operations.ClaimDueAsync(source, 2, cancel.Token);

        Assert.Equal([first], claims.Select(c => c.Key));
        Assert.Equal((true, client.Owner), ((await ReadAsync(source, first)).Held, (await ReadAsync(source, first)).Owner!.Value));
        Assert.Equal(0, (await ReadAsync(source, other)).Epoch);
    }

    [Fact]
    public async Task L29_DueClaimSkipsLimitScopesAnApplicationTransactionHolds()
    {
        RequireConcurrentTransactions();
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30), p => p with { PartitionLimit = 2, OwnerLimit = 10 }), partitioned: true);
        var reserved = new TestKey("p", 1);
        var busy = new TestKey("p", 2);
        var free = new TestKey("q", 1);
        await InsertAsync(source, reserved, TimeSpan.FromSeconds(-3));
        await InsertAsync(source, busy, TimeSpan.FromSeconds(-2));
        await InsertAsync(source, free, TimeSpan.FromSeconds(-1));
        var api = await ClientAsync(source);
        var dispatcher = await ClientAsync(source);

        await using (var transaction = await BeginAsync())
        {
            Assert.Single(await api.Operations.ClaimAsync(transaction.Work, source, [reserved], 1, Cancel));
            // The partition's scope is held until the transaction ends; a due claim takes the other partition's record
            // and returns without waiting for it.
            var claims = await dispatcher.Operations.ClaimDueAsync(source, 10, Cancel).AsTask().WaitAsync(TimeSpan.FromSeconds(2), Cancel);
            Assert.Equal([free], claims.Select(c => c.Key));
            await transaction.CommitAsync(Cancel);
        }
        Assert.Equal([busy], (await dispatcher.Operations.ClaimDueAsync(source, 10, Cancel)).Select(c => c.Key));

        await InsertAsync(source, new TestKey("r", 1), TimeSpan.Zero);
        await using (var transaction = await BeginAsync())
        {
            // The owner's scope is held: every claim of that owner needs it, so the call returns at once without claims.
            Assert.Empty(await dispatcher.Operations.ClaimAsync(transaction.Work, source, [free], 1, Cancel));
            Assert.Empty(await dispatcher.Operations.ClaimDueAsync(source, 10, Cancel).AsTask().WaitAsync(TimeSpan.FromSeconds(2), Cancel));
            await transaction.RollbackAsync(Cancel);
        }
        Assert.Equal(1, (await dispatcher.Operations.ClaimDueAsync(source, 10, Cancel)).Count);
    }
}
