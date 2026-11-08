using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>Crashed holders: takeover, takeover backoff, exhaustion and expiry settlement.</summary>
public abstract class LeaseTakeoverScenarios(ILeaseConformanceProvider provider) : LeaseScenarioBase(provider)
{
    [Fact]
    public async Task L8_CrashedHoldersRecordIsTakenOverAfterExpiryAndBackoff()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(1), p => p with { TakeoverBackoff = Backoff.Constant(TimeSpan.FromSeconds(1)) }));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var crashed = await ClientAsync(source);
        var stopwatch = Stopwatch.StartNew();
        Assert.NotNull(await crashed.Operations.ClaimAsync(source, key, Cancel));
        var taken = new TaskCompletionSource<(TimeSpan At, LeaseClaim<TestKey> Claim)>(TaskCreationOptions.RunContinuationsAsynchronously);
        var script = new HandlerScript
        {
            Run = async (run, cancellationToken) =>
            {
                taken.TrySetResult((stopwatch.Elapsed, run.Claim));
                await EndAsync(run, cancellationToken);
            },
        };
        await WorkerAsync(source, script, pollingInterval: TimeSpan.FromSeconds(10));

        var (at, claim) = await taken.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);
        Assert.InRange(at, TimeSpan.FromSeconds(1.9), TimeSpan.FromSeconds(4));
        Assert.Equal((ClaimReason.Takeover, 1, crashed.Owner, 1L), (claim.Reason, claim.Takeovers, claim.PreviousOwner!.Value, claim.PreviousEpoch));
    }

    [Fact]
    public async Task L9_RepeatedTakeoversBackOffAndThenGoToTheExhaustionHandler()
    {
        var duration = TimeSpan.FromMilliseconds(300);
        var source = await SourceAsync(Policy(duration, p => p with
        {
            MaximumTakeovers = 2,
            TakeoverBackoff = Backoff.Exponential(TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(5)),
        }));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var client = await ClientAsync(source);
        Assert.NotNull(await client.Operations.ClaimAsync(source, key, Cancel));
        var waits = new List<TimeSpan>();
        for (var takeover = 1; takeover <= 2; takeover++)
        {
            var stopwatch = Stopwatch.StartNew();
            var claim = await ClaimEventuallyAsync(client.Operations, source, key, TimeSpan.FromSeconds(10), OwnerId.New());
            waits.Add(stopwatch.Elapsed);
            Assert.Equal((takeover, false), (claim.Takeovers, claim.Exhausted));
        }
        Assert.True(waits[1] - waits[0] >= TimeSpan.FromMilliseconds(300), $"takeover backoff did not grow: {string.Join(", ", waits)}");

        var exhausted = new TaskCompletionSource<LeaseClaim<TestKey>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var script = new HandlerScript
        {
            Run = (_, _) => throw new InvalidOperationException("An exhausted record must not run."),
            Exhausted = async (run, cancellationToken) =>
            {
                await using var transaction = await Provider.BeginAsync(cancellationToken);
                await transaction.WriteProgressAsync(source.Store, run.Key, "gave up after repeated crashes", cancellationToken);
                await run.EndAsync(transaction.Work, cancellationToken: cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                exhausted.SetResult(run.Claim);
            },
        };
        await WorkerAsync(source, script, pollingInterval: TimeSpan.FromMilliseconds(200));

        var last = await exhausted.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);
        Assert.Equal((3, true), (last.Takeovers, last.Exhausted));
        var state = await ReadAsync(source, key);
        Assert.Equal(("gave up after repeated crashes", DueState.None, false), (state.Progress, state.Due, state.Held));
    }

    [Fact]
    public async Task L9_AGuardThatMarksProgressResetsTheTakeovers()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromMilliseconds(300), p => p with { MaximumTakeovers = 1, TakeoverBackoff = Backoff.None }));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var client = await ClientAsync(source);
        Assert.NotNull(await client.Operations.ClaimAsync(source, key, Cancel));
        var holder = await ClaimEventuallyAsync(client.Operations, source, key, TimeSpan.FromSeconds(10), OwnerId.New());
        Assert.Equal((1, false), (holder.Takeovers, holder.Exhausted));

        // A progressed guard whose transaction rolls back resets nothing; one that commits resets the counter.
        await using (var rolledBack = await BeginAsync())
        {
            await client.Operations.GuardAsync(holder.Lease, rolledBack.Work, progressed: true, Cancel);
        }
        Assert.Equal(1, (await ReadAsync(source, key)).Takeovers);
        await using (var progressed = await BeginAsync())
        {
            await progressed.WriteProgressAsync(source.Store, key, "p1", Cancel);
            await client.Operations.GuardAsync(holder.Lease, progressed.Work, progressed: true, Cancel);
            await progressed.CommitAsync(Cancel);
        }
        Assert.Equal(0, (await ReadAsync(source, key)).Takeovers);

        // The holder then disappears: the next takeover is the first without progress, so it is not exhausted.
        var next = await ClaimEventuallyAsync(client.Operations, source, key, TimeSpan.FromSeconds(10), OwnerId.New());
        Assert.Equal((ClaimReason.Takeover, 1, false), (next.Reason, next.Takeovers, next.Exhausted));
        var last = await ClaimEventuallyAsync(client.Operations, source, key, TimeSpan.FromSeconds(10), OwnerId.New());
        Assert.Equal((2, true), (last.Takeovers, last.Exhausted));
    }

    [Fact]
    public async Task L21_ExpiryProcessorSettlesAnUnclaimedExpiredLeaseOnce()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromMilliseconds(500), p => p with { SettleExpiry = true }));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var expiry = new ExpiryScript
        {
            Expired = (expired, transaction, cancellationToken) =>
                new(Provider.JournalAsync(transaction, source.Store, $"release {expired.Key} {expired.Epoch}", cancellationToken)),
        };
        var crashed = await ClientAsync(source);
        await using (var transaction = await BeginAsync())
        {
            var claims = await crashed.Operations.ClaimAsync(transaction.Work, source, [key], 1, Cancel);
            await Provider.JournalAsync(transaction.Work, source.Store, "reserve " + claims[0].Key, Cancel);
            await transaction.CommitAsync(Cancel);
        }
        await ProcessAsync(work =>
        {
            work.Services.AddSingleton(expiry);
            work.AddLeaseSource(source, s => s.OnExpired<ScriptedExpiryHandler>());
        });

        await EventuallyAsync(async () => (await JournalAsync(source)).Count == 2, TimeSpan.FromSeconds(5), "the expired lease is settled");
        await Task.Delay(1000, Cancel);
        Assert.Equal([$"reserve {key}", $"release {key} 1"], await JournalAsync(source));
        var state = await ReadAsync(source, key);
        Assert.Equal((1L, 1L, DueState.Past), (state.SettledEpoch, state.Epoch, state.Due));
    }

    [Fact]
    public async Task L21_AnExpiryHandlerThatFindsARowLockedIsRetriedWithoutASettlementFailure()
    {
        RequireConcurrentTransactions();
        var source = await SourceAsync(Policy(TimeSpan.FromMilliseconds(500), p => p with { SettleExpiry = true, SettlementBackoff = Backoff.Constant(TimeSpan.FromMinutes(5)) }));
        var parent = new TestKey("t", 100);
        var key = new TestKey("t", 1);
        await InsertAsync(source, parent, null);
        await InsertAsync(source, key, TimeSpan.Zero);
        var expiry = new ExpiryScript
        {
            Expired = async (expired, transaction, cancellationToken) =>
            {
                await Provider.LockWithoutWaitingAsync(transaction, source.Store, parent, cancellationToken);
                await Provider.JournalAsync(transaction, source.Store, $"release {expired.Key} {expired.Epoch}", cancellationToken);
            },
        };
        var crashed = await ClientAsync(source);
        Assert.NotNull(await crashed.Operations.ClaimAsync(source, key, Cancel));

        // While an application transaction holds the parent row, the handler cannot take it, and no settlement fails.
        await using (var holder = await BeginAsync())
        {
            await holder.LockAsync(source.Store, parent, Cancel);
            await ProcessAsync(work =>
            {
                work.Services.AddSingleton(expiry);
                work.AddLeaseSource(source, s => s.OnExpired<ScriptedExpiryHandler>());
            });
            await Task.Delay(2000, Cancel);

            var waiting = await ReadAsync(source, key);
            Assert.Equal((0L, 0), (waiting.SettledEpoch, waiting.SettleFailures));
            await holder.RollbackAsync(Cancel);
        }

        // Without a settlement backoff to wait for, a later pass settles it.
        await EventuallyAsync(async () => (await JournalAsync(source)).Count == 1, TimeSpan.FromSeconds(5), "the expired lease is settled");
        Assert.Equal([$"release {key} 1"], await JournalAsync(source));
        var state = await ReadAsync(source, key);
        Assert.Equal((1L, 0), (state.SettledEpoch, state.SettleFailures));
    }

    [Fact]
    public async Task L22_RacingSettlementsAndClaimsSettleEachEpochOnce()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromMilliseconds(400), p => p with { SettleExpiry = true }));
        var keys = await InsertManyAsync(source, 20);
        var expiry = new ExpiryScript
        {
            Expired = (expired, transaction, cancellationToken) =>
                new(Provider.JournalAsync(transaction, source.Store, $"release {expired.Key} {expired.Epoch}", cancellationToken)),
        };
        Action<WorkBuilder> settling = work =>
        {
            work.Services.AddSingleton(expiry);
            work.AddLeaseSource(source, s => s.OnExpired<ScriptedExpiryHandler>());
        };
        var crashed = await ClientAsync(source);
        Assert.Equal(20, (await crashed.Operations.ClaimDueAsync(source, 20, Cancel)).Count);
        await ProcessAsync(settling, pollingInterval: TimeSpan.FromMilliseconds(50));
        await ProcessAsync(settling, pollingInterval: TimeSpan.FromMilliseconds(50));
        var claimer = await ProcessAsync(settling, start: false);

        var claims = await Task.WhenAll(keys.Select(key => ClaimEventuallyAsync(claimer.Operations, source, key, TimeSpan.FromSeconds(10))));

        Assert.All(claims, c => Assert.Equal(2, c.Lease.Epoch));
        var journal = await JournalAsync(source);
        Assert.Equal(keys.Select(k => $"release {k} 1").Order(), journal.Order());
    }

    [Fact]
    public async Task L22_ClaimerWithoutTheExpiryHandlerLeavesAnUnsettledExpiryToTheHandler()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromMilliseconds(400), p => p with { SettleExpiry = true }));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var expiry = new ExpiryScript
        {
            Expired = (expired, transaction, cancellationToken) =>
                new(Provider.JournalAsync(transaction, source.Store, $"release {expired.Key} {expired.Epoch}", cancellationToken)),
        };
        var client = await ClientAsync(source);
        Assert.NotNull(await client.Operations.ClaimAsync(source, key, Cancel));
        await ExpiredAsync(source, key, TimeSpan.FromSeconds(5));

        Assert.Null(await client.Operations.ClaimAsync(source, key, Cancel));
        Assert.Empty(await client.Operations.ClaimDueAsync(source, 1, Cancel));
        Assert.Equal(0, (await ReadAsync(source, key)).SettledEpoch);

        await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(expiry);
                work.AddLeaseSource(source, s => s.OnExpired<ScriptedExpiryHandler>());
            },
            pollingInterval: TimeSpan.FromMilliseconds(100));
        var claim = await ClaimEventuallyAsync(client.Operations, source, key, TimeSpan.FromSeconds(5));

        Assert.Equal((ClaimReason.Takeover, 2L), (claim.Reason, claim.Lease.Epoch));
        Assert.Equal([$"release {key} 1"], await JournalAsync(source));
    }

    [Fact]
    public async Task L22_SettlementWakesAClaimerWithoutTheExpiryHandler()
    {
        if (!Provider.HasSignals)
        {
            Assert.Skip("The store has no commit-time signals.");
        }
        var source = await SourceAsync(Policy(TimeSpan.FromMilliseconds(300), p => p with { SettleExpiry = true }));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var crashed = await ClientAsync(source);
        Assert.NotNull(await crashed.Operations.ClaimAsync(source, key, Cancel));
        await ExpiredAsync(source, key, TimeSpan.FromSeconds(5));
        var ran = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var script = new HandlerScript
        {
            Run = async (run, cancellationToken) =>
            {
                await EndAsync(run, cancellationToken);
                ran.TrySetResult();
            },
        };
        await WorkerAsync(source, script, pollingInterval: TimeSpan.FromSeconds(30));
        await Task.Delay(300, Cancel);
        Assert.False(ran.Task.IsCompleted);

        var expiry = new ExpiryScript();
        await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(expiry);
                work.AddLeaseSource(source, s => s.OnExpired<ScriptedExpiryHandler>());
            },
            pollingInterval: TimeSpan.FromSeconds(30));

        await ran.Task.WaitAsync(TimeSpan.FromSeconds(5), Cancel);
    }

    [Fact]
    public async Task L22_DueClaimSettlesEachExpiredRecordAndPassesOneWhoseSettlementFails()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromMilliseconds(300), p => p with
        {
            SettleExpiry = true,
            SettlementBackoff = Backoff.Constant(TimeSpan.FromSeconds(30)),
        }));
        var keys = await InsertManyAsync(source, 3);
        var crashed = await ClientAsync(source);
        Assert.Equal(3, (await crashed.Operations.ClaimDueAsync(source, 3, Cancel)).Count);
        foreach (var key in keys)
        {
            await ExpiredAsync(source, key, TimeSpan.FromSeconds(5));
        }
        var expiry = new ExpiryScript
        {
            Expired = (expired, transaction, cancellationToken) => expired.Key == keys[1]
                ? throw new InvalidOperationException("The expiry handler failed.")
                : new(Provider.JournalAsync(transaction, source.Store, $"release {expired.Key} {expired.Epoch}", cancellationToken)),
        };
        var claimer = await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(expiry);
                work.AddLeaseSource(source, s => s.OnExpired<ScriptedExpiryHandler>());
            },
            start: false);

        var claims = await claimer.Operations.ClaimDueAsync(source, 3, Cancel);

        Assert.Equal([keys[0], keys[2]], claims.Select(c => c.Key).OrderBy(k => k.Id));
        Assert.All(claims, c => Assert.Equal((ClaimReason.Takeover, 2L), (c.Reason, c.Lease.Epoch)));
        Assert.Equal([$"release {keys[0]} 1", $"release {keys[2]} 1"], (await JournalAsync(source)).Order());
        var failed = await ReadAsync(source, keys[1]);
        Assert.Equal((crashed.Owner, 1L, 0L, 1, 0), (failed.Owner!.Value, failed.Epoch, failed.SettledEpoch, failed.SettleFailures, failed.Failures));
        Assert.Equal((1L, 1L), ((await ReadAsync(source, keys[0])).SettledEpoch, (await ReadAsync(source, keys[2])).SettledEpoch));
    }

    [Fact]
    public async Task L21_SettlementWithoutBackoffIsRetriedOnEveryProcessorPass()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromMilliseconds(300), p => p with
        {
            SettleExpiry = true,
            SettlementBackoff = Backoff.None,
        }));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var crashed = await ClientAsync(source);
        Assert.NotNull(await crashed.Operations.ClaimAsync(source, key, Cancel));
        await ExpiredAsync(source, key, TimeSpan.FromSeconds(5));
        var attempts = 0;
        var expiry = new ExpiryScript
        {
            Expired = (_, _, _) =>
            {
                Interlocked.Increment(ref attempts);
                throw new InvalidOperationException("The expiry handler failed.");
            },
        };
        await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(expiry);
                work.AddLeaseSource(source, s => s.OnExpired<ScriptedExpiryHandler>());
            },
            pollingInterval: TimeSpan.FromSeconds(30));

        // The handler counts an attempt before its failure commits, so the wait is for the committed count.
        RecordState state = default!;
        await EventuallyAsync(async () => (state = await ReadAsync(source, key)).SettleFailures >= 3, TimeSpan.FromSeconds(5), "the failing settlement is retried without waiting for the polling interval");
        Assert.Equal((0L, 0, DueState.Past), (state.SettledEpoch, state.Failures, state.Due));
        Assert.True(Volatile.Read(ref attempts) >= 3, $"{Volatile.Read(ref attempts)} handler attempts");
    }

    [Fact]
    public async Task L21_SettlementPassesLockedAndFailingRecords()
    {
        RequireConcurrentTransactions();
        var source = await SourceAsync(Policy(TimeSpan.FromMilliseconds(300), p => p with { SettleExpiry = true }));
        var keys = await InsertManyAsync(source, 3);
        var client = await ClientAsync(source);
        Assert.Equal(3, (await client.Operations.ClaimDueAsync(source, 3, Cancel)).Count);
        foreach (var key in keys)
        {
            await ExpiredAsync(source, key, TimeSpan.FromSeconds(5));
        }
        var store = Provider.CreateStore();
        var attempts = new ConcurrentQueue<TestKey>();
        var callbacks = new LeaseCallbacks<TestKey>(null, (expired, transaction, cancellationToken) =>
        {
            attempts.Enqueue(expired.Key);
            return expired.Key == keys[1]
                ? throw new InvalidOperationException("The expiry handler failed.")
                : new(Provider.JournalAsync(transaction, source.Store, $"release {expired.Key}", cancellationToken));
        });
        await using var locking = await BeginAsync();
        await locking.LockAsync(source.Store, keys[0], Cancel);

        Assert.Equal(1, await store.Leases.SettleExpiredAsync(source, callbacks, 1, Cancel));

        Assert.Equal([keys[1], keys[2]], attempts);
        Assert.Equal([$"release {keys[2]}"], await JournalAsync(source));
        Assert.Equal(0, (await ReadAsync(source, keys[0])).SettledEpoch);
        Assert.Equal(0, (await ReadAsync(source, keys[1])).SettledEpoch);
        await Assert.ThrowsAsync<ArgumentException>(async () => await store.Leases.SettleExpiredAsync(source, LeaseCallbacks<TestKey>.None, 1, Cancel));
    }

    [Fact]
    public async Task L21_FailedSettlementWaitsForItsBackoffWithoutCountingAFailure()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromMilliseconds(300), p => p with
        {
            SettleExpiry = true,
            SettlementBackoff = Backoff.Constant(TimeSpan.FromSeconds(1)),
        }));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var crashed = await ClientAsync(source);
        Assert.NotNull(await crashed.Operations.ClaimAsync(source, key, Cancel));
        await ExpiredAsync(source, key, TimeSpan.FromSeconds(5));
        var failing = true;
        var attempts = 0;
        var expiry = new ExpiryScript
        {
            Expired = (expired, transaction, cancellationToken) =>
            {
                Interlocked.Increment(ref attempts);
                return failing
                    ? throw new InvalidOperationException("The expiry handler failed.")
                    : new(Provider.JournalAsync(transaction, source.Store, $"release {expired.Key} {expired.Epoch}", cancellationToken));
            },
        };
        var claimer = await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(expiry);
                work.AddLeaseSource(source, s => s.OnExpired<ScriptedExpiryHandler>());
            },
            start: false);

        Assert.Null(await claimer.Operations.ClaimAsync(source, key, Cancel));
        var failed = await ReadAsync(source, key);
        Assert.Equal((1, 0L, 0, DueState.Past, 1L), (failed.SettleFailures, failed.SettledEpoch, failed.Failures, failed.Due, failed.Epoch));
        Assert.Null(await claimer.Operations.ClaimAsync(source, key, Cancel));
        Assert.Empty(await claimer.Operations.ClaimDueAsync(source, 1, Cancel));
        Assert.Equal(1, Volatile.Read(ref attempts));

        failing = false;
        var claim = await ClaimEventuallyAsync(claimer.Operations, source, key, TimeSpan.FromSeconds(5));

        Assert.Equal((ClaimReason.Takeover, 2L), (claim.Reason, claim.Lease.Epoch));
        Assert.Equal([$"release {key} 1"], await JournalAsync(source));
        var settled = await ReadAsync(source, key);
        Assert.Equal((0, 1L, 0), (settled.SettleFailures, settled.SettledEpoch, settled.Failures));
    }
}
