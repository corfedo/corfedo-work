using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>Claiming: concurrency, order, claims by key and in the caller's transaction, hooks, wake-up, clocks.</summary>
public abstract class LeaseClaimScenarios(ILeaseConformanceProvider provider) : LeaseScenarioBase(provider)
{
    [Fact]
    public async Task L1_ConcurrentClaimersClaimEachRecordOnceWithoutWaiting()
    {
        RequireConcurrentTransactions();
        var source = await SourceAsync(Policy(TimeSpan.FromMinutes(1)));
        var keys = await InsertManyAsync(source, 200);
        var client = await ClientAsync(source);

        // A record locked by another transaction is skipped, not waited for.
        await using var blocker = await BeginAsync();
        await blocker.LockAsync(source.Store, keys[0], Cancel);

        var claimed = new ConcurrentBag<TestKey>();
        var owners = Enumerable.Range(0, 20).Select(_ => OwnerId.New()).ToArray();
        var claimers = owners.Select(owner => Task.Run(async () =>
        {
            while (true)
            {
                var claims = await client.Operations.ClaimDueAsync(source, 7, Cancel, owner);
                if (claims.Count == 0)
                {
                    return;
                }
                foreach (var claim in claims)
                {
                    claimed.Add(claim.Key);
                }
            }
        }));
        await Task.WhenAll(claimers).WaitAsync(TimeSpan.FromSeconds(30), Cancel);

        Assert.Equal(keys.Length - 1, claimed.Count);
        Assert.Equal(keys.Skip(1).Select(k => k.Id).Order(), claimed.Select(k => k.Id).Order());
        await blocker.RollbackAsync(Cancel);
        Assert.Equal(0, (await ReadAsync(source, keys[0])).Epoch);
        foreach (var key in keys.Skip(1))
        {
            Assert.Equal(1, (await ReadAsync(source, key)).Epoch);
        }
    }

    [Fact]
    public async Task L2_RecordsAreClaimedInDueOrderAndNotBeforeTheirTime()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30)));
        var early = new TestKey("t", 1);
        var middle = new TestKey("t", 2);
        var late = new TestKey("t", 3);
        var future = new TestKey("t", 4);
        await InsertAsync(source, middle, TimeSpan.FromSeconds(-2));
        await InsertAsync(source, late, TimeSpan.FromSeconds(-1));
        await InsertAsync(source, early, TimeSpan.FromSeconds(-3));
        await InsertAsync(source, future, TimeSpan.FromSeconds(2));
        var order = new ConcurrentQueue<(TestKey Key, DueState DueWhenRun)>();
        var script = new HandlerScript
        {
            Run = async (run, cancellationToken) =>
            {
                order.Enqueue((run.Key, (await Provider.ReadAsync(source.Store, run.Key, "", cancellationToken)).Due));
                await EndAsync(run, cancellationToken);
            },
        };
        var stopwatch = Stopwatch.StartNew();
        await WorkerAsync(source, script, s => s.Capacity(1));

        await EventuallyAsync(() => order.Count == 4, TimeSpan.FromSeconds(10), "all four records run");
        Assert.Equal([early, middle, late, future], order.Select(o => o.Key));
        Assert.All(order, o => Assert.Equal(DueState.Past, o.DueWhenRun));
        Assert.True(stopwatch.Elapsed >= TimeSpan.FromSeconds(1.5), "the future record was claimed before it was due");
    }

    [Fact]
    public async Task L3_ClaimByKeyRacingADispatcherSucceedsExactlyOnce()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30)));
        var ran = new ConcurrentBag<TestKey>();
        var script = new HandlerScript
        {
            Run = async (run, cancellationToken) =>
            {
                ran.Add(run.Key);
                await EndAsync(run, cancellationToken);
            },
        };
        await WorkerAsync(source, script, s => s.Capacity(8));
        var client = await ClientAsync(source);
        var byKey = new List<TestKey>();
        for (var i = 1; i <= 20; i++)
        {
            var key = new TestKey("t", i);
            await InsertAsync(source, key, null);
            await using (var transaction = await BeginAsync())
            {
                await client.Operations.ScheduleAsync(transaction.Work, source, key, Due.Now, cancellationToken: Cancel);
                await transaction.CommitAsync(Cancel);
            }
            if (await client.Operations.ClaimAsync(source, key, Cancel) is { } claim)
            {
                byKey.Add(claim.Key);
                await using var end = await BeginAsync();
                await client.Operations.EndAsync(claim.Lease, end.Work, cancellationToken: Cancel);
                await end.CommitAsync(Cancel);
            }
        }

        await EventuallyAsync(() => byKey.Count + ran.Count >= 20, TimeSpan.FromSeconds(10), "every record is claimed");
        await Task.Delay(300, Cancel);
        Assert.Equal(20, byKey.Count + ran.Count);
        Assert.Empty(byKey.Intersect(ran));
        for (var i = 1; i <= 20; i++)
        {
            Assert.Equal(1, (await ReadAsync(source, new TestKey("t", i))).Epoch);
        }
    }

    [Fact]
    public async Task L10_ProcessHoldsNoMoreThanItsCapacity()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30)));
        var keys = await InsertManyAsync(source, 10);
        var running = 0;
        var maximum = 0;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var script = new HandlerScript
        {
            Run = async (run, cancellationToken) =>
            {
                var now = Interlocked.Increment(ref running);
                InterlockedMax(ref maximum, now);
                await release.Task.WaitAsync(cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                Interlocked.Decrement(ref running);
                await run.ReleaseAsync(Due.None, cancellationToken: CancellationToken.None);
            },
        };
        var worker = await WorkerAsync(source, script, s => s.Capacity(2));
        await EventuallyAsync(() => Volatile.Read(ref running) == 2, TimeSpan.FromSeconds(5), "the worker fills its capacity");
        await Task.Delay(500, Cancel);

        var held = 0;
        foreach (var key in keys)
        {
            held += (await ReadAsync(source, key)) is { Valid: true } state && state.Owner == worker.Owner ? 1 : 0;
        }
        Assert.Equal(2, held);
        Assert.Equal(2, maximum);
        var other = await ClientAsync(source);
        Assert.Equal(8, (await other.Operations.ClaimDueAsync(source, 20, Cancel)).Count);
        release.SetResult();
    }

    [Fact]
    public async Task L11_CommittedScheduleWakesDispatcherAndRolledBackOneWakesNobody()
    {
        if (!Provider.HasSignals)
        {
            Assert.Skip("The store has no commit-time signals.");
        }
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30)));
        var committed = new TestKey("t", 1);
        var rolledBack = new TestKey("t", 2);
        await InsertAsync(source, committed, null);
        await InsertAsync(source, rolledBack, null);
        var signals = new ConcurrentQueue<(SignalTopic Topic, string Key)>();
        using var listening = CancellationTokenSource.CreateLinkedTokenSource(Cancel);
        var connected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var listener = Provider.CreateStore().Signals!.ListenAsync((topic, key) => signals.Enqueue((topic, key)), () => connected.TrySetResult(), listening.Token);
        await connected.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);
        var ran = new ConcurrentQueue<TestKey>();
        var script = new HandlerScript
        {
            Run = async (run, cancellationToken) =>
            {
                ran.Enqueue(run.Key);
                await EndAsync(run, cancellationToken);
            },
        };
        var worker = await WorkerAsync(source, script, pollingInterval: TimeSpan.FromMinutes(5));
        await Task.Delay(300, Cancel);

        await using (var transaction = await BeginAsync())
        {
            await worker.Operations.ScheduleAsync(transaction.Work, source, rolledBack, Due.Now, cancellationToken: Cancel);
            await transaction.RollbackAsync(Cancel);
        }
        var stopwatch = Stopwatch.StartNew();
        await using (var transaction = await BeginAsync())
        {
            await worker.Operations.ScheduleAsync(transaction.Work, source, committed, Due.Now, cancellationToken: Cancel);
            await transaction.CommitAsync(Cancel);
        }

        await EventuallyAsync(() => !ran.IsEmpty, TimeSpan.FromSeconds(5), "the committed record runs without waiting for polling");
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(5));
        await Task.Delay(500, Cancel);
        Assert.Equal([committed], ran);
        Assert.Single(signals, s => s.Topic == WorkTopics.Due && s.Key == source.Name);
        await listening.CancelAsync();
        await listener;
    }

    [Fact]
    public async Task L19_ClaimInCallerTransactionClaimsOnlyStillClaimableCandidates()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30)));
        var eligible = new TestKey("t", 1);
        var ineligible = new TestKey("t", 2);
        var taken = new TestKey("t", 3);
        await InsertManyAsync(source, 3);
        var client = await ClientAsync(source);
        // Selected by a domain query; then one becomes ineligible in the domain and another is claimed elsewhere.
        var selected = new[] { eligible, ineligible, taken };
        await using (var change = await BeginAsync())
        {
            await change.WriteProgressAsync(source.Store, ineligible, "blocked", Cancel);
            await change.CommitAsync(Cancel);
        }
        Assert.NotNull(await client.Operations.ClaimAsync(source, taken, Cancel, OwnerId.New()));

        await using var transaction = await BeginAsync();
        var stillEligible = new List<TestKey>();
        foreach (var key in selected)
        {
            await transaction.LockAsync(source.Store, key, Cancel);
            if (await transaction.ReadProgressAsync(source.Store, key, Cancel) != "blocked")
            {
                stillEligible.Add(key);
            }
        }
        // A candidate listed twice is claimed once.
        var claims = await client.Operations.ClaimAsync(transaction.Work, source, [.. stillEligible, eligible], 10, Cancel);
        foreach (var claim in claims)
        {
            await Provider.JournalAsync(transaction.Work, source.Store, "reserve " + claim.Key, Cancel);
        }
        await transaction.CommitAsync(Cancel);

        Assert.Equal([eligible], claims.Select(c => c.Key));
        Assert.Equal(["reserve " + eligible], await JournalAsync(source));
        Assert.Equal(0, (await ReadAsync(source, ineligible)).Epoch);
        Assert.Equal(1, (await ReadAsync(source, taken)).Epoch);
    }

    [Fact]
    public async Task L20_ClaimHookCommitsWithTheClaimOrRollsItBack()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromMilliseconds(600)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var hook = new ClaimHookScript();
        var first = await ClientAsync(source);
        var client = await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(hook);
                work.AddLeaseSource(source, s => s.OnClaimed<ScriptedClaimHook>());
            },
            start: false);
        var crashed = await first.Operations.ClaimAsync(source, key, Cancel);
        Assert.NotNull(crashed);
        hook.Claimed = (claim, transaction, cancellationToken) =>
            new(Provider.JournalAsync(transaction, source.Store, $"{claim.Reason} {claim.PreviousOwner} {claim.PreviousEpoch}", cancellationToken));

        var takeover = await ClaimEventuallyAsync(client.Operations, source, key, TimeSpan.FromSeconds(5));
        Assert.Equal(ClaimReason.Takeover, takeover.Reason);
        Assert.Equal([$"Takeover {first.Owner} 1"], await JournalAsync(source));

        hook.Claimed = (_, transaction, cancellationToken) => new(ThrowAfterWriteAsync(transaction, source.Store, cancellationToken));
        await ExpiredAsync(source, key, TimeSpan.FromSeconds(5));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await client.Operations.ClaimAsync(source, key, Cancel));
        var state = await ReadAsync(source, key);
        Assert.Equal(2, state.Epoch);
        Assert.Equal(client.Owner, state.Owner);
        Assert.Single(await JournalAsync(source));

        hook.Claimed = (_, _, _) => ValueTask.CompletedTask;
        Assert.NotNull(await client.Operations.ClaimAsync(source, key, Cancel));
        Assert.Equal(3, (await ReadAsync(source, key)).Epoch);
    }

    [Fact]
    public async Task L20_FailingClaimHookInADueClaimCountsAFailureWithoutBlockingLaterRecords()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30), p => p with { FailureBackoff = Backoff.Constant(TimeSpan.FromSeconds(30)) }));
        var poisoned = new TestKey("t", 1);
        var healthy = new TestKey("t", 2);
        await InsertAsync(source, poisoned, TimeSpan.FromSeconds(-2));
        await InsertAsync(source, healthy, TimeSpan.FromSeconds(-1));
        var hook = new ClaimHookScript
        {
            Claimed = (claim, transaction, cancellationToken) =>
                claim.Key == poisoned ? new(ThrowAfterWriteAsync(transaction, source.Store, cancellationToken)) : ValueTask.CompletedTask,
        };
        var client = await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(hook);
                work.AddLeaseSource(source, s => s.OnClaimed<ScriptedClaimHook>());
            },
            start: false);

        var claims = await client.Operations.ClaimDueAsync(source, 1, Cancel);

        Assert.Equal([healthy], claims.Select(c => c.Key));
        var state = await ReadAsync(source, poisoned);
        Assert.Equal((DueState.Future, 1, 0L, false), (state.Due, state.Failures, state.Epoch, state.Held));
        Assert.Empty(await JournalAsync(source));
        Assert.Contains("lease.claim_rolled_back", client.Logs.Events);
    }

    [Fact]
    public async Task L20_AClaimHookThatFindsARowLockedLeavesTheRecordForALaterClaimWithoutAFailure()
    {
        RequireConcurrentTransactions();
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30), p => p with { FailureBackoff = Backoff.Constant(TimeSpan.FromSeconds(30)) }));
        var parent = new TestKey("t", 100);
        var busy = new TestKey("t", 1);
        var healthy = new TestKey("t", 2);
        await InsertAsync(source, parent, null);
        await InsertAsync(source, busy, TimeSpan.FromSeconds(-2));
        await InsertAsync(source, healthy, TimeSpan.FromSeconds(-1));
        var hook = new ClaimHookScript
        {
            Claimed = async (claim, transaction, cancellationToken) =>
            {
                if (claim.Key == busy)
                {
                    await Provider.LockWithoutWaitingAsync(transaction, source.Store, parent, cancellationToken);
                    await Provider.JournalAsync(transaction, source.Store, "claimed " + claim.Key, cancellationToken);
                }
            },
        };
        var client = await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(hook);
                work.AddLeaseSource(source, s => s.OnClaimed<ScriptedClaimHook>());
            },
            start: false);

        // An application transaction holds the parent row the hook takes without waiting.
        await using (var holder = await BeginAsync())
        {
            await holder.LockAsync(source.Store, parent, Cancel);

            var claims = await client.Operations.ClaimDueAsync(source, 1, Cancel);

            Assert.Equal([healthy], claims.Select(c => c.Key));
            var state = await ReadAsync(source, busy);
            Assert.Equal((DueState.Past, 0, 0L, false), (state.Due, state.Failures, state.Epoch, state.Held));
            await holder.RollbackAsync(Cancel);
        }

        Assert.Equal([busy], (await client.Operations.ClaimDueAsync(source, 1, Cancel)).Select(c => c.Key));
        Assert.Equal(["claimed " + busy], await JournalAsync(source));
    }

    [Fact]
    public async Task L30_ARecordDueAfterAClaimsCutoffMakesTheNextDueTimeZero()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.FromSeconds(2));
        var store = Provider.CreateStore().Leases;

        var before = await store.ClaimDueAsync(source, LeaseCallbacks<TestKey>.None, OwnerId.New(), 10, Cancel);
        Assert.Empty(before.Claims);
        var next = await store.NextDueAsync(source, before.Cutoff, Cancel);
        Assert.NotNull(next);
        Assert.InRange(next.Value, TimeSpan.FromTicks(1), TimeSpan.FromSeconds(2));

        // Once due, the record is one the claim could not consider: the next due time is zero, not unknown.
        await EventuallyAsync(async () => (await ReadAsync(source, key)).Due == DueState.Past, TimeSpan.FromSeconds(5), $"{key} becomes due");
        Assert.Equal(TimeSpan.Zero, await store.NextDueAsync(source, before.Cutoff, Cancel));
        var after = await store.ClaimDueAsync(source, LeaseCallbacks<TestKey>.None, OwnerId.New(), 10, Cancel);
        Assert.Equal([key], after.Claims.Select(c => c.Key));
        Assert.True(after.Cutoff > before.Cutoff, $"cutoff {after.Cutoff:O} after {before.Cutoff:O}");
    }

    [Fact]
    public async Task L30_ATakeoverBackoffEndingAfterAClaimsCutoffMakesTheNextDueTimeZero()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromMilliseconds(300), p => p with { TakeoverBackoff = Backoff.Constant(TimeSpan.FromSeconds(1)) }));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var store = Provider.CreateStore().Leases;
        var crashed = OwnerId.New();
        Assert.Single((await store.ClaimDueAsync(source, LeaseCallbacks<TestKey>.None, crashed, 1, Cancel)).Claims);
        await ExpiredAsync(source, key, TimeSpan.FromSeconds(5));

        var before = await store.ClaimDueAsync(source, LeaseCallbacks<TestKey>.None, OwnerId.New(), 10, Cancel);
        Assert.Empty(before.Claims);
        var next = await store.NextDueAsync(source, before.Cutoff, Cancel);
        Assert.NotNull(next);
        Assert.InRange(next.Value, TimeSpan.FromTicks(1), TimeSpan.FromSeconds(1));

        // Lookups after the backoff ended report zero for that cutoff until a claim takes the record.
        await EventuallyAsync(async () => await store.NextDueAsync(source, before.Cutoff, Cancel) == TimeSpan.Zero, TimeSpan.FromSeconds(5), "the takeover backoff ends");
        var claim = Assert.Single((await store.ClaimDueAsync(source, LeaseCallbacks<TestKey>.None, OwnerId.New(), 10, Cancel)).Claims);
        Assert.Equal((ClaimReason.Takeover, crashed), (claim.Reason, claim.PreviousOwner!.Value));
    }

    [Fact]
    public async Task L30_LockedAndLimitedRecordsTheClaimConsideredGiveNoImmediateNextDueTime()
    {
        RequireConcurrentTransactions();
        var locked = await SourceAsync(Policy(TimeSpan.FromSeconds(30)));
        var key = new TestKey("t", 1);
        await InsertAsync(locked, key, TimeSpan.FromSeconds(-1));
        var store = Provider.CreateStore().Leases;
        await using (var blocker = await BeginAsync())
        {
            await blocker.LockAsync(locked.Store, key, Cancel);
            for (var pass = 0; pass < 2; pass++)
            {
                var claims = await store.ClaimDueAsync(locked, LeaseCallbacks<TestKey>.None, OwnerId.New(), 10, Cancel);
                Assert.Empty(claims.Claims);
                Assert.Null(await store.NextDueAsync(locked, claims.Cutoff, Cancel));
            }
        }

        var limited = await SourceAsync(Policy(TimeSpan.FromSeconds(30), p => p with { PartitionLimit = 1 }), partitioned: true);
        await InsertManyAsync(limited, 2, "busy", TimeSpan.FromSeconds(-1));
        Assert.Single((await store.ClaimDueAsync(limited, LeaseCallbacks<TestKey>.None, OwnerId.New(), 10, Cancel)).Claims);
        var saturated = await store.ClaimDueAsync(limited, LeaseCallbacks<TestKey>.None, OwnerId.New(), 10, Cancel);
        Assert.Empty(saturated.Claims);
        // Only the held lease's expiry, a lower bound for when the partition frees a slot.
        var next = await store.NextDueAsync(limited, saturated.Cutoff, Cancel);
        Assert.NotNull(next);
        Assert.InRange(next.Value, TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(30));
    }

    [Fact]
    public async Task L20_DueClaimAttemptsAtMostTwiceItsLimit()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30), p => p with { FailureBackoff = Backoff.Constant(TimeSpan.FromMinutes(5)) }));
        var keys = await InsertManyAsync(source, 5, dueIn: TimeSpan.FromSeconds(-1));
        var attempts = 0;
        var hook = new ClaimHookScript
        {
            Claimed = (_, _, _) =>
            {
                Interlocked.Increment(ref attempts);
                throw new InvalidOperationException("claim hook failed");
            },
        };
        var client = await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(hook);
                work.AddLeaseSource(source, s => s.OnClaimed<ScriptedClaimHook>());
            },
            start: false);

        Assert.Empty(await client.Operations.ClaimDueAsync(source, 2, Cancel));

        Assert.Equal(4, attempts);
        var failures = 0;
        foreach (var key in keys)
        {
            failures += (await ReadAsync(source, key)).Failures;
        }
        Assert.Equal(4, failures);
    }

    [Fact]
    public async Task L20_ClaimHookOutlastingTheLeaseDoesNotStartTheHandler()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromMilliseconds(300)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var hook = new ClaimHookScript { Claimed = (_, _, cancellationToken) => new(Task.Delay(500, cancellationToken)) };
        var runs = 0;
        var script = new HandlerScript { Run = (_, _) => { Interlocked.Increment(ref runs); return ValueTask.CompletedTask; } };
        await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(hook).AddSingleton(script);
                work.AddLeaseHandler<TestKey, ScriptedHandler>(source, s => s.OnClaimed<ScriptedClaimHook>());
            },
            pollingInterval: TimeSpan.FromMilliseconds(100));

        await EventuallyAsync(async () => (await ReadAsync(source, key)).Epoch >= 1, TimeSpan.FromSeconds(5), "the record is claimed");
        await Task.Delay(1000, Cancel);

        Assert.Equal(0, Volatile.Read(ref runs));
    }

    private async Task ThrowAfterWriteAsync(WorkTransaction transaction, StoreLocation table, CancellationToken cancellationToken)
    {
        await Provider.JournalAsync(transaction, table, "written before failing", cancellationToken);
        throw new InvalidOperationException("The claim hook failed.");
    }

    [Fact]
    public async Task L24_SourcesSharingATableAreIndependent()
    {
        var table = await Provider.CreateTableAsync(["a_", "b_"], Cancel);
        var a = LeaseSource.Define("a-" + table.Name, table, TestKey.Codec, Policy(TimeSpan.FromSeconds(30)), fields: Fields("a_"));
        var b = LeaseSource.Define("b-" + table.Name, table, TestKey.Codec, Policy(TimeSpan.FromSeconds(30)), fields: Fields("b_"));
        var key = new TestKey("t", 1);
        await Provider.InsertAsync(table, key, TimeSpan.Zero, Cancel);
        var client = await ProcessAsync(work => work.AddLeaseSource(a).AddLeaseSource(b), start: false);

        var claimA = await client.Operations.ClaimAsync(a, key, Cancel);
        var claimB = await client.Operations.ClaimAsync(b, key, Cancel, OwnerId.New());
        Assert.NotNull(claimA);
        Assert.NotNull(claimB);
        Assert.True((await client.Operations.RenewAsync(claimA.Lease, Cancel)).Valid);
        await using (var transaction = await BeginAsync())
        {
            await client.Operations.EndAsync(claimA.Lease, transaction.Work, cancellationToken: Cancel);
            await transaction.CommitAsync(Cancel);
        }

        var stateA = await Provider.ReadAsync(table, key, "a_", Cancel);
        var stateB = await Provider.ReadAsync(table, key, "b_", Cancel);
        Assert.Equal((DueState.None, false, 1L), (stateA.Due, stateA.Held, stateA.Epoch));
        Assert.Equal((DueState.Past, true, 1L, claimB.Lease.Owner), (stateB.Due, stateB.Valid, stateB.Epoch, stateB.Owner!.Value));
        Assert.True((await client.Operations.RenewAsync(claimB.Lease, Cancel)).Valid);
        Assert.True(await client.Operations.ReleaseAsync(claimB.Lease, Due.Now, cancellationToken: Cancel));
        Assert.Equal(DueState.None, (await Provider.ReadAsync(table, key, "a_", Cancel)).Due);
    }

    [Fact]
    public async Task L25_ProcessClockSkewDoesNotChangeClaimsExpiryOrBackoff()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(1), p => p with { TakeoverBackoff = Backoff.Constant(TimeSpan.FromSeconds(1)) }));
        var crashed = new TestKey("t", 1);
        var future = new TestKey("t", 2);
        await InsertAsync(source, crashed, TimeSpan.Zero);
        await InsertAsync(source, future, TimeSpan.FromSeconds(1.5));
        var ahead = await ProcessAsync(work => work.AddLeaseSource(source), start: false, time: new SkewedTime(TimeSpan.FromMinutes(5)));
        Assert.NotNull(await ahead.Operations.ClaimAsync(source, crashed, Cancel));
        var stopwatch = Stopwatch.StartNew();
        var runs = new ConcurrentDictionary<TestKey, (TimeSpan At, ClaimReason Reason)>();
        var script = new HandlerScript
        {
            Run = async (run, cancellationToken) =>
            {
                runs[run.Key] = (stopwatch.Elapsed, run.Claim.Reason);
                await EndAsync(run, cancellationToken);
            },
        };
        await WorkerAsync(source, script, time: new SkewedTime(TimeSpan.FromMinutes(-5)), pollingInterval: TimeSpan.FromMilliseconds(200));

        await EventuallyAsync(() => runs.Count == 2, TimeSpan.FromSeconds(10), "both records run");
        Assert.InRange(runs[future].At, TimeSpan.FromSeconds(1.2), TimeSpan.FromSeconds(4));
        Assert.Equal(ClaimReason.Due, runs[future].Reason);
        Assert.InRange(runs[crashed].At, TimeSpan.FromSeconds(1.7), TimeSpan.FromSeconds(5));
        Assert.Equal(ClaimReason.Takeover, runs[crashed].Reason);
    }

    [Fact]
    public async Task L31_AKeyLongerThanItsColumnMatchesNoRecord()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30)));
        var record = new TestKey(new string('t', TestKey.TenantLength), 1);
        var longer = record with { Tenant = record.Tenant + "u" };
        await InsertAsync(source, record, TimeSpan.Zero);
        var client = await ClientAsync(source);

        await using (var transaction = await BeginAsync())
        {
            Assert.False(await client.Operations.ScheduleAsync(transaction.Work, source, longer, Due.None, cancellationToken: Cancel));
            Assert.False(await client.Operations.RequestCancelAsync(transaction.Work, source, longer, Cancel));
            Assert.False(await client.Operations.ClearCancelAsync(transaction.Work, source, longer, Cancel));
            Assert.Empty(await client.Operations.ClaimAsync(transaction.Work, source, [longer], 10, Cancel));
            await transaction.CommitAsync(Cancel);
        }
        Assert.Null(await client.Operations.ClaimAsync(source, longer, Cancel));
        var untouched = await ReadAsync(source, record);
        Assert.Equal((DueState.Past, 0L, false), (untouched.Due, untouched.Epoch, untouched.CancelRequested));

        await using (var transaction = await BeginAsync())
        {
            var claims = await client.Operations.ClaimAsync(transaction.Work, source, [longer, record], 10, Cancel);
            await transaction.CommitAsync(Cancel);
            Assert.Equal([record], claims.Select(c => c.Key));
        }
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int current;
        while (value > (current = Volatile.Read(ref target)) && Interlocked.CompareExchange(ref target, value, current) != current)
        {
        }
    }
}
