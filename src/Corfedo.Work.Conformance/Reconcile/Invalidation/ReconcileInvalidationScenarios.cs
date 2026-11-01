using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>Mass invalidation through the source's generation, without writing records.</summary>
public abstract class ReconcileInvalidationScenarios(IReconcileConformanceProvider provider) : ReconcileScenarioBase(provider)
{
    [Fact]
    public async Task C6_InvalidationWritesNoRecordsRunsEveryEligibleRecordOnceAndKeepsServingRequests()
    {
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30)));
        const int count = 200;
        var keys = Enumerable.Range(1, count).Select(i => new TestKey("t", i)).ToArray();
        foreach (var key in keys)
        {
            await InsertAsync(source, key, DueState.None, revision: 3);
        }
        var hot = new TestKey("hot", 1);
        await InsertAsync(source, hot);
        var client = await ClientAsync(source);
        var versions = await Provider.RecordVersionsAsync(source.Store, Cancel);

        Assert.Equal(1L, await InvalidateAsync(client, source));
        Assert.Equal(versions, await Provider.RecordVersionsAsync(source.Store, Cancel));
        var runs = new ConcurrentQueue<ObservedRun>();
        await ReconcilerAsync(source, Recording(runs, async (_, cancellationToken) =>
        {
            await Task.Delay(10, cancellationToken);
            return ReconcileResult.Converged;
        }), s => s.Capacity(2));
        await EventuallyAsync(() => runs.Count >= 20, TimeSpan.FromSeconds(10), "the invalidated records start running");
        var requested = Clock.Elapsed;
        await RequestAsync(client, source, hot);
        await EventuallyAsync(() => runs.Count(r => r.Generation == 1 && r.Key.Tenant == "t") == count, TimeSpan.FromSeconds(30), "every invalidated record runs");
        await ConvergedAsync(source, hot, 1, TimeSpan.FromSeconds(5), generation: 1);
        await Task.Delay(500, Cancel);

        var stale = runs.Where(r => r.Key.Tenant == "t").ToArray();
        Assert.All(stale.GroupBy(r => r.Key), g => Assert.Single(g));
        Assert.All(stale, r => Assert.Equal(3L, r.Revision));
        var hotRun = Assert.Single(runs, r => r.Key == hot);
        Assert.True(hotRun.At - requested < TimeSpan.FromSeconds(1), $"the requested record waited {hotRun.At - requested}");
        Assert.True(hotRun.At < stale.Max(r => r.At), "the requested record waited for the whole invalidation backlog");
        var state = await ReadAsync(source, keys[0]);
        Assert.Equal((1L, 1L, 3L, DueState.None), (state.AttemptedGeneration, state.ConvergedGeneration, state.ConvergedRevision, state.Lease.Due));
    }

    [Fact]
    public async Task C7_WaitingAndFailedRunsAfterAnInvalidationWaitForTheirRetryTime()
    {
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30), p => p with { FailureBackoff = Backoff.Constant(TimeSpan.FromMinutes(5)) }));
        var waiting = new TestKey("t", 1);
        var failing = new TestKey("t", 2);
        await InsertAsync(source, waiting, DueState.None, revision: 1);
        await InsertAsync(source, failing, DueState.None, revision: 1);
        var client = await ClientAsync(source);
        var runs = new ConcurrentQueue<ObservedRun>();
        await ReconcilerAsync(source, Recording(runs, (run, _) => ValueTask.FromResult<ReconcileDisposition>(
            run.Key == waiting ? ReconcileResult.Waiting(TimeSpan.FromMinutes(5)) : ReconcileResult.Failed("unavailable"))), s => s.Capacity(2));

        await InvalidateAsync(client, source);
        await EventuallyAsync(async () => !(await ReadAsync(source, waiting)).Lease.Held && !(await ReadAsync(source, failing)).Lease.Held && runs.Count == 2, TimeSpan.FromSeconds(10), "both records run");
        await Task.Delay(1000, Cancel);

        Assert.Equal(2, runs.Count);
        foreach (var key in new[] { waiting, failing })
        {
            var state = await ReadAsync(source, key);
            Assert.Equal((1L, 0L, DueState.Future), (state.AttemptedGeneration, state.ConvergedGeneration, state.Lease.Due));
            Assert.True(state.DueIn > TimeSpan.FromMinutes(4), $"{key} is due in {state.DueIn}");
        }
        Assert.Equal((0, 1), ((await ReadAsync(source, waiting)).Lease.Failures, (await ReadAsync(source, failing)).Lease.Failures));
    }

    [Fact]
    public async Task C8_InvalidationRunsRecordsWithoutADueTime()
    {
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30)));
        var first = new TestKey("t", 1);
        var second = new TestKey("t", 2);
        await InsertAsync(source, first, DueState.None, revision: 1);
        await InsertAsync(source, second, DueState.None, revision: 1);
        var client = await ClientAsync(source);
        var runs = new ConcurrentQueue<ObservedRun>();
        await ReconcilerAsync(source, Recording(runs), s => s.Capacity(2));

        await InvalidateAsync(client, source);
        await ConvergedAsync(source, first, 1, TimeSpan.FromSeconds(10), generation: 1);
        await ConvergedAsync(source, second, 1, TimeSpan.FromSeconds(10), generation: 1);
        await Task.Delay(1000, Cancel);

        Assert.Equal(2, runs.Count);
        Assert.Equal([first, second], runs.Select(r => r.Key).Distinct().OrderBy(k => k.Id));
        Assert.Equal(DueState.None, (await ReadAsync(source, first)).Lease.Due);
        Assert.Equal(DueState.None, (await ReadAsync(source, second)).Lease.Due);
    }

    [Fact]
    public async Task C14_InvalidationClaimOfAnIdleRecordSurvivesItsHoldersCrash()
    {
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromMilliseconds(600)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, DueState.None, revision: 2);
        var crashing = await ClientAsync(source);
        await InvalidateAsync(crashing, source);
        var claim = Assert.Single(await crashing.Operations.ClaimDueAsync(source.Leases, 10, Cancel));
        Assert.Equal(new ReconcileAttempt(2, 1), claim.Reconcile);
        var claimed = await ReadAsync(source, key);
        Assert.Equal((DueState.Past, 1L), (claimed.Lease.Due, claimed.AttemptedGeneration));

        var runs = new ConcurrentQueue<ObservedRun>();
        await ReconcilerAsync(source, Recording(runs));
        await ConvergedAsync(source, key, 2, TimeSpan.FromSeconds(10), generation: 1);

        var run = Assert.Single(runs);
        Assert.Equal((ClaimReason.Takeover, 1L, 2L), (run.Reason, run.Generation, run.Revision));
    }

    [Fact]
    public async Task C18_RecordDueByTimeAndByAnInvalidationIsClaimedOnce()
    {
        var plainSource = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30)));
        var hookedSource = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30)));
        var key = new TestKey("t", 1);
        var other = new TestKey("t", 2);
        await InsertAsync(plainSource, key, DueState.Past);
        await InsertAsync(hookedSource, key, DueState.Past);
        await InsertAsync(hookedSource, other, DueState.Past);
        var calls = new ConcurrentQueue<TestKey>();
        var hook = new ClaimHookScript { Claimed = (claim, _, _) => { calls.Enqueue(claim.Key); return ValueTask.CompletedTask; } };
        var plain = await ClientAsync(plainSource);
        var hooked = await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(hook);
                work.AddReconcileSource(hookedSource, s => s.OnClaimed<ScriptedClaimHook>());
            },
            start: false);
        await InvalidateAsync(plain, plainSource);
        await InvalidateAsync(plain, hookedSource);

        var claim = Assert.Single(await plain.Operations.ClaimDueAsync(plainSource.Leases, 10, Cancel));
        var hookedClaims = await hooked.Operations.ClaimDueAsync(hookedSource.Leases, 10, Cancel);

        Assert.Equal((key, 1L, ClaimReason.Due), (claim.Key, claim.Lease.Epoch, claim.Reason));
        Assert.Equal([key, other], hookedClaims.Select(c => c.Key).OrderBy(k => k.Id));
        Assert.All(hookedClaims, c => Assert.Equal((1L, 0, ClaimReason.Due), (c.Lease.Epoch, c.Takeovers, c.Reason)));
        Assert.Equal([key, other], calls.OrderBy(k => k.Id));
    }

    [Fact]
    public async Task C19_FailedClaimHookOfAnInvalidatedRecordWaitsForTheFailureBackoff()
    {
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30), p => p with { FailureBackoff = Backoff.Constant(TimeSpan.FromMinutes(5)) }));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, DueState.None, revision: 1);
        var calls = 0;
        var hook = new ClaimHookScript
        {
            Claimed = (_, _, _) =>
            {
                Interlocked.Increment(ref calls);
                throw new InvalidOperationException("claim hook failed");
            },
        };
        var client = await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(hook);
                work.AddReconcileSource(source, s => s.OnClaimed<ScriptedClaimHook>());
            },
            start: false);
        await InvalidateAsync(client, source);

        Assert.Empty(await client.Operations.ClaimDueAsync(source.Leases, 1, Cancel));
        Assert.Empty(await client.Operations.ClaimDueAsync(source.Leases, 1, Cancel));

        Assert.Equal(1, calls);
        var state = await ReadAsync(source, key);
        Assert.Equal((1, 0L, 0L, false), (state.Lease.Failures, state.AttemptedGeneration, state.Lease.Epoch, state.Lease.Held));
        Assert.True(state.NotBeforeIn > TimeSpan.FromMinutes(4), $"the backoff floor is {state.NotBeforeIn}");
        Assert.True(state.DueIn >= state.NotBeforeIn, "the due time is before the backoff floor");
    }

    [Fact]
    public async Task C20_OneSlotServesAnInvalidationWhileAnotherRecordIsAlwaysDue()
    {
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30)));
        var hot = new TestKey("t", 1);
        var idle = new TestKey("t", 2);
        await InsertAsync(source, hot, DueState.Past, revision: 1);
        await InsertAsync(source, idle, DueState.None, revision: 1);
        var client = await ClientAsync(source);
        await InvalidateAsync(client, source);
        var runs = new ConcurrentQueue<ObservedRun>();
        await ReconcilerAsync(source, Recording(runs, (run, _) => ValueTask.FromResult<ReconcileDisposition>(
            run.Key == hot ? ReconcileResult.Waiting(TimeSpan.Zero) : ReconcileResult.Converged)));

        await ConvergedAsync(source, idle, 1, TimeSpan.FromSeconds(5), generation: 1);

        Assert.True(runs.TakeWhile(r => r.Key != idle).Count() <= 2, $"the invalidated record ran after {runs.TakeWhile(r => r.Key != idle).Count()} runs of the due one");
    }
}
