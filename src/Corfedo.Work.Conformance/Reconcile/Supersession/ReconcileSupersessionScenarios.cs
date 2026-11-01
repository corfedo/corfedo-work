using System.Diagnostics;

namespace Corfedo.Work.Conformance;

/// <summary>Superseding sources: newer requests and invalidations make running attempts obsolete, fenced by the generation.</summary>
public abstract class ReconcileSupersessionScenarios(IReconcileConformanceProvider provider) : ReconcileScenarioBase(provider)
{
    /// <summary>What an obsolete run observed after its token was cancelled.</summary>
    private sealed record ObsoleteRun(LeaseStopReason? Reason, bool GuardFailed, bool CompleteFailed);

    /// <summary>
    /// A run of revision 1 that waits for its token and for <paramref name="proceed"/>, then tries to guard and
    /// complete, and returns a result for the dispatcher to apply. Waiting for <paramref name="proceed"/> keeps the
    /// failing writes, which lock the record, and the process's next claim away from the scenario's claimer.
    /// </summary>
    private ReconcileScript Obsolete(ReconcileSource<TestKey> source, TaskCompletionSource<ReconcileRun<TestKey>> running, TaskCompletionSource<ObsoleteRun> observed, Task proceed) => new()
    {
        Run = async (run, cancellationToken) =>
        {
            if (run.Revision != 1)
            {
                return ReconcileResult.Converged;
            }
            running.SetResult(run);
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            await proceed.WaitAsync(TimeSpan.FromSeconds(10), CancellationToken.None);
            var guardFailed = false;
            var completeFailed = false;
            await using (var transaction = await BeginAsync())
            {
                try
                {
                    await run.GuardAsync(transaction.Work, CancellationToken.None);
                }
                catch (LeaseLostException)
                {
                    guardFailed = true;
                }
            }
            await using (var transaction = await BeginAsync())
            {
                await transaction.WriteProgressAsync(source.Store, run.Key, "obsolete", CancellationToken.None);
                try
                {
                    await run.CompleteAsync(transaction.Work, ReconcileResult.Converged, CancellationToken.None);
                    await transaction.CommitAsync(CancellationToken.None);
                }
                catch (LeaseLostException)
                {
                    completeFailed = true;
                }
            }
            observed.SetResult(new(run.StopReason, guardFailed, completeFailed));
            return ReconcileResult.Converged;
        },
    };

    [Fact]
    public async Task C11_RequestDuringARunSupersedesItAndAnotherProcessClaimsAtOnce()
    {
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(3), p => p with { TakeoverBackoff = Backoff.Constant(TimeSpan.FromHours(1)) }, RequestPolicy.Supersede));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key);
        var claimer = await ClientAsync(source);
        var running = new TaskCompletionSource<ReconcileRun<TestKey>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var observed = new TaskCompletionSource<ObsoleteRun>(TaskCreationOptions.RunContinuationsAsynchronously);
        var claimed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await ReconcilerAsync(source, Obsolete(source, running, observed, claimed.Task));
        await RequestAsync(claimer, source, key);
        await running.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);
        var oldEpoch = (await ReadAsync(source, key)).Lease.Epoch;

        await RequestAsync(claimer, source, key);
        // Within a second, while the old lease has seconds left and a takeover would wait an hour; a renewal of the old
        // run can hold the record's lock for a moment.
        var claim = await ClaimEventuallyAsync(claimer.Operations, source.Leases, key, TimeSpan.FromSeconds(1));
        claimed.SetResult();
        var superseded = Stopwatch.StartNew();
        var obsolete = await observed.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);
        await Task.Delay(300, Cancel);

        Assert.Equal((ClaimReason.ByKey, 0, oldEpoch + 1), (claim.Reason, claim.Takeovers, claim.Lease.Epoch));
        Assert.Equal(new ReconcileAttempt(2, 0), claim.Reconcile);
        Assert.True(superseded.Elapsed < TimeSpan.FromSeconds(2), $"the obsolete run was cancelled after {superseded.Elapsed}");
        Assert.Equal(new ObsoleteRun(LeaseStopReason.Superseded, true, true), obsolete);
        var state = await ReadAsync(source, key);
        Assert.Equal((claimer.Owner, true, 0L, null), (state.Lease.Owner!.Value, state.Lease.Valid, state.ConvergedRevision, state.Lease.Progress));
    }

    [Fact]
    public async Task C15_InvalidationWaitsForAnOpenGuardAndFailsLaterGuards()
    {
        RequireConcurrentTransactions();
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30), requests: RequestPolicy.Supersede));
        var key = new TestKey("t", 1);
        var queued = new TestKey("t", 2);
        await InsertAsync(source, key, DueState.Past);
        await InsertAsync(source, queued, DueState.Past);
        var holder = await ClientAsync(source);
        var first = (await holder.Operations.ClaimAsync(source.Leases, key, Cancel))!;
        var queuedClaim = (await holder.Operations.ClaimAsync(source.Leases, queued, Cancel))!;

        Task<long> invalidation;
        Task queuedGuard;
        await using (var guarded = await BeginAsync())
        await using (var later = await BeginAsync())
        {
            await holder.Operations.GuardAsync(first.Lease, guarded.Work, Cancel);
            await guarded.WriteProgressAsync(source.Store, key, "guarded", Cancel);
            invalidation = InvalidateAsync(holder, source);
            await Task.Delay(300, Cancel);
            // A guard that starts while the invalidation waits queues behind it, so a stream of guards cannot keep
            // the invalidation waiting.
            queuedGuard = holder.Operations.GuardAsync(queuedClaim.Lease, later.Work, Cancel).AsTask();
            await Task.Delay(300, Cancel);
            Assert.False(invalidation.IsCompleted, "the invalidation did not wait for the open guarded transaction");
            Assert.False(queuedGuard.IsCompleted, "a guard that started after the invalidation did not wait for it");
            await guarded.CommitAsync(Cancel);
            Assert.Equal(1L, await invalidation.WaitAsync(TimeSpan.FromSeconds(5), Cancel));
            await Assert.ThrowsAsync<LeaseLostException>(() => queuedGuard.WaitAsync(TimeSpan.FromSeconds(5), Cancel));
        }
        Assert.Equal("guarded", (await ReadAsync(source, key)).Lease.Progress);

        var second = (await holder.Operations.ClaimAsync(source.Leases, key, Cancel))!;
        Assert.Equal((ClaimReason.ByKey, new ReconcileAttempt(0, 1)), (second.Reason, second.Reconcile));
        await using (var stale = await BeginAsync())
        {
            await Assert.ThrowsAsync<LeaseLostException>(async () => await holder.Operations.GuardAsync(first.Lease, stale.Work, Cancel));
        }
        Assert.Equal(2L, await InvalidateAsync(holder, source));
        await using (var late = await BeginAsync())
        {
            await late.WriteProgressAsync(source.Store, key, "late", Cancel);
            await Assert.ThrowsAsync<LeaseLostException>(async () => await holder.Operations.GuardAsync(second.Lease, late.Work, Cancel));
            await late.RollbackAsync(Cancel);
        }
        Assert.False((await holder.Operations.RenewAsync(second.Lease, Cancel)).Valid);
        Assert.Equal("guarded", (await ReadAsync(source, key)).Lease.Progress);
    }

    [Fact]
    public async Task C16_SupersedingRequestSkipsTakeoverBackoffAndFreesTheLimitAtOnce()
    {
        var source = await ReconcileSourceAsync(
            Reconcile(TimeSpan.FromMilliseconds(400), p => p with
            {
                TakeoverBackoff = Backoff.Exponential(TimeSpan.FromMilliseconds(100), TimeSpan.FromHours(1)),
                PartitionLimit = 1,
            }, RequestPolicy.Supersede),
            partitioned: true);
        var key = new TestKey("p", 1);
        var sibling = new TestKey("p", 2);
        await InsertAsync(source, key, DueState.Past);
        await InsertAsync(source, sibling, DueState.Past);
        var holder = await ClientAsync(source);
        var claimer = await ClientAsync(source);
        await holder.Operations.ClaimAsync(source.Leases, key, Cancel);
        await ClaimEventuallyAsync(holder.Operations, source.Leases, key, TimeSpan.FromSeconds(5));
        var held = await ClaimEventuallyAsync(holder.Operations, source.Leases, key, TimeSpan.FromSeconds(5));
        Assert.Equal((ClaimReason.Takeover, 2), (held.Reason, held.Takeovers));
        Assert.True((await holder.Operations.RenewAsync(held.Lease, Cancel)).Valid);
        Assert.Null(await claimer.Operations.ClaimAsync(source.Leases, sibling, Cancel));

        await RequestAsync(claimer, source, key);
        var siblingClaim = await claimer.Operations.ClaimAsync(source.Leases, sibling, Cancel);
        Assert.NotNull(siblingClaim);
        Assert.Null(await claimer.Operations.ClaimAsync(source.Leases, key, Cancel));
        await Assert.ThrowsAsync<ArgumentException>(async () => await claimer.Operations.ReleaseAsync(siblingClaim.Lease, Due.None, cancellationToken: Cancel));
        await using (var transaction = await BeginAsync())
        {
            await Assert.ThrowsAsync<ArgumentException>(async () => await claimer.Operations.EndAsync(siblingClaim.Lease, transaction.Work, cancellationToken: Cancel));
        }
        Assert.True(await Reconciliation(claimer).CompleteAsync(siblingClaim.Lease, ReconcileResult.Converged, Cancel));
        var claim = await claimer.Operations.ClaimAsync(source.Leases, key, Cancel);

        Assert.NotNull(claim);
        Assert.Equal((ClaimReason.ByKey, 2, held.Lease.Epoch + 1), (claim.Reason, claim.Takeovers, claim.Lease.Epoch));
        Assert.False((await holder.Operations.RenewAsync(held.Lease, Cancel)).Valid);
        Assert.False(await Reconciliation(holder).CompleteAsync(held.Lease, ReconcileResult.Converged, Cancel));
    }

    [Fact]
    public async Task C17_ClaimRetiresALeaseOfAnObsoleteGenerationWithinThePartitionLimit()
    {
        var source = await ReconcileSourceAsync(
            Reconcile(TimeSpan.FromSeconds(3), p => p with
            {
                TakeoverBackoff = Backoff.Constant(TimeSpan.FromHours(1)),
                PartitionLimit = 1,
            }, RequestPolicy.Supersede),
            partitioned: true);
        var key = new TestKey("p", 1);
        var sibling = new TestKey("p", 2);
        await InsertAsync(source, key);
        var claimer = await ClientAsync(source);
        var running = new TaskCompletionSource<ReconcileRun<TestKey>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var observed = new TaskCompletionSource<ObsoleteRun>(TaskCreationOptions.RunContinuationsAsynchronously);
        var claimed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await ReconcilerAsync(source, Obsolete(source, running, observed, claimed.Task));
        await RequestAsync(claimer, source, key);
        await running.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);
        var oldEpoch = (await ReadAsync(source, key)).Lease.Epoch;
        await InsertAsync(source, sibling, DueState.Past);

        await InvalidateAsync(claimer, source);
        IReadOnlyList<LeaseClaim<TestKey>> claims = [];
        await EventuallyAsync(async () => (claims = await claimer.Operations.ClaimDueAsync(source.Leases, 10, Cancel)).Count > 0, TimeSpan.FromSeconds(1), "the obsolete lease is retired at once");
        claimed.SetResult();
        var obsolete = await observed.Task.WaitAsync(TimeSpan.FromSeconds(5), Cancel);
        await Task.Delay(300, Cancel);

        var claim = Assert.Single(claims);
        Assert.Equal((key, ClaimReason.Due, 0, oldEpoch + 1), (claim.Key, claim.Reason, claim.Takeovers, claim.Lease.Epoch));
        Assert.Equal(new ReconcileAttempt(1, 1), claim.Reconcile);
        Assert.Equal(new ObsoleteRun(LeaseStopReason.Superseded, true, true), obsolete);
        Assert.Empty(await claimer.Operations.ClaimDueAsync(source.Leases, 10, Cancel));
        var state = await ReadAsync(source, key);
        Assert.Equal((claimer.Owner, true, 0L, 0L, null), (state.Lease.Owner!.Value, state.Lease.Valid, state.ConvergedRevision, state.ConvergedGeneration, state.Lease.Progress));
        Assert.Equal(0L, (await ReadAsync(source, sibling)).Lease.Epoch);
    }

    [Fact]
    public async Task C21_OwnerAtItsLimitReplacesItsObsoleteLease()
    {
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromMinutes(1), p => p with { OwnerLimit = 1 }, RequestPolicy.Supersede));
        var key = new TestKey("t", 1);
        var other = new TestKey("t", 2);
        await InsertAsync(source, key, DueState.Past);
        await InsertAsync(source, other, DueState.None);
        var holder = await ClientAsync(source);
        var old = (await holder.Operations.ClaimAsync(source.Leases, key, Cancel))!;
        await InvalidateAsync(holder, source);

        var claim = Assert.Single(await holder.Operations.ClaimDueAsync(source.Leases, 1, Cancel));

        Assert.Equal((key, ClaimReason.Due, old.Lease.Epoch + 1, new ReconcileAttempt(0, 1)), (claim.Key, claim.Reason, claim.Lease.Epoch, claim.Reconcile));
        Assert.Empty(await holder.Operations.ClaimDueAsync(source.Leases, 1, Cancel));
        Assert.Equal(0L, (await ReadAsync(source, other)).Lease.Epoch);
    }

    [Fact]
    public async Task C22_GuardQueuedBehindAnInvalidationHoldsNoRecordLock()
    {
        RequireConcurrentTransactions();
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30), requests: RequestPolicy.Supersede));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, DueState.Past);
        var holder = await ClientAsync(source);
        var claim = (await holder.Operations.ClaimAsync(source.Leases, key, Cancel))!;

        // A transaction that takes the fence first and then writes shared domain rows, here the guarded record.
        await using var fenced = await BeginAsync();
        await Reconciliation(holder).FenceAsync(fenced.Work, source, Cancel);
        var invalidation = InvalidateAsync(holder, source);
        await Task.Delay(200, Cancel);
        Assert.False(invalidation.IsCompleted, "the invalidation waits for the fenced transaction");
        await using var guarded = await BeginAsync();
        var guard = holder.Operations.GuardAsync(claim.Lease, guarded.Work, Cancel).AsTask();
        await Task.Delay(200, Cancel);
        Assert.False(guard.IsCompleted, "the guard queues behind the invalidation");

        await fenced.WriteProgressAsync(source.Store, key, "shared", Cancel).WaitAsync(TimeSpan.FromMilliseconds(500), Cancel);
        await fenced.CommitAsync(Cancel);

        Assert.Equal(1L, await invalidation.WaitAsync(TimeSpan.FromSeconds(5), Cancel));
        await Assert.ThrowsAsync<LeaseLostException>(() => guard.WaitAsync(TimeSpan.FromSeconds(5), Cancel));
        await guarded.RollbackAsync(Cancel);
        Assert.Equal("shared", (await ReadAsync(source, key)).Lease.Progress);
    }
}
