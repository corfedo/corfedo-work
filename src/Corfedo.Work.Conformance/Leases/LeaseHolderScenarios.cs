using System.Collections.Concurrent;
using System.Diagnostics;

namespace Corfedo.Work.Conformance;

/// <summary>Holding a lease: fencing, guards, renewals, cancellation, release, end, shutdown.</summary>
public abstract class LeaseHolderScenarios(ILeaseConformanceProvider provider) : LeaseScenarioBase(provider)
{
    [Fact]
    public async Task L4_PausedHolderCannotCommitAfterTakeover()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromMilliseconds(500)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var first = await ClientAsync(source);
        var second = await ClientAsync(source);
        var paused = (await first.Operations.ClaimAsync(source, key, Cancel))!;
        var current = await ClaimEventuallyAsync(second.Operations, source, key, TimeSpan.FromSeconds(5));

        await using (var late = await BeginAsync())
        {
            await late.WriteProgressAsync(source.Store, key, "late", Cancel);
            await Assert.ThrowsAsync<LeaseLostException>(async () => await first.Operations.GuardAsync(paused.Lease, late.Work, Cancel));
            await late.RollbackAsync(Cancel);
        }
        await using (var output = await BeginAsync())
        {
            await second.Operations.GuardAsync(current.Lease, output.Work, Cancel);
            await output.WriteProgressAsync(source.Store, key, "current", Cancel);
            await output.CommitAsync(Cancel);
        }

        var state = await ReadAsync(source, key);
        Assert.Equal("current", state.Progress);
        Assert.Equal(paused.Lease.Epoch + 1, current.Lease.Epoch);
        Assert.Equal(current.Lease.Epoch, state.Epoch);
    }

    [Fact]
    public async Task L5_ExpiredLeaseCannotBeRenewed()
    {
        RequireConcurrentTransactions();
        var source = await SourceAsync(Policy(TimeSpan.FromMilliseconds(400)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var client = await ClientAsync(source);
        var claim = (await client.Operations.ClaimAsync(source, key, Cancel))!;
        await ExpiredAsync(source, key, TimeSpan.FromSeconds(5));

        Assert.False((await client.Operations.RenewAsync(claim.Lease, Cancel)).Valid);
        Assert.False((await ReadAsync(source, key)).Valid);
    }

    [Fact]
    public async Task L6_GuardedTransactionOutlastsOriginalExpiry()
    {
        RequireConcurrentTransactions();
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(2)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var holder = await ClientAsync(source);
        var claimer = await ClientAsync(source);
        var stopwatch = Stopwatch.StartNew();
        var claim = (await holder.Operations.ClaimAsync(source, key, Cancel))!;
        await Task.Delay(1000, Cancel);

        await using var guarded = await BeginAsync();
        await holder.Operations.GuardAsync(claim.Lease, guarded.Work, Cancel);
        await guarded.WriteProgressAsync(source.Store, key, "guarded", Cancel);
        var renewal = Task.Run(async () => await holder.Operations.RenewAsync(claim.Lease, Cancel));
        await Task.Delay(TimeSpan.FromSeconds(2.4) - stopwatch.Elapsed, Cancel);
        var skip = Stopwatch.StartNew();
        Assert.Null(await claimer.Operations.ClaimAsync(source, key, Cancel));
        Assert.True(skip.Elapsed < TimeSpan.FromSeconds(1), "the claimer waited for the guarded record");
        Assert.False(renewal.IsCompleted, "the renewal did not wait for the guarded transaction");
        await guarded.CommitAsync(Cancel);

        Assert.True((await renewal).Valid);
        var state = await ReadAsync(source, key);
        Assert.Equal(("guarded", true, 1L), (state.Progress, state.Valid, state.Epoch));
    }

    [Fact]
    public async Task L7_RenewalWaitingOnALockEvaluatesExpiryAfterTheWait()
    {
        RequireConcurrentTransactions();
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(1)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var holder = await ClientAsync(source);
        var claimer = await ClientAsync(source);
        var claim = (await holder.Operations.ClaimAsync(source, key, Cancel))!;

        await using var domain = await BeginAsync();
        await domain.LockAsync(source.Store, key, Cancel);
        var renewal = Task.Run(async () => await holder.Operations.RenewAsync(claim.Lease, Cancel));
        await Task.Delay(1300, Cancel);
        var skip = Stopwatch.StartNew();
        Assert.Null(await claimer.Operations.ClaimAsync(source, key, Cancel));
        Assert.True(skip.Elapsed < TimeSpan.FromSeconds(1), "the claimer waited for the locked record");
        Assert.False(renewal.IsCompleted, "the renewal did not wait for the lock");
        await domain.CommitAsync(Cancel);

        Assert.False((await renewal).Valid);
    }

    [Fact]
    public async Task L12_RequestedCancellationReachesTheHolderWithinOneRenewalInterval()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(3)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var stopped = new TaskCompletionSource<LeaseStopReason?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var script = new HandlerScript
        {
            Run = async (run, cancellationToken) =>
            {
                started.SetResult();
                await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                stopped.SetResult(run.Hold.StopReason);
                await EndAsync(run, CancellationToken.None);
            },
        };
        await WorkerAsync(source, script);
        var requester = await ClientAsync(source);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5), Cancel);

        var stopwatch = Stopwatch.StartNew();
        await using (var transaction = await BeginAsync())
        {
            await requester.Operations.RequestCancelAsync(transaction.Work, source, key, Cancel);
            await transaction.CommitAsync(Cancel);
        }

        Assert.Equal(LeaseStopReason.CancelRequested, await stopped.Task.WaitAsync(TimeSpan.FromSeconds(5), Cancel));
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(1.5), $"cancellation took {stopwatch.Elapsed}");
        await EventuallyAsync(async () => (await ReadAsync(source, key)).Due == DueState.None, TimeSpan.FromSeconds(5), "the holder ends the record");
        Assert.False((await ReadAsync(source, key)).CancelRequested);
    }

    [Fact]
    public async Task L13_ClaimantSeesPendingCancellationUntilItIsCleared()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.FromMilliseconds(300));
        var client = await ClientAsync(source);
        await using (var transaction = await BeginAsync())
        {
            await client.Operations.RequestCancelAsync(transaction.Work, source, key, Cancel);
            await transaction.CommitAsync(Cancel);
        }

        var before = await ClaimEventuallyAsync(client.Operations, source, key, TimeSpan.FromSeconds(5));
        Assert.True(before.CancelRequested);
        Assert.True(await client.Operations.ReleaseAsync(before.Lease, Due.After(TimeSpan.FromMilliseconds(300)), failed: true, Cancel));
        await using (var transaction = await BeginAsync())
        {
            await client.Operations.ClearCancelAsync(transaction.Work, source, key, Cancel);
            await transaction.CommitAsync(Cancel);
        }

        var after = await ClaimEventuallyAsync(client.Operations, source, key, TimeSpan.FromSeconds(5));
        Assert.False(after.CancelRequested);
        Assert.Equal(before.ExecutionId, after.ExecutionId);
    }

    [Fact]
    public async Task L14_GracefulShutdownReleasesRecordsStillHeldAfterTheGracePeriod()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var script = new HandlerScript
        {
            Run = async (_, _) =>
            {
                started.SetResult();
                await Task.Delay(TimeSpan.FromSeconds(20), CancellationToken.None);
            },
        };
        var worker = await WorkerAsync(source, script, shutdownGrace: TimeSpan.FromMilliseconds(300));
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5), Cancel);

        var stopwatch = Stopwatch.StartNew();
        await worker.StopAsync(Cancel);
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(5));

        var state = await ReadAsync(source, key);
        Assert.Equal((false, 0, 0, DueState.Past), (state.Held, state.Takeovers, state.Failures, state.Due));
        var other = await ClientAsync(source);
        var claim = await other.Operations.ClaimAsync(source, key, Cancel);
        Assert.NotNull(claim);
        Assert.Equal(ClaimReason.ByKey, claim.Reason);
    }

    [Fact]
    public async Task L15_NewHolderReadsProgressCommittedWithTheLastRenewal()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromMilliseconds(600)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var crashed = await ClientAsync(source);
        var next = await ClientAsync(source);
        var claim = (await crashed.Operations.ClaimAsync(source, key, Cancel))!;
        foreach (var progress in new[] { "p1", "p2" })
        {
            await using var transaction = await BeginAsync();
            await transaction.WriteProgressAsync(source.Store, key, progress, Cancel);
            Assert.True((await crashed.Operations.RenewAsync(claim.Lease, transaction.Work, Cancel)).Valid);
            await transaction.CommitAsync(Cancel);
        }
        await using (var interrupted = await BeginAsync())
        {
            await interrupted.WriteProgressAsync(source.Store, key, "p3", Cancel);
            await crashed.Operations.RenewAsync(claim.Lease, interrupted.Work, Cancel);
        }

        await ClaimEventuallyAsync(next.Operations, source, key, TimeSpan.FromSeconds(5));
        Assert.Equal("p2", (await ReadAsync(source, key)).Progress);
    }

    [Fact]
    public async Task L16_FailedReleasesBackOffAndCountFailuresNotTakeovers()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var client = await ClientAsync(source);
        var claim = (await client.Operations.ClaimAsync(source, key, Cancel))!;
        Assert.True(await client.Operations.ReleaseAsync(claim.Lease, Due.After(TimeSpan.FromSeconds(1)), failed: true, Cancel));
        Assert.Null(await client.Operations.ClaimAsync(source, key, Cancel));
        Assert.Equal(DueState.Future, (await ReadAsync(source, key)).Due);

        claim = await ClaimEventuallyAsync(client.Operations, source, key, TimeSpan.FromSeconds(5));
        Assert.Equal(1, claim.Failures);
        Assert.True(await client.Operations.ReleaseAsync(claim.Lease, Due.After(TimeSpan.FromSeconds(1)), failed: true, Cancel));

        var state = await ReadAsync(source, key);
        Assert.Equal((2, 0, DueState.Future, false), (state.Failures, state.Takeovers, state.Due, state.Held));
        Assert.Null(await client.Operations.ClaimAsync(source, key, Cancel));
    }

    [Fact]
    public async Task L17_EndedRecordIsNotClaimedUntilScheduledAgain()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var client = await ClientAsync(source);
        var claim = (await client.Operations.ClaimAsync(source, key, Cancel))!;
        await using (var terminal = await BeginAsync())
        {
            await terminal.WriteProgressAsync(source.Store, key, "finished", Cancel);
            Assert.Equal(EndResult.Ended, await client.Operations.EndAsync(claim.Lease, terminal.Work, cancellationToken: Cancel));
            await terminal.CommitAsync(Cancel);
        }

        var state = await ReadAsync(source, key);
        Assert.Equal((DueState.None, false), (state.Due, state.Held));
        Assert.Null(await client.Operations.ClaimAsync(source, key, Cancel));
        Assert.Empty(await client.Operations.ClaimDueAsync(source, 10, Cancel));

        var execution = Guid.NewGuid();
        await using (var schedule = await BeginAsync())
        {
            await client.Operations.ScheduleAsync(schedule.Work, source, key, Due.Now, execution, Cancel);
            await schedule.CommitAsync(Cancel);
        }
        var again = await client.Operations.ClaimAsync(source, key, Cancel);
        Assert.NotNull(again);
        Assert.Equal(execution, again.ExecutionId);
        Assert.NotEqual(claim.ExecutionId, again.ExecutionId);
    }

    [Fact]
    public async Task L18_HandlerReturningWithoutEndingReleasesAsFailed()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30), p => p with { FailureBackoff = Backoff.Constant(TimeSpan.FromSeconds(30)) }));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var runs = 0;
        var script = new HandlerScript { Run = (_, _) => { Interlocked.Increment(ref runs); return ValueTask.CompletedTask; } };
        var worker = await WorkerAsync(source, script);

        await EventuallyAsync(async () => (await ReadAsync(source, key)) is { Held: false, Failures: 1 }, TimeSpan.FromSeconds(5), "the record is released as failed");
        var state = await ReadAsync(source, key);
        Assert.Equal((DueState.Future, 0), (state.Due, state.Takeovers));
        Assert.Contains("lease.unended", worker.Logs.Events);
        await Task.Delay(500, Cancel);
        Assert.Equal(1, Volatile.Read(ref runs));
    }

    [Fact]
    public async Task L18_EndWhoseTransactionDoesNotCommitIsReleasedAsFailed()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30), p => p with { FailureBackoff = Backoff.Constant(TimeSpan.FromSeconds(30)) }));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var script = new HandlerScript
        {
            Run = async (run, cancellationToken) =>
            {
                await using var transaction = await Provider.BeginAsync(cancellationToken);
                await run.EndAsync(transaction.Work, cancellationToken: cancellationToken);
                await transaction.RollbackAsync(cancellationToken);
            },
        };
        var worker = await WorkerAsync(source, script);

        await EventuallyAsync(async () => (await ReadAsync(source, key)) is { Held: false, Failures: 1 }, TimeSpan.FromSeconds(5), "the record is released as failed");
        Assert.Equal((DueState.Future, 0), ((await ReadAsync(source, key)).Due, (await ReadAsync(source, key)).Takeovers));
        Assert.Contains("lease.unended", worker.Logs.Events);
    }
}
