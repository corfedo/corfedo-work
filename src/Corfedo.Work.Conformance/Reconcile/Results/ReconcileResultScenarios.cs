using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>Results: waiting, failing with backoff, periodic re-evaluation and completion in the domain's transaction.</summary>
public abstract class ReconcileResultScenarios(IReconcileConformanceProvider provider) : ReconcileScenarioBase(provider)
{
    [Fact]
    public async Task C4_WaitingIsDueAtItsRetryTimeWithoutAFailureAndARequestMakesItDueSooner()
    {
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key);
        var client = await ClientAsync(source);
        var runs = new ConcurrentQueue<ObservedRun>();
        await ReconcilerAsync(source, Recording(runs, (run, _) => ValueTask.FromResult<ReconcileDisposition>(
            run.Revision == 1 ? ReconcileResult.Waiting(TimeSpan.FromMinutes(10)) : ReconcileResult.Converged)));
        await RequestAsync(client, source, key);
        await EventuallyAsync(async () => (await ReadAsync(source, key)).Lease is { Held: false, Epoch: 1 }, TimeSpan.FromSeconds(10), "the run reports waiting");

        var waiting = await ReadAsync(source, key);
        Assert.Equal((0, 0L, DueState.Future), (waiting.Lease.Failures, waiting.ConvergedRevision, waiting.Lease.Due));
        Assert.InRange(waiting.DueIn!.Value, TimeSpan.FromMinutes(9.9), TimeSpan.FromMinutes(10));
        Assert.Null(waiting.NotBeforeIn);
        var requested = Clock.Elapsed;
        await RequestAsync(client, source, key);
        await ConvergedAsync(source, key, 2, TimeSpan.FromSeconds(5));

        Assert.Equal([1L, 2L], runs.Select(r => r.Revision));
        Assert.True(runs.Last().At - requested < TimeSpan.FromSeconds(1), "the request did not make the waiting record due sooner");
    }

    [Fact]
    public async Task C5_FailuresBackOffExponentiallyUpToTheCapAndRequestsKeepTheFloor()
    {
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30), p => p with
        {
            FailureBackoff = Backoff.Exponential(TimeSpan.FromMilliseconds(250), TimeSpan.FromSeconds(1)),
        }));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key);
        var client = await ClientAsync(source);
        var runs = new ConcurrentQueue<ObservedRun>();
        var worker = await ReconcilerAsync(source, Recording(runs, (_, _) => ValueTask.FromResult<ReconcileDisposition>(ReconcileResult.Failed("unavailable"))), pollingInterval: TimeSpan.FromMilliseconds(50));
        await RequestAsync(client, source, key);
        await EventuallyAsync(() => runs.Count >= 6, TimeSpan.FromSeconds(15), "six runs fail");

        var starts = runs.Select(r => r.At).Take(6).ToArray();
        var gaps = starts.Zip(starts.Skip(1), (a, b) => b - a).ToArray();
        Assert.InRange(gaps[0], TimeSpan.FromMilliseconds(220), TimeSpan.FromMilliseconds(600));
        Assert.InRange(gaps[1], TimeSpan.FromMilliseconds(470), TimeSpan.FromMilliseconds(850));
        Assert.All(gaps[2..], gap => Assert.InRange(gap, TimeSpan.FromMilliseconds(970), TimeSpan.FromMilliseconds(1400)));
        Assert.Single(runs.Take(6).Select(r => r.ExecutionId).Distinct());
        Assert.Contains("reconcile.failed", worker.Logs.Events);

        await EventuallyAsync(
            async () => await ReadAsync(source, key) is { Lease.Held: false, NotBeforeIn: { } floor } && floor > TimeSpan.FromMilliseconds(500),
            TimeSpan.FromSeconds(5),
            "the record waits in backoff");
        await RequestAsync(client, source, key);
        var after = await ReadAsync(source, key);
        Assert.True(after.Lease.Failures >= 6, $"failures are {after.Lease.Failures}");
        Assert.Equal(DueState.Future, after.Lease.Due);
        Assert.True(after.DueIn >= after.NotBeforeIn, "an ordinary request moved the run before the backoff floor");

        await using (var transaction = await BeginAsync())
        {
            await Assert.ThrowsAsync<ArgumentException>(async () => await client.Operations.ScheduleAsync(transaction.Work, source.Leases, key, Due.Now, cancellationToken: Cancel));
            var store = client.Services.GetRequiredService<IWorkStore>().Leases;
            await Assert.ThrowsAsync<ArgumentException>(async () => await store.ScheduleAsync(transaction.Work, source.Leases, key, Due.Now, null, Cancel));
            await Assert.ThrowsAsync<ArgumentException>(async () => await store.ScheduleAsync(transaction.Work, source.Leases, key, Due.Now, Guid.NewGuid(), Cancel));
        }
    }

    [Fact]
    public async Task C9_ConvergedRecordIsEvaluatedAgainAfterTheResyncInterval()
    {
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30), resync: TimeSpan.FromMilliseconds(800)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key);
        var client = await ClientAsync(source);
        var runs = new ConcurrentQueue<ObservedRun>();
        await ReconcilerAsync(source, Recording(runs));
        await RequestAsync(client, source, key);

        await EventuallyAsync(() => runs.Count >= 2, TimeSpan.FromSeconds(10), "the converged record runs again");

        var (first, second) = (runs.First(), runs.Skip(1).First());
        Assert.InRange(second.At - first.At, TimeSpan.FromMilliseconds(780), TimeSpan.FromMilliseconds(1500));
        Assert.Equal((1L, 1L), (first.Revision, second.Revision));
        Assert.Equal(first.Generation, second.Generation);
    }

    [Fact]
    public async Task C13_CompletionRolledBackWithTheDomainTransactionChangesNothing()
    {
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key);
        var client = await ClientAsync(source);
        var afterRollback = new TaskCompletionSource<ReconcileState>(TaskCreationOptions.RunContinuationsAsynchronously);
        var runs = new ConcurrentQueue<ObservedRun>();
        await ReconcilerAsync(source, Recording(runs, async (run, cancellationToken) =>
        {
            await using (var rolledBack = await BeginAsync())
            {
                await rolledBack.WriteProgressAsync(source.Store, key, "converged-1", cancellationToken);
                await run.CompleteAsync(rolledBack.Work, ReconcileResult.Converged, cancellationToken);
                await rolledBack.RollbackAsync(cancellationToken);
            }
            afterRollback.SetResult(await Provider.ReadReconcileAsync(source.Store, key, cancellationToken));
            await using var committed = await BeginAsync();
            await committed.WriteProgressAsync(source.Store, key, "converged-2", cancellationToken);
            await run.CompleteAsync(committed.Work, ReconcileResult.Converged, cancellationToken);
            await committed.CommitAsync(cancellationToken);
            return run.Completed;
        }));
        await RequestAsync(client, source, key);

        var rolledBackState = await afterRollback.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);
        await ConvergedAsync(source, key, 1, TimeSpan.FromSeconds(5));
        await Task.Delay(300, Cancel);

        Assert.Equal((null, true, 0L, DueState.Past), (rolledBackState.Lease.Progress, rolledBackState.Lease.Valid, rolledBackState.ConvergedRevision, rolledBackState.Lease.Due));
        var state = await ReadAsync(source, key);
        Assert.Equal(("converged-2", DueState.None, 0), (state.Lease.Progress, state.Lease.Due, state.Lease.Failures));
        Assert.Single(runs);
    }
}
