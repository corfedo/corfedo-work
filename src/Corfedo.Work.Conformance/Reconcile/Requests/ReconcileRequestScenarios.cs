using System.Collections.Concurrent;

namespace Corfedo.Work.Conformance;

/// <summary>Requests: merging, follow-ups during runs, debounce with a ceiling, and resetting backoff.</summary>
public abstract class ReconcileRequestScenarios(IReconcileConformanceProvider provider) : ReconcileScenarioBase(provider)
{
    [Fact]
    public async Task C1_RequestsBeforeARunMergeIntoOneRunOfTheLatestRevision()
    {
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key);
        var client = await ClientAsync(source);
        for (var i = 0; i < 10; i++)
        {
            await RequestAsync(client, source, key);
        }
        var requested = await ReadAsync(source, key);
        Assert.Equal((10L, true, DueState.Past), (requested.RequestedRevision, requested.FirstRequested, requested.Lease.Due));

        var runs = new ConcurrentQueue<ObservedRun>();
        await ReconcilerAsync(source, Recording(runs));
        await ConvergedAsync(source, key, 10, TimeSpan.FromSeconds(10));
        await Task.Delay(500, Cancel);

        Assert.Equal([10L], runs.Select(r => r.Revision));
        var state = await ReadAsync(source, key);
        Assert.Equal((10L, 10L, DueState.None, false), (state.ClaimedRevision!.Value, state.ConvergedRevision, state.Lease.Due, state.FirstRequested));
    }

    [Fact]
    public async Task C2_RequestsDuringARunProduceExactlyOneFollowUp()
    {
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key);
        var client = await ClientAsync(source);
        var running = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var proceed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var runs = new ConcurrentQueue<ObservedRun>();
        await ReconcilerAsync(source, Recording(runs, async (run, cancellationToken) =>
        {
            if (run.Revision == 1)
            {
                running.SetResult();
                await proceed.Task.WaitAsync(cancellationToken);
            }
            return ReconcileResult.Converged;
        }));
        await RequestAsync(client, source, key);
        await running.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);

        await RequestAsync(client, source, key);
        await RequestAsync(client, source, key);
        var during = await ReadAsync(source, key);
        proceed.SetResult();
        await ConvergedAsync(source, key, 3, TimeSpan.FromSeconds(10));
        await Task.Delay(500, Cancel);

        Assert.Equal((true, 3L, 1L), (during.Lease.Valid, during.RequestedRevision, during.ClaimedRevision!.Value));
        Assert.Equal([1L, 3L], runs.Select(r => r.Revision));
        Assert.NotEqual(runs.First().ExecutionId, runs.Last().ExecutionId);
        Assert.Equal(DueState.None, (await ReadAsync(source, key)).Lease.Due);
    }

    [Fact]
    public async Task C3_TrailingDebounceIsCappedByTheMaximumDelay()
    {
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key);
        var client = await ClientAsync(source);
        var runs = new ConcurrentQueue<ObservedRun>();
        await ReconcilerAsync(source, Recording(runs), pollingInterval: TimeSpan.FromSeconds(5));
        var request = new ReconcileRequest { Debounce = Debounce.Trailing(TimeSpan.FromMilliseconds(600), maximum: TimeSpan.FromSeconds(2)) };

        var first = Clock.Elapsed;
        while (runs.IsEmpty && Clock.Elapsed - first < TimeSpan.FromSeconds(5))
        {
            await RequestAsync(client, source, key, request);
            await Task.Delay(200, Cancel);
        }

        var ran = Assert.Single(runs).At - first;
        Assert.InRange(ran, TimeSpan.FromSeconds(1.9), TimeSpan.FromSeconds(2.6));
    }

    [Fact]
    public async Task C12_ResetBackoffMakesAFailedRecordDueNowAndAPlainRequestDoesNot()
    {
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30), p => p with { FailureBackoff = Backoff.Constant(TimeSpan.FromMinutes(5)) }));
        var reset = new TestKey("t", 1);
        var plain = new TestKey("t", 2);
        await InsertAsync(source, reset);
        await InsertAsync(source, plain);
        var client = await ClientAsync(source);
        var runs = new ConcurrentQueue<ObservedRun>();
        await ReconcilerAsync(source, Recording(runs, (run, _) => ValueTask.FromResult<ReconcileDisposition>(
            run.Revision == 1 ? ReconcileResult.Failed("unavailable") : ReconcileResult.Converged)), s => s.Capacity(2));
        await RequestAsync(client, source, reset);
        await RequestAsync(client, source, plain);
        await EventuallyAsync(async () => (await ReadAsync(source, reset)).Lease.Failures == 1 && (await ReadAsync(source, plain)).Lease.Failures == 1, TimeSpan.FromSeconds(10), "both runs fail");

        await RequestAsync(client, source, reset, new ReconcileRequest { ResetBackoff = true });
        await RequestAsync(client, source, plain);
        await ConvergedAsync(source, reset, 2, TimeSpan.FromSeconds(5));
        await Task.Delay(500, Cancel);

        var plainState = await ReadAsync(source, plain);
        Assert.Equal((0, 2L), ((await ReadAsync(source, reset)).Lease.Failures, (await ReadAsync(source, reset)).ConvergedRevision));
        Assert.Equal((1, 2L, 0L, DueState.Future), (plainState.Lease.Failures, plainState.RequestedRevision, plainState.ConvergedRevision, plainState.Lease.Due));
        Assert.True(plainState.NotBeforeIn > TimeSpan.FromMinutes(4), $"the backoff floor moved to {plainState.NotBeforeIn}");
        Assert.True(plainState.DueIn >= plainState.NotBeforeIn, "a plain request moved the record before its backoff floor");
        Assert.Equal(1, runs.Count(r => r.Key == plain));
    }

    [Fact]
    public async Task C23_ARequestForAKeyLongerThanItsColumnMatchesNoRecord()
    {
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30)));
        var record = new TestKey(new string('t', TestKey.TenantLength), 1);
        await InsertAsync(source, record);
        var client = await ClientAsync(source);

        await using (var transaction = await BeginAsync())
        {
            Assert.False(await Reconciliation(client).RequestAsync(transaction.Work, source, record with { Tenant = record.Tenant + "u" }, ReconcileRequest.Default, Cancel));
            await transaction.CommitAsync(Cancel);
        }

        var state = await ReadAsync(source, record);
        Assert.Equal((0L, DueState.None), (state.RequestedRevision, state.Lease.Due));
    }
}
