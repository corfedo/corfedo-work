using System.Collections.Concurrent;

namespace Corfedo.Work.Conformance;

/// <summary>Requests: merging, follow-ups during runs, debounce with a ceiling, resetting backoff, and bulk requests.</summary>
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

    [Fact]
    public async Task C24_ABulkRequestRequestsEachExistingRecordOnceAndSignalsOnce()
    {
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30)));
        var idle = new TestKey("t", 1);
        var waiting = new TestKey("t", 2);
        var held = new TestKey("t", 3);
        await InsertAsync(source, idle);
        await InsertAsync(source, waiting, DueState.Past);
        await InsertAsync(source, held, DueState.Past);
        var client = await ClientAsync(source);
        var holder = await ClientAsync(source);
        var claim = await holder.Operations.ClaimAsync(source.Leases, held, Cancel);
        Assert.NotNull(claim);
        await using var signals = await SignalsAsync();

        var request = new ReconcileRequest { Debounce = Debounce.Trailing(TimeSpan.FromMinutes(1)) };
        IReadOnlyList<TestKey> requested;
        await using (var transaction = await BeginAsync())
        {
            Assert.Empty(await Reconciliation(client).RequestManyAsync(transaction.Work, source, [], Cancel));
            requested = await Reconciliation(client).RequestManyAsync(
                transaction.Work,
                source,
                [held, new TestKey("t", 9), waiting, idle, idle, idle with { Tenant = new string('t', TestKey.TenantLength + 1) }],
                request,
                Cancel);
            await transaction.CommitAsync(Cancel);
        }
        if (signals.Enabled)
        {
            await EventuallyAsync(() => signals.Received.Any(s => s.Topic == WorkTopics.Due), TimeSpan.FromSeconds(5), "claimers are signalled");
        }
        // Requesting only a held record records the request for the run's result and wakes no claimer.
        await using (var transaction = await BeginAsync())
        {
            Assert.Equal([held], await Reconciliation(client).RequestManyAsync(transaction.Work, source, [held], request, Cancel));
            await transaction.CommitAsync(Cancel);
        }

        Assert.Equal([idle, waiting, held], requested);
        var idleState = await ReadAsync(source, idle);
        Assert.Equal((1L, true, DueState.Future, false), (idleState.RequestedRevision, idleState.FirstRequested, idleState.Lease.Due, idleState.Lease.Held));
        var waitingState = await ReadAsync(source, waiting);
        Assert.Equal((1L, true, DueState.Past, false), (waitingState.RequestedRevision, waitingState.FirstRequested, waitingState.Lease.Due, waitingState.Lease.Held));
        var during = await ReadAsync(source, held);
        Assert.Equal((2L, true, claim.Lease.Epoch, DueState.Future), (during.RequestedRevision, during.Lease.Valid, during.Lease.Epoch, during.Lease.Due));
        Assert.True((await holder.Operations.RenewAsync(claim.Lease, Cancel)).Valid);
        if (signals.Enabled)
        {
            await Task.Delay(300, Cancel);
            Assert.Single(signals.Received, s => s.Topic == WorkTopics.Due && s.Key == source.Name);
        }
    }

    [Fact]
    public async Task C25_ABulkRequestSupersedesEveryHeldRunAndSignalsEachHolderOnce()
    {
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30), p => p with { TakeoverBackoff = Backoff.Constant(TimeSpan.FromHours(1)) }, RequestPolicy.Supersede));
        TestKey[] keys = [new("t", 1), new("t", 2), new("t", 3), new("t", 4)];
        foreach (var key in keys)
        {
            await InsertAsync(source, key, DueState.Past);
        }
        var first = await ClientAsync(source);
        var second = await ClientAsync(source);
        var claimer = await ClientAsync(source);
        LeaseClaim<TestKey>[] held =
        [
            Assert.IsType<LeaseClaim<TestKey>>(await first.Operations.ClaimAsync(source.Leases, keys[0], Cancel)),
            Assert.IsType<LeaseClaim<TestKey>>(await first.Operations.ClaimAsync(source.Leases, keys[1], Cancel)),
            Assert.IsType<LeaseClaim<TestKey>>(await second.Operations.ClaimAsync(source.Leases, keys[2], Cancel)),
        ];
        await using var signals = await SignalsAsync();

        await using (var transaction = await BeginAsync())
        {
            Assert.Equal(keys, await Reconciliation(claimer).RequestManyAsync(transaction.Work, source, keys.Reverse(), Cancel));
            await transaction.CommitAsync(Cancel);
        }

        foreach (var claim in held)
        {
            var next = await claimer.Operations.ClaimAsync(source.Leases, claim.Lease.Key, Cancel);
            Assert.NotNull(next);
            Assert.Equal((ClaimReason.ByKey, 0, claim.Lease.Epoch + 1), (next.Reason, next.Takeovers, next.Lease.Epoch));
        }
        Assert.False((await first.Operations.RenewAsync(held[0].Lease, Cancel)).Valid);
        Assert.False((await second.Operations.RenewAsync(held[2].Lease, Cancel)).Valid);
        if (signals.Enabled)
        {
            string[] holders = [WorkTopics.CancelKey(first.Owner, source.Name), WorkTopics.CancelKey(second.Owner, source.Name)];
            await EventuallyAsync(() => signals.Received.Count(s => s.Topic == WorkTopics.Cancel) >= 2, TimeSpan.FromSeconds(5), "both holders are signalled");
            await Task.Delay(300, Cancel);
            Assert.Equal(holders.Order(StringComparer.Ordinal), signals.Received.Where(s => s.Topic == WorkTopics.Cancel).Select(s => s.Key).Order(StringComparer.Ordinal));
        }
    }

    [Fact]
    public async Task C26_BulkRequestsOfOverlappingRecordsInOppositeOrdersDoNotDeadlock()
    {
        RequireConcurrentTransactions();
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30)));
        TestKey[] keys = [.. Enumerable.Range(1, 50).Select(i => new TestKey("t", i))];
        foreach (var key in keys)
        {
            await InsertAsync(source, key);
        }
        var ascending = await ClientAsync(source);
        var descending = await ClientAsync(source);
        const int rounds = 10;

        // Overlapping, unequal sets: the first forty keys ascending and the last forty descending, where canonical order
        // ("t/10" before "t/9") differs from numeric order.
        var low = keys[..40];
        var high = keys[10..].Reverse().ToArray();
        for (var round = 0; round < rounds; round++)
        {
            await Task.WhenAll(RequestAllAsync(ascending, low), RequestAllAsync(descending, high));
        }

        foreach (var key in keys)
        {
            var expected = (low.Contains(key) ? rounds : 0) + (high.Contains(key) ? rounds : 0);
            Assert.Equal(expected, (await ReadAsync(source, key)).RequestedRevision);
        }

        async Task RequestAllAsync(TestProcess process, TestKey[] order)
        {
            await Task.Yield();
            await using var transaction = await BeginAsync();
            Assert.Equal(order.Length, (await Reconciliation(process).RequestManyAsync(transaction.Work, source, order, Cancel)).Count);
            await transaction.CommitAsync(Cancel);
        }
    }

    /// <summary>Signals the store delivers from now on, if it has commit-time signals.</summary>
    private async Task<SignalRecorder> SignalsAsync()
    {
        var recorder = new SignalRecorder(Provider.HasSignals, Cancel);
        if (recorder.Enabled)
        {
            var connected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            recorder.Listener = Provider.CreateStore().Signals!.ListenAsync((topic, key) => recorder.Received.Enqueue((topic, key)), () => connected.TrySetResult(), recorder.Listening.Token);
            await connected.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);
        }
        return recorder;
    }

    private sealed class SignalRecorder(bool enabled, CancellationToken cancellationToken) : IAsyncDisposable
    {
        public bool Enabled { get; } = enabled;

        public ConcurrentQueue<(SignalTopic Topic, string Key)> Received { get; } = new();

        public CancellationTokenSource Listening { get; } = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        public Task Listener { get; set; } = Task.CompletedTask;

        public async ValueTask DisposeAsync()
        {
            await Listening.CancelAsync();
            await Listener;
            Listening.Dispose();
        }
    }
}
