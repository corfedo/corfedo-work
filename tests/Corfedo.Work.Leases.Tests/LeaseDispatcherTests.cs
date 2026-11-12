using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work.Leases.Tests;

public sealed class LeaseDispatcherTests
{
    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ShutdownStopsRenewingRecordsItCouldNotReleaseWhileTheirHandlersRun()
    {
        var source = LeaseSource.Define("s", StoreLocation.Table("app", "records"), new StringCodec(), new LeasePolicy { Duration = TimeSpan.FromMilliseconds(300) });
        var store = new DispatchStore(source);
        var handlerRunning = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new IgnoringHandler(handlerRunning, handlerDone.Task);
        var registration = new LeaseSourceRegistration<string>(source, null, null, _ => handler, capacity: 1, timeout: null);
        var services = new ScopeProvider();
        var workStore = new WorkStore(store);
        var operations = new LeaseOperations(workStore, new WorkProcess(), new LeaseRegistry([registration], services));
        var context = new LeaseDispatchContext(services, workStore, operations, TimeProvider.System, NullLoggerFactory.Instance, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(10));
        var dispatcher = registration.CreateDispatcher(context);
        try
        {
            dispatcher.Start();
            await handlerRunning.Task.WaitAsync(TimeSpan.FromSeconds(5), Cancel);

            await dispatcher.StopAsync(Cancel);
            await Task.Delay(150, Cancel);
            var renewals = store.Renewals;
            await Task.Delay(500, Cancel);

            Assert.Equal(1, store.Releases);
            Assert.Equal(renewals, store.Renewals);
        }
        finally
        {
            handlerDone.TrySetResult();
        }
    }

    [Fact]
    public async Task RunWhoseHoldWasLostIsReleasedAsFailed()
    {
        var source = LeaseSource.Define("s", StoreLocation.Table("app", "records"), new StringCodec(), new LeasePolicy { Duration = TimeSpan.FromMilliseconds(300) });
        var store = new DispatchStore(source, renewalsHang: true, releasesFail: false);
        var stopped = new TaskCompletionSource<LeaseStopReason?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new WaitingHandler(stopped);
        await using var dispatcher = Dispatcher(source, store, handler);

        Assert.Equal(LeaseStopReason.Lost, await stopped.Task.WaitAsync(TimeSpan.FromSeconds(5), Cancel));

        await WaitForAsync(() => store.Releases == 1);
        Assert.Equal(1, store.FailedReleases);
    }

    [Fact]
    public async Task ClaimLostBeforeItsRunStartsIsReleasedWithoutRunningTheHandlerAndFreesItsCapacity()
    {
        var source = LeaseSource.Define("s", StoreLocation.Table("app", "records"), new StringCodec(), new LeasePolicy { Duration = TimeSpan.FromSeconds(30) });
        var store = new DispatchStore(source, validFor: TimeSpan.Zero, releasesFail: false, claims: 2);
        var stopped = new TaskCompletionSource<LeaseStopReason?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new WaitingHandler(stopped);
        await using var dispatcher = Dispatcher(source, store, handler);

        Assert.Equal("r2", await handler.Started.Task.WaitAsync(TimeSpan.FromSeconds(5), Cancel));
        Assert.Equal((1, 1), (store.Releases, store.FailedReleases));
    }

    [Fact]
    public async Task ANextDueOfZeroClaimsAgainAfterTheCutoffOfTheClaimThatRan()
    {
        var source = LeaseSource.Define("s", StoreLocation.Table("app", "records"), new StringCodec(), new LeasePolicy { Duration = TimeSpan.FromSeconds(30) });
        // Nothing is claimed; the first next-due lookup reports a record that became claimable after the claim's cutoff.
        var store = new DispatchStore(source, claims: 0, nextDue: call => call == 1 ? TimeSpan.Zero : null);
        await using var dispatcher = Dispatcher(source, store, new WaitingHandler(new()));

        await WaitForAsync(() => store.NextDueCutoffs.Count == 2);
        await Task.Delay(300, Cancel);

        Assert.Equal(2, store.Claims);
        Assert.Equal([DispatchStore.CutoffOf(1), DispatchStore.CutoffOf(2)], store.NextDueCutoffs);
    }

    private static StartedDispatcher Dispatcher(LeaseSource<string> source, DispatchStore store, ILeaseHandler<string> handler)
    {
        var registration = new LeaseSourceRegistration<string>(source, null, null, _ => handler, capacity: 1, timeout: null);
        var services = new ScopeProvider();
        var workStore = new WorkStore(store);
        var operations = new LeaseOperations(workStore, new WorkProcess(), new LeaseRegistry([registration], services));
        var context = new LeaseDispatchContext(services, workStore, operations, TimeProvider.System, NullLoggerFactory.Instance, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(10));
        var dispatcher = registration.CreateDispatcher(context);
        dispatcher.Start();
        return new StartedDispatcher(dispatcher);
    }

    private static async Task WaitForAsync(Func<bool> condition)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        while (!condition())
        {
            Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(5), "timed out");
            await Task.Delay(10, Cancel);
        }
    }

    private sealed class StartedDispatcher(ILeaseDispatcher dispatcher) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync() => await dispatcher.StopAsync(CancellationToken.None);
    }

    private sealed class WaitingHandler(TaskCompletionSource<LeaseStopReason?> stopped) : ILeaseHandler<string>
    {
        public TaskCompletionSource<string> Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async ValueTask RunAsync(LeaseRun<string> run, CancellationToken cancellationToken)
        {
            Started.TrySetResult(run.Key);
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            stopped.TrySetResult(run.Hold.StopReason);
            cancellationToken.ThrowIfCancellationRequested();
        }

        public ValueTask ExhaustedAsync(LeaseRun<string> run, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    private sealed class IgnoringHandler(TaskCompletionSource running, Task done) : ILeaseHandler<string>
    {
        public async ValueTask RunAsync(LeaseRun<string> run, CancellationToken cancellationToken)
        {
            running.TrySetResult();
            await done;
        }

        public ValueTask ExhaustedAsync(LeaseRun<string> run, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    private sealed class ScopeProvider : IServiceProvider, IServiceScopeFactory, IServiceScope
    {
        public IServiceProvider ServiceProvider => this;

        public object? GetService(Type serviceType) => serviceType == typeof(IServiceScopeFactory) ? this : null;

        public IServiceScope CreateScope() => this;

        public void Dispose()
        {
        }
    }

    private sealed class WorkStore(ILeaseStore leases) : IWorkStore
    {
        public ILeaseStore Leases => leases;

        public IReconcileStore Reconcile => throw new NotSupportedException();

        public IJobStore Jobs => throw new NotSupportedException();

        public ICalendarStore Calendars => throw new NotSupportedException();

        public IChainStore Chains => throw new NotSupportedException();

        public IProcessStore Processes => throw new NotSupportedException();

        public IMonitoringStore Monitoring => throw new NotSupportedException();

        public ISignalTransport? Signals => null;

        public ValueTask PrepareAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public WorkTransaction Transaction(System.Data.Common.DbTransaction transaction) => throw new NotSupportedException();
    }

    private sealed class StringCodec : IKeyCodec<string>
    {
        public IReadOnlyList<KeyField> Fields { get; } = [new("id", KeyFieldType.String)];

        public void Write(string key, IKeyWriter writer) => writer.Write(key);

        public string Read(IKeyReader reader) => reader.ReadString();

        public string Format(string key) => key;

        public string Parse(string value) => value;
    }

    /// <summary>
    /// Hands out <paramref name="claims"/> claims one at a time, the first valid for <paramref name="validFor"/> (the
    /// lease duration by default), renews them as valid or never answers a renewal, and fails every release or reports
    /// it done.
    /// </summary>
    private sealed class DispatchStore(LeaseSource<string> source, TimeSpan? validFor = null, bool renewalsHang = false, bool releasesFail = true, int claims = 1, Func<int, TimeSpan?>? nextDue = null) : ILeaseStore
    {
        private readonly System.Collections.Concurrent.ConcurrentQueue<DateTimeOffset> nextDueCutoffs = new();
        private int claimed;
        private int renewals;
        private int releases;
        private int failedReleases;

        public int Renewals => Volatile.Read(ref renewals);

        public int Releases => Volatile.Read(ref releases);

        /// <summary>Releases that counted a failure.</summary>
        public int FailedReleases => Volatile.Read(ref failedReleases);

        /// <summary>Calls of <see cref="ClaimDueAsync{TKey}"/>.</summary>
        public int Claims => Volatile.Read(ref claimed);

        /// <summary>The cutoffs next-due lookups were asked for, in order.</summary>
        public IReadOnlyList<DateTimeOffset> NextDueCutoffs => [.. nextDueCutoffs];

        /// <summary>The cutoff the given claim call reports.</summary>
        public static DateTimeOffset CutoffOf(int call) => DateTimeOffset.UnixEpoch.AddSeconds(call);

        public ValueTask<DueClaims<TKey>> ClaimDueAsync<TKey>(LeaseSource<TKey> leaseSource, LeaseCallbacks<TKey> callbacks, OwnerId owner, int limit, CancellationToken cancellationToken)
            where TKey : notnull
        {
            var number = Interlocked.Increment(ref claimed);
            if (number > claims)
            {
                return ValueTask.FromResult(new DueClaims<TKey>([], CutoffOf(number)));
            }
            var claim = new LeaseClaim<string>
            {
                Lease = new Lease<string>(source, "r" + number, owner, 1),
                Reason = ClaimReason.Due,
                PreviousOwner = null,
                PreviousEpoch = 0,
                Takeovers = 0,
                Failures = 0,
                Exhausted = false,
                CancelRequested = false,
                ExecutionId = Guid.NewGuid(),
                AttemptId = Guid.NewGuid(),
                Partition = null,
                ValidFor = number == 1 && validFor is { } first ? first : source.Policy.Duration,
            };
            return ValueTask.FromResult(new DueClaims<TKey>([(LeaseClaim<TKey>)(object)claim], CutoffOf(number)));
        }

        public async ValueTask<RenewResult> RenewAsync<TKey>(Lease<TKey> lease, CancellationToken cancellationToken)
            where TKey : notnull
        {
            Interlocked.Increment(ref renewals);
            if (renewalsHang)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            return new RenewResult(true, false, source.Policy.Duration);
        }

        public ValueTask<bool> ReleaseAsync<TKey>(Lease<TKey> lease, Due due, bool failed, CancellationToken cancellationToken)
            where TKey : notnull
        {
            Interlocked.Increment(ref releases);
            if (failed)
            {
                Interlocked.Increment(ref failedReleases);
            }
            return releasesFail ? throw new InvalidOperationException("store unavailable") : ValueTask.FromResult(true);
        }

        public ValueTask<TimeSpan?> NextDueAsync<TKey>(LeaseSource<TKey> leaseSource, DateTimeOffset cutoff, CancellationToken cancellationToken)
            where TKey : notnull
        {
            nextDueCutoffs.Enqueue(cutoff);
            return ValueTask.FromResult(nextDue?.Invoke(nextDueCutoffs.Count));
        }

        public ValueTask<LeaseClaim<TKey>?> ClaimAsync<TKey>(LeaseSource<TKey> leaseSource, LeaseCallbacks<TKey> callbacks, OwnerId owner, TKey key, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask<IReadOnlyList<LeaseClaim<TKey>>> ClaimAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> leaseSource, LeaseCallbacks<TKey> callbacks, OwnerId owner, IReadOnlyList<TKey> candidates, int limit, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask<RenewResult> RenewAsync<TKey>(WorkTransaction transaction, Lease<TKey> lease, bool progressed, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask<EndResult> EndAsync<TKey>(WorkTransaction transaction, Lease<TKey> lease, LeaseCompletion? completion, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask<bool> ScheduleAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> leaseSource, TKey key, Due due, Guid? execution, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask<bool> RequestCancelAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> leaseSource, TKey key, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask<bool> ClearCancelAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> leaseSource, TKey key, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask<int> SettleExpiredAsync<TKey>(LeaseSource<TKey> leaseSource, LeaseCallbacks<TKey> callbacks, int limit, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask<TimeSpan?> NextExpiryAsync<TKey>(LeaseSource<TKey> leaseSource, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask VerifyAsync<TKey>(LeaseSource<TKey> leaseSource, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask<IReadOnlyList<LeaseRecord?>> ReadAsync<TKey>(WorkTransaction? transaction, LeaseSource<TKey> leaseSource, IReadOnlyList<TKey> keys, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();
    }
}
