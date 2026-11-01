using System.Diagnostics;

namespace Corfedo.Work.Leases.Tests;

public sealed class LeaseHoldTests
{
    private static readonly LeaseSource<string> Source = LeaseSource.Define(
        "s", StoreLocation.Table("app", "records"), new StringCodec(), new LeasePolicy { Duration = TimeSpan.FromMilliseconds(300) });

    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task FailedRenewalsAreToleratedUntilTheLastConfirmedExpiry()
    {
        var store = new RenewingStore((_, _) => throw new InvalidOperationException("store unavailable"));
        var stopwatch = Stopwatch.StartNew();
        await using var hold = Hold(store, TimeSpan.FromMilliseconds(600));

        await StoppedAsync(hold);

        Assert.Equal(LeaseStopReason.Lost, hold.StopReason);
        Assert.True(hold.Lost);
        Assert.InRange(stopwatch.Elapsed, TimeSpan.FromMilliseconds(550), TimeSpan.FromSeconds(3));
        Assert.True(store.Calls >= 2);
    }

    [Fact]
    public async Task RenewalReportingCancellationStopsWithThatReasonAndKeepsRenewing()
    {
        var store = new RenewingStore((_, _) => new RenewResult(true, true, TimeSpan.FromMilliseconds(300)));
        await using var hold = Hold(store, TimeSpan.FromMilliseconds(300));

        await StoppedAsync(hold);
        var calls = store.Calls;
        await Task.Delay(400, Cancel);

        Assert.Equal(LeaseStopReason.CancelRequested, hold.StopReason);
        Assert.False(hold.Lost);
        Assert.True(store.Calls > calls);
    }

    [Fact]
    public async Task InvalidRenewalLosesTheLeaseAndTheFirstReasonWins()
    {
        var store = new RenewingStore((_, _) => RenewResult.Lost);
        await using var hold = Hold(store, TimeSpan.FromMilliseconds(300));

        await StoppedAsync(hold);
        hold.Stop(LeaseStopReason.Shutdown);

        Assert.Equal(LeaseStopReason.Lost, hold.StopReason);
        Assert.Equal(1, store.Calls);
    }

    [Fact]
    public async Task RenewalStillRunningAtTheLastConfirmedExpiryLosesTheLease()
    {
        var store = new RenewingStore(hang: true);
        var stopwatch = Stopwatch.StartNew();
        await using var hold = Hold(store, TimeSpan.FromMilliseconds(400));

        await StoppedAsync(hold);

        Assert.Equal(LeaseStopReason.Lost, hold.StopReason);
        Assert.InRange(stopwatch.Elapsed, TimeSpan.FromMilliseconds(350), TimeSpan.FromSeconds(2));
        Assert.Equal(1, store.Calls);
    }

    [Fact]
    public async Task RenewalIgnoringCancellationDoesNotKeepTheHoldPastTheLastConfirmedExpiry()
    {
        var store = new RenewingStore(ignoreCancellation: true);
        var stopwatch = Stopwatch.StartNew();
        await using var hold = Hold(store, TimeSpan.FromMilliseconds(300));

        await StoppedAsync(hold);

        Assert.Equal(LeaseStopReason.Lost, hold.StopReason);
        Assert.True(hold.Lost);
        Assert.InRange(stopwatch.Elapsed, TimeSpan.FromMilliseconds(250), TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task ShortValidityIsEnforcedWithoutWaitingForTheRenewalInterval()
    {
        var store = new RenewingStore((_, _) => throw new InvalidOperationException("store unavailable"));
        var stopwatch = Stopwatch.StartNew();
        await using var hold = Hold(store, TimeSpan.FromMilliseconds(100), duration: TimeSpan.FromSeconds(30));

        await StoppedAsync(hold);

        Assert.True(hold.Lost);
        Assert.InRange(stopwatch.Elapsed, TimeSpan.FromMilliseconds(50), TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task ValidityUsedUpBeforeTheHoldStartsLosesTheLeaseAtOnce()
    {
        var store = new RenewingStore((_, _) => new RenewResult(true, false, TimeSpan.FromMilliseconds(300)));
        var claimedAt = TimeProvider.System.GetTimestamp() - TimeProvider.System.TimestampFrequency;
        await using var hold = Hold(store, TimeSpan.FromMilliseconds(300), claimedAt: claimedAt);

        Assert.True(hold.Lost);
        Assert.Equal(LeaseStopReason.Lost, hold.StopReason);
        await Task.Delay(300, Cancel);
        Assert.Equal(0, store.Calls);
    }

    private static Task StoppedAsync(LeaseHold<string> hold)
    {
        var stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        hold.Token.Register(() => stopped.TrySetResult());
        return stopped.Task.WaitAsync(TimeSpan.FromSeconds(5), Cancel);
    }

    private static LeaseHold<string> Hold(ILeaseStore store, TimeSpan validFor, TimeSpan? duration = null, long? claimedAt = null) => new(
        store,
        TimeProvider.System,
        new LeaseClaim<string>
        {
            Lease = new Lease<string>(
                duration is { } d ? LeaseSource.Define("s", Source.Store, Source.Key, Source.Policy with { Duration = d }) : Source,
                "a",
                OwnerId.New(),
                1),
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
            ValidFor = validFor,
        },
        claimedAt ?? TimeProvider.System.GetTimestamp(),
        timeout: null);

    private sealed class StringCodec : IKeyCodec<string>
    {
        public IReadOnlyList<KeyField> Fields { get; } = [new("id", KeyFieldType.String)];

        public void Write(string key, IKeyWriter writer) => writer.Write(key);

        public string Read(IKeyReader reader) => reader.ReadString();

        public string Format(string key) => key;

        public string Parse(string value) => value;
    }

    /// <summary>A store whose only working operation is renewal.</summary>
    private sealed class RenewingStore(Func<Lease<string>, int, RenewResult>? renew = null, bool hang = false, bool ignoreCancellation = false) : ILeaseStore
    {
        private int calls;

        public int Calls => Volatile.Read(ref calls);

        public async ValueTask<RenewResult> RenewAsync<TKey>(Lease<TKey> lease, CancellationToken cancellationToken)
            where TKey : notnull
        {
            var call = Interlocked.Increment(ref calls);
            if (hang)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            if (ignoreCancellation)
            {
                await Task.Delay(Timeout.Infinite, CancellationToken.None);
            }
            return renew!((Lease<string>)(object)lease, call);
        }

        public ValueTask<DueClaims<TKey>> ClaimDueAsync<TKey>(LeaseSource<TKey> source, LeaseCallbacks<TKey> callbacks, OwnerId owner, int limit, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask<LeaseClaim<TKey>?> ClaimAsync<TKey>(LeaseSource<TKey> source, LeaseCallbacks<TKey> callbacks, OwnerId owner, TKey key, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask<IReadOnlyList<LeaseClaim<TKey>>> ClaimAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, LeaseCallbacks<TKey> callbacks, OwnerId owner, IReadOnlyList<TKey> candidates, int limit, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask<RenewResult> RenewAsync<TKey>(WorkTransaction transaction, Lease<TKey> lease, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask<bool> ReleaseAsync<TKey>(Lease<TKey> lease, Due due, bool failed, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask<EndResult> EndAsync<TKey>(WorkTransaction transaction, Lease<TKey> lease, LeaseCompletion? completion, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask<bool> ScheduleAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, TKey key, Due due, Guid? execution, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask<bool> RequestCancelAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, TKey key, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask<bool> ClearCancelAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, TKey key, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask<int> SettleExpiredAsync<TKey>(LeaseSource<TKey> source, LeaseCallbacks<TKey> callbacks, int limit, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask<TimeSpan?> NextDueAsync<TKey>(LeaseSource<TKey> source, DateTimeOffset cutoff, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask<TimeSpan?> NextExpiryAsync<TKey>(LeaseSource<TKey> source, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();

        public ValueTask VerifyAsync<TKey>(LeaseSource<TKey> source, CancellationToken cancellationToken)
            where TKey : notnull => throw new NotSupportedException();
    }
}
