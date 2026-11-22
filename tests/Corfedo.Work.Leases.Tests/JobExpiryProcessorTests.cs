using System.Reflection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work.Leases.Tests;

public sealed class JobExpiryProcessorTests
{
    private static readonly TimeSpan Polling = TimeSpan.FromSeconds(15);

    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task AnIdlePassReadsTheExpiriesOfEveryDefinitionInOneCallAndSettlesNothing()
    {
        var store = new ExpiryStore();
        var processor = Processor(store, new ManualTime(), Enumerable.Range(0, 30).Select(i => $"d{i}").ToArray());

        var wait = await processor.SettleAsync(Cancel);

        Assert.InRange(wait, Polling * 0.9, Polling);
        var read = Assert.Single(store.Reads);
        Assert.Equal(Enumerable.Range(0, 30).Select(i => $"d{i}"), read);
        Assert.Empty(store.Settlements);
    }

    [Fact]
    public async Task AnIdlePassWaitsNoLongerThanTheShortestLeaseDuration()
    {
        var processor = Processor(new ExpiryStore(), new ManualTime(), TimeSpan.FromSeconds(4), "a");

        Assert.InRange(await processor.SettleAsync(Cancel), TimeSpan.FromSeconds(4) * 0.9, TimeSpan.FromSeconds(4));
    }

    [Fact]
    public async Task AnExpiredLeaseLeftUnsettledIsRetriedSoon()
    {
        var store = new ExpiryStore { Expiries = { ["a"] = TimeSpan.Zero, ["b"] = TimeSpan.FromSeconds(10) } };
        var processor = Processor(store, new ManualTime(), "a", "b");

        Assert.InRange(await processor.SettleAsync(Cancel), ExpiryProcessor<JobId>.ExpiredRetry * 0.9, ExpiryProcessor<JobId>.ExpiredRetry);
        Assert.Equal(["job:a"], store.Settlements);
    }

    [Theory]
    [InlineData(4, 4)]
    [InlineData(60, 15)]
    public async Task ASourcesIdlePassWaitsForThePollingIntervalOrTheLeaseDurationWhenShorter(int durationSeconds, int capSeconds)
    {
        var processor = SourceProcessor(new ExpiryStore(), TimeSpan.FromSeconds(durationSeconds));

        Assert.InRange(await processor.SettleAsync(Cancel), TimeSpan.FromSeconds(capSeconds) * 0.9, TimeSpan.FromSeconds(capSeconds));
    }

    [Fact]
    public async Task ASourcesPassWaitsForAKnownExpiryAndRetriesSoonWhileAnExpiredLeaseIsLeftUnsettled()
    {
        var store = new ExpiryStore { NextExpiry = TimeSpan.FromSeconds(2) };
        var processor = SourceProcessor(store, TimeSpan.FromSeconds(30));

        Assert.Equal(TimeSpan.FromSeconds(2), await processor.SettleAsync(Cancel));
        store.NextExpiry = TimeSpan.Zero;
        Assert.InRange(await processor.SettleAsync(Cancel), ExpiryProcessor<JobId>.ExpiredRetry * 0.9, ExpiryProcessor<JobId>.ExpiredRetry);
    }

    [Fact]
    public async Task OnlyDefinitionsWithAnExpiredLeaseAreSettledAndThePassWaitsForTheNextExpiry()
    {
        var store = new ExpiryStore { Expiries = { ["a"] = TimeSpan.Zero, ["b"] = TimeSpan.FromMilliseconds(200) } };
        var processor = Processor(store, new ManualTime(), "a", "b", "c");

        // The expired lease of a is locked, so nothing is settled and the pass waits for b's expiry.
        var wait = await processor.SettleAsync(Cancel);

        Assert.Equal(["job:a"], store.Settlements);
        Assert.Equal(TimeSpan.FromMilliseconds(200), wait);
    }

    [Fact]
    public async Task APassThatSettledALeaseRunsAgainAtOnce()
    {
        var store = new ExpiryStore { Expiries = { ["a"] = TimeSpan.Zero }, Settled = _ => 1 };
        var processor = Processor(store, new ManualTime(), "a", "b");

        Assert.Equal(TimeSpan.Zero, await processor.SettleAsync(Cancel));
        Assert.Equal(["job:a"], store.Settlements);
    }

    [Fact]
    public async Task AFailedSettlementPassesTheDefinitionOverForThePollingIntervalWithoutHoldingBackTheOthers()
    {
        var time = new ManualTime();
        var store = new ExpiryStore
        {
            Expiries = { ["a"] = TimeSpan.Zero, ["b"] = TimeSpan.Zero },
            Settled = source => source == "job:a" ? throw new InvalidOperationException("boom") : 0,
        };
        var processor = Processor(store, time, "a", "b");

        Assert.InRange(await processor.SettleAsync(Cancel), ExpiryProcessor<JobId>.ExpiredRetry * 0.9, ExpiryProcessor<JobId>.ExpiredRetry);
        Assert.Equal(["job:a", "job:b"], store.Settlements);

        time.Advance(Polling - TimeSpan.FromSeconds(1));
        Assert.InRange(await processor.SettleAsync(Cancel), ExpiryProcessor<JobId>.ExpiredRetry * 0.9, ExpiryProcessor<JobId>.ExpiredRetry);
        Assert.Equal(["job:a", "job:b", "job:b"], store.Settlements);

        time.Advance(TimeSpan.FromSeconds(1));
        await processor.SettleAsync(Cancel);
        Assert.Equal(["job:a", "job:b", "job:b", "job:a", "job:b"], store.Settlements);
    }

    [Fact]
    public async Task AFailedReadWaitsForThePollingInterval()
    {
        var store = new ExpiryStore { ReadFails = true };
        var processor = Processor(store, new ManualTime(), "a");

        Assert.Equal(Polling, await processor.SettleAsync(Cancel));
        Assert.Empty(store.Settlements);
    }

    [Fact]
    public async Task CancellationDuringASettlementEndsThePassWithoutPassingTheDefinitionOver()
    {
        using var stopping = new CancellationTokenSource();
        var store = new ExpiryStore
        {
            Expiries = { ["a"] = TimeSpan.Zero },
            Settled = _ =>
            {
                stopping.Cancel();
                throw new OperationCanceledException(stopping.Token);
            },
        };
        var processor = Processor(store, new ManualTime(), "a");

        await Assert.ThrowsAsync<OperationCanceledException>(() => processor.SettleAsync(stopping.Token));
        store.Settled = _ => 0;
        await processor.SettleAsync(Cancel);
        Assert.Equal(["job:a", "job:a"], store.Settlements);
    }

    private static ExpiryProcessor<JobId> SourceProcessor(ExpiryStore store, TimeSpan duration)
    {
        var registration = Registration("a", duration);
        return new ExpiryProcessor<JobId>(registration, registration.Callbacks(new NoServices()), Context(store, new ManualTime()));
    }

    private static JobExpiryProcessor Processor(ExpiryStore store, TimeProvider time, params string[] definitions) =>
        Processor(store, time, TimeSpan.FromSeconds(30), definitions);

    private static JobExpiryProcessor Processor(ExpiryStore store, TimeProvider time, TimeSpan duration, params string[] definitions)
        => new(definitions.Select(d => Registration(d, duration)).ToArray(), Context(store, time));

    private static LeaseSourceRegistration<JobId> Registration(string definition, TimeSpan duration) => new(
        JobSource.Define(StoreLocation.Table("work", "jobs"), new StoredJobDefinition(definition, 1, null, new JobPolicy(), [], Paused: false), [1], duration),
        claimHook: null,
        expiryHandler: null,
        handler: null,
        capacity: 1,
        timeout: null);

    private static LeaseDispatchContext Context(ExpiryStore store, TimeProvider time) =>
        new(new NoServices(), store.Work, null!, time, NullLoggerFactory.Instance, Polling, TimeSpan.FromMilliseconds(10));

    /// <summary>A monotonic clock that moves only when the test advances it.</summary>
    private sealed class ManualTime : TimeProvider
    {
        private long ticks;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override long GetTimestamp() => ticks;

        public void Advance(TimeSpan by) => ticks += by.Ticks;
    }

    private sealed class NoServices : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }

    /// <summary>
    /// Answers the expiry reads with <see cref="Expiries"/> and records them, and records each settlement by source
    /// name, returning what <see cref="Settled"/> returns; every other store call fails the test.
    /// </summary>
    private sealed class ExpiryStore
    {
        public ExpiryStore()
        {
            var jobs = DispatchProxy.Create<IJobStore, StoreProxy>();
            ((StoreProxy)(object)jobs).Handler = (method, args) => method.Name == nameof(IJobStore.NextExpiriesAsync)
                ? Read((IReadOnlyCollection<string>)args![0]!)
                : throw new NotSupportedException(method.Name);
            var leases = DispatchProxy.Create<ILeaseStore, StoreProxy>();
            ((StoreProxy)(object)leases).Handler = (method, args) => method.Name switch
            {
                nameof(ILeaseStore.SettleExpiredAsync) => Settle(((LeaseSource<JobId>)args![0]!).Name),
                nameof(ILeaseStore.NextExpiryAsync) => ValueTask.FromResult(NextExpiry),
                _ => throw new NotSupportedException(method.Name),
            };
            var work = DispatchProxy.Create<IWorkStore, StoreProxy>();
            ((StoreProxy)(object)work).Handler = (method, _) => method.Name switch
            {
                "get_Jobs" => jobs,
                "get_Leases" => leases,
                _ => throw new NotSupportedException(method.Name),
            };
            Work = work;
        }

        public IWorkStore Work { get; }

        public Dictionary<string, TimeSpan> Expiries { get; } = [];

        /// <summary>What a source's next-expiry read returns.</summary>
        public TimeSpan? NextExpiry { get; set; }

        public Func<string, int> Settled { get; set; } = _ => 0;

        public bool ReadFails { get; init; }

        public List<string[]> Reads { get; } = [];

        public List<string> Settlements { get; } = [];

        private ValueTask<IReadOnlyDictionary<string, TimeSpan>> Read(IReadOnlyCollection<string> definitions)
        {
            Reads.Add([.. definitions]);
            if (ReadFails)
            {
                throw new InvalidOperationException("unavailable");
            }
            IReadOnlyDictionary<string, TimeSpan> found = Expiries.Where(e => definitions.Contains(e.Key)).ToDictionary();
            return ValueTask.FromResult(found);
        }

        private ValueTask<int> Settle(string source)
        {
            Settlements.Add(source);
            return ValueTask.FromResult(Settled(source));
        }
    }

    public class StoreProxy : DispatchProxy
    {
        public Func<MethodInfo, object?[]?, object?> Handler { get; set; } = (method, _) => throw new NotSupportedException(method.Name);

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) => Handler(targetMethod!, args);
    }
}
