using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>
/// The signal contract through <see cref="IWorkSignals"/>: delivery on commit only, to every process of the
/// application and no other, merging, resync on subscription, and limits. Each scenario uses topics of its own.
/// </summary>
public abstract class SignalScenarios(ILeaseConformanceProvider provider) : LeaseScenarioBase(provider)
{
    /// <summary>How long a scenario waits before concluding that a signal was not delivered.</summary>
    private static readonly TimeSpan Quiet = TimeSpan.FromMilliseconds(300);

    [Fact]
    public async Task S1_SignalsAreDeliveredOnCommitToEveryProcessAndNeverAfterARollback()
    {
        var topic = Topic();
        var (first, second) = (new Recorder(), new Recorder());
        await SubscribedAsync(await ProcessAsync(), topic, first);
        var sender = await ProcessAsync();
        await SubscribedAsync(sender, topic, second);
        var signals = Signals(sender);

        await using (var transaction = await BeginAsync())
        {
            await signals.SendAsync(transaction.Work, topic, "rolled-back", Cancel);
            await transaction.RollbackAsync(Cancel);
        }
        await using (var transaction = await BeginAsync())
        {
            await signals.SendAsync(transaction.Work, topic, "before-savepoint", Cancel);
            await transaction.SavepointAsync(Cancel);
            await signals.SendAsync(transaction.Work, topic, "after-savepoint", Cancel);
            await transaction.RollbackToSavepointAsync(Cancel);
            await signals.SendAsync(transaction.Work, topic, "committed", Cancel);
            Assert.Empty(first.Keys);
            await transaction.CommitAsync(Cancel);
        }

        foreach (var recorder in new[] { first, second })
        {
            await EventuallyAsync(() => recorder.Keys.Contains("committed") && recorder.Keys.Contains("before-savepoint"), TimeSpan.FromSeconds(5), "the committed signals arrive");
        }
        await Task.Delay(Quiet, Cancel);
        Assert.All(new[] { first, second }, r => Assert.Equal(["before-savepoint", "committed"], r.Keys.Order(StringComparer.Ordinal)));
    }

    [Fact]
    public async Task S2_ASignalWithoutATransactionIsDeliveredOnceSent()
    {
        var topic = Topic();
        var recorder = new Recorder();
        var process = await ProcessAsync();
        await SubscribedAsync(process, topic, recorder);

        await Signals(process).SendAsync(topic, "alone", Cancel);

        await EventuallyAsync(() => recorder.Keys.Contains("alone"), TimeSpan.FromSeconds(5), "the signal arrives");
    }

    [Fact]
    public async Task S3_ApplicationsSharingTheDatabaseReceiveOnlyTheirOwnSignals()
    {
        var topic = Topic();
        var (ours, theirs) = (new Recorder(), new Recorder());
        var own = await ProcessAsync();
        var other = await ProcessAsync("signals" + Guid.NewGuid().ToString("N")[..8]);
        await SubscribedAsync(own, topic, ours);
        await SubscribedAsync(other, topic, theirs);

        await Signals(own).SendAsync(topic, "ours", Cancel);
        await Signals(other).SendAsync(topic, "theirs", Cancel);

        await EventuallyAsync(() => ours.Keys.Contains("ours") && theirs.Keys.Contains("theirs"), TimeSpan.FromSeconds(5), "each application's signal arrives");
        await Task.Delay(Quiet, Cancel);
        Assert.Equal(["ours"], ours.Keys);
        Assert.Equal(["theirs"], theirs.Keys);
    }

    [Fact]
    public async Task S4_RepeatedSignalsInOneTransactionArriveAtLeastOnceAndAtMostAsOftenAsSent()
    {
        var topic = Topic();
        var recorder = new Recorder();
        var process = await ProcessAsync();
        await SubscribedAsync(process, topic, recorder);
        var signals = Signals(process);

        await using (var transaction = await BeginAsync())
        {
            for (var i = 0; i < 3; i++)
            {
                await signals.SendAsync(transaction.Work, topic, "same", Cancel);
            }
            await transaction.CommitAsync(Cancel);
        }

        await EventuallyAsync(() => !recorder.Keys.IsEmpty, TimeSpan.FromSeconds(5), "the signal arrives");
        await Task.Delay(Quiet, Cancel);
        Assert.InRange(recorder.Keys.Count, 1, 3);
        Assert.All(recorder.Keys, k => Assert.Equal("same", k));
    }

    [Fact]
    public async Task S5_ASubscriberResyncsBeforeAnySignalItCouldMiss()
    {
        var topic = Topic();
        var recorder = new Recorder();
        var process = await ProcessAsync();
        using var subscription = Signals(process).Subscribe(topic, recorder);
        await EventuallyAsync(() => recorder.Resyncs > 0, TimeSpan.FromSeconds(10), "the subscriber resyncs once listening covers it");

        await using (var transaction = await BeginAsync())
        {
            await Signals(process).SendAsync(transaction.Work, topic, "changed", Cancel);
            await transaction.CommitAsync(Cancel);
        }

        await EventuallyAsync(() => recorder.Keys.Contains("changed"), TimeSpan.FromSeconds(5), "the change is signalled");
        Assert.Equal(Recorder.ResyncEvent, recorder.Events.First());
    }

    [Fact]
    public async Task S8_TopicsAndKeysOutsideTheLimitsAreRefusedAndNothingIsSent()
    {
        var topic = Topic();
        var recorder = new Recorder();
        var process = await ProcessAsync();
        await SubscribedAsync(process, topic, recorder);
        var signals = Signals(process);

        Assert.Throws<ArgumentException>(() => new SignalTopic(new string('a', SignalTopic.MaximumLength + 1)));
        Assert.Throws<ArgumentException>(() => new SignalTopic("Upper.case"));
        Assert.Throws<ArgumentException>(() => signals.Subscribe(default, recorder));
        await Assert.ThrowsAsync<ArgumentException>(async () => await signals.SendAsync(default, "key", Cancel));
        await Assert.ThrowsAsync<ArgumentException>(async () => await signals.SendAsync(topic, new string('k', WorkSignals.MaximumKeyBytes + 1), Cancel));
        await using (var transaction = await BeginAsync())
        {
            await Assert.ThrowsAsync<ArgumentException>(async () => await signals.SendAsync(transaction.Work, topic, new string('k', WorkSignals.MaximumKeyBytes + 1), Cancel));
            await transaction.CommitAsync(Cancel);
        }
        await signals.SendAsync(topic, new string('k', WorkSignals.MaximumKeyBytes), Cancel);

        await EventuallyAsync(() => !recorder.Keys.IsEmpty, TimeSpan.FromSeconds(5), "the signal at the limit arrives");
        await Task.Delay(Quiet, Cancel);
        Assert.Equal([new string('k', WorkSignals.MaximumKeyBytes)], recorder.Keys);
    }

    /// <summary>A topic of the scenario's own, so scenarios sharing a store do not see each other's signals.</summary>
    private static SignalTopic Topic() => new("test." + Guid.NewGuid().ToString("N"));

    private async Task<TestProcess> ProcessAsync(string? application = null)
    {
        if (!Provider.HasSignals)
        {
            Assert.Skip("The store has no commit-time signals.");
        }
        return await ServicesAsync(_ => { }, start: false, application: application);
    }

    private static IWorkSignals Signals(TestProcess process) => process.Services.GetRequiredService<IWorkSignals>();

    /// <summary>Subscribes and waits until listening covers the subscription; the process disposes it.</summary>
    private static async Task SubscribedAsync(TestProcess process, SignalTopic topic, Recorder recorder)
    {
        Signals(process).Subscribe(topic, recorder);
        await EventuallyAsync(() => recorder.Resyncs > 0, TimeSpan.FromSeconds(10), "the subscriber resyncs once listening covers it");
    }

    private sealed class Recorder : ISignalSubscriber
    {
        public const string ResyncEvent = "\0resync";

        public ConcurrentQueue<string> Events { get; } = new();

        public ConcurrentQueue<string> Keys { get; } = new();

        public int Resyncs => Events.Count(e => e == ResyncEvent);

        public void Received(string key)
        {
            Events.Enqueue(key);
            Keys.Enqueue(key);
        }

        public void Resync() => Events.Enqueue(ResyncEvent);
    }
}
