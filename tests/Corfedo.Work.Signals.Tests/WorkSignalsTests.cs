using System.Collections.Concurrent;
using System.Data.Common;

namespace Corfedo.Work.Signals.Tests;

public sealed class WorkSignalsTests
{
    private static readonly SignalTopic Orders = new("app.orders");
    private static readonly SignalTopic Invoices = new("app.invoices");

    [Fact]
    public async Task Subscribers_get_resync_when_listening_connects_and_signals_of_their_topic()
    {
        var transport = new TestTransport();
        await using var signals = new WorkSignals(transport);
        var orders = new Recorder();
        var invoices = new Recorder();
        using var a = signals.Subscribe(Orders, orders);
        using var b = signals.Subscribe(Invoices, invoices);
        var listen = await transport.Listening.Task.WaitAsync(TestContext.Current.CancellationToken);
        Assert.Equal(0, orders.Resyncs);

        listen.Connected();
        listen.Received(Orders, "1");
        listen.Received(new SignalTopic("app.other"), "2");

        Assert.Equal(1, orders.Resyncs);
        Assert.Equal(1, invoices.Resyncs);
        Assert.Equal(["1"], orders.Keys);
        Assert.Empty(invoices.Keys);
    }

    [Fact]
    public async Task A_subscription_while_connected_resyncs_at_once_and_every_reconnection_resyncs_all()
    {
        var transport = new TestTransport();
        await using var signals = new WorkSignals(transport);
        var first = new Recorder();
        using var a = signals.Subscribe(Orders, first);
        var listen = await transport.Listening.Task.WaitAsync(TestContext.Current.CancellationToken);
        listen.Connected();

        var second = new Recorder();
        using var b = signals.Subscribe(Orders, second);
        Assert.Equal(1, second.Resyncs);

        listen.Connected();
        Assert.Equal(2, first.Resyncs);
        Assert.Equal(2, second.Resyncs);
        Assert.Equal(1, transport.Listens);
    }

    [Fact]
    public async Task Disposing_the_last_subscription_stops_listening_and_a_new_one_listens_again_without_overlap()
    {
        var transport = new TestTransport();
        await using var signals = new WorkSignals(transport);
        var first = signals.Subscribe(Orders, new Recorder());
        var listen = await transport.Listening.Task.WaitAsync(TestContext.Current.CancellationToken);
        listen.Connected();
        transport.Listening = new TaskCompletionSource<TestListen>(TaskCreationOptions.RunContinuationsAsynchronously);

        first.Dispose();
        var later = new Recorder();
        using var second = signals.Subscribe(Orders, later);
        Assert.Equal(0, later.Resyncs);
        var next = await transport.Listening.Task.WaitAsync(TestContext.Current.CancellationToken);
        Assert.True(listen.Stopped.IsCancellationRequested);
        next.Connected();

        Assert.Equal(1, later.Resyncs);
        Assert.Equal(2, transport.Listens);
        Assert.Equal(1, transport.MostConcurrent);
    }

    [Fact]
    public async Task A_subscriber_that_throws_does_not_keep_the_signal_from_others()
    {
        var transport = new TestTransport();
        await using var signals = new WorkSignals(transport);
        var recorder = new Recorder();
        using var a = signals.Subscribe(Orders, new Throwing());
        using var b = signals.Subscribe(Orders, recorder);
        var listen = await transport.Listening.Task.WaitAsync(TestContext.Current.CancellationToken);

        listen.Connected();
        listen.Received(Orders, "1");

        Assert.Equal(1, recorder.Resyncs);
        Assert.Equal(["1"], recorder.Keys);
        Assert.False(listen.Stopped.IsCancellationRequested);
    }

    [Fact]
    public async Task Sends_pass_the_transaction_to_the_transport()
    {
        var transport = new TestTransport();
        await using var signals = new WorkSignals(transport);
        var transaction = new TestTransaction();

        await signals.SendAsync(transaction, Orders, "1", TestContext.Current.CancellationToken);
        await signals.SendAsync(Orders, "2", TestContext.Current.CancellationToken);

        Assert.Equal([(transaction, "app.orders", "1"), (null, "app.orders", "2")], transport.Sent);
    }

    [Theory]
    [InlineData("")]
    [InlineData("App.orders")]
    [InlineData("app orders")]
    [InlineData("app:orders")]
    public void Topics_outside_the_allowed_characters_are_refused(string name) =>
        Assert.ThrowsAny<ArgumentException>(() => new SignalTopic(name));

    [Fact]
    public async Task Topics_and_keys_outside_the_limits_are_refused_and_nothing_is_sent()
    {
        var transport = new TestTransport();
        await using var signals = new WorkSignals(transport);
        var cancellation = TestContext.Current.CancellationToken;

        Assert.Throws<ArgumentException>(() => new SignalTopic(new string('a', SignalTopic.MaximumLength + 1)));
        _ = new SignalTopic(new string('a', SignalTopic.MaximumLength));
        await Assert.ThrowsAsync<ArgumentException>(async () => await signals.SendAsync(Orders, new string('é', WorkSignals.MaximumKeyBytes / 2 + 1), cancellation));
        await Assert.ThrowsAsync<ArgumentException>(async () => await signals.SendAsync(default, "1", cancellation));
        Assert.Throws<ArgumentException>(() => signals.Subscribe(default, new Recorder()));
        await signals.SendAsync(Orders, new string('é', WorkSignals.MaximumKeyBytes / 2), cancellation);

        Assert.Single(transport.Sent);
    }

    [Fact]
    public void Cancel_keys_round_trip_and_other_keys_do_not_parse()
    {
        var owner = OwnerId.New();

        Assert.True(WorkTopics.TryParseCancelKey(WorkTopics.CancelKey(owner, "orders:eu"), out var parsed, out var source));
        Assert.Equal(owner, parsed);
        Assert.Equal("orders:eu", source);
        Assert.False(WorkTopics.TryParseCancelKey("orders", out _, out _));
        Assert.False(WorkTopics.TryParseCancelKey(owner + ":", out _, out _));
    }

    private sealed class Recorder : ISignalSubscriber
    {
        public ConcurrentQueue<string> Keys { get; } = new();

        public int Resyncs { get; private set; }

        public void Received(string key) => Keys.Enqueue(key);

        public void Resync() => Resyncs++;
    }

    private sealed class Throwing : ISignalSubscriber
    {
        public void Received(string key) => throw new InvalidOperationException("received");

        public void Resync() => throw new InvalidOperationException("resync");
    }

    private sealed class TestTransaction : IWorkTransaction;

    private sealed record TestListen(Action<SignalTopic, string> Received, Action Connected, CancellationToken Stopped);

    private sealed class TestTransport : ISignalTransport
    {
        private int active;

        public TaskCompletionSource<TestListen> Listening { get; set; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Listens { get; private set; }

        public int MostConcurrent { get; private set; }

        public List<(IWorkTransaction?, string, string)> Sent { get; } = [];

        public ValueTask SendAsync(IWorkTransaction? transaction, SignalTopic topic, string key, CancellationToken cancellationToken)
        {
            Sent.Add((transaction, topic.Name, key));
            return ValueTask.CompletedTask;
        }

        public async Task ListenAsync(Action<SignalTopic, string> received, Action connected, CancellationToken cancellationToken)
        {
            Listens++;
            MostConcurrent = Math.Max(MostConcurrent, Interlocked.Increment(ref active));
            try
            {
                Listening.TrySetResult(new TestListen(received, connected, cancellationToken));
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            finally
            {
                Interlocked.Decrement(ref active);
            }
        }

        public IWorkTransaction Transaction(DbTransaction transaction) => throw new NotSupportedException();
    }
}
