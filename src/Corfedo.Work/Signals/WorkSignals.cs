using System.Data.Common;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work;

/// <summary>
/// The process's hub over a provider's <see cref="ISignalTransport"/>. It listens on one connection while it has
/// subscribers and dispatches by topic in process. One listening loop runs at a time: the first subscription starts
/// it, disposing the last stops it, and a subscription made while it stops keeps it running, so a process never
/// listens twice and no subscriber misses its <see cref="ISignalSubscriber.Resync"/>.
/// </summary>
public sealed partial class WorkSignals : IWorkSignals, IAsyncDisposable, IDisposable
{
    /// <summary>The longest key, in UTF-8 bytes.</summary>
    public const int MaximumKeyBytes = 1024;

    private static readonly TimeSpan Retry = TimeSpan.FromSeconds(1);

    private readonly ISignalTransport transport;
    private readonly ILogger logger;
    private readonly WorkTelemetryOptions telemetry;
    private readonly Lock gate = new();

    // Replaced on every change, so the listener reads it without the lock.
    private Dictionary<string, Subscription[]> topics = new(StringComparer.Ordinal);
    private int count;
    private CancellationTokenSource? listening;
    private bool running;
    private bool connected;
    private bool disposed;
    private Task loop = Task.CompletedTask;

    public WorkSignals(ISignalTransport transport, ILoggerFactory? loggerFactory = null)
        : this(transport, loggerFactory ?? NullLoggerFactory.Instance, WorkTelemetryOptions.Default)
    {
    }

    internal WorkSignals(ISignalTransport transport, ILoggerFactory loggerFactory, WorkTelemetryOptions telemetry)
    {
        ArgumentNullException.ThrowIfNull(transport);
        this.transport = transport;
        this.telemetry = telemetry;
        logger = loggerFactory.CreateLogger("Corfedo.Work.Signals");
    }

    public ValueTask SendAsync(IWorkTransaction transaction, SignalTopic topic, string key, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        Validate(topic, key);
        return transport.SendAsync(transaction, topic, key, cancellationToken);
    }

    public ValueTask SendAsync(SignalTopic topic, string key, CancellationToken cancellationToken)
    {
        Validate(topic, key);
        return transport.SendAsync(null, topic, key, cancellationToken);
    }

    public IDisposable Subscribe(SignalTopic topic, ISignalSubscriber subscriber)
    {
        Validate(topic);
        ArgumentNullException.ThrowIfNull(subscriber);
        var subscription = new Subscription(this, topic.Name, subscriber);
        bool resync;
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            var current = topics.GetValueOrDefault(topic.Name, []);
            topics = new Dictionary<string, Subscription[]>(topics, StringComparer.Ordinal) { [topic.Name] = [.. current, subscription] };
            count++;
            resync = connected;
            if (listening is null)
            {
                listening = new CancellationTokenSource();
                if (!running)
                {
                    running = true;
                    loop = Task.Run(RunAsync);
                }
            }
        }
        if (resync)
        {
            subscription.Resync();
        }
        return subscription;
    }

    /// <summary>Stops listening; later subscriptions are refused.</summary>
    public async ValueTask DisposeAsync()
    {
        Stop()?.Cancel();
        await loop.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }

    /// <summary>Stops listening without waiting for the listener to close; later subscriptions are refused.</summary>
    public void Dispose() => Stop()?.Cancel();

    private CancellationTokenSource? Stop()
    {
        lock (gate)
        {
            disposed = true;
            topics = new Dictionary<string, Subscription[]>(StringComparer.Ordinal);
            count = 0;
            connected = false;
            var stopping = listening;
            listening = null;
            return stopping;
        }
    }

    private static void Validate(SignalTopic topic)
    {
        if (topic.Name is null)
        {
            throw new ArgumentException("The signal topic is default; construct it with a name.", nameof(topic));
        }
    }

    private static void Validate(SignalTopic topic, string key)
    {
        Validate(topic);
        ArgumentNullException.ThrowIfNull(key);
        if (key.Length > MaximumKeyBytes / 3 && Encoding.UTF8.GetByteCount(key) > MaximumKeyBytes)
        {
            throw new ArgumentException($"A signal key has at most {MaximumKeyBytes} UTF-8 bytes.", nameof(key));
        }
    }

    /// <summary>Listens while subscriptions exist; a subscription made while a listen stops starts the next one.</summary>
    private async Task RunAsync()
    {
        while (true)
        {
            CancellationTokenSource stop;
            lock (gate)
            {
                if (listening is null)
                {
                    running = false;
                    return;
                }
                stop = listening;
            }
            try
            {
                await transport.ListenAsync(Received, () => Connected(stop), stop.Token);
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
            }
            catch (Exception exception)
            {
                LogListenFailed(logger, telemetry.Logged(exception), telemetry.Describe(exception));
                await Task.Delay(Retry, stop.Token).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
            lock (gate)
            {
                if (listening == stop)
                {
                    connected = false;
                }
            }
        }
    }

    private void Connected(CancellationTokenSource stop)
    {
        Dictionary<string, Subscription[]> current;
        lock (gate)
        {
            if (listening != stop)
            {
                return;
            }
            connected = true;
            current = topics;
        }
        foreach (var subscriptions in current.Values)
        {
            foreach (var subscription in subscriptions)
            {
                subscription.Resync();
            }
        }
    }

    private void Received(SignalTopic topic, string key)
    {
        if (Volatile.Read(ref topics).TryGetValue(topic.Name, out var subscriptions))
        {
            foreach (var subscription in subscriptions)
            {
                subscription.Received(key);
            }
        }
    }

    private void Remove(Subscription subscription)
    {
        CancellationTokenSource? stopping = null;
        lock (gate)
        {
            if (!topics.TryGetValue(subscription.Topic, out var current) || Array.IndexOf(current, subscription) < 0)
            {
                return;
            }
            var remaining = current.Where(s => s != subscription).ToArray();
            var changed = new Dictionary<string, Subscription[]>(topics, StringComparer.Ordinal);
            if (remaining.Length == 0)
            {
                changed.Remove(subscription.Topic);
            }
            else
            {
                changed[subscription.Topic] = remaining;
            }
            topics = changed;
            if (--count == 0)
            {
                stopping = listening;
                listening = null;
                connected = false;
            }
        }
        // Cancelling outside the lock: the transport's cancellation callbacks run synchronously.
        stopping?.Cancel();
    }

    private sealed class Subscription(WorkSignals hub, string topic, ISignalSubscriber subscriber) : IDisposable
    {
        private volatile bool disposed;

        public string Topic => topic;

        public void Received(string key)
        {
            if (disposed)
            {
                return;
            }
            try
            {
                subscriber.Received(key);
            }
            catch (Exception exception)
            {
                LogSubscriberFailed(hub.logger, hub.telemetry.Logged(exception), topic, hub.telemetry.Describe(exception));
            }
        }

        public void Resync()
        {
            if (disposed)
            {
                return;
            }
            try
            {
                subscriber.Resync();
            }
            catch (Exception exception)
            {
                LogSubscriberFailed(hub.logger, hub.telemetry.Logged(exception), topic, hub.telemetry.Describe(exception));
            }
        }

        public void Dispose()
        {
            disposed = true;
            hub.Remove(this);
        }
    }

    [LoggerMessage(EventId = 47, EventName = "signals.subscriber_failed", Level = LogLevel.Warning, Message = "A subscriber of signal topic {Topic} failed: {Error}")]
    private static partial void LogSubscriberFailed(ILogger logger, Exception? exception, string topic, string error);

    [LoggerMessage(EventId = 48, EventName = "signals.transport_failed", Level = LogLevel.Warning, Message = "Listening for signals failed; retrying: {Error}")]
    private static partial void LogListenFailed(ILogger logger, Exception? exception, string error);
}
