using System.Data.Common;

namespace Corfedo.Work;

/// <summary>
/// Signals of one application in a process. A signal is a topic and a key and carries no data: it says that the state
/// the key names may have changed. Signals sent in a transaction are delivered after it commits and never after a
/// rollback, to every subscriber of the topic in every process of the application. They may be lost or merged and
/// have no order, so subscribers read the state again on a signal and still poll.
/// </summary>
public interface IWorkSignals
{
    /// <summary>Sends a signal on commit of <paramref name="transaction"/>.</summary>
    /// <exception cref="ArgumentException">The topic is <c>default</c> or the key is longer than <see cref="WorkSignals.MaximumKeyBytes"/>.</exception>
    ValueTask SendAsync(WorkTransaction transaction, SignalTopic topic, string key, CancellationToken cancellationToken);

    /// <summary>Sends a signal at once.</summary>
    /// <exception cref="ArgumentException">The topic is <c>default</c> or the key is longer than <see cref="WorkSignals.MaximumKeyBytes"/>.</exception>
    ValueTask SendAsync(SignalTopic topic, string key, CancellationToken cancellationToken);

    /// <summary>
    /// Subscribes to a topic until the subscription is disposed. The subscriber gets <see cref="ISignalSubscriber.Resync"/>
    /// once listening covers it and after every reconnection, and reads the state it waits for after the first one.
    /// </summary>
    /// <exception cref="ArgumentException">The topic is <c>default</c>.</exception>
    IDisposable Subscribe(SignalTopic topic, ISignalSubscriber subscriber);
}

/// <summary>
/// A local callback for one topic. Callbacks run on the listener and must not block: they mark work and return. A
/// callback that throws is logged, and the other subscribers still get the signal.
/// </summary>
public interface ISignalSubscriber
{
    /// <summary>The state named by <paramref name="key"/> may have changed.</summary>
    void Received(string key);

    /// <summary>Signals may have been missed: read all the state this subscriber waits for.</summary>
    void Resync();
}
