using System.Data.Common;

namespace Corfedo.Work;

/// <summary>
/// A provider's transport for signals of one application: sends that are delivered when the sending transaction
/// commits, and a listener for them. Applications use it through <c>IWorkSignals</c>, which listens once per process.
/// </summary>
public interface ISignalTransport
{
    /// <summary>
    /// Sends a signal on commit of <paramref name="transaction"/> and never after its rollback, or at once without one.
    /// </summary>
    ValueTask SendAsync(IWorkTransaction? transaction, SignalTopic topic, string key, CancellationToken cancellationToken);

    /// <summary>
    /// Listens until cancelled, calling <paramref name="received"/> for every signal of the application and
    /// <paramref name="connected"/> whenever listening starts or resumes, since signals sent while disconnected are lost.
    /// </summary>
    Task ListenAsync(Action<SignalTopic, string> received, Action connected, CancellationToken cancellationToken);
}
