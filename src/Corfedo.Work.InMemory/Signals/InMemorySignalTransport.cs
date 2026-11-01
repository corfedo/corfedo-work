using System.Data.Common;
using Corfedo.Work.InMemory.Database;

namespace Corfedo.Work.InMemory.Signals;

/// <summary>
/// In-process signals of one application, delivered when the transaction that sent them commits, or at once without
/// one. A rollback, also to a savepoint taken before the send, discards them. Listening never disconnects.
/// </summary>
internal sealed class InMemorySignalTransport(InMemoryWorkDatabase database, ApplicationState application) : ISignalTransport
{
    public ValueTask SendAsync(IWorkTransaction? transaction, SignalTopic topic, string key, CancellationToken cancellationToken)
    {
        if (transaction is null)
        {
            application.Deliver(topic, key);
        }
        else
        {
            InMemoryTransaction.From(transaction, database).Signal(application, topic, key);
        }
        return ValueTask.CompletedTask;
    }

    public async Task ListenAsync(Action<SignalTopic, string> received, Action connected, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(received);
        ArgumentNullException.ThrowIfNull(connected);
        application.Listen(received);
        try
        {
            connected();
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }
        finally
        {
            application.Unlisten(received);
        }
    }
}
