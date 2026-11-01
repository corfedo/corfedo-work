namespace Corfedo.Work;

/// <summary>When a host starts dispatching, see <see cref="WorkBuilder.StartMode"/>.</summary>
public enum StartMode
{
    /// <summary>Dispatch starts with the host.</summary>
    Automatic,

    /// <summary>
    /// The host's start applies the schema and stores definitions, calendars and recurrences; registering the process,
    /// dispatching and background loops wait for <see cref="IWorkHost.StartAsync"/>.
    /// </summary>
    Manual,
}

/// <summary>Starts dispatching in a host registered with <see cref="StartMode.Manual"/>.</summary>
public interface IWorkHost
{
    /// <summary>
    /// Registers the process and starts dispatchers and background loops; a later call does nothing.
    /// </summary>
    /// <exception cref="InvalidOperationException">The host was not started, has stopped, or starts automatically.</exception>
    Task StartAsync(CancellationToken cancellationToken = default);
}
