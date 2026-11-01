namespace Corfedo.Work;

/// <summary>
/// Prepares a layer once the store's schema is applied, before sources are verified and dispatch starts; for
/// example, job definitions are stored by revision here.
/// </summary>
public interface IWorkInitializer
{
    ValueTask InitializeAsync(IWorkStore store, CancellationToken cancellationToken);
}

/// <summary>A background loop of a layer that runs while the host dispatches, outside any pool's capacity.</summary>
public interface IWorkBackground
{
    /// <summary>Starts after the dispatchers; a loop woken by signals subscribes through <see cref="LeaseDispatchContext.Signals"/>.</summary>
    void Start(LeaseDispatchContext context);

    /// <summary>Stops before the dispatchers, disposing its subscriptions.</summary>
    Task StopAsync();
}
