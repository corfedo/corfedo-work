namespace Corfedo.Work;

public sealed partial class JobOperations
{
    /// <summary>
    /// Requests a chain in the caller's transaction (K1): every step and group is created atomically, the root due now
    /// and every other step waiting for its parent. Every step's definition must be registered in this process.
    /// </summary>
    public ValueTask<ChainId> RequestChainAsync(IWorkTransaction transaction, Action<ChainBuilder> chain, CancellationToken cancellationToken = default) =>
        RequestChainAsync(transaction, chain, Schedule.Now, cancellationToken);

    /// <summary>Requests a chain whose root is due by the one-shot <paramref name="schedule"/>, which may name a calendar.</summary>
    public async ValueTask<ChainId> RequestChainAsync(IWorkTransaction transaction, Action<ChainBuilder> chain, Schedule schedule, CancellationToken cancellationToken = default)
    {
        var data = await BuildChainAsync(store, registry, chain, schedule, cancellationToken);
        await store.Chains.RequestChainAsync(transaction, data, cancellationToken);
        return data.Id;
    }

    /// <summary>The chain with its steps and groups (K8), or null when there is none.</summary>
    public ValueTask<ChainStatus?> GetChainAsync(ChainId chain, CancellationToken cancellationToken = default) =>
        store.Chains.GetChainAsync(chain, cancellationToken);

    /// <summary>
    /// Reruns a settled chain from <paramref name="step"/> (K6): the step and everything that depends on it run again
    /// under a new generation; completed ancestors and sibling branches keep their outcomes.
    /// </summary>
    public ValueTask<RetryFromOutcome> RetryFromAsync(ChainId chain, JobId step, CancellationToken cancellationToken = default) =>
        store.Chains.RetryFromAsync(chain, step.Value, cancellationToken);

    /// <summary>Reruns a settled chain from a group: all its branches, its continuations and their dependants run again.</summary>
    public ValueTask<RetryFromOutcome> RetryFromGroupAsync(ChainId chain, Guid group, CancellationToken cancellationToken = default) =>
        store.Chains.RetryFromAsync(chain, group, cancellationToken);

    /// <summary>
    /// Cancels the chain's outstanding steps in the caller's transaction: waiting and due ones end cancelled, running
    /// ones through their lease, and their children follow their conditions. Refused when a step belongs to a definition
    /// this process does not register.
    /// </summary>
    public async ValueTask<int> CancelChainAsync(IWorkTransaction transaction, ChainId chain, CancellationToken cancellationToken = default) =>
        await store.Chains.CancelChainAsync(transaction, chain, await DefinitionsAsync(cancellationToken), cancellationToken);

    /// <summary>
    /// Deletes a settled chain as a unit (K8). One with unsettled steps is refused, or with
    /// <see cref="ChainDelete.Cascade"/> its steps are cancelled and it is deleted once they settled.
    /// </summary>
    public async ValueTask<ChainDeleteOutcome> DeleteChainAsync(ChainId chain, ChainDelete mode = ChainDelete.Refuse, CancellationToken cancellationToken = default) =>
        await store.Chains.DeleteChainAsync(chain, mode, await DefinitionsAsync(cancellationToken), cancellationToken);

    /// <summary>Builds a chain and stores the definitions of its steps, each of which this process must register.</summary>
    internal static async ValueTask<ChainData> BuildChainAsync(IWorkStore store, JobRegistry registry, Action<ChainBuilder> chain, Schedule schedule, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(chain);
        ArgumentNullException.ThrowIfNull(schedule);
        var builder = new ChainBuilder(registry, schedule);
        chain(builder);
        if (builder.Steps.Count == 0)
        {
            throw new ArgumentException("A chain starts with a root step; call StartWith.", nameof(chain));
        }
        await StoreDefinitionsAsync(store, registry, builder.Definitions, cancellationToken);
        return new ChainData(ChainId.New(), builder.Steps, builder.Groups);
    }

    /// <summary>Checks that this process registers every definition and stores them.</summary>
    internal static async ValueTask StoreDefinitionsAsync(IWorkStore store, JobRegistry registry, IEnumerable<JobDefinition> definitions, CancellationToken cancellationToken)
    {
        foreach (var definition in definitions.Distinct())
        {
            await registry.Find(definition).SourceAsync(store, cancellationToken);
        }
    }

    /// <summary>Resolves each registered definition's source and outcome callback by key.</summary>
    private async ValueTask<Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?>> DefinitionsAsync(CancellationToken cancellationToken)
    {
        var sources = new Dictionary<string, (LeaseSource<JobId>, JobEndedCallback?)>(StringComparer.Ordinal);
        foreach (var registration in registry.Definitions)
        {
            sources[registration.Definition.Key] = (await registration.SourceAsync(store, cancellationToken), registry.Ended(registration));
        }
        return key => sources.TryGetValue(key, out var found) ? found : null;
    }
}
