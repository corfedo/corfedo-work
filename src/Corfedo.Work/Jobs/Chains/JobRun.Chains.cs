namespace Corfedo.Work;

public partial class JobRun
{
    /// <summary>
    /// Adds a group following this chain step (K4), in the transaction that completes the step: its branches run once
    /// the step completed, and <paramref name="after"/> adds the continuations that run once every branch settled. A
    /// group without branches settles as succeeded when the step does. Every step's definition must be registered in
    /// this process.
    /// </summary>
    /// <exception cref="InvalidOperationException">The job is not a chain step.</exception>
    /// <exception cref="LeaseLostException">The lease is no longer valid; the transaction must not commit.</exception>
    public async ValueTask AddGroupAsync(IWorkTransaction transaction, Action<ChainGroupBuilder> branches, Action<ChainGroup>? after = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(branches);
        var builder = new ChainBuilder(Registry, Schedule.Now);
        var group = new ChainStep(builder, Id, null).ThenGroup(branches);
        after?.Invoke(group);
        await JobOperations.StoreDefinitionsAsync(Work, Registry, builder.Definitions, cancellationToken);
        await Work.Chains.AddGroupAsync(transaction, Lease.Lease, builder.Steps, builder.Groups, cancellationToken);
    }

    /// <summary>
    /// The latest ended execution of this chain step's parent with its result (K3), decoded with the parent
    /// definition's codec; null when the parent is a group or a step of another definition.
    /// </summary>
    public async ValueTask<JobOutcomeStatus<TResult>?> GetParentResultAsync<TArgs, TResult>(JobDefinition<TArgs, TResult> definition, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        return await Work.Chains.GetParentOutcomeAsync(Id, definition.Key, cancellationToken) is { } stored ? JobOperations.Typed(definition.Key, Registry.Results(definition), stored) : null;
    }

    /// <summary>
    /// For a continuation of a group, the results of the steps of <paramref name="definition"/> that end the group's
    /// branches (K3), decoded with the definition's codec.
    /// </summary>
    public async ValueTask<IReadOnlyList<JobOutcomeStatus<TResult>>> GetGroupResultsAsync<TArgs, TResult>(JobDefinition<TArgs, TResult> definition, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        var results = Registry.Results(definition);
        return [.. (await Work.Chains.GetGroupOutcomesAsync(Id, definition.Key, cancellationToken)).Select(stored => JobOperations.Typed(definition.Key, results, stored))];
    }

    /// <summary>Creates the chain an occurrence owns and leaves the occurrence waiting for it; false when the lease was lost or the occurrence's cancellation was requested.</summary>
    internal async ValueTask<bool> StartOwnedChainAsync(Action<ChainBuilder> chain, CancellationToken cancellationToken)
    {
        var data = await JobOperations.BuildChainAsync(Work, Registry, chain, Schedule.Now, cancellationToken);
        return await Work.Chains.StartOwnedChainAsync(Lease.Lease, data, cancellationToken);
    }
}
