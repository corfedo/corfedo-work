namespace Corfedo.Work;

/// <summary>
/// Chain operations of one store. A chain's steps are jobs of their definitions; every terminal transition of a step
/// decides its children in the same transaction: those whose condition holds become due, the others are recorded to be
/// skipped and ended by their definition's maintenance. A skipped root skips every step.
/// </summary>
public interface IChainStore
{
    /// <summary>
    /// In the caller's transaction, creates the chain with every step and group: the root is due by its request's
    /// schedule, the other steps wait for their parents. Every definition must be stored.
    /// </summary>
    ValueTask RequestChainAsync(WorkTransaction transaction, ChainData chain, CancellationToken cancellationToken);

    /// <summary>
    /// In the store's own transaction, guarded by the lease of an occurrence that owns a recurring chain: creates the
    /// chain with its root due now and gives the lease up, leaving the occurrence outstanding until the chain settles,
    /// when its definition's maintenance ends it with the chain's outcome. False, creating nothing, when the lease is no
    /// longer valid or a cancellation of the occurrence was requested.
    /// </summary>
    ValueTask<bool> StartOwnedChainAsync(Lease<JobId> lease, ChainData chain, CancellationToken cancellationToken);

    /// <summary>
    /// In the caller's transaction, guarded by the lease of a running chain step: adds the groups and steps that follow
    /// it, built as for a request. A group or step without an enclosing group belongs to the step's own enclosing group.
    /// </summary>
    /// <exception cref="InvalidOperationException">The job is not a chain step.</exception>
    /// <exception cref="LeaseLostException">The lease is no longer valid; the transaction must not commit.</exception>
    ValueTask AddGroupAsync(WorkTransaction transaction, Lease<JobId> lease, IReadOnlyList<ChainStepData> steps, IReadOnlyList<ChainGroupData> groups, CancellationToken cancellationToken);

    /// <summary>The latest ended execution of the step's parent step, or null when the parent is a group or of another definition.</summary>
    ValueTask<StoredJobOutcome?> GetParentOutcomeAsync(JobId step, string definition, CancellationToken cancellationToken);

    /// <summary>
    /// For a step that follows a group, the latest ended executions of the steps of the definition that end a branch of
    /// the group: those no step of the group follows.
    /// </summary>
    ValueTask<IReadOnlyList<StoredJobOutcome>> GetGroupOutcomesAsync(JobId step, string definition, CancellationToken cancellationToken);

    /// <summary>The chain with its steps and groups, or null when there is none.</summary>
    ValueTask<ChainStatus?> GetChainAsync(ChainId chain, CancellationToken cancellationToken);

    /// <summary>
    /// In the store's own transaction, reruns a settled chain from a step or group: the generation increments, the node
    /// and everything that depends on it is reset under the new generation with new execution ids, the groups enclosing
    /// it are evaluated again from their settled branches, and the node becomes due. Concurrent retries of one chain
    /// serialize on the chain, so only one finds it settled.
    /// </summary>
    ValueTask<RetryFromOutcome> RetryFromAsync(ChainId chain, Guid node, CancellationToken cancellationToken);

    /// <summary>
    /// In the caller's transaction, cancels the chain's outstanding steps in id order, as subject cancellation does.
    /// <paramref name="definitions"/> resolves each definition's source and outcome callback by key.
    /// </summary>
    /// <exception cref="InvalidOperationException">A step belongs to a definition <paramref name="definitions"/> does not resolve.</exception>
    ValueTask<int> CancelChainAsync(WorkTransaction transaction, ChainId chain, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken);

    /// <summary>
    /// In the store's own transaction, deletes a settled chain as a unit. One with unsettled steps is refused, or with
    /// <see cref="ChainDelete.Cascade"/> its steps are cancelled and the chain is deleted when it settles.
    /// </summary>
    ValueTask<ChainDeleteOutcome> DeleteChainAsync(ChainId chain, ChainDelete mode, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken);
}
