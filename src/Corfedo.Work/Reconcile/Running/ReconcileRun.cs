namespace Corfedo.Work;

/// <summary>Converges claimed records of a reconciliation source.</summary>
public interface IReconciler<TKey>
    where TKey : notnull
{
    /// <summary>
    /// Converges the record to the revision the run claimed. Returns the result for the dispatcher to apply in its own
    /// guarded transaction, or <see cref="ReconcileRun{TKey}.Completed"/> after applying it with
    /// <see cref="ReconcileRun{TKey}.CompleteAsync"/> in the domain's transaction. A thrown exception is
    /// <c>Failed("handler_failed")</c>.
    /// </summary>
    ValueTask<ReconcileDisposition> ReconcileAsync(ReconcileRun<TKey> run, CancellationToken cancellationToken);
}

/// <summary>
/// What a reconciler returns: a <see cref="ReconcileResult"/>, which converts implicitly, for the dispatcher to apply,
/// or <see cref="ReconcileRun{TKey}.Completed"/> when the reconciler applied its result itself.
/// </summary>
public sealed class ReconcileDisposition
{
    private ReconcileDisposition(ReconcileResult? result) => Result = result;

    internal static ReconcileDisposition Completed { get; } = new(null);

    /// <summary>The result for the dispatcher to apply; null when the reconciler applied one itself.</summary>
    public ReconcileResult? Result { get; }

    public static implicit operator ReconcileDisposition(ReconcileResult result) => FromResult(result);

    public static ReconcileDisposition FromResult(ReconcileResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return new(result);
    }
}

/// <summary>One claimed reconciliation record being converged.</summary>
public sealed class ReconcileRun<TKey>
    where TKey : notnull
{
    private readonly ReconcileOperations operations;

    internal ReconcileRun(LeaseRun<TKey> lease, ReconcileOperations operations)
    {
        Lease = lease;
        this.operations = operations;
        Attempt = lease.Claim.Reconcile ?? throw new InvalidOperationException($"The claim of {lease.Lease.Source.Name} record {lease.Lease.Source.Key.Format(lease.Key)} has no revision.");
    }

    /// <summary>The lease run behind this run; reconcilers give the record back with a result, not through it.</summary>
    internal LeaseRun<TKey> Lease { get; }

    public TKey Key => Lease.Key;

    public string? Partition => Lease.Partition;

    /// <summary>The requested revision this run converges.</summary>
    public long Revision => Attempt.Revision;

    /// <summary>The source generation this run started from.</summary>
    public long Generation => Attempt.Generation;

    /// <summary>Unique for this claim.</summary>
    public Guid AttemptId => Lease.AttemptId;

    /// <summary>Derived from the record key, the revision and the generation; retries of the same revision keep it.</summary>
    public Guid ExecutionId => Lease.ExecutionId;

    /// <summary>The holder this run's lease belongs to.</summary>
    public OwnerId Owner => Lease.Lease.Owner;

    /// <summary>Why the record was claimed.</summary>
    public ClaimReason Reason => Lease.Claim.Reason;

    /// <summary>Cancelled when the lease is lost or superseded, cancellation is requested, the run times out or the process stops.</summary>
    public CancellationToken CancellationToken => Lease.CancellationToken;

    /// <summary>Why <see cref="CancellationToken"/> was cancelled; null while the run may continue.</summary>
    public LeaseStopReason? StopReason => Lease.Hold.StopReason;

    /// <summary>What a reconciler returns after <see cref="CompleteAsync"/>, so the dispatcher applies nothing more.</summary>
    public ReconcileDisposition Completed => ReconcileDisposition.Completed;

    private ReconcileAttempt Attempt { get; }

    /// <inheritdoc cref="LeaseOperations.GuardAsync{TKey}"/>
    public ValueTask GuardAsync(WorkTransaction transaction, CancellationToken cancellationToken = default) =>
        Lease.GuardAsync(transaction, cancellationToken);

    /// <summary>Renews in the caller's transaction, so progress written in it commits with the renewal.</summary>
    public ValueTask<RenewResult> RenewAsync(WorkTransaction transaction, CancellationToken cancellationToken = default) =>
        Lease.RenewAsync(transaction, cancellationToken);

    /// <summary>
    /// Applies <paramref name="result"/> in the domain's transaction. If that transaction does not commit, the record
    /// stays held and the run can complete again; a run that returns <see cref="Completed"/> without a committed
    /// completion is applied as failed.
    /// </summary>
    /// <exception cref="LeaseLostException">The lease is no longer valid or was superseded; the transaction must not commit.</exception>
    public ValueTask CompleteAsync(WorkTransaction transaction, ReconcileResult result, CancellationToken cancellationToken = default) =>
        operations.CompleteAsync(Lease.Lease, transaction, result, cancellationToken);
}
