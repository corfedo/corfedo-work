namespace Corfedo.Work;

/// <summary>
/// Lease operations for application code and remote-holder APIs. Claims run the source's registered claim hook and
/// settle expired leases with its expiry handler. Claims default to this process's owner; a remote holder's API
/// passes the holder's own owner id. A job's lifecycle belongs to the job layer: on job sources claims, releases,
/// ends, schedules and cancellation requests are refused, including through parsed lease tokens; renewals and guards
/// remain available. The due time of a reconciliation record belongs to the reconciliation layer: on reconciliation
/// sources releases, ends and schedules are refused, and a holder gives the record back with a result through
/// <see cref="ReconcileOperations.CompleteAsync{TKey}(Lease{TKey}, ReconcileResult, CancellationToken)"/>.
/// </summary>
public sealed partial class LeaseOperations(IWorkStore store, WorkProcess process, LeaseRegistry registry)
{
    private ILeaseStore Store => store.Leases;

    /// <summary>The owner id of this process.</summary>
    public OwnerId Owner => process.Owner;

    public async ValueTask<IReadOnlyList<LeaseClaim<TKey>>> ClaimDueAsync<TKey>(LeaseSource<TKey> source, int limit, CancellationToken cancellationToken = default, OwnerId? owner = null)
        where TKey : notnull => (await Store.ClaimDueAsync(NotJob(source), registry.Callbacks(source), owner ?? Owner, limit, cancellationToken)).Claims;

    public ValueTask<LeaseClaim<TKey>?> ClaimAsync<TKey>(LeaseSource<TKey> source, TKey key, CancellationToken cancellationToken = default, OwnerId? owner = null)
        where TKey : notnull => Store.ClaimAsync(NotJob(source), registry.Callbacks(source), owner ?? Owner, key, cancellationToken);

    /// <summary>
    /// Inside the caller's transaction, claims up to <paramref name="limit"/> of the candidates that are claimable.
    /// The caller rechecks domain eligibility and reserves resources in the same transaction.
    /// </summary>
    public ValueTask<IReadOnlyList<LeaseClaim<TKey>>> ClaimAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, IReadOnlyList<TKey> candidates, int limit, CancellationToken cancellationToken = default, OwnerId? owner = null)
        where TKey : notnull => Store.ClaimAsync(transaction, NotJob(source), registry.Callbacks(source), owner ?? Owner, candidates, limit, cancellationToken);

    public ValueTask<RenewResult> RenewAsync<TKey>(Lease<TKey> lease, CancellationToken cancellationToken = default)
        where TKey : notnull => Store.RenewAsync(lease, cancellationToken);

    /// <summary>Renews inside the caller's transaction, so progress written in it commits with the renewal.</summary>
    public ValueTask<RenewResult> RenewAsync<TKey>(Lease<TKey> lease, WorkTransaction transaction, CancellationToken cancellationToken = default)
        where TKey : notnull => Store.RenewAsync(transaction, lease, false, cancellationToken);

    /// <summary>
    /// Requires a valid lease inside the caller's transaction, extends it and holds the record's lock until the
    /// transaction ends. Every transaction that commits a holder's output calls it.
    /// </summary>
    /// <exception cref="LeaseLostException">The lease is no longer valid; the transaction must not commit.</exception>
    public ValueTask GuardAsync<TKey>(Lease<TKey> lease, WorkTransaction transaction, CancellationToken cancellationToken = default)
        where TKey : notnull => GuardAsync(lease, transaction, false, cancellationToken);

    /// <summary>
    /// Guards the transaction as <see cref="GuardAsync{TKey}(Lease{TKey}, WorkTransaction, CancellationToken)"/> does;
    /// with <paramref name="progressed"/> set, it also marks that the transaction commits progress, which resets the
    /// record's <c>Takeovers</c> when it commits: the counter, which <c>MaximumTakeovers</c> limits, counts takeovers
    /// without progress.
    /// </summary>
    /// <exception cref="LeaseLostException">The lease is no longer valid; the transaction must not commit.</exception>
    public async ValueTask GuardAsync<TKey>(Lease<TKey> lease, WorkTransaction transaction, bool progressed, CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        if (!(await Store.RenewAsync(transaction, lease, progressed, cancellationToken)).Valid)
        {
            throw new LeaseLostException($"The lease on {lease.Source.Name} record {lease.Source.Key.Format(lease.Key)} at epoch {lease.Epoch} is no longer valid.");
        }
    }

    /// <summary>Gives the record back, due at <paramref name="due"/>; false if the lease was already lost.</summary>
    public ValueTask<bool> ReleaseAsync<TKey>(Lease<TKey> lease, Due due, bool failed = false, CancellationToken cancellationToken = default)
        where TKey : notnull => Store.ReleaseAsync(NotOwned(lease), due, failed, cancellationToken);

    /// <inheritdoc cref="ILeaseStore.EndAsync{TKey}"/>
    public ValueTask<EndResult> EndAsync<TKey>(Lease<TKey> lease, WorkTransaction transaction, LeaseCompletion? completion = null, CancellationToken cancellationToken = default)
        where TKey : notnull => Store.EndAsync(transaction, NotOwned(lease), completion, cancellationToken);

    /// <summary>
    /// Sets the due time in the caller's transaction and wakes claimers on commit. A new <paramref name="execution"/>
    /// marks a deliberate new execution; without one the current execution continues.
    /// </summary>
    public ValueTask<bool> ScheduleAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, TKey key, Due due, Guid? execution = null, CancellationToken cancellationToken = default)
        where TKey : notnull => Store.ScheduleAsync(transaction, NotOwned(source), key, due, execution, cancellationToken);

    public ValueTask<bool> RequestCancelAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, TKey key, CancellationToken cancellationToken = default)
        where TKey : notnull => Store.RequestCancelAsync(transaction, NotJob(source), key, cancellationToken);

    public ValueTask<bool> ClearCancelAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, TKey key, CancellationToken cancellationToken = default)
        where TKey : notnull => Store.ClearCancelAsync(transaction, NotJob(source), key, cancellationToken);

    private static LeaseSource<TKey> NotJob<TKey>(LeaseSource<TKey> source)
        where TKey : notnull =>
        source.Job is { } job
            ? throw new ArgumentException($"Lease source {source.Name} holds jobs of definition {job.Definition}; jobs are requested, cancelled and completed through JobOperations and their handlers.", nameof(source))
            : source;

    private static Lease<TKey> NotJob<TKey>(Lease<TKey> lease)
        where TKey : notnull
    {
        NotJob(lease.Source);
        return lease;
    }

    /// <summary>Refuses sources whose due times a higher layer owns: jobs and reconciliation records.</summary>
    private static LeaseSource<TKey> NotOwned<TKey>(LeaseSource<TKey> source)
        where TKey : notnull =>
        NotJob(source).Reconcile is null
            ? source
            : throw new ArgumentException($"Lease source {source.Name} holds reconciliation records; they are made due by requests and invalidations and given back with a result through ReconcileOperations.", nameof(source));

    private static Lease<TKey> NotOwned<TKey>(Lease<TKey> lease)
        where TKey : notnull
    {
        NotOwned(lease.Source);
        return lease;
    }
}
