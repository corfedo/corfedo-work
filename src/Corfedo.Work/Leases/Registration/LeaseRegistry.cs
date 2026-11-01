using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work;

/// <summary>The lease sources this process registers, with their hooks and handlers.</summary>
public sealed class LeaseRegistry
{
    private readonly Dictionary<string, LeaseSourceRegistration> sources;
    private readonly IServiceProvider services;

    public LeaseRegistry(IEnumerable<LeaseSourceRegistration> registrations, IServiceProvider services)
    {
        this.services = services;
        sources = new Dictionary<string, LeaseSourceRegistration>(StringComparer.Ordinal);
        foreach (var registration in registrations)
        {
            if (!sources.TryAdd(registration.Name, registration))
            {
                throw new InvalidOperationException($"Lease source {registration.Name} is registered more than once.");
            }
        }
    }

    public IEnumerable<LeaseSourceRegistration> Sources => sources.Values;

    /// <summary>The callbacks of a registered source; none for a source this process does not register.</summary>
    public LeaseCallbacks<TKey> Callbacks<TKey>(LeaseSource<TKey> source)
        where TKey : notnull
    {
        if (!sources.TryGetValue(source.Name, out var registration))
        {
            return LeaseCallbacks<TKey>.None;
        }
        if (registration is not LeaseSourceRegistration<TKey> typed || !ReferenceEquals(typed.Source, source))
        {
            throw new InvalidOperationException($"Lease source {source.Name} is registered with a different definition.");
        }
        return typed.Callbacks(services);
    }
}

/// <summary>One registered lease source.</summary>
public abstract class LeaseSourceRegistration
{
    private protected LeaseSourceRegistration(string name) => Name = name;

    public string Name { get; }

    /// <summary>
    /// Whether this process runs expiry settlement for the source: with the expiry handler when the source settles
    /// expiry, and on every registering process when only store-enforced limits need it.
    /// </summary>
    public abstract bool Settles { get; }

    public abstract bool HasHandler { get; }

    /// <summary>The dispatch pool the source runs in, or null when it has a capacity of its own.</summary>
    public abstract string? Pool { get; }

    /// <summary>Records of the source this process holds at most at once, for a source outside a pool.</summary>
    public abstract int Capacity { get; }

    /// <summary>The source's priority class in its pool: free capacity goes to higher classes first.</summary>
    public abstract int Priority { get; }

    public abstract ValueTask VerifyAsync(IWorkStore store, CancellationToken cancellationToken);

    /// <summary>Creates the dispatcher of a source with a handler and a capacity of its own.</summary>
    public ILeaseDispatcher CreateDispatcher(LeaseDispatchContext context) => new DispatchPool(Name, Capacity, [this], context);

    /// <summary>
    /// Creates the runs of a source with a handler in a dispatcher, which <paramref name="finished"/> wakes after every
    /// run; <paramref name="stopping"/> is cancelled when the dispatcher stops claiming.
    /// </summary>
    public abstract IDispatchLane CreateLane(LeaseDispatchContext context, Action finished, CancellationToken stopping);

    /// <summary>Creates the expiry processor, for a source that settles.</summary>
    public abstract IExpiryProcessor CreateExpiryProcessor(LeaseDispatchContext context);
}

/// <summary>A registered lease source with its hook, expiry handler, and handler with its local capacity.</summary>
public sealed class LeaseSourceRegistration<TKey> : LeaseSourceRegistration
    where TKey : notnull
{
    private readonly TimeSpan? timeout;
    private LeaseCallbacks<TKey>? callbacks;

    private LeaseSource<TKey> source;

    internal LeaseSourceRegistration(
        LeaseSource<TKey> source,
        Func<IServiceProvider, ILeaseClaimHook<TKey>>? claimHook,
        Func<IServiceProvider, ILeaseExpiryHandler<TKey>>? expiryHandler,
        Func<IServiceProvider, ILeaseHandler<TKey>>? handler,
        int capacity,
        TimeSpan? timeout,
        string? pool = null)
        : base(source.Name)
    {
        Pool = pool;
        this.source = source;
        ClaimHook = claimHook;
        ExpiryHandler = expiryHandler;
        Handler = handler;
        Capacity = capacity;
        this.timeout = timeout;
    }

    /// <summary>
    /// The source the process claims from. On job sources it is rebuilt when a newer definition revision is stored;
    /// leases keep the source they were claimed from.
    /// </summary>
    public LeaseSource<TKey> Source => Volatile.Read(ref source);

    /// <summary>
    /// Replaces the source with one built from the newest stored definition after a claim found it changed; null for
    /// sources that do not change.
    /// </summary>
    internal Func<IWorkStore, CancellationToken, ValueTask>? Refresh { get; init; }

    /// <summary>Replaces the source the process claims from with one of the same name.</summary>
    internal void Replace(LeaseSource<TKey> replacement)
    {
        if (replacement.Name != Name)
        {
            throw new InvalidOperationException($"Lease source {Name} cannot be replaced by {replacement.Name}.");
        }
        Volatile.Write(ref source, replacement);
    }

    internal Func<IServiceProvider, ILeaseClaimHook<TKey>>? ClaimHook { get; }

    internal Func<IServiceProvider, ILeaseExpiryHandler<TKey>>? ExpiryHandler { get; }

    internal Func<IServiceProvider, ILeaseHandler<TKey>>? Handler { get; }

    public override int Capacity { get; }

    public override string? Pool { get; }

    /// <summary>
    /// How long one run may take before its token is cancelled; null for no limit. On job sources without a timeout of
    /// their own registration it is the stored definition's timeout.
    /// </summary>
    public TimeSpan? Timeout => timeout ?? Source.Job?.Policy.Timeout;

    public override int Priority => (int)(Source.Job?.Policy.Priority ?? global::Corfedo.Work.Priority.Normal);

    public override bool Settles => Source.Policy.SettleExpiry ? ExpiryHandler is not null : Source.Settles;

    public override bool HasHandler => Handler is not null;

    /// <summary>Callbacks that resolve the hook and the expiry handler in a new scope for every call.</summary>
    internal LeaseCallbacks<TKey> Callbacks(IServiceProvider services) => callbacks ??= new LeaseCallbacks<TKey>(
        ClaimHook is { } hook
            ? async (claim, transaction, cancellationToken) =>
            {
                await using var scope = services.CreateAsyncScope();
                await hook(scope.ServiceProvider).ClaimedAsync(claim, transaction, cancellationToken);
            }
            : null,
        ExpiryHandler is { } expiry
            ? async (expired, transaction, cancellationToken) =>
            {
                await using var scope = services.CreateAsyncScope();
                await expiry(scope.ServiceProvider).ExpiredAsync(expired, transaction, cancellationToken);
            }
            : null);

    /// <summary>Job sources live on the framework's own table, which its schema scripts create.</summary>
    public override ValueTask VerifyAsync(IWorkStore store, CancellationToken cancellationToken) =>
        Source.Job is null ? store.Leases.VerifyAsync(Source, cancellationToken) : ValueTask.CompletedTask;

    public override IDispatchLane CreateLane(LeaseDispatchContext context, Action finished, CancellationToken stopping) =>
        Handler is null
            ? throw new InvalidOperationException($"Lease source {Name} has no handler in this process.")
            : new LeaseDispatcher<TKey>(this, Callbacks(context.Services), context, finished, stopping);

    public override IExpiryProcessor CreateExpiryProcessor(LeaseDispatchContext context) =>
        new ExpiryProcessor<TKey>(this, Callbacks(context.Services), context);
}
