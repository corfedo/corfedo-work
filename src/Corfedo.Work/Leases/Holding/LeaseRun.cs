using System.Diagnostics;

namespace Corfedo.Work;

/// <summary>One claimed record being run by a handler.</summary>
public sealed class LeaseRun<TKey>
    where TKey : notnull
{
    private const int Running = 0;
    private const int Ended = 1;
    private const int Released = 2;

    private readonly LeaseOperations operations;
    private int state;

    internal LeaseRun(LeaseClaim<TKey> claim, LeaseHold<TKey> hold, LeaseOperations operations)
    {
        Claim = claim;
        Hold = hold;
        this.operations = operations;
    }

    public LeaseClaim<TKey> Claim { get; }

    public LeaseHold<TKey> Hold { get; }

    public Lease<TKey> Lease => Claim.Lease;

    public TKey Key => Claim.Key;

    public string? Partition => Claim.Partition;

    /// <summary>Unique for this claim.</summary>
    public Guid AttemptId => Claim.AttemptId;

    /// <summary>Stable across the attempts of one deliberate execution; deduplicates external effects.</summary>
    public Guid ExecutionId => Claim.ExecutionId;

    /// <summary>Cancelled when the lease is lost, cancellation is requested, the run times out or the process stops.</summary>
    public CancellationToken CancellationToken => Hold.Token;

    /// <summary>Whether the run ended or released its record.</summary>
    public bool Finished => Volatile.Read(ref state) != Running;

    /// <summary>The run's <c>corfedo.work.run</c> span, null while nothing listens.</summary>
    internal Activity? Span { get; set; }

    /// <inheritdoc cref="LeaseOperations.GuardAsync{TKey}"/>
    public async ValueTask GuardAsync(IWorkTransaction transaction, CancellationToken cancellationToken = default)
    {
        using var span = WorkTelemetry.Source.StartActivity("corfedo.work.guard");
        span?.SetTag(WorkTelemetry.SourceTag, Lease.Source.Name);
        try
        {
            await operations.GuardAsync(Lease, transaction, cancellationToken);
        }
        catch (Exception exception)
        {
            WorkTelemetry.Fail(span, exception.GetType().FullName!, null);
            throw;
        }
    }

    /// <summary>Renews in the caller's transaction, so progress written in it commits with the renewal.</summary>
    public ValueTask<RenewResult> RenewAsync(IWorkTransaction transaction, CancellationToken cancellationToken = default) =>
        operations.RenewAsync(Lease, transaction, cancellationToken);

    /// <summary>
    /// Ends the record in the caller's transaction. Whether the end took effect is known only once that transaction
    /// commits, so after the handler returns the dispatcher gives back a record whose end did not commit.
    /// </summary>
    public async ValueTask<EndResult> EndAsync(IWorkTransaction transaction, LeaseCompletion? completion = null, CancellationToken cancellationToken = default)
    {
        var result = await operations.EndAsync(Lease, transaction, completion, cancellationToken);
        Interlocked.CompareExchange(ref state, Ended, Running);
        return result;
    }

    /// <summary>Gives the record back, due at <paramref name="due"/>; counts a failure when <paramref name="failed"/>.</summary>
    public async ValueTask<bool> ReleaseAsync(Due due, bool failed = false, CancellationToken cancellationToken = default)
    {
        var released = await operations.ReleaseAsync(Lease, due, failed, cancellationToken);
        Volatile.Write(ref state, Released);
        return released;
    }

    /// <summary>
    /// Takes over giving the record back; true unless the run released it itself or another caller took over.
    /// </summary>
    internal bool TryFinish() => Interlocked.Exchange(ref state, Released) != Released;
}
