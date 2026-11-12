namespace Corfedo.Work;

/// <summary>
/// The lease and reconciliation fields of one record, and its source's generation, as a single read saw them. Times
/// are the store's: compare them with <see cref="LeaseRecord.ReadAt"/>.
/// </summary>
/// <param name="Lease">The record's lease fields.</param>
/// <param name="RequestedRevision">Incremented by every request.</param>
/// <param name="ClaimedRevision">The requested revision the current or last run started from; null before the first run.</param>
/// <param name="ConvergedRevision">The latest revision a run converged.</param>
/// <param name="FirstRequestedAt">When the first request not yet claimed arrived; a claim clears it.</param>
/// <param name="NotBefore">The backoff floor after a failed run; null without one.</param>
/// <param name="AttemptedGeneration">The source generation the latest run started from.</param>
/// <param name="ConvergedGeneration">The source generation the latest converged run saw.</param>
/// <param name="Generation">The source's current generation; 0 before its first invalidation.</param>
public sealed record ReconcileRecord(
    LeaseRecord Lease,
    long RequestedRevision,
    long? ClaimedRevision,
    long ConvergedRevision,
    DateTimeOffset? FirstRequestedAt,
    DateTimeOffset? NotBefore,
    long AttemptedGeneration,
    long ConvergedGeneration,
    long Generation);
