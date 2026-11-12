namespace Corfedo.Work;

/// <summary>
/// The lease fields of one record as a single read saw them. Times are the store's: compare them with
/// <see cref="ReadAt"/>, the store's clock at the read, not with the local clock.
/// </summary>
/// <param name="ReadAt">The store's clock when the record was read.</param>
/// <param name="DueAt">When the record is due; null when no run is wanted.</param>
/// <param name="Owner">The owner of the latest lease; null before the first claim.</param>
/// <param name="Epoch">The epoch of the latest lease; 0 before the first claim.</param>
/// <param name="ExpiresAt">When the current lease ends unless renewed; null when not held: releasing, ending and superseding clear it.</param>
/// <param name="Takeovers">Consecutive takeovers from an expired lease without progress.</param>
/// <param name="Failures">Consecutive releases after a failed attempt.</param>
/// <param name="CancelRequestedAt">When cancellation was requested; null without a request.</param>
/// <param name="ExecutionId">The current execution; null on reconciliation sources, whose execution ids are derived (<see cref="ReconcileExecution"/>).</param>
public sealed record LeaseRecord(
    DateTimeOffset ReadAt,
    DateTimeOffset? DueAt,
    OwnerId? Owner,
    long Epoch,
    DateTimeOffset? ExpiresAt,
    int Takeovers,
    int Failures,
    DateTimeOffset? CancelRequestedAt,
    Guid? ExecutionId);
