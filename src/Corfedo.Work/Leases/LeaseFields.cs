namespace Corfedo.Work;

/// <summary>The names of the lease fields on a source's records.</summary>
public sealed record LeaseFields
{
    public static LeaseFields Default { get; } = new();

    public string DueAt { get; init; } = "due_at";

    public string LeaseOwner { get; init; } = "lease_owner";

    public string LeaseEpoch { get; init; } = "lease_epoch";

    public string LeaseExpiresAt { get; init; } = "lease_expires_at";

    public string Takeovers { get; init; } = "takeovers";

    public string Failures { get; init; } = "failures";

    public string SettledEpoch { get; init; } = "settled_epoch";

    public string SettleFailures { get; init; } = "settle_failures";

    public string SettleRetryAt { get; init; } = "settle_retry_at";

    public string CancelRequestedAt { get; init; } = "cancel_requested_at";

    public string ExecutionId { get; init; } = "execution_id";

    public string EndedCompletionId { get; init; } = "ended_completion_id";

    public string EndedDigest { get; init; } = "ended_digest";
}
