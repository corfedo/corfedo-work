namespace Corfedo.Work;

/// <summary>Identifies a chain: a set of jobs created together, where a step runs after what it depends on settled.</summary>
public readonly record struct ChainId(Guid Value)
{
    public static ChainId New() => new(Guid.CreateVersion7());

    public override string ToString() => Value.ToString("D");
}

/// <summary>When a step runs once its parent step settled.</summary>
public enum RunCondition
{
    /// <summary>The parent completed.</summary>
    OnSuccess,

    /// <summary>The parent failed or timed out.</summary>
    OnFailure,

    /// <summary>The parent was cancelled.</summary>
    OnCancelled,

    /// <summary>The parent failed, timed out or was cancelled.</summary>
    OnFailureOrCancelled,

    /// <summary>The parent settled in any way, including skipped.</summary>
    OnAnyOutcome,
}

/// <summary>
/// The settled outcomes under which a step runs, as a provider stores its condition. A parent group settles as the
/// aggregate of its branches.
/// </summary>
[Flags]
public enum ChainOutcomes : short
{
    None = 0,
    Succeeded = 1,

    /// <summary>Failed or timed out.</summary>
    Failed = 2,
    Cancelled = 4,
    Skipped = 8,
    Any = Succeeded | Failed | Cancelled | Skipped,
}

/// <summary>What deleting a chain does while steps are unsettled.</summary>
public enum ChainDelete
{
    /// <summary>Deletes a settled chain; refuses one with unsettled steps.</summary>
    Refuse,

    /// <summary>Cancels the unsettled steps and deletes the chain once they settled.</summary>
    Cascade,
}

public enum ChainDeleteOutcome
{
    /// <summary>The settled chain, its groups and its steps were deleted.</summary>
    Deleted,

    /// <summary>The unsettled steps were cancelled; the chain is deleted in the transaction that settles it.</summary>
    Deleting,

    /// <summary>The chain has unsettled steps; nothing changed.</summary>
    Unsettled,

    /// <summary>There is no chain with the id.</summary>
    NotFound,
}

public enum RetryFromOutcome
{
    /// <summary>The step or group and everything that depends on it run again under a new generation.</summary>
    Retried,

    /// <summary>The chain has unsettled steps, also when a concurrent retry reopened it; nothing changed.</summary>
    NotSettled,

    /// <summary>There is no such chain, or the chain has no such step or group.</summary>
    NotFound,
}

/// <summary>
/// A chain as stored: its generation, root step, the occurrence that owns a recurring chain, whether every step settled
/// and with which outcome, and its steps and groups.
/// </summary>
public sealed record ChainStatus(
    ChainId Id,
    long Generation,
    JobId Root,
    JobId? Owner,
    bool Settled,
    JobOutcome? Outcome,
    DateTimeOffset CreatedAt,
    DateTimeOffset? SettledAt,
    IReadOnlyList<ChainStepStatus> Steps,
    IReadOnlyList<ChainGroupStatus> Groups);

/// <summary>
/// One step: its job, definition and generation, the step or group it waits for (null for the root), its enclosing
/// group, whether it waits for its parent, whether it ended, and its latest ended execution's outcome and reason.
/// </summary>
public sealed record ChainStepStatus(JobId Job, string Definition, long Generation, Guid? Parent, Guid? Group, bool Waiting, bool Ended, JobOutcome? Outcome, string? Reason);

/// <summary>One group: the step it follows, its enclosing group, the nodes of its branches not yet settled and its aggregate outcome.</summary>
public sealed record ChainGroupStatus(Guid Id, long Generation, Guid Parent, Guid? Group, int Pending, bool Settled, JobOutcome? Outcome);

/// <summary>
/// A step as a store creates it: its job id, definition and request, the step or group it waits for (null for the
/// root, whose request's schedule makes it due), its enclosing group (null at the chain's top level) and the outcomes of
/// its parent under which it runs.
/// </summary>
public sealed record ChainStepData(JobId Id, string Definition, JobRequestData Request, Guid? Parent, Guid? Group, ChainOutcomes Condition);

/// <summary>A group as a store creates it: the step it follows and its enclosing group (null at the chain's top level).</summary>
public sealed record ChainGroupData(Guid Id, Guid Parent, Guid? Group);

/// <summary>A chain as a store creates it.</summary>
public sealed record ChainData(ChainId Id, IReadOnlyList<ChainStepData> Steps, IReadOnlyList<ChainGroupData> Groups);
