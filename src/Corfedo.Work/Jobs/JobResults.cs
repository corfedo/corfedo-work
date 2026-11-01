using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Corfedo.Work;

/// <summary>Serializes a job's result with a schema version; reads every version a reader may find stored.</summary>
public interface IJobResults<TResult>
{
    /// <summary>The version this codec writes.</summary>
    int Version { get; }

    byte[] Write(TResult result);

    TResult Read(ReadOnlySpan<byte> data, int version);
}

public static class JobResults
{
    /// <summary>The largest serialized result a job may store.</summary>
    public const int MaximumBytes = 256 * 1024;

    /// <summary>
    /// JSON results through <paramref name="typeInfo"/>, written as <paramref name="version"/>, for a definition with
    /// versioned results or JSON settings of its own; other definitions use the host's JSON options.
    /// </summary>
    public static IJobResults<TResult> Json<TResult>(JsonTypeInfo<TResult> typeInfo, int version = 1)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(version);
        return new JsonResults<TResult>(typeInfo, version);
    }

    internal sealed class JsonResults<TResult>(JsonTypeInfo<TResult> typeInfo, int version) : IJobResults<TResult>
    {
        public int Version => version;

        public byte[] Write(TResult result) => JsonSerializer.SerializeToUtf8Bytes(result, typeInfo);

        public TResult Read(ReadOnlySpan<byte> data, int version) => JsonSerializer.Deserialize(data, typeInfo)!;
    }
}

/// <summary>
/// A stored result is of a newer version than the reading codec's <see cref="IJobResults{TResult}.Version"/>: it was
/// written by newer code than the reader's. A job handler that meets it fails its attempt with
/// <see cref="JobReasons.ResultVersion"/>.
/// </summary>
public sealed class JobResultVersionException(string definition, int version, int readable)
    : Exception($"Job definition {definition} stored a result of version {version}; this process reads versions up to {readable}. Deploy readers of a new result version before its writers.")
{
    /// <summary>The definition whose result was read.</summary>
    public string Definition { get; } = definition;

    /// <summary>The version of the stored result.</summary>
    public int Version { get; } = version;

    /// <summary>The newest version the reading codec reads.</summary>
    public int Readable { get; } = readable;
}

/// <summary>
/// The latest ended execution of a job id. <see cref="Ended"/> is false while an execution of the id is outstanding,
/// including a follow-up that reuses it; the other fields then describe the execution that ended before, if any.
/// </summary>
public record JobOutcomeStatus(JobId Job, bool Ended, Guid? ExecutionId, JobOutcome? Outcome, string? Reason, DateTimeOffset? EndedAt);

/// <summary>
/// The latest ended execution of a job with a result type. <see cref="Result"/> is the default value when
/// <see cref="HasResult"/> is false: the execution did not complete, or completed without a result.
/// </summary>
public sealed record JobOutcomeStatus<TResult>(JobId Job, bool Ended, Guid? ExecutionId, JobOutcome? Outcome, string? Reason, DateTimeOffset? EndedAt, bool HasResult, TResult Result)
    : JobOutcomeStatus(Job, Ended, ExecutionId, Outcome, Reason, EndedAt);

/// <summary>A job's outcome as stored, with its serialized result; for providers and operations.</summary>
public sealed record StoredJobOutcome(JobId Job, bool Ended, Guid? ExecutionId, JobOutcome? Outcome, string? Reason, DateTimeOffset? EndedAt, byte[]? Result, int ResultVersion);

/// <summary>What a subject's job is doing.</summary>
public enum JobStatusState
{
    /// <summary>No job is outstanding; a recurrence or an ended job exists.</summary>
    Idle,

    /// <summary>A job is outstanding and not running: waiting for its due time, a retry, its claim or a resume.</summary>
    Scheduled,

    /// <summary>A job is running under a valid lease, or an occurrence of a recurring chain waits for the chain it started.</summary>
    Running,

    /// <summary>A chain step waits for its parent to settle; it has no due time yet.</summary>
    WaitingForParent,
}

/// <summary>A running attempt's reported progress.</summary>
public sealed record JobProgress(int Percent, string? Message);

/// <summary>
/// The scheduling status of one subject of a definition (D5). It describes the outstanding job holding the subject's
/// identity, otherwise its earliest-due outstanding job: its due time, when its attempt started, its occurrence, the
/// attempts its execution used and its progress. <see cref="LastOutcome"/> is that of the subject's latest ended
/// execution, <see cref="NextOccurrence"/> the next planned instant of its recurrence, <see cref="Paused"/> whether
/// the definition or the subject is paused, and <see cref="FollowUpPending"/> whether a follow-up waits for the
/// outstanding job.
/// </summary>
public sealed record JobStatus(
    JobStatusState State,
    JobId? Job,
    DateTimeOffset? DueAt,
    DateTimeOffset? RunningSince,
    string? Occurrence,
    int Attempt,
    JobProgress? Progress,
    JobOutcome? LastOutcome,
    string? LastReason,
    DateTimeOffset? LastEndedAt,
    DateTimeOffset? NextOccurrence,
    bool Paused,
    bool FollowUpPending);

/// <summary>How one attempt ended. The terminal values match <see cref="JobOutcome"/>.</summary>
public enum JobRunOutcome
{
    Completed = 1,
    Failed = 2,
    Cancelled = 3,
    TimedOut = 4,
    Skipped = 5,

    /// <summary>The attempt failed and the job waits for another one.</summary>
    Retried = 6,

    /// <summary>The attempt's lease expired before it ended the attempt.</summary>
    LeaseLost = 7,

    /// <summary>The holder gave the job back, for example when its process stopped.</summary>
    Released = 8,
}

/// <summary>One attempt in a definition's run history (D7), keyed by job and lease epoch.</summary>
public sealed record JobRunEntry(
    JobId Job,
    long Epoch,
    Subject? Subject,
    string? Occurrence,
    Guid ExecutionId,
    int Attempt,
    OwnerId Owner,
    DateTimeOffset StartedAt,
    DateTimeOffset EndedAt,
    JobRunOutcome Outcome,
    string? Reason);

/// <summary>Where a page of run history continues: after the entry with this start, job and epoch.</summary>
public sealed record JobRunCursor(DateTimeOffset StartedAt, JobId Job, long Epoch);

/// <summary>Run history entries, newest first, and the cursor of the next page; null when there are no more.</summary>
public sealed record JobRunPage(IReadOnlyList<JobRunEntry> Runs, JobRunCursor? Next);
