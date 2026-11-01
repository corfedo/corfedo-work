namespace Corfedo.Work;

/// <summary>What a handler's attempt decided.</summary>
public sealed record JobResult
{
    private JobResult(JobResultKind kind, TimeSpan after, string? code)
    {
        Kind = kind;
        After = after;
        Code = code;
    }

    public JobResultKind Kind { get; }

    /// <summary>The handler's minimum delay before the next attempt, for <see cref="JobResultKind.Retry"/>.</summary>
    public TimeSpan After { get; }

    /// <summary>The stable reason code of a retry or failure.</summary>
    public string? Code { get; }

    /// <summary>Terminal success.</summary>
    public static JobResult Completed { get; } = new(JobResultKind.Completed, TimeSpan.Zero, null);

    /// <summary>
    /// Another attempt no sooner than <paramref name="after"/> and the definition's backoff; failed with
    /// <c>attempts_exhausted</c> when no attempt is left.
    /// </summary>
    public static JobResult Retry(TimeSpan after, string code)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(after, TimeSpan.Zero);
        ArgumentException.ThrowIfNullOrWhiteSpace(code);
        return new(JobResultKind.Retry, after, code);
    }

    /// <summary>Terminal failure.</summary>
    public static JobResult Fail(string code)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(code);
        return new(JobResultKind.Fail, TimeSpan.Zero, code);
    }
}

public enum JobResultKind
{
    Completed,
    Retry,
    Fail,
}

/// <summary>The terminal outcome of one execution.</summary>
public enum JobOutcome
{
    Completed = 1,
    Failed = 2,
    Cancelled = 3,
    TimedOut = 4,
    Skipped = 5,
}

/// <summary>A transition a store applies to a held job: a handler's result, or a decision of the dispatcher.</summary>
public sealed record JobTransition
{
    private JobTransition(JobTransitionKind kind, TimeSpan after, string? code, byte[]? result = null, int resultVersion = 0)
    {
        Kind = kind;
        After = after;
        Code = code;
        Result = result;
        ResultVersion = resultVersion;
    }

    public JobTransitionKind Kind { get; }

    public TimeSpan After { get; }

    public string? Code { get; }

    /// <summary>The serialized result a completion stores with its outcome, or null.</summary>
    public byte[]? Result { get; }

    /// <summary>The codec version of <see cref="Result"/>.</summary>
    public int ResultVersion { get; }

    /// <summary>Terminal success with a serialized result of at most <see cref="JobResults.MaximumBytes"/>.</summary>
    public static JobTransition Complete(byte[] result, int version)
    {
        ArgumentNullException.ThrowIfNull(result);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(version);
        if (result.Length > JobResults.MaximumBytes)
        {
            throw new ArgumentException($"The result is {result.Length} bytes; at most {JobResults.MaximumBytes} are stored.", nameof(result));
        }
        return new(JobTransitionKind.Complete, TimeSpan.Zero, null, result, version);
    }

    public static JobTransition From(JobResult result) => result.Kind switch
    {
        JobResultKind.Completed => new(JobTransitionKind.Complete, TimeSpan.Zero, null),
        JobResultKind.Retry => new(JobTransitionKind.Retry, result.After, result.Code),
        _ => new(JobTransitionKind.Fail, TimeSpan.Zero, result.Code),
    };

    /// <summary>The run observed a cancellation request; the job ends cancelled.</summary>
    public static JobTransition Cancel { get; } = new(JobTransitionKind.Cancel, TimeSpan.Zero, "cancelled");

    /// <summary>The attempt timed out: retried when the stored policy retries timeouts, otherwise timed out.</summary>
    public static JobTransition TimeOut { get; } = new(JobTransitionKind.TimeOut, TimeSpan.Zero, "timeout");

    /// <summary>The claim found the job late under <see cref="Late.SkipAfter"/>; it ends skipped.</summary>
    public static JobTransition Late { get; } = new(JobTransitionKind.Skip, TimeSpan.Zero, "late");

    /// <summary>The claim found no attempt left after a lost lease; the job ends failed.</summary>
    public static JobTransition Exhausted { get; } = new(JobTransitionKind.Fail, TimeSpan.Zero, JobReasons.AttemptsExhausted);
}

public enum JobTransitionKind
{
    Complete,
    Retry,
    Fail,
    Cancel,
    TimeOut,
    Skip,
}

/// <summary>Reason codes the framework records.</summary>
public static class JobReasons
{
    public const string AttemptsExhausted = "attempts_exhausted";
    public const string ArgsVersion = "args_version";
    public const string HandlerFailed = "handler_failed";
    public const string Cancelled = "cancelled";
    public const string Uncompleted = "uncompleted";
    public const string Overlap = "overlap";
    public const string ResultTooLarge = "result_too_large";

    /// <summary>The attempt read a result of a newer version than its codec reads.</summary>
    public const string ResultVersion = "result_version";

    /// <summary>The calendar blocked the planned instant.</summary>
    public const string CalendarBlocked = "calendar_blocked";

    /// <summary>The allowed window closed before the work could start, under <see cref="Closed.Skip"/>.</summary>
    public const string WindowClosed = "window_closed";

    /// <summary>The step's condition did not hold for the outcome its parent settled with.</summary>
    public const string ChainCondition = "chain_condition";

    /// <summary>The chain's root was skipped, which skips every step.</summary>
    public const string RootSkipped = "root_skipped";

    /// <summary>The occurrence's chain settled failed.</summary>
    public const string ChainFailed = "chain_failed";

    /// <summary>The occurrence's chain settled cancelled.</summary>
    public const string ChainCancelled = "chain_cancelled";

    /// <summary>The occurrence's chain settled skipped.</summary>
    public const string ChainSkipped = "chain_skipped";
}

/// <summary>
/// One terminal transition of an execution, as an outcome hook sees it. Never carries the arguments.
/// <see cref="Occurrence"/> identifies an occurrence of a recurring job: its scheduled instant in ISO 8601 UTC,
/// <c>trigger:&lt;id&gt;</c> or <c>request:&lt;job id&gt;</c>; null for one-shot jobs.
/// </summary>
public sealed record JobEnd(
    string Definition,
    JobId Job,
    Subject? Subject,
    string? Partition,
    Guid ExecutionId,
    JobOutcome Outcome,
    string? Reason,
    int Attempts,
    string? Occurrence = null);

/// <summary>Runs inside the transaction that makes a terminal transition; a failure rolls the transition back.</summary>
public delegate ValueTask JobEndedCallback(JobEnd end, IWorkTransaction transaction, CancellationToken cancellationToken);

/// <summary>How a job claim found the job.</summary>
public enum JobClaimKind
{
    /// <summary>The handler runs; the claim used an attempt.</summary>
    Run,

    /// <summary>The execution had not started and the claim is late under <see cref="Late.SkipAfter"/>; no attempt used.</summary>
    Late,

    /// <summary>No attempt is left after a lost lease; no attempt used.</summary>
    Exhausted,

    /// <summary>Cancellation was requested while an earlier holder ran it; no attempt used.</summary>
    Cancelled,
}

/// <summary>
/// The job part of a claim of a job record; <see cref="Occurrence"/> as in <see cref="JobEnd"/>. <see cref="DueAt"/> is
/// the job's due time, by the store's clock, and <see cref="TraceParent"/> the W3C id of the activity that requested it.
/// </summary>
public sealed record JobClaim(JobClaimKind Kind, int Attempt, int ArgsVersion, byte[] Args, Subject? Subject, string? Occurrence = null, DateTimeOffset? DueAt = null, string? TraceParent = null);
