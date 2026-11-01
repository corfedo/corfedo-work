namespace Corfedo.Work;

/// <summary>How an operator action treats the work it starts or makes due (J21).</summary>
[Flags]
public enum JobActionOptions
{
    /// <summary>Work with a calendar is planned at the next allowed instant.</summary>
    None = 0,

    /// <summary>Work with a calendar is due now and may start outside its calendar's windows.</summary>
    IgnoreCalendar = 1,
}

/// <summary>What running a job now, or triggering a recurrence, found and did (J21).</summary>
public enum RunNowOutcome
{
    /// <summary>No occurrence of the recurrence was outstanding; a triggered occurrence is due now.</summary>
    Started,

    /// <summary>
    /// The job was scheduled later or waited for a retry backoff, or an outstanding occurrence was not running; it is
    /// due now with its attempts kept.
    /// </summary>
    MadeDue,

    /// <summary>The job is paused by its definition or subject; it is due now and runs once resumed.</summary>
    Paused,

    /// <summary>The job was already due; nothing changed.</summary>
    AlreadyDue,

    /// <summary>The job, or an occurrence of the recurrence, is running; nothing was queued.</summary>
    AlreadyRunning,

    /// <summary>
    /// The job is an occurrence waiting to be recorded skipped, and nothing changed; or a trigger under
    /// <see cref="Overlap.Wait"/> found the named occurrence running and recorded one run after it.
    /// </summary>
    Waiting,

    /// <summary>The job has ended; rerun it with <c>RerunAsync</c>.</summary>
    Ended,

    /// <summary>The job is a chain step, which runs when its parent settled; rerun a chain with <c>RetryFromAsync</c>.</summary>
    ChainStep,

    /// <summary>The definition has no job with the id.</summary>
    NotFound,
}

/// <summary>What running a job now or triggering a recurrence did, and the job it concerns.</summary>
public readonly record struct RunNowResult(JobId Job, RunNowOutcome Outcome);

/// <summary>What rerunning a job did (J21).</summary>
public enum RerunOutcome
{
    /// <summary>The terminal job is outstanding again, due now, with the same job id, a new execution id and fresh attempts.</summary>
    Rerun,

    /// <summary>An execution of the job is outstanding: scheduled, running or waiting for a retry.</summary>
    Outstanding,

    /// <summary>Another outstanding job holds the job's merge identity.</summary>
    IdentityHeld,

    /// <summary>The job is an occurrence of a recurrence, or its identity has a recurrence; run the recurrence with <c>RunNowAsync</c> or <c>TriggerAsync</c>.</summary>
    Recurring,

    /// <summary>The job ran within a calendar that has been removed since; store the calendar again to rerun it.</summary>
    CalendarRemoved,

    /// <summary>The job is a chain step; rerun a chain with <c>RetryFromAsync</c>.</summary>
    ChainStep,

    /// <summary>The same job id came earlier in the same call.</summary>
    Duplicate,

    /// <summary>The definition has no job with the id.</summary>
    NotFound,
}

public readonly record struct RerunResult(JobId Job, RerunOutcome Outcome);

/// <summary>
/// The retention durations of a host: terminal jobs after they ended and run history entries after their attempt
/// ended, for definitions whose policy leaves them unset (J12), and incidents after they resolved.
/// </summary>
public sealed record JobRetention
{
    /// <param name="incidents">How long resolved incidents are kept; 90 days when null.</param>
    public JobRetention(TimeSpan jobs, TimeSpan runHistory, TimeSpan? incidents = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(jobs, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(runHistory, TimeSpan.Zero);
        var resolved = incidents ?? TimeSpan.FromDays(90);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(resolved, TimeSpan.Zero, nameof(incidents));
        Jobs = jobs;
        RunHistory = runHistory;
        Incidents = resolved;
    }

    /// <summary>Seven days for terminal jobs, thirty days for run history, ninety days for resolved incidents.</summary>
    public static JobRetention Default { get; } = new(TimeSpan.FromDays(7), TimeSpan.FromDays(30));

    public TimeSpan Jobs { get; }

    public TimeSpan RunHistory { get; }

    public TimeSpan Incidents { get; }
}

/// <summary>What one retention pass deleted: terminal jobs, run history entries and chains, and whether a batch was full so more may remain.</summary>
public readonly record struct RetentionPass(int Jobs, int Runs, int Chains, bool More);
