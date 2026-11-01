namespace Corfedo.Work.Conformance;

/// <summary>What a provider supplies to run the job scenarios, in addition to the lease fixtures: reads of job records.</summary>
public interface IJobConformanceProvider : ILeaseConformanceProvider
{
    /// <summary>Reads one job record, evaluating times with the store's clock.</summary>
    Task<JobState> ReadJobAsync(JobId job, CancellationToken cancellationToken);

    /// <summary>Every job record of a definition, oldest first.</summary>
    Task<IReadOnlyList<JobState>> ReadJobsAsync(string definition, CancellationToken cancellationToken);

    /// <summary>The copied pause flag of the subject's recurrence of a definition.</summary>
    Task<bool> ReadRecurrencePausedAsync(string definition, Subject subject, CancellationToken cancellationToken);

    /// <summary>Locks the subject's recurrence of a definition and its outstanding jobs in the transaction, as a concurrent writer would.</summary>
    Task LockSubjectAsync(WorkTransaction transaction, string definition, Subject subject, CancellationToken cancellationToken);

    /// <summary>The store's clock.</summary>
    Task<DateTimeOffset> NowAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Transactions the store has committed so far, counted by the store with a lag of about a second, for asserting
    /// that background work waits instead of repeating at once.
    /// </summary>
    Task<long> CommittedTransactionsAsync(CancellationToken cancellationToken);

    /// <summary>The registry's record of a process, or null when it has none.</summary>
    Task<ProcessRecord?> ReadProcessAsync(OwnerId owner, CancellationToken cancellationToken);

    /// <summary>Moves the process record's start, heartbeat and stop back by <paramref name="by"/>, as if it were that much older.</summary>
    Task AgeProcessAsync(OwnerId owner, TimeSpan by, CancellationToken cancellationToken);

    /// <summary>Deletes the process record and its definitions.</summary>
    Task DeleteProcessAsync(OwnerId owner, CancellationToken cancellationToken);
}

/// <summary>
/// A process record: its type and release, the time since its heartbeat by the store's clock, whether it stopped, and
/// the job definitions it registers.
/// </summary>
public sealed record ProcessRecord(string Type, string? Release, TimeSpan HeartbeatAge, bool Stopped, IReadOnlyList<ProcessDefinition> Definitions);

/// <summary>
/// One job record. <see cref="State"/> is null while the job is outstanding; <see cref="Outcome"/> and
/// <see cref="Reason"/> are those of its latest ended execution. <see cref="DueIn"/> is the time left by the store's
/// clock, negative when passed, and null without a due time. <see cref="Occurrence"/> identifies an occurrence of a
/// recurring job; <see cref="Unconsumed"/> says its planner has not consumed its end yet, <see cref="SkipPending"/> that
/// it waits to be recorded skipped, and <see cref="Paused"/> is the copied pause flag. <see cref="HoldsIdentity"/> says
/// the job has a standalone merge identity; occurrences never have one. <see cref="CalendarUnavailable"/> says the job
/// waits for a date set to cover the instant its calendar must decide. <see cref="Waited"/> says someone waits for the
/// job's end, so its end is signalled.
/// </summary>
public sealed record JobState(
    JobId Id,
    string Definition,
    JobOutcome? State,
    JobOutcome? Outcome,
    string? Reason,
    string Args,
    int ArgsVersion,
    bool Started,
    int Attempts,
    string? PendingArgs,
    TimeSpan? DueIn,
    TimeSpan? PendingDueIn,
    bool Held,
    bool CancelRequested,
    Guid ExecutionId,
    string Partition,
    string? Occurrence = null,
    bool Unconsumed = false,
    bool SkipPending = false,
    bool Paused = false,
    DateTimeOffset? EndedAt = null,
    bool HoldsIdentity = false,
    bool CalendarUnavailable = false,
    bool Waited = false);
