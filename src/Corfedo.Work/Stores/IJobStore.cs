namespace Corfedo.Work;

/// <summary>
/// Atomic job operations of one store. Claims, renewals and guards of job records are lease operations on a
/// definition's source from <see cref="JobSource.Define"/>. Every terminal transition runs the definition's outcome
/// callback in the transaction that makes it, then starts a pending follow-up.
/// </summary>
public interface IJobStore
{
    /// <summary>Where the store keeps job records, for <see cref="JobSource.Define"/>.</summary>
    StoreLocation Table { get; }

    /// <summary>
    /// Stores <paramref name="definition"/> if its revision is newer than the stored one, taking the definition fence
    /// exclusively, and returns the newest stored definition. <paramref name="policy"/> is the definition's policy with
    /// the host's defaults for every part it leaves unset, which is stored with the revision; the digest stays the
    /// definition's.
    /// </summary>
    /// <exception cref="JobDefinitionConflictException">The stored revision is equal and its content differs.</exception>
    ValueTask<StoredJobDefinition> StoreDefinitionAsync(JobDefinition definition, JobPolicy policy, CancellationToken cancellationToken);

    /// <summary>The newest stored definition, or null when none is stored.</summary>
    ValueTask<StoredJobDefinition?> GetDefinitionAsync(string key, CancellationToken cancellationToken);

    /// <summary>
    /// In the caller's transaction, creates a standalone job or merges the request into the outstanding standalone job
    /// of its identity, and signals claimers on commit. Recurrences of the identity are not consulted.
    /// </summary>
    /// <exception cref="JobPartitionMismatchException">The outstanding job of the identity has another partition.</exception>
    ValueTask<JobRequestResult> RequestAsync(WorkTransaction transaction, LeaseSource<JobId> source, JobRequestData request, CancellationToken cancellationToken);

    /// <summary>
    /// Applies a request as <see cref="RequestAsync(WorkTransaction, LeaseSource{JobId}, JobRequestData, CancellationToken)"/>
    /// does, in the store's own transaction, at read committed isolation where the store has levels, and signals claimers
    /// on its commit.
    /// </summary>
    /// <exception cref="JobPartitionMismatchException">The outstanding job of the identity has another partition.</exception>
    ValueTask<JobRequestResult> RequestAsync(LeaseSource<JobId> source, JobRequestData request, CancellationToken cancellationToken);

    /// <summary>In the caller's transaction, applies every request in order: all commit or none do.</summary>
    /// <exception cref="JobPartitionMismatchException">An outstanding job of an item's identity has another partition.</exception>
    ValueTask<IReadOnlyList<JobRequestResult>> RequestManyAsync(WorkTransaction transaction, LeaseSource<JobId> source, IReadOnlyList<JobRequestData> requests, CancellationToken cancellationToken);

    /// <summary>In the caller's transaction, applies a transition to a validly held job, guarded like every holder write.</summary>
    /// <exception cref="LeaseLostException">The lease is no longer valid; the transaction must not commit.</exception>
    ValueTask CompleteAsync(WorkTransaction transaction, Lease<JobId> lease, JobTransition transition, JobEndedCallback? ended, CancellationToken cancellationToken);

    /// <summary>Applies a transition to a validly held job in the store's own transaction; false if the lease was already lost.</summary>
    ValueTask<bool> CompleteAsync(Lease<JobId> lease, JobTransition transition, JobEndedCallback? ended, CancellationToken cancellationToken);

    /// <summary>
    /// In the caller's transaction, cancels the outstanding jobs of the definition with <paramref name="subject"/>, or
    /// with <paramref name="mergeKey"/>, standalone jobs and the occurrences of the identity's recurrence: one that is
    /// not held ends cancelled; a held one gets a cancellation request and ends cancelled when its run observes it. Pending follow-ups are discarded. Returns how many it reached.
    /// </summary>
    ValueTask<int> CancelAsync(WorkTransaction transaction, LeaseSource<JobId> source, Subject? subject, string? mergeKey, JobEndedCallback? ended, CancellationToken cancellationToken);

    /// <summary>
    /// In the caller's transaction, cancels the outstanding jobs of every definition acting on <paramref name="subject"/>.
    /// <paramref name="definitions"/> resolves each definition's source and outcome callback by key.
    /// </summary>
    /// <exception cref="InvalidOperationException">A job of the subject belongs to a definition <paramref name="definitions"/> does not resolve.</exception>
    ValueTask<int> CancelAllAsync(WorkTransaction transaction, Subject subject, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken);

    /// <summary>
    /// Ends up to <paramref name="limit"/> outstanding jobs that are not held and whose argument version is below the
    /// stored minimum as failed with <c>args_version</c>, each in its own transaction, without reading their arguments.
    /// </summary>
    ValueTask<int> RetireAsync(LeaseSource<JobId> source, JobEndedCallback? ended, int limit, CancellationToken cancellationToken);

    /// <summary>
    /// In the caller's transaction, pauses or resumes claiming of the definition: takes the definition fence
    /// exclusively, so claims in flight finish first and later claims read the new state after their fence.
    /// </summary>
    ValueTask PauseDefinitionAsync(WorkTransaction transaction, LeaseSource<JobId> source, bool paused, CancellationToken cancellationToken);

    /// <summary>
    /// In the caller's transaction, pauses or resumes one subject of the definition. The stored pause is the authority
    /// claims check after taking the subject's lock in shared mode; pausing and resuming take it exclusively, so a claim
    /// in flight finishes first and a later claim sees the committed state. Jobs and occurrences created while the
    /// pause holds are not claimable.
    /// </summary>
    ValueTask PauseSubjectAsync(WorkTransaction transaction, LeaseSource<JobId> source, Subject subject, bool paused, CancellationToken cancellationToken);

    /// <summary>
    /// In the caller's transaction, creates the recurrence of the request's identity or replaces its schedule, policies
    /// and arguments, waiting for its lock; the replacement applies from the next occurrence. Standalone jobs of the
    /// identity are left alone. Call it before the transaction's own writes and row locks.
    /// </summary>
    /// <exception cref="JobPartitionMismatchException">The recurrence of the identity has another partition.</exception>
    /// <exception cref="ArgumentException">The schedule has no future occurrence.</exception>
    /// <exception cref="InvalidOperationException">The store has isolation levels and the transaction is not read-committed; nothing was changed.</exception>
    ValueTask<RecurrenceOutcome> ScheduleAsync(WorkTransaction transaction, LeaseSource<JobId> source, RecurrenceData recurrence, IRecurrenceCalculator calculator, CancellationToken cancellationToken);

    /// <summary>
    /// Stores a code-defined recurrence of <paramref name="revision"/> in the store's own transaction: creates it, or
    /// replaces a recurrence stored by an older revision; a newer stored revision is kept.
    /// </summary>
    /// <exception cref="JobDefinitionConflictException">The stored revision is equal and its digest differs.</exception>
    ValueTask<RecurrenceOutcome> DeclareRecurrenceAsync(LeaseSource<JobId> source, RecurrenceData recurrence, long revision, byte[] digest, IRecurrenceCalculator calculator, CancellationToken cancellationToken);

    /// <summary>
    /// In the caller's transaction, cancels the outstanding occurrences of the identity's recurrence, then deletes the
    /// recurrence. Occurrences waiting to be recorded skipped are left to finalization. An occurrence whose recurring
    /// chain runs has its chain's outstanding steps cancelled instead, with the definitions
    /// <paramref name="definitions"/> resolves by key, and ends with the chain's outcome once the chain settled.
    /// False when there was no recurrence.
    /// </summary>
    /// <exception cref="InvalidOperationException">A step of such a chain belongs to a definition <paramref name="definitions"/> does not resolve.</exception>
    /// <exception cref="InvalidOperationException">The store has isolation levels and the transaction is not read-committed; nothing was changed.</exception>
    ValueTask<bool> RemoveAsync(WorkTransaction transaction, LeaseSource<JobId> source, string identity, JobEndedCallback? ended, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken);

    /// <summary>
    /// In the caller's transaction, triggers the identity's recurrence without moving it: starts a triggered occurrence
    /// due now when none is outstanding, or makes an outstanding one that is not running due now. With one running it
    /// follows the overlap policy: under <see cref="Overlap.Wait"/> it records the waiting occurrence and reports
    /// <see cref="RunNowOutcome.Waiting"/>, under <see cref="Overlap.Skip"/> it reports the running one, and under
    /// <see cref="Overlap.Allow"/> it starts a separate triggered occurrence.
    /// Work it starts or makes due with a calendar is planned at the next allowed instant, or due now with
    /// <paramref name="ignoreCalendar"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">The identity has no recurrence, or the store has isolation levels and the transaction is not read-committed; nothing was changed.</exception>
    ValueTask<RunNowResult> TriggerAsync(WorkTransaction transaction, LeaseSource<JobId> source, string identity, bool ignoreCalendar, CancellationToken cancellationToken);

    /// <summary>
    /// In the store's own transaction, makes an outstanding job that is not held due now, keeping its attempts, and
    /// reports what it found; a paused one is made due and reported paused, an occurrence waiting to be recorded
    /// skipped is reported waiting. An occurrence of a recurrence that has ended triggers the recurrence
    /// as <see cref="TriggerAsync"/> does, taking the recurrence's lock before the jobs'. A job with a calendar is planned
    /// at the next allowed instant, or due now with <paramref name="ignoreCalendar"/>.
    /// </summary>
    ValueTask<RunNowResult> RunNowAsync(LeaseSource<JobId> source, JobId job, bool ignoreCalendar, CancellationToken cancellationToken);

    /// <summary>
    /// In the store's own transaction, makes a terminal one-shot job outstanding again on the same record: due now, with
    /// a new execution id, fresh attempts and not started; the ended execution's outcome and result stay readable until
    /// the new one ends, and lease epochs continue. Refused, changing nothing, while an execution of the job is
    /// outstanding, while another outstanding standalone job holds its merge identity, and for occurrences. A job with a
    /// calendar is planned at the next allowed instant, or due now with <paramref name="ignoreCalendar"/>.
    /// </summary>
    ValueTask<RerunResult> RerunAsync(LeaseSource<JobId> source, JobId job, bool ignoreCalendar, CancellationToken cancellationToken);

    /// <summary>
    /// One retention pass over every stored definition, each statement in its own transaction and skipping locked
    /// rows: deletes up to <paramref name="limit"/> terminal jobs that ended longer ago than the definition's stored job
    /// retention, keeping occurrences the planner has not consumed and a recurrence's awaited occurrence, and up to
    /// <paramref name="limit"/> run history entries that ended longer ago than its run history retention.
    /// </summary>
    ValueTask<RetentionPass> RetainAsync(int limit, CancellationToken cancellationToken);

    /// <summary>
    /// One planning pass of the definition in the store's own transaction: locks up to <paramref name="limit"/> due
    /// recurrences without waiting, consumes their ended occurrences, applies the missed rule and the overlap policy
    /// with the store's clock and creates occurrences idempotently. It runs no outcome hook: occurrences the overlap
    /// policy skips are recorded to be finalized by <see cref="FinalizeSkippedAsync"/>.
    /// </summary>
    ValueTask<PlanningPass> PlanAsync(LeaseSource<JobId> source, IRecurrenceCalculator calculator, int limit, CancellationToken cancellationToken);

    /// <summary>
    /// One calendar planning pass of the definition in the store's own transaction, with the store's clock and the
    /// stored calendars and date sets. It locks, without waiting, up to <paramref name="limit"/> outstanding jobs that
    /// are not held and whose calendar instant has come, and up to <paramref name="limit"/> whose allowed window closed
    /// before they started. An allowed instant becomes the due time with the end of its window; a blocked one follows the
    /// job's blocked policy, a closed window its closed policy; jobs to be skipped are recorded for
    /// <see cref="FinalizeSkippedAsync"/>, and jobs that need dates a date set does not cover wait until a covering
    /// revision is published. It ignores pauses, which claims apply.
    /// </summary>
    ValueTask<CalendarPass> PlanCalendarAsync(LeaseSource<JobId> source, int limit, CancellationToken cancellationToken);

    /// <summary>
    /// Ends up to <paramref name="limit"/> jobs recorded to be skipped, by the overlap policy, a calendar or a chain, and
    /// occurrences whose chain settled, with the chain's outcome, each in its own transaction through the terminal path
    /// with the outcome hook, holding no recurrence lock. They never run a handler or use an attempt, and are finalized
    /// also while paused or after their recurrence was removed.
    /// </summary>
    ValueTask<int> FinalizeSkippedAsync(LeaseSource<JobId> source, JobEndedCallback? ended, int limit, CancellationToken cancellationToken);

    /// <summary>
    /// Repairs recurrences and outstanding jobs whose copied pause flag disagrees with the stored subject pauses, up to
    /// <paramref name="limit"/> rows in each direction for each, without waiting for locked rows. Returns the most rows
    /// one of them repaired, which reaches <paramref name="limit"/> only while more may remain.
    /// </summary>
    ValueTask<int> RepairPausesAsync(LeaseSource<JobId> source, int limit, CancellationToken cancellationToken);
    /// <summary>
    /// Writes the attempt's progress in the store's own transaction, fenced to the lease; false if the lease is no
    /// longer valid. Every claim that runs the handler clears it.
    /// </summary>
    ValueTask<bool> ReportProgressAsync(Lease<JobId> lease, JobProgress progress, CancellationToken cancellationToken);

    /// <summary>
    /// The status of each subject of the definition that has an outstanding job, a recurrence or an ended job; subjects
    /// with none are left out.
    /// </summary>
    ValueTask<IReadOnlyDictionary<Subject, JobStatus>> GetStatusAsync(string definition, IReadOnlyList<Subject> subjects, CancellationToken cancellationToken);

    /// <summary>
    /// In the store's own transaction, marks an outstanding job of the definition as waited for, so the transition that
    /// leaves it without an outstanding execution signals <see cref="WorkTopics.Ended"/> on commit; a follow-up that
    /// reuses the id keeps the mark. It takes no lease and waits for the job's row lock. A job already marked is not
    /// written again. False when the job is not outstanding or was already marked.
    /// </summary>
    ValueTask<bool> WatchAsync(string definition, JobId job, CancellationToken cancellationToken);

    /// <summary>The latest ended execution of each job of the definition that exists; unknown ids are left out.</summary>
    ValueTask<IReadOnlyList<StoredJobOutcome>> GetOutcomesAsync(string definition, IReadOnlyList<JobId> jobs, CancellationToken cancellationToken);

    /// <summary>
    /// Run history entries of the definition's jobs of <paramref name="subject"/>, or of <paramref name="job"/>, newest
    /// first, at most <paramref name="limit"/>, continuing after <paramref name="after"/>.
    /// </summary>
    ValueTask<JobRunPage> GetRunsAsync(string definition, Subject? subject, JobId? job, int limit, JobRunCursor? after, CancellationToken cancellationToken);

    /// <summary>Records the definitions a registered process registers; definitions it already recorded are left as they are.</summary>
    ValueTask RegisterProcessAsync(OwnerId owner, IReadOnlyList<ProcessDefinition> definitions, CancellationToken cancellationToken);

    /// <summary>
    /// Definitions and argument versions with due work no live process can run, in one snapshot, as of
    /// <paramref name="grace"/> before the store's clock: due work is an unheld job due by then, a lease expired by then,
    /// or a recurrence its planner would have acted on by then, and a process counts as live when it was live at any
    /// instant since then. Paused definitions and subjects are left out, by the stored pauses, and versions below the
    /// stored minimum are not reported as versions. A copied pause flag that no stored pause backs does not hide the work
    /// of a definition no live process runs.
    /// </summary>
    ValueTask<UnservedReport> GetUnservedAsync(TimeSpan grace, CancellationToken cancellationToken);

    /// <summary>Code-defined recurrences that no live process declares, whatever revision it registers.</summary>
    ValueTask<IReadOnlyList<UndeclaredRecurrence>> GetUndeclaredRecurrencesAsync(CancellationToken cancellationToken);

    /// <summary>Live processes registering a definition with an older revision than the stored one, one entry per definition.</summary>
    ValueTask<IReadOnlyList<OutdatedProcess>> GetOutdatedProcessesAsync(CancellationToken cancellationToken);
}
