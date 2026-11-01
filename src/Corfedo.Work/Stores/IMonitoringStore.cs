namespace Corfedo.Work;

/// <summary>
/// Expectation state and incidents of one store. The transaction that ends a job of a definition with expectations
/// records its failures, resets and resolutions (see <see cref="IJobStore"/>); the monitoring loop uses these
/// operations for deadlines, running attempts and date sets. Each runs in the store's own short transactions, locks
/// rows without waiting and leaves locked ones to a later call.
/// </summary>
public interface IMonitoringStore
{
    /// <summary>Where incidents are stored: the records of the incident lease source.</summary>
    StoreLocation Incidents { get; }

    /// <summary>Every stored definition with its expectations and pause, and the store's clock.</summary>
    ValueTask<StoredExpectations> GetExpectationsAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Deletes up to <paramref name="limit"/> queued key changes of the definition and applies them while it expects
    /// completions (<paramref name="window"/> set): a key with an unpaused recurrence and no deadline starts
    /// <paramref name="window"/>, and one without, unless it is the definition-alone key of a definition expected as a
    /// whole, stops being tracked and has its open <see cref="WorkIncidentKind.CompletesBy"/> incident resolved. A key
    /// whose state another transaction holds is queued again. Returns how many changes it deleted.
    /// </summary>
    ValueTask<int> ApplyChangesAsync(string definition, long revision, ExpectationWindow? window, int limit, CancellationToken cancellationToken);

    /// <summary>
    /// Brings the definition-alone key in line with whether the definition, expecting completions, has recurrences:
    /// without them (<paramref name="whole"/>) it is tracked as the whole definition, starting <paramref name="window"/>
    /// unless it has a deadline; with them it is tracked only while a recurrence without subject is unpaused, otherwise it
    /// stops being tracked and its open <see cref="WorkIncidentKind.CompletesBy"/> incident is resolved.
    /// </summary>
    ValueTask TrackWholeAsync(string definition, long revision, bool whole, ExpectationWindow window, CancellationToken cancellationToken);

    /// <summary>
    /// Starts tracking up to <paramref name="limit"/> keys of the definition's unpaused recurrences that have no deadline,
    /// with <paramref name="window"/>; it reads every recurrence of the definition, so only a reconciliation uses it.
    /// </summary>
    ValueTask<int> InsertExpectedAsync(string definition, long revision, ExpectationWindow window, int limit, CancellationToken cancellationToken);

    /// <summary>
    /// Stops tracking the deadline of up to <paramref name="limit"/> keys of the definition not expected to complete and
    /// resolves their open <see cref="WorkIncidentKind.CompletesBy"/> incidents. With <paramref name="tracking"/> false no
    /// key is expected; otherwise the keys of unpaused recurrences and the definition expected as a whole are.
    /// </summary>
    ValueTask<int> ClearUnexpectedAsync(string definition, bool tracking, int limit, CancellationToken cancellationToken);

    /// <summary>
    /// Records that every key of the definition is reconciled with its stored revision and pause, when a read without
    /// locks finds nothing left to do: no open incident of a kind it does not expect, no failures without a failure
    /// limit, and no deadline it does not expect or computed from an older revision. False when something is left, or
    /// the revision or pause changed meanwhile.
    /// </summary>
    ValueTask<bool> MarkReconciledAsync(StoredExpectation definition, CancellationToken cancellationToken);

    /// <summary>
    /// Moves the deadline of the definition's windows held by an uncovered date, and with <paramref name="olderRevisions"/>
    /// of windows computed from a revision older than <paramref name="revision"/>, to <paramref name="next"/> of their
    /// window start, placing their recorded completions against it.
    /// </summary>
    ValueTask<int> RescheduleAsync(string definition, long revision, Func<DateTimeOffset, DateTimeOffset> next, bool olderRevisions, CancellationToken cancellationToken);

    /// <summary>
    /// Locks up to <paramref name="limit"/> windows of the definition whose deadline passed, without waiting, applies
    /// <paramref name="process"/> to each with the store's clock, opens the incidents of their missed deadlines and
    /// stores the processed windows, in one transaction. Returns also the definition's earliest deadline after it,
    /// null when none is known.
    /// </summary>
    ValueTask<DeadlinePass> ProcessDeadlinesAsync(string definition, Func<ExpectationWindow, DateTimeOffset, ExpectationStep> process, int limit, CancellationToken cancellationToken);

    /// <summary>
    /// Opens a <see cref="WorkIncidentKind.RunsAtMost"/> incident per key with a recurring-chain occurrence of the
    /// definition, whose attempt its chain holds, that started more than <paramref name="limit"/> ago, naming the job,
    /// with the occurrences locked in shared mode without waiting; returns also when the next such occurrence reaches
    /// the limit. Attempts held by a lease are checked by their holders with <see cref="CheckAttemptAsync"/>.
    /// </summary>
    ValueTask<RunningCheck> CheckRunningAsync(string definition, TimeSpan limit, CancellationToken cancellationToken);

    /// <summary>
    /// Opens a <see cref="WorkIncidentKind.RunsAtMost"/> incident naming the job of <paramref name="lease"/> when its
    /// open attempt has run longer than the running limit of the definition's stored revision, only while the lease is
    /// held by its epoch; it waits for a transaction holding the job, so a completion committing meanwhile ends the
    /// attempt first. Returns how long until the attempt reaches the stored limit when it has not yet, and null when
    /// nothing is left to check: the incident opened or was already open for the key, the lease is no longer held, or
    /// the stored revision has no running limit.
    /// </summary>
    ValueTask<TimeSpan?> CheckAttemptAsync(Lease<JobId> lease, CancellationToken cancellationToken);

    /// <summary>
    /// Resolves the definition's open incidents of <paramref name="kinds"/>, and with <paramref name="resetFailures"/>
    /// resets its failure counts, while it does not expect them.
    /// </summary>
    ValueTask<IncidentChanges> ResolveRemovedAsync(string definition, IReadOnlyCollection<WorkIncidentKind> kinds, bool resetFailures, CancellationToken cancellationToken);

    /// <summary>
    /// Opens the date-set incidents whose condition holds and resolves the open ones whose condition no longer holds:
    /// <see cref="WorkIncidentKind.DateSetRunningOut"/> for <paramref name="runningOut"/>,
    /// <see cref="WorkIncidentKind.DateSetWorkWaiting"/> for <paramref name="waiting"/>.
    /// </summary>
    ValueTask<IncidentChanges> SyncDateSetsAsync(IReadOnlyCollection<string> runningOut, IReadOnlyCollection<string> waiting, CancellationToken cancellationToken);

    /// <summary>
    /// Incidents newest first by opening time, at most <paramref name="limit"/>, continuing after <paramref name="after"/>;
    /// only open ones with <paramref name="openOnly"/>, only those of <paramref name="definition"/> when it is set.
    /// </summary>
    ValueTask<WorkIncidentPage> GetIncidentsAsync(bool openOnly, string? definition, int limit, WorkIncidentCursor? after, CancellationToken cancellationToken);

    /// <summary>Definitions with expectations, by key, while no live process of the registry monitors.</summary>
    ValueTask<IReadOnlyList<string>> GetUnmonitoredAsync(CancellationToken cancellationToken);

    /// <summary>Records in the process registry that the registered process monitors.</summary>
    ValueTask RegisterMonitorAsync(OwnerId owner, CancellationToken cancellationToken);

    /// <summary>The held incident with the delivery state of its events; null when the lease is no longer valid.</summary>
    ValueTask<IncidentDelivery?> ReadDeliveryAsync(Lease<Guid> lease, CancellationToken cancellationToken);

    /// <summary>Records the delivery of the incident's opening, or of its resolution, fenced to the lease.</summary>
    ValueTask<bool> RecordDeliveryAsync(Lease<Guid> lease, bool resolution, CancellationToken cancellationToken);

    /// <summary>
    /// Gives the incident back in one locked update, fenced to the lease: due again at once when an event still waits
    /// for delivery, such as a resolution committed meanwhile, otherwise not due.
    /// </summary>
    ValueTask<bool> FinishDeliveryAsync(Lease<Guid> lease, CancellationToken cancellationToken);

    /// <summary>
    /// Deletes up to <paramref name="limit"/> incidents resolved longer than <paramref name="retention"/> ago, and up to
    /// <paramref name="limit"/> expectation rows without a deadline, failures or open incident, and up to
    /// <paramref name="limit"/> queued key changes of definitions that expect no completions; returns the most one of
    /// them deleted.
    /// </summary>
    ValueTask<int> RetainAsync(TimeSpan retention, int limit, CancellationToken cancellationToken);

    /// <summary>Open incidents by kind, and the age of each job definition's oldest due job.</summary>
    ValueTask<MonitoringGauges> GetGaugesAsync(CancellationToken cancellationToken);
}
