namespace Corfedo.Work.SqlServer;

internal sealed class StubJobStore(string schema) : IJobStore
{
    public StoreLocation Table { get; } = StoreLocation.Table(schema, "jobs");
    public ValueTask<StoredJobDefinition> StoreDefinitionAsync(JobDefinition definition, JobRetention retention, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<StoredJobDefinition?> GetDefinitionAsync(string key, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<JobRequestResult> RequestAsync(WorkTransaction transaction, LeaseSource<JobId> source, JobRequestData request, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<JobRequestResult> RequestAsync(LeaseSource<JobId> source, JobRequestData request, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<IReadOnlyList<JobRequestResult>> RequestManyAsync(WorkTransaction transaction, LeaseSource<JobId> source, IReadOnlyList<JobRequestData> requests, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask CompleteAsync(WorkTransaction transaction, Lease<JobId> lease, JobTransition transition, JobEndedCallback? ended, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<bool> CompleteAsync(Lease<JobId> lease, JobTransition transition, JobEndedCallback? ended, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<int> CancelAsync(WorkTransaction transaction, LeaseSource<JobId> source, Subject? subject, string? mergeKey, JobEndedCallback? ended, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<int> CancelAllAsync(WorkTransaction transaction, Subject subject, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<int> RetireAsync(LeaseSource<JobId> source, JobEndedCallback? ended, int limit, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask PauseDefinitionAsync(WorkTransaction transaction, LeaseSource<JobId> source, bool paused, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask PauseSubjectAsync(WorkTransaction transaction, LeaseSource<JobId> source, Subject subject, bool paused, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<RecurrenceOutcome> ScheduleAsync(WorkTransaction transaction, LeaseSource<JobId> source, RecurrenceData recurrence, IRecurrenceCalculator calculator, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<RecurrenceOutcome> DeclareRecurrenceAsync(LeaseSource<JobId> source, RecurrenceData recurrence, long revision, byte[] digest, IRecurrenceCalculator calculator, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<bool> RemoveAsync(WorkTransaction transaction, LeaseSource<JobId> source, string identity, JobEndedCallback? ended, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<RunNowResult> TriggerAsync(WorkTransaction transaction, LeaseSource<JobId> source, string identity, bool ignoreCalendar, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<RunNowResult> RunNowAsync(LeaseSource<JobId> source, JobId job, bool ignoreCalendar, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<RerunResult> RerunAsync(LeaseSource<JobId> source, JobId job, bool ignoreCalendar, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<RetentionPass> RetainAsync(int limit, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<PlanningPass> PlanAsync(LeaseSource<JobId> source, IRecurrenceCalculator calculator, int limit, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<CalendarPass> PlanCalendarAsync(LeaseSource<JobId> source, int limit, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<int> FinalizeSkippedAsync(LeaseSource<JobId> source, JobEndedCallback? ended, int limit, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<int> RepairPausesAsync(LeaseSource<JobId> source, int limit, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<bool> ReportProgressAsync(Lease<JobId> lease, JobProgress progress, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<IReadOnlyDictionary<Subject, JobStatus>> GetStatusAsync(string definition, IReadOnlyList<Subject> subjects, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<bool> WatchAsync(string definition, JobId job, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<IReadOnlyList<StoredJobOutcome>> GetOutcomesAsync(string definition, IReadOnlyList<JobId> jobs, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<JobRunPage> GetRunsAsync(string definition, Subject? subject, JobId? job, int limit, JobRunCursor? after, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask RegisterProcessAsync(OwnerId owner, IReadOnlyList<ProcessDefinition> definitions, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<UnservedReport> GetUnservedAsync(CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<IReadOnlyList<UndeclaredRecurrence>> GetUndeclaredRecurrencesAsync(CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<IReadOnlyList<OutdatedProcess>> GetOutdatedProcessesAsync(CancellationToken cancellationToken) => throw new NotImplementedException();
}

internal sealed class StubChainStore : IChainStore
{
    public ValueTask RequestChainAsync(WorkTransaction transaction, ChainData chain, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<bool> StartOwnedChainAsync(Lease<JobId> lease, ChainData chain, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask AddGroupAsync(WorkTransaction transaction, Lease<JobId> lease, IReadOnlyList<ChainStepData> steps, IReadOnlyList<ChainGroupData> groups, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<StoredJobOutcome?> GetParentOutcomeAsync(JobId step, string definition, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<IReadOnlyList<StoredJobOutcome>> GetGroupOutcomesAsync(JobId step, string definition, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<ChainStatus?> GetChainAsync(ChainId chain, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<RetryFromOutcome> RetryFromAsync(ChainId chain, Guid node, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<int> CancelChainAsync(WorkTransaction transaction, ChainId chain, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<ChainDeleteOutcome> DeleteChainAsync(ChainId chain, ChainDelete mode, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken) => throw new NotImplementedException();
}

internal sealed class StubCalendarStore : ICalendarStore
{
    public ValueTask<WorkCalendar> StoreCalendarAsync(WorkCalendar calendar, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<IReadOnlyList<WorkCalendar>> GetCalendarsAsync(CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<bool> RemoveCalendarAsync(string name, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask RegisterProcessAsync(OwnerId owner, IReadOnlyList<WorkCalendar> calendars, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<IReadOnlyList<UnregisteredCalendar>> GetUnregisteredCalendarsAsync(CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<IReadOnlyList<DateSetCoverage>> GetDateSetCoverageAsync(TimeSpan margin, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<bool> PublishDatesAsync(WorkTransaction transaction, CalendarDates dates, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<IReadOnlyList<CalendarDates>> GetDateSetsAsync(CancellationToken cancellationToken) => throw new NotImplementedException();
}

internal sealed class StubMonitoringStore(string schema) : IMonitoringStore
{
    public StoreLocation Incidents { get; } = StoreLocation.Table(schema, "work_incidents");
    public ValueTask<StoredExpectations> GetExpectationsAsync(CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<int> ApplyChangesAsync(string definition, long revision, ExpectationWindow? window, int limit, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask TrackWholeAsync(string definition, long revision, bool whole, ExpectationWindow window, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<int> InsertExpectedAsync(string definition, long revision, ExpectationWindow window, int limit, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<int> ClearUnexpectedAsync(string definition, bool tracking, int limit, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<bool> MarkReconciledAsync(StoredExpectation definition, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<int> RescheduleAsync(string definition, long revision, Func<DateTimeOffset, DateTimeOffset> next, bool olderRevisions, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<DeadlinePass> ProcessDeadlinesAsync(string definition, Func<ExpectationWindow, DateTimeOffset, ExpectationStep> process, int limit, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<RunningCheck> CheckRunningAsync(string definition, TimeSpan limit, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<TimeSpan?> CheckAttemptAsync(Lease<JobId> lease, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<IncidentChanges> ResolveRemovedAsync(string definition, IReadOnlyCollection<WorkIncidentKind> kinds, bool resetFailures, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<IncidentChanges> SyncDateSetsAsync(IReadOnlyCollection<string> runningOut, IReadOnlyCollection<string> waiting, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<WorkIncidentPage> GetIncidentsAsync(bool openOnly, string? definition, int limit, WorkIncidentCursor? after, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<IReadOnlyList<string>> GetUnmonitoredAsync(CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask RegisterMonitorAsync(OwnerId owner, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<IncidentDelivery?> ReadDeliveryAsync(Lease<Guid> lease, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<bool> RecordDeliveryAsync(Lease<Guid> lease, bool resolution, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<bool> FinishDeliveryAsync(Lease<Guid> lease, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<int> RetainAsync(TimeSpan retention, int limit, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask<MonitoringGauges> GetGaugesAsync(CancellationToken cancellationToken) => throw new NotImplementedException();
}
