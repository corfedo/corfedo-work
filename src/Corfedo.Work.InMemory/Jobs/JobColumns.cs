using Corfedo.Work.InMemory.Database;

namespace Corfedo.Work.InMemory.Jobs;

/// <summary>
/// The columns of the framework's job table, named as the PostgreSQL store names them, so that job sources claim job
/// records with the default lease fields. Outstanding jobs have state 0; terminal ones keep their outcome.
/// </summary>
internal static class JobColumns
{
    public const string Id = "id";
    public const string Definition = "definition";
    public const string MergeKey = "merge_key";
    public const string SubjectKind = "subject_kind";
    public const string SubjectId = "subject_id";
    public const string Partition = "partition_key";
    public const string State = "state";
    public const string Args = "args";
    public const string ArgsVersion = "args_version";
    public const string FirstRequestedAt = "first_requested_at";
    public const string Started = "started";
    public const string Attempts = "attempts";
    public const string AttemptEpoch = "attempt_epoch";
    public const string PendingArgs = "pending_args";
    public const string PendingArgsVersion = "pending_args_version";
    public const string PendingDue = "pending_due";
    public const string PendingFirstRequestedAt = "pending_first_requested_at";
    public const string Outcome = "outcome";
    public const string Reason = "reason";
    public const string EndedAt = "ended_at";
    public const string CreatedAt = "created_at";
    public const string RecurrenceId = "recurrence_id";
    public const string Occurrence = "occurrence";
    public const string Unconsumed = "unconsumed";
    public const string SkipPending = "skip_pending";
    public const string Paused = "paused";
    public const string AttemptStartedAt = "attempt_started_at";
    public const string ProgressPercent = "progress_percent";
    public const string ProgressMessage = "progress_message";
    public const string EndedExecutionId = "ended_execution_id";
    public const string Result = "result";
    public const string ResultVersion = "result_version";
    public const string Waited = "waited";
    public const string Calendar = "calendar";
    public const string EndedCalendar = "ended_calendar";
    public const string CalendarBlocked = "calendar_blocked";
    public const string CalendarClosed = "calendar_closed";
    public const string CalendarAt = "calendar_at";
    public const string WindowEnd = "window_end";
    public const string CalendarUnavailable = "calendar_unavailable";
    public const string SkipReason = "skip_reason";
    public const string ChainId = "chain_id";
    public const string ChainGeneration = "chain_generation";
    public const string ChainParent = "chain_parent";
    public const string ChainGroup = "chain_group";
    public const string ChainCondition = "chain_condition";
    public const string ChainWaiting = "chain_waiting";
    public const string OwnedChain = "owned_chain";
    public const string ChainOutcome = "chain_outcome";
    public const string TraceParent = "trace_parent";

    /// <summary>Every column; the key is <see cref="Id"/>.</summary>
    public static IEnumerable<string> All
    {
        get
        {
            var f = LeaseFields.Default;
            return
            [
                Definition, MergeKey, SubjectKind, SubjectId, Partition, State, Args, ArgsVersion, FirstRequestedAt, Started, Attempts,
                AttemptEpoch, PendingArgs, PendingArgsVersion, PendingDue, PendingFirstRequestedAt, Outcome, Reason, EndedAt, CreatedAt,
                RecurrenceId, Occurrence, Unconsumed, SkipPending, Paused, AttemptStartedAt, ProgressPercent, ProgressMessage,
                EndedExecutionId, Result, ResultVersion, Waited, Calendar, EndedCalendar, CalendarBlocked, CalendarClosed, CalendarAt,
                WindowEnd, CalendarUnavailable, SkipReason, ChainId, ChainGeneration, ChainParent, ChainGroup, ChainCondition,
                ChainWaiting, OwnedChain, ChainOutcome, TraceParent,
                f.DueAt, f.LeaseOwner, f.LeaseEpoch, f.LeaseExpiresAt, f.Takeovers, f.Failures, f.SettledEpoch, f.SettleFailures,
                f.SettleRetryAt, f.CancelRequestedAt, f.ExecutionId, f.EndedCompletionId, f.EndedDigest,
            ];
        }
    }

    public static string? Text(this Record r, string column) => (string?)r[column];

    public static bool Bool(this Record r, string column) => r[column] is true;

    public static byte[]? Bytes(this Record r, string column) => (byte[]?)r[column];

    /// <summary>Whether the job is outstanding: state 0.</summary>
    public static bool Outstanding(this Record r) => r.Int32(State) == 0;

    public static JobId JobId(this Record r) => new(r.Uuid(Id)!.Value);

    public static Subject? Subject(this Record r) =>
        r.Text(SubjectKind) is { } kind ? global::Corfedo.Work.Subject.Of(kind, r.Text(SubjectId)!) : null;

    /// <summary>The pause key of the job's subject, or null without one.</summary>
    public static PauseKey? PauseKey(this Record r) =>
        r.Text(SubjectKind) is { } kind ? new PauseKey(r.Text(Definition)!, kind, r.Text(SubjectId)!) : null;
}
