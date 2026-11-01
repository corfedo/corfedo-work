namespace Corfedo.Work;

/// <summary>
/// When a job is due: once (<see cref="Now"/>, <see cref="At"/>, <see cref="After"/>, <see cref="Debounce"/>) or
/// recurring (<see cref="Every"/>, <see cref="Cron"/>). Due times are eligibility, not start guarantees.
/// </summary>
public sealed record Schedule
{
    private Schedule(ScheduleKind kind)
    {
        Kind = kind;
    }

    public ScheduleKind Kind { get; }

    /// <summary>The due time of <see cref="ScheduleKind.At"/>.</summary>
    public DateTimeOffset Time { get; private init; }

    /// <summary>The delay of <see cref="ScheduleKind.After"/> and <see cref="ScheduleKind.Debounce"/>.</summary>
    public TimeSpan Delay { get; private init; }

    /// <summary>The ceiling of <see cref="ScheduleKind.Debounce"/>, from the first outstanding request.</summary>
    public TimeSpan Maximum { get; private init; }

    /// <summary>The interval of <see cref="ScheduleKind.Every"/>.</summary>
    public TimeSpan Period { get; private init; }

    /// <summary>How <see cref="ScheduleKind.Every"/> measures its interval.</summary>
    public Interval Interval { get; private init; }

    /// <summary>The instant the grid of <see cref="Interval.FixedRate"/> starts from.</summary>
    public DateTimeOffset Anchor { get; private init; }

    /// <summary>The cron expression of <see cref="ScheduleKind.Cron"/>, in Cronos syntax.</summary>
    public string? Expression { get; private init; }

    /// <summary>The time zone id of <see cref="ScheduleKind.Cron"/>, such as <c>Europe/Warsaw</c> or <c>UTC</c>.</summary>
    public string? TimeZone { get; private init; }

    /// <summary>Whether the schedule creates occurrences of a recurrence rather than one job.</summary>
    public bool Recurring => Kind is ScheduleKind.Every or ScheduleKind.Cron;

    /// <summary>The calendar that decides when the work may start (J16), or null for none.</summary>
    public string? Calendar { get; private init; }

    /// <summary>What a one-shot schedule does when its planned instant is blocked by the calendar.</summary>
    public Blocked Blocked { get; private init; }

    /// <summary>What happens to work whose allowed window closes before it could start.</summary>
    public Closed Closed { get; private init; }

    /// <summary>As soon as possible; merging keeps the earliest due time.</summary>
    public static Schedule Now { get; } = new(ScheduleKind.Now);

    /// <summary>At <paramref name="time"/>; merging keeps the earliest due time.</summary>
    public static Schedule At(DateTimeOffset time) => new(ScheduleKind.At) { Time = time };

    /// <summary><paramref name="delay"/> after the request, by the store's clock; merging keeps the earliest due time.</summary>
    public static Schedule After(TimeSpan delay)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        return new(ScheduleKind.After) { Delay = delay };
    }

    /// <summary>
    /// <paramref name="delay"/> after the latest request, at most <paramref name="maximum"/> after the first
    /// outstanding one; merging moves the due time later, up to that ceiling.
    /// </summary>
    public static Schedule Debounce(TimeSpan delay, TimeSpan maximum)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(maximum, delay);
        return new(ScheduleKind.Debounce) { Delay = delay, Maximum = maximum };
    }

    /// <summary>
    /// Recurring every <paramref name="period"/>: <see cref="Interval.AfterCompletion"/> measures it from the end of
    /// the previous occurrence; <see cref="Interval.FixedRate"/> plans occurrences on a grid that starts at
    /// <paramref name="anchor"/>, by default the Unix epoch, so every process computes the same instants.
    /// </summary>
    public static Schedule Every(TimeSpan period, Interval interval, DateTimeOffset? anchor = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(period, TimeSpan.Zero);
        if (!Enum.IsDefined(interval))
        {
            throw new ArgumentOutOfRangeException(nameof(interval));
        }
        if (anchor is not null && interval != Interval.FixedRate)
        {
            throw new ArgumentException("Only a fixed-rate interval has an anchor.", nameof(anchor));
        }
        return new(ScheduleKind.Every) { Period = period, Interval = interval, Anchor = anchor ?? DateTimeOffset.UnixEpoch };
    }

    /// <summary>
    /// Recurring on a cron <paramref name="expression"/> in <paramref name="timeZone"/>, with Cronos syntax and
    /// daylight-saving behaviour: five fields, or six with seconds. Validated when it is scheduled; an expression with
    /// no future occurrence is refused.
    /// </summary>
    public static Schedule Cron(string expression, string timeZone)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);
        ArgumentException.ThrowIfNullOrWhiteSpace(timeZone);
        return new(ScheduleKind.Cron) { Expression = expression.Trim(), TimeZone = timeZone };
    }

    /// <summary>
    /// The schedule within the stored calendar <paramref name="calendar"/>. It is checked when the due time is planned
    /// and when the work starts: a recurring instant that is blocked is recorded skipped, a one-shot one follows
    /// <paramref name="blocked"/>; work whose window closes before it could start follows <paramref name="closed"/>.
    /// Retries and operator actions plan at the next allowed instant.
    /// </summary>
    public Schedule Within(string calendar, Blocked blocked = Blocked.MoveForward, Closed closed = Closed.Wait)
    {
        WorkCalendar.CheckName(calendar, nameof(calendar));
        if (!Enum.IsDefined(blocked) || !Enum.IsDefined(closed))
        {
            throw new ArgumentOutOfRangeException(nameof(blocked), "Unknown blocked or closed policy.");
        }
        if (Recurring && blocked != Blocked.MoveForward)
        {
            throw new ArgumentException("A recurring instant that the calendar blocks is recorded skipped; Blocked applies to one-shot schedules.", nameof(blocked));
        }
        return this with { Calendar = calendar, Blocked = blocked, Closed = closed };
    }
}

/// <summary>What planning does with a one-shot due time that its calendar blocks (J16).</summary>
public enum Blocked
{
    /// <summary>Plans it at the next allowed instant.</summary>
    MoveForward,

    /// <summary>Ends it as skipped at its due time, with reason <c>calendar_blocked</c>.</summary>
    Skip,

    /// <summary>
    /// Refuses the request with <see cref="CalendarBlockedException"/>; a due time found blocked only when it is
    /// planned, after a date set changed, ends skipped.
    /// </summary>
    Reject,
}

/// <summary>What happens to work that was due inside an allowed window but could not start before it closed (J16).</summary>
public enum Closed
{
    /// <summary>Planned again at the next allowed instant.</summary>
    Wait,

    /// <summary>Ended as skipped with reason <c>window_closed</c>.</summary>
    Skip,
}

public enum ScheduleKind
{
    Now,
    At,
    After,
    Debounce,
    Every,
    Cron,
}

/// <summary>How a recurring <see cref="Schedule.Every"/> measures its interval.</summary>
public enum Interval
{
    /// <summary>Each occurrence is due the interval after the previous one ended.</summary>
    AfterCompletion,

    /// <summary>Occurrences are due on a fixed grid from the anchor, however long each one runs.</summary>
    FixedRate,
}

/// <summary>What the planner does with recurring instants whose due time passed before they were planned (J2).</summary>
public enum Missed
{
    /// <summary>Several missed instants plan none; the recurrence continues from the next future instant.</summary>
    Skip,

    /// <summary>Several missed instants plan only the latest, subject to <see cref="Late"/>.</summary>
    RunLatest,
}

/// <summary>What happens when an occurrence becomes due while another one of its recurrence is outstanding (J15).</summary>
public enum Overlap
{
    /// <summary>The next occurrence waits for the outstanding one to end; several waiting ones collapse into the latest.</summary>
    Wait,

    /// <summary>The next occurrence is recorded skipped, with its outcome hook.</summary>
    Skip,

    /// <summary>Every occurrence becomes its own job when due; not combined with <see cref="Interval.AfterCompletion"/>.</summary>
    Allow,
}

/// <summary>How a request treats an outstanding job with the same merge identity.</summary>
public enum RequestMode
{
    /// <summary>
    /// Before the job's execution started and while no claim has taken its lease, the latest arguments replace the
    /// stored ones and the schedule merges. Once the execution started or a claim took the lease, the request becomes
    /// the job's pending follow-up, and the current execution keeps its arguments and execution id.
    /// </summary>
    Merge,

    /// <summary>Creates the job only when none is outstanding; an existing one is left untouched.</summary>
    IfAbsent,

    /// <summary>Never merges: the request is its own job.</summary>
    New,
}

/// <summary>The identity and placement of a request.</summary>
public sealed record JobRequestOptions
{
    public static JobRequestOptions Default { get; } = new();

    /// <summary>The domain record the job acts on; its kind must be the definition's subject kind.</summary>
    public Subject? Subject { get; init; }

    /// <summary>The merge identity of a job without a subject.</summary>
    public string? MergeKey { get; init; }

    /// <summary>The partition, such as a tenant, for partition limits; null for none.</summary>
    public string? Partition { get; init; }

    public RequestMode Mode { get; init; } = RequestMode.Merge;
}

/// <summary>
/// One request as a store applies it: serialized arguments with their version, schedule and identity, and the W3C id of
/// the requester's activity, which a job it creates keeps for the span links of its runs.
/// </summary>
public sealed record JobRequestData(byte[] Args, int ArgsVersion, Schedule Schedule, Subject? Subject, string? MergeKey, string? Partition, RequestMode Mode, string? TraceParent = null)
{
    /// <summary>The canonical merge identity, or null when the request never merges.</summary>
    public string? MergeIdentity => Mode == RequestMode.New ? null : Identity(Subject, MergeKey);

    /// <summary>
    /// The canonical merge identity of a subject, or of a merge key for jobs without one; null for neither. A subject's
    /// kind and identifier are joined with a newline, which kinds cannot contain; merge keys start with U+0001.
    /// </summary>
    public static string? Identity(Subject? subject, string? mergeKey) =>
        subject is not null ? subject.Kind + "\n" + subject.Id
        : mergeKey is not null ? "\u0001" + mergeKey
        : null;

    /// <summary>The identity of a definition's code-defined recurrence, which has neither subject nor merge key.</summary>
    public const string DefinitionIdentity = "\u0002";
}

/// <summary>What a request did.</summary>
public enum JobRequestOutcome
{
    /// <summary>A new job was created.</summary>
    Created,

    /// <summary>
    /// The request replaced the arguments and merged the schedule of a job whose execution had not started and whose
    /// lease no claim had taken.
    /// </summary>
    Merged,

    /// <summary>
    /// The request became the follow-up of an execution that started or whose lease a claim took, and runs once that
    /// one ends as a new execution of the same job.
    /// </summary>
    FollowUp,

    /// <summary><see cref="RequestMode.IfAbsent"/> found an outstanding job and left it untouched.</summary>
    Existing,
}

public readonly record struct JobRequestResult(JobId Id, JobRequestOutcome Outcome);

/// <summary>
/// A request with <see cref="Blocked.Reject"/> names an instant its calendar blocks, or one it cannot decide because a
/// date set does not cover it yet (<see cref="Unavailable"/>); nothing was written.
/// </summary>
public sealed class CalendarBlockedException(string message, bool unavailable) : Exception(message)
{
    /// <summary>Whether the instant needs dates a date set does not cover yet.</summary>
    public bool Unavailable { get; } = unavailable;
}

/// <summary>A request would merge into an outstanding job of another partition.</summary>
public sealed class JobPartitionMismatchException(string message) : Exception(message);

