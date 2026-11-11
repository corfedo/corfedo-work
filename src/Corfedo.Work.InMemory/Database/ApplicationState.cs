namespace Corfedo.Work.InMemory.Database;

/// <summary>
/// The framework state of one application, isolated from other applications of the database as a framework schema
/// isolates them: source generations, job definitions, jobs and their recurrences, pauses and run history, chains,
/// calendars and date sets, expectation state and incidents, the process registry and the listeners of its signals.
/// </summary>
internal sealed class ApplicationState(string name, DomainTable jobs, DomainTable incidents)
{
    private readonly Lock sync = new();
    private readonly List<Action<SignalTopic, string>> listeners = [];

    public string Name { get; } = name;

    /// <summary>The generation of each reconciliation source that was invalidated.</summary>
    public Table<string, Generation> Generations { get; } = new();

    public Table<OwnerId, ProcessRow> Processes { get; } = new();

    /// <summary>The newest stored revision of each job definition.</summary>
    public Table<string, DefinitionRow> Definitions { get; } = new();

    /// <summary>
    /// Job records of every definition, keyed by id, with the columns of the framework's job table, so job sources claim
    /// them as lease records.
    /// </summary>
    public DomainTable Jobs { get; } = jobs;

    public Table<Guid, RecurrenceRow> Recurrences { get; } = new();

    public Table<PauseKey, PauseRow> Pauses { get; } = new();

    public Table<RunKey, RunRow> Runs { get; } = new();

    /// <summary>Keys whose recurrences changed, queued for the monitoring loop, in queue order.</summary>
    public Table<long, ExpectationChange> ExpectationChanges { get; } = new();

    /// <summary>Chains with their top level as a group; their steps are jobs.</summary>
    public Table<Guid, ChainRow> Chains { get; } = new();

    public Table<Guid, ChainGroupRow> ChainGroups { get; } = new();

    /// <summary>The newest stored revision of each calendar.</summary>
    public Table<string, CalendarRow> Calendars { get; } = new();

    /// <summary>The newest published revision of each date set.</summary>
    public Table<string, CalendarDates> DateSets { get; } = new();

    /// <summary>The expectation state of each key: a definition with a subject, or alone.</summary>
    public Table<ExpectationKey, ExpectationRow> Expectations { get; } = new();

    /// <summary>
    /// Incidents with the delivery times of both events, keyed by id, with the columns of the framework's incident table,
    /// so the incident source claims them as lease records.
    /// </summary>
    public DomainTable Incidents { get; } = incidents;

    public void Listen(Action<SignalTopic, string> listener)
    {
        lock (sync)
        {
            listeners.Add(listener);
        }
    }

    public void Unlisten(Action<SignalTopic, string> listener)
    {
        lock (sync)
        {
            listeners.Remove(listener);
        }
    }

    public void Deliver(SignalTopic topic, string key)
    {
        Action<SignalTopic, string>[] current;
        lock (sync)
        {
            current = [.. listeners];
        }
        foreach (var listener in current)
        {
            listener(topic, key);
        }
    }
}

internal sealed record Generation(long Value);

/// <summary>
/// A process record with what other layers recorded for it: the job definitions and calendars it registers and whether
/// it monitors.
/// </summary>
internal sealed record ProcessRow(
    OwnerId Owner,
    string Type,
    string? Release,
    TimeSpan StaleAfter,
    DateTimeOffset StartedAt,
    DateTimeOffset HeartbeatAt,
    DateTimeOffset? StoppedAt)
{
    public IReadOnlyList<ProcessDefinition> Definitions { get; init; } = [];

    public IReadOnlyList<(string Calendar, long Revision)> Calendars { get; init; } = [];

    public bool Monitors { get; init; }

    public ProcessState StateAt(DateTimeOffset now) =>
        StoppedAt is not null ? ProcessState.Stopped : HeartbeatAt + StaleAfter > now ? ProcessState.Live : ProcessState.Stale;

    /// <summary>Not stopped, with a heartbeat within its stale window.</summary>
    public bool LiveAt(DateTimeOffset now) => StateAt(now) == ProcessState.Live;

    /// <summary>Live at some instant from <paramref name="at"/> on: not stopped before it, with a heartbeat not stale at it.</summary>
    public bool LiveSince(DateTimeOffset at) => (StoppedAt is null || StoppedAt > at) && HeartbeatAt + StaleAfter > at;
}

/// <summary>
/// A stored job definition with its pause state, and the revision and pause its expectation state was last reconciled
/// with.
/// </summary>
internal sealed record DefinitionRow(StoredJobDefinition Stored, DateTimeOffset StoredAt)
{
    public long? MonitoredRevision { get; init; }

    public bool? MonitoredPaused { get; init; }
}

/// <summary>
/// A recurrence of a job definition, keyed by its merge identity within the definition. <see cref="NextAt"/> is the next
/// instant to plan, <see cref="WaitingAt"/> the instant of an occurrence waiting for an outstanding one,
/// <see cref="Awaited"/> the occurrence an after-completion schedule waits for, and <see cref="Paused"/> the copied
/// pause flag of its subject.
/// </summary>
internal sealed record RecurrenceRow(
    Guid Id,
    string Definition,
    string MergeKey,
    Subject? Subject,
    string Partition,
    byte[] Args,
    int ArgsVersion,
    Schedule? Schedule,
    Missed Missed,
    Overlap Overlap,
    Guid? Awaited,
    long? DeclaredRevision,
    byte[]? DeclaredDigest,
    DateTimeOffset? NextAt,
    DateTimeOffset? WaitingAt,
    bool Paused,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt)
{
    /// <summary>The recurrence's calendar, which every occurrence it creates runs within.</summary>
    public string? Calendar { get; init; }

    /// <summary>The closed-window policy of its occurrences when it has a calendar.</summary>
    public Closed? CalendarClosed { get; init; }

    /// <summary>The end of the last occurrence that advanced the after-completion schedule; null for other schedules.</summary>
    public DateTimeOffset? LastEndedAt { get; init; }
}

internal readonly record struct PauseKey(string Definition, string SubjectKind, string SubjectId)
{
    public static PauseKey Of(string definition, Subject subject) => new(definition, subject.Kind, subject.Id);
}

internal sealed record PauseRow(DateTimeOffset PausedAt);

/// <summary>A run history entry, keyed by its job and the lease epoch of the claim that used the attempt.</summary>
internal readonly record struct RunKey(Guid Job, long Epoch);

internal sealed record RunRow(string Definition, JobRunEntry Entry);

internal sealed record ExpectationChange(string Definition, Subject? Subject);

/// <summary>
/// A chain: its generation, root step, owning occurrence with its definition, the definitions of its steps, its steps
/// in creation order and its groups, and its top level as a group with the number of nodes pending and the worst
/// outcome rank among those that ran (0 none, 1 succeeded, 2 cancelled, 3 failed).
/// </summary>
internal sealed record ChainRow(
    long Generation,
    Guid RootJob,
    Guid? OwnerJob,
    string? OwnerDefinition,
    IReadOnlyList<string> Definitions,
    IReadOnlyList<Guid> Steps,
    IReadOnlyList<Guid> Groups,
    DateTimeOffset CreatedAt,
    DateTimeOffset? SettledAt,
    JobOutcome? Outcome,
    int Pending,
    int Worst,
    bool DeleteRequested);

/// <summary>A group of a chain: the node it follows, its enclosing group, and its pending count and worst rank.</summary>
internal sealed record ChainGroupRow(
    Guid Chain,
    long Generation,
    Guid Parent,
    Guid? EnclosingGroup,
    int Pending,
    int Worst,
    DateTimeOffset? SettledAt,
    JobOutcome? Outcome,
    bool Dynamic);

/// <summary>A stored calendar with the time it was stored.</summary>
internal sealed record CalendarRow(WorkCalendar Calendar, DateTimeOffset StoredAt);

/// <summary>An expectation key: a definition with a subject, or alone with a null subject.</summary>
internal readonly record struct ExpectationKey(string Definition, string? SubjectKind, string? SubjectId)
{
    public static ExpectationKey Of(string definition, Subject? subject) => new(definition, subject?.Kind, subject?.Id);

    public Subject? Subject => SubjectKind is null ? null : global::Corfedo.Work.Subject.Of(SubjectKind, SubjectId!);
}

/// <summary>
/// The expectation state of one key: failures in a row and, while it is expected to complete, its deadline
/// (<see cref="ExpectationWindow.Uncovered"/> while a date set does not cover it) with its window and the revision it was
/// computed from. <see cref="Whole"/> marks the definition-alone key of a definition expected as a whole.
/// </summary>
internal sealed record ExpectationRow(
    bool Whole = false,
    int Failures = 0,
    DateTimeOffset? DeadlineAt = null,
    DateTimeOffset? WindowStart = null,
    DateTimeOffset? MetAt = null,
    DateTimeOffset? FirstAfter = null,
    DateTimeOffset? LastAfter = null,
    long? Revision = null);
