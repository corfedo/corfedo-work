namespace Corfedo.Work;

/// <summary>
/// The identity and planning of a recurrence. Its identity is the definition plus its subject, or plus a merge key for
/// definitions without a subject kind, the same identity requests merge by.
/// </summary>
public sealed record RecurrenceOptions
{
    public static RecurrenceOptions Default { get; } = new();

    /// <summary>The domain record the recurrence acts on; its kind must be the definition's subject kind.</summary>
    public Subject? Subject { get; init; }

    /// <summary>The identity of a recurrence of a definition without a subject kind.</summary>
    public string? MergeKey { get; init; }

    public Missed Missed { get; init; } = Missed.Skip;

    public Overlap Overlap { get; init; } = Overlap.Wait;

    /// <summary>The partition of the recurrence's occurrences, such as a tenant; null for none.</summary>
    public string? Partition { get; init; }

    /// <summary>
    /// <see cref="RequestMode.Merge"/> creates the recurrence or replaces its schedule; <see cref="RequestMode.IfAbsent"/>
    /// creates it only when the identity has no recurrence. <see cref="RequestMode.New"/>
    /// is refused: an identity has one recurrence.
    /// </summary>
    public RequestMode Mode { get; init; } = RequestMode.Merge;
}

/// <summary>
/// One recurrence as a store applies it. <see cref="Identity"/> is the canonical merge identity: the subject, the merge
/// key, or <see cref="JobRequestData.DefinitionIdentity"/> for a definition's code-defined recurrence.
/// </summary>
public sealed record RecurrenceData(byte[] Args, int ArgsVersion, Schedule Schedule, Missed Missed, Overlap Overlap, string Identity, Subject? Subject, string? Partition, RequestMode Mode);

/// <summary>What scheduling a recurrence did.</summary>
public enum RecurrenceOutcome
{
    /// <summary>A new recurrence was created.</summary>
    Created,

    /// <summary>The recurrence's schedule, policies and arguments were replaced; they apply from its next occurrence.</summary>
    Replaced,

    /// <summary>
    /// <see cref="RequestMode.IfAbsent"/> found a recurrence or an outstanding job with the identity, or a declared
    /// recurrence was stored at the same or a newer revision; nothing changed.
    /// </summary>
    Existing,
}

/// <summary>The instants of a planning step, as <see cref="IRecurrenceCalculator.Plan"/> decides them.</summary>
public readonly record struct RecurrenceInstants(DateTimeOffset? Planned, DateTimeOffset? Next);

/// <summary>
/// Computes the instants of recurring schedules for a store, so every process plans alike with the store's clock
/// reading. Implemented with Cronos by the jobs layer.
/// </summary>
public interface IRecurrenceCalculator
{
    /// <summary>
    /// The first instant of a new or replaced schedule: the next cron or grid instant after <paramref name="now"/>. An
    /// after-completion schedule is due the interval after <paramref name="lastEnded"/>, the end of the recurrence's
    /// last occurrence that advanced an after-completion schedule, and at <paramref name="now"/> when that is earlier or
    /// there is none. Null when there is no instant.
    /// </summary>
    DateTimeOffset? First(Schedule schedule, DateTimeOffset now, DateTimeOffset? lastEnded);

    /// <summary>
    /// Applies the missed rule to the instants from <paramref name="next"/> up to <paramref name="now"/>: the instant to
    /// plan, if any, and the next instant to plan, null for an after-completion schedule, whose next instant follows
    /// from the end of the occurrence it plans.
    /// </summary>
    RecurrenceInstants Plan(Schedule schedule, Missed missed, DateTimeOffset next, DateTimeOffset now);
}

/// <summary>
/// What one planning pass of a definition did, and the definition's stored revision it read; null when the pass found
/// the definition being stored and planned nothing.
/// </summary>
public readonly record struct PlanningPass(int Recurrences, int Occurrences, TimeSpan? NextIn, long? Revision = null);

/// <summary>
/// What one calendar planning pass of a definition did: the jobs it planned of each kind, so a kind that filled its
/// limit may have more, and when the next calendar instant or window end comes.
/// </summary>
public readonly record struct CalendarPass(int Instants, int Closed, TimeSpan? NextIn);
