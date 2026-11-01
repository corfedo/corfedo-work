namespace Corfedo.Work;

/// <summary>
/// The completion window of one expected key: the next deadline it must meet (<see cref="DateTimeOffset.MaxValue"/>
/// while a date set does not cover it), the start of its window, the first completion in the window, and the first and
/// latest completion at or after the deadline while it has not been processed.
/// </summary>
public sealed record ExpectationWindow(
    DateTimeOffset Deadline,
    DateTimeOffset WindowStart,
    DateTimeOffset? MetAt = null,
    DateTimeOffset? FirstAfter = null,
    DateTimeOffset? LastAfter = null)
{
    /// <summary>The deadline of a window whose days a date set does not cover yet.</summary>
    public static DateTimeOffset Uncovered => DateTimeOffset.MaxValue;
}

/// <summary>A window after its passed deadlines were processed, and the deadlines it missed in order.</summary>
public sealed record ExpectationStep(ExpectationWindow Window, IReadOnlyList<MissedDeadline> Missed);

/// <summary>A missed deadline: when its incident opens, and when a late completion resolved it, if one did.</summary>
public readonly record struct MissedDeadline(DateTimeOffset At, DateTimeOffset? ResolvedAt);

/// <summary>
/// A definition's stored expectations at its stored revision, null when it has none, whether it is paused, whether it
/// has recurrences (one without is expected to complete as a whole), whether its keys are reconciled with this revision
/// and pause, and whether key changes are queued for it.
/// </summary>
public sealed record StoredExpectation(string Definition, long Revision, JobExpectation? Expectation, bool Paused, bool HasRecurrences, bool Reconciled, bool HasChanges);

/// <summary>Every stored definition's expectations, read with the store's clock.</summary>
public sealed record StoredExpectations(DateTimeOffset Now, IReadOnlyList<StoredExpectation> Definitions);

/// <summary>What a deadline pass processed and changed, and the definition's earliest deadline after it.</summary>
public sealed record DeadlinePass(int Processed, IncidentChanges Changes, DateTimeOffset? Next);

/// <summary>What a check of running attempts opened, and when the next running attempt reaches the limit.</summary>
public readonly record struct RunningCheck(IReadOnlyList<WorkIncident> Opened, DateTimeOffset? NextAt);

/// <summary>Incidents a monitoring statement opened and resolved.</summary>
public sealed record IncidentChanges(IReadOnlyList<WorkIncident> Opened, IReadOnlyList<WorkIncident> Resolved)
{
    public static IncidentChanges None { get; } = new([], []);

    public bool Any => Opened.Count > 0 || Resolved.Count > 0;
}

/// <summary>An incident with the delivery state of its events, as a delivery claim reads it.</summary>
public sealed record IncidentDelivery(WorkIncident Incident, bool OpenedDelivered, bool ResolvedDelivered);

/// <summary>Store-wide values of the monitoring gauges.</summary>
public sealed record MonitoringGauges(IReadOnlyDictionary<WorkIncidentKind, long> OpenIncidents, IReadOnlyDictionary<string, TimeSpan> OldestDue);
