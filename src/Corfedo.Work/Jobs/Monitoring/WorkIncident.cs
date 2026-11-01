namespace Corfedo.Work;

/// <summary>
/// A broken expectation: its kind, the definition and subject or the date set, the job that broke it when there is one,
/// and when it opened and resolved. A key has at most one open incident of each kind.
/// </summary>
public sealed record WorkIncident(
    Guid Id,
    WorkIncidentKind Kind,
    string? Definition,
    Subject? Subject,
    string? DateSet,
    JobId? Job,
    DateTimeOffset OpenedAt,
    DateTimeOffset? ResolvedAt);

/// <summary>Which expectation an incident reports.</summary>
public enum WorkIncidentKind
{
    /// <summary>An expected key had no successful completion in the window of a deadline.</summary>
    CompletesBy = 0,

    /// <summary>A running attempt exceeded the definition's running limit.</summary>
    RunsAtMost = 1,

    /// <summary>A key failed more times in a row than the definition allows.</summary>
    FailsAtMostInARow = 2,

    /// <summary>A date set's coverage is running out, or work waits for it.</summary>
    DateSetRunningOut = 3,

    /// <summary>Work waits for dates a date set does not cover.</summary>
    DateSetWorkWaiting = 4,
}

/// <summary>Where a page of incidents ended: the opening time and id of its last incident.</summary>
public sealed record WorkIncidentCursor(DateTimeOffset OpenedAt, Guid Id);

/// <summary>Incidents newest first, and the cursor of the next page; null when there are no more.</summary>
public sealed record WorkIncidentPage(IReadOnlyList<WorkIncident> Incidents, WorkIncidentCursor? Next);

/// <summary>
/// Receives incidents in a process that registers it with <c>OnIncident</c>. Each event is delivered at least once,
/// the opening before the resolution; a handler that throws is called again after a backoff.
/// </summary>
public interface IWorkIncidentHandler
{
    ValueTask OpenedAsync(WorkIncident incident, CancellationToken cancellationToken);

    ValueTask ResolvedAsync(WorkIncident incident, CancellationToken cancellationToken);
}
