namespace Corfedo.Work;

/// <summary>Starts the expectations of a definition (M1): <c>Expect.CompletesBy(...).RunsAtMost(...).FailsAtMostInARow(...)</c>.</summary>
public static class Expect
{
    /// <inheritdoc cref="JobExpectation.CompletesBy"/>
    public static JobExpectation CompletesBy(TimeOnly time, string timeZone, string? calendar = null) => JobExpectation.None.CompletesBy(time, timeZone, calendar);

    /// <inheritdoc cref="JobExpectation.RunsAtMost"/>
    public static JobExpectation RunsAtMost(TimeSpan duration) => JobExpectation.None.RunsAtMost(duration);

    /// <inheritdoc cref="JobExpectation.FailsAtMostInARow"/>
    public static JobExpectation FailsAtMostInARow(int failures) => JobExpectation.None.FailsAtMostInARow(failures);
}

/// <summary>
/// What a definition's jobs are expected to do, tracked per subject: a key is the definition with a job's subject, or
/// the definition alone for jobs without one. Part of the definition's policy, stored with its revision.
/// </summary>
public sealed record JobExpectation
{
    private JobExpectation()
    {
    }

    internal static JobExpectation None { get; } = new();

    /// <summary>The completion deadline, or null.</summary>
    public CompletionDeadline? Deadline { get; private init; }

    /// <summary>The longest an attempt may run, or null.</summary>
    public TimeSpan? RunningLimit { get; private init; }

    /// <summary>The most failures in a row, or null.</summary>
    public int? FailureLimit { get; private init; }

    /// <summary>
    /// Every expected key completes successfully by <paramref name="time"/> in <paramref name="timeZone"/> on each day
    /// <paramref name="calendar"/> allows any instant of, or on every day without a calendar. The window of a deadline
    /// starts at the previous one.
    /// </summary>
    /// <exception cref="TimeZoneNotFoundException">The time zone is not known on this system.</exception>
    public JobExpectation CompletesBy(TimeOnly time, string timeZone, string? calendar = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(timeZone);
        TimeZoneInfo.FindSystemTimeZoneById(timeZone);
        if (calendar is not null)
        {
            WorkCalendar.CheckName(calendar, nameof(calendar));
        }
        return this with { Deadline = new CompletionDeadline(time, timeZone, calendar) };
    }

    /// <summary>No attempt of the definition runs longer than <paramref name="duration"/>.</summary>
    public JobExpectation RunsAtMost(TimeSpan duration)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(duration, TimeSpan.Zero);
        return this with { RunningLimit = duration };
    }

    /// <summary>
    /// No key ends more than <paramref name="failures"/> times in a row as <see cref="JobOutcome.Failed"/> or
    /// <see cref="JobOutcome.TimedOut"/>; a completion resets the count, cancellations and skips leave it.
    /// </summary>
    public JobExpectation FailsAtMostInARow(int failures)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(failures);
        return this with { FailureLimit = failures };
    }

    /// <summary>Whether no expectation is set.</summary>
    public bool IsEmpty => Deadline is null && RunningLimit is null && FailureLimit is null;
}

/// <summary>A daily completion deadline: a local time in a time zone, on the days a calendar allows.</summary>
public sealed record CompletionDeadline(TimeOnly Time, string TimeZone, string? Calendar);

/// <summary>The kinds of key incidents an expectation covers.</summary>
internal static class ExpectationKinds
{
    /// <summary>The kinds of key incidents <paramref name="expectation"/> does not expect.</summary>
    public static WorkIncidentKind[] Unexpected(JobExpectation? expectation) =>
    [
        .. expectation?.Deadline is null ? [WorkIncidentKind.CompletesBy] : Array.Empty<WorkIncidentKind>(),
        .. expectation?.RunningLimit is null ? [WorkIncidentKind.RunsAtMost] : Array.Empty<WorkIncidentKind>(),
        .. expectation?.FailureLimit is null ? [WorkIncidentKind.FailsAtMostInARow] : Array.Empty<WorkIncidentKind>(),
    ];
}
