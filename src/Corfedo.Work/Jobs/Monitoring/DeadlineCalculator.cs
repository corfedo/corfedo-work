namespace Corfedo.Work;

/// <summary>
/// The deadlines of a <see cref="CompletionDeadline"/> and the processing of a key's window when its deadline passes.
/// A deadline is the local time on every day the calendar allows any instant of, as calendars read local times; a day
/// a date set does not cover holds the deadline (<see cref="ExpectationWindow.Uncovered"/>), and so does a calendar that
/// allows no day within the planning lookahead.
/// </summary>
internal sealed class DeadlineCalculator
{
    private readonly CompletionDeadline deadline;
    private readonly CalendarEvaluator? calendars;
    private readonly TimeZoneInfo zone;
    private readonly int days = (int)CalendarEvaluator.Lookahead.TotalDays + 1;

    /// <param name="calendars">The stored calendars and date sets; needed when the deadline has a calendar.</param>
    public DeadlineCalculator(CompletionDeadline deadline, CalendarEvaluator? calendars)
    {
        ArgumentNullException.ThrowIfNull(deadline);
        if (deadline.Calendar is not null && calendars is null)
        {
            throw new ArgumentException("A deadline with a calendar needs the calendars.", nameof(calendars));
        }
        this.deadline = deadline;
        this.calendars = calendars;
        zone = TimeZoneInfo.FindSystemTimeZoneById(deadline.TimeZone);
    }

    /// <summary>The first deadline after <paramref name="after"/>, or <see cref="ExpectationWindow.Uncovered"/>.</summary>
    public DateTimeOffset Next(DateTimeOffset after)
    {
        var date = CalendarEvaluator.DateAt(zone, after);
        for (var i = 0; i <= days; i++, date = date.AddDays(1))
        {
            var instant = CalendarEvaluator.At(zone, date, deadline.Time);
            if (instant <= after)
            {
                continue;
            }
            switch (Allows(date))
            {
                case CalendarSlotKind.Allowed:
                    return instant;
                case CalendarSlotKind.Unavailable:
                    return ExpectationWindow.Uncovered;
            }
        }
        return ExpectationWindow.Uncovered;
    }

    /// <summary>The last deadline before <paramref name="before"/>; null when a date set does not cover it or none is within the lookahead.</summary>
    public DateTimeOffset? Previous(DateTimeOffset before)
    {
        var date = CalendarEvaluator.DateAt(zone, before);
        for (var i = 0; i <= days; i++, date = date.AddDays(-1))
        {
            var instant = CalendarEvaluator.At(zone, date, deadline.Time);
            if (instant >= before)
            {
                continue;
            }
            switch (Allows(date))
            {
                case CalendarSlotKind.Allowed:
                    return instant;
                case CalendarSlotKind.Unavailable:
                    return null;
            }
        }
        return null;
    }

    /// <summary>
    /// The window of a key first seen at <paramref name="now"/>: first checked at the next deadline, with its window
    /// starting at the deadline before, or at <paramref name="now"/> when that is not known.
    /// </summary>
    public ExpectationWindow First(DateTimeOffset now) => new(Next(now), Previous(now) ?? now);

    /// <summary>
    /// Processes every deadline of <paramref name="window"/> passed by <paramref name="now"/>, in order. A deadline
    /// whose window was already started after it, by a completion that resolved an earlier incident open at the
    /// deadline, adds nothing. Otherwise a completion in the window meets it, and the next window starts at it; without
    /// one it is missed: resolved at the first completion after it, where the next window then starts, or left open,
    /// and a key has one open incident however many deadlines pass. Completions carried into the next window are placed
    /// against its deadline: the first before it meets it, the first and latest at or after it stay completions after
    /// the deadline; others are not distinguished.
    /// </summary>
    public ExpectationStep Process(ExpectationWindow window, DateTimeOffset now)
    {
        ArgumentNullException.ThrowIfNull(window);
        var missed = new List<MissedDeadline>();
        while (window.Deadline <= now)
        {
            var due = window.Deadline;
            DateTimeOffset start;
            IEnumerable<DateTimeOffset?> carried;
            if (window.WindowStart >= due)
            {
                start = window.WindowStart;
                carried = [window.FirstAfter, window.LastAfter];
            }
            else if (window.MetAt is not null)
            {
                start = due;
                carried = [window.FirstAfter, window.LastAfter];
            }
            else if (window.FirstAfter is { } late)
            {
                missed.Add(new MissedDeadline(due, late));
                start = late;
                carried = [window.LastAfter];
            }
            else
            {
                if (missed.Count == 0 || missed[^1].ResolvedAt is not null)
                {
                    missed.Add(new MissedDeadline(due, null));
                }
                start = due;
                carried = [];
            }
            var next = Next(start);
            var completions = carried.OfType<DateTimeOffset>().Where(c => c > start).Distinct().Order().ToArray();
            var after = completions.Where(c => c >= next).ToArray();
            window = new ExpectationWindow(
                next,
                start,
                completions.Where(c => c < next).Select(c => (DateTimeOffset?)c).FirstOrDefault(),
                after.Length > 0 ? after[0] : null,
                after.Length > 0 ? after[^1] : null);
        }
        return new ExpectationStep(window, missed);
    }

    private CalendarSlotKind Allows(DateOnly date)
    {
        if (deadline.Calendar is not { } calendar)
        {
            return CalendarSlotKind.Allowed;
        }
        var from = CalendarEvaluator.At(zone, date, TimeOnly.MinValue);
        var to = CalendarEvaluator.At(zone, date.AddDays(1), TimeOnly.MinValue);
        return calendars!.AllowsAny(calendar, from, to);
    }
}
