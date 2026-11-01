using Cronos;

namespace Corfedo.Work;

/// <summary>
/// The instants of a recurring schedule, computed alike by every process: Cronos occurrences in the schedule's time
/// zone, or a fixed-rate grid from its anchor. An after-completion schedule has no instants of its own; its next one
/// is the interval after the previous occurrence ended.
/// </summary>
internal sealed class RecurrenceSchedule
{
    /// <summary>The first window searched backwards for the latest passed instant; it doubles until it holds one.</summary>
    private static readonly TimeSpan LatestWindow = TimeSpan.FromMinutes(1);

    private readonly Schedule schedule;
    private readonly CronExpression? cron;
    private readonly TimeZoneInfo? zone;

    private RecurrenceSchedule(Schedule schedule, CronExpression? cron, TimeZoneInfo? zone)
    {
        this.schedule = schedule;
        this.cron = cron;
        this.zone = zone;
    }

    /// <summary>
    /// Validates a recurring schedule as <see cref="Parse"/> does and requires an instant after <paramref name="now"/>.
    /// </summary>
    /// <exception cref="ArgumentException">The schedule is not recurring or not valid.</exception>
    public static RecurrenceSchedule Create(Schedule schedule, DateTimeOffset now)
    {
        var created = Parse(schedule);
        if (created.Next(now) is null)
        {
            throw new ArgumentException($"Cron expression '{schedule.Expression}' has no future occurrence.", nameof(schedule));
        }
        return created;
    }

    /// <summary>
    /// Validates a recurring schedule without requiring a future instant. A cron expression must parse with Cronos (five
    /// fields, or six with seconds), use no hashed field (H) and name a known time zone.
    /// </summary>
    /// <exception cref="ArgumentException">The schedule is not recurring or not valid.</exception>
    public static RecurrenceSchedule Parse(Schedule schedule)
    {
        ArgumentNullException.ThrowIfNull(schedule);
        if (schedule.Kind == ScheduleKind.Every)
        {
            return new(schedule, null, null);
        }
        if (schedule.Kind != ScheduleKind.Cron)
        {
            throw new ArgumentException($"A {schedule.Kind} schedule is not recurring.", nameof(schedule));
        }
        var expression = schedule.Expression!;
        var format = expression.Split(' ', StringSplitOptions.RemoveEmptyEntries).Length == 6 ? CronFormat.IncludeSeconds : CronFormat.Standard;
        CronExpression cron;
        try
        {
            cron = CronExpression.Parse(expression, format);
        }
        catch (CronFormatException exception)
        {
            throw new ArgumentException($"Cron expression '{expression}' is not valid: {exception.Message}", nameof(schedule), exception);
        }
        catch (MissingSeedException exception)
        {
            // Hashed fields (H) need a seed; without one, every process computes the same occurrences.
            throw new ArgumentException($"Cron expression '{expression}' uses H, which is not accepted.", nameof(schedule), exception);
        }
        TimeZoneInfo zone;
        try
        {
            zone = TimeZoneInfo.FindSystemTimeZoneById(schedule.TimeZone!);
        }
        catch (TimeZoneNotFoundException exception)
        {
            throw new ArgumentException($"Time zone '{schedule.TimeZone}' is not known.", nameof(schedule), exception);
        }
        return new RecurrenceSchedule(schedule, cron, zone);
    }

    /// <summary>Whether each occurrence is due the interval after the previous one ended.</summary>
    public bool AfterCompletion => schedule.Kind == ScheduleKind.Every && schedule.Interval == Interval.AfterCompletion;

    /// <summary>
    /// The first instant later than <paramref name="after"/>, or null when there is none. For an after-completion
    /// schedule, <paramref name="after"/> is the end of the previous occurrence.
    /// </summary>
    public DateTimeOffset? Next(DateTimeOffset after)
    {
        if (cron is not null)
        {
            return cron.GetNextOccurrence(after, zone!)?.ToUniversalTime();
        }
        if (AfterCompletion)
        {
            return after + schedule.Period;
        }
        if (after < schedule.Anchor)
        {
            return schedule.Anchor;
        }
        return schedule.Anchor + (Steps(after) + 1) * schedule.Period;
    }

    /// <summary>
    /// The latest instant in [<paramref name="from"/>, <paramref name="now"/>], or null when there is none; not defined
    /// for after-completion schedules. A cron
    /// schedule is searched backwards from <paramref name="now"/> in doubling windows, so a long downtime does not
    /// enumerate every instant it missed.
    /// </summary>
    public DateTimeOffset? Latest(DateTimeOffset from, DateTimeOffset now)
    {
        if (now < from)
        {
            return null;
        }
        if (AfterCompletion)
        {
            throw new InvalidOperationException("An after-completion schedule has one next instant at a time.");
        }
        if (cron is null)
        {
            if (now < schedule.Anchor)
            {
                return null;
            }
            var latest = schedule.Anchor + Steps(now) * schedule.Period;
            return latest >= from ? latest : null;
        }
        var window = LatestWindow;
        while (true)
        {
            var start = now - window;
            if (start <= from)
            {
                return LatestFrom(from, now, inclusive: true);
            }
            if (LatestFrom(start, now, inclusive: false) is { } found)
            {
                return found;
            }
            window += window;
        }
    }

    private DateTimeOffset? LatestFrom(DateTimeOffset start, DateTimeOffset now, bool inclusive)
    {
        DateTimeOffset? latest = null;
        for (var next = cron!.GetNextOccurrence(start, zone!, inclusive); next is { } instant && instant <= now; next = cron.GetNextOccurrence(instant, zone!))
        {
            latest = instant;
        }
        return latest;
    }

    /// <summary>Whole grid steps from the anchor to <paramref name="time"/>, rounded down.</summary>
    private long Steps(DateTimeOffset time) => (time - schedule.Anchor).Ticks / schedule.Period.Ticks;

}
