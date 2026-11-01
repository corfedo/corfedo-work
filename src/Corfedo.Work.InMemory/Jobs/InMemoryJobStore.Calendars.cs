using Corfedo.Work.InMemory.Calendars;
using Corfedo.Work.InMemory.Database;
using static Corfedo.Work.InMemory.Leases.InMemoryLeaseStore;

namespace Corfedo.Work.InMemory.Jobs;

internal sealed partial class InMemoryJobStore
{
    /// <summary>What calendar planning decided for one job.</summary>
    private enum CalendarDecision
    {
        /// <summary>Due at the planned time, startable until the window end; a null time keeps the due time.</summary>
        Planned,

        /// <summary>Recorded to be skipped with a reason.</summary>
        Skip,

        /// <summary>Waiting for a date set to cover its instant, planned from that instant once it does.</summary>
        Unavailable,
    }

    /// <summary>
    /// One decision. <see cref="MovesForward"/> says that a job left waiting for dates is planned at the next allowed
    /// instant from <see cref="Instant"/> once they are published, whatever its blocked policy was.
    /// </summary>
    private readonly record struct CalendarPlan(Record Job, CalendarDecision Decision, DateTimeOffset? Due, DateTimeOffset? WindowEnd, DateTimeOffset? Instant, string? Reason, bool MovesForward = false);

    /// <summary>
    /// Refuses requests whose calendar is not stored, and under <see cref="Blocked.Reject"/> requests whose instant,
    /// computed with the store's clock, the calendar blocks or cannot decide yet; before anything is written.
    /// </summary>
    private void CheckCalendars(InMemoryTransaction work, IReadOnlyList<JobRequestData> requests)
    {
        if (!requests.Any(r => r.Schedule.Calendar is not null))
        {
            return;
        }
        var evaluator = InMemoryCalendarStore.Evaluator(work, application);
        var now = Now;
        foreach (var schedule in requests.Select(r => r.Schedule).Where(s => s.Calendar is not null).Distinct())
        {
            if (!evaluator.Contains(schedule.Calendar!))
            {
                throw new ArgumentException($"Calendar {schedule.Calendar} is not stored; register it with AddCalendar.", nameof(requests));
            }
            if (schedule.Blocked != Blocked.Reject)
            {
                continue;
            }
            var instant = schedule.Kind switch
            {
                ScheduleKind.At => schedule.Time,
                ScheduleKind.After or ScheduleKind.Debounce => now + schedule.Delay,
                _ => now,
            };
            var slot = evaluator.Next(schedule.Calendar!, instant);
            if (slot.Kind == CalendarSlotKind.Unavailable)
            {
                throw new CalendarBlockedException($"Calendar {schedule.Calendar} cannot decide {instant:O} yet: a date set does not cover it.", unavailable: true);
            }
            if (slot.Kind != CalendarSlotKind.Allowed || slot.Start != instant)
            {
                throw new CalendarBlockedException($"Calendar {schedule.Calendar} blocks {instant:O}.", unavailable: false);
            }
        }
    }

    /// <summary>Refuses a calendar that is not stored.</summary>
    private void RequireCalendar(InMemoryTransaction work, string calendar, string parameter)
    {
        if (!CalendarStored(work, calendar))
        {
            throw new ArgumentException($"Calendar {calendar} is not stored; register it with AddCalendar.", parameter);
        }
    }

    private bool CalendarStored(InMemoryTransaction work, string calendar) => work.Get(application.Calendars, calendar) is not null;

    /// <summary>
    /// Takes the definition's unheld outstanding jobs whose calendar instant has come and those whose window closed before
    /// they started, up to <paramref name="limit"/> of each, and decides each with the stored calendars and one clock
    /// reading:
    /// <list type="bullet">
    /// <item>An instant the calendar allows is the due time, with the end of its window. A blocked one is moved to the
    /// next allowed instant under <see cref="Blocked.MoveForward"/>, and otherwise recorded to be skipped with
    /// <c>calendar_blocked</c>.</item>
    /// <item>An instant whose window has already closed, also after moving forward, and a planned job whose window
    /// closed, keep their due time when the calendar allows now, since the window continues; otherwise they follow the
    /// closed policy: planned at the next allowed instant, or recorded to be skipped with <c>window_closed</c>.</item>
    /// <item>A job that needs dates a date set does not cover waits until a covering revision is published; one with no
    /// allowed instant within the lookahead is recorded to be skipped.</item>
    /// </list>
    /// The next instant to wake for is the earliest calendar instant or window end still to come.
    /// </summary>
    public async ValueTask<CalendarPass> PlanCalendarAsync(LeaseSource<JobId> source, int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var rules = Rules(source);
        using var work = await database.BeginAsync(cancellationToken);
        var now = Now;
        var unheld = rules.Scan(work).Select(r => r.Row).Where(r => r.Outstanding() && r.Time(LeaseFields.Default.LeaseExpiresAt) is null).ToList();
        var instants = unheld
            .Where(r => r.Time(JobColumns.CalendarAt) <= now && !r.Bool(JobColumns.CalendarUnavailable))
            .OrderBy(r => r.Time(JobColumns.CalendarAt))
            .Take(limit)
            .ToList();
        var closed = unheld
            .Where(r => r.Time(JobColumns.WindowEnd) <= now)
            .OrderBy(r => r.Time(JobColumns.WindowEnd))
            .Take(limit)
            .ToList();
        var plans = new List<CalendarPlan>(instants.Count + closed.Count);
        if (instants.Count + closed.Count > 0)
        {
            var evaluator = InMemoryCalendarStore.Evaluator(work, application);
            // Every job whose window closed is decided by the calendar at now, once per calendar.
            var atNow = new Dictionary<string, CalendarSlot>(StringComparer.Ordinal);
            CalendarSlot At(string calendar) => atNow.TryGetValue(calendar, out var slot) ? slot : atNow[calendar] = evaluator.Next(calendar, now);
            CalendarPlan CloseWindow(Record r, Closed policy, DateTimeOffset? keep)
            {
                var slot = At(r.Text(JobColumns.Calendar)!);
                if (slot.Kind == CalendarSlotKind.Allowed && slot.Start == now)
                {
                    return new(r, CalendarDecision.Planned, keep, slot.End, null, null);
                }
                return policy == Closed.Skip ? new(r, CalendarDecision.Skip, null, null, null, JobReasons.WindowClosed) : Plan(r, slot, now);
            }
            foreach (var r in instants)
            {
                var instant = r.Time(JobColumns.CalendarAt)!.Value;
                var policy = (Closed)r.Int32(JobColumns.CalendarClosed);
                var slot = evaluator.Next(r.Text(JobColumns.Calendar)!, instant);
                if (slot.Kind == CalendarSlotKind.Allowed && slot.Start == instant)
                {
                    plans.Add(slot.End is { } end && end <= now ? CloseWindow(r, policy, instant) : new(r, CalendarDecision.Planned, instant, slot.End, null, null));
                }
                else if (slot.Kind == CalendarSlotKind.Unavailable)
                {
                    plans.Add(new(r, CalendarDecision.Unavailable, null, null, instant, null));
                }
                else if ((Blocked)r.Int32(JobColumns.CalendarBlocked) != Blocked.MoveForward || slot.Kind == CalendarSlotKind.None)
                {
                    plans.Add(new(r, CalendarDecision.Skip, null, null, null, JobReasons.CalendarBlocked));
                }
                else
                {
                    // The next allowed window may also have closed already; then the closed policy applies to it.
                    plans.Add(slot.End is { } end && end <= now ? CloseWindow(r, policy, slot.Start) : Plan(r, slot, instant));
                }
            }
            foreach (var r in closed)
            {
                plans.Add(CloseWindow(r, (Closed)r.Int32(JobColumns.CalendarClosed), null));
            }
            foreach (var plan in plans)
            {
                var r = Job(work, plan.Job.Uuid(JobColumns.Id)!.Value)!;
                Write(work, r, [
                    Set(LeaseFields.Default.DueAt, plan.Decision == CalendarDecision.Planned ? plan.Due ?? r.Time(LeaseFields.Default.DueAt) : null),
                    Set(JobColumns.WindowEnd, plan.Decision == CalendarDecision.Planned ? plan.WindowEnd : null),
                    Set(JobColumns.CalendarAt, plan.Decision == CalendarDecision.Unavailable ? plan.Instant : null),
                    Set(JobColumns.CalendarUnavailable, plan.Decision == CalendarDecision.Unavailable),
                    Set(JobColumns.CalendarBlocked, plan.MovesForward ? (int)Blocked.MoveForward : r[JobColumns.CalendarBlocked]),
                    Set(JobColumns.SkipPending, plan.Decision == CalendarDecision.Skip),
                    Set(JobColumns.SkipReason, plan.Reason),
                ]);
            }
            if (plans.Any(p => p.Decision == CalendarDecision.Planned))
            {
                NotifyDue(work, source);
            }
        }
        var after = Now;
        var next = rules.Scan(work).Select(r => r.Row)
            .Where(r => r.Outstanding() && r.Time(LeaseFields.Default.LeaseExpiresAt) is null)
            .SelectMany(r => new[]
            {
                r.Bool(JobColumns.CalendarUnavailable) ? null : r.Time(JobColumns.CalendarAt),
                r.Time(JobColumns.WindowEnd),
            })
            .Where(t => t > now)
            .Min();
        work.Commit();
        return new(instants.Count, closed.Count, next is { } at ? at - after : null);
    }

    /// <summary>Plans a job at the allowed instant of <paramref name="slot"/>, not before <paramref name="from"/>, or as the slot decides.</summary>
    private static CalendarPlan Plan(Record r, CalendarSlot slot, DateTimeOffset from) => slot.Kind switch
    {
        CalendarSlotKind.Allowed => new(r, CalendarDecision.Planned, slot.Start > from ? slot.Start : from, slot.End, null, null),
        CalendarSlotKind.Unavailable => new(r, CalendarDecision.Unavailable, null, null, from, null, MovesForward: true),
        _ => new(r, CalendarDecision.Skip, null, null, null, JobReasons.CalendarBlocked),
    };
}
