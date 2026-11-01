using System.Collections.Concurrent;

namespace Corfedo.Work;

/// <summary>Computes recurring instants with Cronos for the store's planning, caching parsed schedules.</summary>
internal sealed class RecurrenceCalculator : IRecurrenceCalculator
{
    public static RecurrenceCalculator Instance { get; } = new();

    private readonly ConcurrentDictionary<Schedule, RecurrenceSchedule> parsed = new();

    public DateTimeOffset? First(Schedule schedule, DateTimeOffset now) => Parsed(schedule).Next(now);

    public RecurrenceInstants Plan(Schedule schedule, Missed missed, DateTimeOffset next, DateTimeOffset now)
    {
        var planned = OccurrencePlanner.Plan(Parsed(schedule), next, now, missed);
        return new(planned.Planned, planned.Next);
    }

    private RecurrenceSchedule Parsed(Schedule schedule) => parsed.GetOrAdd(schedule, RecurrenceSchedule.Parse);
}
