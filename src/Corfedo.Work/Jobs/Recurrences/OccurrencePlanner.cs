namespace Corfedo.Work;

/// <summary>What one planning of a recurrence decided: the instant to plan, if any, and the next instant to plan.</summary>
internal readonly record struct PlannedInstants(DateTimeOffset? Planned, DateTimeOffset? Next);

/// <summary>
/// The missed rule (J2), applied with the store's clock reading taken in the planning transaction. Of the instants
/// from the next one to plan up to now, one is not missed and is planned; several were missed:
/// <see cref="Missed.Skip"/> plans none and <see cref="Missed.RunLatest"/> plans the latest, and both continue from the
/// next future instant. An after-completion schedule has one instant at a time, so it is always planned and only
/// <see cref="Late"/> applies to it; its next instant follows from the occurrence's end.
/// </summary>
internal static class OccurrencePlanner
{
    public static PlannedInstants Plan(RecurrenceSchedule schedule, DateTimeOffset next, DateTimeOffset now, Missed missed)
    {
        ArgumentNullException.ThrowIfNull(schedule);
        if (next > now)
        {
            return new(null, next);
        }
        if (schedule.AfterCompletion)
        {
            return new(next, null);
        }
        var second = schedule.Next(next);
        if (second is null || second > now)
        {
            return new(next, second);
        }
        var following = schedule.Next(now);
        return missed == Missed.RunLatest ? new(schedule.Latest(second.Value, now), following) : new(null, following);
    }
}
