namespace Corfedo.Work;

/// <summary>
/// What maintenance one definition has, read from committed records by <see cref="IJobStore.MaintenanceDueAsync"/>: its
/// stored revision, null while none is stored, which maintenance calls have work, and how long until its next
/// recurrence instant, calendar instant or window end. A flag is set whenever the call would find rows; the call may
/// still find none, for example when they are locked or changed since the read.
/// </summary>
/// <param name="Revision">The stored revision of the definition, null while none is stored.</param>
/// <param name="Planning">Whether <see cref="IJobStore.PlanAsync"/> has due or waiting recurrences or unconsumed ended occurrences.</param>
/// <param name="Calendar">Whether <see cref="IJobStore.PlanCalendarAsync"/> has calendar instants that came or windows that closed.</param>
/// <param name="Skipped">Whether <see cref="IJobStore.FinalizeSkippedAsync"/> has jobs to end.</param>
/// <param name="Retired">Whether <see cref="IJobStore.RetireAsync"/> has unheld jobs below the stored minimum argument version.</param>
/// <param name="Pauses">Whether <see cref="IJobStore.RepairPausesAsync"/> has copied pause flags that disagree with the stored pauses.</param>
/// <param name="NextIn">How long until the earliest future instant of the definition, null when it has none.</param>
public readonly record struct JobMaintenanceDue(long? Revision, bool Planning, bool Calendar, bool Skipped, bool Retired, bool Pauses, TimeSpan? NextIn)
{
    /// <summary>Whether any maintenance call has work.</summary>
    public bool Any => Planning || Calendar || Skipped || Retired || Pauses;
}
