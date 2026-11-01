using Corfedo.Work.InMemory.Database;
using static Corfedo.Work.InMemory.Leases.InMemoryLeaseStore;

namespace Corfedo.Work.InMemory.Jobs;

internal sealed partial class InMemoryJobStore
{
    /// <summary>
    /// A chain step is refused. An occurrence of a stored recurrence is evaluated among the recurrence's outstanding
    /// occurrences: an outstanding one as a standalone job, one waiting to be recorded skipped is reported waiting, and an
    /// ended one triggers the recurrence. Any other job is evaluated alone.
    /// </summary>
    public async ValueTask<RunNowResult> RunNowAsync(LeaseSource<JobId> source, JobId job, bool ignoreCalendar, CancellationToken cancellationToken)
    {
        var rules = Rules(source);
        using var work = await database.BeginAsync(cancellationToken);
        if (rules.Find(work, Key(job.Value)) is not { } r)
        {
            return new(job, RunNowOutcome.NotFound);
        }
        if (r.Uuid(JobColumns.ChainId) is not null)
        {
            return new(job, RunNowOutcome.ChainStep);
        }
        RunNowResult result;
        if (r.Uuid(JobColumns.RecurrenceId) is { } id && work.Get(application.Recurrences, id) is { } recurrence)
        {
            var jobs = Occurrences(work, recurrence.Id);
            result = jobs.Any(j => j.Uuid(JobColumns.Id) == job.Value)
                ? RunLockedNow(work, source, job, ignoreCalendar)
                : r.Outstanding()
                    ? new(job, RunNowOutcome.Waiting)
                    : TriggerLocked(work, source, recurrence, jobs, ignoreCalendar);
        }
        else
        {
            result = RunLockedNow(work, source, job, ignoreCalendar);
        }
        work.Commit();
        return result;
    }

    /// <summary>
    /// Evaluates the job: a terminal one is ended, a held one or an occurrence waiting for its chain running, one with an
    /// expired lease already due for its takeover, an occurrence to be recorded skipped waiting. Any other is made due
    /// now with its attempts kept, reported paused while its definition or subject is paused; one with a calendar is
    /// planned at the next allowed instant unless <paramref name="ignoreCalendar"/>.
    /// </summary>
    private RunNowResult RunLockedNow(InMemoryTransaction work, LeaseSource<JobId> source, JobId job, bool ignoreCalendar)
    {
        var rules = Rules(source);
        if (rules.Find(work, Key(job.Value)) is not { } r)
        {
            return new(job, RunNowOutcome.NotFound);
        }
        var now = Now;
        var outcome = !r.Outstanding() ? RunNowOutcome.Ended
            : Running(r, now) ? RunNowOutcome.AlreadyRunning
            : r.Time(LeaseFields.Default.LeaseExpiresAt) is not null ? RunNowOutcome.AlreadyDue
            : r.Bool(JobColumns.SkipPending) ? RunNowOutcome.Waiting
            : SubjectPaused(work, r) || Stored(work, rules.Job!.Definition) is { Paused: true } ? RunNowOutcome.Paused
            : r.Time(LeaseFields.Default.DueAt) <= now ? RunNowOutcome.AlreadyDue
            : RunNowOutcome.MadeDue;
        if (outcome is not (RunNowOutcome.MadeDue or RunNowOutcome.Paused))
        {
            return new(job, outcome);
        }
        if (MakeDueNow(work, r, ignoreCalendar))
        {
            NotifyPlan(work, source);
        }
        else if (outcome == RunNowOutcome.MadeDue)
        {
            NotifyDue(work, source);
        }
        return new(job, outcome);
    }

    /// <summary>
    /// Makes the job due now, or one with a calendar plannable from now at the next allowed instant unless
    /// <paramref name="ignoreCalendar"/>; true when its calendar plans it.
    /// </summary>
    private bool MakeDueNow(InMemoryTransaction work, Record r, bool ignoreCalendar) =>
        Write(work, r, DueNow(r, ignoreCalendar, r.Text(JobColumns.Calendar))).Time(JobColumns.CalendarAt) is not null;

    /// <summary>
    /// Changes that make job <paramref name="r"/> due now, keeping an earlier due time, or for a job with the calendar
    /// <paramref name="calendar"/>, unless <paramref name="ignoreCalendar"/>, its calendar instant now, planned at the
    /// next allowed instant. Ignoring the calendar sets an endless window, which claims accept and a date-set publish
    /// does not plan again.
    /// </summary>
    private KeyValuePair<string, object?>[] DueNow(Record r, bool ignoreCalendar, string? calendar)
    {
        var now = Now;
        var planned = calendar is not null && !ignoreCalendar;
        return
        [
            Set(LeaseFields.Default.DueAt, planned ? null : Least(r.Time(LeaseFields.Default.DueAt) ?? now, now)),
            Set(JobColumns.CalendarAt, planned ? now : null),
            Set(JobColumns.WindowEnd, calendar is not null && ignoreCalendar ? DateTimeOffset.MaxValue : null),
            Set(JobColumns.CalendarUnavailable, false),
            Set(JobColumns.CalendarBlocked, planned ? (int)Blocked.MoveForward : r[JobColumns.CalendarBlocked]),
        ];
    }

    /// <summary>
    /// A chain step, a job with an outstanding execution, an occurrence, and one whose identity another outstanding
    /// standalone job holds are refused. The record keeps its lease epoch, so earlier leases stay invalid and run
    /// history entries distinct, and the ended execution's outcome and result. A job with a calendar gets it back and is
    /// planned at the next allowed instant unless <paramref name="ignoreCalendar"/>; it is refused when the calendar was
    /// removed.
    /// </summary>
    public async ValueTask<RerunResult> RerunAsync(LeaseSource<JobId> source, JobId job, bool ignoreCalendar, CancellationToken cancellationToken)
    {
        var rules = Rules(source);
        using var work = await database.BeginAsync(cancellationToken);
        var r = rules.Find(work, Key(job.Value));
        var outcome = r is null ? RerunOutcome.NotFound
            : r.Uuid(JobColumns.ChainId) is not null ? RerunOutcome.ChainStep
            : r.Outstanding() ? RerunOutcome.Outstanding
            : r.Uuid(JobColumns.RecurrenceId) is not null ? RerunOutcome.Recurring
            : r.Text(JobColumns.MergeKey) is { } identity && Jobs(work, rules.Job!.Definition).Any(o => o.Outstanding() && o.Text(JobColumns.MergeKey) == identity) ? RerunOutcome.IdentityHeld
            : r.Text(JobColumns.EndedCalendar) is { } calendar && !CalendarStored(work, calendar) ? RerunOutcome.CalendarRemoved
            : RerunOutcome.Rerun;
        if (r is null || outcome != RerunOutcome.Rerun)
        {
            return new(job, outcome);
        }
        var calendarName = r.Text(JobColumns.EndedCalendar);
        var written = Write(work, r, [
            Set(JobColumns.State, 0),
            .. DueNow(r, ignoreCalendar, calendarName),
            Set(JobColumns.Calendar, calendarName),
            Set(JobColumns.EndedCalendar, null),
            Set(JobColumns.FirstRequestedAt, Now),
            Set(LeaseFields.Default.ExecutionId, Guid.NewGuid()),
            Set(JobColumns.Attempts, 0),
            Set(JobColumns.Started, false),
            Set(LeaseFields.Default.Takeovers, 0),
            Set(LeaseFields.Default.Failures, 0),
            Set(LeaseFields.Default.CancelRequestedAt, null),
            Set(JobColumns.ProgressPercent, null),
            Set(JobColumns.ProgressMessage, null),
            Set(JobColumns.Paused, SubjectPaused(work, r)),
        ]);
        if (written.Time(JobColumns.CalendarAt) is not null)
        {
            NotifyPlan(work, source);
        }
        else
        {
            NotifyDue(work, source);
        }
        work.Commit();
        return new(job, RerunOutcome.Rerun);
    }
}
