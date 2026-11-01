using Corfedo.Work.InMemory.Database;
using static Corfedo.Work.InMemory.Leases.InMemoryLeaseStore;

namespace Corfedo.Work.InMemory.Jobs;

internal sealed partial class InMemoryJobStore
{
    /// <summary>Whether a stored pause holds the recurrence's subject.</summary>
    private bool SubjectPaused(InMemoryTransaction work, RecurrenceRow recurrence) => SubjectPaused(work, recurrence.Definition, recurrence.Subject);

    /// <summary>Whether the recurrence has an outstanding occurrence, not one to be skipped.</summary>
    private bool OccurrenceOutstanding(InMemoryTransaction work, RecurrenceRow recurrence) =>
        Jobs(work).Any(r => r.Uuid(JobColumns.RecurrenceId) == recurrence.Id && r.Outstanding() && !r.Bool(JobColumns.SkipPending));

    private static bool AfterCompletion(RecurrenceRow recurrence) => recurrence.Schedule is { Kind: ScheduleKind.Every, Interval: Interval.AfterCompletion };

    /// <summary>
    /// Whether the recurrence can plan its due instant: an after-completion instant waits while one of its occurrences
    /// is outstanding, whose end signals planning.
    /// </summary>
    private bool DueCanPlan(InMemoryTransaction work, RecurrenceRow recurrence) => !AfterCompletion(recurrence) || !OccurrenceOutstanding(work, recurrence);

    /// <summary>
    /// One planning pass in its own transaction. It takes up to <paramref name="limit"/> recurrences of each kind of
    /// work: due by their next instant, with a waiting occurrence and no outstanding occurrence, or with unconsumed ended
    /// occurrences. Due and waiting ones count only while neither the definition nor their subject is paused, a due
    /// after-completion one only while none of its occurrences is outstanding, and so does the next instant it returns.
    /// Then, with one clock reading, each recurrence: ended occurrences are consumed (the one an after-completion
    /// schedule awaits sets its next instant); unless the definition or the subject is paused, a waiting occurrence is
    /// created once none is outstanding, and due instants pass the missed rule and then the overlap policy. Only the
    /// recurrence's own occurrences count. Ended occurrences of removed recurrences are cleared. No outcome hook runs
    /// here.
    /// </summary>
    public async ValueTask<PlanningPass> PlanAsync(LeaseSource<JobId> source, IRecurrenceCalculator calculator, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(calculator);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var definition = Rules(source).Job!.Definition;
        using var work = await database.BeginAsync(cancellationToken);
        var now = Now;
        var stored = Stored(work, definition);
        var definitionPaused = stored?.Paused ?? false;
        var recurrences = work.Scan(application.Recurrences).Select(c => c.Row).Where(c => c.Definition == definition).ToList();
        var known = recurrences.Select(c => c.Id).ToHashSet();
        foreach (var r in Jobs(work, definition).Where(r => r.Bool(JobColumns.Unconsumed) && !known.Contains(r.Uuid(JobColumns.RecurrenceId)!.Value)).Take(limit).ToList())
        {
            Write(work, r, [Set(JobColumns.Unconsumed, false)]);
        }
        var ids = new HashSet<Guid>();
        // Due and waiting recurrences of a paused definition or subject, and after-completion ones with an outstanding
        // occurrence, cannot progress; left in, they would fill every pass ahead of recurrences that can.
        if (!definitionPaused)
        {
            ids.UnionWith(recurrences
                .Where(c => c.NextAt <= now && !c.Paused && !SubjectPaused(work, c) && DueCanPlan(work, c))
                .OrderBy(c => c.NextAt)
                .Take(limit)
                .Select(c => c.Id));
            ids.UnionWith(recurrences
                .Where(c => c.WaitingAt is not null && !c.Paused && !SubjectPaused(work, c) && !OccurrenceOutstanding(work, c))
                .Take(limit)
                .Select(c => c.Id));
        }
        var unconsumed = Jobs(work, definition).Where(r => r.Bool(JobColumns.Unconsumed)).Select(r => r.Uuid(JobColumns.RecurrenceId)!.Value).ToHashSet();
        ids.UnionWith(recurrences.Where(c => unconsumed.Contains(c.Id)).Take(limit).Select(c => c.Id));
        var planned = recurrences.Where(c => ids.Contains(c.Id)).ToList();
        var ended = Jobs(work, definition).Where(r => r.Bool(JobColumns.Unconsumed) && ids.Contains(r.Uuid(JobColumns.RecurrenceId)!.Value)).ToList();
        var outstanding = planned.Where(c => OccurrenceOutstanding(work, c)).Select(c => c.Id).ToHashSet();
        var occurrences = 0;
        foreach (var recurrence in planned)
        {
            occurrences += PlanRecurrence(
                work,
                calculator,
                recurrence,
                [.. ended.Where(e => e.Uuid(JobColumns.RecurrenceId) == recurrence.Id)],
                outstanding.Contains(recurrence.Id),
                definitionPaused,
                now);
        }
        // Resuming a definition or subject and ending an occurrence signal planning, so the next instant counts only
        // recurrences that can progress.
        TimeSpan? nextIn = null;
        if (!definitionPaused)
        {
            var next = work.Scan(application.Recurrences).Select(c => c.Row)
                .Where(c => c.Definition == definition && c.NextAt is not null && !c.Paused && !SubjectPaused(work, c) && DueCanPlan(work, c))
                .Min(c => c.NextAt);
            nextIn = Until(next, Now);
        }
        if (occurrences > 0)
        {
            NotifyDue(work, source);
        }
        work.Commit();
        return new(planned.Count, occurrences, nextIn, stored?.Revision);
    }

    /// <summary>Plans one recurrence; returns how many occurrences it created.</summary>
    private int PlanRecurrence(
        InMemoryTransaction work,
        IRecurrenceCalculator calculator,
        RecurrenceRow recurrence,
        List<Record> ended,
        bool held,
        bool definitionPaused,
        DateTimeOffset now)
    {
        var schedule = recurrence.Schedule!;
        var afterCompletion = AfterCompletion(recurrence);
        var next = recurrence.NextAt;
        var waiting = recurrence.WaitingAt;
        var awaited = recurrence.Awaited;
        // The occurrence an after-completion schedule awaits; it is named by its occurrence identity when planned in
        // this pass, so an insert that met an existing occurrence names that one.
        string? awaitedOccurrence = null;
        foreach (var occurrence in ended)
        {
            if (afterCompletion && next is null && occurrence.Uuid(JobColumns.Id) == awaited)
            {
                next = occurrence.Time(JobColumns.EndedAt)!.Value + schedule.Period;
                awaited = null;
            }
        }
        foreach (var occurrence in ended)
        {
            Write(work, occurrence, [Set(JobColumns.Unconsumed, false)]);
        }
        var created = 0;
        // An after-completion schedule awaits only an instant it planned; its waiting occurrence comes from a trigger.
        // A released waiting occurrence is planned at the next allowed instant of its calendar, a scheduled instant only
        // when the calendar allows it.
        void Insert(DateTimeOffset instant, bool skip, bool awaits, bool released = false)
        {
            var occurrence = OccurrenceOf(instant);
            InsertOccurrence(work, recurrence, JobId.New().Value, occurrence, instant.ToUniversalTime(), skip, released ? CalendarStart.MoveForward : CalendarStart.Instant);
            if (!skip)
            {
                created++;
                if (afterCompletion && awaits)
                {
                    awaitedOccurrence = occurrence;
                }
            }
        }
        if (!definitionPaused && !SubjectPaused(work, recurrence))
        {
            if (waiting is { } released && !held)
            {
                Insert(released, skip: false, awaits: false, released: true);
                waiting = null;
                held = true;
            }
            if (next is { } due && due <= now && !(afterCompletion && held))
            {
                // An after-completion instant due while another occurrence is outstanding stays due until it ends.
                var instants = calculator.Plan(schedule, recurrence.Missed, due, now);
                next = instants.Next;
                if (instants.Planned is { } instant)
                {
                    if (!held)
                    {
                        Insert(instant, skip: false, awaits: true);
                    }
                    else
                    {
                        switch (recurrence.Overlap)
                        {
                            case Overlap.Wait:
                                waiting = instant;
                                break;
                            case Overlap.Skip:
                                Insert(instant, skip: true, awaits: false);
                                break;
                            default:
                                Insert(instant, skip: false, awaits: true);
                                break;
                        }
                    }
                }
            }
        }
        if (awaitedOccurrence is not null)
        {
            awaited = Jobs(work).FirstOrDefault(r => r.Uuid(JobColumns.RecurrenceId) == recurrence.Id && r.Text(JobColumns.Occurrence) == awaitedOccurrence)?.Uuid(JobColumns.Id);
        }
        work.Put(application.Recurrences, recurrence.Id, recurrence with
        {
            NextAt = next?.ToUniversalTime(),
            WaitingAt = waiting?.ToUniversalTime(),
            Awaited = awaited,
            UpdatedAt = Now,
        });
        return created;
    }

    /// <summary>
    /// Ends jobs recorded to be skipped, one per transaction, skipped through the terminal path, which runs the outcome
    /// hook, with the reason its calendar or chain recorded or <c>overlap</c>; an occurrence whose chain settled ends with
    /// the chain's outcome. They need no claim, so pause and removal of their recurrence do not hold them back. One whose
    /// transition fails, for example in its outcome hook, stays recorded and is skipped for the rest of the call.
    /// </summary>
    public async ValueTask<int> FinalizeSkippedAsync(LeaseSource<JobId> source, JobEndedCallback? ended, int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var rules = Rules(source);
        static bool Skipping(Record r) => r.Outstanding() && (r.Bool(JobColumns.SkipPending) || r[JobColumns.ChainOutcome] is not null);
        var failed = new HashSet<Guid>();
        var count = 0;
        while (count < limit)
        {
            using var work = await database.BeginAsync(cancellationToken);
            if (rules.Scan(work).Select(r => r.Row).Where(r => Skipping(r) && !failed.Contains(r.Uuid(JobColumns.Id)!.Value)).MinBy(r => r.Uuid(JobColumns.Id)) is not { } found)
            {
                return count;
            }
            var job = found.JobId();
            var outcome = JobOutcome.Skipped;
            var reason = found.Text(JobColumns.SkipReason) ?? JobReasons.Overlap;
            if (!found.Bool(JobColumns.SkipPending))
            {
                // An occurrence whose chain settled ends with the chain's outcome.
                outcome = (JobOutcome)found.Int32(JobColumns.ChainOutcome);
                reason = OwnerReason(outcome);
            }
            try
            {
                // A standalone job skipped by its calendar starts a pending follow-up; occurrences have none.
                if (await EndJobAsync(work, source, job, outcome, reason, discardPending: false, Skipping, ended, cancellationToken))
                {
                    work.Commit();
                    count++;
                }
            }
            catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
            {
                failed.Add(job.Value);
                LogFinalizeFailed(logger, telemetry.Logged(exception), rules.Job!.Definition, job.Value, telemetry.Describe(exception));
            }
        }
        return count;
    }
}
