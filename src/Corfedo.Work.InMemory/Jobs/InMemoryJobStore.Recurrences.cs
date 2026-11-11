using System.Globalization;
using Corfedo.Work.InMemory.Database;
using static Corfedo.Work.InMemory.Leases.InMemoryLeaseStore;

namespace Corfedo.Work.InMemory.Jobs;

internal sealed partial class InMemoryJobStore
{
    public ValueTask<RecurrenceOutcome> ScheduleAsync(WorkTransaction transaction, LeaseSource<JobId> source, RecurrenceData recurrence, IRecurrenceCalculator calculator, CancellationToken cancellationToken)
    {
        Validate(recurrence, calculator);
        return ValueTask.FromResult(StoreRecurrence(Join(transaction), source, recurrence, calculator, null, null));
    }

    public async ValueTask<RecurrenceOutcome> DeclareRecurrenceAsync(LeaseSource<JobId> source, RecurrenceData recurrence, long revision, byte[] digest, IRecurrenceCalculator calculator, CancellationToken cancellationToken)
    {
        Validate(recurrence, calculator);
        ArgumentNullException.ThrowIfNull(digest);
        using var transaction = await database.BeginAsync(cancellationToken);
        var outcome = StoreRecurrence(transaction, source, recurrence, calculator, revision, digest);
        transaction.Commit();
        return outcome;
    }

    private static void Validate(RecurrenceData recurrence, IRecurrenceCalculator calculator)
    {
        ArgumentNullException.ThrowIfNull(recurrence);
        ArgumentNullException.ThrowIfNull(calculator);
        if (!recurrence.Schedule.Recurring)
        {
            throw new ArgumentException($"A recurrence needs a recurring schedule, not {recurrence.Schedule.Kind}.", nameof(recurrence));
        }
        if (recurrence.Mode == RequestMode.New)
        {
            throw new ArgumentException("An identity has one recurrence; RequestMode.New does not apply to recurring schedules.", nameof(recurrence));
        }
    }

    /// <summary>
    /// Creates the identity's recurrence or replaces it. A declared recurrence (<paramref name="revision"/> set) replaces
    /// only a recurrence stored by an older revision or by none. The stored recurrence copies its subject's pause to its
    /// flag, and its first instant follows the store's clock. An after-completion schedule that replaces one awaiting an
    /// occurrence keeps awaiting it, so its end sets the next instant; otherwise its first instant is measured from the
    /// last end it recorded. The recurrence's key is queued for the monitoring loop. Standalone jobs of the identity are
    /// left alone.
    /// </summary>
    private RecurrenceOutcome StoreRecurrence(InMemoryTransaction work, LeaseSource<JobId> source, RecurrenceData recurrence, IRecurrenceCalculator calculator, long? revision, byte[]? digest)
    {
        var definition = Rules(source).Job!.Definition;
        var partition = recurrence.Partition ?? "";
        var schedule = recurrence.Schedule;
        var now = Now;
        if (calculator.First(schedule, now, null) is null)
        {
            throw new ArgumentException("The schedule has no future occurrence.", nameof(recurrence));
        }
        if (schedule.Calendar is { } calendar)
        {
            RequireCalendar(work, calendar, nameof(recurrence));
        }
        var existing = Recurrence(work, definition, recurrence.Identity);
        if (existing is not null)
        {
            if (existing.Partition != partition)
            {
                throw new JobPartitionMismatchException($"The recurrence of {definition} with this identity belongs to another partition.");
            }
            if (revision is { } declared && existing.DeclaredRevision is { } stored)
            {
                if (stored > declared)
                {
                    return RecurrenceOutcome.Existing;
                }
                if (stored == declared)
                {
                    if (!existing.DeclaredDigest.AsSpan().SequenceEqual(digest))
                    {
                        throw new JobDefinitionConflictException(
                            $"The recurrence of job definition {definition} revision {declared} is stored with different content; increase the revision when the recurrence changes.");
                    }
                    return RecurrenceOutcome.Existing;
                }
            }
            if (recurrence.Mode == RequestMode.IfAbsent)
            {
                return RecurrenceOutcome.Existing;
            }
        }
        var afterCompletion = schedule is { Kind: ScheduleKind.Every, Interval: Interval.AfterCompletion };
        var awaited = afterCompletion ? existing?.Awaited : null;
        var lastEnded = afterCompletion ? existing?.LastEndedAt : null;
        var next = awaited is null ? calculator.First(schedule, now, lastEnded) : null;
        var id = existing?.Id ?? Guid.CreateVersion7();
        work.Put(application.Recurrences, id, new RecurrenceRow(
            id,
            definition,
            recurrence.Identity,
            recurrence.Subject,
            partition,
            recurrence.Args,
            recurrence.ArgsVersion,
            schedule,
            recurrence.Missed,
            recurrence.Overlap,
            awaited,
            revision ?? existing?.DeclaredRevision,
            digest ?? existing?.DeclaredDigest,
            next?.ToUniversalTime(),
            WaitingAt: null,
            SubjectPaused(work, definition, recurrence.Subject),
            existing?.CreatedAt ?? now,
            now)
        {
            Calendar = schedule.Calendar,
            CalendarClosed = schedule.Calendar is null ? null : schedule.Closed,
            LastEndedAt = lastEnded,
        });
        QueueExpectationChange(work, source, definition, recurrence.Subject);
        NotifyPlan(work, source);
        return existing is null ? RecurrenceOutcome.Created : RecurrenceOutcome.Replaced;
    }

    /// <summary>
    /// Cancels the recurrence's outstanding occurrences, clears the unconsumed marks of its ended occurrences and deletes
    /// it, queueing its key for the monitoring loop. Occurrences to be skipped are left to finalization.
    /// </summary>
    public async ValueTask<bool> RemoveAsync(WorkTransaction transaction, LeaseSource<JobId> source, string identity, JobEndedCallback? ended, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(identity);
        ArgumentNullException.ThrowIfNull(definitions);
        var work = Join(transaction);
        var definition = Rules(source).Job!.Definition;
        if (Recurrence(work, definition, identity) is not { } recurrence)
        {
            return false;
        }
        var locked = SelectForCancel(work, Jobs(work, definition).Where(r => r.Uuid(JobColumns.RecurrenceId) == recurrence.Id));
        await CancelSelectedAsync(work, locked, key => key == definition ? (source, ended) : definitions(key), cancellationToken);
        foreach (var r in Jobs(work).Where(r => r.Uuid(JobColumns.RecurrenceId) == recurrence.Id && r.Bool(JobColumns.Unconsumed)).ToList())
        {
            Write(work, r, [Set(JobColumns.Unconsumed, false)]);
        }
        work.Remove(application.Recurrences, recurrence.Id);
        QueueExpectationChange(work, source, definition, recurrence.Subject);
        return true;
    }

    /// <summary>Triggers the identity's recurrence as <see cref="TriggerLocked"/> describes; its schedule does not move.</summary>
    public ValueTask<RunNowResult> TriggerAsync(WorkTransaction transaction, LeaseSource<JobId> source, string identity, bool ignoreCalendar, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(identity);
        var work = Join(transaction);
        var definition = Rules(source).Job!.Definition;
        var recurrence = Recurrence(work, definition, identity)
            ?? throw new InvalidOperationException($"Job definition {definition} has no recurrence with this identity.");
        return ValueTask.FromResult(TriggerLocked(work, source, recurrence, Occurrences(work, recurrence.Id), ignoreCalendar));
    }

    /// <summary>
    /// Triggers a recurrence with its outstanding occurrences. A running occurrence follows the overlap policy: under
    /// <see cref="Overlap.Wait"/> the recurrence records its waiting occurrence, which the planner starts once no
    /// occurrence is outstanding, under <see cref="Overlap.Skip"/> it is reported running, and under
    /// <see cref="Overlap.Allow"/> another occurrence starts. Otherwise one that is not running is made due now with its
    /// attempts kept, and with none a triggered occurrence is created due now; with a calendar both are planned at the
    /// next allowed instant unless <paramref name="ignoreCalendar"/>.
    /// </summary>
    private RunNowResult TriggerLocked(InMemoryTransaction work, LeaseSource<JobId> source, RecurrenceRow recurrence, List<Record> jobs, bool ignoreCalendar)
    {
        var now = Now;
        if (jobs.FirstOrDefault(j => Running(j, now)) is { } running && recurrence.Overlap != Overlap.Allow)
        {
            if (recurrence.Overlap == Overlap.Skip)
            {
                return new(running.JobId(), RunNowOutcome.AlreadyRunning);
            }
            work.Put(application.Recurrences, recurrence.Id, recurrence with { WaitingAt = recurrence.WaitingAt ?? now, UpdatedAt = now });
            return new(running.JobId(), RunNowOutcome.Waiting);
        }
        RunNowResult result;
        if (jobs.FirstOrDefault(j => !Running(j, now)) is { } waiting)
        {
            MakeDueNow(work, waiting, ignoreCalendar);
            result = new(waiting.JobId(), RunNowOutcome.MadeDue);
        }
        else
        {
            var id = JobId.New();
            InsertOccurrence(work, recurrence, id.Value, "trigger:" + Guid.CreateVersion7().ToString("N"), now, skip: false, ignoreCalendar ? CalendarStart.Ignore : CalendarStart.MoveForward);
            result = new(id, RunNowOutcome.Started);
        }
        if (recurrence.Calendar is not null && !ignoreCalendar)
        {
            NotifyPlan(work, source);
        }
        else
        {
            NotifyDue(work, source);
        }
        return result;
    }

    /// <summary>The recurrence of a definition's identity as the transaction sees it, or null.</summary>
    private RecurrenceRow? Recurrence(InMemoryTransaction work, string definition, string identity) =>
        work.Scan(application.Recurrences).Select(c => c.Row).FirstOrDefault(c => c.Definition == definition && c.MergeKey == identity);

    /// <summary>The recurrence's outstanding occurrences in id order, leaving out those to be skipped.</summary>
    private List<Record> Occurrences(InMemoryTransaction work, Guid recurrence) =>
        [.. Jobs(work).Where(r => r.Uuid(JobColumns.RecurrenceId) == recurrence && r.Outstanding() && !r.Bool(JobColumns.SkipPending)).OrderBy(r => r.Uuid(JobColumns.Id))];

    /// <summary>
    /// Inserts an occurrence of the recurrence with job id <paramref name="id"/> and occurrence identity
    /// <paramref name="occurrence"/>, taking arguments, subject, partition and calendar from the recurrence, unless the
    /// recurrence has an occurrence with that identity. One to be skipped has no due time; with a calendar,
    /// <paramref name="due"/> is its calendar instant, planned under <paramref name="start"/>. Occurrences have no merge
    /// key, so they never meet standalone jobs.
    /// </summary>
    private void InsertOccurrence(InMemoryTransaction work, RecurrenceRow recurrence, Guid id, string occurrence, DateTimeOffset due, bool skip, CalendarStart start = CalendarStart.Instant)
    {
        if (Jobs(work).Any(r => r.Uuid(JobColumns.RecurrenceId) == recurrence.Id && r.Text(JobColumns.Occurrence) == occurrence))
        {
            return;
        }
        var calendar = start != CalendarStart.Ignore && recurrence.Calendar is not null;
        var blocked = start == CalendarStart.Instant ? Blocked.Skip : Blocked.MoveForward;
        var now = Now;
        Insert(work, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            [JobColumns.Id] = id,
            [JobColumns.Definition] = recurrence.Definition,
            [JobColumns.RecurrenceId] = recurrence.Id,
            [JobColumns.SubjectKind] = recurrence.Subject?.Kind,
            [JobColumns.SubjectId] = recurrence.Subject?.Id,
            [JobColumns.Partition] = recurrence.Partition,
            [JobColumns.Args] = recurrence.Args,
            [JobColumns.ArgsVersion] = recurrence.ArgsVersion,
            [LeaseFields.Default.DueAt] = skip || calendar ? null : due,
            [JobColumns.CalendarAt] = !skip && calendar ? due : null,
            [JobColumns.WindowEnd] = start == CalendarStart.Ignore && recurrence.Calendar is not null ? DateTimeOffset.MaxValue : null,
            [JobColumns.Calendar] = recurrence.Calendar,
            [JobColumns.CalendarBlocked] = recurrence.Calendar is null ? null : (int)blocked,
            [JobColumns.CalendarClosed] = recurrence.CalendarClosed is { } closed ? (int)closed : null,
            [JobColumns.SkipPending] = skip,
            [JobColumns.FirstRequestedAt] = now,
            [JobColumns.CreatedAt] = now,
            [LeaseFields.Default.ExecutionId] = Guid.NewGuid(),
            [JobColumns.Occurrence] = occurrence,
            [JobColumns.Paused] = SubjectPaused(work, recurrence.Definition, recurrence.Subject),
        });
    }

    /// <summary>How a new occurrence of a recurrence with a calendar is planned.</summary>
    private enum CalendarStart
    {
        /// <summary>A scheduled instant: recorded skipped when the calendar blocks it.</summary>
        Instant,

        /// <summary>A waiting or triggered occurrence: planned at the next allowed instant.</summary>
        MoveForward,

        /// <summary>A triggered occurrence that ignores the calendar: due at once, with an endless window.</summary>
        Ignore,
    }

    /// <summary>The occurrence identity of a scheduled instant: ISO 8601 in UTC.</summary>
    private static string OccurrenceOf(DateTimeOffset instant) => instant.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'", CultureInfo.InvariantCulture);
}
