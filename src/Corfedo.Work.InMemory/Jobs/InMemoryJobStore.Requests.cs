using Corfedo.Work.InMemory.Database;
using static Corfedo.Work.InMemory.Leases.InMemoryLeaseStore;

namespace Corfedo.Work.InMemory.Jobs;

internal sealed partial class InMemoryJobStore
{
    public async ValueTask<JobRequestResult> RequestAsync(IWorkTransaction transaction, LeaseSource<JobId> source, JobRequestData request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        return (await RequestManyAsync(transaction, source, [request], cancellationToken))[0];
    }

    /// <summary>
    /// Creates or merges standalone jobs in order, so a later request of an identity sees what an earlier one wrote;
    /// recurrences of the same identity are not consulted. A request meeting an outstanding job of another partition is
    /// refused, under <c>IfAbsent</c> an existing one is reported, and otherwise it merges: before the execution started,
    /// and while no claim holds or held its lease and it is not to be skipped, it replaces the arguments, the calendar
    /// and its policies and merges the instant; otherwise it becomes the pending follow-up.
    /// </summary>
    public ValueTask<IReadOnlyList<JobRequestResult>> RequestManyAsync(IWorkTransaction transaction, LeaseSource<JobId> source, IReadOnlyList<JobRequestData> requests, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(requests);
        var work = Join(transaction);
        var definition = Rules(source).Job!.Definition;
        CheckCalendars(work, requests);
        var now = Now;
        // The outstanding standalone job of each merge identity; requests of this call keep it current.
        var outstanding = Jobs(work, definition)
            .Where(r => r.Outstanding() && r.Text(JobColumns.MergeKey) is not null)
            .ToDictionary(r => r.Text(JobColumns.MergeKey)!, r => r.Uuid(JobColumns.Id)!.Value, StringComparer.Ordinal);
        var results = new JobRequestResult[requests.Count];
        for (var i = 0; i < requests.Count; i++)
        {
            results[i] = Request(work, definition, requests[i], now, outstanding);
        }
        if (requests.Any(r => r.Schedule.Calendar is null))
        {
            NotifyDue(work, source);
        }
        if (requests.Any(r => r.Schedule.Calendar is not null))
        {
            NotifyPlan(work, source);
        }
        return ValueTask.FromResult<IReadOnlyList<JobRequestResult>>(results);
    }

    private JobRequestResult Request(InMemoryTransaction work, string definition, JobRequestData request, DateTimeOffset now, Dictionary<string, Guid> outstanding)
    {
        var schedule = request.Schedule;
        var (requested, maximum) = Timing(schedule, now);
        var partition = request.Partition ?? "";
        var identity = request.MergeIdentity;
        var existing = identity is not null && outstanding.TryGetValue(identity, out var found) ? Job(work, found) : null;
        if (existing is null)
        {
            var id = JobId.New().Value;
            if (identity is not null)
            {
                outstanding[identity] = id;
            }
            var calendar = schedule.Calendar;
            // A job with a calendar keeps its requested instant as the calendar instant, without a due time, until its
            // definition's maintenance plans it.
            Insert(work, new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                [JobColumns.Id] = id,
                [JobColumns.Definition] = definition,
                [JobColumns.MergeKey] = identity,
                [JobColumns.SubjectKind] = request.Subject?.Kind,
                [JobColumns.SubjectId] = request.Subject?.Id,
                [JobColumns.Partition] = partition,
                [JobColumns.Args] = request.Args,
                [JobColumns.ArgsVersion] = request.ArgsVersion,
                [JobColumns.FirstRequestedAt] = now,
                [JobColumns.CreatedAt] = now,
                [LeaseFields.Default.DueAt] = calendar is null ? requested : null,
                [JobColumns.CalendarAt] = calendar is null ? null : requested,
                [JobColumns.Calendar] = calendar,
                [JobColumns.CalendarBlocked] = calendar is null ? null : (int)schedule.Blocked,
                [JobColumns.CalendarClosed] = calendar is null ? null : (int)schedule.Closed,
                [LeaseFields.Default.ExecutionId] = Guid.NewGuid(),
                // The pause flag is copied from the stored subject pauses; claims check the stored pause itself.
                [JobColumns.Paused] = SubjectPaused(work, definition, request.Subject),
                [JobColumns.TraceParent] = request.TraceParent,
            });
            return new JobRequestResult(new JobId(id), JobRequestOutcome.Created);
        }
        var job = existing.JobId();
        if (existing.Text(JobColumns.Partition) != partition)
        {
            throw new JobPartitionMismatchException(
                $"The outstanding job {job} of {definition} with this identity belongs to another partition; a request cannot merge into it.");
        }
        if (request.Mode == RequestMode.IfAbsent)
        {
            return new JobRequestResult(job, JobRequestOutcome.Existing);
        }
        DateTimeOffset? Merged(DateTimeOffset? current, DateTimeOffset? firstRequested) =>
            maximum is { } ceiling ? Least(requested, firstRequested + ceiling) : Least(current, requested);
        var followUp = existing.Bool(JobColumns.Started) || existing.Time(LeaseFields.Default.LeaseExpiresAt) is not null || existing.Bool(JobColumns.SkipPending);
        if (followUp)
        {
            var pendingFirst = existing.Time(JobColumns.PendingFirstRequestedAt);
            Write(work, existing, [
                Set(JobColumns.PendingArgs, request.Args),
                Set(JobColumns.PendingArgsVersion, request.ArgsVersion),
                Set(JobColumns.PendingDue, pendingFirst is null ? requested : Merged(existing.Time(JobColumns.PendingDue), pendingFirst)),
                Set(JobColumns.PendingFirstRequestedAt, pendingFirst ?? now),
            ]);
            return new JobRequestResult(job, JobRequestOutcome.FollowUp);
        }
        // A merged request keeps the trace of the request that created the job.
        var merged = Merged(existing.Time(JobColumns.CalendarAt) ?? existing.Time(LeaseFields.Default.DueAt), existing.Time(JobColumns.FirstRequestedAt));
        Write(work, existing, [
            Set(JobColumns.Args, request.Args),
            Set(JobColumns.ArgsVersion, request.ArgsVersion),
            Set(LeaseFields.Default.DueAt, schedule.Calendar is null ? merged : null),
            Set(JobColumns.CalendarAt, schedule.Calendar is null ? null : merged),
            Set(JobColumns.WindowEnd, null),
            Set(JobColumns.CalendarUnavailable, false),
            Set(JobColumns.Calendar, schedule.Calendar),
            Set(JobColumns.CalendarBlocked, schedule.Calendar is null ? null : (int)schedule.Blocked),
            Set(JobColumns.CalendarClosed, schedule.Calendar is null ? null : (int)schedule.Closed),
        ]);
        return new JobRequestResult(job, JobRequestOutcome.Merged);
    }

    /// <summary>
    /// The instant a request asks for, its calendar instant with a calendar and its due time otherwise, and the debounce
    /// ceiling from the first outstanding request, or null when the schedule is no debounce.
    /// </summary>
    private static (DateTimeOffset Requested, TimeSpan? Maximum) Timing(Schedule schedule, DateTimeOffset now) => schedule.Kind switch
    {
        ScheduleKind.Now => (now, null),
        ScheduleKind.At => (schedule.Time.ToUniversalTime(), null),
        ScheduleKind.After => (now + schedule.Delay, null),
        ScheduleKind.Debounce => (now + schedule.Delay, schedule.Maximum),
        _ => throw new ArgumentException($"A request takes a one-shot schedule, not {schedule.Kind}.", nameof(schedule)),
    };
}
