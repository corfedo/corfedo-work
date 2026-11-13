using Corfedo.Work.InMemory.Database;
using static Corfedo.Work.InMemory.Leases.InMemoryLeaseStore;

namespace Corfedo.Work.InMemory.Jobs;

internal sealed partial class InMemoryJobStore
{
    public async ValueTask<bool> ReportProgressAsync(Lease<JobId> lease, JobProgress progress, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(progress);
        var rules = Rules(lease.Source);
        using var work = await database.BeginAsync(cancellationToken);
        if (rules.Find(work, Key(lease.Key.Value)) is not { } r || !r.Outstanding() || !rules.Valid(r, lease.Owner, lease.Epoch, Now, 0))
        {
            return false;
        }
        Write(work, r, [Set(JobColumns.ProgressPercent, progress.Percent), Set(JobColumns.ProgressMessage, progress.Message)]);
        work.Commit();
        return true;
    }

    /// <summary>
    /// From committed state, per subject: the running job, otherwise the earliest due one, standalone job or occurrence
    /// (occurrences waiting to be recorded skipped are not candidates); the latest ended execution; the recurrence by its
    /// identity; and the stored pause.
    /// </summary>
    public ValueTask<IReadOnlyDictionary<Subject, JobStatus>> GetStatusAsync(string definition, IReadOnlyList<Subject> subjects, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(subjects);
        var now = Now;
        var statuses = database.ReadCommitted(() =>
        {
            var statuses = new Dictionary<Subject, JobStatus>();
            var definitionPaused = application.Definitions.Committed.GetValueOrDefault(definition)?.Stored.Paused ?? false;
            var jobs = JobRows.Committed.Values.Where(r => r.Text(JobColumns.Definition) == definition && r.Text(JobColumns.SubjectKind) is not null).ToList();
            foreach (var subject in subjects)
            {
                var own = jobs.Where(r => r.Text(JobColumns.SubjectKind) == subject.Kind && r.Text(JobColumns.SubjectId) == subject.Id).ToList();
                var o = own
                    .Where(r => r.Outstanding() && !r.Bool(JobColumns.SkipPending))
                    .OrderByDescending(r => Running(r, now))
                    .ThenBy(r => r.Time(LeaseFields.Default.DueAt) ?? r.Time(JobColumns.CalendarAt) ?? DateTimeOffset.MaxValue)
                    .ThenBy(r => r.Time(LeaseFields.Default.DueAt) is null && r.Time(JobColumns.CalendarAt) is null)
                    .ThenBy(r => r.Uuid(JobColumns.Id))
                    .FirstOrDefault();
                var e = own.Where(r => r.Time(JobColumns.EndedAt) is not null).MaxBy(r => r.Time(JobColumns.EndedAt));
                var identity = JobRequestData.Identity(subject, null)!;
                var c = application.Recurrences.Committed.Values.FirstOrDefault(c => c.Definition == definition && c.MergeKey == identity);
                if (o is null && e is null && c is null)
                {
                    continue;
                }
                var running = o is not null && Running(o, now);
                var due = o?.Time(LeaseFields.Default.DueAt) is { } at && at != DateTimeOffset.MaxValue ? at : o?.Time(JobColumns.CalendarAt);
                statuses[subject] = new JobStatus(
                    o is null ? JobStatusState.Idle : running ? JobStatusState.Running : o.Bool(JobColumns.ChainWaiting) ? JobStatusState.WaitingForParent : JobStatusState.Scheduled,
                    o?.JobId(),
                    o is not null && !running ? due : null,
                    running ? o!.Time(JobColumns.AttemptStartedAt) : null,
                    o?.Text(JobColumns.Occurrence),
                    o?.Int32(JobColumns.Attempts) ?? 0,
                    o?[JobColumns.ProgressPercent] is int percent ? new JobProgress(percent, o.Text(JobColumns.ProgressMessage)) : null,
                    e?[JobColumns.Outcome] is int outcome ? (JobOutcome)outcome : null,
                    e?.Text(JobColumns.Reason),
                    e?.Time(JobColumns.EndedAt),
                    c?.NextAt,
                    definitionPaused || application.Pauses.Committed.ContainsKey(PauseKey.Of(definition, subject)),
                    o?.Bytes(JobColumns.PendingArgs) is not null);
            }
            return statuses;
        });
        return ValueTask.FromResult<IReadOnlyDictionary<Subject, JobStatus>>(statuses);
    }

    /// <summary>Marks an outstanding job of the definition that is not marked yet.</summary>
    public async ValueTask<bool> WatchAsync(string definition, JobId job, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        using var work = await database.BeginAsync(cancellationToken);
        if (Job(work, definition, job.Value) is not { } r || !r.Outstanding() || r.Bool(JobColumns.Waited))
        {
            return false;
        }
        Write(work, r, [Set(JobColumns.Waited, true)]);
        work.Commit();
        return true;
    }

    public ValueTask<IReadOnlyList<StoredJobOutcome>> GetOutcomesAsync(string definition, IReadOnlyList<JobId> jobs, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(jobs);
        IReadOnlyList<StoredJobOutcome> outcomes = database.ReadCommitted(() => jobs
            .Distinct()
            .Select(j => JobRows.Committed.GetValueOrDefault(Key(j.Value)))
            .Where(r => r is not null && r.Text(JobColumns.Definition) == definition)
            .Select(r => StoredOutcome(r!))
            .ToList());
        return ValueTask.FromResult(outcomes);
    }

    private static StoredJobOutcome StoredOutcome(Record r) => new(
        r.JobId(),
        !r.Outstanding(),
        r.Uuid(JobColumns.EndedExecutionId),
        r[JobColumns.Outcome] is int outcome ? (JobOutcome)outcome : null,
        r.Text(JobColumns.Reason),
        r.Time(JobColumns.EndedAt),
        r.Bytes(JobColumns.Result),
        r.Int32(JobColumns.ResultVersion));

    /// <summary>One page newest first, by start, job and epoch, continuing after the cursor.</summary>
    public ValueTask<JobRunPage> GetRunsAsync(string definition, Subject? subject, JobId? job, int limit, JobRunCursor? after, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        if ((subject is null) == (job is null))
        {
            throw new ArgumentException("Run history is read by a subject or by a job.");
        }
        static int Compare(JobRunEntry e, JobRunCursor c)
        {
            var compared = e.StartedAt.CompareTo(c.StartedAt);
            compared = compared != 0 ? compared : e.Job.Value.CompareTo(c.Job.Value);
            return compared != 0 ? compared : e.Epoch.CompareTo(c.Epoch);
        }
        var entries = database.ReadCommitted(() => application.Runs.Committed.Values
            .Where(r => r.Definition == definition && (subject is not null ? r.Entry.Subject == subject : r.Entry.Job == job))
            .Select(r => r.Entry)
            .Where(e => after is null || Compare(e, after) < 0)
            .OrderByDescending(e => e.StartedAt)
            .ThenByDescending(e => e.Job.Value)
            .ThenByDescending(e => e.Epoch)
            .Take(limit + 1)
            .ToList());
        if (entries.Count <= limit)
        {
            return ValueTask.FromResult(new JobRunPage(entries, null));
        }
        entries.RemoveAt(limit);
        var last = entries[^1];
        return ValueTask.FromResult(new JobRunPage(entries, new JobRunCursor(last.StartedAt, last.Job, last.Epoch)));
    }

    public ValueTask<IReadOnlyDictionary<string, TimeSpan>> NextExpiriesAsync(IReadOnlyCollection<string> definitions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definitions);
        var wanted = definitions.ToHashSet(StringComparer.Ordinal);
        var fields = LeaseFields.Default;
        var now = Now;
        IReadOnlyDictionary<string, TimeSpan> expiries = database.ReadCommitted(() =>
        {
            var earliest = new Dictionary<string, DateTimeOffset>(StringComparer.Ordinal);
            foreach (var r in JobRows.Committed.Values)
            {
                if (r.Text(JobColumns.Definition) is not { } definition || !wanted.Contains(definition)
                    || r.Time(fields.LeaseExpiresAt) is not { } expires || r.Int64(fields.SettledEpoch) >= r.Int64(fields.LeaseEpoch))
                {
                    continue;
                }
                earliest[definition] = earliest.TryGetValue(definition, out var known) && known <= expires ? known : expires;
            }
            return earliest.ToDictionary(e => e.Key, e => e.Value > now ? e.Value - now : TimeSpan.Zero, StringComparer.Ordinal);
        });
        return ValueTask.FromResult(expiries);
    }
}
