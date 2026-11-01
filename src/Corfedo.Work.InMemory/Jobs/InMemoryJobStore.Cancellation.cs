using Corfedo.Work.InMemory.Database;
using static Corfedo.Work.InMemory.Leases.InMemoryLeaseStore;

namespace Corfedo.Work.InMemory.Jobs;

internal sealed partial class InMemoryJobStore
{
    /// <summary>
    /// Clears the occurrence waiting in the identity's recurrence, then cancels the definition's outstanding jobs of the
    /// subject or merge key, standalone jobs and occurrences of the identity's recurrence; the recurrence continues.
    /// Occurrences to be skipped are left to finalization.
    /// </summary>
    public async ValueTask<int> CancelAsync(WorkTransaction transaction, LeaseSource<JobId> source, Subject? subject, string? mergeKey, JobEndedCallback? ended, CancellationToken cancellationToken)
    {
        if ((subject is null) == (mergeKey is null))
        {
            throw new ArgumentException("Cancel by a subject or by a merge key.", nameof(subject));
        }
        var work = Join(transaction);
        var definition = Rules(source).Job!.Definition;
        var identity = JobRequestData.Identity(subject, mergeKey)!;
        var recurrences = work.Scan(application.Recurrences).Select(c => c.Row).Where(c => c.Definition == definition && c.MergeKey == identity).ToList();
        foreach (var recurrence in recurrences.Where(c => c.WaitingAt is not null))
        {
            work.Put(application.Recurrences, recurrence.Id, recurrence with { WaitingAt = null });
        }
        var ids = recurrences.Select(c => c.Id).ToHashSet();
        var locked = SelectForCancel(work, Jobs(work, definition).Where(r =>
            subject is null
                ? r.Text(JobColumns.MergeKey) == identity || (r.Uuid(JobColumns.RecurrenceId) is { } recurrence && ids.Contains(recurrence))
                : r.Text(JobColumns.SubjectKind) == subject.Kind && r.Text(JobColumns.SubjectId) == subject.Id));
        await CancelSelectedAsync(work, locked, key => key == definition ? (source, ended) : null, cancellationToken);
        return locked.Selected.Count;
    }

    /// <summary>
    /// Clears the occurrences waiting in the subject's recurrences, refuses the cancellation when one of the subject's
    /// outstanding jobs belongs to a definition <paramref name="definitions"/> does not resolve, then cancels them per
    /// definition. Occurrences to be skipped are left to finalization.
    /// </summary>
    public async ValueTask<int> CancelAllAsync(WorkTransaction transaction, Subject subject, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(subject);
        ArgumentNullException.ThrowIfNull(definitions);
        var work = Join(transaction);
        foreach (var (id, recurrence) in work.Scan(application.Recurrences).Where(c => c.Row.Subject == subject && c.Row.WaitingAt is not null).ToList())
        {
            work.Put(application.Recurrences, id, recurrence with { WaitingAt = null });
        }
        var locked = SelectForCancel(work, Jobs(work).Where(r => r.Text(JobColumns.SubjectKind) == subject.Kind && r.Text(JobColumns.SubjectId) == subject.Id));
        await CancelSelectedAsync(work, locked, definitions, cancellationToken);
        return locked.Selected.Count;
    }

    /// <summary>
    /// The outstanding jobs among <paramref name="selection"/> that are not to be skipped, with the steps of their chains
    /// that wait for their parent and every outstanding step of the chains they own, in the order cancellations end
    /// them: chain steps waiting for their parent after the others, and occurrences that own a recurring chain last,
    /// each in id order.
    /// </summary>
    private LockedForCancel SelectForCancel(InMemoryTransaction work, IEnumerable<Record> selection)
    {
        static bool Live(Record r) => r.Outstanding() && !r.Bool(JobColumns.SkipPending);
        var selected = selection.Where(Live).ToList();
        var ids = selected.Select(r => r.Uuid(JobColumns.Id)!.Value).ToHashSet();
        var chains = selected.Select(r => r.Uuid(JobColumns.ChainId)).OfType<Guid>().ToHashSet();
        var owned = selected.Select(r => r.Uuid(JobColumns.OwnedChain)).OfType<Guid>().ToHashSet();
        var locked = new LockedForCancel([], [], []);
        var candidates = Jobs(work).Where(r => Live(r)
            && (ids.Contains(r.Uuid(JobColumns.Id)!.Value)
                || (r.Uuid(JobColumns.ChainId) is { } chain && ((chains.Contains(chain) && r.Bool(JobColumns.ChainWaiting)) || owned.Contains(chain)))));
        foreach (var r in candidates.OrderBy(r => r.Uuid(JobColumns.OwnedChain) is not null).ThenBy(r => r.Bool(JobColumns.ChainWaiting)).ThenBy(r => r.Uuid(JobColumns.Id)))
        {
            var job = (r.Uuid(JobColumns.Id)!.Value, r.Text(JobColumns.Definition)!);
            if (ids.Contains(job.Value))
            {
                (r.Uuid(JobColumns.OwnedChain) is not null ? locked.Owners : locked.Selected).Add(job);
            }
            else if (r.Uuid(JobColumns.ChainId) is { } chain && owned.Contains(chain))
            {
                locked.OwnedSteps.Add(job);
            }
        }
        locked.Selected.AddRange(locked.Owners);
        return locked;
    }

    /// <summary>
    /// The jobs a cancellation selected: those it ends, the occurrences among them that own a recurring chain, and the
    /// outstanding steps of the chains they own.
    /// </summary>
    private sealed record LockedForCancel(List<(Guid Id, string Definition)> Selected, List<(Guid Id, string Definition)> Owners, List<(Guid Id, string Definition)> OwnedSteps);

    /// <summary>
    /// Cancels what <see cref="SelectForCancel"/> selected, per definition: first the steps of the chains selected
    /// occurrences own, so those occurrences stay running until their chains settle and their definition's maintenance
    /// ends them with the chains' outcome, then every other selected job. Refused when a job belongs to a definition
    /// <paramref name="definitions"/> does not resolve, since its outcome hook must run.
    /// </summary>
    private async Task CancelSelectedAsync(InMemoryTransaction work, LockedForCancel locked, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken)
    {
        var owners = locked.Owners.Select(j => j.Id).ToHashSet();
        var resolved = new List<((LeaseSource<JobId> Source, JobEndedCallback? Ended) Definition, Guid[] Jobs)>();
        foreach (var batch in new[] { locked.OwnedSteps, locked.Selected.Where(j => !owners.Contains(j.Id)).ToList() })
        {
            foreach (var group in batch.GroupBy(j => j.Definition, StringComparer.Ordinal))
            {
                var definition = definitions(group.Key)
                    ?? throw new InvalidOperationException($"Job definition {group.Key} has jobs this cancellation ends and is not registered in this process, so its outcome hook cannot run; register it with AddJob or AddJobDefinition.");
                resolved.Add((definition, [.. group.Select(j => j.Id)]));
            }
        }
        foreach (var (definition, ids) in resolved)
        {
            await CancelSelectedAsync(work, definition.Source, ids, definition.Ended, cancellationToken);
        }
    }

    /// <summary>
    /// Cancels outstanding jobs and discards their pending follow-ups. A job whose lease is valid gets a cancellation
    /// request, and its holder's process is signalled; it ends cancelled when its run observes the request. Every other
    /// job ends cancelled now, through the terminal path.
    /// </summary>
    private async Task CancelSelectedAsync(InMemoryTransaction work, LeaseSource<JobId> source, Guid[] jobs, JobEndedCallback? ended, CancellationToken cancellationToken)
    {
        var rules = Rules(source);
        var now = Now;
        var held = new HashSet<Guid>();
        var owners = new List<Guid>();
        foreach (var id in jobs)
        {
            if (rules.Find(work, Key(id)) is { } r && r.Time(LeaseFields.Default.LeaseExpiresAt) > now)
            {
                Write(work, r, [
                    Set(LeaseFields.Default.CancelRequestedAt, r.Time(LeaseFields.Default.CancelRequestedAt) ?? now),
                    Set(JobColumns.PendingArgs, null),
                    Set(JobColumns.PendingArgsVersion, null),
                    Set(JobColumns.PendingDue, null),
                    Set(JobColumns.PendingFirstRequestedAt, null),
                ]);
                held.Add(id);
                owners.Add(r.Uuid(LeaseFields.Default.LeaseOwner)!.Value);
            }
        }
        foreach (var owner in owners.Distinct())
        {
            leases.Signal(work, WorkTopics.Cancel, WorkTopics.CancelKey(new OwnerId(owner), source.Name));
        }
        foreach (var job in jobs.Where(j => !held.Contains(j)))
        {
            await EndJobAsync(work, source, new JobId(job), JobOutcome.Cancelled, JobReasons.Cancelled, discardPending: true, r => NotHeld(r, Now), ended, cancellationToken);
        }
    }

    /// <summary>
    /// Ends unheld outstanding jobs whose argument version is below the stored minimum, one per transaction, failed with
    /// <c>args_version</c> through the terminal path, which then starts a pending follow-up. A job whose transition
    /// fails, for example in its outcome hook, is skipped for the rest of the call.
    /// </summary>
    public async ValueTask<int> RetireAsync(LeaseSource<JobId> source, JobEndedCallback? ended, int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var rules = Rules(source);
        var definition = rules.Job!.Definition;
        var failed = new HashSet<Guid>();
        var count = 0;
        while (count < limit)
        {
            using var transaction = await database.BeginAsync(cancellationToken);
            bool Retired(Record r) =>
                r.Outstanding() && NotHeld(r, Now) && Stored(transaction, definition) is { } stored && r.Int32(JobColumns.ArgsVersion) < stored.Policy.MinimumArgsVersion;
            if (rules.Scan(transaction).Select(r => r.Row).Where(r => Retired(r) && !failed.Contains(r.Uuid(JobColumns.Id)!.Value)).MinBy(r => r.Uuid(JobColumns.Id)) is not { } found)
            {
                return count;
            }
            var job = found.JobId();
            try
            {
                if (await EndJobAsync(transaction, source, job, JobOutcome.Failed, JobReasons.ArgsVersion, discardPending: false, Retired, ended, cancellationToken))
                {
                    transaction.Commit();
                    count++;
                }
            }
            catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
            {
                failed.Add(job.Value);
                LogRetireFailed(logger, telemetry.Logged(exception), definition, job.Value, telemetry.Describe(exception));
            }
        }
        return count;
    }
}
