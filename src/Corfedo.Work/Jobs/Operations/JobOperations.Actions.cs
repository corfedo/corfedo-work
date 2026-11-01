namespace Corfedo.Work;

/// <summary>
/// Operator actions on one job by its id (J21). Each runs in a transaction of its own, runs no outcome hook and never
/// duplicates running work; the job's state decides the reported outcome instead of an exception.
/// </summary>
public sealed partial class JobOperations
{
    /// <summary>
    /// Makes <paramref name="job"/> due now: one scheduled later or waiting for a retry keeps its attempts
    /// (<see cref="RunNowOutcome.MadeDue"/>); a paused one is due once resumed (<see cref="RunNowOutcome.Paused"/>). A
    /// running job, one already due, an occurrence waiting to be recorded skipped and a terminal one are reported as
    /// found. An occurrence of a recurrence that has ended triggers the recurrence, with the outcomes of
    /// <see cref="TriggerAsync(WorkTransaction, JobDefinition, Subject, CancellationToken)"/>. A job with a calendar is
    /// planned at the next allowed instant.
    /// </summary>
    public ValueTask<RunNowResult> RunNowAsync(JobDefinition definition, JobId job, CancellationToken cancellationToken = default) =>
        RunNowAsync(definition, job, JobActionOptions.None, cancellationToken);

    /// <summary>
    /// <see cref="RunNowAsync(JobDefinition, JobId, CancellationToken)"/>; with
    /// <see cref="JobActionOptions.IgnoreCalendar"/> a job with a calendar is due now.
    /// </summary>
    public async ValueTask<RunNowResult> RunNowAsync(JobDefinition definition, JobId job, JobActionOptions options, CancellationToken cancellationToken = default)
    {
        var registration = registry.Find(definition);
        return await Store.RunNowAsync(await registration.SourceAsync(store, cancellationToken), job, Ignores(options), cancellationToken);
    }

    /// <summary>
    /// Runs the terminal one-shot <paramref name="job"/> again, due now, with the same job id, a new execution id and
    /// fresh attempts. Its last outcome stays readable until the new execution ends. Refused while an execution of the
    /// job is outstanding or another outstanding standalone job holds its identity, and for occurrences of recurrences.
    /// A job with a calendar is planned at the next allowed instant.
    /// </summary>
    public ValueTask<RerunResult> RerunAsync(JobDefinition definition, JobId job, CancellationToken cancellationToken = default) =>
        RerunAsync(definition, job, JobActionOptions.None, cancellationToken);

    /// <summary>
    /// <see cref="RerunAsync(JobDefinition, JobId, CancellationToken)"/>; with
    /// <see cref="JobActionOptions.IgnoreCalendar"/> a job with a calendar is due now.
    /// </summary>
    public async ValueTask<RerunResult> RerunAsync(JobDefinition definition, JobId job, JobActionOptions options, CancellationToken cancellationToken = default)
    {
        var registration = registry.Find(definition);
        return await Store.RerunAsync(await registration.SourceAsync(store, cancellationToken), job, Ignores(options), cancellationToken);
    }

    /// <summary>
    /// Reruns each job as <see cref="RerunAsync(JobDefinition, JobId, CancellationToken)"/> does, each in its own
    /// transaction, and reports one result per given id in order; an id given again is reported
    /// <see cref="RerunOutcome.Duplicate"/>.
    /// </summary>
    public ValueTask<IReadOnlyList<RerunResult>> RerunManyAsync(JobDefinition definition, IEnumerable<JobId> jobs, CancellationToken cancellationToken = default) =>
        RerunManyAsync(definition, jobs, JobActionOptions.None, cancellationToken);

    /// <summary>
    /// <see cref="RerunManyAsync(JobDefinition, IEnumerable{JobId}, CancellationToken)"/>; with
    /// <see cref="JobActionOptions.IgnoreCalendar"/> jobs with a calendar are due now.
    /// </summary>
    public async ValueTask<IReadOnlyList<RerunResult>> RerunManyAsync(JobDefinition definition, IEnumerable<JobId> jobs, JobActionOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(jobs);
        var registration = registry.Find(definition);
        var source = await registration.SourceAsync(store, cancellationToken);
        var seen = new HashSet<JobId>();
        var results = new List<RerunResult>();
        foreach (var job in jobs)
        {
            results.Add(seen.Add(job) ? await Store.RerunAsync(source, job, Ignores(options), cancellationToken) : new RerunResult(job, RerunOutcome.Duplicate));
        }
        return results;
    }

    private static bool Ignores(JobActionOptions options) => (options & JobActionOptions.IgnoreCalendar) != 0;
}
