namespace Corfedo.Work;

/// <summary>
/// Reads of job status, outcomes and run history. They run no outcome hook, so they do not require the definition to
/// be registered in this process.
/// </summary>
public sealed partial class JobOperations
{
    /// <summary>The most subjects one <see cref="GetManyAsync"/> reads.</summary>
    public const int MaximumSubjects = 1000;

    /// <summary>The most entries one page of run history holds.</summary>
    public const int MaximumRuns = 1000;

    private JobWaiter? waiter;

    private JobWaiter Waiter => LazyInitializer.EnsureInitialized(ref waiter, () => new JobWaiter(store, signals));

    /// <summary>
    /// The scheduling status of <paramref name="subject"/>'s jobs of the definition (D5), or null when the subject has
    /// no outstanding job, recurrence or ended job.
    /// </summary>
    public async ValueTask<JobStatus?> GetAsync(JobDefinition definition, Subject subject, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        Identity(definition, subject, null);
        return (await Store.GetStatusAsync(definition.Key, [subject], cancellationToken)).GetValueOrDefault(subject);
    }

    /// <summary>
    /// The scheduling status of up to <see cref="MaximumSubjects"/> subjects in one read; subjects with no outstanding
    /// job, recurrence or ended job are left out.
    /// </summary>
    public async ValueTask<IReadOnlyDictionary<Subject, JobStatus>> GetManyAsync(JobDefinition definition, IEnumerable<Subject> subjects, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(subjects);
        var distinct = subjects.Distinct().ToList();
        if (distinct.Count > MaximumSubjects)
        {
            throw new ArgumentException($"At most {MaximumSubjects} subjects are read at once; {distinct.Count} were given.", nameof(subjects));
        }
        foreach (var subject in distinct)
        {
            Identity(definition, subject, null);
        }
        return await Store.GetStatusAsync(definition.Key, distinct, cancellationToken);
    }

    /// <summary>
    /// The latest ended execution of <paramref name="job"/>, with <see cref="JobOutcomeStatus.Ended"/> false while an
    /// execution of it is outstanding; null when the definition has no such job.
    /// </summary>
    public async ValueTask<JobOutcomeStatus?> GetOutcomeAsync(JobDefinition definition, JobId job, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        return await ReadOutcomeAsync(definition, job, cancellationToken) is { } stored
            ? new JobOutcomeStatus(stored.Job, stored.Ended, stored.ExecutionId, stored.Outcome, stored.Reason, stored.EndedAt)
            : null;
    }

    /// <summary>
    /// The latest ended execution of <paramref name="job"/> with its result (J20), decoded with the definition's codec;
    /// null when the definition has no such job.
    /// </summary>
    public async ValueTask<JobOutcomeStatus<TResult>?> GetOutcomeAsync<TArgs, TResult>(JobDefinition<TArgs, TResult> definition, JobId job, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        return await ReadOutcomeAsync(definition, job, cancellationToken) is { } stored ? Typed(definition.Key, registry.Results(definition), stored) : null;
    }

    /// <summary>
    /// Waits until <paramref name="job"/> has no outstanding execution, including a follow-up that reuses its id, or
    /// <paramref name="timeout"/> passes, and returns its outcome as <see cref="GetOutcomeAsync(JobDefinition, JobId, CancellationToken)"/>
    /// does: with <see cref="JobOutcomeStatus.Ended"/> false at the timeout. Woken by the store's signals where it has
    /// them. Null when the definition has no such job.
    /// </summary>
    public async ValueTask<JobOutcomeStatus?> WaitAsync(JobDefinition definition, JobId job, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        await WaitEndedAsync(definition, job, timeout, cancellationToken);
        return await GetOutcomeAsync(definition, job, cancellationToken);
    }

    /// <summary>
    /// Waits as <see cref="WaitAsync(JobDefinition, JobId, TimeSpan, CancellationToken)"/> and returns the outcome
    /// with its result.
    /// </summary>
    public async ValueTask<JobOutcomeStatus<TResult>?> WaitAsync<TArgs, TResult>(JobDefinition<TArgs, TResult> definition, JobId job, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        await WaitEndedAsync(definition, job, timeout, cancellationToken);
        return await GetOutcomeAsync(definition, job, cancellationToken);
    }

    /// <summary>
    /// Run history of <paramref name="subject"/>'s jobs of the definition (D7), newest first: up to
    /// <paramref name="limit"/> entries after <paramref name="after"/>, the cursor of the previous page.
    /// </summary>
    public ValueTask<JobRunPage> GetRunsAsync(JobDefinition definition, Subject subject, int limit = 50, JobRunCursor? after = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        Identity(definition, subject, null);
        return Store.GetRunsAsync(definition.Key, subject, null, CheckLimit(limit), after, cancellationToken);
    }

    /// <summary>Run history of one job of the definition, newest first, paged as for a subject.</summary>
    public ValueTask<JobRunPage> GetRunsAsync(JobDefinition definition, JobId job, int limit = 50, JobRunCursor? after = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        return Store.GetRunsAsync(definition.Key, null, job, CheckLimit(limit), after, cancellationToken);
    }

    private async ValueTask WaitEndedAsync(JobDefinition definition, JobId job, TimeSpan timeout, CancellationToken cancellationToken)
    {
        if (timeout != Timeout.InfiniteTimeSpan)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(timeout, TimeSpan.Zero);
        }
        if (await ReadOutcomeAsync(definition, job, cancellationToken) is { Ended: false })
        {
            await Waiter.WaitAsync(definition, job, timeout, cancellationToken);
        }
    }

    private async ValueTask<StoredJobOutcome?> ReadOutcomeAsync(JobDefinition definition, JobId job, CancellationToken cancellationToken) =>
        (await Store.GetOutcomesAsync(definition.Key, [job], cancellationToken)).SingleOrDefault();

    /// <exception cref="JobResultVersionException">The stored result is newer than the definition's codec reads.</exception>
    internal static JobOutcomeStatus<TResult> Typed<TResult>(string definition, IJobResults<TResult> results, StoredJobOutcome stored)
    {
        var hasResult = stored.Result is not null;
        if (hasResult && stored.ResultVersion > results.Version)
        {
            throw new JobResultVersionException(definition, stored.ResultVersion, results.Version);
        }
        var result = hasResult ? results.Read(stored.Result, stored.ResultVersion) : default!;
        return new JobOutcomeStatus<TResult>(stored.Job, stored.Ended, stored.ExecutionId, stored.Outcome, stored.Reason, stored.EndedAt, hasResult, result);
    }

    private static int CheckLimit(int limit)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(limit, MaximumRuns);
        return limit;
    }
}
