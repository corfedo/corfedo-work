using System.Diagnostics;

namespace Corfedo.Work;

/// <summary>One item of a fan-out: its arguments and identity.</summary>
public sealed record JobRequestItem<TArgs>(TArgs Args, JobRequestOptions Options);

/// <summary>
/// Job operations for application code. Every operation that joins the caller's transaction is refused for a
/// definition this process does not register, since a terminal transition it causes must run the definition's hook.
/// </summary>
public sealed partial class JobOperations(IWorkStore store, JobRegistry registry, IWorkSignals? signals = null)
{
    private IJobStore Store => store.Jobs;

    /// <summary>Requests a job in the caller's transaction; it is signalled to claimers on commit (R1, J1, J3).</summary>
    public async ValueTask<JobRequestResult> RequestAsync<TArgs>(IWorkTransaction transaction, JobDefinition<TArgs> definition, TArgs args, Schedule schedule, JobRequestOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schedule);
        var registration = registry.Find(definition);
        var request = Data(registry.Args(definition), definition, args, schedule, options ?? JobRequestOptions.Default);
        return await Store.RequestAsync(transaction, await registration.SourceAsync(store, cancellationToken), request, cancellationToken);
    }

    /// <summary>Requests one job per item in the caller's transaction: all commit or none do (J9).</summary>
    public async ValueTask<IReadOnlyList<JobRequestResult>> RequestManyAsync<TArgs>(IWorkTransaction transaction, JobDefinition<TArgs> definition, IEnumerable<JobRequestItem<TArgs>> items, Schedule schedule, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(items);
        ArgumentNullException.ThrowIfNull(schedule);
        var registration = registry.Find(definition);
        var codec = registry.Args(definition);
        var requests = items.Select(item => Data(codec, definition, item.Args, schedule, item.Options)).ToList();
        return await Store.RequestManyAsync(transaction, await registration.SourceAsync(store, cancellationToken), requests, cancellationToken);
    }

    /// <summary>
    /// Cancels the definition's outstanding jobs of <paramref name="subject"/>, standalone jobs and occurrences of its
    /// recurrence, in the caller's transaction: a scheduled one ends cancelled with its outcome hook; a running one is
    /// cancelled through its lease and ends cancelled (J7, D2).
    /// </summary>
    public ValueTask<int> CancelAsync(IWorkTransaction transaction, JobDefinition definition, Subject subject, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(subject);
        return CancelAsync(transaction, definition, subject, null, cancellationToken);
    }

    /// <summary>Cancels the definition's outstanding job with <paramref name="mergeKey"/> in the caller's transaction.</summary>
    public ValueTask<int> CancelAsync(IWorkTransaction transaction, JobDefinition definition, string mergeKey, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(mergeKey);
        return CancelAsync(transaction, definition, null, mergeKey, cancellationToken);
    }

    /// <summary>
    /// Cancels the outstanding jobs of every definition acting on <paramref name="subject"/> in the caller's
    /// transaction; refused when one belongs to a definition this process does not register.
    /// </summary>
    public async ValueTask<int> CancelAllAsync(IWorkTransaction transaction, Subject subject, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(subject);
        return await Store.CancelAllAsync(transaction, subject, await DefinitionsAsync(cancellationToken), cancellationToken);
    }

    private async ValueTask<int> CancelAsync(IWorkTransaction transaction, JobDefinition definition, Subject? subject, string? mergeKey, CancellationToken cancellationToken)
    {
        var registration = registry.Find(definition);
        return await Store.CancelAsync(transaction, await registration.SourceAsync(store, cancellationToken), subject, mergeKey, registry.Ended(registration), cancellationToken);
    }

    /// <summary>
    /// Creates the recurrence of the options' identity, or replaces its schedule, policies and arguments, in the caller's
    /// transaction (J1, D6). Standalone jobs of the identity are left alone; a replacement applies from the next
    /// occurrence. Call it before the transaction's own writes.
    /// Recurrence operations need the store's read-committed isolation where it has one, and refuse other levels with
    /// <see cref="InvalidOperationException"/> before changing anything.
    /// </summary>
    public async ValueTask<RecurrenceOutcome> ScheduleAsync<TArgs>(IWorkTransaction transaction, JobDefinition<TArgs> definition, TArgs args, Schedule schedule, RecurrenceOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        var registration = registry.Find(definition);
        var data = Recurrence(registry.Args(definition), definition, args, schedule, Identity(definition, options.Subject, options.MergeKey), options.Subject, options);
        return await Store.ScheduleAsync(transaction, await registration.SourceAsync(store, cancellationToken), data, RecurrenceCalculator.Instance, cancellationToken);
    }

    /// <summary>
    /// Cancels the current occurrences of the subject's recurrence and deletes it, in the caller's transaction; false
    /// when there is none.
    /// </summary>
    public ValueTask<bool> RemoveAsync(IWorkTransaction transaction, JobDefinition definition, Subject subject, CancellationToken cancellationToken = default) =>
        RemoveIdentityAsync(transaction, definition, Identity(definition, subject, null), cancellationToken);

    /// <summary>Cancels the current occurrences of the merge key's recurrence and deletes it; false when there is none.</summary>
    public ValueTask<bool> RemoveAsync(IWorkTransaction transaction, JobDefinition definition, string mergeKey, CancellationToken cancellationToken = default) =>
        RemoveIdentityAsync(transaction, definition, Identity(definition, null, mergeKey), cancellationToken);

    /// <summary>
    /// Removes the definition's code-defined recurrence, which no process declares any longer; false when there is none.
    /// A process that still declares it stores it again at its next start.
    /// </summary>
    public ValueTask<bool> RemoveAsync(IWorkTransaction transaction, JobDefinition definition, CancellationToken cancellationToken = default) =>
        RemoveIdentityAsync(transaction, definition, JobRequestData.DefinitionIdentity, cancellationToken);

    /// <summary>
    /// Triggers the subject's recurrence without moving it (J21): a triggered occurrence due now when none is
    /// outstanding, an outstanding one that is not running due now with its attempts kept, or with one running the
    /// overlap policy: <see cref="RunNowOutcome.Waiting"/> under <see cref="Overlap.Wait"/>, which runs once after it,
    /// <see cref="RunNowOutcome.AlreadyRunning"/> under <see cref="Overlap.Skip"/>. Work it starts or makes due with a
    /// calendar is planned at the next allowed instant.
    /// </summary>
    public ValueTask<RunNowResult> TriggerAsync(IWorkTransaction transaction, JobDefinition definition, Subject subject, CancellationToken cancellationToken = default) =>
        TriggerIdentityAsync(transaction, definition, Identity(definition, subject, null), JobActionOptions.None, cancellationToken);

    /// <summary>
    /// Triggers the subject's recurrence as <see cref="TriggerAsync(IWorkTransaction, JobDefinition, Subject, CancellationToken)"/>
    /// does; with <see cref="JobActionOptions.IgnoreCalendar"/> the occurrence it starts or makes due is due now.
    /// </summary>
    public ValueTask<RunNowResult> TriggerAsync(IWorkTransaction transaction, JobDefinition definition, Subject subject, JobActionOptions options, CancellationToken cancellationToken = default) =>
        TriggerIdentityAsync(transaction, definition, Identity(definition, subject, null), options, cancellationToken);

    /// <summary>Triggers the merge key's recurrence without moving it.</summary>
    public ValueTask<RunNowResult> TriggerAsync(IWorkTransaction transaction, JobDefinition definition, string mergeKey, CancellationToken cancellationToken = default) =>
        TriggerIdentityAsync(transaction, definition, Identity(definition, null, mergeKey), JobActionOptions.None, cancellationToken);

    /// <summary>Triggers the merge key's recurrence without moving it, ignoring its calendar with <see cref="JobActionOptions.IgnoreCalendar"/>.</summary>
    public ValueTask<RunNowResult> TriggerAsync(IWorkTransaction transaction, JobDefinition definition, string mergeKey, JobActionOptions options, CancellationToken cancellationToken = default) =>
        TriggerIdentityAsync(transaction, definition, Identity(definition, null, mergeKey), options, cancellationToken);

    /// <summary>Triggers the definition's code-defined recurrence without moving it.</summary>
    public ValueTask<RunNowResult> TriggerAsync(IWorkTransaction transaction, JobDefinition definition, CancellationToken cancellationToken = default) =>
        TriggerIdentityAsync(transaction, definition, JobRequestData.DefinitionIdentity, JobActionOptions.None, cancellationToken);

    /// <summary>Triggers the definition's code-defined recurrence without moving it, ignoring its calendar with <see cref="JobActionOptions.IgnoreCalendar"/>.</summary>
    public ValueTask<RunNowResult> TriggerAsync(IWorkTransaction transaction, JobDefinition definition, JobActionOptions options, CancellationToken cancellationToken = default) =>
        TriggerIdentityAsync(transaction, definition, JobRequestData.DefinitionIdentity, options, cancellationToken);

    /// <summary>
    /// Stops claiming the definition's jobs and planning its recurrences, in the caller's transaction (J8); claims in
    /// flight finish first. Running jobs continue.
    /// </summary>
    public ValueTask PauseAsync(IWorkTransaction transaction, JobDefinition definition, CancellationToken cancellationToken = default) =>
        PauseDefinitionAsync(transaction, definition, true, cancellationToken);

    /// <summary>
    /// Restarts claiming and planning of the definition. Instants that passed while it was paused follow the missed
    /// rule; jobs that became due follow their <see cref="Late"/> policy at their claim.
    /// </summary>
    public ValueTask ResumeAsync(IWorkTransaction transaction, JobDefinition definition, CancellationToken cancellationToken = default) =>
        PauseDefinitionAsync(transaction, definition, false, cancellationToken);

    /// <summary>
    /// Stops claiming the subject's jobs of the definition, including jobs requested and occurrences planned while it is
    /// paused, and planning its recurrence, in the caller's transaction (J8, D2). Running jobs continue.
    /// </summary>
    public ValueTask PauseAsync(IWorkTransaction transaction, JobDefinition definition, Subject subject, CancellationToken cancellationToken = default) =>
        PauseSubjectAsync(transaction, definition, subject, true, cancellationToken);

    /// <summary>Restarts claiming and planning for the subject, under the missed and late policies as for a definition.</summary>
    public ValueTask ResumeAsync(IWorkTransaction transaction, JobDefinition definition, Subject subject, CancellationToken cancellationToken = default) =>
        PauseSubjectAsync(transaction, definition, subject, false, cancellationToken);

    private async ValueTask<bool> RemoveIdentityAsync(IWorkTransaction transaction, JobDefinition definition, string identity, CancellationToken cancellationToken)
    {
        var registration = registry.Find(definition);
        return await Store.RemoveAsync(transaction, await registration.SourceAsync(store, cancellationToken), identity, registry.Ended(registration), await DefinitionsAsync(cancellationToken), cancellationToken);
    }

    private async ValueTask<RunNowResult> TriggerIdentityAsync(IWorkTransaction transaction, JobDefinition definition, string identity, JobActionOptions options, CancellationToken cancellationToken)
    {
        var registration = registry.Find(definition);
        return await Store.TriggerAsync(transaction, await registration.SourceAsync(store, cancellationToken), identity, (options & JobActionOptions.IgnoreCalendar) != 0, cancellationToken);
    }

    private async ValueTask PauseDefinitionAsync(IWorkTransaction transaction, JobDefinition definition, bool paused, CancellationToken cancellationToken)
    {
        var registration = registry.Find(definition);
        await Store.PauseDefinitionAsync(transaction, await registration.SourceAsync(store, cancellationToken), paused, cancellationToken);
    }

    private async ValueTask PauseSubjectAsync(IWorkTransaction transaction, JobDefinition definition, Subject subject, bool paused, CancellationToken cancellationToken)
    {
        Identity(definition, subject, null);
        var registration = registry.Find(definition);
        await Store.PauseSubjectAsync(transaction, await registration.SourceAsync(store, cancellationToken), subject, paused, cancellationToken);
    }

    /// <summary>The canonical identity of a subject or merge key, checked against the definition's subject kind.</summary>
    private static string Identity(JobDefinition definition, Subject? subject, string? mergeKey)
    {
        if (definition.Subject is { } kind)
        {
            if (subject is null || subject.Kind != kind)
            {
                throw new ArgumentException($"Jobs of {definition.Key} act on {kind} subjects; name one.", nameof(subject));
            }
            if (mergeKey is not null)
            {
                throw new ArgumentException($"Jobs of {definition.Key} merge by their subject; a merge key is not used.", nameof(mergeKey));
            }
        }
        else if (subject is not null)
        {
            throw new ArgumentException($"Jobs of {definition.Key} have no subject kind.", nameof(subject));
        }
        else if (mergeKey is null)
        {
            throw new ArgumentException($"A recurrence of {definition.Key} needs a merge key; the definition's own recurrence is declared with AddRecurring.", nameof(mergeKey));
        }
        return JobRequestData.Identity(subject, mergeKey)!;
    }

    /// <summary>A recurrence as the store applies it, with its schedule validated.</summary>
    internal static RecurrenceData Recurrence<TArgs>(IJobArgs<TArgs> codec, JobDefinition definition, TArgs args, Schedule schedule, string identity, Subject? subject, RecurrenceOptions options)
    {
        CheckRecurrence(schedule, options);
        var data = codec.Write(args);
        if (data.Length > JobArgs.MaximumBytes)
        {
            throw new ArgumentException($"Arguments of {definition.Key} are {data.Length} bytes; at most {JobArgs.MaximumBytes} are stored.", nameof(args));
        }
        return new RecurrenceData(data, codec.Version, schedule, options.Missed, options.Overlap, identity, subject, options.Partition, options.Mode);
    }

    /// <summary>Validates a recurrence's schedule and options.</summary>
    internal static void CheckRecurrence(Schedule schedule, RecurrenceOptions options)
    {
        ArgumentNullException.ThrowIfNull(schedule);
        RecurrenceSchedule.Create(schedule, DateTimeOffset.UtcNow);
        if (options.Mode == RequestMode.New)
        {
            throw new ArgumentException("An identity has one recurrence; RequestMode.New does not apply to recurring schedules.", nameof(options));
        }
        if (options.Overlap == Overlap.Allow && schedule is { Kind: ScheduleKind.Every, Interval: Interval.AfterCompletion })
        {
            throw new ArgumentException("Overlap.Allow does not combine with an after-completion interval.", nameof(options));
        }
        if (!Enum.IsDefined(options.Missed) || !Enum.IsDefined(options.Overlap))
        {
            throw new ArgumentException("Unknown missed or overlap policy.", nameof(options));
        }
        if (options.Partition is "")
        {
            throw new ArgumentException("A partition cannot be empty.", nameof(options));
        }
    }

    internal static JobRequestData Data<TArgs>(IJobArgs<TArgs> codec, JobDefinition definition, TArgs args, Schedule schedule, JobRequestOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (schedule.Recurring)
        {
            throw new ArgumentException($"A request takes a one-shot schedule; {schedule.Kind} is recurring.", nameof(schedule));
        }
        if (definition.Subject is { } kind)
        {
            if (options.Subject is not { } subject || subject.Kind != kind)
            {
                throw new ArgumentException($"Jobs of {definition.Key} act on {kind} subjects; the request must name one.", nameof(options));
            }
            if (options.MergeKey is not null)
            {
                throw new ArgumentException($"Jobs of {definition.Key} merge by their subject; a merge key is not used.", nameof(options));
            }
        }
        else if (options.Subject is not null)
        {
            throw new ArgumentException($"Jobs of {definition.Key} have no subject kind.", nameof(options));
        }
        if (options.Partition is "")
        {
            throw new ArgumentException("A partition cannot be empty.", nameof(options));
        }
        var data = codec.Write(args);
        if (data.Length > JobArgs.MaximumBytes)
        {
            throw new ArgumentException($"Arguments of {definition.Key} are {data.Length} bytes; at most {JobArgs.MaximumBytes} are stored. Store large inputs in the domain and pass their identity.", nameof(args));
        }
        return new JobRequestData(data, codec.Version, schedule, options.Subject, options.MergeKey, options.Partition, options.Mode, TraceParent());
    }

    /// <summary>The W3C id of the current activity, which a created job keeps for the span links of its runs; null without one.</summary>
    private static string? TraceParent() => Activity.Current is { IdFormat: ActivityIdFormat.W3C } current ? current.Id : null;
}
