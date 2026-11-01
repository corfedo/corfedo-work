namespace Corfedo.Work;

/// <summary>Runs claimed jobs of one definition.</summary>
public interface IJobHandler<TArgs>
{
    /// <summary>
    /// Runs one attempt. Returns the result for the dispatcher to apply in its own guarded transaction, or
    /// <see cref="JobRun.Completed"/> after applying it with <see cref="JobRun.CompleteAsync"/> in the domain's
    /// transaction. A thrown exception is <c>Retry(0, "handler_failed")</c>.
    /// </summary>
    ValueTask<JobDisposition> RunAsync(TArgs args, JobRun run, CancellationToken cancellationToken);
}

/// <summary>
/// What a job handler returns: a <see cref="JobResult"/>, which converts implicitly, for the dispatcher to apply, or
/// <see cref="JobRun.Completed"/> when the handler applied its result itself.
/// </summary>
public sealed class JobDisposition
{
    private JobDisposition(JobResult? result) => Result = result;

    internal static JobDisposition Completed { get; } = new(null);

    /// <summary>The result for the dispatcher to apply; null when the handler applied one itself.</summary>
    public JobResult? Result { get; }

    public static implicit operator JobDisposition(JobResult result) => FromResult(result);

    public static JobDisposition FromResult(JobResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return new(result);
    }
}

/// <summary>Runs claimed jobs of a definition with a result type.</summary>
public interface IJobHandler<TArgs, TResult>
{
    /// <summary>
    /// Runs one attempt. Returns a result value, which completes the job with it stored, or a <see cref="JobResult"/>
    /// for the dispatcher to apply (<see cref="JobResult.Completed"/> completes without a result), or
    /// <see cref="JobRun.Completed"/> after completing with <see cref="JobRun{TResult}.CompleteAsync(WorkTransaction, TResult, CancellationToken)"/>
    /// in the domain's transaction. A thrown exception is <c>Retry(0, "handler_failed")</c>.
    /// </summary>
    ValueTask<JobDisposition<TResult>> RunAsync(TArgs args, JobRun<TResult> run, CancellationToken cancellationToken);
}

/// <summary>
/// What a handler of a definition with a result type returns: a result value, a <see cref="JobResult"/> or
/// <see cref="JobRun.Completed"/>, each of which converts implicitly.
/// </summary>
public sealed class JobDisposition<TResult>
{
    private JobDisposition(JobDisposition disposition, bool hasValue, TResult value)
    {
        Disposition = disposition;
        HasValue = hasValue;
        Value = value;
    }

    /// <summary>Whether the handler returned a result value.</summary>
    public bool HasValue { get; }

    /// <summary>The returned result value, when <see cref="HasValue"/>.</summary>
    public TResult Value { get; }

    /// <summary>The disposition without its value: completed, a <see cref="JobResult"/>, or applied by the handler.</summary>
    public JobDisposition Disposition { get; }

    public static implicit operator JobDisposition<TResult>(TResult value) => new(JobResult.Completed, true, value);

    public static implicit operator JobDisposition<TResult>(JobResult result) => new(JobDisposition.FromResult(result), false, default!);

    public static implicit operator JobDisposition<TResult>(JobDisposition disposition)
    {
        ArgumentNullException.ThrowIfNull(disposition);
        return new(disposition, false, default!);
    }
}

/// <summary>Runs inside every terminal transition of a definition's jobs, whoever causes it. No external calls.</summary>
public interface IJobEnded
{
    ValueTask EndedAsync(JobEnd end, WorkTransaction transaction, CancellationToken cancellationToken);
}

/// <summary>One attempt of a claimed job.</summary>
public partial class JobRun
{
    private readonly JobEndedCallback? ended;

    internal JobRun(LeaseRun<JobId> lease, JobDefinition definition, IWorkStore work, JobRegistry registry, JobEndedCallback? ended)
    {
        Lease = lease;
        Definition = definition;
        Work = work;
        Registry = registry;
        this.ended = ended;
        Claim = lease.Claim.Job ?? throw new InvalidOperationException($"The claim of {lease.Lease.Source.Name} record {lease.Key} is not a job claim.");
    }

    /// <summary>The lease run; its lifecycle belongs to the dispatcher and the job store.</summary>
    internal LeaseRun<JobId> Lease { get; }

    public JobDefinition Definition { get; }

    public JobId Id => Lease.Key;

    public Subject? Subject => Claim.Subject;

    /// <summary>The partition, or null for a job without one.</summary>
    public string? Partition => string.IsNullOrEmpty(Lease.Partition) ? null : Lease.Partition;

    /// <summary>
    /// The occurrence of a recurring job: its scheduled instant in ISO 8601 UTC, <c>trigger:&lt;id&gt;</c> or
    /// <c>request:&lt;job id&gt;</c>; null for a one-shot job.
    /// </summary>
    public string? Occurrence => Claim.Occurrence;

    /// <summary>The attempt of the current execution, from 1.</summary>
    public int Attempt => Claim.Attempt;

    /// <summary>The argument version the job was requested with.</summary>
    public int ArgsVersion => Claim.ArgsVersion;

    /// <summary>Stable across the attempts of one execution; new for a follow-up, an occurrence or a rerun.</summary>
    public Guid ExecutionId => Lease.ExecutionId;

    /// <summary>Unique for this claim.</summary>
    public Guid AttemptId => Lease.AttemptId;

    /// <summary>The holder of this attempt's lease: this process, by its owner id.</summary>
    public OwnerId Owner => Lease.Lease.Owner;

    /// <summary>Cancelled when the lease is lost, cancellation is requested, the attempt times out or the process stops.</summary>
    public CancellationToken CancellationToken => Lease.CancellationToken;

    /// <summary>Why <see cref="CancellationToken"/> was cancelled; null while the attempt may continue.</summary>
    public LeaseStopReason? StopReason => Lease.Hold.StopReason;

    /// <summary>What a handler returns after <see cref="CompleteAsync"/>, so the dispatcher applies nothing more.</summary>
    public JobDisposition Completed => JobDisposition.Completed;

    private JobClaim Claim { get; }

    private protected IJobStore Store => Work.Jobs;

    private IWorkStore Work { get; }

    private JobRegistry Registry { get; }

    /// <summary>The largest progress message <see cref="ReportProgressAsync"/> stores, in characters.</summary>
    public const int MaximumProgressMessage = 256;

    /// <summary>
    /// Writes the attempt's progress: a whole percentage from 0 to 100 and a short message (J19). It is a durable
    /// write fenced to this attempt's lease and cleared when the next attempt starts; report at checkpoints.
    /// </summary>
    /// <exception cref="LeaseLostException">The lease is no longer valid.</exception>
    public async ValueTask ReportProgressAsync(int percent, string? message = null, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(percent);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(percent, 100);
        if (message is { Length: > MaximumProgressMessage })
        {
            throw new ArgumentException($"A progress message has at most {MaximumProgressMessage} characters.", nameof(message));
        }
        if (!await Store.ReportProgressAsync(Lease.Lease, new JobProgress(percent, message), cancellationToken))
        {
            throw new LeaseLostException($"The lease on {Lease.Lease.Source.Name} record {Lease.Key} at epoch {Lease.Lease.Epoch} is no longer valid.");
        }
    }

    /// <inheritdoc cref="LeaseOperations.GuardAsync{TKey}"/>
    public ValueTask GuardAsync(WorkTransaction transaction, CancellationToken cancellationToken = default) =>
        Lease.GuardAsync(transaction, cancellationToken);

    /// <summary>
    /// Applies <paramref name="result"/> in the domain's transaction, with the definition's outcome hook for a terminal
    /// result. If that transaction does not commit, the job stays held and the run can complete again; a run that
    /// returns <see cref="Completed"/> without a committed completion is retried as <c>uncompleted</c>.
    /// </summary>
    /// <exception cref="LeaseLostException">The lease is no longer valid; the transaction must not commit.</exception>
    public ValueTask CompleteAsync(WorkTransaction transaction, JobResult result, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(result);
        return CompleteAsync(transaction, JobTransition.From(result), cancellationToken);
    }

    /// <summary>The transition the handler last applied in its own transaction, whether or not that committed.</summary>
    internal JobTransition? Applied { get; private set; }

    /// <summary>
    /// Applies a transition in the domain's transaction with the definition's outcome hook, as a
    /// <c>corfedo.work.complete</c> span.
    /// </summary>
    private protected async ValueTask CompleteAsync(WorkTransaction transaction, JobTransition transition, CancellationToken cancellationToken)
    {
        using var span = JobSpans.StartComplete(Lease, transition);
        try
        {
            await Store.CompleteAsync(transaction, Lease.Lease, transition, ended, cancellationToken);
        }
        catch (Exception exception)
        {
            WorkTelemetry.Fail(span, exception.GetType().FullName!, transition.Code);
            throw;
        }
        Applied = transition;
    }
}

/// <summary>One attempt of a claimed job of a definition with a result type.</summary>
public sealed class JobRun<TResult> : JobRun
{
    internal JobRun(LeaseRun<JobId> lease, JobDefinition definition, IJobResults<TResult> results, IWorkStore work, JobRegistry registry, JobEndedCallback? ended)
        : base(lease, definition, work, registry, ended) => Results = results;

    private IJobResults<TResult> Results { get; }

    /// <summary>
    /// Completes the job with <paramref name="result"/> stored with its outcome, in the domain's transaction, with the
    /// definition's outcome hook. If that transaction does not commit, the job stays held and the run can complete again.
    /// </summary>
    /// <exception cref="ArgumentException">The serialized result exceeds <see cref="JobResults.MaximumBytes"/>; nothing is written.</exception>
    /// <exception cref="LeaseLostException">The lease is no longer valid; the transaction must not commit.</exception>
    public ValueTask CompleteAsync(WorkTransaction transaction, TResult result, CancellationToken cancellationToken = default) =>
        CompleteAsync(transaction, JobTransition.Complete(Results.Write(result), Results.Version), cancellationToken);
}
