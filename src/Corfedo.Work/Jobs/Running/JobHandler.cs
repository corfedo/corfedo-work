using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Corfedo.Work;

/// <summary>
/// Runs a job handler as the lease handler of its definition's source and applies the attempt's outcome. A claim that
/// found the job late, out of attempts or with a cancellation request ends it without a handler. A result returned by
/// the handler is applied in a guarded transaction of its own; a thrown exception is <c>Retry(0, "handler_failed")</c>,
/// and a timeout is applied as a timed-out attempt. The store ends a retry of a job with a cancellation request as
/// cancelled. A run that returns <see cref="JobRun.Completed"/> without a committed completion is retried as
/// <c>uncompleted</c>. A transition whose outcome hook fails is retried while the lease is held. During shutdown an
/// exception is left to the dispatcher, which gives the job back and returns its attempt. Each attempt records its
/// duration and outcome, on its run span too, and a claim that runs a job when due, not by takeover, its lateness. An
/// attempt is tracked by its definition's running-limit timers until its outcome is applied. An attempt that ends
/// failed, timed out or retried, applied by the dispatcher or by the handler in its own transaction, is logged as
/// <c>job.failed</c> with its reason code, unless a thrown exception or a missing completion was logged already.
/// </summary>
internal abstract partial class JobHandler<TArgs>(JobRegistry registry, IWorkStore store, ILoggerFactory? logging) : ILeaseHandler<JobId>
{
    private static readonly JobTransition HandlerFailed = JobTransition.From(JobResult.Retry(TimeSpan.Zero, JobReasons.HandlerFailed));
    private static readonly JobTransition ResultVersion = JobTransition.From(JobResult.Retry(TimeSpan.Zero, JobReasons.ResultVersion));
    private static readonly JobTransition Uncompleted = JobTransition.From(JobResult.Retry(TimeSpan.Zero, JobReasons.Uncompleted));

    private readonly ILogger? logger = logging?.CreateLogger("Corfedo.Work.Jobs");
    private readonly WorkTelemetryOptions telemetry = registry.Telemetry;

    public async ValueTask RunAsync(LeaseRun<JobId> run, CancellationToken cancellationToken)
    {
        var registration = (JobDefinitionRegistration<TArgs>)registry.Find(DefinitionKey(run))!;
        var ended = registry.Ended(registration);
        var claim = run.Claim.Job!;
        if (claim is { Kind: JobClaimKind.Run, DueAt: { } due } && run.Claim.Reason != ClaimReason.Takeover)
        {
            var lateness = registry.Time.GetUtcNow() - due;
            WorkTelemetry.JobLateness.Record(Math.Max(lateness.TotalSeconds, 0), Tags(run));
        }
        using var limit = claim.Kind == JobClaimKind.Run ? registration.RunningLimits.Track(run, store, registry.Time, logger, telemetry) : null;
        var (transition, own) = claim.Kind switch
        {
            JobClaimKind.Late => (JobTransition.Late, null),
            JobClaimKind.Exhausted => (JobTransition.Exhausted, null),
            JobClaimKind.Cancelled => (JobTransition.Cancel, null),
            _ => await RunHandlerAsync(run, registration, ended, cancellationToken),
        };
        var applied = await ApplyAsync(run, transition, ended);
        // Uncompleted changes nothing once the handler's own completion committed and ended the lease.
        var outcome = applied ? transition : ReferenceEquals(transition, Uncompleted) && !run.Hold.Lost ? own : null;
        if (outcome is { Kind: JobTransitionKind.Fail or JobTransitionKind.TimeOut or JobTransitionKind.Retry } && !Logged(outcome) && logger is not null)
        {
            LogFailedResult(logger, DefinitionKey(run), run.Key.ToString(), WorkTelemetry.Value(outcome.Kind), outcome.Code!);
        }
        run.TryFinish();
    }

    /// <summary>Whether the transition has a log event of its own: a handler that threw, or a missing completion.</summary>
    private static bool Logged(JobTransition transition) =>
        ReferenceEquals(transition, HandlerFailed) || ReferenceEquals(transition, ResultVersion) || ReferenceEquals(transition, Uncompleted);

    /// <summary>
    /// The attempt's transition for the dispatcher to apply, and the one the handler applied in its own transaction, if
    /// any; an exception during shutdown is left to the dispatcher.
    /// </summary>
    private async Task<(JobTransition Transition, JobTransition? Own)> RunHandlerAsync(LeaseRun<JobId> run, JobDefinitionRegistration<TArgs> registration, JobEndedCallback ended, CancellationToken cancellationToken)
    {
        using var timeoutReport = run.Hold.Token.Register(() => ReportIgnoredTimeout(run));
        var started = registry.Time.GetTimestamp();
        var job = CreateRun(run, registration, ended);
        JobTransition transition;
        string? errorType = null;
        try
        {
            var args = registration.Args.Read(run.Claim.Job!.Args, run.Claim.Job.ArgsVersion);
            var returned = await CallAsync(args, job, registration, cancellationToken);
            // A completion that committed ended the lease, so applying Uncompleted changes nothing; otherwise it is
            // retried. A timed-out attempt ends as timed out whatever it returns. The store ends a retry of a job with a
            // cancellation request as cancelled.
            transition = returned is null ? Uncompleted : run.Hold.StopReason == LeaseStopReason.Timeout ? JobTransition.TimeOut : returned;
        }
        catch (Exception exception) when (run.Hold.StopReason != LeaseStopReason.Shutdown)
        {
            switch (run.Hold.StopReason)
            {
                case LeaseStopReason.Timeout:
                    transition = JobTransition.TimeOut;
                    break;
                case null when logger is not null:
                    LogFailed(logger, telemetry.Logged(exception), registration.Definition.Key, run.Key.ToString(), telemetry.Describe(exception));
                    goto default;
                default:
                    // A retry of a job with a cancellation request ends it cancelled. A result newer than the handler's
                    // codec reads was written by newer code, so the attempt fails with its own reason.
                    transition = exception is JobResultVersionException ? ResultVersion : HandlerFailed;
                    errorType = exception.GetType().FullName;
                    break;
            }
        }
        // The duration and the span record what the attempt asked for: the transition it applied itself, or the one
        // returned for the dispatcher to apply.
        var outcome = ReferenceEquals(transition, Uncompleted) && job.Applied is { } applied ? applied : transition;
        WorkTelemetry.JobDuration.Record(registry.Time.GetElapsedTime(started).TotalSeconds, Tags(run, outcome));
        Record(run.Span, outcome, errorType);
        return (transition, job.Applied);
    }

    /// <summary>The attempt as its handler sees it.</summary>
    private protected abstract JobRun CreateRun(LeaseRun<JobId> run, JobDefinitionRegistration<TArgs> registration, JobEndedCallback ended);

    /// <summary>Runs the handler; returns the transition it asks for, or null when it applied its own.</summary>
    private protected abstract ValueTask<JobTransition?> CallAsync(TArgs args, JobRun run, JobDefinitionRegistration<TArgs> registration, CancellationToken cancellationToken);

    private protected JobRegistry Registry => registry;

    private protected IWorkStore Store => store;

    /// <summary>
    /// Applies the transition in the store's own fenced transaction, as a <c>corfedo.work.complete</c> span. A failure,
    /// such as an outcome hook that throws, rolls it back; it is retried with a growing delay while the lease is held and
    /// the process is not stopping. Returns whether it changed the job, which it does not once the lease ended.
    /// </summary>
    private async Task<bool> ApplyAsync(LeaseRun<JobId> run, JobTransition transition, JobEndedCallback ended)
    {
        var delay = TimeSpan.FromMilliseconds(100);
        while (true)
        {
            using var span = JobSpans.StartComplete(run, transition);
            try
            {
                var applied = await store.Jobs.CompleteAsync(run.Lease, transition, ended, CancellationToken.None);
                if (applied && ReferenceEquals(transition, Uncompleted) && logger is not null)
                {
                    LogUncompleted(logger, DefinitionKey(run), run.Key.ToString());
                }
                return applied;
            }
            catch (Exception exception) when (!run.Hold.Lost && run.Hold.StopReason != LeaseStopReason.Shutdown)
            {
                WorkTelemetry.Fail(span, exception.GetType().FullName!, transition.Code);
                span?.Dispose();
                if (logger is not null)
                {
                    LogTransitionFailed(logger, telemetry.Logged(exception), DefinitionKey(run), run.Key.ToString(), telemetry.Describe(exception));
                }
                await Task.Delay(delay, run.Hold.Token).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, TimeSpan.FromSeconds(30).Ticks));
            }
            catch (Exception exception)
            {
                WorkTelemetry.Fail(span, exception.GetType().FullName!, transition.Code);
                throw;
            }
        }
    }

    /// <summary>
    /// Records the attempt's outcome and reason on its run span; an attempt that failed, timed out or is retried marks
    /// it failed, with the exception's type when the handler threw and the reason code otherwise.
    /// </summary>
    private static void Record(Activity? span, JobTransition transition, string? errorType)
    {
        if (span is null)
        {
            return;
        }
        span.SetTag(WorkTelemetry.OutcomeTag, WorkTelemetry.Value(transition.Kind));
        if (transition.Kind is JobTransitionKind.Fail or JobTransitionKind.TimeOut or JobTransitionKind.Retry)
        {
            WorkTelemetry.Fail(span, errorType ?? transition.Code ?? WorkTelemetry.Value(transition.Kind), transition.Code);
        }
        else
        {
            span.SetTag(WorkTelemetry.ReasonTag, transition.Code);
        }
    }

    /// <summary>The definition, the attempt's outcome when given, and the partition when the host adds it.</summary>
    private TagList Tags(LeaseRun<JobId> run, JobTransition? outcome = null)
    {
        var tags = new TagList { { WorkTelemetry.DefinitionTag, DefinitionKey(run) } };
        if (outcome is not null)
        {
            tags.Add(WorkTelemetry.OutcomeTag, WorkTelemetry.Value(outcome.Kind));
        }
        if (registry.Telemetry.PartitionOf(run.Partition) is { } partition)
        {
            tags.Add(WorkTelemetry.PartitionTag, partition);
        }
        return tags;
    }

    private void ReportIgnoredTimeout(LeaseRun<JobId> run)
    {
        if (run.Hold.StopReason != LeaseStopReason.Timeout || logger is null)
        {
            return;
        }
        // A handler still running one lease duration after its timeout is reported; the lease is kept until it returns.
        _ = Task.Delay(run.Lease.Source.Policy.Duration).ContinueWith(
            _ =>
            {
                if (!run.Finished)
                {
                    LogTimeoutIgnored(logger, DefinitionKey(run), run.Key.ToString());
                }
            },
            TaskScheduler.Default);
    }

    public ValueTask ExhaustedAsync(LeaseRun<JobId> run, CancellationToken cancellationToken) =>
        throw new InvalidOperationException("Job sources have no maximum takeovers; their attempts end them.");

    private static string DefinitionKey(LeaseRun<JobId> run) => run.Lease.Source.Job!.Definition;

    [LoggerMessage(EventId = 30, EventName = "job.handler_failed", Level = LogLevel.Error, Message = "Handler of job {Definition} {Job} failed; retried: {Error}")]
    private static partial void LogFailed(ILogger logger, Exception? exception, string definition, string job, string error);

    [LoggerMessage(EventId = 31, EventName = "job.uncompleted", Level = LogLevel.Warning, Message = "Handler of job {Definition} {Job} returned Completed without a committed completion; retried")]
    private static partial void LogUncompleted(ILogger logger, string definition, string job);

    [LoggerMessage(EventId = 32, EventName = "job.transition_failed", Level = LogLevel.Warning, Message = "Applying the outcome of job {Definition} {Job} failed; retried while its lease is held: {Error}")]
    private static partial void LogTransitionFailed(ILogger logger, Exception? exception, string definition, string job, string error);

    [LoggerMessage(EventId = 35, EventName = "job.failed", Level = LogLevel.Warning, Message = "Attempt of job {Definition} {Job} ended {Outcome} with {Code}")]
    private static partial void LogFailedResult(ILogger logger, string definition, string job, string outcome, string code);

    [LoggerMessage(EventId = 33, EventName = "job.timeout_ignored", Level = LogLevel.Warning, Message = "Handler of job {Definition} {Job} has not returned since its attempt timed out; the job keeps its lease until it returns")]
    private static partial void LogTimeoutIgnored(ILogger logger, string definition, string job);
}

/// <summary>Runs a handler of a definition without a result type.</summary>
internal sealed class JobHandler<TArgs, THandler>(THandler handler, JobRegistry registry, IWorkStore store, ILoggerFactory? logging = null) : JobHandler<TArgs>(registry, store, logging)
    where THandler : class, IJobHandler<TArgs>
{
    private protected override JobRun CreateRun(LeaseRun<JobId> run, JobDefinitionRegistration<TArgs> registration, JobEndedCallback ended) =>
        new(run, registration.Definition, Store, Registry, ended);

    private protected override async ValueTask<JobTransition?> CallAsync(TArgs args, JobRun run, JobDefinitionRegistration<TArgs> registration, CancellationToken cancellationToken)
    {
        var disposition = await handler.RunAsync(args, run, cancellationToken);
        return disposition.Result is { } result ? JobTransition.From(result) : null;
    }
}

/// <summary>
/// Runs a handler of a definition with a result type. A returned value is serialized with the definition's codec and
/// stored with the outcome; a value larger than <see cref="JobResults.MaximumBytes"/> ends the job failed with
/// <c>result_too_large</c>, since another attempt would not change it.
/// </summary>
internal sealed class JobHandler<TArgs, TResult, THandler>(THandler handler, JobRegistry registry, IWorkStore store, ILoggerFactory? logging = null) : JobHandler<TArgs>(registry, store, logging)
    where THandler : class, IJobHandler<TArgs, TResult>
{
    private static readonly JobTransition TooLarge = JobTransition.From(JobResult.Fail(JobReasons.ResultTooLarge));

    private protected override JobRun CreateRun(LeaseRun<JobId> run, JobDefinitionRegistration<TArgs> registration, JobEndedCallback ended) =>
        new JobRun<TResult>(run, registration.Definition, (IJobResults<TResult>)registration.Results!, Store, Registry, ended);

    private protected override async ValueTask<JobTransition?> CallAsync(TArgs args, JobRun run, JobDefinitionRegistration<TArgs> registration, CancellationToken cancellationToken)
    {
        var results = (IJobResults<TResult>)registration.Results!;
        var disposition = await handler.RunAsync(args, (JobRun<TResult>)run, cancellationToken);
        if (!disposition.HasValue)
        {
            return disposition.Disposition.Result is { } result ? JobTransition.From(result) : null;
        }
        var data = results.Write(disposition.Value);
        return data.Length > JobResults.MaximumBytes ? TooLarge : JobTransition.Complete(data, results.Version);
    }
}
