using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>The arguments of conformance jobs.</summary>
public sealed record TestArgs(string Value);

/// <summary>The result of conformance jobs with a result type.</summary>
public sealed record TestResult(string Value);

[JsonSerializable(typeof(TestArgs))]
[JsonSerializable(typeof(TestResult))]
internal sealed partial class ConformanceJson : JsonSerializerContext;

/// <summary>What a scenario's job handler does; one instance per process.</summary>
public sealed class JobScript
{
    public Func<TestArgs, JobRun, CancellationToken, ValueTask<JobDisposition>> Run { get; set; } =
        (_, _, _) => ValueTask.FromResult<JobDisposition>(JobResult.Completed);
}

public sealed class ScriptedJobHandler(JobScript script) : IJobHandler<TestArgs>
{
    public ValueTask<JobDisposition> RunAsync(TestArgs args, JobRun run, CancellationToken cancellationToken) => script.Run(args, run, cancellationToken);
}

/// <summary>What a scenario's handler of a definition with a result type does; one instance per process.</summary>
public sealed class ResultScript
{
    public Func<TestArgs, JobRun<TestResult>, CancellationToken, ValueTask<JobDisposition<TestResult>>> Run { get; set; } =
        (args, _, _) => ValueTask.FromResult<JobDisposition<TestResult>>(new TestResult(args.Value));
}

public sealed class ScriptedResultHandler(ResultScript script) : IJobHandler<TestArgs, TestResult>
{
    public ValueTask<JobDisposition<TestResult>> RunAsync(TestArgs args, JobRun<TestResult> run, CancellationToken cancellationToken) => script.Run(args, run, cancellationToken);
}

/// <summary>What a scenario's outcome hook does: records every call and can fail or write domain data.</summary>
public sealed class EndedScript
{
    public ConcurrentQueue<JobEnd> Calls { get; } = new();

    public Func<JobEnd, IWorkTransaction, CancellationToken, ValueTask> Ended { get; set; } = (_, _, _) => ValueTask.CompletedTask;
}

public sealed class ScriptedJobEnded(EndedScript script) : IJobEnded
{
    public async ValueTask EndedAsync(JobEnd end, IWorkTransaction transaction, CancellationToken cancellationToken)
    {
        script.Calls.Enqueue(end);
        await script.Ended(end, transaction, cancellationToken);
    }
}

/// <summary>One attempt as a scenario observed it.</summary>
public sealed record ObservedAttempt(JobId Job, string Args, int Attempt, Guid ExecutionId, Guid AttemptId, OwnerId Owner, TimeSpan At);

/// <summary>One attempt of an occurrence: when its handler started and returned, by the scenario's clock.</summary>
public sealed record ObservedOccurrence(JobId Job, string? Occurrence, string Args, int Attempt, Guid ExecutionId, TimeSpan Started, TimeSpan Returned)
{
    /// <summary>The scheduled instant of a planned occurrence.</summary>
    public DateTimeOffset Instant => DateTimeOffset.Parse(Occurrence!, System.Globalization.CultureInfo.InvariantCulture);
}

/// <summary>Shared setup for job scenarios. Every scenario defines its own definitions, so jobs never mix.</summary>
public abstract class JobScenarioBase(IJobConformanceProvider provider) : LeaseScenarioBase(provider)
{
    private static int definitions;

    protected new IJobConformanceProvider Provider { get; } = provider;

    protected Stopwatch Clock { get; } = Stopwatch.StartNew();

    /// <summary>A definition with a key unique to this run of the suite.</summary>
    protected static JobDefinition<TestArgs> Definition(JobPolicy? policy = null, string? subject = null, long revision = 1, int argsVersion = 1, string? key = null) =>
        JobDefinition.Create(
            key ?? $"j{Interlocked.Increment(ref definitions)}-{Guid.NewGuid():N}"[..20],
            revision,
            JobArgs.Json(ConformanceJson.Default.TestArgs, argsVersion),
            policy,
            subject);

    /// <summary>A definition with a result type and a key unique to this run of the suite.</summary>
    protected static JobDefinition<TestArgs, TestResult> ResultDefinition(JobPolicy? policy = null, string? subject = null, int resultVersion = 1, string? key = null) =>
        JobDefinition.Create(
            key ?? $"r{Interlocked.Increment(ref definitions)}-{Guid.NewGuid():N}"[..20],
            1,
            JobArgs.Json(ConformanceJson.Default.TestArgs),
            JobResults.Json(ConformanceJson.Default.TestResult, resultVersion),
            policy,
            subject);

    /// <summary>A started process running <paramref name="script"/> for a definition with a result type.</summary>
    protected Task<TestProcess> ResultWorkerAsync(JobDefinition<TestArgs, TestResult> definition, ResultScript script, int capacity = 1) =>
        ProcessAsync(work =>
        {
            work.Services.AddSingleton(script);
            work.AddJob<TestArgs, TestResult, ScriptedResultHandler>(definition, job => job.Lease(TimeSpan.FromSeconds(2)).Capacity(capacity));
        });

    /// <summary>A started process running <paramref name="script"/> for <paramref name="definition"/>, with its outcome hook.</summary>
    protected Task<TestProcess> WorkerAsync(
        JobDefinition<TestArgs> definition,
        JobScript script,
        EndedScript? ended = null,
        Action<JobBuilder>? configure = null,
        Action<WorkBuilder>? also = null,
        TimeSpan? pollingInterval = null,
        TimeSpan? shutdownGrace = null) =>
        ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(script);
                work.Services.AddSingleton(ended ?? new EndedScript());
                work.AddJob<TestArgs, ScriptedJobHandler>(definition, job =>
                {
                    job.Lease(TimeSpan.FromSeconds(2)).OnEnded<ScriptedJobEnded>();
                    configure?.Invoke(job);
                });
                also?.Invoke(work);
            },
            pollingInterval: pollingInterval,
            shutdownGrace: shutdownGrace);

    /// <summary>
    /// A started process that only requests and cancels jobs of the definitions, with their outcome hooks; its startup
    /// applies the framework schema and stores the definitions, and it runs no dispatcher.
    /// </summary>
    protected Task<TestProcess> ClientAsync(EndedScript? ended, params JobDefinition<TestArgs>[] definitions) =>
        ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(ended ?? new EndedScript());
                foreach (var definition in definitions)
                {
                    work.AddJobDefinition(definition, d => d.OnEnded<ScriptedJobEnded>());
                }
            });

    protected static JobOperations Jobs(TestProcess process) => process.Services.GetRequiredService<JobOperations>();

    /// <summary>Requests a job in a transaction of its own, committed.</summary>
    protected async Task<JobRequestResult> RequestAsync(TestProcess process, JobDefinition<TestArgs> definition, string args, Schedule? schedule = null, JobRequestOptions? options = null)
    {
        await using var transaction = await Provider.BeginAsync(Cancel);
        var result = await Jobs(process).RequestAsync(transaction.Work, definition, new TestArgs(args), schedule, options, Cancel);
        await transaction.CommitAsync(Cancel);
        return result;
    }

    /// <summary>Schedules a recurrence in a transaction of its own, committed.</summary>
    protected async Task<RecurrenceOutcome> ScheduleAsync(TestProcess process, JobDefinition<TestArgs> definition, string args, Schedule schedule, RecurrenceOptions options)
    {
        await using var transaction = await Provider.BeginAsync(Cancel);
        var result = await Jobs(process).ScheduleAsync(transaction.Work, definition, new TestArgs(args), schedule, options, Cancel);
        await transaction.CommitAsync(Cancel);
        return result;
    }

    /// <summary>Runs <paramref name="operation"/> in a transaction of its own, committed.</summary>
    protected async Task<T> InTransactionAsync<T>(Func<IWorkTransaction, ValueTask<T>> operation)
    {
        await using var transaction = await Provider.BeginAsync(Cancel);
        var result = await operation(transaction.Work);
        await transaction.CommitAsync(Cancel);
        return result;
    }

    /// <summary>Runs <paramref name="operation"/> in a transaction of its own, committed.</summary>
    protected async Task InTransactionAsync(Func<IWorkTransaction, ValueTask> operation)
    {
        await using var transaction = await Provider.BeginAsync(Cancel);
        await operation(transaction.Work);
        await transaction.CommitAsync(Cancel);
    }

    protected static RecurrenceOptions Recurring(Subject subject, Overlap overlap = Overlap.Wait, Missed missed = Missed.Skip) =>
        new() { Subject = subject, Overlap = overlap, Missed = missed };

    /// <summary>
    /// A script that records every attempt with its occurrence and the times its handler started and returned, after
    /// doing what <paramref name="then"/> returns.
    /// </summary>
    protected JobScript Occurrences(ConcurrentQueue<ObservedOccurrence> runs, Func<TestArgs, JobRun, CancellationToken, ValueTask<JobDisposition>>? then = null) => new()
    {
        Run = async (args, run, cancellationToken) =>
        {
            var started = Clock.Elapsed;
            JobDisposition disposition = JobResult.Completed;
            try
            {
                if (then is not null)
                {
                    disposition = await then(args, run, cancellationToken);
                }
            }
            finally
            {
                runs.Enqueue(new ObservedOccurrence(run.Id, run.Occurrence, args.Value, run.Attempt, run.ExecutionId, started, Clock.Elapsed));
            }
            return disposition;
        },
    };

    /// <summary>Waits until <paramref name="runs"/> holds at least <paramref name="count"/> runs and returns them in order of start.</summary>
    protected static async Task<ObservedOccurrence[]> RunsAsync(ConcurrentQueue<ObservedOccurrence> runs, int count, TimeSpan timeout)
    {
        await EventuallyAsync(() => runs.Count >= count, timeout, $"{count} runs");
        return [.. runs.OrderBy(r => r.Started)];
    }

    protected static JobRequestOptions For(Subject subject, RequestMode mode = RequestMode.Merge, string? partition = null) =>
        new() { Subject = subject, Mode = mode, Partition = partition };

    protected Task<JobState> ReadJobAsync(JobId job) => Provider.ReadJobAsync(job, Cancel);

    protected Task<IReadOnlyList<JobState>> ReadJobsAsync(JobDefinition definition) => Provider.ReadJobsAsync(definition.Key, Cancel);

    /// <summary>Waits until the job is terminal and returns it.</summary>
    protected async Task<JobState> EndedAsync(JobId job, TimeSpan timeout)
    {
        JobState? state = null;
        await EventuallyAsync(async () => (state = await ReadJobAsync(job)).State is not null, timeout, $"job {job} ends");
        return state!;
    }

    /// <summary>
    /// Claims one due job of the definition as a holder that then crashes: the claim uses an attempt and its lease of
    /// <paramref name="lease"/> is never renewed or given back.
    /// </summary>
    protected async Task<LeaseClaim<JobId>> CrashClaimAsync(TestProcess process, JobDefinition definition, TimeSpan lease, TimeSpan timeout)
    {
        var (store, source) = await JobSourceAsync(process, definition, lease);
        LeaseClaim<JobId>? claim = null;
        await EventuallyAsync(async () => (claim = (await store.Leases.ClaimDueAsync(source, LeaseCallbacks<JobId>.None, process.Owner, 1, Cancel)).Claims.SingleOrDefault()) is not null, timeout, $"a job of {definition.Key} is claimable");
        return claim!;
    }

    /// <summary>The definition's stored job source with leases of <paramref name="lease"/>, for claims through the store.</summary>
    protected async Task<(IWorkStore Store, LeaseSource<JobId> Source)> JobSourceAsync(TestProcess process, JobDefinition definition, TimeSpan lease)
    {
        var store = process.Services.GetRequiredService<IWorkStore>();
        var stored = await store.Jobs.GetDefinitionAsync(definition.Key, Cancel) ?? throw new InvalidOperationException($"{definition.Key} is not stored.");
        return (store, JobSource.Define(store.Jobs.Table, stored, [1], lease));
    }

    /// <summary>A script that records every attempt and then does what <paramref name="then"/> returns.</summary>
    protected JobScript Recording(ConcurrentQueue<ObservedAttempt> attempts, Func<TestArgs, JobRun, CancellationToken, ValueTask<JobDisposition>>? then = null) => new()
    {
        Run = (args, run, cancellationToken) =>
        {
            attempts.Enqueue(new ObservedAttempt(run.Id, args.Value, run.Attempt, run.ExecutionId, run.AttemptId, run.Owner, Clock.Elapsed));
            return then?.Invoke(args, run, cancellationToken) ?? ValueTask.FromResult<JobDisposition>(JobResult.Completed);
        },
    };

    /// <summary>The provider's controlled clock; skips a scenario on a store whose clock follows real time.</summary>
    protected IClockConformanceProvider RequireControlledClock()
    {
        if (Provider is not IClockConformanceProvider clock)
        {
            Assert.Skip("The store's clock follows real time; the scenario advances a controlled clock.");
            throw new InvalidOperationException();
        }
        return clock;
    }

    /// <summary>The store's clock, and a fixed-offset time zone in which it is between 12:00 and 13:00.</summary>
    protected async Task<CalendarClock> ClockAsync()
    {
        var now = await Provider.NowAsync(Cancel);
        var hours = 12 - now.UtcDateTime.Hour;
        // Etc/GMT zones count the other way round: Etc/GMT-3 is three hours ahead of UTC.
        var zone = hours == 0 ? "Etc/GMT" : hours > 0 ? $"Etc/GMT-{hours}" : $"Etc/GMT+{-hours}";
        return new CalendarClock(now, zone, TimeSpan.FromHours(hours));
    }

    /// <summary>The store's clock at the start of a scenario in a fixed-offset time zone, with its offset.</summary>
    protected sealed record CalendarClock(DateTimeOffset Start, string Zone, TimeSpan Offset)
    {
        public DateOnly Today => DateOnly.FromDateTime(Start.UtcDateTime + Offset);

        /// <summary>The local time of day <paramref name="after"/> the start.</summary>
        public TimeOnly Local(TimeSpan after) => TimeOnly.FromDateTime((Start + after).UtcDateTime + Offset);

        /// <summary>The first whole second at least <paramref name="after"/> the start.</summary>
        public DateTimeOffset WholeSecondAfter(TimeSpan after)
        {
            var ticks = (Start + after).UtcTicks;
            return new DateTimeOffset(ticks + ((TimeSpan.TicksPerSecond - (ticks % TimeSpan.TicksPerSecond)) % TimeSpan.TicksPerSecond), TimeSpan.Zero);
        }
    }
}
