using System.Collections.Concurrent;
using System.Diagnostics;

namespace Corfedo.Work.Conformance;

/// <summary>Results stored with the outcome, outcome reads and waiting for a job to end.</summary>
public abstract class JobResultScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    [Fact]
    public async Task J20_AReturnedResultIsStoredWithTheOutcome()
    {
        var definition = ResultDefinition();
        var executions = new ConcurrentQueue<Guid>();
        var worker = await ResultWorkerAsync(definition, new ResultScript
        {
            Run = (args, run, _) =>
            {
                executions.Enqueue(run.ExecutionId);
                return ValueTask.FromResult<JobDisposition<TestResult>>(new TestResult("sum of " + args.Value));
            },
        });
        var job = await RequestAsync(worker, definition, "x");

        var outcome = await Jobs(worker).WaitAsync(definition, job.Id, TimeSpan.FromSeconds(10), Cancel);
        Assert.NotNull(outcome);
        Assert.Equal((true, JobOutcome.Completed, (string?)null, true), (outcome.Ended, outcome.Outcome, outcome.Reason, outcome.HasResult));
        Assert.Equal(new TestResult("sum of x"), outcome.Result);
        Assert.Equal(Assert.Single(executions), outcome.ExecutionId);
        Assert.NotNull(outcome.EndedAt);
        Assert.Equal(outcome, await Jobs(worker).GetOutcomeAsync(definition, job.Id, Cancel));
        var untyped = await Jobs(worker).GetOutcomeAsync((JobDefinition)definition, job.Id, Cancel);
        Assert.Equal((true, JobOutcome.Completed), (untyped!.Ended, untyped.Outcome));
    }

    [Fact]
    public async Task J20_ACompletionInTheDomainTransactionStoresItsResult()
    {
        var definition = ResultDefinition();
        var journal = await Provider.CreateTableAsync([""], Cancel);
        var worker = await ResultWorkerAsync(definition, new ResultScript
        {
            Run = async (args, run, cancellationToken) =>
            {
                await using var transaction = await Provider.BeginAsync(cancellationToken);
                await Provider.JournalAsync(transaction.Work, journal, "domain output", cancellationToken);
                await run.CompleteAsync(transaction.Work, new TestResult("in " + args.Value), cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                return run.Completed;
            },
        });
        var job = await RequestAsync(worker, definition, "domain");

        var outcome = await Jobs(worker).WaitAsync(definition, job.Id, TimeSpan.FromSeconds(10), Cancel);
        Assert.Equal((true, JobOutcome.Completed, true, new TestResult("in domain")), (outcome!.Ended, outcome.Outcome, outcome.HasResult, outcome.Result));
        Assert.Equal(["domain output"], await Provider.ReadJournalAsync(journal, Cancel));
    }

    [Fact]
    public async Task J20_AResultOverTheLimitEndsTheJobFailedAndIsRefusedInATransaction()
    {
        var definition = ResultDefinition(new JobPolicy { Attempts = 3, Backoff = Backoff.None });
        var large = new string('x', JobResults.MaximumBytes);
        var refused = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;
        var worker = await ResultWorkerAsync(definition, new ResultScript
        {
            Run = async (args, run, cancellationToken) =>
            {
                Interlocked.Increment(ref attempts);
                if (args.Value == "in transaction")
                {
                    await using var transaction = await Provider.BeginAsync(cancellationToken);
                    try
                    {
                        await run.CompleteAsync(transaction.Work, new TestResult(large), cancellationToken);
                        refused.TrySetResult(null);
                    }
                    catch (ArgumentException exception)
                    {
                        refused.TrySetResult(exception);
                    }
                    return JobResult.Fail("refused");
                }
                return new TestResult(large);
            },
        });
        var returned = await RequestAsync(worker, definition, "returned");

        var outcome = await Jobs(worker).WaitAsync(definition, returned.Id, TimeSpan.FromSeconds(10), Cancel);
        Assert.Equal((true, JobOutcome.Failed, JobReasons.ResultTooLarge, false), (outcome!.Ended, outcome.Outcome, outcome.Reason, outcome.HasResult));
        Assert.Null(outcome.Result);
        Assert.Equal(1, Volatile.Read(ref attempts));

        var inTransaction = await RequestAsync(worker, definition, "in transaction");
        Assert.IsType<ArgumentException>(await refused.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel));
        var failed = await Jobs(worker).WaitAsync(definition, inTransaction.Id, TimeSpan.FromSeconds(10), Cancel);
        Assert.Equal((JobOutcome.Failed, "refused", false), (failed!.Outcome, failed.Reason, failed.HasResult));
    }

    [Fact]
    public async Task J20_ATypedHandlerCompletingOrFailingWithoutAValueStoresNoResult()
    {
        var definition = ResultDefinition();
        var worker = await ResultWorkerAsync(definition, new ResultScript
        {
            Run = (args, _, _) => ValueTask.FromResult<JobDisposition<TestResult>>(args.Value == "fail" ? JobResult.Fail("broken") : JobResult.Completed),
        });
        var completed = await RequestAsync(worker, definition, "complete");
        var failed = await RequestAsync(worker, definition, "fail");

        var first = await Jobs(worker).WaitAsync(definition, completed.Id, TimeSpan.FromSeconds(10), Cancel);
        Assert.Equal((JobOutcome.Completed, false), (first!.Outcome, first.HasResult));
        var second = await Jobs(worker).WaitAsync(definition, failed.Id, TimeSpan.FromSeconds(10), Cancel);
        Assert.Equal((JobOutcome.Failed, "broken", false), (second!.Outcome, second.Reason, second.HasResult));
        Assert.Null(await Jobs(worker).GetOutcomeAsync(definition, JobId.New(), Cancel));
        Assert.Null(await Jobs(worker).WaitAsync(definition, JobId.New(), TimeSpan.FromSeconds(5), Cancel));
    }

    [Fact]
    public async Task J29_WaitingReturnsWhenTheJobEndsAndAtItsTimeout()
    {
        var definition = ResultDefinition();
        var releases = new ConcurrentDictionary<string, TaskCompletionSource>();
        TaskCompletionSource Release(string args) => releases.GetOrAdd(args, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
        var started = new ConcurrentQueue<string>();
        await ResultWorkerAsync(definition, new ResultScript
        {
            Run = async (args, _, cancellationToken) =>
            {
                started.Enqueue(args.Value);
                await Release(args.Value).Task.WaitAsync(cancellationToken);
                return new TestResult(args.Value);
            },
        });
        // A caller in another process, which runs no dispatcher and so no signal listener of a host.
        var caller = await ClientAsync(null, definition);
        var job = await RequestAsync(caller, definition, "0");
        await EventuallyAsync(() => started.Contains("0"), TimeSpan.FromSeconds(10), "the job runs");

        var clock = Stopwatch.StartNew();
        var timedOut = await Jobs(caller).WaitAsync(definition, job.Id, TimeSpan.FromMilliseconds(300), Cancel);
        Assert.Equal((false, (JobOutcome?)null, false), (timedOut!.Ended, timedOut.Outcome, timedOut.HasResult));
        Assert.InRange(clock.Elapsed, TimeSpan.FromMilliseconds(300), TimeSpan.FromSeconds(3));

        // Each wait is woken by the store's signal, well before the one-second fallback reread; five rounds keep a
        // fallback that happens to fall soon after the end from passing for a signal.
        for (var round = 0; round < 5; round++)
        {
            var args = round.ToString(System.Globalization.CultureInfo.InvariantCulture);
            var current = round == 0 ? job : await RequestAsync(caller, definition, args);
            await EventuallyAsync(() => started.Contains(args), TimeSpan.FromSeconds(10), $"job {args} runs");
            var waiting = Jobs(caller).WaitAsync(definition, current.Id, TimeSpan.FromSeconds(30), Cancel).AsTask();
            await Task.Delay(1200, Cancel);
            Assert.False(waiting.IsCompleted);
            clock.Restart();
            Release(args).SetResult();
            var outcome = await waiting.WaitAsync(TimeSpan.FromSeconds(10), Cancel);
            Assert.InRange(clock.Elapsed, TimeSpan.Zero, TimeSpan.FromMilliseconds(500));
            Assert.Equal((true, JobOutcome.Completed, new TestResult(args)), (outcome!.Ended, outcome.Outcome, outcome.Result));
        }
    }

    [Fact]
    public async Task J29_WaitingCoversAFollowUpThatReusesTheJobId()
    {
        var definition = ResultDefinition(subject: "feed");
        var subject = Subject.Of("feed", "npm");
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var executions = new ConcurrentQueue<(string Args, Guid Execution)>();
        var worker = await ResultWorkerAsync(definition, new ResultScript
        {
            Run = async (args, run, cancellationToken) =>
            {
                executions.Enqueue((args.Value, run.ExecutionId));
                if (args.Value == "first")
                {
                    await release.Task.WaitAsync(cancellationToken);
                }
                return new TestResult(args.Value);
            },
        });
        var job = await RequestAsync(worker, definition, "first", options: For(subject));
        await EventuallyAsync(() => !executions.IsEmpty, TimeSpan.FromSeconds(10), "the first execution runs");
        Assert.Equal(new JobRequestResult(job.Id, JobRequestOutcome.FollowUp), await RequestAsync(worker, definition, "second", options: For(subject)));

        var waiting = Jobs(worker).WaitAsync(definition, job.Id, TimeSpan.FromSeconds(20), Cancel).AsTask();
        release.SetResult();
        var outcome = await waiting.WaitAsync(TimeSpan.FromSeconds(20), Cancel);
        Assert.Equal((true, JobOutcome.Completed, new TestResult("second")), (outcome!.Ended, outcome.Outcome, outcome.Result));
        // The mark survived the first execution's end and signalled the follow-up's.
        Assert.InRange(await LagAsync(outcome), TimeSpan.Zero, SignalLag);
        var runs = executions.ToArray();
        Assert.Equal(["first", "second"], runs.Select(r => r.Args));
        Assert.Equal(runs[1].Execution, outcome.ExecutionId);
        Assert.NotEqual(runs[0].Execution, outcome.ExecutionId);
    }

    [Fact]
    public async Task J29_WaitsRegisteringWhileJobsCompleteAreWokenBySignals()
    {
        var definition = ResultDefinition();
        await ResultWorkerAsync(definition, new ResultScript
        {
            Run = async (args, _, cancellationToken) =>
            {
                await Task.Delay(int.Parse(args.Value, System.Globalization.CultureInfo.InvariantCulture) % 30, cancellationToken);
                return new TestResult(args.Value);
            },
        }, capacity: 8);
        var caller = await ClientAsync(null, definition);

        var waits = await Task.WhenAll(Enumerable.Range(0, 40).Select(async i =>
        {
            var job = await RequestAsync(caller, definition, (i * 7).ToString(System.Globalization.CultureInfo.InvariantCulture));
            var outcome = await Jobs(caller).WaitAsync(definition, job.Id, TimeSpan.FromSeconds(30), Cancel);
            return (Outcome: outcome!, Lag: await LagAsync(outcome!));
        }));

        Assert.All(waits, w =>
        {
            Assert.Equal((true, JobOutcome.Completed), (w.Outcome.Ended, w.Outcome.Outcome));
            Assert.InRange(w.Lag, TimeSpan.Zero, SignalLag);
        });
    }

    [Fact]
    public async Task J29_WaitersShareTheMarkAndOneTimingOutLeavesTheOthers()
    {
        var definition = ResultDefinition();
        var releases = new ConcurrentDictionary<string, TaskCompletionSource>();
        TaskCompletionSource Release(string args) => releases.GetOrAdd(args, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
        var started = new ConcurrentQueue<string>();
        await ResultWorkerAsync(definition, new ResultScript
        {
            Run = async (args, _, cancellationToken) =>
            {
                started.Enqueue(args.Value);
                await Release(args.Value).Task.WaitAsync(cancellationToken);
                return new TestResult(args.Value);
            },
        });
        var first = await ClientAsync(null, definition);
        var second = await ClientAsync(null, definition);

        // Three rounds, each with two waiters in two processes, so a fallback reread cannot pass for a signal.
        for (var round = 0; round < 3; round++)
        {
            var args = "shared" + round.ToString(System.Globalization.CultureInfo.InvariantCulture);
            var job = await RequestAsync(first, definition, args);
            await EventuallyAsync(() => started.Contains(args), TimeSpan.FromSeconds(10), $"job {args} runs");
            using var cancelled = CancellationTokenSource.CreateLinkedTokenSource(Cancel);
            var cancelledWait = Jobs(second).WaitAsync(definition, job.Id, TimeSpan.FromSeconds(30), cancelled.Token).AsTask();
            var long1 = Jobs(first).WaitAsync(definition, job.Id, TimeSpan.FromSeconds(30), Cancel).AsTask();
            var long2 = Jobs(second).WaitAsync(definition, job.Id, TimeSpan.FromSeconds(30), Cancel).AsTask();
            var timedOut = await Jobs(first).WaitAsync(definition, job.Id, TimeSpan.FromMilliseconds(300), Cancel);
            Assert.False(timedOut!.Ended);
            await cancelled.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cancelledWait);
            await Task.Delay(900 + (round * 300), Cancel);
            Assert.False(long1.IsCompleted || long2.IsCompleted);

            Release(args).SetResult();
            foreach (var outcome in await Task.WhenAll(long1, long2))
            {
                Assert.Equal((true, new TestResult(args)), (outcome!.Ended, outcome.Result));
                Assert.InRange(await LagAsync(outcome), TimeSpan.Zero, SignalLag);
            }
        }
    }

    [Fact]
    public async Task J29_TheMarkSurvivesARetry()
    {
        var definition = ResultDefinition(new JobPolicy { Attempts = 2, Backoff = Backoff.None });
        var attempts = new ConcurrentDictionary<string, int>();
        await ResultWorkerAsync(definition, new ResultScript
        {
            Run = async (args, run, cancellationToken) =>
            {
                attempts.AddOrUpdate(args.Value, 1, (_, n) => n + 1);
                if (run.Attempt == 1)
                {
                    await Task.Delay(300, cancellationToken);
                    // Staggered retries, so the jobs end at different points of the waiter's fallback rereads.
                    return JobResult.Retry(TimeSpan.FromMilliseconds(1500 + (int.Parse(args.Value, System.Globalization.CultureInfo.InvariantCulture) * 270)), "busy");
                }
                return new TestResult(args.Value);
            },
        }, capacity: 4);
        var caller = await ClientAsync(null, definition);

        var outcomes = await Task.WhenAll(Enumerable.Range(0, 4).Select(async i =>
        {
            var job = await RequestAsync(caller, definition, i.ToString(System.Globalization.CultureInfo.InvariantCulture));
            var outcome = await Jobs(caller).WaitAsync(definition, job.Id, TimeSpan.FromSeconds(30), Cancel);
            return (Outcome: outcome!, Lag: await LagAsync(outcome!));
        }));

        Assert.All(outcomes, o =>
        {
            Assert.Equal((true, JobOutcome.Completed, 2), (o.Outcome.Ended, o.Outcome.Outcome, attempts[o.Outcome.Result!.Value]));
            Assert.InRange(o.Lag, TimeSpan.Zero, SignalLag);
        });
    }

    [Fact]
    public async Task J29_OnlyJobsSomeoneWaitsForSignalTheirEnd()
    {
        var definition = ResultDefinition();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var worker = await ResultWorkerAsync(definition, new ResultScript
        {
            Run = async (args, _, cancellationToken) =>
            {
                if (args.Value == "waited")
                {
                    await gate.Task.WaitAsync(cancellationToken);
                }
                return new TestResult(args.Value);
            },
        });
        var signals = new ConcurrentQueue<(SignalTopic Topic, string Key)>();
        var connected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var listening = CancellationTokenSource.CreateLinkedTokenSource(Cancel);
        var listener = Provider.CreateStore().Signals!.ListenAsync((topic, key) => signals.Enqueue((topic, key)), () => connected.TrySetResult(), listening.Token);
        await connected.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);

        var unwaited = await RequestAsync(worker, definition, "unwaited");
        await EndedAsync(unwaited.Id, TimeSpan.FromSeconds(10));
        await Task.Delay(300, Cancel);
        Assert.DoesNotContain(signals, s => s.Topic == WorkTopics.Ended && s.Key == definition.SourceName);

        // The job ends only once the wait has marked it; a job ending before the mark is not signalled.
        var waited = await RequestAsync(worker, definition, "waited");
        var waiting = Jobs(worker).WaitAsync(definition, waited.Id, TimeSpan.FromSeconds(10), Cancel).AsTask();
        await EventuallyAsync(async () => (await Provider.ReadJobAsync(waited.Id, Cancel)).Waited, TimeSpan.FromSeconds(5), "the wait marks the job");
        gate.SetResult();
        Assert.True((await waiting)!.Ended);
        await EventuallyAsync(() => signals.Any(s => s.Topic == WorkTopics.Ended && s.Key == definition.SourceName), TimeSpan.FromSeconds(5), "the waited job signals");
        await listening.CancelAsync();
        await listener;
    }

    /// <summary>
    /// The longest time between a job's end and its waiter's return when a signal wakes it, well below the fallback
    /// reread of one second.
    /// </summary>
    private static readonly TimeSpan SignalLag = TimeSpan.FromMilliseconds(400);

    /// <summary>How long after the job's end, by the store's clock, its waiter returned.</summary>
    private async Task<TimeSpan> LagAsync(JobOutcomeStatus outcome) => await Provider.NowAsync(Cancel) - outcome.EndedAt!.Value;
}
