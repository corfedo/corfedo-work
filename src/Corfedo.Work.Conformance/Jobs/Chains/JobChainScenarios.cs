using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>Chains: steps, run conditions, groups, results between steps, recurring chains, retry, deletion and retention.</summary>
public abstract class JobChainScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    private static readonly TimeSpan Settles = TimeSpan.FromSeconds(20);

    [Fact]
    public async Task K1_StepsRunInOrderAndConditionsDecideTheRestAtomically()
    {
        var step = Definition();
        var runs = new ConcurrentQueue<string>();
        var ended = new EndedScript();
        var worker = await ChainWorkerAsync(Steps(runs, args => args.Value == "b" ? JobResult.Fail("broken") : JobResult.Completed), ended, step);
        ChainStep? third = null;
        ChainStep? fourth = null;

        var chain = await InTransactionAsync(tx => Jobs(worker).RequestChainAsync(tx, builder =>
        {
            var second = builder.StartWith(step, new TestArgs("a")).Then(step, new TestArgs("b"));
            third = second.Then(step, new TestArgs("c"));
            fourth = second.Then(step, new TestArgs("d"), RunCondition.OnFailure);
        }, Cancel));

        var settled = await SettledAsync(worker, chain);
        Assert.Equal(["a", "b", "d"], runs);
        Assert.Equal(JobOutcome.Failed, settled.Outcome);
        var steps = settled.Steps.ToDictionary(s => s.Job);
        Assert.Equal(4, steps.Count);
        Assert.Equal((JobOutcome.Skipped, JobReasons.ChainCondition), (steps[third!.Id].Outcome, steps[third.Id].Reason));
        Assert.Equal(JobOutcome.Completed, steps[fourth!.Id].Outcome);
        Assert.Contains(ended.Calls, e => e.Job == third.Id && e.Outcome == JobOutcome.Skipped);

        ChainId rolledBack;
        await using (var transaction = await Provider.BeginAsync(Cancel))
        {
            rolledBack = await Jobs(worker).RequestChainAsync(transaction.Work, builder => builder.StartWith(step, new TestArgs("x")).Then(step, new TestArgs("y")), Cancel);
        }
        Assert.Null(await Jobs(worker).GetChainAsync(rolledBack, Cancel));
        Assert.DoesNotContain(await ReadJobsAsync(step), j => j.Args is "x" or "y");
    }

    [Fact]
    public async Task K2_FailureContinuationsStartOnlyAfterEveryBranchSettled()
    {
        var step = Definition();
        var runs = new ConcurrentQueue<string>();
        var slow = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var worker = await ChainWorkerAsync(
            Steps(runs, async (args, cancellationToken) =>
            {
                if (args.Value == "slow")
                {
                    await slow.Task.WaitAsync(cancellationToken);
                }
                return args.Value == "fail" ? JobResult.Fail("broken") : JobResult.Completed;
            }),
            null,
            step,
            capacity: 4);
        ChainGroup? group = null;

        var chain = await InTransactionAsync(tx => Jobs(worker).RequestChainAsync(tx, builder =>
        {
            group = builder.StartWith(step, new TestArgs("root"))
                .ThenGroup(g =>
                {
                    g.Add(step, new TestArgs("ok"));
                    g.Add(step, new TestArgs("fail"));
                    g.Add(step, new TestArgs("slow"));
                })
                .AfterAll(step, new TestArgs("all"))
                .AfterAnyFailed(step, new TestArgs("any-failed"))
                .AfterAnyFailedOrCancelled(step, new TestArgs("any-failed-or-cancelled"))
                .AfterAllSettled(step, new TestArgs("settled"));
        }, Cancel));

        await EventuallyAsync(() => runs.Contains("fail") && runs.Contains("ok") && runs.Contains("slow"), TimeSpan.FromSeconds(10), "the branches run");
        await Task.Delay(1000, Cancel);
        Assert.Equal(["fail", "ok", "root", "slow"], runs.Order());
        slow.SetResult();

        var settled = await SettledAsync(worker, chain);
        Assert.Equal(["any-failed", "any-failed-or-cancelled", "settled"], runs.Skip(4).Order());
        Assert.Equal(JobOutcome.Failed, settled.Groups.Single(g => g.Id == group!.Id).Outcome);
        Assert.Equal(JobOutcome.Skipped, (await ByArgsAsync(settled))["all"].Outcome);
    }

    [Fact]
    public async Task K3_ResultsRoundTripToChildrenAndGroupContinuations()
    {
        var producer = ResultDefinition();
        var reader = Definition();
        var read = new ConcurrentDictionary<string, string>();
        var worker = await ProcessAsync(work =>
        {
            work.Services.AddSingleton(new ResultScript { Run = (args, _, _) => ValueTask.FromResult<JobDisposition<TestResult>>(new TestResult("result of " + args.Value)) });
            work.Services.AddSingleton(new JobScript
            {
                Run = async (args, run, cancellationToken) =>
                {
                    if (args.Value == "child")
                    {
                        var parent = await run.GetParentResultAsync(producer, cancellationToken);
                        read[args.Value] = parent!.Result.Value;
                    }
                    else
                    {
                        var results = await run.GetGroupResultsAsync(producer, cancellationToken);
                        read[args.Value] = string.Join("|", results.Select(r => r.Result.Value).Order());
                    }
                    return JobResult.Completed;
                },
            });
            work.AddJob<TestArgs, TestResult, ScriptedResultHandler>(producer, job => job.Lease(TimeSpan.FromSeconds(2)).Capacity(2));
            work.AddJob<TestArgs, ScriptedJobHandler>(reader, job => job.Lease(TimeSpan.FromSeconds(2)));
        });

        var chain = await InTransactionAsync(tx => Jobs(worker).RequestChainAsync(tx, builder => builder
            .StartWith(producer, new TestArgs("root"))
            .Then(reader, new TestArgs("child"))
            .ThenGroup(g =>
            {
                g.Add(producer, new TestArgs("x"));
                g.Add(producer, new TestArgs("y"));
                g.Add(producer, new TestArgs("z")).ThenGroup(n => n.Add(producer, new TestArgs("nested"))).AfterAll(producer, new TestArgs("z after"));
            })
            .AfterAll(reader, new TestArgs("after")), Cancel));

        Assert.Equal(JobOutcome.Completed, (await SettledAsync(worker, chain)).Outcome);
        Assert.Equal("result of root", read["child"]);
        Assert.Equal("result of x|result of y|result of z after", read["after"]);
    }

    [Fact]
    public async Task K4_ADynamicGroupContinuationRunsOnceAfterItsSlowestChild()
    {
        const int Children = 10_000;
        var fan = Definition();
        var child = Definition();
        var completed = 0;
        var summaries = new ConcurrentQueue<TimeSpan>();
        var slow = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        TimeSpan? slowEnded = null;
        var script = new JobScript
        {
            Run = async (args, run, cancellationToken) =>
            {
                switch (args.Value)
                {
                    case "fan":
                        await using (var transaction = await Provider.BeginAsync(cancellationToken))
                        {
                            await run.AddGroupAsync(
                                transaction.Work,
                                g => g.AddMany(child, Enumerable.Range(0, Children).Select(i => new JobRequestItem<TestArgs>(new TestArgs(i == 0 ? "slow" : "c"), JobRequestOptions.Default))),
                                after => after.AfterAllSettled(fan, new TestArgs("summary")),
                                cancellationToken);
                            await run.CompleteAsync(transaction.Work, JobResult.Completed, cancellationToken);
                            await transaction.CommitAsync(cancellationToken);
                        }
                        return run.Completed;
                    case "slow":
                        await slow.Task.WaitAsync(cancellationToken);
                        slowEnded = Clock.Elapsed;
                        return JobResult.Completed;
                    case "summary":
                        summaries.Enqueue(Clock.Elapsed);
                        return JobResult.Completed;
                    default:
                        Interlocked.Increment(ref completed);
                        return JobResult.Completed;
                }
            },
        };
        var worker = await ProcessAsync(work =>
        {
            work.Services.AddSingleton(script);
            // The fan inserts all children in one transaction, which must commit within the fan's lease.
            work.AddJob<TestArgs, ScriptedJobHandler>(fan, job => job.Lease(TimeSpan.FromSeconds(30)));
            work.AddJob<TestArgs, ScriptedJobHandler>(child, job => job.Lease(TimeSpan.FromSeconds(10)).Capacity(32));
        });

        var chain = await InTransactionAsync(tx => Jobs(worker).RequestChainAsync(tx, builder => builder.StartWith(fan, new TestArgs("fan")), Cancel));
        await EventuallyAsync(() => Volatile.Read(ref completed) == Children - 1, TimeSpan.FromMinutes(2), "every child but the slowest completes");
        await Task.Delay(1000, Cancel);
        Assert.Empty(summaries);
        slow.SetResult();

        var settled = await SettledAsync(worker, chain);
        Assert.Equal(JobOutcome.Completed, settled.Outcome);
        Assert.Single(summaries);
        Assert.True(summaries.Single() > slowEnded, "the continuation ran after the slowest child");
        Assert.Equal(Children + 2, settled.Steps.Count);
    }

    [Fact]
    public async Task K5_ARecurringChainUnderOverlapSkipRecordsTheNextOccurrenceSkippedWithoutAChain()
    {
        var daily = Definition();
        var step = Definition();
        var runs = new ConcurrentQueue<string>();
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gated = 0;
        var worker = await ChainWorkerAsync(
            Steps(runs, async (args, cancellationToken) =>
            {
                if (args.Value == "first" && Interlocked.Increment(ref gated) == 1)
                {
                    await first.Task.WaitAsync(cancellationToken);
                }
                return JobResult.Completed;
            }),
            null,
            step,
            also: work => work.AddRecurringChain(
                daily,
                new TestArgs("daily"),
                Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate),
                chain => chain.StartWith(step, new TestArgs("first")).Then(step, new TestArgs("second")),
                new RecurrenceOptions { Overlap = Overlap.Skip }));

        await EventuallyAsync(async () => (await ReadJobsAsync(daily)).Count(j => j.Reason == JobReasons.Overlap) >= 2, TimeSpan.FromSeconds(10), "later occurrences are skipped");
        Assert.Equal(2, (await ReadJobsAsync(step)).Count);
        var occurrences = await ReadJobsAsync(daily);
        var owner = Assert.Single(occurrences, j => j.State is null && !j.SkipPending);
        Assert.All(occurrences.Where(j => j.State is not null), j => Assert.Equal((JobOutcome.Skipped, JobReasons.Overlap), (j.State, j.Reason)));
        first.SetResult();

        var ended = await EndedAsync(owner.Id, Settles);
        Assert.Equal(JobOutcome.Completed, ended.State);
        Assert.Equal(["first", "second"], runs.Take(2));
    }

    [Fact]
    public async Task K6_ASettledChainIsRetriedFromAStepOnceAndALateWriteOfTheOldGenerationIsRejected()
    {
        var step = Definition();
        var runs = new ConcurrentQueue<string>();
        var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        JobRun? stale = null;
        var attempts = 0;
        var worker = await ChainWorkerAsync(
            new JobScript
            {
                Run = async (args, run, cancellationToken) =>
                {
                    runs.Enqueue(args.Value);
                    if (args.Value != "b")
                    {
                        return JobResult.Completed;
                    }
                    if (Interlocked.Increment(ref attempts) == 1)
                    {
                        stale = run;
                        return JobResult.Fail("broken");
                    }
                    await second.Task.WaitAsync(cancellationToken);
                    return JobResult.Completed;
                },
            },
            null,
            step);
        ChainStep? failing = null;
        var chain = await InTransactionAsync(tx => Jobs(worker).RequestChainAsync(tx, builder =>
        {
            failing = builder.StartWith(step, new TestArgs("a")).Then(step, new TestArgs("b"));
            failing.Then(step, new TestArgs("c"));
        }, Cancel));
        Assert.Equal(JobOutcome.Failed, (await SettledAsync(worker, chain)).Outcome);
        Assert.Equal(["a", "b"], runs);

        var retries = await Task.WhenAll(
            Jobs(worker).RetryFromAsync(chain, failing!.Id, Cancel).AsTask(),
            Jobs(worker).RetryFromAsync(chain, failing.Id, Cancel).AsTask());
        Assert.Equal([RetryFromOutcome.Retried, RetryFromOutcome.NotSettled], retries.Order());
        await EventuallyAsync(() => runs.Count == 3, TimeSpan.FromSeconds(10), "the step runs again");
        await using (var transaction = await Provider.BeginAsync(Cancel))
        {
            await Assert.ThrowsAsync<LeaseLostException>(() => stale!.CompleteAsync(transaction.Work, JobResult.Completed, Cancel).AsTask());
        }
        second.SetResult();

        var settled = await SettledAsync(worker, chain);
        Assert.Equal((JobOutcome.Completed, 2L), (settled.Outcome, settled.Generation));
        Assert.Equal(["a", "b", "b", "c"], runs);
        var steps = await ByArgsAsync(settled);
        Assert.Equal((1L, 2L, 2L), (steps["a"].Generation, steps["b"].Generation, steps["c"].Generation));
        Assert.Equal(RetryFromOutcome.NotFound, await Jobs(worker).RetryFromAsync(ChainId.New(), failing.Id, Cancel));
    }

    [Fact]
    public async Task K7_EveryStepOfAChainWhoseRootIsSkippedSettlesSkippedWithoutAHandler()
    {
        var late = Definition(new JobPolicy { Late = Late.SkipAfter(TimeSpan.FromSeconds(1)) });
        var step = Definition();
        var now = await Provider.NowAsync(Cancel);
        var today = DateOnly.FromDateTime(now.UtcDateTime);
        var closed = WorkCalendar.Create("k7-" + Guid.NewGuid().ToString("N")[..12], 1).InTimeZone("UTC").ExcludeDate(today).ExcludeDate(today.AddDays(1));
        var runs = new ConcurrentQueue<string>();
        var worker = await ChainWorkerAsync(Steps(runs, _ => JobResult.Completed), null, [late, step], also: work => work.AddCalendar(closed));

        var blocked = await InTransactionAsync(tx => Jobs(worker).RequestChainAsync(
            tx,
            builder => builder.StartWith(step, new TestArgs("blocked root")).Then(step, new TestArgs("after blocked")).Then(step, new TestArgs("last"), RunCondition.OnAnyOutcome),
            Schedule.At(now + TimeSpan.FromSeconds(1)).Within(closed.Name, Blocked.Skip),
            Cancel));
        var tooLate = await InTransactionAsync(tx => Jobs(worker).RequestChainAsync(
            tx,
            builder => builder.StartWith(late, new TestArgs("late root")).ThenGroup(g => g.Add(step, new TestArgs("branch"))).AfterAll(step, new TestArgs("after late")),
            Schedule.At(now - TimeSpan.FromSeconds(10)),
            Cancel));

        var first = await SettledAsync(worker, blocked);
        var second = await SettledAsync(worker, tooLate);
        var blockedSteps = await ByArgsAsync(first);
        Assert.Equal(JobReasons.CalendarBlocked, blockedSteps["blocked root"].Reason);
        Assert.Equal(JobReasons.RootSkipped, blockedSteps["last"].Reason);
        Assert.All(first.Steps.Concat(second.Steps), s => Assert.Equal(JobOutcome.Skipped, s.Outcome));
        Assert.Equal((JobOutcome.Skipped, JobOutcome.Skipped), (first.Outcome, second.Outcome));
        Assert.Empty(runs);
    }

    [Fact]
    public async Task K8_AChainIsDeletedOnlyOnceSettledOrByCascadeAndRetainedAsAUnit()
    {
        var step = Definition(new JobPolicy { Retention = TimeSpan.FromSeconds(1) });
        var kept = Definition(new JobPolicy { Retention = TimeSpan.FromHours(1) });
        var runs = new ConcurrentQueue<string>();
        var worker = await ChainWorkerAsync(
            Steps(runs, async (args, cancellationToken) =>
            {
                if (args.Value == "running")
                {
                    await Task.Delay(Timeout.Infinite, cancellationToken);
                }
                return JobResult.Completed;
            }),
            null,
            [step, kept]);

        var running = await InTransactionAsync(tx => Jobs(worker).RequestChainAsync(tx, builder => builder.StartWith(step, new TestArgs("running")).Then(step, new TestArgs("next")), Cancel));
        await EventuallyAsync(() => runs.Contains("running"), TimeSpan.FromSeconds(10), "the step runs");
        Assert.Equal(ChainDeleteOutcome.Unsettled, await Jobs(worker).DeleteChainAsync(running, ChainDelete.Refuse, Cancel));
        Assert.NotNull(await Jobs(worker).GetChainAsync(running, Cancel));
        Assert.Equal(ChainDeleteOutcome.Deleting, await Jobs(worker).DeleteChainAsync(running, ChainDelete.Cascade, Cancel));
        await EventuallyAsync(async () => await Jobs(worker).GetChainAsync(running, Cancel) is null, Settles, "the cascade deletes the chain once it settled");
        Assert.DoesNotContain(await ReadJobsAsync(step), j => j.Args is "running" or "next");
        Assert.DoesNotContain("next", runs);

        var expired = await InTransactionAsync(tx => Jobs(worker).RequestChainAsync(tx, builder => builder.StartWith(step, new TestArgs("expired")).Then(step, new TestArgs("expired too")), Cancel));
        var longer = await InTransactionAsync(tx => Jobs(worker).RequestChainAsync(tx, builder => builder.StartWith(step, new TestArgs("short")).Then(kept, new TestArgs("long")), Cancel));
        await SettledAsync(worker, expired);
        await SettledAsync(worker, longer);
        var standalone = await RequestAsync(worker, step, "standalone");
        await EndedAsync(standalone.Id, TimeSpan.FromSeconds(10));
        await Task.Delay(1500, Cancel);
        var deleted = await InTransactionAsync(tx => Jobs(worker).RequestChainAsync(tx, builder => builder.StartWith(step, new TestArgs("deleted")), Cancel));
        await SettledAsync(worker, deleted);
        Assert.Equal(ChainDeleteOutcome.Deleted, await Jobs(worker).DeleteChainAsync(deleted, ChainDelete.Refuse, Cancel));
        Assert.Null(await Jobs(worker).GetChainAsync(deleted, Cancel));
        Assert.Equal(ChainDeleteOutcome.NotFound, await Jobs(worker).DeleteChainAsync(deleted, ChainDelete.Refuse, Cancel));

        var store = worker.Services.GetRequiredService<IWorkStore>();
        while ((await store.Jobs.RetainAsync(WorkRetention.BatchSize, Cancel)).More)
        {
        }
        Assert.Null(await Jobs(worker).GetChainAsync(expired, Cancel));
        Assert.NotNull(await Jobs(worker).GetChainAsync(longer, Cancel));
        Assert.Equal(["long", "short"], (await ReadJobsAsync(step)).Concat(await ReadJobsAsync(kept)).Select(j => j.Args).Order());
    }

    [Fact]
    public async Task K9_EmptyAllSkippedPartlySkippedAndFailedBranchGroupsSettleByTheirRules()
    {
        var step = Definition();
        var runs = new ConcurrentQueue<string>();
        var script = Steps(runs, async (args, run, cancellationToken) =>
        {
            if (args.Value == "dynamic")
            {
                await using var transaction = await Provider.BeginAsync(cancellationToken);
                await run.AddGroupAsync(transaction.Work, _ => { }, after => after.AfterAll(step, new TestArgs("after empty")), cancellationToken);
                await run.CompleteAsync(transaction.Work, JobResult.Completed, cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                return run.Completed;
            }
            return args.Value == "fail first" ? JobResult.Fail("broken") : JobResult.Completed;
        });
        var worker = await ChainWorkerAsync(script, null, step, capacity: 4);
        var groups = new Dictionary<string, ChainGroup>();

        var chain = await InTransactionAsync(tx => Jobs(worker).RequestChainAsync(tx, builder =>
        {
            var root = builder.StartWith(step, new TestArgs("root"));
            root.Then(step, new TestArgs("dynamic"));
            groups["skipped"] = root.ThenGroup(g =>
                {
                    g.Add(step, new TestArgs("s1"), RunCondition.OnFailure);
                    g.Add(step, new TestArgs("s2"), RunCondition.OnFailure);
                })
                .AfterAll(step, new TestArgs("all skipped: all"))
                .AfterAllSettled(step, new TestArgs("all skipped: settled"));
            groups["partly"] = root.ThenGroup(g =>
                {
                    g.Add(step, new TestArgs("ok"));
                    g.Add(step, new TestArgs("p1"), RunCondition.OnFailure);
                    g.Add(step, new TestArgs("p2"), RunCondition.OnFailure);
                })
                .AfterAll(step, new TestArgs("partly: all"));
            groups["failed"] = root.ThenGroup(g =>
                {
                    g.Add(step, new TestArgs("fail first")).Then(step, new TestArgs("skipped second"));
                    g.Add(step, new TestArgs("fine"));
                })
                .AfterAll(step, new TestArgs("failed: all"))
                .AfterAnyFailed(step, new TestArgs("failed: any failed"));
        }, Cancel));

        var settled = await SettledAsync(worker, chain);
        var outcomes = settled.Groups.ToDictionary(g => g.Id, g => g.Outcome);
        Assert.Equal(JobOutcome.Skipped, outcomes[groups["skipped"].Id]);
        Assert.Equal(JobOutcome.Completed, outcomes[groups["partly"].Id]);
        Assert.Equal(JobOutcome.Failed, outcomes[groups["failed"].Id]);
        Assert.Contains(settled.Groups, g => g.Id != groups["skipped"].Id && g.Id != groups["partly"].Id && g.Id != groups["failed"].Id && g.Outcome == JobOutcome.Completed);
        Assert.Contains("after empty", runs);
        Assert.Contains("all skipped: settled", runs);
        Assert.Contains("partly: all", runs);
        Assert.Contains("failed: any failed", runs);
        Assert.DoesNotContain("all skipped: all", runs);
        Assert.DoesNotContain("failed: all", runs);
        Assert.DoesNotContain("skipped second", runs);
    }

    [Fact]
    public async Task K10_AWaitingStepIsReportedRefusedByActionsAndCancelledLikeAnyOutstandingJob()
    {
        var step = Definition();
        var feed = Definition(subject: "feed");
        var runs = new ConcurrentQueue<string>();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var worker = await ChainWorkerAsync(
            Steps(runs, async (args, cancellationToken) =>
            {
                if (args.Value == "first")
                {
                    await gate.Task.WaitAsync(cancellationToken);
                }
                return JobResult.Completed;
            }),
            null,
            [step, feed]);
        var subject = Subject.Of("feed", "npm");
        ChainStep? waiting = null;
        ChainStep? first = null;
        var chain = await InTransactionAsync(tx => Jobs(worker).RequestChainAsync(tx, builder =>
        {
            first = builder.StartWith(step, new TestArgs("first"));
            waiting = first.Then(feed, new TestArgs("waiting"), new JobRequestOptions { Subject = subject });
            waiting.Then(step, new TestArgs("after cancelled"), RunCondition.OnCancelled);
            waiting.Then(step, new TestArgs("after success"));
        }, Cancel));
        await EventuallyAsync(() => runs.Contains("first"), TimeSpan.FromSeconds(10), "the first step runs");

        var status = await Jobs(worker).GetAsync(feed, subject, Cancel);
        Assert.Equal((JobStatusState.WaitingForParent, waiting!.Id, (DateTimeOffset?)null), (status!.State, status.Job, status.DueAt));
        Assert.Equal(RunNowOutcome.ChainStep, (await Jobs(worker).RunNowAsync(feed, waiting.Id, Cancel)).Outcome);
        Assert.Equal(1, await InTransactionAsync(tx => Jobs(worker).CancelAsync(tx, feed, subject, Cancel)));
        Assert.Equal(JobOutcome.Cancelled, (await EndedAsync(waiting.Id, TimeSpan.FromSeconds(5))).State);
        gate.SetResult();

        var settled = await SettledAsync(worker, chain);
        Assert.Equal(JobOutcome.Cancelled, settled.Outcome);
        Assert.Contains("after cancelled", runs);
        Assert.DoesNotContain("after success", runs);
        Assert.DoesNotContain("waiting", runs);
        Assert.Equal(RerunOutcome.ChainStep, (await Jobs(worker).RerunAsync(step, first!.Id, Cancel)).Outcome);
    }

    [Fact]
    public async Task K11_ASettledChainIsRetriedFromANestedGroupAndItsEnclosingGroupIsDecidedAgain()
    {
        var step = Definition();
        var runs = new ConcurrentQueue<string>();
        var failures = 0;
        var worker = await ChainWorkerAsync(
            Steps(runs, args => args.Value == "n2" && Interlocked.Increment(ref failures) == 1 ? JobResult.Fail("broken") : JobResult.Completed),
            null,
            step,
            capacity: 4);
        ChainGroup? nested = null;
        var chain = await InTransactionAsync(tx => Jobs(worker).RequestChainAsync(tx, builder => builder
            .StartWith(step, new TestArgs("r"))
            .ThenGroup(g =>
            {
                g.Add(step, new TestArgs("x"));
                nested = g.Add(step, new TestArgs("y"))
                    .ThenGroup(n =>
                    {
                        n.Add(step, new TestArgs("n1"));
                        n.Add(step, new TestArgs("n2"));
                    })
                    .AfterAll(step, new TestArgs("after nested"));
            })
            .AfterAll(step, new TestArgs("all"))
            .AfterAnyFailed(step, new TestArgs("any failed")), Cancel));
        Assert.Equal(JobOutcome.Failed, (await SettledAsync(worker, chain)).Outcome);
        Assert.Equal(["any failed", "n1", "n2", "r", "x", "y"], runs.Order(StringComparer.Ordinal));
        runs.Clear();

        Assert.Equal(RetryFromOutcome.Retried, await Jobs(worker).RetryFromGroupAsync(chain, nested!.Id, Cancel));

        var settled = await SettledAsync(worker, chain);
        Assert.Equal((JobOutcome.Completed, 2L), (settled.Outcome, settled.Generation));
        Assert.Equal(["after nested", "all", "n1", "n2"], runs.Order(StringComparer.Ordinal));
        Assert.All(settled.Groups, g => Assert.Equal(JobOutcome.Completed, g.Outcome));
        var steps = await ByArgsAsync(settled);
        Assert.Equal(JobOutcome.Skipped, steps["any failed"].Outcome);
        Assert.Equal((1L, 1L, 2L, 2L), (steps["x"].Generation, steps["y"].Generation, steps["n1"].Generation, steps["all"].Generation));
    }

    [Fact]
    public async Task K12_RetryingFromAStepThatAddedAGroupReplacesTheGroupItAdded()
    {
        var step = Definition();
        var runs = new ConcurrentQueue<string>();
        var failures = 0;
        var script = Steps(runs, async (args, run, cancellationToken) =>
        {
            switch (args.Value)
            {
                case "fan":
                    await using (var transaction = await Provider.BeginAsync(cancellationToken))
                    {
                        await run.AddGroupAsync(
                            transaction.Work,
                            g => g.AddMany(step, [new(new TestArgs("d1"), JobRequestOptions.Default), new(new TestArgs("d2"), JobRequestOptions.Default)]),
                            after => after.AfterAllSettled(step, new TestArgs("summary")),
                            cancellationToken);
                        await run.CompleteAsync(transaction.Work, JobResult.Completed, cancellationToken);
                        await transaction.CommitAsync(cancellationToken);
                    }
                    return run.Completed;
                case "summary" when Interlocked.Increment(ref failures) == 1:
                    return JobResult.Fail("broken");
                default:
                    return JobResult.Completed;
            }
        });
        var worker = await ChainWorkerAsync(script, null, step, capacity: 4);
        ChainStep? fan = null;
        var chain = await InTransactionAsync(tx => Jobs(worker).RequestChainAsync(tx, builder => builder
            .StartWith(step, new TestArgs("r"))
            .ThenGroup(g => fan = g.Add(step, new TestArgs("fan")))
            .AfterAll(step, new TestArgs("done")), Cancel));
        Assert.Equal(JobOutcome.Failed, (await SettledAsync(worker, chain)).Outcome);
        Assert.Equal(["d1", "d2", "fan", "r", "summary"], runs.Order(StringComparer.Ordinal));
        runs.Clear();

        Assert.Equal(RetryFromOutcome.Retried, await Jobs(worker).RetryFromAsync(chain, fan!.Id, Cancel));

        var settled = await SettledAsync(worker, chain);
        Assert.Equal(JobOutcome.Completed, settled.Outcome);
        Assert.Equal(["d1", "d2", "done", "fan", "summary"], runs.Order(StringComparer.Ordinal));
        Assert.Equal(6, settled.Steps.Count);
        Assert.Equal(2, settled.Groups.Count);
        Assert.All(settled.Steps.Where(s => s.Job != settled.Root), s => Assert.Equal(2L, s.Generation));
    }

    [Fact]
    public async Task K13_CancellingAChainWhileAStepEndsWaitsForTheStepToDecideItsChildren()
    {
        var step = Definition();
        var runs = new ConcurrentQueue<string>();
        var hooked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ChainStep? parent = null;
        var ended = new EndedScript
        {
            Ended = async (end, _, cancellationToken) =>
            {
                if (end.Job == parent!.Id)
                {
                    hooked.TrySetResult();
                    await release.Task.WaitAsync(cancellationToken);
                }
            },
        };
        var worker = await ChainWorkerAsync(Steps(runs, _ => JobResult.Completed), ended, step);
        // Children get random ids around their parent's, so some precede it in id order.
        var chain = await InTransactionAsync(tx => Jobs(worker).RequestChainAsync(tx, builder =>
        {
            parent = builder.StartWith(step, new TestArgs("parent"));
            for (var i = 0; i < 16; i++)
            {
                parent.Then(step, new TestArgs("child"));
            }
        }, Cancel));
        await hooked.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);

        var cancel = InTransactionAsync(tx => Jobs(worker).CancelChainAsync(tx, chain, Cancel));
        await Task.Delay(500, Cancel);
        release.SetResult();

        await cancel.WaitAsync(TimeSpan.FromSeconds(10), Cancel);
        var settled = await SettledAsync(worker, chain);
        Assert.Equal(JobOutcome.Completed, settled.Steps.Single(s => s.Job == parent!.Id).Outcome);
        Assert.Equal(["parent"], runs.Where(r => r == "parent"));
    }

    [Fact]
    public async Task K15_CancellingASubjectAndItsChainAtOnceNeitherDeadlocks()
    {
        var step = Definition();
        var feed = Definition(subject: "feed");
        var runs = new ConcurrentQueue<string>();
        var hooked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ChainStep? waiting = null;
        var ended = new EndedScript
        {
            Ended = async (end, _, cancellationToken) =>
            {
                if (end.Job == waiting!.Id)
                {
                    hooked.TrySetResult();
                    await release.Task.WaitAsync(cancellationToken);
                }
            },
        };
        var worker = await ChainWorkerAsync(
            Steps(runs, async (args, cancellationToken) =>
            {
                if (args.Value == "root")
                {
                    await Task.Delay(Timeout.Infinite, cancellationToken);
                }
                return JobResult.Completed;
            }),
            ended,
            [step, feed]);
        var subject = Subject.Of("feed", "npm");
        // Children get random ids around their parent's, so some precede it in id order.
        var chain = await InTransactionAsync(tx => Jobs(worker).RequestChainAsync(tx, builder =>
        {
            waiting = builder.StartWith(step, new TestArgs("root")).Then(feed, new TestArgs("waiting"), new JobRequestOptions { Subject = subject });
            for (var i = 0; i < 16; i++)
            {
                waiting.Then(step, new TestArgs("child"), RunCondition.OnAnyOutcome);
            }
        }, Cancel));
        await EventuallyAsync(() => runs.Contains("root"), TimeSpan.FromSeconds(10), "the root runs");

        // The subject's cancellation holds the waiting step in its outcome hook, before it decides the step's children.
        var bySubject = InTransactionAsync(tx => Jobs(worker).CancelAllAsync(tx, subject, Cancel));
        await hooked.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);
        var byChain = InTransactionAsync(tx => Jobs(worker).CancelChainAsync(tx, chain, Cancel));
        await Task.Delay(500, Cancel);
        release.SetResult();

        Assert.Equal(1, await bySubject.WaitAsync(TimeSpan.FromSeconds(10), Cancel));
        await byChain.WaitAsync(TimeSpan.FromSeconds(10), Cancel);
        var settled = await SettledAsync(worker, chain);
        Assert.Equal(JobOutcome.Cancelled, settled.Steps.Single(s => s.Job == waiting!.Id).Outcome);
        Assert.All(settled.Steps.Where(s => s.Job != settled.Root && s.Job != waiting!.Id), s => Assert.Equal(JobOutcome.Cancelled, s.Outcome));
        Assert.DoesNotContain("child", runs);
    }

    [Fact]
    public async Task K14_ARecurringChainOccurrenceRunsUntilItsChainSettledAndRecordsOneAttempt()
    {
        var daily = Definition(new JobPolicy { RunHistory = true });
        var step = Definition();
        var runs = new ConcurrentQueue<string>();
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gated = 0;
        var worker = await ChainWorkerAsync(
            Steps(runs, async (args, cancellationToken) =>
            {
                if (Interlocked.Increment(ref gated) == 1)
                {
                    await first.Task.WaitAsync(cancellationToken);
                }
                return JobResult.Completed;
            }),
            null,
            step,
            also: work => work.AddRecurringChain(
                daily,
                new TestArgs("daily"),
                Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate),
                chain => chain.StartWith(step, new TestArgs("root")),
                new RecurrenceOptions { Overlap = Overlap.Skip }));
        await EventuallyAsync(() => !runs.IsEmpty, TimeSpan.FromSeconds(10), "the first chain's root runs");

        var owner = Assert.Single(await ReadJobsAsync(daily), j => j.State is null && !j.SkipPending);
        Assert.Equal(RunNowOutcome.AlreadyRunning, (await Jobs(worker).RunNowAsync(daily, owner.Id, Cancel)).Outcome);
        await Task.Delay(1000, Cancel);
        first.SetResult();

        Assert.Equal(JobOutcome.Completed, (await EndedAsync(owner.Id, Settles)).State);
        var run = Assert.Single((await Jobs(worker).GetRunsAsync(daily, owner.Id, cancellationToken: Cancel)).Runs);
        Assert.Equal((JobRunOutcome.Completed, (string?)null), (run.Outcome, run.Reason));
        Assert.True(run.EndedAt - run.StartedAt >= TimeSpan.FromSeconds(1), "the attempt spans its chain");
    }

    [Fact]
    public async Task K16_CancellingARecurringChainOccurrenceCancelsItsChainAndEndsItOnceTheChainSettled()
    {
        var daily = Definition(new JobPolicy { RunHistory = true });
        var step = Definition();
        var runs = new ConcurrentQueue<string>();
        var worker = await ChainWorkerAsync(
            Steps(runs, async (args, cancellationToken) =>
            {
                await Task.Delay(args.Value == "root" ? Timeout.InfiniteTimeSpan : TimeSpan.Zero, cancellationToken);
                return JobResult.Completed;
            }),
            null,
            step,
            also: work => work.AddRecurringChain(
                daily,
                new TestArgs("daily"),
                Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate),
                chain => chain.StartWith(step, new TestArgs("root")).Then(step, new TestArgs("next"), RunCondition.OnAnyOutcome),
                new RecurrenceOptions { Overlap = Overlap.Skip }));
        await EventuallyAsync(() => runs.Contains("root"), TimeSpan.FromSeconds(10), "the chain's root runs");
        var owner = Assert.Single(await ReadJobsAsync(daily), j => j.State is null && !j.SkipPending);

        // Removing the code-defined recurrence cancels its current occurrence, which its running chain holds.
        Assert.True(await InTransactionAsync(tx => Jobs(worker).RemoveAsync(tx, daily, Cancel)));
        Assert.Null(Assert.Single(await ReadJobsAsync(daily), j => j.Id == owner.Id).State);

        var ended = await EndedAsync(owner.Id, Settles);
        Assert.Equal((JobOutcome.Cancelled, JobReasons.ChainCancelled), (ended.State, ended.Reason));
        var steps = await ReadJobsAsync(step);
        Assert.Equal(JobOutcome.Cancelled, Assert.Single(steps, s => s.Args == "root").State);
        Assert.Equal(JobOutcome.Cancelled, Assert.Single(steps, s => s.Args == "next").State);
        Assert.DoesNotContain("next", runs);
        var run = Assert.Single((await Jobs(worker).GetRunsAsync(daily, owner.Id, cancellationToken: Cancel)).Runs);
        Assert.Equal((JobRunOutcome.Cancelled, JobReasons.ChainCancelled), (run.Outcome, run.Reason));
    }

    [Fact]
    public async Task K17_AStepWhoseParentResultIsNewerThanItsCodecReadsFailsItsAttemptsWithResultVersion()
    {
        var producer = ResultDefinition(resultVersion: 2);
        // The reader's code knows the producer's results only up to version 1, as an older release would.
        var older = ResultDefinition(resultVersion: 1, key: producer.Key);
        var reader = Definition(new JobPolicy { Attempts = 2, Backoff = Backoff.None, RunHistory = true });
        var read = 0;
        var worker = await ProcessAsync(work =>
        {
            work.Services.AddSingleton(new ResultScript { Run = (args, _, _) => ValueTask.FromResult<JobDisposition<TestResult>>(new TestResult(args.Value)) });
            work.Services.AddSingleton(new JobScript
            {
                Run = async (_, run, cancellationToken) =>
                {
                    await run.GetParentResultAsync(older, cancellationToken);
                    Interlocked.Increment(ref read);
                    return JobResult.Completed;
                },
            });
            work.AddJob<TestArgs, TestResult, ScriptedResultHandler>(producer, job => job.Lease(TimeSpan.FromSeconds(2)));
            work.AddJob<TestArgs, ScriptedJobHandler>(reader, job => job.Lease(TimeSpan.FromSeconds(2)));
        });
        ChainStep? root = null;
        ChainStep? child = null;
        await InTransactionAsync(tx => Jobs(worker).RequestChainAsync(tx, builder =>
        {
            root = builder.StartWith(producer, new TestArgs("root"));
            child = root.Then(reader, new TestArgs("child"));
        }, Cancel));

        var ended = await EndedAsync(child!.Id, Settles);
        Assert.Equal((JobOutcome.Failed, JobReasons.AttemptsExhausted), (ended.State, ended.Reason));
        Assert.Equal(0, read);
        var runs = (await Jobs(worker).GetRunsAsync(reader, child.Id, cancellationToken: Cancel)).Runs;
        Assert.Equal([JobReasons.ResultVersion, JobReasons.AttemptsExhausted], runs.OrderBy(r => r.Attempt).Select(r => r.Reason));
        var error = await Assert.ThrowsAsync<JobResultVersionException>(() => Jobs(worker).GetOutcomeAsync(older, root!.Id, Cancel).AsTask());
        Assert.Equal((producer.Key, 2, 1), (error.Definition, error.Version, error.Readable));
    }

    /// <summary>A started process running <paramref name="script"/> for every definition, with <paramref name="ended"/> as their outcome hook.</summary>
    private Task<TestProcess> ChainWorkerAsync(JobScript script, EndedScript? ended, JobDefinition<TestArgs> definition, int capacity = 1, Action<WorkBuilder>? also = null) =>
        ChainWorkerAsync(script, ended, [definition], capacity, also);

    private Task<TestProcess> ChainWorkerAsync(JobScript script, EndedScript? ended, JobDefinition<TestArgs>[] definitions, int capacity = 1, Action<WorkBuilder>? also = null) =>
        ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(script);
                work.Services.AddSingleton(ended ?? new EndedScript());
                foreach (var definition in definitions)
                {
                    work.AddJob<TestArgs, ScriptedJobHandler>(definition, job => job.Lease(TimeSpan.FromSeconds(2)).Capacity(capacity).OnEnded<ScriptedJobEnded>());
                }
                also?.Invoke(work);
            },
            pollingInterval: TimeSpan.FromMilliseconds(500));

    /// <summary>A script that records the arguments of every attempt and returns what <paramref name="then"/> returns.</summary>
    private static JobScript Steps(ConcurrentQueue<string> runs, Func<TestArgs, JobResult> then) =>
        Steps(runs, (args, _) => ValueTask.FromResult(then(args)));

    private static JobScript Steps(ConcurrentQueue<string> runs, Func<TestArgs, CancellationToken, ValueTask<JobResult>> then) =>
        Steps(runs, async (args, _, cancellationToken) => (JobDisposition)await then(args, cancellationToken));

    private static JobScript Steps(ConcurrentQueue<string> runs, Func<TestArgs, JobRun, CancellationToken, ValueTask<JobDisposition>> then) => new()
    {
        Run = (args, run, cancellationToken) =>
        {
            runs.Enqueue(args.Value);
            return then(args, run, cancellationToken);
        },
    };

    /// <summary>The chain's steps by their arguments, read from the job records.</summary>
    private async Task<Dictionary<string, ChainStepStatus>> ByArgsAsync(ChainStatus chain)
    {
        var steps = new Dictionary<string, ChainStepStatus>(StringComparer.Ordinal);
        foreach (var step in chain.Steps)
        {
            steps[(await ReadJobAsync(step.Job)).Args] = step;
        }
        return steps;
    }

    /// <summary>Waits until the chain has settled and returns it.</summary>
    private async Task<ChainStatus> SettledAsync(TestProcess process, ChainId chain)
    {
        ChainStatus? status = null;
        await EventuallyAsync(async () => (status = await Jobs(process).GetChainAsync(chain, Cancel)) is { Settled: true }, Settles, $"chain {chain} settles");
        return status!;
    }
}
