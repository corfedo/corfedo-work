using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>Failed jobs: their retention, the failed-jobs list and count, and discard (J12, J21).</summary>
public abstract class FailedJobScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    private static readonly TimeSpan Short = TimeSpan.FromSeconds(1);

    private static readonly TimeSpan Long = TimeSpan.FromHours(1);

    private static readonly TimeSpan Ends = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task J43_FailedJobsAreKeptForTheirOwnWindowOrUntilHandled()
    {
        var keeping = Definition(Policy(Short, Timeout.InfiniteTimeSpan), subject: "feed");
        var shortFailed = Definition(Policy(Long, Short), subject: "feed");
        var unset = Definition(Policy(Short, null), subject: "feed");
        var flaky = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var worker = await WorkerAsync(keeping, Outcomes(flaky), configure: job => job.Capacity(4), also: work =>
        {
            work.AddJob<TestArgs, ScriptedJobHandler>(shortFailed, job => job.Lease(TimeSpan.FromSeconds(2)));
            work.AddJob<TestArgs, ScriptedJobHandler>(unset, job => job.Lease(TimeSpan.FromSeconds(2)));
        });
        var store = worker.Services.GetRequiredService<IWorkStore>();
        Assert.Equal(Timeout.InfiniteTimeSpan, (await store.Jobs.GetDefinitionAsync(keeping.Key, Cancel))!.Policy.FailedRetention);
        Assert.Equal(Short, (await store.Jobs.GetDefinitionAsync(shortFailed.Key, Cancel))!.Policy.FailedRetention);
        Assert.Equal(Short, (await store.Jobs.GetDefinitionAsync(unset.Key, Cancel))!.Policy.FailedRetention);

        var completed = await RequestAsync(worker, keeping, "complete", options: For(Subject.Of("feed", "completed")));
        var failed = await RequestAsync(worker, keeping, "fail", options: For(Subject.Of("feed", "failed")));
        var exhausted = await RequestAsync(worker, keeping, "retry", options: For(Subject.Of("feed", "exhausted")));
        var timedOut = await RequestAsync(worker, keeping, "timeout", options: For(Subject.Of("feed", "timed-out")));
        var skipped = await RequestAsync(worker, keeping, "complete", Schedule.At(DateTimeOffset.UtcNow.AddSeconds(-5)), For(Subject.Of("feed", "skipped")));
        var cancelled = await RequestAsync(worker, keeping, "complete", Schedule.After(Long), For(Subject.Of("feed", "cancelled")));
        await InTransactionAsync(t => Jobs(worker).CancelAsync(t, keeping, Subject.Of("feed", "cancelled"), Cancel));
        var rerunCompletes = await RequestAsync(worker, keeping, "flaky", options: For(Subject.Of("feed", "rerun-completes")));
        var rerunFails = await RequestAsync(worker, keeping, "fail", options: For(Subject.Of("feed", "rerun-fails")));
        var chain = await InTransactionAsync(t => Jobs(worker).RequestChainAsync(t, builder => builder.StartWith(keeping, new TestArgs("fail"), For(Subject.Of("feed", "chain"))), Cancel));
        var occurrenceSubject = Subject.Of("feed", "occurrence");
        await ScheduleAsync(worker, keeping, "fail", Schedule.Every(Long, Interval.FixedRate, await Provider.NowAsync(Cancel)), Recurring(occurrenceSubject));
        var occurrence = await InTransactionAsync(t => Jobs(worker).TriggerAsync(t, keeping, occurrenceSubject, Cancel));
        var shortFailure = await RequestAsync(worker, shortFailed, "fail", options: For(Subject.Of("feed", "a")));
        var shortCompletion = await RequestAsync(worker, shortFailed, "complete", options: For(Subject.Of("feed", "b")));
        var unsetFailure = await RequestAsync(worker, unset, "fail", options: For(Subject.Of("feed", "a")));

        Assert.Equal((JobOutcome.Completed, JobOutcome.Failed), ((await EndedAsync(completed.Id, Ends)).State, (await EndedAsync(failed.Id, Ends)).State));
        Assert.Equal((JobOutcome.Failed, JobReasons.AttemptsExhausted), Ended(await EndedAsync(exhausted.Id, Ends)));
        Assert.Equal(JobOutcome.TimedOut, (await EndedAsync(timedOut.Id, Ends)).State);
        Assert.Equal(JobOutcome.Skipped, (await EndedAsync(skipped.Id, Ends)).State);
        Assert.Equal(JobOutcome.Cancelled, (await EndedAsync(cancelled.Id, Ends)).State);
        Assert.Equal(JobOutcome.Failed, (await EndedAsync(occurrence.Job, Ends)).State);
        await EventuallyAsync(async () => !(await ReadJobAsync(occurrence.Job)).Unconsumed, Ends, "the planner consumes the failed occurrence");
        await EventuallyAsync(async () => await Jobs(worker).GetChainAsync(chain, Cancel) is { Settled: true }, Ends, "the chain settles");
        foreach (var job in new[] { rerunCompletes, rerunFails, shortFailure, shortCompletion, unsetFailure })
        {
            await EndedAsync(job.Id, Ends);
        }
        foreach (var job in new[] { rerunCompletes, rerunFails })
        {
            Assert.Equal(RerunOutcome.Rerun, (await Jobs(worker).RerunAsync(keeping, job.Id, Cancel)).Outcome);
            await EventuallyAsync(async () => (await ReadJobAsync(job.Id)).State is not null, Ends, "the rerun ends");
        }
        Assert.Equal(JobOutcome.Completed, (await ReadJobAsync(rerunCompletes.Id)).State);
        Assert.Equal(JobOutcome.Failed, (await ReadJobAsync(rerunFails.Id)).State);
        await Task.Delay(Short + TimeSpan.FromMilliseconds(500), Cancel);

        await RetainAllAsync(worker);

        var kept = (await ReadJobsAsync(keeping)).Where(j => j.State is not null).Select(j => j.Id).ToHashSet();
        Assert.Equal(new[] { failed.Id, exhausted.Id, timedOut.Id, rerunFails.Id, occurrence.Job }.ToHashSet(), kept);
        Assert.Null(await Jobs(worker).GetChainAsync(chain, Cancel));
        Assert.Equal([shortCompletion.Id], (await ReadJobsAsync(shortFailed)).Select(j => j.Id));
        Assert.Empty(await ReadJobsAsync(unset));
        Assert.Equal(JobOutcome.Failed, (await Jobs(worker).GetOutcomeAsync(keeping, failed.Id, Cancel))!.Outcome);
    }

    [Fact]
    public async Task J43_AnUnsetFailedRetentionTakesTheJobWindowTheHostDefaults()
    {
        var definition = Definition(new JobPolicy());
        var process = await ProcessAsync(work =>
        {
            work.Retention(Short, Long);
            work.AddJobDefinition(definition);
        });
        var stored = (await process.Services.GetRequiredService<IWorkStore>().Jobs.GetDefinitionAsync(definition.Key, Cancel))!;
        Assert.Equal((Short, Short), (stored.Policy.Retention, stored.Policy.FailedRetention));
    }

    [Fact]
    public void J43_ADeclaredFailedRetentionIsPartOfTheDigest()
    {
        JobDefinition Create(JobPolicy policy) => Definition(policy, key: "failed.digest");
        var unset = Create(new JobPolicy());
        Assert.Equal(unset.Digest, Create(new JobPolicy()).Digest);
        Assert.NotEqual(unset.Digest, Create(new JobPolicy { FailedRetention = Short }).Digest);
        Assert.NotEqual(unset.Digest, Create(new JobPolicy { FailedRetention = Timeout.InfiniteTimeSpan }).Digest);
        Assert.NotEqual(Create(new JobPolicy { FailedRetention = Short }).Digest, Create(new JobPolicy { FailedRetention = Timeout.InfiniteTimeSpan }).Digest);
        Assert.Throws<ArgumentOutOfRangeException>(() => Create(new JobPolicy { FailedRetention = TimeSpan.Zero }));
        Assert.Throws<ArgumentOutOfRangeException>(() => Create(new JobPolicy { FailedRetention = TimeSpan.FromSeconds(-1) }));
    }

    [Fact]
    public async Task J44_FailedJobsArePagedNewestFirstAcrossChanges()
    {
        var definition = Definition(Policy(Long, Timeout.InfiniteTimeSpan), subject: "feed");
        var other = Definition(Policy(Long, Timeout.InfiniteTimeSpan), subject: "feed");
        var worker = await WorkerAsync(definition, Outcomes(new(StringComparer.Ordinal)), configure: job => job.Capacity(16), also: work =>
            work.AddJob<TestArgs, ScriptedJobHandler>(other, job => job.Lease(TimeSpan.FromSeconds(2))));
        var items = Enumerable.Range(0, 120).Select(i => new JobRequestItem<TestArgs>(new TestArgs(i < 100 ? "fail" : "timeout"), For(Subject.Of("feed", $"s{i}"))));
        var requested = (await InTransactionAsync(t => Jobs(worker).RequestManyAsync(t, definition, items, Schedule.Now, Cancel))).Select(r => r.Id).ToArray();
        var completed = await RequestAsync(worker, definition, "complete", options: For(Subject.Of("feed", "completed")));
        var otherFailed = await RequestAsync(worker, other, "fail", options: For(Subject.Of("feed", "o")));
        var chain = await InTransactionAsync(t => Jobs(worker).RequestChainAsync(t, builder => builder.StartWith(definition, new TestArgs("fail"), For(Subject.Of("feed", "chain"))), Cancel));
        foreach (var job in requested.Append(completed.Id).Append(otherFailed.Id))
        {
            await EndedAsync(job, Ends);
        }
        await EventuallyAsync(async () => await Jobs(worker).GetChainAsync(chain, Cancel) is { Settled: true }, Ends, "the chain settles");

        // Twenty-five jobs end at one instant before all others, so the second page boundary falls among them.
        var tied = (await Provider.NowAsync(Cancel)).AddMinutes(-1);
        foreach (var job in requested.Take(25))
        {
            await Provider.SetEndedAtAsync(job, tied, Cancel);
        }
        var outstanding = await RerunPausedAsync(worker, definition, requested[110], SubjectOf(requested[110], requested));

        var first = await Jobs(worker).GetFailedAsync(definition, cancellationToken: Cancel);
        Assert.Equal(50, first.Jobs.Count);
        var listed = first.Jobs.Select(j => j.Job).ToHashSet();
        var later = requested.Where(id => id != outstanding && !listed.Contains(id)).ToArray();
        var newer = await RequestAsync(worker, definition, "fail", options: For(Subject.Of("feed", "newer")));
        await EndedAsync(newer.Id, Ends);
        var rerunLater = await RerunPausedAsync(worker, definition, later[0], SubjectOf(later[0], requested));
        Assert.Equal(DiscardOutcome.Discarded, (await Jobs(worker).DiscardAsync(definition, later[1], Cancel)).Outcome);

        var pages = new List<FailedJobPage> { first };
        while (pages[^1].Next is { } next)
        {
            pages.Add(await Jobs(worker).GetFailedAsync(definition, 50, next, Cancel));
        }
        var all = pages.SelectMany(p => p.Jobs).ToList();
        var expected = requested.Where(id => id != outstanding && id != rerunLater && id != later[1]).ToHashSet();
        Assert.Equal(expected.Count, all.Count);
        Assert.Equal(expected, all.Select(j => j.Job).ToHashSet());
        Assert.True(all.Zip(all.Skip(1)).All(p => p.First.EndedAt >= p.Second.EndedAt), "pages run newest end first");
        Assert.Null(pages[^1].Next);
        Assert.Equal(3, pages.Count);

        var state = await ReadJobAsync(requested[50]);
        var entry = all.Single(j => j.Job == requested[50]);
        Assert.Equal(
            new FailedJob(requested[50], state.ExecutionId, JobOutcome.Failed, "broken", entry.EndedAt, 1, Subject.Of("feed", "s50"), null, 1),
            entry);
        Assert.Equal((JobOutcome.TimedOut, "timeout"), (all.Single(j => j.Job == requested[100]).Outcome, all.Single(j => j.Job == requested[100]).Reason));
        Assert.Equal(requested.Take(25).Count(expected.Contains), all.Count(j => j.EndedAt == all[^1].EndedAt));
    }

    [Fact]
    public async Task J45_FailedJobsAreCountedForSeveralDefinitionsInOneRead()
    {
        var none = Definition(Policy(Long, Timeout.InfiniteTimeSpan), subject: "feed");
        var few = Definition(Policy(Long, Timeout.InfiniteTimeSpan), subject: "feed");
        var many = Definition(Policy(Long, Timeout.InfiniteTimeSpan), subject: "feed");
        var never = Definition();
        var worker = await WorkerAsync(many, Outcomes(new(StringComparer.Ordinal)), configure: job => job.Capacity(16), also: work =>
        {
            work.AddJob<TestArgs, ScriptedJobHandler>(none, job => job.Lease(TimeSpan.FromSeconds(2)));
            work.AddJob<TestArgs, ScriptedJobHandler>(few, job => job.Lease(TimeSpan.FromSeconds(2)).Capacity(4));
        });
        var failing = (await InTransactionAsync(t => Jobs(worker).RequestManyAsync(
            t, many, Enumerable.Range(0, 1200).Select(i => new JobRequestItem<TestArgs>(new TestArgs("fail"), For(Subject.Of("feed", $"m{i}")))), Schedule.Now, Cancel))).Select(r => r.Id).ToArray();
        var fewFailing = (await InTransactionAsync(t => Jobs(worker).RequestManyAsync(
            t, few, Enumerable.Range(0, 3).Select(i => new JobRequestItem<TestArgs>(new TestArgs("fail"), For(Subject.Of("feed", $"f{i}")))), Schedule.Now, Cancel))).Select(r => r.Id).ToArray();
        await RequestAsync(worker, few, "complete", options: For(Subject.Of("feed", "completed")));
        await EventuallyAsync(async () => (await ReadJobsAsync(many)).All(j => j.State is not null), TimeSpan.FromSeconds(60), "every job of many ends");
        await EventuallyAsync(async () => (await ReadJobsAsync(few)).All(j => j.State is not null), Ends, "every job of few ends");

        var counts = await Jobs(worker).CountFailedAsync([none, few, many, never], Cancel);
        Assert.Equal(
            new[] { (none.Key, 0L), (few.Key, 3L), (many.Key, 1200L), (never.Key, 0L) }.OrderBy(c => c.Key, StringComparer.Ordinal),
            counts.OrderBy(c => c.Key, StringComparer.Ordinal).Select(c => (c.Key, c.Value)));
        Assert.Equal(3, (await Jobs(worker).GetFailedAsync(few, cancellationToken: Cancel)).Jobs.Count);

        await RerunPausedAsync(worker, few, fewFailing[0], Subject.Of("feed", "f0"));
        Assert.Equal(DiscardOutcome.Discarded, (await Jobs(worker).DiscardAsync(many, failing[0], Cancel)).Outcome);
        counts = await Jobs(worker).CountFailedAsync([few, many], Cancel);
        Assert.Equal((2L, 1199L), (counts[few.Key], counts[many.Key]));
        Assert.Empty(await Jobs(worker).CountFailedAsync([], Cancel));
    }

    [Fact]
    public async Task J46_DiscardDeletesTerminalJobsAndRefusesWhatRetentionKeeps()
    {
        var definition = Definition(Policy(Long, Timeout.InfiniteTimeSpan) with { RunHistory = true, Timeout = null }, subject: "feed");
        var ended = new EndedScript();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var outcomes = Outcomes(new(StringComparer.Ordinal));
        var worker = await WorkerAsync(
            definition,
            new JobScript
            {
                Run = async (args, run, cancellationToken) =>
                {
                    if (args.Value == "hold")
                    {
                        await release.Task.WaitAsync(cancellationToken);
                        return JobResult.Completed;
                    }
                    return await outcomes.Run(args, run, cancellationToken);
                },
            },
            ended,
            configure: job => job.Capacity(8));
        var failed = await RequestAsync(worker, definition, "fail", options: For(Subject.Of("feed", "failed")));
        var completed = await RequestAsync(worker, definition, "complete", options: For(Subject.Of("feed", "completed")));
        var scheduled = await RequestAsync(worker, definition, "complete", Schedule.After(Long), For(Subject.Of("feed", "scheduled")));
        var running = await RequestAsync(worker, definition, "hold", options: For(Subject.Of("feed", "running")));
        var rerun = await RequestAsync(worker, definition, "fail", options: For(Subject.Of("feed", "rerun")));
        var chain = await InTransactionAsync(t => Jobs(worker).RequestChainAsync(t, builder => builder.StartWith(definition, new TestArgs("fail"), For(Subject.Of("feed", "chain"))), Cancel));
        var racing = (await InTransactionAsync(t => Jobs(worker).RequestManyAsync(
            t, definition, Enumerable.Range(0, 20).Select(i => new JobRequestItem<TestArgs>(new TestArgs("fail"), For(Subject.Of("feed", $"race{i}")))), Schedule.Now, Cancel))).Select(r => r.Id).ToArray();
        foreach (var job in racing.Append(failed.Id).Append(completed.Id).Append(rerun.Id))
        {
            await EndedAsync(job, Ends);
        }
        await EventuallyAsync(async () => (await ReadJobAsync(running.Id)).Held, Ends, "the held job runs");
        var step = (await EventuallyChainAsync(worker, chain)).Steps.Single().Job;
        await RerunPausedAsync(worker, definition, rerun.Id, Subject.Of("feed", "rerun"));

        // An occurrence ended through its lease by a process that runs no planner stays unconsumed.
        var occurrenceDefinition = Definition(Policy(Long, Timeout.InfiniteTimeSpan), subject: "feed");
        var client = await ClientAsync(null, occurrenceDefinition);
        var occurrenceSubject = Subject.Of("feed", "occurrence");
        await ScheduleAsync(client, occurrenceDefinition, "x", Schedule.Every(Long, Interval.FixedRate, await Provider.NowAsync(Cancel)), Recurring(occurrenceSubject));
        var occurrence = await InTransactionAsync(t => Jobs(client).TriggerAsync(t, occurrenceDefinition, occurrenceSubject, Cancel));
        var claim = await CrashClaimAsync(client, occurrenceDefinition, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(5));
        Assert.True(await client.Services.GetRequiredService<IWorkStore>().Jobs.CompleteAsync(claim.Lease, JobTransition.From(JobResult.Fail("broken")), null, Cancel));
        Assert.True((await ReadJobAsync(occurrence.Job)).Unconsumed);
        var hookCalls = ended.Calls.Count;

        var unknown = new JobId(Guid.NewGuid());
        var results = await Jobs(worker).DiscardManyAsync(definition, [failed.Id, completed.Id, scheduled.Id, running.Id, rerun.Id, step, unknown, failed.Id], Cancel);
        Assert.Equal(
            [
                new(failed.Id, DiscardOutcome.Discarded), new(completed.Id, DiscardOutcome.Discarded), new(scheduled.Id, DiscardOutcome.Outstanding),
                new(running.Id, DiscardOutcome.Outstanding), new(rerun.Id, DiscardOutcome.Outstanding), new(step, DiscardOutcome.ChainStep),
                new(unknown, DiscardOutcome.NotFound), new DiscardResult(failed.Id, DiscardOutcome.Duplicate),
            ],
            results);
        Assert.Equal(new DiscardResult(occurrence.Job, DiscardOutcome.HeldByRecurrence), await Jobs(client).DiscardAsync(occurrenceDefinition, occurrence.Job, Cancel));
        Assert.Null(await Jobs(worker).GetOutcomeAsync(definition, failed.Id, Cancel));
        Assert.Null(await Jobs(worker).GetOutcomeAsync(definition, completed.Id, Cancel));
        Assert.Single((await Jobs(worker).GetRunsAsync(definition, failed.Id, cancellationToken: Cancel)).Runs);
        Assert.DoesNotContain((await Jobs(worker).GetFailedAsync(definition, cancellationToken: Cancel)).Jobs, j => j.Job == failed.Id);
        Assert.Equal(hookCalls, ended.Calls.Count);
        Assert.True((await ReadJobAsync(running.Id)).Held);
        Assert.Null((await ReadJobAsync(scheduled.Id)).State);
        Assert.True((await ReadJobAsync(occurrence.Job)).Unconsumed);

        // A discard and a rerun of the same job: exactly one of them succeeds.
        await worker.StopAsync(Cancel);
        var other = await ClientAsync(null, definition);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var discards = Task.Run(async () => { await start.Task; return await Jobs(other).DiscardManyAsync(definition, racing, Cancel); }, Cancel);
        var reruns = Task.Run(async () => { await start.Task; return await Jobs(other).RerunManyAsync(definition, racing.Reverse(), Cancel); }, Cancel);
        start.SetResult();
        var discarded = (await discards).ToDictionary(r => r.Job, r => r.Outcome);
        var rerunResults = (await reruns).ToDictionary(r => r.Job, r => r.Outcome);
        foreach (var job in racing)
        {
            Assert.True(
                (discarded[job], rerunResults[job]) is (DiscardOutcome.Discarded, RerunOutcome.NotFound) or (DiscardOutcome.Outstanding, RerunOutcome.Rerun),
                $"{job}: {discarded[job]}, {rerunResults[job]}");
        }
        release.SetResult();
    }

    private static JobPolicy Policy(TimeSpan retention, TimeSpan? failedRetention) => new()
    {
        Attempts = 1,
        Backoff = Backoff.None,
        Timeout = TimeSpan.FromSeconds(1),
        Late = Late.SkipAfter(TimeSpan.FromSeconds(2)),
        Retention = retention,
        FailedRetention = failedRetention,
    };

    /// <summary>
    /// Ends each job by its arguments: completes, fails, retries, times out, or fails on its first run and completes on
    /// later ones (<c>flaky</c>).
    /// </summary>
    private static JobScript Outcomes(ConcurrentDictionary<string, int> runs) => new()
    {
        Run = async (args, run, cancellationToken) =>
        {
            switch (args.Value)
            {
                case "fail":
                    return JobResult.Fail("broken");
                case "retry":
                    return JobResult.Retry(TimeSpan.Zero, "again");
                case "timeout":
                    await Task.Delay(Timeout.Infinite, cancellationToken);
                    return JobResult.Completed;
                case "flaky":
                    return runs.AddOrUpdate(run.Id.ToString(), 1, (_, n) => n + 1) == 1 ? JobResult.Fail("broken") : JobResult.Completed;
                default:
                    return JobResult.Completed;
            }
        },
    };

    private static (JobOutcome?, string?) Ended(JobState job) => (job.State, job.Reason);

    /// <summary>The subject a job of <paramref name="requested"/> was requested for.</summary>
    private static Subject SubjectOf(JobId job, JobId[] requested) => Subject.Of("feed", $"s{Array.IndexOf(requested, job)}");

    /// <summary>Pauses the job's subject and reruns the job, so the rerun stays outstanding without running.</summary>
    private async Task<JobId> RerunPausedAsync(TestProcess process, JobDefinition definition, JobId job, Subject subject)
    {
        await InTransactionAsync(t => Jobs(process).PauseAsync(t, definition, subject, Cancel));
        Assert.Equal(RerunOutcome.Rerun, (await Jobs(process).RerunAsync(definition, job, Cancel)).Outcome);
        return job;
    }

    private async Task<ChainStatus> EventuallyChainAsync(TestProcess process, ChainId chain)
    {
        ChainStatus? status = null;
        await EventuallyAsync(async () => (status = await Jobs(process).GetChainAsync(chain, Cancel)) is { Settled: true }, Ends, "the chain settles");
        return status!;
    }

    /// <summary>Runs retention passes through the store until one leaves nothing more.</summary>
    private async Task RetainAllAsync(TestProcess process)
    {
        var store = process.Services.GetRequiredService<IWorkStore>();
        while ((await store.Jobs.RetainAsync(WorkRetention.BatchSize, Cancel)).More)
        {
        }
    }
}
