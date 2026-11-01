using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>Definitions by revision, limits, priority, argument versions and request-only processes.</summary>
public abstract class JobDefinitionScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    [Fact]
    public async Task J31_ADefinitionLimitHoldsAcrossProcesses()
    {
        var definition = Definition(new JobPolicy { DefinitionLimit = 2 });
        var running = 0;
        var maximum = 0;
        var done = 0;
        var script = new JobScript
        {
            Run = async (_, _, cancellationToken) =>
            {
                var now = Interlocked.Increment(ref running);
                InterlockedMax(ref maximum, now);
                await Task.Delay(400, cancellationToken);
                Interlocked.Decrement(ref running);
                Interlocked.Increment(ref done);
                return JobResult.Completed;
            },
        };
        for (var i = 0; i < 3; i++)
        {
            await WorkerAsync(definition, script, configure: job => job.Capacity(2));
        }
        var client = await ClientAsync(null, definition);
        for (var i = 0; i < 10; i++)
        {
            await RequestAsync(client, definition, "x" + i);
        }

        await EventuallyAsync(() => Volatile.Read(ref done) == 10, TimeSpan.FromSeconds(30), "every job runs");
        Assert.InRange(maximum, 1, 2);
    }

    [Fact]
    public async Task J31_JobsWithoutAPartitionShareOneDefaultPartitionAcrossProcesses()
    {
        var definition = Definition(new JobPolicy { PartitionLimit = 1 });
        var running = new ConcurrentDictionary<string, int>();
        var maximum = new ConcurrentDictionary<string, int>();
        var total = 0;
        var maximumTotal = 0;
        var done = 0;
        var script = new JobScript
        {
            Run = async (_, run, cancellationToken) =>
            {
                var partition = run.Partition ?? "";
                var now = running.AddOrUpdate(partition, 1, (_, value) => value + 1);
                maximum.AddOrUpdate(partition, now, (_, value) => Math.Max(value, now));
                InterlockedMax(ref maximumTotal, Interlocked.Increment(ref total));
                await Task.Delay(400, cancellationToken);
                running.AddOrUpdate(partition, 0, (_, value) => value - 1);
                Interlocked.Decrement(ref total);
                Interlocked.Increment(ref done);
                return JobResult.Completed;
            },
        };
        await WorkerAsync(definition, script, configure: job => job.Capacity(2));
        await WorkerAsync(definition, script, configure: job => job.Capacity(2));
        var client = await ClientAsync(null, definition);
        for (var i = 0; i < 4; i++)
        {
            await RequestAsync(client, definition, "none" + i);
        }
        for (var i = 0; i < 2; i++)
        {
            await RequestAsync(client, definition, "a" + i, options: new JobRequestOptions { Partition = "a" });
        }

        await EventuallyAsync(() => Volatile.Read(ref done) == 6, TimeSpan.FromSeconds(20), "every job runs");
        Assert.Equal(1, maximum[""]);
        Assert.Equal(1, maximum["a"]);
        Assert.Equal(2, maximumTotal);
    }

    [Fact]
    public async Task J26_HigherPriorityIsClaimedFirstAndNeverPreempts()
    {
        var low = Definition(new JobPolicy { Priority = Priority.Low });
        var high = Definition(new JobPolicy { Priority = Priority.High });
        var client = await ClientAsync(null, low, high);
        await RequestAsync(client, low, "low-1");
        await RequestAsync(client, high, "high-1");
        var order = new ConcurrentQueue<string>();
        var release = new TaskCompletionSource();
        var script = new JobScript
        {
            Run = async (args, _, cancellationToken) =>
            {
                order.Enqueue("start:" + args.Value);
                if (args.Value == "low-1")
                {
                    await release.Task.WaitAsync(cancellationToken);
                }
                order.Enqueue("end:" + args.Value);
                return JobResult.Completed;
            },
        };
        await ProcessAsync(work =>
        {
            work.Services.AddSingleton(script);
            work.AddPool("jobs", capacity: 1)
                .AddJob<TestArgs, ScriptedJobHandler>(low, job => job.Pool("jobs"))
                .AddJob<TestArgs, ScriptedJobHandler>(high, job => job.Pool("jobs"));
        });

        await EventuallyAsync(() => order.Contains("start:low-1"), TimeSpan.FromSeconds(10), "the low-priority job starts after the high one");
        await RequestAsync(client, high, "high-2");
        await Task.Delay(1500, Cancel);
        Assert.DoesNotContain("start:high-2", order);
        release.SetResult();
        await EventuallyAsync(() => order.Contains("end:high-2"), TimeSpan.FromSeconds(10), "the second high-priority job runs");
        Assert.Equal(["start:high-1", "end:high-1", "start:low-1", "end:low-1", "start:high-2", "end:high-2"], order);
    }

    [Fact]
    public async Task J20_AnEqualRevisionWithDifferentContentFailsAtStartup()
    {
        var stored = Definition(new JobPolicy { Attempts = 3 });
        var conflicting = Definition(new JobPolicy { Attempts = 4 }, key: stored.Key);
        await ProcessAsync(work => work.AddJobDefinition(stored));

        var conflict = await ProcessAsync(work => work.AddJobDefinition(conflicting), start: false);
        await Assert.ThrowsAsync<JobDefinitionConflictException>(() => conflict.StartAsync(Cancel));
        var same = await ProcessAsync(work => work.AddJobDefinition(Definition(new JobPolicy { Attempts = 3 }, key: stored.Key)), start: false);
        await same.StartAsync(Cancel);
    }

    [Fact]
    public async Task R18_AProcessWithAnOlderRevisionAppliesTheNewestStoredLimits()
    {
        var older = Definition(new JobPolicy { Attempts = 3 });
        var newer = Definition(new JobPolicy { Attempts = 3, DefinitionLimit = 1 }, revision: 2, key: older.Key);
        var running = 0;
        var maximum = 0;
        var done = 0;
        var script = new JobScript
        {
            Run = async (_, _, cancellationToken) =>
            {
                InterlockedMax(ref maximum, Interlocked.Increment(ref running));
                await Task.Delay(300, cancellationToken);
                Interlocked.Decrement(ref running);
                Interlocked.Increment(ref done);
                return JobResult.Completed;
            },
        };
        var worker = await WorkerAsync(older, script, configure: job => job.Capacity(4));
        await ProcessAsync(work => work.AddJobDefinition(newer));
        for (var i = 0; i < 6; i++)
        {
            await RequestAsync(worker, older, "x" + i);
        }

        await EventuallyAsync(() => Volatile.Read(ref done) == 6, TimeSpan.FromSeconds(20), "every job runs");
        Assert.Equal(1, maximum);
        var restarted = await WorkerAsync(older, script);
        var store = restarted.Services.GetRequiredService<IWorkStore>();
        Assert.Equal(2, (await store.Jobs.GetDefinitionAsync(older.Key, Cancel))!.Revision);
    }

    [Fact]
    public async Task J35_OldProcessesNeverClaimJobsOfANewerArgumentVersion()
    {
        var oldDefinition = Definition();
        var newDefinition = Definition(argsVersion: 2, key: oldDefinition.Key);
        var oldAttempts = new ConcurrentQueue<ObservedAttempt>();
        await WorkerAsync(oldDefinition, Recording(oldAttempts));
        var client = await ClientAsync(null, newDefinition);
        var job = await RequestAsync(client, newDefinition, "v2");

        await Task.Delay(2500, Cancel);
        Assert.Empty(oldAttempts);
        var waiting = await ReadJobAsync(job.Id);
        Assert.Equal((2, 0, (JobOutcome?)null), (waiting.ArgsVersion, waiting.Attempts, waiting.State));

        var newAttempts = new ConcurrentQueue<ObservedAttempt>();
        await WorkerAsync(newDefinition, Recording(newAttempts), configure: j => j.AcceptsArgs(1, 2));
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(job.Id, TimeSpan.FromSeconds(10))).State);
        Assert.Single(newAttempts);
        Assert.Empty(oldAttempts);
    }

    [Fact]
    public async Task J22_ARetiredArgumentVersionEndsFailedThroughTheHookWithoutRunning()
    {
        var current = Definition();
        var client = await ClientAsync(null, current);
        var job = await RequestAsync(client, current, "v1", Schedule.After(TimeSpan.FromHours(1)));
        var retiring = Definition(new JobPolicy { MinimumArgsVersion = 2 }, revision: 2, argsVersion: 2, key: current.Key);
        var attempts = new ConcurrentQueue<ObservedAttempt>();
        var ended = new EndedScript();
        await WorkerAsync(retiring, Recording(attempts), ended);

        var state = await EndedAsync(job.Id, TimeSpan.FromSeconds(10));
        Assert.Equal((JobOutcome.Failed, JobReasons.ArgsVersion), (state.State, state.Reason));
        Assert.Empty(attempts);
        var end = Assert.Single(ended.Calls);
        Assert.Equal((job.Id, JobOutcome.Failed, JobReasons.ArgsVersion), (end.Job, end.Outcome, end.Reason));
    }

    [Fact]
    public async Task J32_AProcessWithoutAHandlerNeverRunsTheJobs()
    {
        var definition = Definition();
        var requester = await ProcessAsync(work => work.AddJobDefinition(definition));
        var job = await RequestAsync(requester, definition, "x");

        await Task.Delay(2000, Cancel);
        var state = await ReadJobAsync(job.Id);
        Assert.Equal((false, 0, (JobOutcome?)null), (state.Held, state.Attempts, state.State));
    }

    private static void InterlockedMax(ref int target, int value)
    {
        var current = Volatile.Read(ref target);
        while (value > current && Interlocked.CompareExchange(ref target, value, current) is var seen && seen != current)
        {
            current = seen;
        }
    }
}
