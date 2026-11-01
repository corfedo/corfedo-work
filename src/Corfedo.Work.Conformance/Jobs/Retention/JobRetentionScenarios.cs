using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>Retention of terminal jobs and run history by each definition's stored durations (J12, D7).</summary>
public abstract class JobRetentionScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    private static readonly TimeSpan Short = TimeSpan.FromSeconds(1);

    private static readonly TimeSpan Long = TimeSpan.FromHours(1);

    [Fact]
    public async Task J24_RetentionDeletesTerminalJobsAfterTheirWindowAndKeepsOutstandingAndRecentOnes()
    {
        var definition = Definition(new JobPolicy { Retention = Short, RunHistoryRetention = Long, RunHistory = true }, subject: "feed");
        var subjectless = Definition(new JobPolicy { Retention = Short });
        var kept = Definition(new JobPolicy { Retention = Long });
        var subject = Subject.Of("feed", "a");
        var worker = await WorkerAsync(definition, new JobScript());
        var other = await WorkerAsync(subjectless, new JobScript());
        var longer = await WorkerAsync(kept, new JobScript());
        var ended = await RequestAsync(worker, definition, "ended", options: For(subject));
        var anonymous = await RequestAsync(other, subjectless, "anonymous");
        var recent = await RequestAsync(longer, kept, "recent");
        await EndedAsync(ended.Id, TimeSpan.FromSeconds(10));
        await EndedAsync(anonymous.Id, TimeSpan.FromSeconds(10));
        await EndedAsync(recent.Id, TimeSpan.FromSeconds(10));
        var outstanding = await RequestAsync(worker, definition, "outstanding", Schedule.After(Long), For(Subject.Of("feed", "b")));
        await Task.Delay(Short + TimeSpan.FromMilliseconds(500), Cancel);

        await RetainAllAsync(worker);

        Assert.Null(await Jobs(worker).GetOutcomeAsync(definition, ended.Id, Cancel));
        Assert.Null(await Jobs(worker).GetAsync(definition, subject, Cancel));
        Assert.Null(await Jobs(other).GetOutcomeAsync(subjectless, anonymous.Id, Cancel));
        Assert.Equal([outstanding.Id], (await ReadJobsAsync(definition)).Select(j => j.Id));
        Assert.Equal([recent.Id], (await ReadJobsAsync(kept)).Select(j => j.Id));
        Assert.Single((await Jobs(worker).GetRunsAsync(definition, ended.Id, cancellationToken: Cancel)).Runs);
    }

    [Fact]
    public async Task D8_RunHistoryIsRetainedIndependentlyOfItsJobs()
    {
        var definition = Definition(new JobPolicy { Retention = Long, RunHistoryRetention = Short, RunHistory = true }, subject: "feed");
        var subjectless = Definition(new JobPolicy { Retention = Long, RunHistoryRetention = Short, RunHistory = true });
        var subject = Subject.Of("feed", "a");
        var worker = await WorkerAsync(definition, new JobScript());
        var other = await WorkerAsync(subjectless, new JobScript());
        var job = await RequestAsync(worker, definition, "x", options: For(subject));
        var anonymous = await RequestAsync(other, subjectless, "y");
        await EndedAsync(job.Id, TimeSpan.FromSeconds(10));
        await EndedAsync(anonymous.Id, TimeSpan.FromSeconds(10));
        Assert.Single((await Jobs(other).GetRunsAsync(subjectless, anonymous.Id, cancellationToken: Cancel)).Runs);
        await Task.Delay(Short + TimeSpan.FromMilliseconds(500), Cancel);

        await RetainAllAsync(worker);

        Assert.Empty((await Jobs(worker).GetRunsAsync(definition, subject, cancellationToken: Cancel)).Runs);
        Assert.Empty((await Jobs(other).GetRunsAsync(subjectless, anonymous.Id, cancellationToken: Cancel)).Runs);
        Assert.Equal(JobOutcome.Completed, (await Jobs(worker).GetOutcomeAsync(definition, job.Id, Cancel))!.Outcome);
        Assert.Equal(JobOutcome.Completed, (await Jobs(worker).GetAsync(definition, subject, Cancel))!.LastOutcome);
    }

    [Fact]
    public async Task J24_RetentionKeepsOccurrencesThePlannerHasNotConsumed()
    {
        var definition = Definition(new JobPolicy { Retention = Short, Backoff = Backoff.None }, subject: "feed");
        var subject = Subject.Of("feed", "a");
        var client = await ClientAsync(null, definition);
        await ScheduleAsync(client, definition, "scheduled", Schedule.Every(Long, Interval.FixedRate, await Provider.NowAsync(Cancel)), Recurring(subject));
        var triggered = await InTransactionAsync(t => Jobs(client).TriggerAsync(t, definition, subject, Cancel));
        var claim = await CrashClaimAsync(client, definition, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(5));
        var store = client.Services.GetRequiredService<IWorkStore>();
        Assert.True(await store.Jobs.CompleteAsync(claim.Lease, JobTransition.From(JobResult.Completed), null, Cancel));
        Assert.True((await ReadJobAsync(triggered.Job)).Unconsumed);
        await Task.Delay(Short + TimeSpan.FromMilliseconds(500), Cancel);

        await RetainAllAsync(client);
        Assert.Equal(JobOutcome.Completed, (await ReadJobAsync(triggered.Job)).State);

        await WorkerAsync(definition, new JobScript());
        await EventuallyAsync(async () => !(await ReadJobAsync(triggered.Job)).Unconsumed, TimeSpan.FromSeconds(20), "the planner consumes the occurrence");
        await RetainAllAsync(client);
        Assert.Empty(await ReadJobsAsync(definition));
    }

    [Fact]
    public async Task J24_RetentionSkipsLockedJobsAndKeepsARerun()
    {
        RequireConcurrentTransactions();
        var definition = Definition(new JobPolicy { Retention = Short }, subject: "feed");
        var locked = Subject.Of("feed", "locked");
        var rerun = Subject.Of("feed", "rerun");
        var worker = await WorkerAsync(definition, new JobScript());
        var first = await RequestAsync(worker, definition, "x", options: For(locked));
        var second = await RequestAsync(worker, definition, "y", options: For(rerun));
        await EndedAsync(first.Id, TimeSpan.FromSeconds(10));
        await EndedAsync(second.Id, TimeSpan.FromSeconds(10));
        await worker.StopAsync(Cancel);
        var client = await ClientAsync(null, definition);
        await Task.Delay(Short + TimeSpan.FromMilliseconds(500), Cancel);
        Assert.Equal(RerunOutcome.Rerun, (await Jobs(client).RerunAsync(definition, second.Id, Cancel)).Outcome);

        await using (var transaction = await Provider.BeginAsync(Cancel))
        {
            await Provider.LockSubjectAsync(transaction.Work, definition.Key, locked, Cancel);
            await RetainAllAsync(client).WaitAsync(TimeSpan.FromSeconds(5), Cancel);
            await transaction.RollbackAsync(Cancel);
        }
        Assert.Equal([first.Id, second.Id], (await ReadJobsAsync(definition)).Select(j => j.Id));

        await RetainAllAsync(client);
        Assert.Equal([second.Id], (await ReadJobsAsync(definition)).Select(j => j.Id));
        Assert.Equal(new RerunResult(first.Id, RerunOutcome.NotFound), await Jobs(client).RerunAsync(definition, first.Id, Cancel));
        Assert.Equal(new RunNowResult(first.Id, RunNowOutcome.NotFound), await Jobs(client).RunNowAsync(definition, first.Id, Cancel));
    }

    [Fact]
    public async Task J24_RetentionRacingRerunsDeletesAJobOrLeavesItsRerunOutstanding()
    {
        var definition = Definition(new JobPolicy { Retention = Short }, subject: "feed");
        var worker = await WorkerAsync(definition, new JobScript(), configure: job => job.Capacity(8));
        var requested = await InTransactionAsync(transaction => Jobs(worker).RequestManyAsync(
            transaction, definition, Enumerable.Range(0, 300).Select(i => new JobRequestItem<TestArgs>(new TestArgs("x"), For(Subject.Of("feed", $"s{i}")))), Schedule.Now, Cancel));
        var ids = requested.Select(r => r.Id).ToArray();
        await EventuallyAsync(async () => (await ReadJobsAsync(definition)).All(j => j.State is not null), TimeSpan.FromSeconds(30), "every job ends");
        await worker.StopAsync(Cancel);
        var client = await ClientAsync(null, definition);
        var store = client.Services.GetRequiredService<IWorkStore>();
        await Task.Delay(Short + TimeSpan.FromMilliseconds(500), Cancel);

        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reruns = Enumerable.Range(0, 4).Select(n => Task.Run(
            async () =>
            {
                await start.Task;
                return await Jobs(client).RerunManyAsync(definition, ids.Where((_, i) => i % 4 == n).Reverse(), Cancel);
            },
            Cancel)).ToArray();
        var rerunning = Task.WhenAll(reruns);
        var retention = Enumerable.Range(0, 2).Select(_ => Task.Run(
            async () =>
            {
                await start.Task;
                while (!rerunning.IsCompleted)
                {
                    await store.Jobs.RetainAsync(5, Cancel);
                }
            },
            Cancel)).ToArray();
        start.SetResult();
        var results = (await rerunning).SelectMany(r => r).ToArray();
        await Task.WhenAll(retention);
        await RetainAllAsync(client);

        var remaining = (await ReadJobsAsync(definition)).ToDictionary(j => j.Id);
        Assert.Equal(ids.Select(id => id.Value).Order(), results.Select(r => r.Job.Value).Order());
        Assert.All(results, r => Assert.True(
            r.Outcome == RerunOutcome.Rerun ? remaining.TryGetValue(r.Job, out var job) && job.State is null : r.Outcome == RerunOutcome.NotFound && !remaining.ContainsKey(r.Job),
            $"{r.Job} {r.Outcome}"));
        Assert.Contains(results, r => r.Outcome == RerunOutcome.Rerun);
        Assert.Contains(results, r => r.Outcome == RerunOutcome.NotFound);
    }

    [Fact]
    public async Task J24_AProcessThatDoesNotOptInRegistersNoRetention()
    {
        var definition = Definition(subject: "feed");
        var client = await ClientAsync(null, definition);
        var worker = await WorkerAsync(definition, new JobScript());

        foreach (var process in new[] { client, worker })
        {
            Assert.DoesNotContain(process.Services.GetRequiredService<JobRegistry>().Definitions, r => r.Definition.Key == WorkRetention.Definition.Key);
            Assert.DoesNotContain(process.Services.GetServices<DeclaredRecurrence>(), r => r.Definition.Key == WorkRetention.Definition.Key);
            Assert.DoesNotContain(process.Services.GetServices<LeaseSourceRegistration>(), r => r.Name.Contains(WorkRetention.Definition.Key, StringComparison.Ordinal));
        }
        Assert.Empty(client.Services.GetServices<LeaseSourceRegistration>());
        Assert.Empty(client.Services.GetServices<IWorkBackground>());
        Assert.Single(worker.Services.GetServices<IWorkBackground>());
    }

    [Fact]
    public async Task J24_TheHostRegistersRetentionAsARecurringJobWithItsDefaults()
    {
        var definition = Definition(subject: "feed");
        var expecting = Definition(new JobPolicy { Attempts = 1, Expect = Expect.FailsAtMostInARow(0) }, subject: "feed");
        var subject = Subject.Of("feed", "a");
        var client = await ClientAsync(null, Definition());
        var store = client.Services.GetRequiredService<IWorkStore>();
        Assert.Null(await store.Jobs.GetDefinitionAsync(WorkRetention.Definition.Key, Cancel));
        var worker = await ProcessAsync(work =>
        {
            work.Services.AddSingleton(new JobScript
            {
                Run = (args, _, _) => ValueTask.FromResult<JobDisposition>(args.Value == "fail" ? JobResult.Fail("broken") : JobResult.Completed),
            });
            work.Services.AddSingleton(new EndedScript());
            work.Retention(Short, Long, incidents: Short);
            work.AddJob<TestArgs, ScriptedJobHandler>(definition, job => job.Lease(TimeSpan.FromSeconds(2)));
            work.AddJob<TestArgs, ScriptedJobHandler>(expecting, job => job.Lease(TimeSpan.FromSeconds(2)));
            work.AddRetention();
        });
        Assert.Single(worker.Services.GetServices<DeclaredRecurrence>(), r => r.Definition.Key == WorkRetention.Definition.Key);
        var stored = (await store.Jobs.GetDefinitionAsync(definition.Key, Cancel))!;
        Assert.Equal((Short, Long), (stored.Policy.Retention, stored.Policy.RunHistoryRetention));
        var retention = (await store.Jobs.GetDefinitionAsync(WorkRetention.Definition.Key, Cancel))!;
        Assert.False(retention.Policy.RunHistory);
        var job = await RequestAsync(worker, definition, "x", options: For(subject));
        await EndedAsync(job.Id, TimeSpan.FromSeconds(10));

        // One incident resolved, one open, by jobs of a definition that keeps its jobs for the host default.
        foreach (var (key, args) in new[] { ("resolved", "fail"), ("resolved", "ok"), ("open", "fail") })
        {
            var ended = await RequestAsync(worker, expecting, args, options: For(Subject.Of("feed", key), RequestMode.New));
            await EndedAsync(ended.Id, TimeSpan.FromSeconds(10));
        }
        var incidents = worker.Services.GetRequiredService<IncidentOperations>();
        Assert.Equal(2, (await incidents.GetIncidentsAsync(definition: expecting.Key, cancellationToken: Cancel)).Incidents.Count);
        await Task.Delay(Short + TimeSpan.FromMilliseconds(500), Cancel);

        var triggered = await InTransactionAsync(t => Jobs(worker).TriggerAsync(t, WorkRetention.Definition, Cancel));
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(triggered.Job, TimeSpan.FromSeconds(10))).State);

        Assert.Empty(await ReadJobsAsync(definition));
        var kept = Assert.Single((await incidents.GetIncidentsAsync(definition: expecting.Key, cancellationToken: Cancel)).Incidents);
        Assert.Equal((Subject.Of("feed", "open"), (DateTimeOffset?)null), (kept.Subject, kept.ResolvedAt));
        await Assert.ThrowsAsync<InvalidOperationException>(() => ProcessAsync(work => work.AddRetention().AddRetention()));
    }

    [Fact]
    public void J24_RetentionDurationsArePartOfTheDefinitionsDigest()
    {
        JobDefinition Create(JobPolicy policy) => Definition(policy, key: "digest");
        var unset = Create(new JobPolicy());
        Assert.NotEqual(unset.Digest, Create(new JobPolicy { Retention = Short }).Digest);
        Assert.NotEqual(unset.Digest, Create(new JobPolicy { RunHistoryRetention = Short }).Digest);
        Assert.Equal(unset.Digest, Create(new JobPolicy()).Digest);
        Assert.Throws<ArgumentOutOfRangeException>(() => Create(new JobPolicy { Retention = TimeSpan.Zero }));
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
