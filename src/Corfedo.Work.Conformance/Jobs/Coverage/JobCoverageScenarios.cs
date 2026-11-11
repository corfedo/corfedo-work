using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>Reports of which job definitions live processes cover (R12, R18, R21, J10).</summary>
public abstract class JobCoverageScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    private static readonly TimeSpan Hourly = TimeSpan.FromHours(1);

    [Fact]
    public async Task J21_ARecurrenceNoLiveProcessDeclaresIsReportedUndeclaredUntilRemoved()
    {
        var definition = Definition();
        var declarer = await ProcessAsync(work => work.AddJobDefinition(definition).AddRecurring(definition, new TestArgs("r"), Schedule.Every(Hourly, Interval.FixedRate)));
        var worker = await WorkerAsync(definition, new JobScript());
        Assert.Empty(await UndeclaredAsync(worker, definition));

        await declarer.StopAsync(Cancel);
        var undeclared = Assert.Single(await UndeclaredAsync(worker, definition));
        Assert.Equal(1, undeclared.DeclaredRevision);
        Assert.NotNull(undeclared.NextAt);

        var store = worker.Services.GetRequiredService<IWorkStore>();
        var crashed = OwnerId.New();
        await store.Processes.RegisterAsync(new ProcessRegistration(crashed, "api", null, TimeSpan.FromSeconds(1)), Cancel);
        await store.Jobs.RegisterProcessAsync(crashed, [new ProcessDefinition(definition.Key, 1, false, [], true)], Cancel);
        Assert.Empty(await UndeclaredAsync(worker, definition));
        await EventuallyAsync(async () => (await UndeclaredAsync(worker, definition)).Count == 1, TimeSpan.FromSeconds(5), "the crashed declarer's heartbeat becomes stale");

        await InTransactionAsync(t => Jobs(worker).RemoveAsync(t, definition, Cancel));
        Assert.Empty(await UndeclaredAsync(worker, definition));
    }

    [Fact]
    public async Task J20_AProcessWithAnOlderRevisionIsReportedOutdated()
    {
        var older = Definition();
        var newer = Definition(revision: 2, key: older.Key);
        var schedule = Schedule.Every(Hourly, Interval.FixedRate);
        var current = await ProcessAsync(work => work.AddJobDefinition(newer).AddRecurring(newer, new TestArgs("new"), schedule).Process(p => p.Release("2.0")));
        var outdated = await ProcessAsync(work => work.AddJobDefinition(older).AddRecurring(older, new TestArgs("old"), schedule).Process(p => p.Release("1.0")));

        var report = Assert.Single(await OutdatedAsync(current, older));
        Assert.Equal((outdated.Owner, "1.0", 1L, 2L), (report.Process.Owner, report.Process.Release, report.Revision, report.StoredRevision));
        Assert.Empty(await UndeclaredAsync(current, older));

        await outdated.StopAsync(Cancel);
        Assert.Empty(await OutdatedAsync(current, older));
    }

    [Fact]
    public async Task J32_DueWorkOfADefinitionNoLiveProcessRunsIsReportedUnserved()
    {
        var definition = Definition(subject: "feed");
        var recurring = Definition();
        var paused = Definition();
        var subjectPaused = Definition(subject: "feed");
        var later = Definition();
        var requester = await ClientAsync(null, definition, recurring, paused, subjectPaused, later);
        await RequestAsync(requester, definition, "x", options: For(Subject.Of("feed", "a")));
        await ScheduleAsync(requester, recurring, "r", Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate), RecurrenceOptions.Default with { MergeKey = "r" });
        await RequestAsync(requester, paused, "x");
        await InTransactionAsync(t => Jobs(requester).PauseAsync(t, paused, Cancel));
        await RequestAsync(requester, subjectPaused, "x", options: For(Subject.Of("feed", "b")));
        await InTransactionAsync(t => Jobs(requester).PauseAsync(t, subjectPaused, Subject.Of("feed", "b"), Cancel));
        await RequestAsync(requester, later, "x", Schedule.After(Hourly));
        await Task.Delay(1500, Cancel);

        var report = await UnservedAsync(requester, definition, recurring, paused, subjectPaused, later);
        Assert.Equal(new[] { definition.Key, recurring.Key }.Order(StringComparer.Ordinal), report.Definitions.Select(d => d.Definition));
        Assert.Empty(report.Versions);

        await WorkerAsync(definition, new JobScript());
        await WorkerAsync(recurring, new JobScript());
        Assert.Empty((await UnservedAsync(requester, definition, recurring)).Definitions);
    }

    [Fact]
    public async Task J32_DueWorkWithAStalePauseFlagOfADefinitionNoLiveProcessRunsIsReportedUnserved()
    {
        var requested = Definition(subject: "feed");
        var recurring = Definition(subject: "feed");
        var paused = Definition(subject: "feed");
        var requester = await ClientAsync(null, requested, recurring, paused);
        var a = Subject.Of("feed", "a");
        var b = Subject.Of("feed", "b");
        await RaceResumeAsync(requester, requested, a, t => Jobs(requester).RequestAsync(t, requested, new TestArgs("x"), Schedule.Now, For(a), Cancel).AsTask());
        await RaceResumeAsync(requester, recurring, a, t => Jobs(requester).ScheduleAsync(t, recurring, new TestArgs("r"), Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate), Recurring(a), Cancel).AsTask());
        await InTransactionAsync(t => Jobs(requester).PauseAsync(t, paused, b, Cancel));
        await RequestAsync(requester, paused, "x", options: For(b));
        await ScheduleAsync(requester, paused, "r", Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate), Recurring(b));
        await Task.Delay(1500, Cancel);

        var expected = new[] { requested.Key, recurring.Key }.Order(StringComparer.Ordinal);
        Assert.Equal(expected, (await UnservedAsync(requester, requested, recurring, paused)).Definitions.Select(d => d.Definition));

        foreach (var definition in new[] { requested, recurring, paused })
        {
            var (store, source) = await JobSourceAsync(requester, definition, TimeSpan.FromSeconds(30));
            await store.Jobs.RepairPausesAsync(source, 100, Cancel);
        }
        Assert.Equal(expected, (await UnservedAsync(requester, requested, recurring, paused)).Definitions.Select(d => d.Definition));
    }

    [Fact]
    public async Task J32_AnExpiredLeaseOfADefinitionNoLiveProcessRunsIsReportedUnserved()
    {
        var definition = Definition();
        var requester = await ClientAsync(null, definition);
        await RequestAsync(requester, definition, "x");
        await CrashClaimAsync(requester, definition, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5));
        Assert.Empty((await UnservedAsync(requester, definition)).Definitions);

        await EventuallyAsync(async () => (await UnservedAsync(requester, definition)).Definitions.Count == 1, TimeSpan.FromSeconds(5), "the expired lease is reported");
    }

    [Fact]
    public async Task J32_WithAGraceOnlyWorkDueForItAndUnservedByAnyProcessLiveSinceIsReported()
    {
        var grace = TimeSpan.FromSeconds(2);
        var definition = Definition();
        var started = new TaskCompletionSource();
        var worker = await WorkerAsync(definition, new JobScript
        {
            Run = async (_, _, cancellationToken) =>
            {
                started.TrySetResult();
                await Task.Delay(Timeout.Infinite, cancellationToken);
                return JobResult.Completed;
            },
        }, configure: j => j.Capacity(1));
        await RequestAsync(worker, definition, "running");
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);
        await RequestAsync(worker, definition, "waiting");
        await Task.Delay(grace + TimeSpan.FromMilliseconds(500), Cancel);
        Assert.Empty((await UnservedAsync(worker, grace, definition)).Definitions);

        // The waiting job was due for longer than the grace, and the worker stopped within it, so it still counts.
        await worker.StopAsync(Cancel);
        Assert.Equal([new UnservedDefinition(definition.Key)], (await UnservedAsync(worker, TimeSpan.Zero, definition)).Definitions);
        Assert.Empty((await UnservedAsync(worker, grace, definition)).Definitions);
        await EventuallyAsync(async () => (await UnservedAsync(worker, grace, definition)).Definitions.Count == 1, TimeSpan.FromSeconds(10), "the grace passes since the worker stopped");
    }

    [Fact]
    public async Task J35_JobsOfAVersionNoLiveProcessAcceptsAreReportedUnservedForTheVersion()
    {
        var oldDefinition = Definition();
        var newDefinition = Definition(argsVersion: 2, key: oldDefinition.Key);
        var oldAttempts = new ConcurrentQueue<ObservedAttempt>();
        await WorkerAsync(oldDefinition, Recording(oldAttempts));
        var client = await ClientAsync(null, newDefinition);
        var job = await RequestAsync(client, newDefinition, "v2");

        var report = await UnservedAsync(client, newDefinition);
        Assert.Empty(report.Definitions);
        Assert.Equal([new UnservedVersion(newDefinition.Key, 2)], report.Versions);

        await WorkerAsync(newDefinition, new JobScript(), configure: j => j.AcceptsArgs(1, 2));
        await EndedAsync(job.Id, TimeSpan.FromSeconds(10));
        Assert.Empty((await UnservedAsync(client, newDefinition)).Versions);
        Assert.Empty(oldAttempts);
    }

    [Fact]
    public async Task J35_AJobWithAStalePauseFlagOfAVersionNoLiveProcessAcceptsIsReportedOnceTheRunnerRepairsTheFlag()
    {
        var oldDefinition = Definition(subject: "feed");
        var newDefinition = Definition(subject: "feed", argsVersion: 2, key: oldDefinition.Key);
        var oldAttempts = new ConcurrentQueue<ObservedAttempt>();
        await WorkerAsync(oldDefinition, Recording(oldAttempts), pollingInterval: TimeSpan.FromMilliseconds(250));
        var client = await ClientAsync(null, newDefinition);
        var a = Subject.Of("feed", "a");
        await RaceResumeAsync(client, newDefinition, a, t => Jobs(client).RequestAsync(t, newDefinition, new TestArgs("v2"), Schedule.Now, For(a), Cancel).AsTask());

        await EventuallyAsync(async () => (await UnservedAsync(client, newDefinition)).Versions.Count == 1, TimeSpan.FromSeconds(5), "the runner's maintenance repairs the flag");
        Assert.Equal([new UnservedVersion(newDefinition.Key, 2)], (await UnservedAsync(client, newDefinition)).Versions);
        Assert.Empty(oldAttempts);
    }

    /// <summary>
    /// Runs <paramref name="write"/> in a transaction that commits after <paramref name="subject"/>, paused before it,
    /// is resumed in another transaction, so the written row keeps a pause flag no stored pause backs.
    /// </summary>
    private async Task RaceResumeAsync(TestProcess process, JobDefinition definition, Subject subject, Func<WorkTransaction, Task> write)
    {
        RequireConcurrentTransactions();
        await InTransactionAsync(t => Jobs(process).PauseAsync(t, definition, subject, Cancel));
        await using var writing = await BeginAsync();
        await write(writing.Work);
        await InTransactionAsync(t => Jobs(process).ResumeAsync(t, definition, subject, Cancel));
        await writing.CommitAsync(Cancel);
    }

    private static Task<UnservedReport> UnservedAsync(TestProcess process, params JobDefinition[] definitions) =>
        UnservedAsync(process, TimeSpan.Zero, definitions);

    private static async Task<UnservedReport> UnservedAsync(TestProcess process, TimeSpan grace, params JobDefinition[] definitions)
    {
        var keys = definitions.Select(d => d.Key).ToHashSet(StringComparer.Ordinal);
        var report = grace == TimeSpan.Zero ? await Jobs(process).GetUnservedAsync(Cancel) : await Jobs(process).GetUnservedAsync(grace, Cancel);
        return new UnservedReport([.. report.Definitions.Where(d => keys.Contains(d.Definition))], [.. report.Versions.Where(v => keys.Contains(v.Definition))]);
    }

    private static async Task<IReadOnlyList<UndeclaredRecurrence>> UndeclaredAsync(TestProcess process, JobDefinition definition) =>
        [.. (await Jobs(process).GetUndeclaredRecurrencesAsync(Cancel)).Where(r => r.Definition == definition.Key)];

    private static async Task<IReadOnlyList<OutdatedProcess>> OutdatedAsync(TestProcess process, JobDefinition definition) =>
        [.. (await Jobs(process).GetOutdatedProcessesAsync(Cancel)).Where(r => r.Definition == definition.Key)];
}
