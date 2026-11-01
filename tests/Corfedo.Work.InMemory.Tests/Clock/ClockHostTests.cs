using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json.Serialization;
using Corfedo.Work.Conformance;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Corfedo.Work.InMemory.Tests.Clock;

/// <summary>Lease expiry, retry backoffs, recurrences and timeouts of a host on an <see cref="InMemoryClock"/> wait for the clock.</summary>
public sealed class ClockHostTests : IAsyncDisposable
{
    private static readonly DateTimeOffset Start = new(2027, 1, 4, 6, 30, 0, TimeSpan.Zero);
    private static readonly TimeSpan Polling = TimeSpan.FromMinutes(1);
    private readonly InMemoryClock clock = new(Start);
    private readonly InMemoryWorkDatabase database;
    private readonly ConcurrentQueue<(string Definition, int Attempt, DateTimeOffset At)> runs = new();
    private readonly List<IHost> hosts = [];

    public ClockHostTests() => database = new InMemoryWorkDatabase(clock);

    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ALeaseExpiresWhenTheClockPassesItsDuration()
    {
        var table = StoreLocation.Table("app", "records");
        var f = LeaseFields.Default;
        database.CreateTable(table, ["tenant", "id"], [
            f.DueAt, f.LeaseOwner, f.LeaseEpoch, f.LeaseExpiresAt, f.Takeovers, f.Failures, f.SettledEpoch, f.SettleFailures,
            f.SettleRetryAt, f.CancelRequestedAt, f.ExecutionId, f.EndedCompletionId, f.EndedDigest]);
        using (var insert = await database.BeginAsync(Cancel))
        {
            insert.Insert(table, new Dictionary<string, object?> { ["tenant"] = "t", ["id"] = 1L, [f.DueAt] = clock.GetUtcNow() });
            insert.Commit();
        }
        var source = LeaseSource.Define("records", table, TestKey.Codec, new LeasePolicy { Duration = TimeSpan.FromMinutes(2) });
        var operations = (await HostAsync(work => work.AddLeaseSource(source), start: false)).Services.GetRequiredService<LeaseOperations>();

        var claim = Assert.IsType<LeaseClaim<TestKey>>(await operations.ClaimAsync(source, new TestKey("t", 1), Cancel));
        await Task.Delay(50, Cancel);
        clock.Advance(TimeSpan.FromMinutes(1));
        Assert.True((await operations.RenewAsync(claim.Lease, Cancel)).Valid);
        clock.Advance(TimeSpan.FromMinutes(2) - TimeSpan.FromSeconds(1));
        Assert.True((await operations.RenewAsync(claim.Lease, Cancel)).Valid);

        clock.Advance(TimeSpan.FromMinutes(2) + TimeSpan.FromSeconds(1));
        Assert.False((await operations.RenewAsync(claim.Lease, Cancel)).Valid);
    }

    [Fact]
    public async Task ARetryWaitsForTheClockToPassItsBackoff()
    {
        var definition = JobDefinition.Create("clock.retry", 1, JobArgs.Json(ClockJson.Default.ClockArgs), new JobPolicy { Attempts = 2, Backoff = Backoff.None, RunHistory = true });
        var backoff = TimeSpan.FromMinutes(10);
        var host = await HostAsync(work => work.AddJob<ClockArgs, RecordingHandler>(definition, job => job.Lease(TimeSpan.FromHours(1))));
        var jobs = host.Services.GetRequiredService<JobOperations>();
        var job = await RequestAsync(jobs, definition);

        await EventuallyAsync(async () => (await jobs.GetRunsAsync(definition, job, cancellationToken: Cancel)).Runs.Count == 1, "the first attempt is recorded");
        var first = Assert.Single((await jobs.GetRunsAsync(definition, job, cancellationToken: Cancel)).Runs);
        Assert.Equal((JobRunOutcome.Retried, "busy", Start), (first.Outcome, first.Reason, first.StartedAt));
        await Task.Delay(200, Cancel);
        clock.Advance(backoff - TimeSpan.FromSeconds(1));
        await Task.Delay(200, Cancel);
        Assert.Single(runs);

        await AdvanceUntilAsync(Start + backoff, async () => (await jobs.GetOutcomeAsync(definition, job, Cancel))?.Ended == true, "the retry ends the job");
        Assert.Equal(JobOutcome.Completed, (await jobs.GetOutcomeAsync(definition, job, Cancel))?.Outcome);
        Assert.Equal([1, 2], runs.Select(r => r.Attempt));
        Assert.InRange(runs.Last().At, Start + backoff, Start + backoff + (Polling * 10));
    }

    [Fact]
    public async Task ARecurrenceRunsEachOccurrenceOnlyWhenTheClockReachesIt()
    {
        var definition = JobDefinition.Create("clock.hourly", 1, JobArgs.Json(ClockJson.Default.ClockArgs), new JobPolicy { Attempts = 1 });
        await HostAsync(work => work
            .AddJob<ClockArgs, RecordingHandler>(definition, job => job.Lease(TimeSpan.FromHours(2)))
            .AddRecurring(definition, new ClockArgs(0), Schedule.Every(TimeSpan.FromHours(1), Interval.FixedRate)));
        var seven = new DateTimeOffset(2027, 1, 4, 7, 0, 0, TimeSpan.Zero);

        await Task.Delay(200, Cancel);
        clock.Advance(seven - TimeSpan.FromSeconds(1) - clock.GetUtcNow());
        await Task.Delay(200, Cancel);
        Assert.Empty(runs);

        await AdvanceUntilAsync(seven, () => Task.FromResult(runs.Count == 1), "the 07:00 occurrence runs");
        await Task.Delay(200, Cancel);
        Assert.Single(runs);
        await AdvanceUntilAsync(seven.AddHours(1), () => Task.FromResult(runs.Count == 2), "the 08:00 occurrence runs");

        var at = runs.Select(r => r.At).ToArray();
        Assert.InRange(at[0], seven, seven + (Polling * 10));
        Assert.InRange(at[1], seven.AddHours(1), seven.AddHours(1) + (Polling * 10));
    }

    [Fact]
    public async Task AnAttemptTimesOutOnlyWhenTheClockPassesItsTimeout()
    {
        var timeout = TimeSpan.FromMinutes(5);
        var definition = JobDefinition.Create("clock.timeout", 1, JobArgs.Json(ClockJson.Default.ClockArgs), new JobPolicy { Attempts = 1, Timeout = timeout });
        var host = await HostAsync(work => work.AddJob<ClockArgs, RecordingHandler>(definition, job => job.Lease(TimeSpan.FromHours(1))));
        var jobs = host.Services.GetRequiredService<JobOperations>();
        var job = await RequestAsync(jobs, definition, new ClockArgs(Hang: 1));

        await EventuallyAsync(() => Task.FromResult(!runs.IsEmpty), "the attempt starts");
        await Task.Delay(200, Cancel);
        clock.Advance(timeout - TimeSpan.FromSeconds(1));
        await Task.Delay(200, Cancel);
        Assert.False((await jobs.GetOutcomeAsync(definition, job, Cancel))?.Ended);

        await AdvanceUntilAsync(Start + timeout, async () => (await jobs.GetOutcomeAsync(definition, job, Cancel))?.Ended == true, "the attempt times out");
        var outcome = await jobs.GetOutcomeAsync(definition, job, Cancel);
        Assert.Equal(JobOutcome.TimedOut, outcome?.Outcome);
        Assert.InRange(outcome!.EndedAt!.Value, Start + timeout, Start + timeout + (Polling * 10));
    }

    private async Task<JobId> RequestAsync(JobOperations jobs, JobDefinition<ClockArgs> definition, ClockArgs? args = null)
    {
        using var transaction = await database.BeginAsync(Cancel);
        var requested = await jobs.RequestAsync(transaction, definition, args ?? new ClockArgs(0), Schedule.Now, cancellationToken: Cancel);
        transaction.Commit();
        return requested.Id;
    }

    private async Task<IHost> HostAsync(Action<WorkBuilder> configure, bool start = true)
    {
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Services.AddSingleton<TimeProvider>(clock);
        builder.Services.AddSingleton(runs);
        builder.Services.AddInMemoryWorkStore(database);
        builder.Services.AddWork(work => configure(work.PollingInterval(Polling)));
        var host = builder.Build();
        hosts.Add(host);
        if (start)
        {
            await host.StartAsync(Cancel);
        }
        return host;
    }

    /// <summary>
    /// Advances the clock to <paramref name="at"/>, then by the polling interval until <paramref name="condition"/>
    /// holds: a background loop arms its next timer from the clock's time when it gets to it, which may be after an
    /// advance returned, and every loop wakes at least once per polling interval. A dispatcher measures a claim's
    /// validity from before its claim query, so the tests' leases outlast their largest jump.
    /// </summary>
    private async Task AdvanceUntilAsync(DateTimeOffset at, Func<Task<bool>> condition, string because)
    {
        clock.Advance(at - clock.GetUtcNow());
        for (var steps = 0; !await HoldsWithinAsync(condition, TimeSpan.FromSeconds(1)); steps++)
        {
            Assert.True(steps < 10, $"Not within ten polling intervals after {at:O}: {because}.");
            clock.Advance(Polling);
        }
    }

    private static async Task EventuallyAsync(Func<Task<bool>> condition, string because) =>
        Assert.True(await HoldsWithinAsync(condition, TimeSpan.FromSeconds(10)), because);

    private static async Task<bool> HoldsWithinAsync(Func<Task<bool>> condition, TimeSpan wait)
    {
        var waited = Stopwatch.StartNew();
        while (!await condition())
        {
            if (waited.Elapsed > wait)
            {
                return false;
            }
            await Task.Delay(20, Cancel);
        }
        return true;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var host in hosts)
        {
            await host.StopAsync(CancellationToken.None);
            host.Dispose();
        }
    }
}

internal sealed record ClockArgs(int Hang);

[JsonSerializable(typeof(ClockArgs))]
internal sealed partial class ClockJson : JsonSerializerContext;

/// <summary>Records each attempt with the clock's time; the first attempt retries, and arguments with a hang wait for cancellation.</summary>
internal sealed class RecordingHandler(ConcurrentQueue<(string Definition, int Attempt, DateTimeOffset At)> runs, TimeProvider clock) : IJobHandler<ClockArgs>
{
    public async ValueTask<JobDisposition> RunAsync(ClockArgs args, JobRun run, CancellationToken cancellationToken)
    {
        runs.Enqueue((run.Definition.Key, run.Attempt, clock.GetUtcNow()));
        if (args.Hang > 0)
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        }
        return run.Definition.Key == "clock.retry" && run.Attempt == 1 ? JobResult.Retry(TimeSpan.FromMinutes(10), "busy") : JobResult.Completed;
    }
}
