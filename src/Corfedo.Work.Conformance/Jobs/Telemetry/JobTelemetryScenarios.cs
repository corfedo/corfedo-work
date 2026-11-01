using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>
/// Spans, counters, histograms and gauges of the <c>Corfedo.Work</c> source and meter, observed with in-process
/// listeners and filtered by each scenario's definition keys and sources.
/// </summary>
public abstract class JobTelemetryScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    private static readonly TimeSpan Polling = TimeSpan.FromMilliseconds(250);

    [Fact]
    public async Task M4_RunsReportSpansCountersAndHistogramsAndLinkToTheRequestsTrace()
    {
        using var telemetry = new TelemetryCapture();
        var definition = Definition(new JobPolicy { Attempts = 1 }, subject: "feed");
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var script = new JobScript
        {
            Run = async (args, run, cancellationToken) =>
            {
                switch (args.Value)
                {
                    case "guarded":
                        await using (var transaction = await Provider.BeginAsync(cancellationToken))
                        {
                            await run.GuardAsync(transaction.Work, cancellationToken);
                            await transaction.CommitAsync(cancellationToken);
                        }
                        return JobResult.Completed;
                    case "blocked":
                        started.SetResult();
                        await release.Task.WaitAsync(cancellationToken);
                        return JobResult.Completed;
                    default:
                        return JobResult.Fail("broken");
                }
            },
        };
        var worker = await WorkerAsync(definition, script, configure: j => j.Capacity(2), pollingInterval: Polling);
        var key = definition.Key;
        var source = definition.SourceName;

        // A job requested inside a trace; its run links to the request's span.
        JobId traced;
        ActivityContext request;
        using (var activity = new Activity("request").SetIdFormat(ActivityIdFormat.W3C).Start())
        {
            traced = (await RequestAsync(worker, definition, "guarded", options: For(Subject.Of("feed", "traced")))).Id;
            request = activity.Context;
        }
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(traced, TimeSpan.FromSeconds(10))).State);
        var failed = (await RequestAsync(worker, definition, "fail", options: For(Subject.Of("feed", "failing")))).Id;
        Assert.Equal(JobOutcome.Failed, (await EndedAsync(failed, TimeSpan.FromSeconds(10))).State);

        // The running gauge counts the attempt that runs now.
        var blocked = (await RequestAsync(worker, definition, "blocked", options: For(Subject.Of("feed", "blocked")))).Id;
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);
        telemetry.RecordGauges();
        Assert.Equal(1, telemetry.Of("corfedo.work.running", "corfedo.work.source", source).Last().Value);
        release.SetResult();
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(blocked, TimeSpan.FromSeconds(10))).State);

        // Spans: claim passes, runs with the definition and the job, the traced run's link, its guard and completion.
        Assert.NotEmpty(telemetry.SpansOf("corfedo.work.claim", "corfedo.work.source", source));
        await EventuallyAsync(() => telemetry.SpansOf("corfedo.work.run", "corfedo.work.definition", key).Length == 3, TimeSpan.FromSeconds(5), "three run spans");
        var runs = telemetry.SpansOf("corfedo.work.run", "corfedo.work.definition", key);
        var tracedRun = Assert.Single(runs, s => s.GetTagItem("corfedo.work.job")?.ToString() == traced.ToString());
        var link = Assert.Single(tracedRun.Links);
        Assert.Equal((request.TraceId, request.SpanId), (link.Context.TraceId, link.Context.SpanId));
        Assert.NotEqual(request.TraceId, tracedRun.TraceId);
        Assert.Equal(source, tracedRun.GetTagItem("corfedo.work.source"));
        Assert.Single(telemetry.Spans, s => s.OperationName == "corfedo.work.guard" && s.ParentSpanId == tracedRun.SpanId);
        Assert.Contains(telemetry.Spans, s => s.OperationName == "corfedo.work.complete" && s.ParentSpanId == tracedRun.SpanId);
        var failedRun = Assert.Single(runs, s => s.GetTagItem("corfedo.work.job")?.ToString() == failed.ToString());
        Assert.Equal((ActivityStatusCode.Error, "broken", "broken"), (failedRun.Status, failedRun.GetTagItem("error.type"), failedRun.GetTagItem("corfedo.work.reason")));
        Assert.Equal(ActivityStatusCode.Unset, tracedRun.Status);

        // Counters and histograms carry the definition, outcomes and reason codes.
        Assert.Equal(3, telemetry.Of("corfedo.work.claims", "corfedo.work.source", source).Sum(m => m.Value));
        Assert.Empty(telemetry.Of("corfedo.work.takeovers", "corfedo.work.source", source));
        var outcomes = telemetry.Of("corfedo.work.job.outcomes", "corfedo.work.definition", key);
        Assert.Equal(
            [("completed", null, 2.0), ("failed", "broken", 1.0)],
            outcomes.GroupBy(m => (m.Tag("corfedo.work.outcome"), m.Tag("corfedo.work.reason"))).Select(g => (g.Key.Item1, g.Key.Item2, g.Sum(m => m.Value))).OrderBy(g => g.Item1));
        var durations = telemetry.Of("corfedo.work.job.duration", "corfedo.work.definition", key);
        Assert.Equal(["completed", "completed", "failed"], durations.Select(m => m.Tag("corfedo.work.outcome")).Order());
        Assert.All(durations, m => Assert.Equal("s", m.Unit));
        var lateness = telemetry.Of("corfedo.work.job.lateness", "corfedo.work.definition", key);
        Assert.Equal(3, lateness.Length);
        Assert.All(lateness, m => Assert.InRange(m.Value, 0, 10));
    }

    [Fact]
    public async Task M4_TakeoversSkippedOccurrencesAndIncidentsAreCountedAndMonitoringReportsGauges()
    {
        using var telemetry = new TelemetryCapture();
        var taken = Definition(new JobPolicy { Attempts = 3 });
        var skipping = Definition(subject: "feed");
        var watched = Definition(new JobPolicy { Attempts = 1, Expect = Expect.FailsAtMostInARow(0) }, subject: "feed");
        var waiting = Definition();

        // A job whose holder crashed is taken over.
        var client = await ClientAsync(null, taken, waiting);
        var takenJob = (await RequestAsync(client, taken, "x")).Id;
        await CrashClaimAsync(client, taken, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5));
        await RequestAsync(client, waiting, "x", Schedule.Now);
        var runs = new ConcurrentQueue<ObservedOccurrence>();
        var worker = await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(new JobScript());
                work.Services.AddSingleton(new EndedScript());
                work.AddJob<TestArgs, ScriptedJobHandler>(taken, job => job.Lease(TimeSpan.FromSeconds(2)));
                work.AddMonitoring();
            },
            pollingInterval: Polling);
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(takenJob, TimeSpan.FromSeconds(10))).State);
        Assert.Equal(1, telemetry.Of("corfedo.work.takeovers", "corfedo.work.source", taken.SourceName).Sum(m => m.Value));
        Assert.Empty(telemetry.Of("corfedo.work.job.lateness", "corfedo.work.definition", taken.Key));

        // Occurrences due while a long one runs are skipped.
        var occurrences = await WorkerAsync(skipping, Occurrences(runs, async (_, _, cancellationToken) =>
        {
            await Task.Delay(runs.IsEmpty ? 2500 : 0, cancellationToken);
            return JobResult.Completed;
        }), pollingInterval: Polling);
        await ScheduleAsync(occurrences, skipping, "x", Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate, await Provider.NowAsync(Cancel)), Recurring(Subject.Of("feed", "a"), Overlap.Skip));
        await RunsAsync(runs, 2, TimeSpan.FromSeconds(15));
        var skipped = (await ReadJobsAsync(skipping)).Count(j => j.State == JobOutcome.Skipped);
        Assert.NotEqual(0, skipped);
        Assert.Equal(skipped, telemetry.Of("corfedo.work.occurrences.skipped", "corfedo.work.definition", skipping.Key).Where(m => m.Tag("corfedo.work.reason") == JobReasons.Overlap).Sum(m => m.Value));
        Assert.Equal(skipped, telemetry.Of("corfedo.work.job.outcomes", "corfedo.work.definition", skipping.Key).Where(m => m.Tag("corfedo.work.outcome") == "skipped").Sum(m => m.Value));

        // A failure opens an incident, which the monitoring process reports as open, and a success resolves it.
        var failing = await WorkerAsync(watched, new JobScript
        {
            Run = (args, _, _) => ValueTask.FromResult<JobDisposition>(args.Value == "fail" ? JobResult.Fail("broken") : JobResult.Completed),
        }, pollingInterval: Polling);
        var subject = Subject.Of("feed", "a");
        await EndedAsync((await RequestAsync(failing, watched, "fail", options: For(subject))).Id, TimeSpan.FromSeconds(10));
        var opened = telemetry.Of("corfedo.work.incidents", "corfedo.work.definition", watched.Key);
        Assert.Equal(("fails_at_most_in_a_row", "opened", 1.0), (Assert.Single(opened).Tag("corfedo.work.kind"), opened[0].Tag("corfedo.work.event"), opened[0].Value));
        await EventuallyAsync(
            () =>
            {
                telemetry.RecordGauges();
                return telemetry.Of("corfedo.work.incidents.open", "corfedo.work.kind", "fails_at_most_in_a_row").Any(m => m.Value >= 1)
                    && telemetry.Of("corfedo.work.due.age", "corfedo.work.definition", waiting.Key).Any(m => m.Value > 0);
            },
            TimeSpan.FromSeconds(10),
            "the monitoring process reports open incidents and the age of the oldest due job");
        await EndedAsync((await RequestAsync(failing, watched, "ok", options: For(subject))).Id, TimeSpan.FromSeconds(10));
        Assert.Equal(["opened", "resolved"], telemetry.Of("corfedo.work.incidents", "corfedo.work.definition", watched.Key).Select(m => m.Tag("corfedo.work.event")));
        Assert.Equal("{incident}", opened[0].Unit);
        await worker.StopAsync(Cancel);
    }

    [Fact]
    public async Task H3_SpansAndFrameworkLogsDescribeExceptionsWithoutMessagesKeysOrArguments()
    {
        using var telemetry = new TelemetryCapture();
        var (worker, definition, failed) = await FailingTwiceAsync(details: false);

        var handlerFailed = Assert.Single(worker.Logs.Entries, e => e.Event == "job.handler_failed");
        Assert.Contains(definition.Key, handlerFailed.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(InvalidOperationException).FullName!, handlerFailed.Message, StringComparison.Ordinal);
        var transitionFailed = Assert.Single(worker.Logs.Entries, e => e.Event == "job.transition_failed");
        Assert.Contains(definition.Key, transitionFailed.Message, StringComparison.Ordinal);
        Assert.Contains("Exception", transitionFailed.Message, StringComparison.Ordinal);
        Assert.All(worker.Logs.Entries, e =>
        {
            Assert.Null(e.Exception);
            Assert.DoesNotContain(Secret, e.Message, StringComparison.Ordinal);
        });

        var runs = telemetry.SpansOf("corfedo.work.run", "corfedo.work.definition", definition.Key);
        var first = Assert.Single(runs, s => s.GetTagItem("corfedo.work.attempt") is 1);
        Assert.Equal((ActivityStatusCode.Error, typeof(InvalidOperationException).FullName, JobReasons.HandlerFailed), (first.Status, first.GetTagItem("error.type"), first.GetTagItem("corfedo.work.reason")));
        var violation = Assert.Single(telemetry.SpansOf("corfedo.work.complete", "corfedo.work.definition", definition.Key), s => s.Status == ActivityStatusCode.Error);
        Assert.Contains("Exception", (string)violation.GetTagItem("error.type")!, StringComparison.Ordinal);
        Assert.All(
            telemetry.Spans.Where(s => runs.Contains(s) || s.ParentSpanId == first.SpanId || runs.Any(r => r.SpanId == s.ParentSpanId)),
            s => Assert.All(s.TagObjects, t => Assert.DoesNotContain(Secret, t.Value?.ToString() ?? "", StringComparison.Ordinal)));
        Assert.Equal(JobOutcome.Completed, failed.State);
    }

    [Fact]
    public async Task H3_WithExceptionDetailsFrameworkLogsCarryTheException()
    {
        var (worker, _, _) = await FailingTwiceAsync(details: true);

        var handlerFailed = Assert.Single(worker.Logs.Entries, e => e.Event == "job.handler_failed");
        Assert.Contains(Secret, Assert.IsType<InvalidOperationException>(handlerFailed.Exception).Message, StringComparison.Ordinal);
        Assert.NotNull(Assert.Single(worker.Logs.Entries, e => e.Event == "job.transition_failed").Exception);
    }

    private const string Secret = "customer-4711@example.com";

    /// <summary>
    /// A job of a subject named by user data whose first attempt throws with the data in its message, and whose outcome
    /// hook then fails once on a unique violation of a domain key holding it; returns once the job completed.
    /// </summary>
    private async Task<(TestProcess Worker, JobDefinition<TestArgs> Definition, JobState Job)> FailingTwiceAsync(bool details)
    {
        var table = await Provider.CreateTableAsync([""], Cancel);
        var taken = new TestKey(Secret, 1);
        await Provider.InsertAsync(table, taken, null, Cancel);
        var definition = Definition(new JobPolicy { Attempts = 2, Backoff = Backoff.None }, subject: "customer");
        var hooks = 0;
        var ended = new EndedScript
        {
            Ended = (_, transaction, cancellationToken) => Interlocked.Increment(ref hooks) == 1
                ? new ValueTask(Provider.InsertInTransactionAsync(transaction, table, taken, cancellationToken))
                : ValueTask.CompletedTask,
        };
        var script = new JobScript
        {
            Run = (args, run, _) => run.Attempt == 1
                ? throw new InvalidOperationException($"Order of {args.Value} rejected")
                : ValueTask.FromResult<JobDisposition>(JobResult.Completed),
        };
        var worker = await WorkerAsync(definition, script, ended, also: work => work.Telemetry(t =>
        {
            if (details)
            {
                t.ExceptionDetails();
            }
        }), pollingInterval: Polling);
        var job = await RequestAsync(worker, definition, Secret, options: For(Subject.Of("customer", Secret)));
        return (worker, definition, await EndedAsync(job.Id, TimeSpan.FromSeconds(10)));
    }
}
