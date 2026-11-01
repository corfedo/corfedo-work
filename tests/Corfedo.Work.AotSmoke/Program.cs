using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text.Json.Serialization;
using Corfedo.Work;
using Corfedo.Work.InMemory;
using Corfedo.Work.PostgreSql;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;

// Builds a host that registers a lease source with a handler, a claim hook and an expiry handler, two reconciliation
// sources and a job definition in a dispatch pool, a job definition run elsewhere, a job definition with a
// code-defined cron recurrence within a calendar that uses a date set, a job definition with a result type and run
// history and expectations, a recurring chain with a group, the framework's retention job, monitoring, an incident
// handler and telemetry with partitions and exception details, described in the process registry and started manually,
// and resolves everything the host would start, without connecting to a database. It round-trips a result through its codec, a
// calendar through its rule and evaluates the calendar, builds a status, an outcome, a page of run history and a chain,
// and counts a job outcome that in-process listeners observe. A second host only registers its process. A third host
// stores work in an in-memory database on a controllable clock: it claims a record and loses the lease when the clock
// advances past its duration. A fourth host runs work on that database as another application: it requests a job and a
// chain of two steps and a job with a result, serialized through the source-generated context it registers with
// UseJson, and its handlers complete them. A fifth host only registers signals over PostgreSQL: it subscribes to a topic
// and stops listening when the subscription is disposed. A sixth host registers a definition whose argument type its
// context lacks and fails when its registry is built.
var partitions = new List<object?>();
using var meters = new MeterListener
{
    InstrumentPublished = (instrument, listener) =>
    {
        if (instrument.Meter.Name == "Corfedo.Work")
        {
            listener.EnableMeasurementEvents(instrument);
        }
    },
};
meters.SetMeasurementEventCallback<long>((instrument, _, tags, _) =>
{
    foreach (var tag in tags)
    {
        if (instrument.Name == "corfedo.work.job.outcomes" && tag.Key == "corfedo.work.partition")
        {
            partitions.Add(tag.Value);
        }
    }
});
meters.Start();
var sources = new List<string>();
using var spans = new ActivityListener
{
    ShouldListenTo = source =>
    {
        sources.Add(source.Name);
        return source.Name == "Corfedo.Work";
    },
    Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
};
ActivitySource.AddActivityListener(spans);
var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
await using var dataSource = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder { Host = "smoke.invalid" }.ConnectionString);
builder.Services.AddPostgreSqlWorkStore(dataSource);
builder.Services.AddWork(work => work
    .StartMode(StartMode.Manual)
    .UseJson(SmokeJson.Default)
    .Process(process => process.Type("feeds-worker").Release("1.0.0"))
    .AddLeaseHandler<InstanceKey, InstanceExecutor>(Sources.Instances, handler => handler
        .Capacity(4)
        .OnClaimed<InstanceClaimed>()
        .OnExpired<InstanceExpired>())
    .AddPool("convergence", capacity: 8)
    .AddReconciler<InstanceKey, PlacementReconciler>(Sources.Placements, reconciler => reconciler.Pool("convergence"))
    .AddReconciler<InstanceKey, PlacementReconciler>(Sources.Quotas, reconciler => reconciler.Pool("convergence"))
    .AddJob<SyncFeed, FeedSyncHandler>(Sources.FeedSync, job => job.Pool("convergence").AcceptsArgs(1, 2).OnEnded<FeedSyncEnded>())
    .AddJobDefinition(Sources.Cleanup)
    .AddJob<SyncFeed, FeedPruneHandler>(Sources.Prune)
    .AddCalendar(Sources.BusinessDays)
    .AddRecurring(Sources.Prune, new SyncFeed("all"), Schedule.Cron("0 3 * * *", "Europe/Warsaw").Within(Sources.BusinessDays.Name, closed: Closed.Skip), new RecurrenceOptions { Missed = Missed.RunLatest })
    .AddJob<SyncFeed, FeedReport, FeedReportHandler>(Sources.Report)
    .AddRecurringChain(Sources.Nightly, new SyncFeed("all"), Schedule.Cron("0 1 * * *", "UTC"), chain => chain
        .StartWith(Sources.Prune, new SyncFeed("all"))
        .ThenGroup(group => group.Add(Sources.Report, new SyncFeed("npm"), new JobRequestOptions { Subject = Subject.Of("feed", "npm") }))
        .AfterAllSettled(Sources.Cleanup, new SyncFeed("all")))
    .AddRetention()
    .AddMonitoring()
    .OnIncident<IncidentNotifier>()
    .Telemetry(telemetry => telemetry.Partition().ExceptionDetails()));
using var host = builder.Build();

var processBuilder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
processBuilder.Services.AddPostgreSqlWorkStore(dataSource);
processBuilder.Services.AddWorkProcess(process => process.Type("control-plane").Release("1.5.0").Heartbeat(TimeSpan.FromSeconds(10)).StaleAfter(TimeSpan.FromSeconds(40)));
using var processHost = processBuilder.Build();
processHost.Services.GetRequiredService<ProcessRegistry>();
var processHosted = processHost.Services.GetServices<IHostedService>().ToArray();

var operations = host.Services.GetRequiredService<LeaseOperations>();
var registry = host.Services.GetRequiredService<LeaseRegistry>();
var hosted = host.Services.GetServices<IHostedService>().ToArray();
var workHost = host.Services.GetRequiredService<IWorkHost>();
var newerResult = new JobResultVersionException(Sources.Report.Key, 2, 1);
var lease = new Lease<InstanceKey>(Sources.Instances, new InstanceKey("tenant", 7), operations.Owner, 3);
var restored = Sources.Instances.ParseToken(lease.Token);
var callbacks = registry.Callbacks(Sources.Instances);
using var scope = host.Services.CreateScope();
scope.ServiceProvider.GetRequiredService<InstanceExecutor>();
scope.ServiceProvider.GetRequiredService<PlacementReconciler>();
scope.ServiceProvider.GetRequiredService<FeedSyncHandler>();
scope.ServiceProvider.GetRequiredService<FeedSyncEnded>();
scope.ServiceProvider.GetRequiredService<FeedPruneHandler>();
scope.ServiceProvider.GetRequiredService<FeedReportHandler>();
var incidentHandlers = scope.ServiceProvider.GetServices<IWorkIncidentHandler>().ToArray();
host.Services.GetRequiredService<IncidentOperations>();
host.Services.GetRequiredService<ReconcileOperations>();
host.Services.GetRequiredService<JobOperations>();
host.Services.GetRequiredService<ProcessRegistry>();
host.Services.GetRequiredService<CalendarOperations>();
var jobs = host.Services.GetRequiredService<JobRegistry>();
var parsed = WorkCalendar.Parse(Sources.BusinessDays.Name, Sources.BusinessDays.Revision, Sources.BusinessDays.Rule);
var evaluator = new CalendarEvaluator([parsed], [new CalendarDates("public-holidays", 1, new HashSet<DateOnly> { new(2027, 1, 1) }, new DateOnly(2027, 12, 31))]);
var slot = evaluator.Next(parsed.Name, new DateTimeOffset(2026, 12, 31, 17, 30, 0, TimeSpan.Zero));
var feedCodec = JobArgs.Json(SmokeJson.Default.SyncFeed, version: 2);
var feed = feedCodec.Read(feedCodec.Write(new SyncFeed("npm")), 2);
var reportCodec = JobResults.Json(SmokeJson.Default.FeedReport);
var report = reportCodec.Read(reportCodec.Write(new FeedReport("npm", 3)), 1);
JobDisposition<FeedReport> returned = report;
JobDisposition<FeedReport> retried = JobResult.Retry(TimeSpan.Zero, "busy");
var job = JobId.New();
var outcome = new JobOutcomeStatus<FeedReport>(job, true, Guid.NewGuid(), JobOutcome.Completed, null, DateTimeOffset.UtcNow, true, report);
var status = new JobStatus(JobStatusState.Running, job, null, DateTimeOffset.UtcNow, null, 1, new JobProgress(40, "half"), JobOutcome.Completed, null, null, null, false, false);
var runs = new JobRunPage([new JobRunEntry(job, 2, Subject.Of("feed", "npm"), null, Guid.NewGuid(), 1, operations.Owner, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, JobRunOutcome.Retried, "busy")], null);
var declared = jobs.ProcessDefinitions();
var instance = new ProcessInstance(operations.Owner, "control-plane", "1.5.0", ProcessState.Live, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, null);
var chain = new ChainStatus(
    ChainId.New(), 1, job, null, true, JobOutcome.Completed, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow,
    [new ChainStepStatus(job, Sources.Prune.Key, 1, null, null, false, true, JobOutcome.Completed, null)],
    [new ChainGroupStatus(Guid.NewGuid(), 1, job.Value, null, 0, true, JobOutcome.Completed)]);
await jobs.Ended(jobs.Find(Sources.Prune))(new JobEnd(Sources.Prune.Key, job, null, "tenant", Guid.NewGuid(), JobOutcome.Failed, "broken", 1), null!, CancellationToken.None);
var clock = new InMemoryClock(new DateTimeOffset(2027, 1, 4, 8, 0, 0, TimeSpan.Zero));
var memory = new InMemoryWorkDatabase(clock);
var fields = LeaseFields.Default;
memory.CreateTable(Sources.Instances.Store, ["tenant_id", "id"], [
    fields.DueAt, fields.LeaseOwner, fields.LeaseEpoch, fields.LeaseExpiresAt, fields.Takeovers, fields.Failures, fields.SettledEpoch,
    fields.SettleFailures, fields.SettleRetryAt, fields.CancelRequestedAt, fields.ExecutionId, fields.EndedCompletionId, fields.EndedDigest]);
using (var insert = await memory.BeginAsync())
{
    insert.Insert(Sources.Instances.Store, new Dictionary<string, object?> { ["tenant_id"] = "tenant", ["id"] = 7L, [fields.DueAt] = clock.GetUtcNow() });
    insert.Commit();
}
var memoryBuilder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
memoryBuilder.Services.AddSingleton<TimeProvider>(clock);
memoryBuilder.Services.AddInMemoryWorkStore(memory);
memoryBuilder.Services.AddWork(work => work.AddLeaseSource(Sources.Instances));
using var memoryHost = memoryBuilder.Build();
var memoryOperations = memoryHost.Services.GetRequiredService<LeaseOperations>();
var memoryClaim = await memoryOperations.ClaimAsync(Sources.Instances, new InstanceKey("tenant", 7));
var fired = 0;
using var timer = clock.CreateTimer(_ => fired++, null, TimeSpan.FromMinutes(1), Timeout.InfiniteTimeSpan);
var renewed = memoryClaim is null ? RenewResult.Lost : await memoryOperations.RenewAsync(memoryClaim.Lease);
clock.Advance(TimeSpan.FromMinutes(3));
var expired = memoryClaim is null || (await memoryOperations.RenewAsync(memoryClaim.Lease)).Valid;
var jobsBuilder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
jobsBuilder.Services.AddSingleton<TimeProvider>(clock);
jobsBuilder.Services.AddInMemoryWorkStore(memory, "jobs");
jobsBuilder.Services.AddWork(work => work
    .UseJson(SmokeJson.Default)
    .AddJob<SyncFeed, FeedSyncHandler>(Sources.FeedSync, job => job.AcceptsArgs(1, 2))
    .AddJob<SyncFeed, FeedPruneHandler>(Sources.Prune)
    .AddJob<SyncFeed, FeedReport, FeedReportHandler>(Sources.Summary));
using var jobsHost = jobsBuilder.Build();
await jobsHost.StartAsync();
var jobOperations = jobsHost.Services.GetRequiredService<JobOperations>();
JobRequestResult requested;
using (var request = await memory.BeginAsync())
{
    requested = await jobOperations.RequestAsync(request, Sources.FeedSync, new SyncFeed("npm"), Schedule.Now, new JobRequestOptions { Subject = Subject.Of("feed", "npm"), Partition = "tenant" });
    request.Commit();
}
using var waiting = new CancellationTokenSource(TimeSpan.FromSeconds(30));
var completed = await jobOperations.WaitAsync(Sources.FeedSync, requested.Id, TimeSpan.FromSeconds(30), waiting.Token);
JobRequestResult summarized;
using (var request = await memory.BeginAsync())
{
    summarized = await jobOperations.RequestAsync(request, Sources.Summary, new SyncFeed("npm"), Schedule.Now);
    request.Commit();
}
await jobOperations.WaitAsync(Sources.Summary, summarized.Id, TimeSpan.FromSeconds(30), waiting.Token);
var summary = await jobOperations.GetOutcomeAsync(Sources.Summary, summarized.Id, waiting.Token);
ChainId requestedChain;
using (var request = await memory.BeginAsync())
{
    requestedChain = await jobOperations.RequestChainAsync(request, builder => builder.StartWith(Sources.FeedSync, new SyncFeed("pypi"), new JobRequestOptions { Subject = Subject.Of("feed", "pypi") }).Then(Sources.Prune, new SyncFeed("pypi")));
    request.Commit();
}
ChainStatus? ranChain = null;
while (ranChain?.Settled != true && !waiting.IsCancellationRequested)
{
    await Task.Delay(50);
    ranChain = await jobOperations.GetChainAsync(requestedChain);
}
await jobsHost.StopAsync();
var signalsBuilder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
signalsBuilder.Services.AddPostgreSqlSignals(dataSource, "fleet");
using var signalsHost = signalsBuilder.Build();
var signals = signalsHost.Services.GetRequiredService<IWorkSignals>();
signals.Subscribe(new SignalTopic("fleet.changed"), new FleetChanged()).Dispose();
var unlistedBuilder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
unlistedBuilder.Services.AddInMemoryWorkStore(memory, "unlisted");
unlistedBuilder.Services.AddWork(work => work.UseJson(SmokeJson.Default).AddJobDefinition(JobDefinition.Create<Unlisted>("feeds.unlisted")));
using var unlistedHost = unlistedBuilder.Build();
string? unlisted = null;
try
{
    unlistedHost.Services.GetRequiredService<JobRegistry>();
}
catch (InvalidOperationException exception)
{
    unlisted = exception.Message;
}
var execution = ReconcileExecution.Id(Sources.Placements.Name, "tenant/7", 2, 1);
if (restored != lease || hosted.Length != 1 || !ReferenceEquals(hosted[0], workHost) || newerResult.Readable != 1 || processHosted.Length != 1 || instance.State != ProcessState.Live
    || declared.Count != 6 || !declared.Single(d => d.Definition == Sources.Nightly.Key).DeclaresRecurrence || !declared.Single(d => d.Definition == Sources.Prune.Key).DeclaresRecurrence || !declared.Single(d => d.Definition == Sources.FeedSync.Key).AcceptedArgs.SequenceEqual([1, 2])
    || declared.Single(d => d.Definition == Sources.Cleanup.Key).Runs || callbacks.Claimed is null || callbacks.Expired is null
    || !registry.Sources.First(s => s.Name == Sources.Instances.Name).Settles
    || registry.Sources.Count(s => s.Pool == "convergence") != 3
    || jobs.Definitions.Count() != 6 || chain.Steps[0].Outcome != JobOutcome.Completed || jobs.Find(WorkRetention.Definition.Key) is null || report.Runs != 3 || !returned.HasValue || retried.HasValue || outcome.Result.Feed != "npm"
    || status.Progress?.Percent != 40 || runs.Runs[0].Outcome != JobRunOutcome.Retried || !Sources.Report.Policy.RunHistory || host.Services.GetServices<DeclaredRecurrence>().Count() != 3 || feed.Feed != "npm" || jobs.Ended(jobs.Find(Sources.FeedSync)) is null
    || execution == ReconcileExecution.Id(Sources.Placements.Name, "tenant/7", 3, 1) || (execution.ToByteArray(bigEndian: true)[6] >> 4) != 8
    || !parsed.Digest.AsSpan().SequenceEqual(Sources.BusinessDays.Digest) || jobs.Calendars.Count != 1 || host.Services.GetServices<IWorkBackground>().Count() != 7
    || Sources.Report.Policy.Expect?.Deadline?.Calendar != Sources.BusinessDays.Name || !partitions.SequenceEqual(["tenant"]) || !sources.Contains("Corfedo.Work") || incidentHandlers.Single() is not IncidentNotifier
    || memoryClaim?.Lease.Epoch != 1 || completed?.Outcome != JobOutcome.Completed || ranChain?.Outcome != JobOutcome.Completed || ranChain.Steps.Count != 2
    || summary?.Result.Feed != "npm" || summary.Result.Runs != 1 || unlisted?.Contains("[JsonSerializable(typeof(Unlisted))]", StringComparison.Ordinal) != true || !renewed.Valid || expired || fired != 1 || signalsHost.Services.GetServices<IHostedService>().Any()
    || slot.Kind != CalendarSlotKind.Allowed || slot.Start != new DateTimeOffset(2027, 1, 4, 7, 0, 0, TimeSpan.Zero) || slot.End != new DateTimeOffset(2027, 1, 4, 17, 0, 0, TimeSpan.Zero))
{
    Console.Error.WriteLine("smoke check failed");
    return 1;
}
Console.WriteLine("ok");
return 0;

internal static class Sources
{
    public static readonly LeaseSource<InstanceKey> Instances = LeaseSource.Define(
        "agent-instance",
        StoreLocation.Table("agents", "instances"),
        InstanceKey.Codec,
        new LeasePolicy
        {
            Duration = TimeSpan.FromMinutes(2),
            MaximumTakeovers = 3,
            TakeoverBackoff = Backoff.Exponential(TimeSpan.FromSeconds(5), TimeSpan.FromMinutes(5)),
            PartitionLimit = 8,
            SettleExpiry = true,
        },
        partition: "tenant_id");

    public static readonly ReconcileSource<InstanceKey> Placements = ReconcileSource.Define(
        "placement",
        StoreLocation.Table("agents", "placements"),
        InstanceKey.Codec,
        new ReconcilePolicy
        {
            Lease = new LeasePolicy { Duration = TimeSpan.FromMinutes(1), FailureBackoff = Backoff.Exponential(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(5)) },
            Requests = RequestPolicy.Supersede,
            ResyncInterval = TimeSpan.FromHours(1),
        },
        partition: "tenant_id");

    public static readonly ReconcileSource<InstanceKey> Quotas = ReconcileSource.Define(
        "quota",
        StoreLocation.Table("agents", "quotas"),
        InstanceKey.Codec,
        new ReconcilePolicy { Lease = new LeasePolicy { Duration = TimeSpan.FromMinutes(1) } });

    public static readonly JobDefinition<SyncFeed> FeedSync = JobDefinition.Create(
        "feeds.sync",
        3,
        JobArgs.Json(SmokeJson.Default.SyncFeed, version: 2),
        new JobPolicy
        {
            Attempts = 5,
            Backoff = Backoff.Exponential(TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(30)),
            PartitionLimit = 1,
            DefinitionLimit = 4,
            Priority = Priority.High,
            Timeout = TimeSpan.FromMinutes(20),
            Late = Late.SkipAfter(TimeSpan.FromMinutes(5)),
        },
        subject: "feed");

    public static readonly WorkCalendar BusinessDays = WorkCalendar.Create("business-days", 1)
        .InTimeZone("Europe/Warsaw")
        .AllowWeekdays(DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday)
        .AllowBetween(new TimeOnly(8, 0), new TimeOnly(18, 0))
        .ExcludeAnnual(month: 12, day: 24)
        .ExcludeDates("public-holidays");

    public static readonly JobDefinition<SyncFeed> Cleanup = JobDefinition.Create<SyncFeed>("feeds.cleanup");

    public static readonly JobDefinition<SyncFeed> Prune = JobDefinition.Create<SyncFeed>("feeds.prune");

    public static readonly JobDefinition<SyncFeed> Nightly = JobDefinition.Create<SyncFeed>("feeds.nightly", 1, new JobPolicy { Late = Late.SkipAfter(TimeSpan.FromHours(1)) });

    public static readonly JobDefinition<SyncFeed, FeedReport> Summary = JobDefinition.Create<SyncFeed, FeedReport>("feeds.summary");

    public static readonly JobDefinition<SyncFeed, FeedReport> Report = JobDefinition.Create<SyncFeed, FeedReport>(
        "feeds.report",
        1,
        new JobPolicy
        {
            RunHistory = true,
            Expect = Expect.CompletesBy(new TimeOnly(6, 0), "Europe/Warsaw", BusinessDays.Name).RunsAtMost(TimeSpan.FromHours(1)).FailsAtMostInARow(3),
        },
        subject: "feed");
}

internal sealed record SyncFeed(string Feed);

internal sealed record FeedReport(string Feed, int Runs);

internal sealed record Unlisted(string Name);

[JsonSerializable(typeof(SyncFeed))]
[JsonSerializable(typeof(FeedReport))]
internal sealed partial class SmokeJson : JsonSerializerContext;

internal sealed class FeedSyncHandler : IJobHandler<SyncFeed>
{
    public ValueTask<JobDisposition> RunAsync(SyncFeed args, JobRun run, CancellationToken cancellationToken) => ValueTask.FromResult<JobDisposition>(JobResult.Completed);
}

internal sealed class FeedPruneHandler : IJobHandler<SyncFeed>
{
    public ValueTask<JobDisposition> RunAsync(SyncFeed args, JobRun run, CancellationToken cancellationToken) => ValueTask.FromResult<JobDisposition>(JobResult.Completed);
}

internal sealed class FeedReportHandler : IJobHandler<SyncFeed, FeedReport>
{
    public async ValueTask<JobDisposition<FeedReport>> RunAsync(SyncFeed args, JobRun<FeedReport> run, CancellationToken cancellationToken)
    {
        await run.ReportProgressAsync(50, "counting", cancellationToken);
        return new FeedReport(args.Feed, run.Attempt);
    }
}

internal sealed class IncidentNotifier : IWorkIncidentHandler
{
    public ValueTask OpenedAsync(WorkIncident incident, CancellationToken cancellationToken) => ValueTask.CompletedTask;

    public ValueTask ResolvedAsync(WorkIncident incident, CancellationToken cancellationToken) => ValueTask.CompletedTask;
}

internal sealed class FeedSyncEnded : IJobEnded
{
    public ValueTask EndedAsync(JobEnd end, IWorkTransaction transaction, CancellationToken cancellationToken) => ValueTask.CompletedTask;
}

internal sealed record InstanceKey(string TenantId, long Id)
{
    public static IKeyCodec<InstanceKey> Codec { get; } = new KeyCodec();

    private sealed class KeyCodec : IKeyCodec<InstanceKey>
    {
        public IReadOnlyList<KeyField> Fields { get; } = [new("tenant_id", KeyFieldType.String), new("id", KeyFieldType.Int64)];

        public void Write(InstanceKey key, IKeyWriter writer)
        {
            writer.Write(key.TenantId);
            writer.Write(key.Id);
        }

        public InstanceKey Read(IKeyReader reader) => new(reader.ReadString(), reader.ReadInt64());

        public string Format(InstanceKey key) => key.TenantId + "/" + key.Id.ToString(CultureInfo.InvariantCulture);

        public InstanceKey Parse(string value)
        {
            var slash = value.LastIndexOf('/');
            return new(value[..slash], long.Parse(value.AsSpan(slash + 1), CultureInfo.InvariantCulture));
        }
    }
}

internal sealed class InstanceExecutor : ILeaseHandler<InstanceKey>
{
    public ValueTask RunAsync(LeaseRun<InstanceKey> run, CancellationToken cancellationToken) => ValueTask.CompletedTask;

    public ValueTask ExhaustedAsync(LeaseRun<InstanceKey> run, CancellationToken cancellationToken) => ValueTask.CompletedTask;
}

internal sealed class PlacementReconciler : IReconciler<InstanceKey>
{
    public ValueTask<ReconcileDisposition> ReconcileAsync(ReconcileRun<InstanceKey> run, CancellationToken cancellationToken) => ValueTask.FromResult<ReconcileDisposition>(ReconcileResult.Converged);
}

internal sealed class InstanceClaimed : ILeaseClaimHook<InstanceKey>
{
    public ValueTask ClaimedAsync(LeaseClaim<InstanceKey> claim, IWorkTransaction transaction, CancellationToken cancellationToken) => ValueTask.CompletedTask;
}

internal sealed class InstanceExpired : ILeaseExpiryHandler<InstanceKey>
{
    public ValueTask ExpiredAsync(LeaseExpiry<InstanceKey> expiry, IWorkTransaction transaction, CancellationToken cancellationToken) => ValueTask.CompletedTask;
}

internal sealed class FleetChanged : ISignalSubscriber
{
    public void Received(string key)
    {
    }

    public void Resync()
    {
    }
}
