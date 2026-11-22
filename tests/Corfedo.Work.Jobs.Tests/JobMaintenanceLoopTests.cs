using System.Collections.Concurrent;
using System.Reflection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work.Jobs.Tests;

/// <summary>The maintenance loop runs the calls its probe finds, paces calls that change nothing and isolates failures.</summary>
public sealed class JobMaintenanceLoopTests
{
    public sealed record NameArgs(string Name);

    private static readonly TimeSpan Polling = TimeSpan.FromMilliseconds(400);

    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ACallThatChangesNothingWaitsForThePollingIntervalWhileOtherDefinitionsKeepTheProbeBusy()
    {
        var store = new MaintenanceStore();
        // Skipped work whose outcome hook keeps failing ends nothing; the busy definition plans at every pass.
        store.Due["m.stuck"] = new(1, false, false, true, false, false, null);
        store.Due["m.busy"] = new(1, true, false, false, false, false, null);
        store.Planned = key => key == "m.busy" ? new PlanningPass(1, 1, Revision: 1) : new PlanningPass(0, 0, Revision: 1);
        await using var loop = await StartAsync(store, "m.stuck", "m.busy");

        await WaitForAsync(() => store.Count("plan m.busy") >= 30, "the busy definition keeps the probe running");
        var finalized = store.Count("skipped m.stuck");
        Assert.InRange(finalized, 1, 3);

        // The definition's own planning signal releases the held call at once.
        loop.Loop.Received(Source("m.stuck"));
        await WaitForAsync(() => store.Count("skipped m.stuck") > finalized, "the signal releases the held call");
    }

    [Fact]
    public async Task ADefinitionWhoseSourceFailsIsLeftOutAndTheOthersAreMaintained()
    {
        var store = new MaintenanceStore { FailingDefinition = "m.broken" };
        store.Due["m.healthy"] = new(1, true, false, false, false, false, null);
        await using var loop = await StartAsync(store, "m.broken", "m.healthy");

        await WaitForAsync(() => store.Count("plan m.healthy") >= 1, "the healthy definition is planned");
        Assert.All(store.Probes, probed => Assert.Equal(["m.healthy"], probed));
    }

    [Fact]
    public async Task PlanningThatFindsTheDefinitionBeingStoredRunsAgainShortlyWithoutAnotherProbe()
    {
        var store = new MaintenanceStore();
        store.Due["m.storing"] = new(1, true, false, false, false, false, null);
        var busy = 2;
        store.Planned = _ => Interlocked.Decrement(ref busy) >= 0 ? new PlanningPass(0, 0, DefinitionBusy: true) : new PlanningPass(1, 0, Revision: 1);
        store.Once = true;
        await using var loop = await StartAsync(store, "m.storing");

        await WaitForAsync(() => store.Count("plan m.storing") == 3, "planning runs until the definition is stored");
        Assert.Equal(1, store.ProbesBefore("plan m.storing", 3));
    }

    [Fact]
    public async Task DrainingABacklogInBatchesWakesTheProbeOnceAtItsEnd()
    {
        var store = new MaintenanceStore();
        store.Due["m.backlog"] = new(1, false, true, false, false, false, null);
        var batches = 0;
        store.Calendar = _ => Interlocked.Increment(ref batches) <= 5 ? new CalendarPass(100, 0) : new CalendarPass(7, 0);
        store.Once = true;
        await using var loop = await StartAsync(store, "m.backlog", polling: TimeSpan.FromSeconds(30));

        await WaitForAsync(() => store.Count("calendar m.backlog") == 6, "the backlog drains");
        await WaitForAsync(() => store.Probes.Count == 2, "the probe reads the drained state");
        await Task.Delay(300, Cancel);
        Assert.Equal(2, store.Probes.Count);
    }

    private static string Source(string key) => JobDefinition.Create<NameArgs>(key).SourceName;

    private static async Task<RunningLoop> StartAsync(MaintenanceStore store, params string[] keys) => await StartAsync(store, keys, Polling);

    private static Task<RunningLoop> StartAsync(MaintenanceStore store, string key, TimeSpan polling) => StartAsync(store, [key], polling);

    private static Task<RunningLoop> StartAsync(MaintenanceStore store, string[] keys, TimeSpan polling)
    {
        var registrations = keys.Select(k => new JobDefinitionRegistration<NameArgs>(JobDefinition.Create<NameArgs>(k), [1], TimeSpan.FromSeconds(30), null)).ToArray();
        var registry = new JobRegistry(registrations, [], [], new NoServices());
        var loop = new JobMaintenanceLoop(registrations.Select(r => new JobMaintenance(r, registry)));
        loop.Start(new LeaseDispatchContext(new NoServices(), store.Work, null!, TimeProvider.System, NullLoggerFactory.Instance, polling, TimeSpan.FromMilliseconds(10)));
        return Task.FromResult(new RunningLoop(loop));
    }

    private static async Task WaitForAsync(Func<bool> condition, string what)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, $"not reached within 5 s: {what}");
            await Task.Delay(10, Cancel);
        }
    }

    /// <summary>Stops the loop when the test ends.</summary>
    private sealed class RunningLoop(JobMaintenanceLoop loop) : IAsyncDisposable
    {
        public JobMaintenanceLoop Loop { get; } = loop;

        public async ValueTask DisposeAsync() => await Loop.StopAsync();
    }

    private sealed class NoServices : IServiceProvider
    {
        public object? GetService(Type serviceType) => null;
    }

    /// <summary>
    /// Answers the probe with <see cref="Due"/> (only on its first read when <see cref="Once"/>), records every probe and
    /// maintenance call in order, and answers the calls with what the delegates return; every other call fails the test.
    /// </summary>
    private sealed class MaintenanceStore
    {
        private readonly ConcurrentQueue<string> calls = new();
        private int probes;

        public MaintenanceStore()
        {
            var jobs = DispatchProxy.Create<IJobStore, StoreProxy>();
            ((StoreProxy)(object)jobs).Handler = (method, args) => method.Name switch
            {
                "get_Table" => StoreLocation.Table("work", "jobs"),
                nameof(IJobStore.StoreDefinitionAsync) => Store((JobDefinition)args![0]!),
                nameof(IJobStore.MaintenanceDueAsync) => Probe((IReadOnlyCollection<LeaseSource<JobId>>)args![0]!),
                nameof(IJobStore.PlanAsync) => ValueTask.FromResult(Planned(Record("plan", args!))),
                nameof(IJobStore.PlanCalendarAsync) => ValueTask.FromResult(Calendar(Record("calendar", args!))),
                nameof(IJobStore.FinalizeSkippedAsync) => ValueTask.FromResult(Count(Record("skipped", args!), 0)),
                nameof(IJobStore.RetireAsync) => ValueTask.FromResult(Count(Record("retired", args!), 0)),
                nameof(IJobStore.RepairPausesAsync) => ValueTask.FromResult(Count(Record("pauses", args!), 0)),
                _ => throw new NotSupportedException(method.Name),
            };
            var work = DispatchProxy.Create<IWorkStore, StoreProxy>();
            ((StoreProxy)(object)work).Handler = (method, _) => method.Name == "get_Jobs" ? jobs : throw new NotSupportedException(method.Name);
            Work = work;
        }

        public IWorkStore Work { get; }

        public ConcurrentDictionary<string, JobMaintenanceDue> Due { get; } = new(StringComparer.Ordinal);

        public bool Once { get; set; }

        public string? FailingDefinition { get; init; }

        public Func<string, PlanningPass> Planned { get; set; } = _ => new PlanningPass(0, 0, Revision: 1);

        public Func<string, CalendarPass> Calendar { get; set; } = _ => new CalendarPass(0, 0);

        public ConcurrentQueue<string[]> Probes { get; } = new();

        public int Count(string call) => calls.Count(c => c == call);

        /// <summary>The probes recorded before the <paramref name="n"/>th <paramref name="call"/>.</summary>
        public int ProbesBefore(string call, int n)
        {
            var seen = 0;
            var before = 0;
            foreach (var c in calls)
            {
                if (c == "probe")
                {
                    before++;
                }
                else if (c == call && ++seen == n)
                {
                    return before;
                }
            }
            return before;
        }

        private static int Count(string _, int count) => count;

        private string Record(string call, object?[] args)
        {
            var key = ((LeaseSource<JobId>)args[0]!).Job!.Definition;
            calls.Enqueue($"{call} {key}");
            return key;
        }

        private ValueTask<StoredJobDefinition> Store(JobDefinition definition) => definition.Key == FailingDefinition
            ? throw new InvalidOperationException("stored with another policy")
            : ValueTask.FromResult(new StoredJobDefinition(definition.Key, 1, null, new JobPolicy(), [], Paused: false));

        private ValueTask<IReadOnlyDictionary<string, JobMaintenanceDue>> Probe(IReadOnlyCollection<LeaseSource<JobId>> sources)
        {
            calls.Enqueue("probe");
            var keys = sources.Select(s => s.Job!.Definition).ToArray();
            Probes.Enqueue(keys);
            var first = Interlocked.Increment(ref probes) == 1;
            IReadOnlyDictionary<string, JobMaintenanceDue> due = keys.ToDictionary(
                k => k,
                k => (first || !Once) && Due.TryGetValue(k, out var found) ? found : new JobMaintenanceDue(1, false, false, false, false, false, null),
                StringComparer.Ordinal);
            return ValueTask.FromResult(due);
        }
    }

    public class StoreProxy : DispatchProxy
    {
        public Func<MethodInfo, object?[]?, object?> Handler { get; set; } = (method, _) => throw new NotSupportedException(method.Name);

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) => Handler(targetMethod!, args);
    }
}
