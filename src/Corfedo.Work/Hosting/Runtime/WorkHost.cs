using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work;

/// <summary>
/// Starts the process's work: prepares the framework schema, runs the layers' initializers (job definitions are
/// stored by revision), verifies every registered source, registers the process with its job definitions when it
/// describes itself, runs a handler, declares a recurrence, registers a calendar or monitors, recording whether it
/// monitors, then subscribes to due and cancellation signals and starts the dispatch loop, the expiry loop and the layers' background
/// loops. A source with a handler runs in its pool, or in a pool of its own with its capacity; every pool claims on the
/// one dispatch loop and every settling source settles on the one expiry loop, so the number of loops does not grow
/// with the number of sources. With <see cref="StartMode.Manual"/> the host's start ends after verifying sources, and
/// <see cref="IWorkHost.StartAsync"/> does the rest. On stop, dispatchers release what they still hold; the process
/// heartbeats until then and is marked stopped last.
/// </summary>
internal sealed class WorkHost : IHostedService, IWorkHost
{
    private readonly IServiceProvider services;
    private readonly IWorkStore store;
    private readonly LeaseRegistry registry;
    private readonly OwnerId owner;
    private readonly LeaseDispatchContext context;
    private readonly Dictionary<string, DispatchPoolDefinition> pools;
    private readonly List<ILeaseDispatcher> started = [];
    private readonly Dictionary<string, ILeaseDispatcher> dispatchers = new(StringComparer.Ordinal);
    private readonly IWorkInitializer[] initializers;
    private readonly IWorkBackground[] backgrounds;
    private readonly JobRegistry jobs;
    private readonly ProcessOptions process;
    private readonly TimeProvider time;
    private readonly ILogger logger;
    private readonly StartMode mode;
    private readonly SemaphoreSlim starting = new(1, 1);
    private readonly IWorkSignals? signals;
    private readonly List<IDisposable> subscriptions = [];
    private HostState state;
    private ProcessHeartbeat? heartbeat;
    private PassLoop? dispatchLoop;
    private PassLoop? expiryLoop;

    public WorkHost(
        IServiceProvider services,
        IWorkStore store,
        LeaseRegistry registry,
        JobRegistry jobs,
        LeaseOperations operations,
        TimeProvider time,
        WorkHostOptions options,
        ProcessOptions process,
        IEnumerable<DispatchPoolDefinition> pools,
        IEnumerable<IWorkInitializer> initializers,
        IEnumerable<IWorkBackground> backgrounds)
    {
        this.initializers = [.. initializers];
        this.backgrounds = [.. backgrounds];
        this.pools = new Dictionary<string, DispatchPoolDefinition>(StringComparer.Ordinal);
        foreach (var pool in pools)
        {
            if (!this.pools.TryAdd(pool.Name, pool))
            {
                throw new InvalidOperationException($"Dispatch pool {pool.Name} is defined more than once.");
            }
        }
        this.services = services;
        this.store = store;
        this.registry = registry;
        this.jobs = jobs;
        this.time = time;
        this.process = process;
        owner = operations.Owner;
        mode = options.StartMode;
        var logging = (ILoggerFactory?)services.GetService(typeof(ILoggerFactory)) ?? NullLoggerFactory.Instance;
        logger = logging.CreateLogger("Corfedo.Work.Processes");
        signals = (IWorkSignals?)services.GetService(typeof(IWorkSignals));
        context = new LeaseDispatchContext(services, store, operations, time, logging, options.PollingInterval, options.ShutdownGrace, signals);
    }

    /// <summary>Running is set before dispatch starts, so a stop after a failed start stops whatever started.</summary>
    private enum HostState
    {
        Created,
        Initialized,
        Running,
        Stopped,
    }

    async Task IHostedService.StartAsync(CancellationToken cancellationToken)
    {
        await starting.WaitAsync(cancellationToken);
        try
        {
            await InitializeAsync(cancellationToken);
            state = HostState.Initialized;
            if (mode == StartMode.Automatic)
            {
                state = HostState.Running;
                await RunAsync(cancellationToken);
            }
        }
        finally
        {
            starting.Release();
        }
    }

    async Task IWorkHost.StartAsync(CancellationToken cancellationToken)
    {
        if (mode != StartMode.Manual)
        {
            throw new InvalidOperationException("The work host starts with its host; IWorkHost.StartAsync starts hosts registered with StartMode.Manual.");
        }
        await starting.WaitAsync(cancellationToken);
        try
        {
            switch (state)
            {
                case HostState.Created:
                    throw new InvalidOperationException("The host has not started; start it before IWorkHost.StartAsync.");
                case HostState.Stopped:
                    throw new InvalidOperationException("The host has stopped.");
                case HostState.Initialized:
                    state = HostState.Running;
                    await RunAsync(cancellationToken);
                    break;
            }
        }
        finally
        {
            starting.Release();
        }
    }

    /// <summary>Prepares the schema, runs the initializers and verifies every registered source.</summary>
    private async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await store.PrepareAsync(cancellationToken);
        foreach (var initializer in initializers)
        {
            await initializer.InitializeAsync(store, cancellationToken);
        }
        foreach (var registration in registry.Sources)
        {
            await registration.VerifyAsync(store, cancellationToken);
        }
    }

    /// <summary>
    /// Creates the pools on the dispatch loop and the expiry processors on the expiry loop, registers the process when it
    /// is recorded, then starts the signal listener, both loops and the background loops.
    /// </summary>
    private async Task RunAsync(CancellationToken cancellationToken)
    {
        dispatchLoop = new PassLoop("dispatch", context);
        expiryLoop = new PassLoop("expiry", context);
        var pooled = new Dictionary<string, List<LeaseSourceRegistration>>(StringComparer.Ordinal);
        foreach (var registration in registry.Sources.Where(r => r.HasHandler))
        {
            if (registration.Pool is not { } pool)
            {
                started.Add(new DispatchPool(registration.Name, registration.Capacity, [registration], context, dispatchLoop));
            }
            else if (!pools.ContainsKey(pool))
            {
                throw new InvalidOperationException($"Lease source {registration.Name} runs in pool {pool}, which is not defined; define it with AddPool.");
            }
            else
            {
                (pooled.TryGetValue(pool, out var members) ? members : pooled[pool] = []).Add(registration);
            }
        }
        foreach (var (pool, members) in pooled)
        {
            started.Add(new DispatchPool(pool, pools[pool].Capacity, members, context, dispatchLoop));
        }
        foreach (var dispatcher in started)
        {
            foreach (var source in dispatcher.Sources)
            {
                dispatchers[source] = dispatcher;
            }
        }
        foreach (var registration in registry.Sources)
        {
            if (registration.Settles)
            {
                var processor = registration.CreateExpiryProcessor(context);
                expiryLoop.Add(processor.Source, processor.SettleAsync);
            }
        }
        var monitors = services.GetService(typeof(MonitoringRegistration)) is not null;
        if (process.Configured || registry.Sources.Any(r => r.HasHandler) || jobs.DeclaresRecurrences || jobs.Calendars.Count > 0 || monitors)
        {
            var definitions = jobs.ProcessDefinitions();
            var calendars = jobs.Calendars;
            heartbeat = new ProcessHeartbeat(
                store,
                process.Registration(owner, services),
                process.Heartbeat,
                time,
                logger,
                WorkTelemetryOptions.From(services),
                registered: async token =>
                {
                    await store.Jobs.RegisterProcessAsync(owner, definitions, token);
                    await store.Calendars.RegisterProcessAsync(owner, calendars, token);
                    if (monitors)
                    {
                        await store.Monitoring.RegisterMonitorAsync(owner, token);
                    }
                });
            await heartbeat.StartAsync(cancellationToken);
        }
        if (signals is not null && dispatchers.Count > 0)
        {
            subscriptions.Add(signals.Subscribe(WorkTopics.Due, new Subscriber(Due, WakeAll)));
            subscriptions.Add(signals.Subscribe(WorkTopics.Cancel, new Subscriber(Cancel, static () => { })));
        }
        dispatchLoop.Start();
        expiryLoop.Start();
        foreach (var background in backgrounds)
        {
            background.Start(context);
        }
    }

    private void Due(string source)
    {
        if (dispatchers.TryGetValue(source, out var dispatcher))
        {
            dispatcher.Wake();
        }
    }

    /// <summary>Only the holder's process checks a cancellation request at once; others find it when they renew.</summary>
    private void Cancel(string key)
    {
        if (WorkTopics.TryParseCancelKey(key, out var holder, out var source) && holder == owner && dispatchers.TryGetValue(source, out var dispatcher))
        {
            dispatcher.CheckCancellation(source);
        }
    }

    /// <summary>Signals may have been missed: every dispatcher claims again.</summary>
    private void WakeAll()
    {
        foreach (var dispatcher in started)
        {
            dispatcher.Wake();
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        bool running;
        await starting.WaitAsync(cancellationToken);
        try
        {
            running = state == HostState.Running;
            state = HostState.Stopped;
        }
        finally
        {
            starting.Release();
        }
        if (!running)
        {
            return;
        }
        await Task.WhenAll(backgrounds.Select(b => b.StopAsync()));
        if (dispatchLoop is not null)
        {
            await dispatchLoop.StopAsync();
        }
        await Task.WhenAll(started.Select(d => d.StopAsync(cancellationToken)));
        if (expiryLoop is not null)
        {
            await expiryLoop.StopAsync();
        }
        foreach (var subscription in subscriptions)
        {
            subscription.Dispose();
        }
        subscriptions.Clear();
        if (heartbeat is not null)
        {
            await heartbeat.StopAsync(cancellationToken);
        }
    }

    private sealed class Subscriber(Action<string> received, Action resync) : ISignalSubscriber
    {
        public void Received(string key) => received(key);

        public void Resync() => resync();
    }
}
