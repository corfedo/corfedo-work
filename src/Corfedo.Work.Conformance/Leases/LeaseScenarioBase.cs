using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Corfedo.Work.Conformance;

/// <summary>Shared setup for lease scenarios. Every scenario creates its own table and source.</summary>
public abstract class LeaseScenarioBase(ILeaseConformanceProvider provider) : IAsyncDisposable
{
    private readonly List<TestProcess> processes = [];

    protected ILeaseConformanceProvider Provider { get; } = provider;

    protected static CancellationToken Cancel => TestContext.Current.CancellationToken;

    /// <summary>A lease policy with short durations, adjusted by <paramref name="configure"/>.</summary>
    protected static LeasePolicy Policy(TimeSpan duration, Func<LeasePolicy, LeasePolicy>? configure = null)
    {
        var policy = new LeasePolicy { Duration = duration };
        return configure is null ? policy : configure(policy);
    }

    /// <summary>A new table and a source on it.</summary>
    protected async Task<LeaseSource<TestKey>> SourceAsync(LeasePolicy policy, bool partitioned = false)
    {
        var table = await Provider.CreateTableAsync([""], Cancel);
        return LeaseSource.Define("s-" + table.Name, table, TestKey.Codec, policy, partitioned ? "tenant" : null);
    }

    protected static LeaseFields Fields(string prefix) => new()
    {
        DueAt = prefix + "due_at",
        LeaseOwner = prefix + "lease_owner",
        LeaseEpoch = prefix + "lease_epoch",
        LeaseExpiresAt = prefix + "lease_expires_at",
        Takeovers = prefix + "takeovers",
        Failures = prefix + "failures",
        SettledEpoch = prefix + "settled_epoch",
        SettleFailures = prefix + "settle_failures",
        SettleRetryAt = prefix + "settle_retry_at",
        CancelRequestedAt = prefix + "cancel_requested_at",
        ExecutionId = prefix + "execution_id",
        EndedCompletionId = prefix + "ended_completion_id",
        EndedDigest = prefix + "ended_digest",
    };

    protected Task InsertAsync(LeaseSource<TestKey> source, TestKey key, TimeSpan? dueIn) => Provider.InsertAsync(source.Store, key, dueIn, Cancel);

    protected async Task<TestKey[]> InsertManyAsync(LeaseSource<TestKey> source, int count, string tenant = "t", TimeSpan? dueIn = null)
    {
        var keys = Enumerable.Range(1, count).Select(i => new TestKey(tenant, i)).ToArray();
        foreach (var key in keys)
        {
            await InsertAsync(source, key, dueIn ?? TimeSpan.Zero);
        }
        return keys;
    }

    protected Task<RecordState> ReadAsync(LeaseSource<TestKey> source, TestKey key, string prefix = "") => Provider.ReadAsync(source.Store, key, prefix, Cancel);

    protected Task<IReadOnlyList<string>> JournalAsync(LeaseSource<TestKey> source) => Provider.ReadJournalAsync(source.Store, Cancel);

    /// <summary>
    /// A process that registers what <paramref name="configure"/> adds. Started processes run dispatchers and expiry
    /// processors; unstarted ones only offer operations.
    /// </summary>
    protected Task<TestProcess> ProcessAsync(Action<WorkBuilder> configure, bool start = true, TimeProvider? time = null, TimeSpan? pollingInterval = null, TimeSpan? shutdownGrace = null, string? application = null) =>
        ServicesAsync(
            services => services.AddWork(work =>
            {
                work.PollingInterval(pollingInterval ?? TimeSpan.FromSeconds(1)).ShutdownGrace(shutdownGrace ?? TimeSpan.FromSeconds(1));
                configure(work);
            }),
            start,
            time,
            application);

    /// <summary>A process with the store and what <paramref name="register"/> adds, such as a process registration without work.</summary>
    protected async Task<TestProcess> ServicesAsync(Action<IServiceCollection> register, bool start = true, TimeProvider? time = null, string? application = null)
    {
        var logs = new CapturedLogs();
        var services = new ServiceCollection();
        services.AddSingleton<ILoggerFactory>(logs);
        if (time is not null)
        {
            services.AddSingleton(time);
        }
        if (application is null)
        {
            Provider.AddStore(services);
        }
        else
        {
            Provider.AddApplicationStore(services, application);
        }
        register(services);
        var process = new TestProcess(services.BuildServiceProvider(), logs);
        processes.Add(process);
        if (start)
        {
            await process.StartAsync(Cancel);
        }
        return process;
    }

    /// <summary>A process that only offers operations for <paramref name="source"/>.</summary>
    protected Task<TestProcess> ClientAsync(LeaseSource<TestKey> source, Action<LeaseSourceBuilder<TestKey>>? configure = null) =>
        ProcessAsync(work => work.AddLeaseSource(source, configure), start: false);

    /// <summary>A started process running <paramref name="script"/> as the source's handler.</summary>
    protected Task<TestProcess> WorkerAsync(LeaseSource<TestKey> source, HandlerScript script, Action<LeaseSourceBuilder<TestKey>>? configure = null, TimeProvider? time = null, TimeSpan? pollingInterval = null, TimeSpan? shutdownGrace = null) =>
        ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(script);
                work.AddLeaseHandler<TestKey, ScriptedHandler>(source, configure);
            },
            time: time,
            pollingInterval: pollingInterval,
            shutdownGrace: shutdownGrace);

    protected async Task<IConformanceTransaction> BeginAsync() => await Provider.BeginAsync(Cancel);

    /// <summary>Skips a scenario that keeps one transaction open while another runs, on a store that serializes them.</summary>
    protected void RequireConcurrentTransactions()
    {
        if (!Provider.ConcurrentTransactions)
        {
            Assert.Skip("The store serializes transactions; the scenario keeps one open while another runs.");
        }
    }

    /// <summary>Claims by key, retrying until the record is claimable or the timeout passes.</summary>
    protected static async Task<LeaseClaim<TestKey>> ClaimEventuallyAsync(LeaseOperations operations, LeaseSource<TestKey> source, TestKey key, TimeSpan timeout, OwnerId? owner = null)
    {
        var stopwatch = Stopwatch.StartNew();
        while (true)
        {
            if (await operations.ClaimAsync(source, key, Cancel, owner) is { } claim)
            {
                return claim;
            }
            if (stopwatch.Elapsed > timeout)
            {
                throw new TimeoutException($"{key} was not claimable within {timeout}.");
            }
            await Task.Delay(25, Cancel);
        }
    }

    /// <summary>Waits until <paramref name="condition"/> holds, polling.</summary>
    protected static async Task EventuallyAsync(Func<Task<bool>> condition, TimeSpan timeout, string because)
    {
        var stopwatch = Stopwatch.StartNew();
        while (!await condition())
        {
            if (stopwatch.Elapsed > timeout)
            {
                Assert.Fail($"Timed out after {timeout}: {because}");
            }
            await Task.Delay(25, Cancel);
        }
    }

    protected static Task EventuallyAsync(Func<bool> condition, TimeSpan timeout, string because) =>
        EventuallyAsync(() => Task.FromResult(condition()), timeout, because);

    /// <summary>Waits until the record's lease has expired by the store's clock.</summary>
    protected Task ExpiredAsync(LeaseSource<TestKey> source, TestKey key, TimeSpan timeout) =>
        EventuallyAsync(async () => !(await ReadAsync(source, key)).Valid, timeout, $"the lease on {key} expires");

    /// <summary>Ends the run's record in its own transaction.</summary>
    protected async Task EndAsync(LeaseRun<TestKey> run, CancellationToken cancellationToken)
    {
        await using var transaction = await Provider.BeginAsync(cancellationToken);
        await run.EndAsync(transaction.Work, cancellationToken: cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var process in processes)
        {
            await process.DisposeAsync();
        }
        GC.SuppressFinalize(this);
    }
}
