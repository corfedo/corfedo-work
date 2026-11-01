using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>What a scenario's reconciler does; one instance per process.</summary>
public sealed class ReconcileScript
{
    public Func<ReconcileRun<TestKey>, CancellationToken, ValueTask<ReconcileDisposition>> Run { get; set; } = (_, _) => ValueTask.FromResult<ReconcileDisposition>(ReconcileResult.Converged);
}

public sealed class ScriptedReconciler(ReconcileScript script) : IReconciler<TestKey>
{
    public ValueTask<ReconcileDisposition> ReconcileAsync(ReconcileRun<TestKey> run, CancellationToken cancellationToken) => script.Run(run, cancellationToken);
}

/// <summary>One run as a scenario observed it: when it started, by the scenario's stopwatch, and what it claimed.</summary>
public sealed record ObservedRun(TestKey Key, TimeSpan At, long Revision, long Generation, ClaimReason Reason, OwnerId Owner, Guid ExecutionId);

/// <summary>Shared setup for reconciliation scenarios. Every scenario creates its own table and source.</summary>
public abstract class ReconcileScenarioBase(IReconcileConformanceProvider provider) : LeaseScenarioBase(provider)
{
    protected new IReconcileConformanceProvider Provider { get; } = provider;

    protected Stopwatch Clock { get; } = Stopwatch.StartNew();

    /// <summary>A reconciliation policy with short leases, adjusted by <paramref name="lease"/>.</summary>
    protected static ReconcilePolicy Reconcile(
        TimeSpan duration,
        Func<LeasePolicy, LeasePolicy>? lease = null,
        RequestPolicy requests = RequestPolicy.FollowUp,
        TimeSpan? resync = null) => new()
        {
            Lease = Policy(duration, lease),
            Requests = requests,
            ResyncInterval = resync,
        };

    /// <summary>A new table and a source on it, with the framework schema applied for processes that are not started.</summary>
    protected async Task<ReconcileSource<TestKey>> ReconcileSourceAsync(ReconcilePolicy policy, bool partitioned = false)
    {
        await Provider.CreateStore().MigrateAsync(Cancel);
        var table = await Provider.CreateReconcileTableAsync(Cancel);
        return ReconcileSource.Define("r-" + table.Name, table, TestKey.Codec, policy, partitioned ? "tenant" : null);
    }

    protected Task InsertAsync(ReconcileSource<TestKey> source, TestKey key, DueState due = DueState.None, long revision = 0) =>
        Provider.InsertReconcileAsync(source.Store, key, due, revision, Cancel);

    protected Task<ReconcileState> ReadAsync(ReconcileSource<TestKey> source, TestKey key) => Provider.ReadReconcileAsync(source.Store, key, Cancel);

    /// <summary>A started process converging the source's records with <paramref name="script"/>.</summary>
    protected Task<TestProcess> ReconcilerAsync(
        ReconcileSource<TestKey> source,
        ReconcileScript script,
        Action<LeaseSourceBuilder<TestKey>>? configure = null,
        TimeSpan? pollingInterval = null) =>
        ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(script);
                work.AddReconciler<TestKey, ScriptedReconciler>(source, configure);
            },
            pollingInterval: pollingInterval ?? TimeSpan.FromMilliseconds(200));

    /// <summary>An unstarted process that registers the source for claims by key.</summary>
    protected Task<TestProcess> ClientAsync(ReconcileSource<TestKey> source) =>
        ProcessAsync(work => work.AddReconcileSource(source), start: false);

    protected static ReconcileOperations Reconciliation(TestProcess process) => process.Services.GetRequiredService<ReconcileOperations>();

    /// <summary>Requests the record in a transaction of its own.</summary>
    protected async Task RequestAsync(TestProcess process, ReconcileSource<TestKey> source, TestKey key, ReconcileRequest? request = null)
    {
        await using var transaction = await BeginAsync();
        Assert.True(await Reconciliation(process).RequestAsync(transaction.Work, source, key, request ?? ReconcileRequest.Default, Cancel));
        await transaction.CommitAsync(Cancel);
    }

    /// <summary>Invalidates the source in a transaction of its own and returns the new generation.</summary>
    protected async Task<long> InvalidateAsync(TestProcess process, ReconcileSource<TestKey> source)
    {
        await using var transaction = await BeginAsync();
        var generation = await Reconciliation(process).InvalidateAllAsync(transaction.Work, source, Cancel);
        await transaction.CommitAsync(Cancel);
        return generation;
    }

    /// <summary>A script that records every run and answers with <paramref name="result"/>.</summary>
    protected ReconcileScript Recording(ConcurrentQueue<ObservedRun> runs, Func<ReconcileRun<TestKey>, CancellationToken, ValueTask<ReconcileDisposition>>? result = null) => new()
    {
        Run = async (run, cancellationToken) =>
        {
            runs.Enqueue(Observe(run));
            return result is null ? ReconcileResult.Converged : await result(run, cancellationToken);
        },
    };

    protected ObservedRun Observe(ReconcileRun<TestKey> run) =>
        new(run.Key, Clock.Elapsed, run.Revision, run.Generation, run.Reason, run.Owner, run.ExecutionId);

    /// <summary>Waits until the record converged <paramref name="revision"/> and nothing holds it.</summary>
    protected Task ConvergedAsync(ReconcileSource<TestKey> source, TestKey key, long revision, TimeSpan timeout, long generation = 0) =>
        EventuallyAsync(
            async () => await ReadAsync(source, key) is { ConvergedRevision: var converged, ConvergedGeneration: var g, Lease.Held: false }
                && converged == revision && g == generation,
            timeout,
            $"{key} converges revision {revision} under generation {generation}");
}
