using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>Dispatch pools shared by several sources.</summary>
public abstract class ReconcilePoolScenarios(IReconcileConformanceProvider provider) : ReconcileScenarioBase(provider)
{
    [Fact]
    public async Task C10_SourceSharingAPoolWithABackloggedSourceIsServedWithinOneRotation()
    {
        var backlogged = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30)));
        var quiet = await ReconcileSourceAsync(Reconcile(TimeSpan.FromSeconds(30)));
        const int backlog = 40;
        for (var i = 1; i <= backlog; i++)
        {
            await InsertAsync(backlogged, new TestKey("t", i), DueState.Past);
        }
        var key = new TestKey("t", 1);
        await InsertAsync(quiet, key);
        var client = await ClientAsync(quiet);
        var runs = new ConcurrentQueue<ObservedRun>();
        var running = 0;
        var maximum = 0;
        var script = Recording(runs, async (_, cancellationToken) =>
        {
            var now = Interlocked.Increment(ref running);
            int seen;
            while (now > (seen = Volatile.Read(ref maximum)) && Interlocked.CompareExchange(ref maximum, now, seen) != seen)
            {
            }
            await Task.Delay(150, cancellationToken);
            Interlocked.Decrement(ref running);
            return ReconcileResult.Converged;
        });
        await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(script);
                work.AddPool("shared", capacity: 2)
                    .AddReconciler<TestKey, ScriptedReconciler>(backlogged, s => s.Pool("shared"))
                    .AddReconciler<TestKey, ScriptedReconciler>(quiet, s => s.Pool("shared"));
            },
            pollingInterval: TimeSpan.FromSeconds(5));
        await EventuallyAsync(() => runs.Count >= 6, TimeSpan.FromSeconds(10), "the backlog runs");

        var requested = Clock.Elapsed;
        await RequestAsync(client, quiet, key);
        await ConvergedAsync(quiet, key, 1, TimeSpan.FromSeconds(5));

        var quietRun = Assert.Single(runs, r => r.Revision == 1);
        var backlogRunsBefore = runs.Count(r => r.Revision == 0 && r.At < quietRun.At);
        Assert.True(quietRun.At - requested < TimeSpan.FromMilliseconds(600), $"the quiet source waited {quietRun.At - requested}");
        Assert.True(backlogRunsBefore < backlog, "the quiet source waited for the whole backlog");
        Assert.Equal(2, maximum);
    }
}
