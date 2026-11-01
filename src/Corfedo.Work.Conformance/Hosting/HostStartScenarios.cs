using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>Host start: <see cref="StartMode.Manual"/> and hosts that are built but never started.</summary>
public abstract class HostStartScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    [Fact]
    public async Task H7_AManualHostClaimsRegistersAndPlansNothingBeforeItIsStarted()
    {
        var definition = Definition();
        var recurring = Definition();
        var runs = new ConcurrentQueue<string>();
        var script = new JobScript
        {
            Run = (args, _, _) =>
            {
                runs.Enqueue(args.Value);
                return ValueTask.FromResult<JobDisposition>(JobResult.Completed);
            },
        };
        var process = await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(script);
                work.StartMode(StartMode.Manual).Process(p => p.Type("manual"));
                work.AddJob<TestArgs, ScriptedJobHandler>(definition, job => job.Lease(TimeSpan.FromSeconds(2)));
                work.AddJob<TestArgs, ScriptedJobHandler>(recurring, job => job.Lease(TimeSpan.FromSeconds(2)));
                work.AddRecurring(recurring, new TestArgs("occurrence"), Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate));
            },
            pollingInterval: TimeSpan.FromMilliseconds(200));
        var host = process.Services.GetRequiredService<IWorkHost>();

        // The host's own start applies the schema and stores definitions, so the application can request work.
        await InTransactionAsync(tx => Jobs(process).RequestAsync(tx, definition, new TestArgs("requested"), cancellationToken: Cancel));
        await Task.Delay(1500, Cancel);
        Assert.Empty(runs);
        Assert.Null(await Provider.ReadProcessAsync(process.Owner, Cancel));
        Assert.Empty(await ReadJobsAsync(recurring));

        await host.StartAsync(Cancel);
        await host.StartAsync(Cancel);
        await EventuallyAsync(() => runs.Contains("requested") && runs.Contains("occurrence"), TimeSpan.FromSeconds(10), "the requested job and an occurrence run");
        Assert.NotNull(await Provider.ReadProcessAsync(process.Owner, Cancel));
    }

    [Fact]
    public async Task H7_AManualHostRefusesToStartBeforeTheHostStartedAndAnAutomaticOneRefusesAlways()
    {
        var definition = Definition();
        var manual = await ProcessAsync(work => work.StartMode(StartMode.Manual).AddJobDefinition(definition), start: false);
        await Assert.ThrowsAsync<InvalidOperationException>(() => manual.Services.GetRequiredService<IWorkHost>().StartAsync(Cancel));

        var automatic = await ProcessAsync(work => work.AddJobDefinition(definition));
        await Assert.ThrowsAsync<InvalidOperationException>(() => automatic.Services.GetRequiredService<IWorkHost>().StartAsync(Cancel));
    }

    [Fact]
    public async Task H7_AHostThatIsBuiltButNeverStartedTouchesNothing()
    {
        // Build-time tools, such as API document generation, build the host and resolve services without starting it.
        var definition = Definition();
        var runs = 0;
        var script = new JobScript
        {
            Run = (_, _, _) =>
            {
                Interlocked.Increment(ref runs);
                return ValueTask.FromResult<JobDisposition>(JobResult.Completed);
            },
        };
        var worker = await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(script);
                work.Process(p => p.Type("build-time"));
                work.AddJob<TestArgs, ScriptedJobHandler>(definition, job => job.Lease(TimeSpan.FromSeconds(2)));
            },
            start: false,
            pollingInterval: TimeSpan.FromMilliseconds(200));
        _ = worker.Services.GetRequiredService<IWorkHost>();
        _ = Jobs(worker);
        var client = await ClientAsync(null, definition);
        var job = await InTransactionAsync(tx => Jobs(client).RequestAsync(tx, definition, new TestArgs("a"), cancellationToken: Cancel));

        await Task.Delay(1000, Cancel);
        Assert.Equal(0, runs);
        Assert.Null(await Provider.ReadProcessAsync(worker.Owner, Cancel));
        Assert.Null(Assert.Single(await ReadJobsAsync(definition)).State);
    }
}
