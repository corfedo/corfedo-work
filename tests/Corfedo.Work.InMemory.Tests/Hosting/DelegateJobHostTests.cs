using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Corfedo.Work.InMemory.Tests.Hosting;

/// <summary>A job handler given as a delegate runs like a handler class, with the service scope of the attempt.</summary>
public sealed class DelegateJobHostTests
{
    public sealed record NameArgs(string Name);

    private static readonly JobDefinition<NameArgs> Greet = JobDefinition.Create<NameArgs>("delegate.greet");

    private static readonly JobDefinition<NameArgs> Farewell = JobDefinition.Create<NameArgs>("delegate.farewell");

    private static readonly JobDefinition<NameArgs, NameArgs> Echo = JobDefinition.Create<NameArgs, NameArgs>("delegate.echo");

    private static readonly JobDefinition<NameArgs> Flaky = JobDefinition.Create<NameArgs>(
        "delegate.flaky", 1, new JobPolicy { Attempts = 3, Backoff = Backoff.None });

    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ADelegateRunsEachJobInTheScopeOfItsAttempt()
    {
        var database = new InMemoryWorkDatabase();
        var seen = new ConcurrentBag<(string Name, AttemptScope Scope)>();
        using var host = Build(database, services => services.AddScoped<AttemptScope>(), work => work
            .AddJob(Greet, async (args, run, services, ct) =>
            {
                await Task.Yield();
                seen.Add((args.Name, services.GetRequiredService<AttemptScope>()));
                return JobResult.Completed;
            }));
        await host.StartAsync(Cancel);
        var jobs = host.Services.GetRequiredService<JobOperations>();

        var first = await jobs.RequestAsync(Greet, new NameArgs("npm"), cancellationToken: Cancel);
        var second = await jobs.RequestAsync(Greet, new NameArgs("nuget"), cancellationToken: Cancel);
        await jobs.WaitAsync(Greet, first.Id, TimeSpan.FromSeconds(10), Cancel);
        await jobs.WaitAsync(Greet, second.Id, TimeSpan.FromSeconds(10), Cancel);

        Assert.Equal(JobOutcome.Completed, (await jobs.GetOutcomeAsync(Greet, first.Id, Cancel))!.Outcome);
        Assert.Equal(["npm", "nuget"], seen.Select(s => s.Name).Order());
        Assert.Equal(2, seen.Select(s => s.Scope).Distinct().Count());
        await host.StopAsync(Cancel);
    }

    [Fact]
    public async Task DelegatesOfDefinitionsWithTheSameArgumentsRunTheirOwnJobs()
    {
        var database = new InMemoryWorkDatabase();
        var ran = new ConcurrentBag<string>();
        using var host = Build(database, null, work => work
            .AddJob(Greet, (args, _, _, _) => Run(ran, "greet " + args.Name))
            .AddJob(Farewell, (args, _, _, _) => Run(ran, "farewell " + args.Name)));
        await host.StartAsync(Cancel);
        var jobs = host.Services.GetRequiredService<JobOperations>();

        var greet = await jobs.RequestAsync(Greet, new NameArgs("npm"), cancellationToken: Cancel);
        var farewell = await jobs.RequestAsync(Farewell, new NameArgs("npm"), cancellationToken: Cancel);
        await jobs.WaitAsync(Greet, greet.Id, TimeSpan.FromSeconds(10), Cancel);
        await jobs.WaitAsync(Farewell, farewell.Id, TimeSpan.FromSeconds(10), Cancel);

        Assert.Equal(["farewell npm", "greet npm"], ran.Order());
        await host.StopAsync(Cancel);
    }

    [Fact]
    public async Task AnExceptionFromADelegateRetriesTheJob()
    {
        var database = new InMemoryWorkDatabase();
        var attempts = 0;
        using var host = Build(database, null, work => work
            .AddJob(Flaky, (_, _, _, _) => Interlocked.Increment(ref attempts) < 3
                ? throw new InvalidOperationException("not yet")
                : ValueTask.FromResult<JobDisposition>(JobResult.Completed)));
        await host.StartAsync(Cancel);
        var jobs = host.Services.GetRequiredService<JobOperations>();

        var requested = await jobs.RequestAsync(Flaky, new NameArgs("npm"), cancellationToken: Cancel);
        await jobs.WaitAsync(Flaky, requested.Id, TimeSpan.FromSeconds(10), Cancel);

        Assert.Equal(JobOutcome.Completed, (await jobs.GetOutcomeAsync(Flaky, requested.Id, Cancel))!.Outcome);
        Assert.Equal(3, attempts);
        await host.StopAsync(Cancel);
    }

    [Fact]
    public async Task ADelegateOfADefinitionWithAResultTypeStoresItsResult()
    {
        var database = new InMemoryWorkDatabase();
        using var host = Build(database, null, work => work
            .AddJob(Echo, (args, _, _, _) => ValueTask.FromResult<JobDisposition<NameArgs>>(args with { Name = args.Name + "!" })));
        await host.StartAsync(Cancel);
        var jobs = host.Services.GetRequiredService<JobOperations>();

        var requested = await jobs.RequestAsync(Echo, new NameArgs("npm"), cancellationToken: Cancel);
        await jobs.WaitAsync(Echo, requested.Id, TimeSpan.FromSeconds(10), Cancel);

        Assert.Equal(new NameArgs("npm!"), (await jobs.GetOutcomeAsync(Echo, requested.Id, Cancel))!.Result);
        await host.StopAsync(Cancel);
    }

    [Fact]
    public async Task ADelegateOfADefinitionWithAResultTypeMayReturnAJobResult()
    {
        var database = new InMemoryWorkDatabase();
        using var host = Build(database, null, work => work
            .AddJob(Echo, async (_, _, _, ct) =>
            {
                await Task.Yield();
                return JobResult.Fail("unknown_feed");
            }));
        await host.StartAsync(Cancel);
        var jobs = host.Services.GetRequiredService<JobOperations>();

        var requested = await jobs.RequestAsync(Echo, new NameArgs("npm"), cancellationToken: Cancel);
        await jobs.WaitAsync(Echo, requested.Id, TimeSpan.FromSeconds(10), Cancel);

        var outcome = (await jobs.GetOutcomeAsync(Echo, requested.Id, Cancel))!;
        Assert.Equal((JobOutcome.Failed, "unknown_feed", false), (outcome.Outcome!.Value, outcome.Reason, outcome.HasResult));
        await host.StopAsync(Cancel);
    }

    private static ValueTask<JobDisposition> Run(ConcurrentBag<string> ran, string what)
    {
        ran.Add(what);
        return ValueTask.FromResult<JobDisposition>(JobResult.Completed);
    }

    private static IHost Build(InMemoryWorkDatabase database, Action<IServiceCollection>? services, Action<WorkBuilder> configure)
    {
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        services?.Invoke(builder.Services);
        builder.Services.AddInMemoryWorkStore(database);
        builder.Services.AddWork(configure);
        return builder.Build();
    }

    private sealed class AttemptScope;
}
