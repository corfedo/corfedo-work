using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Corfedo.Work.InMemory.Tests.Hosting;

/// <summary>A host serializes job arguments and results with its JSON options and checks their types at startup.</summary>
public sealed partial class JobJsonHostTests
{
    public sealed record ListedArgs(string Name);

    public sealed record UnlistedArgs(string Name);

    [JsonSerializable(typeof(ListedArgs))]
    internal sealed partial class ListedJson : JsonSerializerContext;

    private static readonly JobDefinition<ListedArgs, ListedArgs> Echo = JobDefinition.Create<ListedArgs, ListedArgs>("json.echo");

    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task TheDefaultOptionsRunAJobAndReadItsResult()
    {
        var database = new InMemoryWorkDatabase();
        using var host = Build(database, work => work.AddJob<ListedArgs, ListedArgs, EchoHandler>(Echo));
        await host.StartAsync(Cancel);
        var jobs = host.Services.GetRequiredService<JobOperations>();
        JobRequestResult requested;
        using (var transaction = await database.BeginAsync(Cancel))
        {
            requested = await jobs.RequestAsync(transaction, Echo, new ListedArgs("npm"), Schedule.Now, cancellationToken: Cancel);
            transaction.Commit();
        }

        await jobs.WaitAsync(Echo, requested.Id, TimeSpan.FromSeconds(10), Cancel);

        Assert.Equal(new ListedArgs("npm"), (await jobs.GetOutcomeAsync(Echo, requested.Id, Cancel))!.Result);
        await host.StopAsync(Cancel);
    }

    [Fact]
    public async Task ATypeMissingFromTheResolversStopsStartup()
    {
        using var host = Build(new InMemoryWorkDatabase(), work => work
            .ConfigureJson(options => options.TypeInfoResolver = ListedJson.Default)
            .AddJobDefinition(JobDefinition.Create<UnlistedArgs>("json.unlisted")));

        var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync(Cancel));

        Assert.Contains("[JsonSerializable(typeof(UnlistedArgs))]", refused.Message, StringComparison.Ordinal);
    }

    private static IHost Build(InMemoryWorkDatabase database, Action<WorkBuilder> configure)
    {
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Services.AddInMemoryWorkStore(database);
        builder.Services.AddWork(configure);
        return builder.Build();
    }

    private sealed class EchoHandler : IJobHandler<ListedArgs, ListedArgs>
    {
        public ValueTask<JobDisposition<ListedArgs>> RunAsync(ListedArgs args, JobRun<ListedArgs> run, CancellationToken cancellationToken) =>
            ValueTask.FromResult<JobDisposition<ListedArgs>>(args);
    }
}
