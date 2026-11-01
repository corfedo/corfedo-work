using System.Diagnostics;
using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Corfedo.Work.InMemory.Tests.Samples;

/// <summary>
/// An application's test of a two-step chain on the in-memory store and clock: the import step retries while the
/// package registry is busy, and each step writes its domain record in the transaction that completes it.
/// </summary>
public sealed class FeedImportChainSample
{
    private static readonly StoreLocation Feeds = StoreLocation.Table("catalog", "feeds");
    private static readonly TimeSpan Polling = TimeSpan.FromMinutes(1);

    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task TheChainImportsAfterTheRegistryRecoversAndPublishesTheFeed()
    {
        var clock = new InMemoryClock(new DateTimeOffset(2027, 1, 4, 9, 0, 0, TimeSpan.Zero));
        var database = new InMemoryWorkDatabase(clock);
        database.CreateTable(Feeds, ["feed"], ["status", "packages"]);
        using (var seed = await database.BeginAsync(Cancel))
        {
            seed.Insert(Feeds, new Dictionary<string, object?> { ["feed"] = "npm", ["status"] = "new" });
            seed.Commit();
        }
        var registry = new PackageRegistry { Busy = true };
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Services.AddSingleton<TimeProvider>(clock);
        builder.Services.AddSingleton(database);
        builder.Services.AddSingleton(registry);
        builder.Services.AddInMemoryWorkStore(database);
        builder.Services.AddWork(work => work
            .PollingInterval(Polling)
            // A dispatcher measures a claim's validity from before its claim query: leases outlast the test's clock jumps.
            .AddJob<FeedArgs, ImportFeed>(Catalog.Import, job => job.Lease(TimeSpan.FromHours(1)))
            .AddJob<FeedArgs, PublishFeed>(Catalog.Publish, job => job.Lease(TimeSpan.FromHours(1))));
        using var host = builder.Build();
        await host.StartAsync(Cancel);
        var jobs = host.Services.GetRequiredService<JobOperations>();

        ChainId chain;
        using (var request = await database.BeginAsync(Cancel))
        {
            chain = await jobs.RequestChainAsync(request, steps => steps
                .StartWith(Catalog.Import, new FeedArgs("npm"))
                .Then(Catalog.Publish, new FeedArgs("npm")), Cancel);
            request.Commit();
        }

        // The first attempt finds the registry busy; the retry waits for the clock to pass its backoff.
        Assert.True(await HoldsWithinAsync(() => Task.FromResult(registry.Calls == 1), TimeSpan.FromSeconds(10)), "the import runs");
        registry.Busy = false;
        await Task.Delay(200, Cancel);
        Assert.Equal(1, registry.Calls);

        var settled = clock.GetUtcNow() + ImportFeed.Backoff;
        clock.Advance(ImportFeed.Backoff);
        for (var steps = 0; !await HoldsWithinAsync(async () => (await jobs.GetChainAsync(chain, Cancel))?.Settled == true, TimeSpan.FromSeconds(1)); steps++)
        {
            // Background loops arm their next timer when they get to it, so the clock moves on by the polling interval.
            Assert.True(steps < 10, $"The chain did not settle within ten polling intervals after {settled:O}.");
            clock.Advance(Polling);
        }

        var status = await jobs.GetChainAsync(chain, Cancel);
        Assert.Equal(JobOutcome.Completed, status?.Outcome);
        Assert.Equal([(Catalog.Import.Key, JobOutcome.Completed), (Catalog.Publish.Key, JobOutcome.Completed)], status!.Steps.Select(s => (s.Definition, s.Outcome ?? default)));
        Assert.Equal(2, registry.Calls);
        var feed = database.Find(Feeds, ["npm"]);
        Assert.Equal(("published", (object?)3), ((string?)feed?["status"], feed?["packages"]));
        await host.StopAsync(Cancel);
    }

    private static async Task<bool> HoldsWithinAsync(Func<Task<bool>> condition, TimeSpan wait)
    {
        var waited = Stopwatch.StartNew();
        while (!await condition())
        {
            if (waited.Elapsed > wait)
            {
                return false;
            }
            await Task.Delay(20, Cancel);
        }
        return true;
    }

    private static class Catalog
    {
        public static readonly JobDefinition<FeedArgs> Import = JobDefinition.Create("catalog.import", 1, JobArgs.Json(SampleJson.Default.FeedArgs), new JobPolicy { Attempts = 3, Backoff = Backoff.None });

        public static readonly JobDefinition<FeedArgs> Publish = JobDefinition.Create("catalog.publish", 1, JobArgs.Json(SampleJson.Default.FeedArgs));
    }

    /// <summary>A test double of the package registry the import reads.</summary>
    private sealed class PackageRegistry
    {
        private int calls;

        public volatile bool Busy;

        public int Calls => Volatile.Read(ref calls);

        public int? CountPackages(string feed)
        {
            Interlocked.Increment(ref calls);
            return Busy ? null : 3;
        }
    }

    private sealed class ImportFeed(PackageRegistry registry, InMemoryWorkDatabase database) : IJobHandler<FeedArgs>
    {
        public static readonly TimeSpan Backoff = TimeSpan.FromMinutes(15);

        public async ValueTask<JobDisposition> RunAsync(FeedArgs args, JobRun run, CancellationToken cancellationToken)
        {
            if (registry.CountPackages(args.Feed) is not { } packages)
            {
                return JobResult.Retry(Backoff, "registry_busy");
            }
            using var transaction = await database.BeginAsync(cancellationToken);
            transaction.Update(Feeds, [args.Feed], new Dictionary<string, object?> { ["status"] = "imported", ["packages"] = packages });
            await run.CompleteAsync(transaction, JobResult.Completed, cancellationToken);
            transaction.Commit();
            return run.Completed;
        }
    }

    private sealed class PublishFeed(InMemoryWorkDatabase database) : IJobHandler<FeedArgs>
    {
        public async ValueTask<JobDisposition> RunAsync(FeedArgs args, JobRun run, CancellationToken cancellationToken)
        {
            using var transaction = await database.BeginAsync(cancellationToken);
            Assert.Equal("imported", transaction.Find(Feeds, [args.Feed])?["status"]);
            transaction.Update(Feeds, [args.Feed], new Dictionary<string, object?> { ["status"] = "published" });
            await run.CompleteAsync(transaction, JobResult.Completed, cancellationToken);
            transaction.Commit();
            return run.Completed;
        }
    }
}

internal sealed record FeedArgs(string Feed);

[JsonSerializable(typeof(FeedArgs))]
internal sealed partial class SampleJson : JsonSerializerContext;
