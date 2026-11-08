using System.Text.Json.Serialization;
using Corfedo.Work.EntityFrameworkCore.Tests.Provisioning;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Corfedo.Work.EntityFrameworkCore.Tests.Transactions;

/// <summary>Work operations join a <see cref="DbContext"/>'s transaction and commit or roll back with its changes (R1).</summary>
public abstract class DbContextTransactionTests(TestDatabase database) : IAsyncLifetime
{
    private ServiceProvider services = null!;
    private IHostedService[] hosted = [];

    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    private IWorkStore Store => services.GetRequiredService<IWorkStore>();

    private JobOperations Jobs => services.GetRequiredService<JobOperations>();

    public async ValueTask InitializeAsync()
    {
        await using (var context = Context())
        {
            await context.Database.EnsureCreatedAsync(Cancel);
        }
        var collection = new ServiceCollection();
        collection.AddSingleton(database);
        database.AddWorkStore(collection);
        collection.AddWork(work => work.PollingInterval(TimeSpan.FromMilliseconds(200)).AddJob<ShipOrder, ShipOrderHandler>(Shop.Ship, job => job.OnEnded<ShipmentRecorded>()));
        services = collection.BuildServiceProvider();
        hosted = [.. services.GetServices<IHostedService>()];
        foreach (var service in hosted)
        {
            await service.StartAsync(Cancel);
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var service in hosted.Reverse())
        {
            await service.StopAsync(CancellationToken.None);
        }
        await services.DisposeAsync();
    }

    [Fact]
    public async Task ARequestInTheDbContextTransactionCommitsWithTheDomainChange()
    {
        var order = Guid.NewGuid();
        JobId job;
        await using (var context = Context())
        {
            await using var transaction = await context.Database.BeginTransactionAsync(Cancel);
            context.Orders.Add(new Order { Id = order, Status = "placed" });
            await context.SaveChangesAsync(Cancel);
            job = (await Jobs.RequestAsync(context.Database.WorkTransaction(), Shop.Ship, new ShipOrder(order, 0), cancellationToken: Cancel)).Id;
            await transaction.CommitAsync(Cancel);
        }

        var outcome = await Jobs.WaitAsync(Shop.Ship, job, TimeSpan.FromSeconds(10), Cancel);
        Assert.Equal(JobOutcome.Completed, outcome?.Outcome);
        Assert.Equal("shipped", await StatusAsync(order));
    }

    [Fact]
    public async Task AnOutcomeHookWritesThroughADbContextJoinedToTheTransitionTransaction()
    {
        var order = Guid.NewGuid();
        await using (var context = Context())
        {
            context.Orders.Add(new Order { Id = order, Status = "placed" });
            await context.SaveChangesAsync(Cancel);
        }

        // The hook's write commits with the ending transition, here a cancellation outside any handler.
        JobId job;
        await using (var context = Context())
        {
            await using var transaction = await context.Database.BeginTransactionAsync(Cancel);
            var options = new JobRequestOptions { MergeKey = order.ToString() };
            job = (await Jobs.RequestAsync(context.Database.WorkTransaction(), Shop.Ship, new ShipOrder(order, 0), Schedule.After(TimeSpan.FromHours(1)), options, Cancel)).Id;
            await transaction.CommitAsync(Cancel);
        }
        await using (var context = Context())
        {
            await using var transaction = await context.Database.BeginTransactionAsync(Cancel);
            await Jobs.CancelAsync(context.Database.WorkTransaction(), Shop.Ship, order.ToString(), Cancel);
            await transaction.CommitAsync(Cancel);
        }

        await using var read = Context();
        Assert.Equal(["Cancelled"], await read.Shipments.Where(s => s.Job == job.Value).Select(s => s.Outcome).ToListAsync(Cancel));
    }

    [Fact]
    public async Task RollingBackTheDbContextTransactionRollsBackTheRequest()
    {
        var order = Guid.NewGuid();
        JobId job;
        await using (var context = Context())
        {
            await using var transaction = await context.Database.BeginTransactionAsync(Cancel);
            context.Orders.Add(new Order { Id = order, Status = "placed" });
            await context.SaveChangesAsync(Cancel);
            job = (await Jobs.RequestAsync(context.Database.WorkTransaction(), Shop.Ship, new ShipOrder(order, 0), cancellationToken: Cancel)).Id;
            await transaction.RollbackAsync(Cancel);
        }

        Assert.Null(await Jobs.GetOutcomeAsync(Shop.Ship, job, Cancel));
        Assert.Null(await StatusAsync(order));
    }

    [Fact]
    public async Task AHandlerGuardsAndCompletesInItsDbContextTransactionAndAnAttemptThatRollsBackIsRetried()
    {
        var order = Guid.NewGuid();
        await using (var context = Context())
        {
            context.Orders.Add(new Order { Id = order, Status = "placed" });
            await context.SaveChangesAsync(Cancel);
        }
        JobId job;
        await using (var context = Context())
        {
            await using var transaction = await context.Database.BeginTransactionAsync(Cancel);
            job = (await Jobs.RequestAsync(context.Database.WorkTransaction(), Shop.Ship, new ShipOrder(order, RollBackAttempts: 1), cancellationToken: Cancel)).Id;
            await transaction.CommitAsync(Cancel);
        }

        var outcome = await Jobs.WaitAsync(Shop.Ship, job, TimeSpan.FromSeconds(30), Cancel);
        Assert.Equal(JobOutcome.Completed, outcome?.Outcome);
        Assert.Equal("shipped", await StatusAsync(order));
        var runs = (await Jobs.GetRunsAsync(Shop.Ship, job, cancellationToken: Cancel)).Runs.OrderBy(r => r.Attempt).ToList();
        Assert.Equal([(1, JobRunOutcome.Retried, (string?)"uncompleted"), (2, JobRunOutcome.Completed, null)], runs.Select(r => (r.Attempt, r.Outcome, r.Reason)));
    }

    [Fact]
    public async Task WithoutAnActiveTransactionTheDbContextIsRefused()
    {
        await using var context = Context();

        var refused = Assert.Throws<InvalidOperationException>(() => context.Database.WorkTransaction());
        Assert.Contains("no active transaction", refused.Message, StringComparison.Ordinal);
    }

    private ShopContext Context() => new(database);

    private async Task<string?> StatusAsync(Guid order)
    {
        await using var context = Context();
        return await context.Orders.Where(o => o.Id == order).Select(o => o.Status).SingleOrDefaultAsync(Cancel);
    }
}

public sealed class PostgreSqlDbContextTransactionTests(PostgreSqlTestDatabase database)
    : DbContextTransactionTests(database), IClassFixture<PostgreSqlTestDatabase>;

public sealed class SqlServerDbContextTransactionTests(SqlServerTestDatabase database)
    : DbContextTransactionTests(database), IClassFixture<SqlServerTestDatabase>;

internal static class Shop
{
    public static readonly JobDefinition<ShipOrder> Ship = JobDefinition.Create(
        "shop.ship", 1, JobArgs.Json(ShopJson.Default.ShipOrder), new JobPolicy { Attempts = 3, Backoff = Backoff.None, RunHistory = true });
}

internal sealed record ShipOrder(Guid Order, int RollBackAttempts);

[JsonSerializable(typeof(ShipOrder))]
internal sealed partial class ShopJson : JsonSerializerContext;

internal sealed class Order
{
    public Guid Id { get; set; }

    public required string Status { get; set; }
}

internal sealed class Shipment
{
    public Guid Id { get; set; }

    public Guid Job { get; set; }

    public required string Outcome { get; set; }
}

internal sealed class ShopContext(TestDatabase database) : DbContext
{
    public DbSet<Order> Orders => Set<Order>();

    public DbSet<Shipment> Shipments => Set<Shipment>();

    protected override void OnConfiguring(DbContextOptionsBuilder builder) => database.Configure(builder);

    protected override void OnModelCreating(ModelBuilder model)
    {
        model.Entity<Order>(order =>
        {
            order.ToTable("orders", "shop");
            order.Property(o => o.Id).HasColumnName("id");
            order.Property(o => o.Status).HasColumnName("status");
        });
        model.Entity<Shipment>(shipment =>
        {
            shipment.ToTable("shipments", "shop");
            shipment.Property(s => s.Id).HasColumnName("id");
            shipment.Property(s => s.Job).HasColumnName("job_id");
            shipment.Property(s => s.Outcome).HasColumnName("outcome");
        });
    }
}

/// <summary>Records each shipping job's outcome as a new row, in the transaction of the transition that ends it.</summary>
internal sealed class ShipmentRecorded(TestDatabase database) : IJobEnded
{
    public async ValueTask EndedAsync(JobEnd end, WorkTransaction transaction, CancellationToken cancellationToken)
    {
        await using var context = new ShopContext(database);
        await context.Database.UseWorkTransactionAsync(transaction, cancellationToken);
        context.Shipments.Add(new Shipment { Id = Guid.NewGuid(), Job = end.Job.Value, Outcome = end.Outcome.ToString() });
        await context.SaveChangesAsync(cancellationToken);
    }
}

/// <summary>
/// Ships the order in a <see cref="DbContext"/> transaction: guards the attempt, writes the order and completes the job
/// there. Attempts up to the arguments' count roll back after completing, so the run returns without a committed
/// completion.
/// </summary>
internal sealed class ShipOrderHandler(TestDatabase database) : IJobHandler<ShipOrder>
{
    public async ValueTask<JobDisposition> RunAsync(ShipOrder args, JobRun run, CancellationToken cancellationToken)
    {
        await using var context = new ShopContext(database);
        await using var transaction = await context.Database.BeginTransactionAsync(cancellationToken);
        var work = context.Database.WorkTransaction();
        await run.GuardAsync(work, cancellationToken);
        var order = await context.Orders.SingleAsync(o => o.Id == args.Order, cancellationToken);
        order.Status = "shipped";
        await context.SaveChangesAsync(cancellationToken);
        await run.CompleteAsync(work, JobResult.Completed, cancellationToken);
        if (run.Attempt <= args.RollBackAttempts)
        {
            await transaction.RollbackAsync(cancellationToken);
        }
        else
        {
            await transaction.CommitAsync(cancellationToken);
        }
        return run.Completed;
    }
}
