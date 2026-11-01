using System.Globalization;
using Corfedo.Work.PostgreSql;
using Corfedo.Work.PostgreSql.Tests.Provisioning;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.EntityFrameworkCore.Tests.Mapping;

/// <summary>
/// Tables an EF model creates with <c>HasLease</c> and <c>HasReconciliation</c> pass the store's verification, and
/// their entities serve as domain lease and reconciliation sources.
/// </summary>
public sealed class ModelMappingTests(PostgreSqlDatabase database) : IClassFixture<PostgreSqlDatabase>, IAsyncLifetime
{
    private ServiceProvider services = null!;

    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    private IWorkStore Store => services.GetRequiredService<IWorkStore>();

    public async ValueTask InitializeAsync()
    {
        await using (var context = Context())
        {
            await context.Database.EnsureCreatedAsync(Cancel);
        }
        var collection = new ServiceCollection();
        collection.AddPostgreSqlWorkStore(database.DataSource);
        collection.AddWork(work => work.AddLeaseSource(Fleet.Instances).AddReconcileSource(Fleet.Placements));
        services = collection.BuildServiceProvider();
        await Store.MigrateAsync(Cancel);
    }

    public async ValueTask DisposeAsync() => await services.DisposeAsync();

    [Fact]
    public async Task AnEntityMappedWithHasLeaseVerifiesAndIsClaimedThroughItsDueAtProperty()
    {
        await Store.Leases.VerifyAsync(Fleet.Instances, Cancel);
        await using (var context = Context())
        {
            context.Instances.Add(new Instance { TenantId = "acme", Id = 7, Name = "web", DueAt = DateTimeOffset.UtcNow.AddSeconds(-1) });
            await context.SaveChangesAsync(Cancel);
        }

        var claim = await services.GetRequiredService<LeaseOperations>().ClaimAsync(Fleet.Instances, new InstanceKey("acme", 7), Cancel);

        Assert.Equal(1, claim?.Lease.Epoch);
        await using var read = Context();
        var instance = await read.Instances.SingleAsync(i => i.TenantId == "acme" && i.Id == 7, Cancel);
        Assert.Equal((1L, (Guid?)claim!.Lease.Owner.Value), (read.Entry(instance).Property<long>("LeaseEpoch").CurrentValue, read.Entry(instance).Property<Guid?>("LeaseOwner").CurrentValue));
    }

    [Fact]
    public async Task AnEntityMappedWithHasReconciliationVerifiesAndTakesRequestsInTheDbContextTransaction()
    {
        await Store.Leases.VerifyAsync(Fleet.Placements.Leases, Cancel);
        await using (var context = Context())
        {
            // A record with a due time takes part in reconciliation; infinity, until something requests it.
            var placement = context.Placements.Add(new Placement { TenantId = "acme", Id = 8, Region = "eu" });
            placement.Property<DateTimeOffset?>("DueAt").CurrentValue = DateTimeOffset.MaxValue;
            await context.SaveChangesAsync(Cancel);
        }

        await using (var context = Context())
        {
            await using var transaction = await context.Database.BeginTransactionAsync(Cancel);
            var placement = await context.Placements.SingleAsync(p => p.TenantId == "acme" && p.Id == 8, Cancel);
            placement.Region = "us";
            await context.SaveChangesAsync(Cancel);
            Assert.True(await services.GetRequiredService<ReconcileOperations>().RequestAsync(context.Database.WorkTransaction(), Fleet.Placements, new InstanceKey("acme", 8), Cancel));
            await transaction.CommitAsync(Cancel);
        }

        await using var read = Context();
        var stored = await read.Placements.SingleAsync(p => p.TenantId == "acme" && p.Id == 8, Cancel);
        Assert.Equal(("us", 1L), (stored.Region, read.Entry(stored).Property<long>("RequestedRevision").CurrentValue));
        Assert.True(read.Entry(stored).Property<DateTimeOffset?>("DueAt").CurrentValue < DateTimeOffset.MaxValue, "the request makes the placement due");
    }

    private FleetContext Context() => new(database.ConnectionString);
}

internal static class Fleet
{
    public static readonly LeaseSource<InstanceKey> Instances = LeaseSource.Define(
        "fleet-instance",
        StoreLocation.Table("fleet", "instances"),
        InstanceKey.Codec,
        new LeasePolicy { Duration = TimeSpan.FromMinutes(1), PartitionLimit = 4, OwnerLimit = 8, SettleExpiry = true },
        partition: "tenant_id");

    public static readonly ReconcileSource<InstanceKey> Placements = ReconcileSource.Define(
        "fleet-placement",
        StoreLocation.Table("fleet", "placements"),
        InstanceKey.Codec,
        new ReconcilePolicy { Lease = new LeasePolicy { Duration = TimeSpan.FromMinutes(1) } });
}

internal sealed class Instance
{
    public required string TenantId { get; set; }

    public long Id { get; set; }

    public required string Name { get; set; }

    public DateTimeOffset? DueAt { get; set; }
}

internal sealed class Placement
{
    public required string TenantId { get; set; }

    public long Id { get; set; }

    public required string Region { get; set; }
}

/// <summary>The fleet's records; with <paramref name="wakeUps"/>, saved changes of their due times wake the application's dispatchers.</summary>
internal sealed class FleetContext(string connectionString, IWorkSignals? wakeUps = null) : DbContext
{
    public DbSet<Instance> Instances => Set<Instance>();

    public DbSet<Placement> Placements => Set<Placement>();

    protected override void OnConfiguring(DbContextOptionsBuilder builder)
    {
        builder.UseNpgsql(connectionString);
        if (wakeUps is not null)
        {
            builder.AddWorkWakeUps(wakeUps);
        }
    }

    protected override void OnModelCreating(ModelBuilder model)
    {
        model.Entity<Instance>(instance =>
        {
            instance.HasKey(i => new { i.TenantId, i.Id });
            instance.Property(i => i.TenantId).HasColumnName("tenant_id");
            instance.Property(i => i.Id).HasColumnName("id");
            instance.Property(i => i.Name).HasColumnName("name");
            instance.HasLease(Fleet.Instances);
        });
        model.Entity<Placement>(placement =>
        {
            placement.HasKey(p => new { p.TenantId, p.Id });
            placement.Property(p => p.TenantId).HasColumnName("tenant_id");
            placement.Property(p => p.Id).HasColumnName("id");
            placement.Property(p => p.Region).HasColumnName("region");
            placement.HasReconciliation(Fleet.Placements);
        });
    }
}

internal sealed record InstanceKey(string TenantId, long Id)
{
    public static IKeyCodec<InstanceKey> Codec { get; } = new KeyCodec();

    private sealed class KeyCodec : IKeyCodec<InstanceKey>
    {
        public IReadOnlyList<KeyField> Fields { get; } = [new("tenant_id", KeyFieldType.String), new("id", KeyFieldType.Int64)];

        public void Write(InstanceKey key, IKeyWriter writer)
        {
            writer.Write(key.TenantId);
            writer.Write(key.Id);
        }

        public InstanceKey Read(IKeyReader reader) => new(reader.ReadString(), reader.ReadInt64());

        public string Format(InstanceKey key) => key.TenantId + "/" + key.Id.ToString(CultureInfo.InvariantCulture);

        public InstanceKey Parse(string value)
        {
            var slash = value.LastIndexOf('/');
            return new(value[..slash], long.Parse(value.AsSpan(slash + 1), CultureInfo.InvariantCulture));
        }
    }
}
