using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Corfedo.Work;

/// <summary>
/// Maps the work fields of a source onto the entity of its records, so the application's migrations create them with
/// the indexes the store verifies at startup. Each field is a property named after its column in Pascal case, such as
/// <c>DueAt</c> for <c>due_at</c>: an entity's own property of that name and type is used, otherwise a shadow property.
/// </summary>
public static class WorkModelExtensions
{
    /// <summary>The annotation on a mapped <c>DueAt</c> property naming its source, whose claimers saved changes wake.</summary>
    internal const string DueSource = "Corfedo.Work:DueSource";

    /// <summary>
    /// Maps the entity to the source's table with the lease fields, and adds the indexes the source's claims, expiry
    /// settlement and limits use. Map the key and partition columns before calling it.
    /// </summary>
    /// <exception cref="ArgumentException">The source is a reconciliation source's; map it with <see cref="HasReconciliation"/>.</exception>
    public static EntityTypeBuilder<TEntity> HasLease<TEntity, TKey>(this EntityTypeBuilder<TEntity> entity, LeaseSource<TKey> source)
        where TEntity : class
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(entity);
        ArgumentNullException.ThrowIfNull(source);
        if (source.Reconcile is not null)
        {
            throw new ArgumentException($"Lease source {source.Name} reconciles; map its records with HasReconciliation.", nameof(source));
        }
        MapLease(entity, source);
        return entity;
    }

    /// <summary>
    /// Maps the entity to the source's table with the lease and reconciliation fields, and adds the indexes its claims
    /// and limits use. Map the key and partition columns before calling it.
    /// </summary>
    public static EntityTypeBuilder<TEntity> HasReconciliation<TEntity, TKey>(this EntityTypeBuilder<TEntity> entity, ReconcileSource<TKey> source)
        where TEntity : class
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(entity);
        ArgumentNullException.ThrowIfNull(source);
        MapLease(entity, source.Leases);
        var r = source.Fields;
        Counter<long>(entity, r.RequestedRevision);
        Field<long?>(entity, r.ClaimedRevision);
        Counter<long>(entity, r.ConvergedRevision);
        Field<DateTimeOffset?>(entity, r.FirstRequestedAt);
        Field<DateTimeOffset?>(entity, r.NotBefore);
        Counter<long>(entity, r.AttemptedGeneration);
        Counter<long>(entity, r.ConvergedGeneration);
        Index(entity, source.Store, [r.AttemptedGeneration], null, "generation");
        return entity;
    }

    private static void MapLease<TKey>(EntityTypeBuilder entity, LeaseSource<TKey> source)
        where TKey : notnull
    {
        entity.ToTable(source.Store.Name, source.Store.Schema);
        var f = source.Fields;
        entity.Property<DateTimeOffset?>(Name(f.DueAt)).HasColumnName(f.DueAt).HasAnnotation(DueSource, source.Name);
        Field<Guid?>(entity, f.LeaseOwner);
        Counter<long>(entity, f.LeaseEpoch);
        Field<DateTimeOffset?>(entity, f.LeaseExpiresAt);
        Counter<int>(entity, f.Takeovers);
        Counter<int>(entity, f.Failures);
        Counter<long>(entity, f.SettledEpoch);
        Counter<int>(entity, f.SettleFailures);
        Field<DateTimeOffset?>(entity, f.SettleRetryAt);
        Field<DateTimeOffset?>(entity, f.CancelRequestedAt);
        Field<Guid?>(entity, f.ExecutionId);
        Field<Guid?>(entity, f.EndedCompletionId);
        Field<byte[]?>(entity, f.EndedDigest);
        var held = $"{Quote(f.LeaseExpiresAt)} IS NOT NULL";
        var outstanding = $"{held} AND {Quote(f.SettledEpoch)} < {Quote(f.LeaseEpoch)}";
        Index(entity, source.Store, [f.DueAt], $"{Quote(f.LeaseExpiresAt)} IS NULL", "due");
        Index(entity, source.Store, [f.LeaseExpiresAt], held, "expiry");
        if (source.Settles)
        {
            Index(entity, source.Store, [f.LeaseExpiresAt], outstanding, "settle");
        }
        if (source.Partition is { } partition && (source.Policy.PartitionLimit is not null || source.Policy.SpreadLimit is not null))
        {
            var property = entity.Metadata.GetProperties().FirstOrDefault(p => p.GetColumnName() == partition)
                ?? throw new InvalidOperationException($"Lease source {source.Name}: map the partition column {partition} of {entity.Metadata.DisplayName()} before HasLease.");
            entity.HasIndex([property.Name, Name(f.LeaseOwner)], IndexName(source.Store, partition, "partition")).HasFilter(outstanding);
        }
        if (source.Policy.OwnerLimit is not null)
        {
            Index(entity, source.Store, [f.LeaseOwner], held, "owner");
        }
    }

    private static void Field<T>(EntityTypeBuilder entity, string column) => entity.Property<T>(Name(column)).HasColumnName(column);

    /// <summary>A counter or epoch: not null, zero by default, so rows inserted without it start at zero.</summary>
    private static void Counter<T>(EntityTypeBuilder entity, string column)
        where T : struct => entity.Property<T>(Name(column)).HasColumnName(column).IsRequired().HasDefaultValue(default(T));

    private static void Index(EntityTypeBuilder entity, StoreLocation table, string[] columns, string? filter, string purpose) =>
        entity.HasIndex([.. columns.Select(Name)], IndexName(table, columns[0], purpose)).HasFilter(filter);

    private static string IndexName(StoreLocation table, string column, string purpose) => $"ix_{table.Name}_{column}_{purpose}";

    private static string Quote(string column) => "\"" + column.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    /// <summary>The property of a column: its name in Pascal case, such as <c>DueAt</c> for <c>due_at</c>.</summary>
    private static string Name(string column) =>
        string.Concat(column.Split('_', StringSplitOptions.RemoveEmptyEntries).Select(part => char.ToUpperInvariant(part[0]) + part[1..]));
}
