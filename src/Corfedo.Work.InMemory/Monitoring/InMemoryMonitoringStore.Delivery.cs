using Corfedo.Work.InMemory.Database;
using static Corfedo.Work.InMemory.Leases.InMemoryLeaseStore;

namespace Corfedo.Work.InMemory.Monitoring;

internal sealed partial class InMemoryMonitoringStore
{
    /// <summary>The incident of the lease while the lease is held by its owner and epoch by <paramref name="now"/>.</summary>
    private static Record? Held(Record? r, Lease<Guid> lease, DateTimeOffset now) =>
        r is not null
        && r.Uuid(LeaseFields.Default.LeaseOwner) == lease.Owner.Value
        && r.Int64(LeaseFields.Default.LeaseEpoch) == lease.Epoch
        && r.Time(LeaseFields.Default.LeaseExpiresAt) > now
            ? r
            : null;

    public ValueTask<IncidentDelivery?> ReadDeliveryAsync(Lease<Guid> lease, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        var now = Now;
        return ValueTask.FromResult(database.ReadCommitted(() =>
            Held(IncidentRows.Committed.GetValueOrDefault(new RowKey([lease.Key])), lease, now) is { } r
                ? new IncidentDelivery(r.Incident(), r[IncidentColumns.OpenedDeliveredAt] is not null, r[IncidentColumns.ResolvedDeliveredAt] is not null)
                : null));
    }

    public async ValueTask<bool> RecordDeliveryAsync(Lease<Guid> lease, bool resolution, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        using var work = await database.BeginAsync(cancellationToken);
        var now = Now;
        if (Held(work.Get(IncidentRows, new RowKey([lease.Key])), lease, now) is not { } r || (resolution && r[IncidentColumns.ResolvedAt] is null))
        {
            return false;
        }
        Write(work, r, [Set(resolution ? IncidentColumns.ResolvedDeliveredAt : IncidentColumns.OpenedDeliveredAt, now)]);
        work.Commit();
        return true;
    }

    /// <summary>
    /// Gives the incident back, fenced to the lease: due again at once when an event still waits for delivery, such as a
    /// resolution committed meanwhile, otherwise not due.
    /// </summary>
    public async ValueTask<bool> FinishDeliveryAsync(Lease<Guid> lease, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        using var work = await database.BeginAsync(cancellationToken);
        var now = Now;
        if (Held(work.Get(IncidentRows, new RowKey([lease.Key])), lease, now) is not { } r)
        {
            return false;
        }
        var waiting = r[IncidentColumns.OpenedDeliveredAt] is null || (r[IncidentColumns.ResolvedAt] is not null && r[IncidentColumns.ResolvedDeliveredAt] is null);
        Write(work, r, [
            Set(LeaseFields.Default.LeaseExpiresAt, null),
            Set(LeaseFields.Default.Takeovers, 0),
            Set(LeaseFields.Default.Failures, 0),
            Set(LeaseFields.Default.DueAt, waiting ? now : null),
        ]);
        work.Commit();
        return true;
    }
}
