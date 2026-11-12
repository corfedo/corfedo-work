using Corfedo.Work.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>Typed reads of reconciliation records and ageing of their reconciliation times.</summary>
public abstract class ReconcileRecordScenarios(IReconcileConformanceProvider provider) : ReconcileScenarioBase(provider)
{
    [Fact]
    public async Task C27_ReadsAndAgeingCoverTheReconciliationFields()
    {
        var source = await ReconcileSourceAsync(Reconcile(TimeSpan.FromMinutes(1), p => p with { FailureBackoff = Backoff.Constant(TimeSpan.FromMinutes(10)) }));
        var key = new TestKey("t", 1);
        var missing = new TestKey("t", 2);
        await InsertAsync(source, key, DueState.None, revision: 3);
        var client = await ClientAsync(source);
        var reconciliation = Reconciliation(client);
        var ageing = new RecordAgeing(client.Services.GetRequiredService<IWorkStore>());

        var idle = await reconciliation.ReadAsync(source, key, Cancel);

        Assert.NotNull(idle);
        Assert.Equal((3L, (long?)3, 3L, 0L, 0L, 0L), (idle.RequestedRevision, idle.ClaimedRevision, idle.ConvergedRevision, idle.AttemptedGeneration, idle.ConvergedGeneration, idle.Generation));
        Assert.Equal((null, null, null, null), (idle.FirstRequestedAt, idle.NotBefore, idle.Lease.DueAt, idle.Lease.ExecutionId));
        Assert.Null(await reconciliation.ReadAsync(source, missing, Cancel));

        // A request debounced by an hour, aged by an hour: due, with its first request time moved as well.
        await RequestAsync(client, source, key, new ReconcileRequest { Debounce = Debounce.Trailing(TimeSpan.FromHours(1)) });
        var requested = (await reconciliation.ReadAsync(source, key, Cancel))!;
        Assert.Equal(4, requested.RequestedRevision);
        Assert.InRange(requested.Lease.DueAt!.Value - requested.Lease.ReadAt, TimeSpan.FromMinutes(59), TimeSpan.FromHours(1));
        Assert.True(requested.FirstRequestedAt <= requested.Lease.ReadAt);
        Assert.Null(await client.Operations.ClaimAsync(source.Leases, key, Cancel));
        Assert.True(await ageing.AgeAsync(source, key, TimeSpan.FromHours(1), Cancel));
        var aged = (await reconciliation.ReadAsync(source, key, Cancel))!;
        Assert.Equal(requested.Lease.DueAt - TimeSpan.FromHours(1), aged.Lease.DueAt);
        Assert.Equal(requested.FirstRequestedAt - TimeSpan.FromHours(1), aged.FirstRequestedAt);
        var claim = await client.Operations.ClaimAsync(source.Leases, key, Cancel);
        Assert.NotNull(claim);
        var running = (await reconciliation.ReadAsync(source, key, Cancel))!;
        Assert.Equal((4L, null, client.Owner, 1L), (running.ClaimedRevision, running.FirstRequestedAt, running.Lease.Owner, running.Lease.Epoch));

        // A failed run's backoff floor, aged by the backoff: claimable again.
        Assert.True(await reconciliation.CompleteAsync(claim.Lease, ReconcileResult.Failed("test"), Cancel));
        var failed = (await reconciliation.ReadAsync(source, key, Cancel))!;
        Assert.Equal(1, failed.Lease.Failures);
        Assert.InRange(failed.NotBefore!.Value - failed.Lease.ReadAt, TimeSpan.FromMinutes(9), TimeSpan.FromMinutes(10));
        Assert.Null(await client.Operations.ClaimAsync(source.Leases, key, Cancel));
        Assert.True(await ageing.AgeAsync(source, key, TimeSpan.FromMinutes(10), Cancel));
        var floor = (await reconciliation.ReadAsync(source, key, Cancel))!;
        Assert.Equal(failed.NotBefore - TimeSpan.FromMinutes(10), floor.NotBefore);
        Assert.Equal(failed.Lease.DueAt - TimeSpan.FromMinutes(10), floor.Lease.DueAt);
        claim = await client.Operations.ClaimAsync(source.Leases, key, Cancel);
        Assert.NotNull(claim);
        Assert.True(await reconciliation.CompleteAsync(claim.Lease, ReconcileResult.Converged, Cancel));

        // An invalidation shows as the source's generation; the record's generations stay until a run.
        await InvalidateAsync(client, source);
        var read = Assert.Single(await reconciliation.ReadManyAsync(source, [key, missing], Cancel));
        Assert.Equal((key, 1L, 0L, 0L, 4L), (read.Key, read.Value.Generation, read.Value.AttemptedGeneration, read.Value.ConvergedGeneration, read.Value.ConvergedRevision));
    }
}
