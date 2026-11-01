# Reconciliation

Some rows describe what should exist, and the work makes the world match: a site's DNS records, a tenant's search
index, a published page. A change bumps the row's requested revision in its transaction, and a reconciler brings the
world in line with the row as it is now.

- Ten changes before the run start one run.
- A change during a run causes one follow-up run.
- A failed run retries with a backoff and never gives up on its own.

## Define the source

```csharp
public static readonly ReconcileSource<long> Sites = ReconcileSource.Define("sites",
    StoreLocation.Table("public", "sites"),
    KeyCodec.Int64("id"),
    new ReconcilePolicy
    {
        Lease = new LeasePolicy { Duration = TimeSpan.FromMinutes(1), FailureBackoff = Backoff.Exponential(TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(10)) },
    });

work.AddReconciler<long, SiteReconciler>(Sites);
```

The table carries the lease columns (see [leases](leases.md), without `execution_id`) and the revision columns
`requested_revision`, `claimed_revision`, `converged_revision`, `first_requested_at`, `not_before`,
`attempted_generation` and `converged_generation`, with an index on `attempted_generation`. With EF Core,
`HasReconciliation` maps them.

## Request and reconcile

```csharp
await reconcile.RequestAsync(db.Database.WorkTransaction(), Sites, siteId, ct);

public sealed class SiteReconciler(AppDb db) : IReconciler<long>
{
    public async ValueTask<ReconcileDisposition> ReconcileAsync(ReconcileRun<long> run, CancellationToken ct)
    {
        var site = await db.Sites.FindAsync([run.Key], ct);
        if (site is null || site.Suspended)
        {
            return ReconcileResult.Converged;   // nothing to apply
        }
        // Apply the row as it is now; run.Revision is the revision this run converges to.
        return ReconcileResult.Converged;
    }
}
```

Return `ReconcileResult.Waiting(retryAfter)` when the world is not ready yet, or `ReconcileResult.Failed(code)` to
retry after the backoff. The result is how a run gives the row back: the framework sets the row's due time from
requests and results, so the lease operations that release, end or schedule a row refuse reconciliation sources.

## More

- **Debounce.** `new ReconcileRequest { Debounce = ... }` waits for a quiet period, with a ceiling.
- **Mass invalidation.** After a change of shared configuration, `reconcile.InvalidateAllAsync(tx, Sites)` runs every
  row of the source once, without writing any of them.
- **Periodic runs.** `ResyncInterval` runs converged rows again after an interval.
- **Supersede.** `RequestPolicy.Supersede` cancels a running run when a newer request arrives, instead of a follow-up.

The full semantics are in the [reconciliation design](../design/reconcile.md).
