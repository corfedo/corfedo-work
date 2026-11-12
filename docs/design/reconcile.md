# Reconciliation

Reconciliation is for records that state what they should be. A request increments the record's requested revision;
a run converges the world to that revision and records the result on the record. It never gives up and never
creates a second record of the work.

Requirements: C1–C10, plus everything [leases](leases.md) provide.

## Fields

A reconciliation source carries the [lease fields](leases.md#lease-fields) and:

| Field | Type | Meaning |
|---|---|---|
| `RequestedRevision` | 64-bit integer | Incremented by every request. |
| `ClaimedRevision` | 64-bit integer, nullable | The requested revision the current or last run started from. |
| `ConvergedRevision` | 64-bit integer | The latest revision a run converged. |
| `FirstRequestedAt` | timestamp, nullable | The first request not yet claimed; bounds the debounce. |
| `NotBefore` | timestamp, nullable | Backoff floor after a failure. |
| `AttemptedGeneration` | 64-bit integer | The source generation the latest run started from. |
| `ConvergedGeneration` | 64-bit integer | The source generation the latest converged run saw. |

Field names are defaults (`requested_revision`, ... in snake case on PostgreSQL); a source maps them with
`ReconcileFields`. A reconciliation source derives its executions (see [runs and results](#runs-and-results)), so its
records need no `ExecutionId` field, and its lease policy cannot set `MaximumTakeovers`: reconciliation never gives up.

```csharp
public static readonly ReconcileSource<PlacementKey> Placements = ReconcileSource.Define(
    name: "placement",
    store: StoreLocation.Table("scheduling", "placements"),
    key: PlacementKey.Codec,
    partition: "TenantId",
    policy: new ReconcilePolicy
    {
        Lease = new LeasePolicy
        {
            Duration = TimeSpan.FromMinutes(1),
            FailureBackoff = Backoff.Exponential(TimeSpan.FromSeconds(5), TimeSpan.FromMinutes(10)),  // Failed results
            PartitionLimit = 4,
        },
        Requests = RequestPolicy.Supersede,          // default FollowUp
        ResyncInterval = TimeSpan.FromHours(1),      // optional
    });
```

`Placements.Leases` is the source's lease view: claims, renewals, guards and cancellation requests are the
[lease operations](leases.md#operations) on it, and they follow the rules below. Releases, ends and schedules are
refused, since the reconciliation layer owns `DueAt`: requests and invalidations make a record due, and a result gives
it back (`ReconcileOperations.CompleteAsync` for a holder that claimed through the lease view).

The domain keeps its own outcome fields (for example a status, a waiting reason and an error code) and writes them in
the same transaction as the result.

`ReconcileOperations.ReadAsync` and `ReadManyAsync` read these fields with the lease fields and the source's current
generation (`ReconcileRecord`), as lease reads do.

## Claimability

A reconciliation record is claimable when the [lease rule](leases.md#validity-and-claimability) holds with its first
condition replaced by:

> `NotBefore` is null or not later than `now`, and either `DueAt` is not later than `now` or `AttemptedGeneration` is
> older than the source's current generation.

So backoff beats invalidation, and an invalidation makes every record of the source due once, including records
with no due time: the claim sets `AttemptedGeneration`, and after a `Waiting` or `Failed` result the record waits for
its retry time like any other (C6). Only `Converged` advances `ConvergedGeneration`, so "attempted under this policy"
and "converged under this policy" stay distinguishable. Whether a record should converge at all, for example one the
domain deleted or suspended, is the reconciler's decision: it reads the record and returns `Converged`.

`DueAt` is never earlier than `NotBefore`: a failure sets both, and requests keep a due time at
or after the floor (a request that resets backoff clears the floor instead). `LeaseOperations` and the stores refuse
`ScheduleAsync` on reconciliation sources, so nothing else sets the due time. So the next due time of a record in
backoff is its `DueAt`, and a dispatcher wakes for it without polling.

A claim sets `ClaimedRevision` to `RequestedRevision`, `AttemptedGeneration` to the current generation and clears
`FirstRequestedAt`; the claim reports both as `LeaseClaim.Reconcile`.

On `Supersede` sources the lease's "not held" condition is relaxed as well: a lease from an obsolete generation is
retired by the claim that replaces it, as described under [requests](#requests).

A claim never consumes its own trigger without leaving recoverable work behind: when it claims a record through the
generation condition, it also sets `DueAt` to the claim time in the same statement. If the holder crashes before a
result, the record is still due and is claimed again once the lease is settled or taken over. Every result then sets
`DueAt` explicitly.

## Requests

```csharp
await reconcile.RequestAsync(tx, Placements, key, cancellationToken);   // ReconcileOperations
await reconcile.RequestAsync(tx, Placements, key, new ReconcileRequest
{
    Debounce = Debounce.Trailing(TimeSpan.FromSeconds(5), maximum: TimeSpan.FromMinutes(1)),
    ResetBackoff = true,                       // explicit: clears NotBefore and failures (C10)
}, cancellationToken);
```

A request locks the record, increments `RequestedRevision` (C1) and sets `FirstRequestedAt` if it is null. Its due
time is the trailing debounce from now (now without a debounce), capped at `FirstRequestedAt` plus the maximum (C3).
A record with no outstanding request and no run keeps an earlier due time it already has (a waiting record due
sooner, an ordinarily due one); the due time is never earlier than `NotBefore` unless the request resets backoff.
What happens to a record a run holds depends on the source's request policy:

| Policy | During a run |
|---|---|
| `FollowUp` (default) | The request records its due time; the record is not claimable while held, and the run's result schedules exactly one follow-up from it (C2). |
| `Supersede` | The request gives the current lease back on the holder's behalf: `LeaseExpiresAt` becomes null, the epoch stays, `Takeovers` and `Failures` are unchanged and the lease stops counting against limits. The record is claimable at its due time, without takeover backoff, and the next claim is not a takeover. The old holder's renewals, guards and results fail validity and change nothing; its process is signalled to renew at once, and its token is cancelled with reason `Superseded` (C9). |

`RequestManyAsync(tx, source, keys, request)` requests many records of one source in one round trip, each by the
rules above, and returns the keys of the records that exist; missing records are skipped as `RequestAsync` returns
false for them, and duplicate keys request a record once. It signals claimers once and each superseded holder once. It
locks the records in ordinal order of their canonical keys, so bulk requests of overlapping records do not deadlock
each other (C-26). A transaction that requests several records of one source does so in one bulk request: requests
for single records in another order can deadlock with it.

A superseding request waits for an open guarded transaction of the old holder, because both lock the record: either
the guarded output commits first and the request then supersedes the next step, or the request commits first and the
guard fails.

Invalidation (`InvalidateAllAsync`) supersedes running attempts on `Supersede` sources through a generation fence
instead of writing every record. On those sources, every operation that commits a holder's output in the caller's
transaction (`GuardAsync`, `RenewAsync` in a transaction, `CompleteAsync`, `EndAsync`) holds the source's generation
lock in shared mode until that transaction ends, and requires the generation the run claimed (its
`AttemptedGeneration`) to still be current; invalidation takes the lock exclusively. That transaction must be READ
COMMITTED, where the store has isolation levels: the generation check reads the generation after the lock is granted,
and a snapshot taken earlier could predate an invalidation that committed meanwhile. Other levels are refused with an
error that aborts the transaction, so output written after ignoring the refusal cannot commit. So an invalidation that commits
first makes later guards fail, an invalidation issued while a guarded transaction is open waits for it to end, and a
guard that starts while an invalidation waits queues behind it, so a stream of guards cannot keep an invalidation
waiting. Renewals and releases in their own transaction compare the claimed generation without the lock; a renewal
that finds it stale cancels the run's token.

Generation locks are always taken before row locks. Every fenced operation takes the lock before it locks its record,
and the provider takes several sources' locks in one fixed order. A transaction must follow the same order for its own
row locks, otherwise a guard queued behind an invalidation can wait for a row the transaction holds while the
invalidation waits for the transaction, and the store aborts one of them as a deadlock:

- A transaction that commits holder output on `Supersede` sources fences first: it starts with `run.GuardAsync(tx)`,
  or with `reconcile.FenceAsync(tx, sources)` when it handles several leases or sources, before its own writes and
  row locks. Shared domain updates and several leases in one transaction are fine after that.
- A transaction that invalidates calls `InvalidateAllAsync` before its own writes and row locks; one that invalidates
  several sources does so in ordinal order of their names, the order every fence uses.

```csharp
await using var tx = await store.BeginAsync(cancellationToken);
await reconcile.FenceAsync(tx, [Placements, Assignments], cancellationToken);   // before any row lock
// ... shared domain writes, guards and completions of several runs ...
await tx.CommitAsync(cancellationToken);
```

Because invalidation writes no records, the obsolete lease is retired lazily, by the claim that replaces it. On a
`Supersede` source, a record whose lease is outstanding but whose `AttemptedGeneration` is older than the current
generation counts as **not held** for the [claim rule](leases.md#validity-and-claimability), whether or not the lease
has expired. The claim:

1. locks the record, skipping it if an open guarded transaction holds the lock, as for any claim;
2. re-evaluates, with a fresh clock reading and a fresh reading of the generation, that the outstanding lease's
   generation is still older than the current one. The claim takes no lock on the generation: generations only grow,
   so a lease found obsolete stays obsolete, and an invalidation that commits during the claim leaves the new lease
   obsolete in turn, to be retired by a later claim. A claim therefore never waits for an invalidation while it holds
   record locks;
3. retires the obsolete lease as a supersession: it stops counting against limits, `Takeovers` and `Failures` stay
   unchanged, no takeover backoff applies and the claim is not a takeover. An obsolete lease that has expired on a
   source that settles expiry is settled first, as for any expired lease; one that has not expired has a live holder
   and runs no expiry handler;
4. claims the record in the same transaction, counting the new lease against limits after the retirement: the
   replaced lease's slot is reused, and the claim adds nothing to its partition. An owner at its owner limit can
   still claim records whose obsolete lease it holds itself.

Until something ends it, an obsolete lease keeps counting against limits. Limited sources count a lease while it is
outstanding, not while it is unexpired: a claim that replaces it ends it at once, and an obsolete lease whose record
is not claimable (its `NotBefore` has not passed) ends only when its expiry is settled, after the lease expires. Settlement
waits for record locks and for failing expiry handlers' settlement backoff, and needs a process that runs expiry
settlement for the source, so that slot can stay counted longer than one lease duration.

The obsolete holder may keep running until its next renewal cancels its token; it can no longer renew, guard or
complete. Limits bound valid ownership, so on `Supersede` sources an obsolete run's last moments overlap with its
replacement. A source that cannot accept that overlap uses `FollowUp`.

Checks against revisions owned by the domain elsewhere (another table's policy or catalogue revision) belong to the
application's guarded transactions: the run captures them when it starts and compares them before committing output.

## Runs and results

```csharp
work.AddPool("convergence", capacity: 8)
    .AddReconciler<PlacementKey, PlacementReconciler>(Placements, r => r.Pool("convergence"));

public sealed class PlacementReconciler : IReconciler<PlacementKey>
{
    public async ValueTask<ReconcileDisposition> ReconcileAsync(ReconcileRun<PlacementKey> run, CancellationToken cancellationToken)
    {
        // ... converge, committing intermediate output with run.GuardAsync(tx) ...
        await using var tx = await store.BeginAsync(cancellationToken);
        await run.GuardAsync(tx, cancellationToken);   // a Supersede source: fence before the transaction's writes
        record.Status = PlacementStatus.WaitingForCapacity;
        await run.CompleteAsync(tx, ReconcileResult.Waiting(retryAfter: TimeSpan.FromMinutes(1)), cancellationToken);
        await tx.CommitAsync(cancellationToken);
        return run.Completed;
    }
}
```

| Result | Effect on the record |
|---|---|
| `Converged` | `ConvergedRevision` and `ConvergedGeneration` advance to what the run claimed; failures reset and `NotBefore` clears. Due at the time a request during the run recorded (with its debounce), after the resync interval if the source has one (C7), otherwise null. |
| `Waiting(retryAfter)` | Due at the retry time, or at the time a request during the run recorded if that is sooner; failures unchanged (C4). A new request can make it due sooner. |
| `Failed(code)` | Failures increment; `NotBefore` and the due time move by the source's `FailureBackoff` for the earlier failures, exponential with a cap (C5). The code is for logs: the dispatcher reports a failed result it applies as `reconcile.failed` with the code; the domain stores its own error fields. |

Every result gives the lease back and sets `DueAt` explicitly. On a `FollowUp` source invalidated during the run, a `Converged` result records the generation the run started
from, so the record is still stale and runs once more under the new generation.

A reconciler returns a `ReconcileDisposition`: a `ReconcileResult`, which converts implicitly, or `run.Completed`.
`ReconcileRun` offers guards, renewals and completion, not its lease run, so a result is the only way a run gives the
record back.

`run.CompleteAsync(tx, result)` applies the result in the domain's transaction, guarded like every holder write, so
the domain's status and the record's work state commit together; the reconciler then returns `run.Completed`. If that
transaction rolls back, nothing changed and the run still holds the record and can complete again (D3). A reconciler
may instead return the result; the dispatcher then applies it in its own guarded transaction, no longer atomic with
domain output. A thrown exception is `Failed("handler_failed")`, and a run that returns `run.Completed` without a
committed completion is applied as `Failed("uncompleted")` and reported as `reconcile.uncompleted`. Every result the
dispatcher applies is fenced, so it changes nothing once the lease was lost or superseded. During shutdown a run
that throws is given back as it is, like a lease run. `run.Completed` is not a result, so it cannot be passed to
`CompleteAsync`.

Two cases have no result: a claim whose hold ran out before its run started, and a run whose result could not be
applied (the store failed). The dispatcher gives the record back as a failed [lease run](leases.md#dispatching-due-records):
`Failures` increments and the record is due after the failure backoff, without a `NotBefore` floor, so a request can
make it due sooner. The release is fenced like every holder write, so it changes nothing once the lease expired or
was superseded.

`ReconcileRun.ExecutionId` is derived from the source, the canonical key, the claimed revision and the attempted
generation (`ReconcileExecution.Id`, a name-based UUID every provider computes alike): retries of the same revision
keep it, a new request or invalidation produces a new one. External operations deduplicate on it (R17).

## Mass invalidation

`InvalidateAllAsync(tx, source)` increments the source's generation, stored once in the framework's tables, and
returns it. It writes no records (C6). Claims serve stale records through an index on `AttemptedGeneration`, in index
order, and alternate them one for one with records due by time, so that neither an
invalidation's backlog nor a stream of requests keeps the other waiting. The alternation continues across the
process's claims of the source, so a dispatcher with one free slot serves both kinds. A record that is both due by
time and stale is claimed once.

A claim hook that fails on a reconciliation record counts a failure and sets `NotBefore` with the due time, as a
`Failed` result does, so a record due only because of an invalidation waits for the failure backoff as well.

## Fairness across sources

Several reconciliation (and lease) sources can share a dispatch pool. The pool's capacity is shared and claims
rotate between sources, so a source with a large backlog cannot keep another's due records waiting for more than one
rotation (C8). See [hosting](hosting.md#pools).
