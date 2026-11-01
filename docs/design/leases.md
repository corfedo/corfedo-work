# Leases

Leases give one holder at a time fenced ownership of a record the application owns. They know nothing about the
record's states; the domain decides when the record is eligible and what claiming it means.

Requirements: R2–R9, R11, R13, R17, E1–E13, W1–W4 in [problems](problems.md).

## Lease fields

A lease source's records carry these fields next to the domain's own:

| Field | Type | Meaning |
|---|---|---|
| `DueAt` | timestamp, nullable | When the record next needs a run. Null: no run wanted. |
| `LeaseOwner` | owner id, nullable | The current or last holder. |
| `LeaseEpoch` | 64-bit integer | Incremented on every claim; never decreases. |
| `LeaseExpiresAt` | timestamp, nullable | When the current lease ends unless renewed. Null: not held. |
| `Takeovers` | integer | Consecutive claims that took the record over from an expired lease. |
| `Failures` | integer | Consecutive releases after a failed attempt. |
| `SettledEpoch` | 64-bit integer | The latest epoch whose expiry was settled. Required for sources that settle expiry (`SettleExpiry`) and for sources with any store-enforced limit. |
| `SettleFailures`, `SettleRetryAt` | integer and timestamp, nullable | Consecutive failed settlements of the expired epoch and when the next may run. Required for sources that settle expiry. |
| `CancelRequestedAt` | timestamp, nullable | Cancellation is requested for the record. |
| `ExecutionId` | identifier, nullable | The current deliberate execution; kept across its attempts, replaced for a new one. The first claim of a record without one assigns one. Not used on reconciliation sources, which derive executions from revisions. |
| `EndedCompletionId`, `EndedDigest` | identifier and bytes, nullable | Identity and payload digest of the transition that ended the record, for replays (W4). |

Field names are defaults (`due_at`, `lease_owner`, ... in snake case on PostgreSQL); a source maps them to the record's own names
with `LeaseFields`. Several sources may live on one table, each
with its own identity and its own fields (E13).

## Validity and claimability

These two predicates are the whole contract; every operation is defined by them.

A lease is **valid** for `(owner, epoch)` when `LeaseOwner = owner`, `LeaseEpoch = epoch` and
`LeaseExpiresAt > now`, where `now` is the store's clock. Every operation that depends on validity first acquires the
record's lock and only then evaluates validity against the record's latest committed version, with a clock reading
taken after the lock is held. A clock read before a lock wait is never used. An expired lease is invalid even if
nobody has claimed the record since; it cannot be renewed back to life.

A lease is **outstanding** from its claim until it is released, ended, superseded, taken over or settled. Expiry
alone does not end it: an expired lease keeps counting against limits until a transition that locks the record
settles it (see [limits](#limits)). Releasing, ending and superseding set `LeaseExpiresAt` to null.

A record is **claimable** when all of the following hold:

1. `DueAt` is not null and not later than `now`;
2. it is not held: `LeaseExpiresAt` is null, or it is not later than `now` minus the takeover backoff for the
   record's `Takeovers` (E9); a null `LeaseExpiresAt` means the previous lease was given back, so the claim is not a
   takeover and no backoff applies; on reconciliation sources with the `Supersede` policy, a lease from an obsolete
   generation also counts as not held ([lazy supersession](reconcile.md#requests));
3. if the source settles expiry and the previous lease expired, its expiry is settled or settles in the claim
   transaction (E10); a claimer without the source's expiry handler leaves such a record to the processes that run it;
4. the claim stays within the source's limits (below).

Takeover backoff is the source's `TakeoverBackoff` for the record's `Takeovers` before the claim: the first takeover
waits the initial delay, and each later one the delay for one more previous takeover (`Backoff.Exponential` doubles it
up to its maximum).

Claiming does not clear `DueAt`. If a holder dies, the record is claimable again once the lease expires and the
backoff passed, with no sweep and no re-dispatch message (R5). The domain moves `DueAt` when its state changes: now
or later for work to do, null when no run is wanted.

## Sources

```csharp
public static readonly LeaseSource<InstanceKey> Instances = LeaseSource.Define(
    name: "agent-instance",
    store: StoreLocation.Table("agents", "instances"),
    key: InstanceKey.Codec,                       // canonical, round-trippable; composite keys in declared order
    partition: "TenantId",                        // optional
    policy: new LeasePolicy
    {
        Duration = TimeSpan.FromMinutes(2),
        MaximumTakeovers = 3,
        TakeoverBackoff = Backoff.Exponential(TimeSpan.FromSeconds(5), TimeSpan.FromMinutes(5)),
        FailureBackoff = Backoff.Exponential(TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(10)),
        SettleExpiry = true,                        // expired leases hold resources, see expiry settlement
        PartitionLimit = 4,                         // store-enforced limits, see below
    });
```

The key codec is written by the application; the framework does not inspect key types. It maps a key to its fields
and to a canonical string (for tokens and telemetry) and back without loss. At startup the provider verifies that the
mapped fields and the indexes the claim statements need exist.

## Operations

All times are the store's (R4). Due times are given as `Due` values resolved with the store's clock: `Due.Now`,
`Due.After(delay)`, `Due.None` (no run wanted) and `Due.Unchanged`; the API takes no absolute process
times. Operations on a held record require a valid lease and change nothing otherwise, reporting the lease as lost
(R3): `RenewAsync` returns an invalid result, `ReleaseAsync` returns false, and `GuardAsync` and `EndAsync` throw
`LeaseLostException` so that the caller's transaction does not commit.

The operations are methods of `LeaseOperations`. Claims run the source's registered claim hook and expiry handler,
and default to the process's owner id; an API acting for a remote holder passes the holder's owner id. On job sources
the job layer owns the lifecycle: claims, releases, ends, schedules and cancellation requests through
`LeaseOperations` throw `ArgumentException`, also for a lease parsed from a token; renewals and guards remain. Job
dispatchers claim and release through the store, and jobs end through the job store's transitions. On
reconciliation sources the reconciliation layer owns `DueAt`: releases, ends and schedules through `LeaseOperations`
throw `ArgumentException`, also for a lease parsed from a token; claims, renewals, guards and cancellation requests
remain, and a holder gives a record back with a result through `ReconcileOperations.CompleteAsync`.

| Operation | Effect |
|---|---|
| `ClaimDueAsync(source, limit)` | Claims up to `limit` claimable records, earliest due first, skipping records another claim is taking (E2, R6). It may commit in several steps; a failure after a step committed returns the claims committed so far. |
| `ClaimAsync(source, key)` | Claims one specific record if it is claimable (E3). |
| `ClaimAsync(tx, source, candidates, limit)` | Inside the caller's transaction, claims up to `limit` of the given keys that are claimable. The caller rechecks domain eligibility and reserves resources in the same transaction (E7). |
| `RenewAsync(lease)` | Extends a valid lease. Returns whether it is still valid and whether cancellation is requested (R8). |
| `RenewAsync(lease, tx)` | The same inside the caller's transaction, so progress written in it commits with the renewal (E4). |
| `GuardAsync(lease, tx)` | Inside the caller's transaction: requires a valid lease, extends it, and locks the record against claims and other writers until the transaction ends. Every transaction that commits a holder's output calls it (R3, E4). |
| `ReleaseAsync(lease, due, failed)` | Gives the record back, due at `due`; counts a failure when `failed` (E5, E11). Never counts a takeover. |
| `EndAsync(lease, tx, completion)` | In the caller's transaction: clears the lease, sets `DueAt` to null, clears cancellation and records the completion identity (E6, W4). Returns `Ended`, or `Replayed` for a completion already recorded. |
| `ScheduleAsync(tx, source, key, due, execution)` | Sets `DueAt` in the caller's transaction and wakes claimers after commit (R1, R7). A new `execution` id marks a deliberate new execution; omitted, the current one continues. Refused on reconciliation sources. |
| `RequestCancelAsync(tx, source, key)` | Sets `CancelRequestedAt` in the caller's transaction and wakes the holder's process (R8). |
| `ClearCancelAsync(tx, source, key)` | Withdraws a cancellation request. |

`GuardAsync` modifies the record on purpose: it is a fenced renewal inside the caller's transaction, and it holds
the record's write lock until that transaction ends. While it is held:

- claims skip the record, and settlement and takeover, which must lock the record, cannot happen, so the lease stays
  outstanding and keeps counting against every limit even after its previously committed expiry passes;
- renewals from the holder's background loop wait for the transaction and then evaluate validity as usual: they
  succeed if the expiry the guard set is still in the future, and fail otherwise, which happens only when the guarded
  transaction ran longer than a lease duration;
- stores whose transactions detect conflicts only on writes (document stores) get the conflict they need.

The guard therefore authorizes the whole transaction through commit, and ownership and capacity accounting agree at
every moment. Short guarded transactions matter for liveness, not for correctness.

Lock order is the application's. Claimers never wait for locked records, and a claim in a transaction of the framework
waits for a limit scope only while it holds no record lock, so neither a guard nor an application transaction that
claimed in a limit scope can deadlock with a claim; deadlocks can arise only between application transactions, which
must lock records in a consistent order. When the
work record is the domain record being changed, guard first and then write it. When the work record is separate (a
job), guard after the domain's own locks, to hold the work record's lock for the shortest time.

Domain code may write `DueAt` through its own data access; such changes are picked up by polling and next-due timers
instead of an immediate wake-up. The EF package emits the wake-up for tracked changes of mapped `DueAt` properties on
save: a context configured with `AddWorkWakeUps(signals)` sends `work.due` for the sources of new records with a due
time and of changed due times, on commit of the context's transaction, or after the save without one.

## Claims, takeovers and exhaustion

A claim sets the owner, increments the epoch and sets the expiry. A claim of a record whose previous lease expired is
a takeover and increments `Takeovers`; a release or end by a valid holder resets it (R9). Failed releases increment
`Failures`, successful releases and ends reset it (E11); the domain reads both and may keep its own counters.

A source can register a **claim hook** that runs inside the claim transaction, before the claim commits (E8):

```csharp
public sealed class InstanceClaimed : ILeaseClaimHook<InstanceKey>
{
    public ValueTask ClaimedAsync(LeaseClaim<InstanceKey> claim, WorkTransaction tx, CancellationToken cancellationToken)
    {
        // claim.Reason: Due, ByKey, Takeover; claim.PreviousOwner, claim.PreviousEpoch
        // Settle the interrupted attempt, start a new one, journal it. No external calls here.
    }
}
```

A failing hook rolls the claim back. A claim by key reports the hook's exception and leaves the record as it was.
Claims of several records roll back only the record whose hook failed, log it and count a failure: `Failures`
increments and the record is due again after the source's `FailureBackoff`, so a record whose hook keeps failing does
not hold back the records due after it. An expiry handler failing in a claim counts as a failed settlement (see
[expiry settlement](#expiry-settlement)). Errors of the provider's own statements are not counted against the record;
they abort the claim. Hooks and expiry handlers are resolved in a new DI scope for every call.

When a takeover would make `Takeovers` exceed `MaximumTakeovers`, the claim still succeeds after the takeover backoff but is marked
exhausted: the dispatcher hands it to the exhaustion handler, which must end the record or release it with a later due
time, recording why in the domain's own fields. The framework never decides a domain outcome on its own.

## Expiry settlement

Some leases hold resources (capacity reservations, credentials) that must be released when the holder disappears,
whether or not anyone claims the record again (E10). Such a source sets `SettleExpiry` in its policy, so every process
treats its expired leases alike, and the processes that can release the resources register an expiry handler
(`OnExpired`). Each expired epoch gets a settlement transition identified by `(source, key, expired epoch)`:

- an expiry processor, run by every process that registers the expiry handler, finds expired leases with
  `SettledEpoch < LeaseEpoch`, skipping locked records, and settles each in its own transaction: it locks the record,
  re-evaluates expiry with a fresh clock reading, runs the handler and sets `SettledEpoch`;
- a claim of such a record by a process with the handler settles it first, in the claim transaction; a claimer
  without the handler skips the record until it is settled;
- both paths settle an epoch at most once; a failed settlement rolls back the handler's work, counts in
  `SettleFailures` and is retried after the source's `SettlementBackoff` (exponential from one second to five minutes
  by default; with `Backoff.None` on every pass of the expiry processor); until then neither the expiry processor nor
  a claim runs the handler for it, and the record is not claimed. A failed settlement is not a failed attempt of the record: `Failures` and `DueAt` are unchanged;
- the expiry processor takes records one at a time, passing records another transaction has locked and records whose
  handler failed in the same pass, so neither keeps later records from being settled;
- the expiry processor runs at the earliest known expiry and at least every 750 ms, also when it knows of no expiry:
  a claim can commit after its lease expired and a record can stay locked past expiry, and neither is signalled. The
  wait bounds how long such a lease waits once it is committed, expired and unlocked;
- a settlement wakes claimers, since it frees a slot of the source's limits and makes the record claimable for
  claimers without the handler.

Settlement records `SettledEpoch` and leaves `LeaseExpiresAt` as it is, so the record keeps its takeover backoff and
the next claim of it is still a takeover.

Recovery of the work itself still needs no sweep; only resource cleanup does.

## Cancellation

Cancellation belongs to the record, not to one claim. `RequestCancelAsync` sets `CancelRequestedAt`; every holder from
then on sees it: the current holder at its next renewal, a later claimant in its claim result. The holder observes the
token, then ends the record (which clears the request) or releases it. The domain withdraws a request it no longer
wants with `ClearCancelAsync`, which also makes the record runnable again by the normal due rules. A record that must
not run while cancellation is pending has its `DueAt` set to null by the domain.

## Limits

| Limit | Scope | Enforced |
|---|---|---|
| Capacity | Records of the source held by this process. | By the dispatcher. |
| Partition limit | Records of one partition held across all processes (R13). | By the store, on every claim path. |
| Spread limit | Records of one partition held by one process (E12). | By the store, on every claim path. |
| Owner limit | Records of the source held by one owner, local or remote (W3). | By the store, on every claim path. |

Limits are part of the source's `LeasePolicy` (`PartitionLimit`, `SpreadLimit`, `OwnerLimit`). Partition and spread
limits need a partition field that is never null.

Store-enforced limits count **outstanding** leases, not merely unexpired ones. A crashed holder stops counting when
its expired lease is settled or taken over. Every source with a store-enforced limit therefore runs expiry
settlement in every process that registers it, with or without an application expiry handler, so capacity held by
crashed holders is returned promptly.
Settlement and takeover skip records whose lock is held, which is exactly the case of an open guarded transaction.
Counting and claiming of one limit scope are serialized by the provider, so two claimers cannot both see a free slot.
A claim that finds a scope busy while it holds record locks skips that scope's records. A due claim does not wait for
the scope: it leaves that partition out for the rest of the call, or ends the call when the owner's scope is busy,
so a long application transaction delays neither other partitions nor the dispatcher's other sources; a later call
claims the skipped records. A claim by key waits for the scope before it locks its record again.

A due claim attempts at most twice its limit: up to that many records whose claim hook or expiry handler fails do
not reduce what it claims, and records that keep failing cannot keep it running.

## Holding a lease

`LeaseHold<TKey>` keeps a claimed lease alive while work runs:

- renews every third of the lease duration;
- tolerates failed renewals until the last confirmed expiry would have passed, then treats the lease as lost; it
  tracks that expiry with the local monotonic clock from the validity the store reports, never with wall-clock time,
  and a timer of its own cancels the token at that moment whether or not a renewal is still running; a claim whose
  validity ran out before the hold started (for example in a slow claim hook) does not start its handler; lost
  here means the hold's conservative deadline passed, which can be before the store's expiry;
- exposes one `CancellationToken` that is cancelled when the lease is lost, when cancellation is requested, at a
  timeout, or when the process shuts down, with the reason available to the holder. On reconciliation sources that
  supersede, a renewal that fails while the hold's deadline has not passed means the lease was superseded, since a
  lease that has not expired is taken away only by supersession; the reason is then `Superseded`.

## Dispatching due records

```csharp
work.AddLeaseHandler<InstanceKey, InstanceExecutor>(Instances, handler => handler
    .Capacity(16)
    .Timeout(TimeSpan.FromMinutes(30))           // optional: cancels the run's token with the timeout reason
    .OnClaimed<InstanceClaimed>()
    .OnExpired<InstanceExpired>());

work.AddLeaseSource(Instances, source => source  // a process without a handler: claims by key, remote holders
    .OnClaimed<InstanceClaimed>()
    .OnExpired<InstanceExpired>());

public sealed class InstanceExecutor : ILeaseHandler<InstanceKey>
{
    public ValueTask RunAsync(LeaseRun<InstanceKey> run, CancellationToken cancellationToken) { ... }
    public ValueTask ExhaustedAsync(LeaseRun<InstanceKey> run, CancellationToken cancellationToken) { ... }
}
```

The dispatcher claims due records up to free capacity, of its source or of the sources of its
[pool](hosting.md#pools), runs each handler in a new DI scope with a `LeaseHold`, and is woken by the provider's signal, no later than the earliest time a record not yet claimable may become claimable, and
by polling. Records already due that a claim did not take (locked by another transaction, or held back by limits) are
found by signals and polling. A dispatcher at capacity waits for a run to finish. A handler that throws has its record
released as failed, due after the source's `FailureBackoff` for the record's `Failures`. A handler that returns while
still holding its record has it released the same way and is reported as `lease.unended`, so a forgotten transition
cannot spin; so has a handler whose `EndAsync` ran in a transaction that did not commit. A run whose hold was lost,
including a claim whose handler was not started, is released as failed too: the release is fenced, so it changes
nothing once the lease expired, and gives the record back at once when only the hold's deadline passed, for example
while a renewal reply was late, instead of leaving it to a takeover. After the shutdown grace
period, records still held are released and their holds stop renewing, even if the release fails or the handler keeps
running, so the lease expires and another process can take the record over. During shutdown a handler that returns or throws without ending its record has it released as it is,
without counting a failure.

## Remote holders

Processes that do not run the framework hold leases through the application's own API (W1). The application calls
the same operations with the remote holder's owner id (W2), and returns `Lease.Token`, an opaque string carrying the
source, canonical key, owner and epoch, which the remote holder sends back with renewals and results. The token is a
fence, not a credential: the API authenticates and authorizes the caller itself, and the store still checks validity.

A remote completion carries a completion identity and a digest of its payload. `EndAsync` with an identity and digest
already recorded in `EndedBy` returns the recorded outcome instead of reporting a lost lease; a different digest, or a
completion for an older epoch, is rejected (W4).

## Execution guarantee

Leases make ownership exclusive at any instant and make fenced transactions safe. They cannot make work run once:
a holder can lose its lease after an external call and before its guarded commit, and the next holder repeats the
call. `LeaseRun` exposes `AttemptId` (unique per claim) and `ExecutionId`: stable across the attempts of one
deliberate execution and new for every deliberate new execution. On plain lease sources the domain defines executions
by passing an execution id to `ScheduleAsync`; reconciliation and jobs define it themselves (R17).
