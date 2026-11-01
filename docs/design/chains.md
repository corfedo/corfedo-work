# Chains

A chain is a set of jobs created together, where a step runs after what it depends on has settled. Chains cover
ordered steps, parallel groups, fan-out with fan-in, result passing and recurring chains. They are deliberately not a
workflow engine: there is no branching on data, no loops and no signals between steps.

Requirements: K1–K8 in [problems](problems.md).

## Shape

```csharp
var chainId = await jobs.RequestChainAsync(tx, chain => chain
    .StartWith(FetchOrders, new FetchOrdersArgs(day))
    .Then(GenerateInvoices, new InvoiceArgs(day))            // reads FetchOrders' result
    .ThenGroup(group =>
    {
        group.Add(SendEmails, new EmailArgs(day)).Then(RecordDelivery, new DeliveryArgs(day));
        group.Add(UpdateLedger, new LedgerArgs(day));
    })
    .AfterAll(Finalize, new FinalizeArgs(day))               // every branch succeeded
    .AfterAnyFailed(ReportFailure, new ReportArgs(day))      // at least one branch failed
    .AfterAllSettled(ReleaseResources, new ReleaseArgs(day)), // always, once every branch settled
    cancellationToken);
```

Every step is a job of its own definition with its own arguments and policy (attempts, timeout, limits, priority),
which the requesting process must register; steps take request options (subject, partition) but never merge.
`StartWith` names the one root, due now or by the one-shot schedule of `RequestChainAsync(tx, chain, schedule)`,
which may name a calendar. `Then` and `ThenGroup` return the new step or group, so a step can have several children.
The chain adds:

| Field | Meaning |
|---|---|
| Chain id and generation | Identify the chain and the current pass (incremented by retry-from). |
| Parent | The step or group the step depends on. |
| Run condition | When the step runs once its parent settled. |
| Pending | For groups: the number of branches not yet settled. |

All steps are created in one transaction (K1). A step that is not yet runnable has `DueAt` null; it becomes due when
its parent settles and its condition holds, or skipped when the condition does not hold. Both happen inside the
transaction that settles the parent, so a chain never has a settled parent with a child in limbo.

## Run conditions

| Condition | Runs when the parent... |
|---|---|
| `OnSuccess` (default for `Then`) | completed. |
| `OnFailure` | failed or timed out. |
| `OnCancelled` | was cancelled. |
| `OnFailureOrCancelled` | failed, timed out or was cancelled. |
| `OnAnyOutcome` | settled in any way, including skipped. |

`Then(definition, args, condition)` names the condition. A skipped step settles as skipped, and its own children
evaluate their conditions against that.

## Groups

A branch starts with a step that follows the group's step, with its own condition (`group.Add(definition, args,
condition)`, `OnSuccess` by default); a branch whose first step's condition does not hold is skipped. `AddMany` adds
one single-step branch per item. A group settles when every branch subtree has settled (K2). A branch subtree's outcome is the worst outcome
(failed or timed out, then cancelled, then succeeded) among its steps that ran; skipped steps do not count, so a
failure followed by skipped success steps is still a failure. A branch none of whose steps ran is skipped. The group's aggregate outcome is
decided in this order:

| Aggregate | When |
|---|---|
| Failed | Any branch failed or timed out. |
| Cancelled | No branch failed, and any was cancelled. |
| Succeeded | No branch failed or was cancelled, and at least one succeeded; skipped branches do not change this. |
| Skipped | Every branch was skipped. |
| Succeeded | The group has no branches; it settles as soon as it is created. |

Group continuations:

| Continuation | Runs when the group... |
|---|---|
| `AfterAll` | succeeded. |
| `AfterAnyFailed` | failed. |
| `AfterAnyFailedOrCancelled` | failed or was cancelled. |
| `AfterAllSettled` | settled, whatever the outcome. |

Failure continuations wait for the other branches to settle; they never race running siblings. To stop siblings early,
a failure continuation or an outcome hook cancels the chain.

## Dynamic groups

A running step can add a group of children and continuations (K4), in the transaction that completes the step:

```csharp
await run.AddGroupAsync(tx, group => group
    .AddMany(AnalyzeTenant, tenants.Select(t => new JobRequestItem<AnalyzeTenantArgs>(new(t), new() { Partition = t }))),
    after => after.AfterAllSettled(Summarize, new SummaryArgs()),
    cancellationToken);
await run.CompleteAsync(tx, JobResult.Completed, cancellationToken);
```

This is fan-out with fan-in: the continuation runs once, after the last child settles, however long the slowest
child takes. The group keeps one pending count, decremented in each child's terminal transaction; the transaction
that brings it to zero settles the group and makes its continuations due. Child settlements of one group therefore
serialize on that count; that is accepted until a measurement shows it limits real workloads. A dynamic group with no
children settles at once as succeeded.

When the batch is itself a domain concept with its own status, the domain can track it on its own record instead,
with the final child requesting the aggregate in its completing transaction.

## Results

A step reads the result of its parent with `run.GetParentResultAsync(definition)` (K3), typed by the parent
definition's result codec; it is null when the parent is a group or of another definition. A group continuation reads
with `run.GetGroupResultsAsync(definition)` the results of the steps of that definition that end the group's branches,
those no step or group of the group follows. Both return the outcome of the latest ended execution with its result. Results have
the same size limit as job results.

## Recurring chains

A recurring schedule can own a chain template (K5):

```csharp
work.AddRecurringChain(DailyInvoices, new DailyArgs(), Schedule.Cron("0 7 * * *", "Europe/Warsaw").Within("business-days"),
    chain => chain.StartWith(FetchOrders, new FetchOrdersArgs()).Then(GenerateInvoices, new InvoiceArgs()).Then(SendEmails, new EmailArgs()));
```

`AddRecurringChain` declares the code-defined recurrence of `DailyInvoices` and runs its occurrences, which have no
handler of their own. When an occurrence runs, it creates a fresh chain from the template, with the root due at once,
and gives up its lease; its attempt stays open, held by the chain instead of a lease, so it is running for every read
and operation until the chain has settled. Then its definition's maintenance ends it with the chain's outcome, and its
run history entry spans the chain, with the chain's outcome (`chain_failed`, `chain_cancelled` or `chain_skipped` as the
reason). So `Overlap`, `Late`, `Missed`, monitoring expectations and the outcome hook apply to the chain as a whole,
and an occurrence skipped before it runs (overlap, calendar, late) creates no chain. Steps retry within their own
attempts; the occurrence does not retry a failed chain, and a chain retried from a step does not reopen its
occurrence. Cancelling the occurrence, which removing its recurrence (`RemoveAsync(tx, definition)`) does, cancels
its chain in the same transaction, as `CancelChainAsync` does, and leaves the occurrence running until the chain has
settled, so it ends with `chain_cancelled` and no other occurrence overlaps the chain (K-16). A cancellation requested
while the occurrence holds its lease, before it created its chain, creates no chain and ends the occurrence
cancelled.

## Skipped roots

A chain whose root is skipped (late beyond its grace, blocked by a calendar) settles every step as skipped, with
`root_skipped` as the reason and whatever its condition, even `OnAnyOutcome`, without running any handler (K7).

## Retry from a step

`RetryFromAsync(chainId, stepId)` reruns a settled chain from a step, and `RetryFromGroupAsync(chainId, groupId)` from a
group (K6); both return `RetryFromOutcome` (`Retried`, `NotSettled`, `NotFound`):

- the chain must be settled; concurrent retries of one chain cannot both win;
- the chain's generation increments; the chosen step and everything that depends on it are reset to pending under the
  new generation, each with a new execution id; completed ancestors keep their outcomes and results and do not run
  again;
- within a group, completed sibling branches keep their outcomes; the group and its continuations are evaluated again
  from the new aggregate; choosing the group itself reruns all its branches;
- a dynamic group added by a step that runs again is discarded with its steps, since the step adds it again when it
  reruns; retrying from a dynamic group or a step inside it keeps the group;
- a late write from the previous generation is rejected by the step's fence.

## Chains as units

A chain is read (`GetChainAsync`, which returns `ChainStatus` with its steps and groups), retained and deleted as a
unit (K8). Retention removes a chain only when every step has settled and its settlement is older than its window.
`DeleteChainAsync(chainId)` deletes a settled chain and refuses one with unsettled steps (`Unsettled`);
`ChainDelete.Cascade` cancels its steps and deletes it in the transaction that settles it (`Deleting`).
`CancelChainAsync(tx, chainId)` cancels the outstanding steps in the caller's transaction, waiting ones and due ones
ended cancelled and running ones through their lease, and their children follow their conditions. Both are refused
when a step belongs to a definition the process does not register, since its outcome hook must run.

## Waiting steps

A step whose parent has not settled is an outstanding job waiting for its parent: `JobStatus` reports it as
`WaitingForParent`, without a due time. Operations treat it as follows:

| Operation | Waiting step |
|---|---|
| `RunNowAsync`, `RerunAsync` | Refused with `ChainStep`, also for a chain step that ran; `RetryFromAsync` reruns a chain. `TriggerAsync` acts on recurrences, whose occurrences are never chain steps. |
| Cancellation by subject or merge key | Ends it cancelled, like any outstanding job; its children then follow their conditions. |
| Subject and definition pauses | Apply to it; it waits for the resume once it becomes due. |
| Calendar removal | Blocked by it, as by any live work. |
| Retirement of argument versions | Applies to it, as to any outstanding job. |

## Storage (PostgreSQL)

Every step is a `jobs` row of its definition from the moment the chain is requested, so leases, claims, retries,
calendars and run history apply to a running step unchanged. Steps never merge (`merge_key` null). A chain's rows
are inserted in one statement per table.

- `jobs` columns: `chain_id`, `chain_generation`, `chain_parent` (the step or group the step waits for, null for the
  root; a branch's first step waits for the group's step), `chain_group` (the enclosing group, null at the chain's
  top level), `chain_condition` (the parent outcomes under which it runs, as flags) and `chain_waiting`, true while
  the step waits for its parent. A waiting step has `due_at` and `calendar_at` null, so it is outside `jobs_due` and
  `jobs_calendar_due`, and claims, which require a due time, never see it. `jobs_chain (chain_id, chain_parent) WHERE
  chain_id IS NOT NULL` finds a step's children and a chain's steps. An occurrence of a recurring chain points at it
  with `owned_chain` while it waits, with `attempt_started_at` kept and no lease; running is defined once, as a valid
  lease or `owned_chain` set, and `chain_outcome` holds the outcome it ends with once the chain settled, found
  by `jobs_chain_owner (definition, id) WHERE chain_outcome IS NOT NULL AND state = 0`.
- `job_chains`: `id`, `generation`, `root_job`, `owner_job` and `owner_definition` (the occurrence of a recurring
  chain), `definitions` (the distinct definitions of its steps), `created_at`, `settled_at`, `outcome`,
  `delete_requested`, and the chain's top level as a group: `pending` and `worst`. `job_chains_settled (settled_at)`
  serves retention.
- `job_chain_groups`: `id`, `chain_id`, `generation`, `parent` (the group's step), `enclosing_group`, `pending`,
  `worst`, `settled_at`, `outcome` and `dynamic` (added by a running step), found by `(chain_id, parent)`. A group has no condition: its branches' first steps
  carry their own.

A group's aggregate is the worst outcome among the steps that ran in its branches (failed or timed out, then
cancelled, then succeeded), skipped when none ran and succeeded when empty: the branch rule applied once across the
branches. A group therefore keeps `pending`, the nodes in it not yet settled, and `worst`. A step or nested group that
settles counts itself in its enclosing group, or the chain's top level, with one `UPDATE ... SET pending = pending -
1, worst = greatest(worst, $rank)` that also sets `settled_at` and the aggregate when nothing is pending any longer;
that transaction then decides the group's continuations and counts the group in its own enclosing group. When the top
level settles, the chain records its owning occurrence's `chain_outcome` and signals its planners, and a chain whose
deletion was requested is deleted.

- Deciding children: after the outcome hook, the step's terminal transaction runs one `UPDATE` over its waiting
  children through `jobs_chain`: those whose condition holds get `due_at` from the clock, the others `skip_pending`
  with `chain_condition` as the reason, and each of their definitions is signalled. Skipped children are ended by
  their own definition's maintenance (`FinalizeSkippedAsync`), as calendar and overlap skips are, so only a process
  that registers a definition runs its outcome hook; their own children follow when they end. Groups that follow
  the step and have no branches settle as succeeded in the same transaction. A skipped root marks every waiting step
  of the chain with `root_skipped` instead.
- Locks: a settling step holds its own row, then its children's, then the group row, so siblings serialize on the
  group (decision "Dynamic group settlement") without deadlocking. Chain cancellation and cascade deletion lock the
  chain's outstanding steps before ending any of them, as subject cancellation does, and only then the chain row.
  Every cancellation locks the steps waiting for their parent after the others, each in id order: a terminal
  transition holds its own row before its waiting children, so a cancellation never holds a waiting child while it
  waits for the parent that decides it. A step's id says nothing about its place in the chain, since ids created in
  the same millisecond have no order, and ending a step decides its waiting children and the continuations of the
  groups it settles. So a cancellation by subject or merge key also locks, in the same pass, the waiting steps of
  every chain whose steps it selected, and holds every row it may decide before it ends anything, as a chain
  cancellation does by locking all of the chain's outstanding steps; two cancellations of overlapping steps then wait
  for each other at their first common row instead of deadlocking (K-15). A settling chain records its outcome on
  its owning occurrence after the chain row, so cancellations lock occurrences that own a chain after every step,
  and cancelling such an occurrence locks its chain's outstanding steps in the same pass.
- Dynamic groups: `AddGroupAsync` locks the running step and checks its lease after the lock, inserts the group and
  its steps under the chain's current generation, and adds the new nodes of the step's own enclosing group to its
  pending count.
- Results: a step reads its parent's row; a group continuation reads the group's steps that no step or group of the
  group follows.
- Retry from a step locks the chain row, which serializes retries, requires it settled, increments `generation`,
  resets the chosen node, its dependants and the continuations of every group it reopens to waiting under the new
  generation with new execution ids, and recounts each reopened group from its settled nodes. A dynamic group whose
  step is reset is deleted with its steps and groups instead. The chosen step is due now; the branches of a chosen
  group are decided by the outcome of the group's step.
- Retention: a chain is deleted as a unit, its steps through `jobs_chain` and its groups by foreign key, once settled
  for longer than its window: the owner definition's `job_retention` for a recurring chain, otherwise the longest
  `job_retention` of the definitions in `definitions`, read when retention runs. The scan of `job_chains_settled` is
  bounded by the shortest stored retention; the selected chains are collected into an array, so their rows are deleted
  by key. Per-definition job retention skips chain steps: `jobs_retention` has `chain_id IS NULL` in its predicate.
- Cost outside chains: claims and requests are unchanged; the terminal `UPDATE` returns the chain columns, and a job
  outside a chain runs no further statement. Measurements are in [providers](providers.md#postgresql).
