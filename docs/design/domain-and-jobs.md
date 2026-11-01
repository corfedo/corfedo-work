# Domain records and jobs

Most inconsistency between "the job" and "the thing it works on" comes from two records describing the same work:
the domain record says one thing, the job says another, and code keeps them in sync by hand. The framework removes
the second record where it can and keeps the two consistent where it cannot.

## One owner per fact

Whatever users see about a piece of work (last success, current error, progress, enabled or not) lives on the domain
record. A job record holds scheduling mechanics only and is never the source of domain status.

## Choosing the layer

| The work... | Use | Why |
|---|---|---|
| has a record with a lifecycle users see, and runs as a long execution with its own states | [Leases](leases.md) on that record | The domain state machine stays the only state machine; the lease adds ownership, fencing, recovery and cancellation. |
| has a record that must converge to its latest requested state | [Reconciliation](reconcile.md) on that record | Requests, follow-ups, waiting and backoff live on the record; no job exists. |
| acts on a domain record but is an operation on it (a sync, an import, a refresh) | A [job](jobs.md) with a subject | The record keeps the domain status; the job keeps timing and attempts; the mechanisms below keep them consistent. |
| has no domain record (retention, fan-out, a one-off recompute) | A job without a subject | There is nothing to keep consistent. |
| is several dependent steps | A [chain](chains.md) | Steps are jobs; the chain keeps their order and results. |

Choosing leases or reconciliation means adding the layer's fields to the domain record. That is the cost of having
one record; it is preferred whenever the work state would otherwise be duplicated.

## Jobs with a subject

A subject is a kind and an identifier, such as `("feed", "npm")`. Definitions declare the subject kind they act on;
every request names the subject.

### Merging by subject

Requests for the same definition and subject merge into one outstanding job (D1). No merge-key function is needed for
the common case, and "is there already a job for this record" is answered by the framework (`IfAbsent`, `GetAsync`),
not by queries the domain writes against framework tables.

### The subject's lifecycle in the domain transaction

```csharp
// Disabling a record pauses its jobs; deleting it cancels them. Both commit with the domain change (D2).
await jobs.PauseAsync(tx, FeedSync, Subject.Of("feed", feed.Id), cancellationToken);
await jobs.CancelAsync(tx, FeedSync, Subject.Of("feed", feed.Id), cancellationToken);
await jobs.CancelAllAsync(tx, Subject.Of("feed", feed.Id), cancellationToken);   // every definition acting on it
```

Cancelling a running job requests cancellation through its lease. If its handler commits output before it observes
the cancellation, the domain's own check in that transaction (the record no longer exists or is disabled), next to
`GuardAsync`, rejects it.

A user-chosen recurrence is changed in the same way (D6):

```csharp
await jobs.ScheduleAsync(tx, FeedSync, Subject.Of("feed", feed.Id), Schedule.Every(feed.Interval, Interval.AfterCompletion), cancellationToken);
```

### Completion with the domain output

```csharp
public async ValueTask<JobDisposition> RunAsync(SyncFeed args, JobRun run, CancellationToken cancellationToken)
{
    // ... fetch and apply in batches, each batch committed with run.GuardAsync(tx) ...
    await using var tx = await store.BeginAsync(cancellationToken);
    feed.LastSyncedAt = now;
    feed.LastError = null;
    await run.CompleteAsync(tx, JobResult.Completed, cancellationToken);
    await tx.CommitAsync(cancellationToken);
    return run.Completed;
}
```

`CompleteAsync` applies the job's transition inside the domain transaction (D3). The domain record and the job agree
at every commit: there is no moment where the record shows the sync finished and the job still shows it running.
A handler that returns a result without calling it gets the same transition in the dispatcher's own transaction,
which is fenced but committed separately from the domain output.

### Outcome hooks

Every terminal transition goes through one path, whoever causes it: the handler's `CompleteAsync`, the dispatcher
applying a returned result, an operator action, a cancelling request from any process, a skipped or late occurrence,
attempts exhausted by lost leases, or arguments whose version has been retired. That path runs the definition's
outcome hook inside the transaction that makes the transition (D4):

```csharp
work.AddJobDefinition(FeedSync, definition => definition.OnEnded<FeedSyncEnded>());

public sealed class FeedSyncEnded : IJobEnded
{
    public ValueTask EndedAsync(JobEnd end, WorkTransaction tx, CancellationToken cancellationToken)
    {
        // end.Subject, end.Partition, end.Occurrence, end.Outcome, end.Reason ("lease_lost", "args_version", ...)
        // Record the outcome on the feed through the domain's data access bound to tx.
    }
}
```

- The hook belongs to the **definition registration**, not to the handler. Every process that requests, cancels or
  runs the definition registers it (`AddJob` includes it); a transactional operation on a definition the process has
  not registered is refused, so no process can end a job without its hook.
- The hook receives identities and the outcome, never deserialized arguments, so it also runs when the stored
  arguments can no longer be read.
- The hook may run more than once if its transaction rolls back and the transition is retried; its committed effects
  happen exactly once, together with the transition. Hooks make no external calls; work that must reach outside is
  requested as a job from the hook.

### Reading status by subject

```csharp
JobStatus? status = await jobs.GetAsync(FeedSync, Subject.Of("feed", feed.Id), cancellationToken);
IReadOnlyDictionary<Subject, JobStatus> page = await jobs.GetManyAsync(FeedSync, subjects, cancellationToken);
```

`JobStatus` describes the outstanding job holding the subject's identity, otherwise its earliest-due outstanding job
(D5):

| Field | Meaning |
|---|---|
| `State` | `Running` under a valid lease, `WaitingForParent` for a chain step whose parent has not settled, `Scheduled` while outstanding otherwise, `Idle` when no job is outstanding. |
| `Job`, `DueAt`, `RunningSince` | The outstanding job, its due time while not running (the requested instant while it waits for its [calendar](calendars.md) to plan it), and when its attempt started while running. |
| `Occurrence`, `Attempt`, `Progress` | Its occurrence of a recurrence, the attempts its execution used, the running attempt's progress. |
| `LastOutcome`, `LastReason`, `LastEndedAt` | The subject's latest ended execution. |
| `NextOccurrence` | The next planned instant of the subject's recurrence. |
| `Paused` | The definition or the subject is paused. |
| `FollowUpPending` | A follow-up waits for the outstanding job. |

A subject with no outstanding job, recurrence or ended job has no status: `GetAsync` returns null and `GetManyAsync`
leaves it out. `GetManyAsync` reads up to 1 000 subjects in one statement. Domain screens combine the status with
their own record. The framework's tables are not a query surface; their layout can change between versions.

### Run history

A definition whose stored policy sets `RunHistory` (off by default) records every attempt once, when it ends, keyed
by job and the lease epoch of the claim that used the attempt, with the subject, occurrence, execution id, attempt,
holder, start, end, outcome and reason code (D7). The policy is stored, since whichever process ends, settles or takes
over an attempt writes its entry:

| Attempt ends by | Entry |
|---|---|
| a terminal transition of its holder | the job's outcome: `Completed`, `Failed`, `Cancelled` or `TimedOut` |
| a retry, a failed attempt or a retried timeout | `Retried` with the reason code |
| a graceful release, for example on shutdown, which returns the attempt | `Released` |
| a release after the attempt failed without its outcome, such as after its hold was lost while the lease was still valid | `LeaseLost`, ended at the release |
| expiry of its lease, settled or taken over by the next claim | `LeaseLost`, ended at the lease's expiry |

A release whose lease is no longer valid changes nothing and records nothing; settlement or the next claim records the
loss. `Skipped` describes jobs ended without running, which have no attempt of their own.

A claim that ends a job without a handler (late, exhausted or cancelled) records no entry of its own; a running attempt
appears in `JobStatus`, not in the history. `GetRunsAsync(definition, subject)` and `GetRunsAsync(definition, jobId)`
return pages of up to 1 000 entries newest first, with a cursor for the next page. The history has its own retention,
`RunHistoryRetention`, stored with the definition: entries are deleted that long after their attempt ended, whether
or not their job still exists, and a job deleted by its retention leaves its entries (see
[retention](jobs.md#retention)). A rerun continues the record's lease epochs, so its entries never replace those of
earlier executions.

## External effects

Fencing keeps database output consistent; it cannot undo an HTTP request, an uploaded file or a sent message (R17).
A crash can always fall between an external call and the commit that records it, so a local marker alone cannot
tell the next attempt whether the call happened. Deduplication needs the external system's cooperation:

- pass `run.ExecutionId` (or a value derived from it per operation) as the idempotency key where the remote API
  accepts one; a deliberate rerun has a new execution id and is therefore not suppressed;
- otherwise give the operation a client-chosen identifier derived from the execution id, and on the next attempt look
  it up in the external system before repeating it;
- where neither is possible, the effect may repeat; design the effect to tolerate that, or move it behind a system
  that offers one of the above.

Recording the operation's identity in a guarded transaction before the call is still useful: it tells the next
attempt which identifier to look up.
