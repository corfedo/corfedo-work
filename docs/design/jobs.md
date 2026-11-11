# Jobs

A job is work that has no record of its own. Jobs are [leases](leases.md) on records the library owns, plus
arguments, schedules, occurrences, attempts and terminal states. Work whose state belongs to a domain record uses
leases or reconciliation on that record instead; see [domain records and jobs](domain-and-jobs.md). Jobs that depend
on each other are [chains](chains.md); availability rules are [calendars](calendars.md).

Requirements: J1–J22 and D1–D7 in [problems](problems.md).

## Definitions and registrations

A **definition** is what every process that requests, cancels or runs the job agrees on. It is stored in the
framework's tables with a revision:

```csharp
public sealed record SyncFeed(string Feed);

public static readonly JobDefinition<SyncFeed> FeedSync = JobDefinition.Create<SyncFeed>(
    key: "feeds.sync",                                // stable registered name, never a CLR name
    revision: 3,                                      // increases whenever any shared policy below changes; 1 by default
    subject: "feed",                                  // optional: jobs act on "feed" subjects
    policy: new JobPolicy
    {
        Attempts = 5,
        Backoff = Backoff.Exponential(TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(30)),
        PartitionLimit = 1,
        DefinitionLimit = 4,                          // running jobs of this definition across all processes (J22)
        Priority = Priority.Normal,                   // J17, see priority below
        Timeout = TimeSpan.FromMinutes(20),           // per attempt (J18)
        Late = Late.Run,                              // or Late.SkipAfter(grace) (J2)
        RunHistory = true,                            // record every attempt (D7); unset: the host's default, off
        Retention = TimeSpan.FromDays(3),             // terminal jobs (J12); unset: the host's default
        RunHistoryRetention = TimeSpan.FromDays(90),  // run history entries (D7); unset: the host's default
    });
```

The arguments are serialized with the host's JSON options (`ConfigureJson`, `UseJson`, see [hosting](hosting.md)),
resolved when the definition is registered, and written as argument version 1. A definition with versioned arguments
or another format takes an `IJobArgs<TArgs>` codec after the revision, such as
`JobArgs.Json(AppJson.Default.SyncFeedV2, version: 2)`; its version is the one requests write.

A **registration** is local to the process that runs the job: capacity, lease duration, scope binding.

```csharp
work.AddJob<SyncFeed, FeedSyncHandler>(FeedSync, job => job.Capacity(2).Lease(TimeSpan.FromMinutes(10)));

public sealed class FeedSyncHandler : IJobHandler<SyncFeed>
{
    public ValueTask<JobDisposition> RunAsync(SyncFeed args, JobRun run, CancellationToken cancellationToken) { ... }
}
```

A handler returns a `JobDisposition`: a `JobResult`, which converts implicitly, for the dispatcher to apply, or
`run.Completed` after it applied its result with `run.CompleteAsync(tx, result)` in its own transaction.
`run.Completed` is not a result, so it cannot be passed to `CompleteAsync`; the same rule as for
[reconcilers](reconcile.md#runs-and-results).

**Revisions** (R18). At startup a process stores its definition only if its revision is newer than the stored one; an
equal revision with different content is a startup error. Stored policy is data, so every process enforces the newest
attempt limits, distributed limits, priority and schedules during a rolling deployment:

- Transitions (results, retries, exhaustion, cancellation, planning) read the policy from the stored definition inside
  their own statement.
- Claims depend on the policy through their limits and filters, so a claim must commit under the revision stored when
  it commits. Every job claim starts with the definition's **definition fence**: a shared lock that storing a
  definition takes exclusively. After the fence the claim reads the stored revision and pause state; if the revision
  differs from the one the process built its claim statements from, the claim takes nothing, and the dispatcher
  rebuilds them from the stored definition and claims again. Storing a revision waits for claims in flight, and claims
  that start meanwhile queue behind it. The fence comes before every record lock and limit-scope lock, as the
  [generation fence](reconcile.md#requests) does, so claims still never wait while they hold record locks.
- A process with an older declared revision therefore applies the stored policy, and keeps running the argument
  versions its own code accepts.

**Executable compatibility** (R21). Stored policy cannot update code. Handlers, codecs and hooks are code, so what a
process can execute is declared by its registration, not by the stored definition:

```csharp
work.AddJob<SyncFeed, FeedSyncHandler>(FeedSync, job => job.AcceptsArgs(1, 2));
```

- Every job records the argument version it was requested with.
- A process claims only jobs whose recorded version its registration accepts. Claim statements filter on them; an
  older worker never claims, and never fails, a job written for a newer one.
- Accepted versions are part of the process registration, so a job no live process can execute is reported as
  unserved for that version (R12, `GetUnservedAsync`, see [processes](hosting.md#processes)) instead of failing.
- A job fails with `args_version` only when its version is below the minimum the newest stored definition still
  accepts (`MinimumArgsVersion`), that is, when its version has been retired on purpose. A process that runs the
  definition ends such jobs once they are not held, through the outcome hook and without reading their arguments.
- A process whose declared definition revision is older than the stored one keeps running the versions it accepts and
  is reported as outdated (`GetOutdatedProcessesAsync`).
- Results are not routed by version. A result codec reads every version its definition has written, and a release
  that writes a new result version is deployed only once every process that reads the definition's results, chain
  steps through `GetParentResultAsync` and `GetGroupResultsAsync` included, reads it: readers first, then writers. A
  result can only be newer than its reader while a rolling deployment runs, since a newer reader reads older results,
  whereas arguments wait in the queue across any number of deployments. A read that finds a result of a newer
  version than the reading codec's `Version` throws `JobResultVersionException` before decoding; in a handler it
  fails the attempt with `result_version`, so the step retries and, once exhausted, is rerun with
  `RetryFromAsync` (K-17).

`JobRun` carries the job id, subject, partition, occurrence, attempt, argument version, `ExecutionId`, `AttemptId`,
the holder's `Owner`, the cancellation token with its `StopReason`, `GuardAsync`, progress reporting and
`CompleteAsync(tx, result)`; `JobRun<TResult>` adds `CompleteAsync(tx, value)` for definitions with a result type. It
does not expose the lease run: the attempt's lifecycle belongs to the dispatcher and the job store, and lease
operations refuse to claim, release, end, schedule or cancel jobs.

### Scope of a definition's statements

All definitions share the framework's job table. Each definition is a lease source named `job:<key>` whose every
statement is scoped to the definition: claims, limit counts (partition, spread, owner and definition limits), expiry
settlement, cancellation, next-due and next-expiry lookups. Two filters apply on top of that scope:

- **Eligibility** decides what a handler may claim: an accepted argument version not below `MinimumArgsVersion`, the
  definition not paused, the job not paused. Only claims and next-due lookups apply it.
- **Lifecycle maintenance** ignores eligibility: expiry settlement, cancellation, retirement of argument versions and
  outcome hooks also reach paused and incompatible jobs.

Job sources always settle expiry, so a store-enforced limit introduced by a later revision counts outstanding leases
the same way as one present from the start.

## Job identity and execution identity

The **job id** identifies the job record. The **execution id** identifies one deliberate execution of it and is what
handlers use to deduplicate external effects (R17). The job record stores it; claims keep it. An execution id is new
for every:

- new job, and every follow-up run produced by a request that merged into a started execution;
- occurrence of a recurring job, including triggered occurrences;
- rerun of a terminal job (`RerunAsync`), which keeps the job id and its record; the record's lease epochs continue,
  so leases of earlier executions stay invalid and their run history entries stay distinct;
- chain generation of a step, so a step retried from it runs as a new execution.

It stays the same across the attempts of one execution: retries, takeovers after lost leases and releases on
shutdown. The attempt id is new for every claim.

An execution has **started** from its first claim that runs, or may run, its handler. The job record stores this as
its own fact, independent of the attempt count: a graceful release returns the attempt to the budget, but external
effects of that attempt may already have happened, so the execution stays started.

## Identity and merging

A job's merge identity is its definition plus its subject, or plus an explicit merge key for jobs without a subject.
A request chooses how it treats an outstanding job (scheduled or running) with the same identity:

| Mode | Effect |
|---|---|
| `Merge` (default) | Before the execution started: the latest arguments replace the stored ones and the schedule merges as below. After it started, or once a claim took the job's lease: the request becomes the job's pending follow-up (J3, J4). |
| `IfAbsent` | Creates the job only when none is outstanding; an existing one keeps its revision, arguments, schedule and attempts, and gets no follow-up (J13). Occurrences of a recurrence never count. |
| `New` | Never merges: the request is its own job. Requests without subject or merge key always behave this way. |

The partition must match; a request that would merge into another partition's job is refused.
Backoff and attempts
of a merged job are never reset by a request. Jobs requested without a partition share one default partition per definition,
across processes: with `PartitionLimit = 1` unpartitioned jobs of the definition run one at a time. Named partitions
have their own limits, and `DefinitionLimit` covers the whole definition.

**Follow-ups.** Once an execution started, whether it runs now or waits for a retry, its arguments and execution id
stay fixed, so one execution id never covers two argument values. The same holds while a claim holds the lease of an
execution that has not started, for example one that ends it skipped under `Late`, so the request is not lost with
it. A merging request writes the job's pending
follow-up instead: the latest pending arguments and argument version win; the pending due time merges by the same
rules as a due time (earliest wins for `Now`, `At` and `After`; a debounce moves it later, up to its maximum measured
from the first pending request). When the execution reaches a terminal outcome, that outcome is recorded and its
outcome hook runs; then, in the same transition, a job with a pending follow-up becomes outstanding again with the
same job id, the pending arguments, a new execution id, fresh attempts, failures and takeovers, not started, due at the
pending due time. Cancelling a job also discards its pending follow-up; a request after the cancellation, while the
cancelled run has not ended yet, becomes a new follow-up. A follow-up keeps its pending due time, so under
`Late.SkipAfter` lateness counts from that instant: the follow-up of an execution that ran longer than the grace can
end skipped at its first claim, with no attempt used and through the outcome hook. Definitions whose follow-ups must
run however late use `Late.Run`.

**Standalone jobs and recurrences are separate lifecycles**, as in Temporal, Quartz.NET and Hangfire. They share
definitions, handlers, workers, attempts, leases, results and history, but neither changes the other:

- `RequestAsync` and `RequestManyAsync` create or merge **standalone jobs** only. Merging, `IfAbsent` and follow-ups
  apply among a definition's standalone jobs of one identity; a recurrence of that identity, appearing or
  disappearing, changes nothing about them.
- A recurrence owns its **occurrences**. They do not hold the standalone merge identity: overlap, waiting and
  after-completion spacing count only the recurrence's own occurrences. `ScheduleAsync` never adopts a standalone job.
- Running a recurrence manually is explicit: `TriggerAsync` goes through the recurrence and follows its overlap
  policy (see [triggering](#recurring-jobs-and-occurrences)).
- A standalone job and an occurrence of the same subject can run at the same time. When scheduled, manual and
  webhook work must coalesce, use a **dispatcher**: a recurrence whose handler requests the standalone jobs, so every
  source of work merges by the ordinary standalone rules (see [common patterns](#common-patterns)).
- Partitions of standalone jobs and of recurrences are independent; a request is refused only when it would merge
  into another partition's standalone job.

## Schedules

One `Schedule` type covers every timing (J1). One-shot schedules go through `RequestAsync` and `RequestManyAsync`,
which refuse recurring ones and default to `Schedule.Now`; recurring schedules go through `ScheduleAsync`, which
creates or replaces the recurrence and names no job.

Requests join the caller's transaction. `RequestAsync(definition, args, ...)` without one commits in the store's own
READ COMMITTED transaction and signals claimers after that commit, with the same merging as a transactional request; a
rollback of the caller's own transaction does not remove the job.

| Schedule | Due | When requests merge |
|---|---|---|
| `Schedule.Now` | As soon as possible. | Earliest due time wins. |
| `Schedule.At(time)` | At a point in time, given with any offset; the instant is kept. | Earliest due time wins. |
| `Schedule.After(delay)` | After a delay from the request. | Earliest due time wins. |
| `Schedule.Debounce(delay, maximum)` | `delay` after the latest request, at most `maximum` after the first outstanding one. | The due time moves later, up to the ceiling. |
| `Schedule.Every(interval, Interval.AfterCompletion)` | Recurring; the first occurrence is due when the recurrence is created, each later one `interval` after the previous one ended. | Replaces the recurrence. |
| `Schedule.Every(interval, Interval.FixedRate, anchor)` | Recurring on a fixed grid from the anchor. | Replaces the recurrence. |
| `Schedule.Cron(expression, timeZone)` | Recurring on a cron expression in the time zone. | Replaces the recurrence. |

Due times are eligibility, not start guarantees: a due job starts when a process has capacity and limits allow it.
Occurrence instants are computed by the process from the stored schedule with the store's clock reading taken in the
planning transaction (R4); the store holds them as absolute times.

Any schedule can take a stored [calendar](calendars.md) (J16):
`Schedule.Cron("0 7 * * *", "Europe/Warsaw").Within("business-days")`. A job with a calendar is stored with its
requested instant and no due time; the definition's planner makes the instant its due time with the end of its allowed
window when the calendar allows it, and otherwise follows `Blocked` for one-shot schedules or records the occurrence
skipped for recurring ones. Claims start it only before the window ends; work the window closed on follows `Closed`.
Merging uses the requested instant like a due time.

### Cron semantics

The syntax is Cronos's: five fields, or six with seconds, including `L`, `W`, `#` and reversed ranges. Occurrences
are the ones Cronos computes in the schedule's time zone, which follow Vixie cron around daylight-saving changes:

- **Spring gap.** No occurrence is lost: an occurrence whose local time does not exist runs at the first valid time
  after the gap.
- **Autumn overlap, interval expressions.** An expression whose second, minute or hour field contains `*`, a range or
  a step (for example `*/30 * * * *` or `30 * * * *`) is interval based; its occurrences continue through both
  instances of the repeated local time.
- **Autumn overlap, other expressions.** An expression without those (for example `30 1 * * *`) runs only in the
  first instance of the repeated local time, so a daily job runs once.

An expression with no future occurrence is refused when it is requested. Jitter (`H`) is not accepted, so every
process computes the same occurrences.

### Late and missed occurrences

Two separate decisions (J2):

| Policy | Controls | Options |
|---|---|---|
| `Late` | A due job or occurrence whose execution starts late because of capacity, an outage or a restart. | `Late.Run` (default): run however late. `Late.SkipAfter(grace)`: end as skipped when its first claim is later than `grace` after it became due; no handler runs and no attempt is used. Retries of a started execution are not late. |
| `Missed` | Recurring occurrences whose due time passed before they were planned. | `Missed.Skip` (default): continue from the next future occurrence. `Missed.RunLatest`: run the latest missed occurrence once, subject to `Late`. |

Running every missed occurrence is not offered.

**The missed rule.** When the planner handles a recurrence, it takes the instants that became due since the last one
it planned and up to now:

- one instant is not missed: it is planned, and `Late` decides when its occurrence is claimed;
- several instants were missed: `Missed.Skip` plans none of them and continues from the next future instant;
  `Missed.RunLatest` plans only the latest.

Planner delay, downtime of every process and a pause use the same rule; there is no separate misfire threshold.
Precedence is fixed: `Missed` reduces the passed instants first, the overlap policy then applies to what is left, and
`Late` applies when an occurrence is claimed. Occurrences already created keep their own lifecycle. `AfterCompletion`
schedules have one next instant at a time, so only `Late` applies to them.

| Case | Timeline | Outcome |
|---|---|---|
| Dispatch delay | Planned at t1 plus a delay shorter than the interval; claimed 3 minutes late for lack of capacity. | One instant: the occurrence exists; `Late` runs it, or ends it skipped under `SkipAfter(2 min)`. |
| Downtime (J-6) | Every process down from before t1 to a time between t3 and t4. | Three instants: `Skip` runs nothing until t4; `RunLatest` plans t3 once, subject to `Late`, then t4. |
| Short downtime | Down across t1 only, back before t2. | One instant: t1 runs late, subject to `Late`. |
| Pause and resume | Paused across t1–t3, resumed between t3 and t4; or paused across t1 only. | As for downtime: missed, or one instant subject to `Late`. |
| `Overlap.Wait` (J-4) | t1 runs until after t3. The planner sees t1 outstanding at t2 and at t3 and keeps the latest as the waiting occurrence; when t1 ends, t3 is planned and starts. | Starts stay on the grid; the next instant is t4. |
| Retrying occurrence (J-14) | t1 is still retrying at t2. | `Wait`: t2 starts after t1 ends. `Skip`: t2 is recorded skipped. `Allow`: t2 runs alongside t1. |
| Planner down while t1 is outstanding | The planner returns between t3 and t4. | Two instants, t2 and t3, were missed: `Skip` leaves nothing waiting; `RunLatest` passes t3 to the overlap policy. |

## Recurring jobs and occurrences

A recurring job is one **recurrence** per identity: its schedule, the next instant to plan, the occurrence waiting
under `Overlap.Wait`, its pause state and its arguments. Each **occurrence** is a job record of its own, linked to the
recurrence, with an identity (its scheduled instant, or `trigger:<id>` for a triggered run), its attempts and its
outcome (J14). The recurrence and its occurrences are stored apart, so planning never touches an execution's lease.

1. The occurrence becomes due at its scheduled instant (or calendar-adjusted instant).
2. Each claim is an attempt. Failures and lost leases retry within the occurrence with the definition's backoff.
3. The occurrence ends as completed, failed (attempts exhausted, or `Fail`), cancelled, timed out or skipped. Its
   outcome is recorded (status, history, outcome hook) and the recurrence advances: to the next occurrence after the
   scheduled instant for cron and fixed-rate schedules, or to `interval` after the end for `AfterCompletion`.

**Planning.** Every process that runs a definition's handler runs its planner, which needs no execution capacity. A
planning pass is one short transaction: it locks due recurrences, skipping locked ones, computes their instants with
the store's clock reading, applies `Missed` and the overlap policy, creates the occurrences and moves the next instant.
An occurrence is unique per recurrence and occurrence identity, so a second planner or a retry after a crash creates
nothing twice, and a crash before commit leaves nothing behind. Planning takes no lease.

**Ended occurrences.** An occurrence's terminal transition does not lock its recurrence, since it runs in transactions
that hold domain locks. It leaves the ended occurrence unconsumed; the planner consumes it:

- an `AfterCompletion` schedule awaits the occurrence it planned; that occurrence's end sets the next instant
  `interval` after it;
- any ended occurrence releases an occurrence waiting under `Overlap.Wait` once no other occurrence of the recurrence
  is outstanding, and an `AfterCompletion` instant that came due while another occurrence was outstanding; until then
  the planner leaves that recurrence out of its passes and its next wake-up;
- triggered occurrences, occurrences recorded skipped and occurrences of a replaced schedule set no instant, except
  the occurrence an `AfterCompletion` schedule awaits when another `AfterCompletion` schedule replaces it.

**First instant.** A new or replaced cron or fixed-rate schedule starts at its next instant after the store's clock.
An `AfterCompletion` schedule measures its interval from the end of the last occurrence that advanced it, which the
recurrence records: a new recurrence has none and is due at once, as is one whose last end plus the new interval has
passed. One that replaces an `AfterCompletion` schedule awaiting an occurrence keeps awaiting it, so that occurrence's
end plus the new interval sets the next instant (D-7). A process restart that finds its declared recurrence stored
changes nothing, so it runs nothing again.

Occurrences have no follow-ups: work that must run again after a running occurrence is a trigger, which the overlap
policy queues, or a standalone job.

Retention keeps ended occurrences until the planner consumed them.

The overlap policy (J15) decides what happens when the next occurrence becomes due before the current one ended,
including while it waits for a retry:

| Overlap | Effect |
|---|---|
| `Overlap.Wait` (default) | The next occurrence waits for the current one to end, then starts subject to `Late`. Several waiting occurrences collapse into the latest one. |
| `Overlap.Skip` | Occurrences that become due while one is outstanding are recorded as skipped, with their outcome hook. |
| `Overlap.Allow` | Every occurrence becomes its own job when due; occurrences run and retry independently. Not combined with `AfterCompletion`. |

**Triggering** a recurring job (`TriggerAsync`) is the explicit manual run of a recurrence and does not move its
schedule. With no outstanding occurrence it starts a triggered occurrence now (`Started`); with one waiting for a retry
it makes that retry due now without resetting attempts (`MadeDue`). With one running it follows the overlap policy, as
a due instant would: under `Overlap.Wait` it records the waiting occurrence, which starts when the running one ends
(`Waiting`; several collapse into one); under `Overlap.Skip` it returns `AlreadyRunning` and queues nothing; under
`Overlap.Allow` it starts a separate occurrence. It returns the same `RunNowResult` as
[`RunNowAsync`](#operator-actions).

**Lock order.** Recurrence operations in an application transaction (`ScheduleAsync`, `RemoveAsync`, `TriggerAsync`)
lock the recurrence and then its outstanding occurrences; they are documented to come before the transaction's own
writes and need read-committed isolation where the store has isolation levels, refusing others before changing
anything, since they find occurrences the planner committed after the transaction began. The planner runs no outcome
hook: occurrences it records skipped are finalized afterwards, one per transaction, through the terminal path. It
locks recurrences with `SKIP LOCKED` and waits for rows at most a short lock timeout, so a transaction that waits for
it never waits for application code.

## Attempts and terminal states

An attempt starts with every claim that runs the handler, including takeovers after a lost lease. A graceful release
on shutdown returns the attempt to the budget, and the execution stays started (J5). A claim that finds no attempt
left after a lost lease runs no handler and uses no attempt: it ends the job failed with `attempts_exhausted`,
without a run-history entry. A claim that ends a job skipped by `Late` uses no attempt either, nor does a claim that
finds a cancellation requested while an earlier holder ran it: that holder was lost, and the job ends cancelled without
a handler. A pending cancellation takes precedence over lateness and over exhaustion, so a cancelled job whose last
attempt's holder crashed ends cancelled, not failed. The handler's result decides the next step:

| Result | Effect |
|---|---|
| `JobResult.Completed` / `Completed(result)` | Terminal success; the result is stored (J20). |
| `JobResult.Retry(after, code)` | Another attempt no sooner than `after` and the backoff; failed with `attempts_exhausted` when none are left. |
| `JobResult.Fail(code)` | Terminal failure immediately. |
| exception | `Retry(0, "handler_failed")`. |

An attempt that ends failed, timed out or retried is logged as `job.failed` (warning) with its reason code, whether the
dispatcher applied the outcome or the handler applied it in its own transaction; an exception is logged as
`job.handler_failed` with its type instead. The log carries the definition and job, never arguments or messages (R14).

A retry gives the record back due at the later of the backoff for the attempts so far and `after`; a job with a
calendar is planned at the next allowed instant from then, without using an attempt. A retry of a job with a
cancellation request ends it cancelled instead. Requests during the
wait become the pending follow-up and do not move it; `RunNowAsync` and `TriggerAsync` move it explicitly.

Terminal states are completed, failed, cancelled, timed out and skipped (J6). Every terminal transition goes through
one path that runs the definition's outcome hook in the same transaction and then starts a pending follow-up; see
[domain records and jobs](domain-and-jobs.md#outcome-hooks).

**Timeouts** (J18) are cooperative. At the attempt timeout the run's token is cancelled with reason `timeout`. The
attempt keeps its lease and DI scope until the handler returns; a handler that ignores cancellation is reported as
`job.timeout_ignored` and is never claimed twice meanwhile. The attempt then ends the job as timed out, or counts as a
failed attempt when the definition sets `RetryTimeouts`.

## Cancellation and pause

- `CancelAsync(tx, definition, subject)` cancels the subject's outstanding standalone jobs and the outstanding
  occurrences of its recurrence (the recurrence continues). A running job or occurrence is cancelled cooperatively through its lease and ends as
  cancelled (J7); the request survives a crash of its holder. Cancelling discards a pending follow-up.
- `CancelAllAsync(tx, subject)` cancels the subject's jobs of every definition; it is refused when one of them belongs
  to a definition the process does not register, whose outcome hook it could not run.
- `RemoveAsync(tx, definition, subject)` cancels the outstanding occurrences and deletes the recurrence.
- `PauseAsync` and `ResumeAsync`, for a definition or one subject, stop and restart claiming; resuming computes the
  next due time under the `Missed` and `Late` policies (J8). A paused subject stays paused for jobs requested and
  occurrences planned while it is paused, one-shot and recurring alike. The stored subject pause is the authority:
  pausing and resuming take the subject's lock exclusively, and claims try it in shared mode and read the stored pause
  after it, so no job is claimable after the pause commits, including one requested concurrently. Requests take no
  pause lock; jobs and recurrences carry a copy of the pause that keeps paused work out of the due indexes. Pausing and
  resuming update the copies of the subject's recurrences and then its outstanding jobs. A copy stored by a request,
  schedule or planning pass that raced a pause or resume can be stale; claims and planning read the stored pause
  instead, and maintenance repairs copies in both directions on every pass, skipping locked rows, so a late commit is
  corrected by a later pass. Repairing a recurrence's copy to unpaused signals planning, and a job's copy, claiming.
  A definition's pause is read by every claim after its definition fence. The planner leaves out due and waiting
  recurrences of a paused definition or subject, so they neither hold back other recurrences nor wake it; resuming
  signals planning.

## Progress, results and waiting

- `run.ReportProgressAsync(percent, message)` writes a whole percentage from 0 to 100 and a message of at most 256
  characters, fenced to the attempt's lease: a report from an attempt whose lease is no longer valid throws
  `LeaseLostException` (J19). Every claim that runs the handler clears it. It is a durable write in its own
  transaction; report at checkpoints. `JobStatus.Progress` shows it.
- A definition with a result type (`JobDefinition<TArgs, TResult>`, whose results use the host's JSON options as
  version 1, or a result codec such as `JobResults.Json(typeInfo, version)`) is run by an
  `IJobHandler<TArgs, TResult>`. A returned value completes the job and is stored with the outcome in the terminal
  transaction, with the codec's version (J20); a `JobResult` retries,
  fails or completes without a result, and `JobRun<TResult>.CompleteAsync(tx, value)` completes in the domain
  transaction. A serialized result holds at most 256 KiB (`JobResults.MaximumBytes`): a larger returned value ends the
  job failed with `result_too_large`, and `CompleteAsync` refuses it before writing.

  ```csharp
  public static readonly JobDefinition<BuildReport, ReportSummary> Build = JobDefinition.Create<BuildReport, ReportSummary>("reports.build");

  public sealed class BuildHandler : IJobHandler<BuildReport, ReportSummary>
  {
      public async ValueTask<JobDisposition<ReportSummary>> RunAsync(BuildReport args, JobRun<ReportSummary> run, CancellationToken cancellationToken)
      {
          await run.ReportProgressAsync(50, "rendering", cancellationToken);
          return new ReportSummary(pages: 12);
      }
  }
  ```
- `GetOutcomeAsync(definition, jobId)` returns the job id's latest ended execution: its execution id, outcome, reason
  code, end time and, for a definition with a result type, `HasResult` and the decoded `Result`. `Ended` is false
  while an execution of the id is outstanding, including a follow-up that reuses it; the other fields then describe the
  execution that ended before, if any. Unknown ids return null. Follow-ups of occurrences have job ids of their own.
- `WaitAsync(definition, jobId, timeout)` returns the same record once the id has no outstanding execution, or at the
  timeout with `Ended` false. While anyone in a process waits, one waiter listens for the store's signals. A caller
  marks its job as waited for once the waiter listens, without owning the job's lease, and rereads it after the mark
  committed; the transition that leaves the job without an outstanding execution clears the mark and signals `Ended`
  on commit. A completion is therefore either read or signalled. Waiters share the mark: retries and a follow-up that
  reuses the id keep it, and one waiter timing out leaves it for the others. Jobs nobody waits for signal nothing,
  since every notification serializes commits. The waiter rereads the waited jobs of a signalled definition in one
  statement, and all of them on reconnect and every second, so lost signals and stores without signals only delay it.

## Operator actions

Operator actions take a definition and a job id. Each runs in a transaction of its own, runs no outcome hook, never
duplicates running work and never throws for the job's state: it reports what it found (J21).

| Action | Effect |
|---|---|
| `RunNowAsync(definition, jobId)` | Returns `RunNowResult(Job, Outcome)`. An outstanding job that is not held becomes due now in one locked update, with its attempts and backoff count kept: `MadeDue` for one scheduled later or waiting for a retry backoff, `Paused` for one whose definition or subject is paused (due now, runs once resumed), `AlreadyDue` for one already due or whose lost lease waits for its takeover. `AlreadyRunning`: it is held; nothing is queued. `Waiting`: an occurrence waiting to be recorded skipped. `Ended`: a terminal one-shot job; rerun it with `RerunAsync`. `ChainStep`: a [chain](chains.md) step, which runs when its parent settled; nothing changed. `NotFound`: the definition has no such job. An occurrence of a recurrence that has ended triggers the recurrence, with the trigger's outcomes (`Started` names the new occurrence). |
| `RerunAsync(definition, jobId)` | Returns `RerunResult(Job, Outcome)`. A terminal one-shot job becomes outstanding again on the same record (`Rerun`): due now, the same job id, a new execution id, fresh attempts, failures and takeovers, not started, its pause copied anew. Refused, changing nothing: `Outstanding` while any execution of the job is outstanding (scheduled, running or waiting for a retry); `IdentityHeld` while another outstanding standalone job holds its merge identity, also when a request takes it concurrently; `Recurring` for an occurrence, which `RunNowAsync` or `TriggerAsync` run; `ChainStep` for a chain step, which `RetryFromAsync` reruns with its chain; `CalendarRemoved` for a job whose calendar has been removed; `NotFound`. A rerun never queues a follow-up. |
| `RerunManyAsync(definition, jobIds)` | `RerunAsync` for each, each in its own transaction, one result per given id in order; an id given again is `Duplicate`. |
| `RetryFromAsync(chainId, stepId)` | For chains; see [chains](chains.md#retry-from-a-step). |

Work with a [calendar](calendars.md#applying-a-calendar) is planned at the next allowed instant instead of made due
now. Each action has an overload with `JobActionOptions`; `IgnoreCalendar` makes the work due now and lets it start
outside the calendar's windows. `RunNowAsync` and `TriggerAsync` report `MadeDue` and `Started` either way.

After a rerun, the outcome reads as for a follow-up that reuses the id: `GetOutcomeAsync` returns the ended execution's
outcome, reason and result with `Ended` false until the rerun execution ends, and `WaitAsync` waits for that end.

## Fan-out

`RequestManyAsync(tx, definition, items)` requests one job per item (subject or partition, with arguments) in one
transaction (J9); a crash before commit leaves none, after commit all exist. For a continuation that must run after
every child has finished, use a [dynamic group](chains.md#dynamic-groups).

## Priority

`Priority` is part of a definition's stored policy, so it orders definitions, not jobs of one definition. Where
several sources share a [dispatch pool](hosting.md#pools), free capacity goes to the highest priority class with
claimable work first; rotation and equal shares apply within a class. Lower classes may starve while higher classes
keep claimable work; higher-priority work that cannot be claimed (limits, pause, incompatible versions) leaves the
capacity to lower classes. Priority never preempts a running job (J17). Lease and reconciliation sources are `Normal`.
A stored revision with a new priority applies once the dispatcher rebuilds the definition's claim statements.

## Common patterns

| Pattern | Expressed as |
|---|---|
| Fire and forget | `RequestAsync(tx, definition, args)` with no subject or merge key. |
| Delayed one-shot | `Schedule.At(time)` or `Schedule.After(delay)`. |
| One-shot only if still useful | `Late.SkipAfter(grace)`. |
| Deduplicated "make sure it runs" | Same subject or merge key with `Schedule.Now`; a request during a run causes one follow-up. |
| "Make sure one is outstanding", no follow-up | `RequestMode.IfAbsent`. |
| Debounced recompute after changes | `Schedule.Debounce(delay, maximum)` keyed by what is recomputed. |
| Fixed-delay polling | Code-defined recurring job with `Every(interval, AfterCompletion)`. |
| Wall-clock recurrence (07:00 on business days) | Code-defined recurring job with `Cron(expression, timeZone)` and a calendar. |
| Per-record recurrence chosen by users | Recurring job with a subject; the domain sets and changes it in its own transaction (D6). |
| No overlapping runs | `Overlap.Wait` or `Overlap.Skip`. |
| Run a recurring job now | `TriggerAsync`; under `Overlap.Wait` a trigger during a run queues one run after it. |
| Scheduled, manual and webhook runs that coalesce (sync a repository every 15 minutes and on demand) | A dispatcher: a recurrence on its own definition (per tenant, say) whose handler requests the standalone jobs with `RequestManyAsync`; manual actions and webhooks request the same standalone identity, so all of them merge or become one follow-up. |
| Run a scheduled or retrying job now; rerun finished ones | `RunNowAsync`; `RerunAsync`, `RerunManyAsync`. |
| Fan-out over partitions or subjects | `RequestManyAsync`. |
| Fan-out then final aggregate | A [dynamic group](chains.md#dynamic-groups) with an after-all continuation. |
| Incremental aggregate as children finish | Each child requests the aggregate with `Schedule.Debounce`. |
| Steps in order, with results | A [chain](chains.md). |
| Retry with backoff; give up | `JobResult.Retry` / exceptions; `JobResult.Fail`. |
| Wait for a job from a caller | `WaitAsync(jobId, timeout)`. |
| Request without running here | Request only; a process that registers no handler never runs the job. |

## Code-defined recurring jobs

```csharp
work.AddRecurring(DailyReport, new ReportArgs(), Schedule.Cron("0 3 * * *", "Europe/Warsaw"), new RecurrenceOptions { Missed = Missed.RunLatest });
```

The recurrence is part of the definition's revision (J10, R18). Every process that declares it ensures one record for
it; a newer revision replaces the stored recurrence, an older one never does. A recurring record that no live process
declares any longer is reported (`GetUndeclaredRecurrencesAsync`) and kept until removed explicitly with
`RemoveAsync(tx, definition)`, so a rolling deployment cannot delete a schedule an older process still runs. A process
that declares a recurrence is recorded in the [process registry](hosting.md#processes) also when it runs no handler.

## Arguments

Arguments are serialized by the requesting process's codec and stored with their version (J11). Which processes may
claim a job depends on that version, as described under [executable compatibility](#definitions-and-registrations).
A version below the newest definition's `MinimumArgsVersion` ends the job as failed with `args_version`, without
deserializing the arguments. Arguments have a size limit; large inputs belong in the domain store, referenced by
identity.

## Retention

Finished jobs, finished chains, run history, resolved [incidents](monitoring.md#storage-and-retention) and stopped
process records are deleted after their retention windows (J12). Resolved incidents are kept 90 days unless
`work.Retention(jobs, runHistory, incidents)` sets another duration, and the retention job also deletes the expectation
rows of keys no longer expected. Process records are kept 7 days after the process stopped or its heartbeat became
stale and are deleted by the next starting process, not by the retention job (see [processes](hosting.md#processes)). For jobs and run history the windows are the definition's `Retention` and `RunHistoryRetention`, measured from
the job's or the attempt's end. They are stored with the definition's revision like the rest of its policy; a policy
that leaves one unset takes the default of the host that stores the revision, 7 days for jobs and 30 days for run
history unless `work.Retention(jobs, runHistory, incidents)` sets others. Whether attempts are recorded at all follows
the same rule: `RunHistory` left unset takes the host's `work.RunHistory(record)`, off by default. The declared values
are part of the digest, the host's defaults are not, so a changed default applies to a definition from its next stored
revision. An unset `RunHistory` and `false` share one digest, the one definitions had before the host default existed,
so upgrading keeps stored revisions.

- Only terminal jobs are deleted, selected by state: a restarted follow-up or a rerun keeps the end time of its
  previous execution while it is outstanding. Jobs with and without a subject are retained alike.
- Ended occurrences are kept until their recurrence's planner consumed them, and a recurrence's awaited occurrence
  is kept.
- Run history is retained independently of its jobs: entries outlive a deleted job, and expire while it is kept.
- A deleted job's outcome and result go with it: `GetOutcomeAsync` returns null as for an unknown id, and status by
  subject no longer shows it. Applications that need outcomes for longer persist them through the outcome hook.

Retention is a recurring job (`WorkRetention.Definition`, hourly on the hour, no run history of its own) that a
process runs only when it opts in with `work.AddRetention()`; `work.Retention(jobs, runHistory, incidents)` only sets
default durations. Processes that only request work stay without a dispatcher. At least one worker deployment per shared store
and application opts in; several replicas may, and the job's lease runs one pass at a time. Each run deletes in
batches of 1 000 per definition until a pass leaves nothing more, each statement in its own transaction and skipping
locked rows, so it never waits for a transition, a rerun or a planner; it can be run at once with
`TriggerAsync(WorkRetention.Definition)`.
