# Problems

An application with background processing rarely has one kind of background work. It has several, and they differ in
who owns the state, who runs the work and what "done" means. A framework that models all of them as jobs forces each
domain to keep two records of the same work in sync. This document names the kinds and the requirements each one
places on a framework. Requirement IDs are stable; the design documents and the [scenarios](scenarios.md) reference
them.

## The kinds of durable work

| Kind | Example shape | Owner of the state | Needs a scheduler |
|---|---|---|---|
| Execution | A long-running agent session, a streamed answer being delivered to a chat thread | The domain record and its state machine | No |
| Remote work | A build claimed and run by a process in another language, reporting back through an API | The domain record | No |
| Convergence | A resource whose placement, projection or publication must match its latest requested state | The domain record | No |
| Job | Retention cleanup, a periodic feed import, a fan-out over every tenant, a recompute after a change | The job store | Yes |
| Job on a domain record | Synchronizing one configured feed or connection whose status users see | Both, unless the framework prevents it | Yes |
| Chain | Fetch orders, generate invoices, then send them; parallel steps followed by a final step | The job store | Yes |
| Event delivery | "A repository was deleted": several subscribers react | The message infrastructure | No; out of scope |
| Process-local check | A replica checking its own certificate files every hour | Nobody; nothing is persisted | No; out of scope |

## Requirements shared by every durable kind

- **R1 Transactional requests.** Requesting, cancelling and rescheduling work commit in the caller's transaction, with
  the domain change that causes them. A rolled-back change leaves no work behind.
- **R2 Exclusive ownership.** At most one holder holds a piece of work at a time. Ownership is a lease that expires
  when the holder stops renewing it.
- **R3 Fencing.** Every claim has an epoch. A holder whose lease is no longer valid cannot commit output, renew,
  release or complete; the check happens inside the transaction that commits the output.
- **R4 Store clock.** Due times, lease expiry and backoff use the database clock, so process clock skew cannot hand
  one piece of work to two holders.
- **R5 Recovery without a sweep.** Work whose holder crashed becomes claimable again once its lease expires, without a
  separate re-dispatch process or message.
- **R6 Capacity-bounded claims.** A process claims only as much work as it has free slots for. Work is pulled, never
  pushed at a process that cannot take it.
- **R7 Prompt wake-up.** New or newly due work is picked up without waiting for a polling interval when the store can
  signal it; polling remains the correctness fallback.
- **R8 Cooperative cancellation from anywhere.** Any process can request cancellation of running work; the holder
  observes it within one renewal interval through its cancellation token. No extra infrastructure is needed.
- **R9 Takeover accounting.** A crash that loses a lease counts against the work; a graceful shutdown that releases
  it does not. Work that repeatedly takes its holder down stops being handed out and the domain decides what happens
  to it.
- **R10 Roles, not scheduler nodes.** Different process roles run different work. A process runs what it registers;
  one that registers nothing only requests work. There is no designated scheduler process.
- **R11 Graceful shutdown.** Running work gets a grace period to observe shutdown; work still held after it is
  released for another process.
- **R12 Unserved work is visible.** Due work that no live process registers is reported.
- **R13 Partition fairness.** Work can carry a partition, such as a tenant. Limits on concurrently running work per
  partition hold across all processes, and one busy partition cannot starve the others.
- **R14 Observability without data leaks.** Traces, metrics and logs identify work by kind, key and outcome code.
  Payloads and error messages, which may contain user data, are not exported by default.
- **R15 No reflection.** Registration and serialization work under trimming and Native AOT; reflection-based JSON is
  an opt-in convenience that the analyzers flag.
- **R16 Store independence.** Semantics are defined once and implemented natively per database; no layer depends on
  one database, ORM or message bus.
- **R17 Execution guarantee.** Work runs at least once. Fencing protects only transactions that take part in the
  guard; it cannot retract an HTTP call or a sent message. Every run exposes an execution identity, stable across the
  attempts of one deliberate execution and new for every deliberate new execution (a rerun, a follow-up, an
  occurrence, a retried chain step), and an attempt identity. Deduplicating external effects needs the external
  system's cooperation (an idempotency key or a lookup by a client-chosen id); the framework states this rather than
  implying a local marker is enough.
- **R18 Rolling deployments.** Definitions carry revisions. A process with an older revision never overwrites what a
  newer one stored, and policies that must agree across processes (attempt limits, distributed limits, schedules)
  come from the newest stored revision.
- **R19 Application isolation.** Several applications can share one database without seeing, claiming or reporting
  each other's work.
- **R20 Testability.** Applications can test their handlers and flows in one process without a database, with
  controllable time, using the same semantics as the real providers wherever one process can observe them.
- **R21 Executable compatibility.** Stored policy cannot change code. A process claims only work whose argument
  version its own handlers and codecs accept; work requested by newer code is never claimed, or failed, by older code.
  Results read by newer code than their reader's are detected and refused before they are decoded.

## Executions

Long-running work with its own lifecycle: it may pause to wait for something external, spawn child executions, stream
output, and has domain deadlines.

- **E1 Domain state machine.** The domain defines the states (runnable, running, waiting, finished, ...). The
  framework only knows whether the record is eligible, when it is due and who holds it.
- **E2 Due-based claiming.** The domain marks a record claimable now or at a later time (backoff), or not claimable
  (waiting, finished). Executors claim the earliest due records up to their capacity.
- **E3 Claim by identity.** A process can also try to claim one specific record, for example the one it just created.
- **E4 Progress with renewal.** A holder can write progress, such as a delivery cursor, in the same atomic step that
  extends its lease, so a successor resumes from the last confirmed progress.
- **E5 Release with backoff.** A holder can give the record back with a not-before time after a failed attempt.
- **E6 Terminal transition in the domain transaction.** Ending the execution and releasing the lease commit together
  with the domain's final state.
- **E7 Claims from domain-selected candidates.** When eligibility depends on the claimant and on other domain data,
  the domain selects candidates with its own query and claims among them inside its own transaction, rechecking
  eligibility and reserving resources there.
- **E8 Claim hook.** Domain bookkeeping that must commit with the claim (settling an interrupted attempt, starting a
  new one, journaling) runs inside the claim transaction, with the previous owner, epoch and the reason for the claim.
  A failing hook rolls the claim back.
- **E9 Takeover backoff.** After a lost lease, the record becomes claimable only after a delay that grows with
  consecutive takeovers.
- **E10 Expiry settlement.** Resources tied to an expired lease are released promptly, even when nobody claims the
  record again. Settlement happens once per expired epoch, whether by an expiry processor or by the next claimant.
- **E11 Failure accounting.** Releases after a failed attempt are counted separately from takeovers.
- **E12 Spread limits.** A process can be limited to a number of held records per partition, so long-held records
  spread across replicas.
- **E13 Several sources per record type.** One table can carry several independent lease sources, each with its own
  identity and fields.

## Remote work

- **W1 Claims as operations.** Claim, renew, release and complete are callable by an API on behalf of a holder that
  does not run the framework. The fence travels as an opaque token.
- **W2 Holder identity.** A remote holder has its own owner identity; expiry alone decides that it is gone.
- **W3 Holder capacity.** A limit on records held by one owner is enforced by the store on every claim path.
- **W4 Replayed completion.** A holder that lost the reply to its completion can send it again: the same completion
  identity and payload returns the recorded result; a different payload or a superseded epoch is rejected.

## Convergence

A record states what it should be (a requested revision); the work makes the world match it.

- **C1 Request by revision.** A request increments the record's requested revision in the caller's transaction.
  Many requests before a run merge into one run.
- **C2 Requests during a run.** By default a request that arrives during a run produces exactly one follow-up run
  after the current one ends.
- **C3 Debounce with a ceiling.** Requests may delay eligibility by a trailing debounce, never beyond a maximum delay
  from the first outstanding request. The ceiling bounds when the record becomes due, not when capacity is free.
- **C4 Waiting is not failing.** The handler can report that the record is waiting for something (capacity, another
  resource) with a retry time and a reason the domain stores. Waiting does not consume attempts.
- **C5 Never give up.** Failed convergence retries with capped exponential backoff for as long as the record exists.
- **C6 Mass invalidation.** A change of global policy makes every record of a kind due once without writing
  every record, while waiting and backoff still apply after that attempt.
- **C7 Periodic re-evaluation.** A converged record can be re-evaluated after an interval even without a request.
- **C8 Fairness across kinds.** One worker pool can serve several kinds of records without one kind starving the
  others.
- **C9 Supersession.** A source can choose that a newer request or invalidation makes the running attempt obsolete:
  its renewals and output fail, and the newer revision runs without waiting for it.
- **C10 Request options.** A request can explicitly reset backoff and failures, so a user action is not delayed by
  earlier failures.

## Jobs

Work that has no record of its own.

- **J1 One schedule model.** Run now, at a time, after a delay, after a trailing debounce, every interval (after
  completion or at a fixed rate) and on a cron expression with a time zone are values of one schedule type, used by
  one API. Recurrence is a property of the schedule, not a separate kind of job.
- **J2 Late and missed work.** A job can be skipped when it would start more than a grace period late. A recurring
  job states whether occurrences missed while nothing ran are skipped, or whether the latest one still inside the
  grace period runs once.
- **J3 Merging.** Requests with the same identity merge into one outstanding job; the latest arguments win, so
  arguments carry identities rather than data.
- **J4 Follow-up runs.** A request that arrives while a job runs produces one follow-up run.
- **J5 Attempts.** A job, or one occurrence of a recurring job, has a maximum number of attempts, counting lost
  leases, with capped exponential backoff and a handler-requested minimum delay.
- **J6 Terminal states.** Completed, failed, cancelled and skipped are final for a one-shot job and for one occurrence
  of a recurring job; the recurrence continues after each occurrence.
- **J7 Cancellation.** A scheduled job or occurrence can be cancelled in the caller's transaction. A running one is
  cancelled cooperatively (R8) and ends as cancelled.
- **J8 Pause.** A job definition, or the jobs of one subject, can be paused and resumed.
- **J9 Fan-out.** One transaction can request one job per partition (or per subject) for a large set; either all
  requests commit or none do.
- **J10 Code-defined recurring jobs.** Recurring jobs declared in code exist exactly once regardless of how many
  processes declare them, follow the newest declared revision, keep their schedule across restarts, and are reported
  when no live process declares them any longer.
- **J11 Argument versions.** Arguments carry a schema version and are never misread. A job fails with a stable reason
  only when its version has been retired on purpose; otherwise it waits for a process that accepts it (R21).
- **J12 Retention.** Finished jobs, run history, resolved incidents and stopped process records are removed after
  retention windows.
- **J13 Ensure outstanding.** A request can create a job only if none with its identity is outstanding, leaving an
  existing one (revision, arguments, schedule, attempts) untouched and creating no follow-up.
- **J14 Occurrence lifecycle.** An occurrence of a recurring job keeps its identity and attempts until it succeeds,
  fails or is exhausted, then records its outcome and the schedule advances. Retries never run past the next
  occurrence's due time unnoticed: the definition chooses whether the next occurrence waits or the retrying one ends.
- **J15 Overlap.** A recurring job can forbid overlapping occurrences across all processes: an occurrence due while
  the previous one runs is skipped, not queued.
- **J16 Calendars.** Named availability calendars (time zone, weekdays, daily windows, excluded dates, composition,
  application-provided dates) restrict when work may start. A blocked one-shot job is rejected, skipped or moved to the
  next allowed time; a blocked cron occurrence is skipped. The rule applies to the actual start, including retries,
  and to work that became due inside a window but could not start before it closed. Application-provided dates give
  every process the same answer, have a bounded lookahead and a defined behaviour when unavailable.
- **J17 Priority.** Due work of higher priority is claimed first; priority never preempts running work.
- **J18 Timeouts.** An attempt can have a cooperative timeout. A timed-out attempt is cancelled and keeps its lease
  until the handler actually returns; it then ends the job as timed out, or counts as a failed attempt when the
  definition retries timeouts.
- **J19 Progress.** A running job can report a percentage and a short message, fenced to its attempt.
- **J20 Results.** A job can return a typed result, stored with its outcome; callers can read the outcome or wait for
  it with a timeout.
- **J21 Operator actions.** Run a job or recurring job now, rerun a finished one, retry many at once, all without
  duplicating running work.
- **J22 Definition-wide limits.** A definition can limit its running jobs across all processes, in addition to
  per-process capacity and partition limits.

## Chains

Jobs that depend on each other, without becoming a general workflow engine.

- **K1 Steps and run conditions.** A chain is created atomically. A step runs after its predecessor settles, if its
  run condition (on success, on failure, on cancellation, on any outcome) holds; otherwise it is skipped.
- **K2 Parallel groups.** A group runs branches in parallel; continuations of the group run when every branch has
  settled, by the group's aggregate outcome (all succeeded, any failed, any failed or cancelled, always).
- **K3 Result passing.** A step reads the typed result of its predecessor.
- **K4 Dynamic groups.** A step can add a group of many children (fan-out) with a continuation that runs once all of
  them have settled (fan-in).
- **K5 Recurring chains.** A recurring schedule can create a fresh chain for every occurrence; the overlap policy
  considers the whole previous chain.
- **K6 Retry from a step.** A settled chain can be retried from a chosen step: completed ancestors are not run again;
  the step and what depends on it run again under a new generation.
- **K7 Skipped roots.** A chain whose root is skipped (late, blocked by a calendar) skips all its steps without
  running any handler.
- **K8 Chains as units.** Chains are retained, deleted and read as a unit; deleting a chain with running steps is
  refused or cascades by an explicit choice.

## Jobs that act on domain records

The source of most inconsistency between "the job" and "the thing": a feed, a connection or a report has a status
users see, and a job does the work.

- **D1 Subject identity.** A job names the domain record it acts on. Requests for the same job definition and subject
  merge by default.
- **D2 Lifecycle follows the subject.** Deleting or disabling the subject cancels or pauses its jobs in the same
  transaction; output of a job already running for it is rejected inside its own transaction.
- **D3 Atomic completion.** A handler can complete its job inside the transaction that commits its domain output, so
  the domain never shows "done" while the job shows "running", or the reverse.
- **D4 Outcome hooks.** Every terminal transition, whether the handler, an operator, a cancelling request or the
  framework causes it, tells the domain inside the transaction that makes the transition, including when the stored
  arguments can no longer be read.
- **D5 Status by subject.** The domain reads the scheduling status of its subjects (next run, running since, attempts,
  last outcome) through an API, singly and in batches, never by querying framework tables.
- **D6 Schedules set by users.** A per-subject recurring schedule, such as a user-chosen sync interval, is set and
  changed in the domain transaction.
- **D7 Run history.** Optional per-attempt history per subject, with its own retention.

## Monitoring

- **M1 Expectations.** A definition can declare that it completes by a local time on calendar days, that an attempt
  does not run longer than a duration, and that it does not fail more than N times in a row, tracked per subject.
- **M2 Incidents.** A broken expectation opens an incident and a later success resolves it. Incidents are delivered to
  application handlers and as telemetry; notification channels belong to the application.
- **M3 Telemetry.** OpenTelemetry traces and metrics cover claims, runs, outcomes, lateness, takeovers and incidents.

## Out of scope

- Event delivery to several subscribers, inboxes and outboxes for messages: use the application's message
  infrastructure. Work dispatch does not need it; claimable state replaces "a message that says work exists".
- Process-local timers with no persisted state.
- Workflow engines beyond K1–K8: no branching on data, loops, signals between steps or long-lived sagas.
- A dashboard, AI assistance and notification transports (chat, mail, webhooks). Domains present work state in their
  own UI; the framework exposes read APIs, incidents and telemetry.
- Storing handler log lines. Logs flow through the application's logging pipeline, correlated by work identities.
- Ordered per-key queues (one active item per key, FIFO waiting) until a second, different use case needs them.
