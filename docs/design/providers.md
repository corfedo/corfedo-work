# Store providers

A provider implements the framework's semantics natively for one database. The core defines what each operation
means; the provider decides how to make it atomic (R16).

## The contract is operations, not queries

Providers implement operation interfaces: lease operations (claim due, claim by key, claim candidates in a caller's
transaction, renew, guard, release, end, schedule, cancel, settle expiry), reconciliation operations (request,
complete, invalidate), job operations (request in each mode, complete, cancel, pause, trigger, rerun, read status,
chain settlement, retention), calendars and definitions by revision, incidents and the process registry. Each
operation is one atomic step in the store; the core never composes a provider operation out of generic reads and
writes.

This keeps database-specific behaviour where it belongs. A generic query layer would force every provider into the
lowest common denominator and push the gaps to runtime failures.

## Required guarantees

Every provider must provide, and the conformance suite checks:

- **Store clock.** Every time comparison and every stored time uses one clock of the store.
- **Lock, then evaluate.** Every operation that depends on lease validity or claimability first holds the record's
  lock and then evaluates the condition against the latest committed version, with a clock reading taken after the
  lock is held. A condition evaluated before a lock wait is never trusted, whatever the store's own re-check rules.
- **Non-blocking claims.** Claims and settlements skip records whose lock another transaction holds instead of waiting
  for them.
- **Modifying guard.** `GuardAsync` writes the record (it extends the lease) and holds its write lock until the
  caller's transaction ends, so it conflicts with every concurrent claim, renewal, settlement, supersession and guard.
- **Outstanding-lease accounting.** Partition, spread, owner and definition limits count outstanding leases (claimed
  and not yet released, ended, superseded, taken over or settled), never merely unexpired ones, and hold on every
  claim path, including claim by key and claims in the caller's transaction. Counting and claiming within one limit
  scope are serialized.
- **Generation fence.** On supersede sources, guards, renewals in the caller's transaction, completions and ends hold
  the source's generation lock in shared mode until their transaction ends and check the claimed generation;
  invalidation takes the lock exclusively, and fenced transactions that start while it waits queue behind it. Fences
  are taken before row locks, several sources' fences in one fixed order, and a fence refused for the transaction's
  isolation level aborts the transaction. Claims
  retire outstanding leases of obsolete generations as supersessions, evaluated after the record lock is held, without
  locking the generation: generations only grow, so a lease found obsolete stays obsolete.
- **Version filters.** Claims include only records whose argument version the claiming registration accepts (R21).
- **Definition fence.** Every job claim takes its definition's fence in shared mode before any record or
  limit-scope lock and reads the stored revision and pause state after it; storing a definition takes the fence
  exclusively, and claims that start while it waits queue behind it. A claim built from another revision takes
  nothing.
- **Subject pause serialization.** Pausing and resuming a subject is serialized with claims of its jobs, which read
  the stored pause after the serialization point, so a job created concurrently with a pause is not claimed once the
  pause commits. Requests are not serialized with pauses.
- **Atomic transitions.** Claim with its hook, terminal transitions with their outcome hooks and chain settlement,
  expiry settlement with its handler, and fan-out each commit or roll back as a whole.

## Transactions

Operations that join the caller's transaction take a `WorkTransaction`, a handle to the application's own
transaction. Any ADO.NET `DbTransaction` converts to it implicitly, without naming the provider, and every conversion of
one transaction gives the same handle; the store or
signal transport an operation runs on refuses a transaction of another provider's type. The handle also numbers the
savepoints operations take inside the transaction.

| Application uses | Adapter |
|---|---|
| ADO.NET | The `NpgsqlTransaction` itself, by the implicit conversion |
| EF Core | `Corfedo.Work.EntityFrameworkCore`: `context.Database.WorkTransaction()`, from the `DbContext`'s current transaction, for work and signals |
| The in-memory store | Its transactions are handles themselves |
| A document store | The provider's session type, for example a MongoDB client session |

A transactional operation without an active transaction is refused; it would otherwise commit ahead of the domain
change (R1). Claims in the caller's transaction require the store's read-committed isolation where it has one
(PostgreSQL's default): limits count the outstanding leases of other records, and a snapshot taken before the
claim serializes its limit scope would hide claims committed meanwhile. Other levels are refused. Recurrence operations
in the caller's transaction (`ScheduleAsync`, `RemoveAsync`, `TriggerAsync`) require it as well: they find the
recurrence's outstanding occurrences after their locks, and an earlier snapshot would hide one the planner committed
meanwhile. They refuse other levels before any lock or write; the caller's transaction is neither switched to another
level nor retried. Requests, job completions in the caller's transaction, `CancelAsync` and `CancelAllAsync` run at
any level. Guards, renewals and
ends lock their own record first and are safe at any level on sources that do not supersede, since a
snapshot-isolated transaction fails when that record changed after its snapshot. On superseding reconciliation
sources, guards, renewals in a transaction, completions and ends require read-committed isolation as well and refuse
other levels: an invalidation changes the source's generation, not the record, so a snapshot taken before it commits
would still authorize an obsolete holder's output. Leases and reconciliation on domain records require the domain data and the work fields to be in the
same store; that is what makes `GuardAsync` and `CompleteAsync` atomic. Hooks that run inside framework-initiated
transactions (claim hooks, outcome hooks, expiry handlers) receive the handle and bind their own data access to it.

## Schema ownership

- **Framework tables** (jobs, chains, run history, definitions, calendars, source generations, expectation states, incidents,
  processes)
  are created and upgraded by the provider's versioned scripts, applied at startup under a store-level lock, or by an
  explicit command for deployments that do not change schemas at runtime. The application's ORM migrations never
  contain them. A provider isolates applications that share a database (R19); PostgreSQL gives each its own
  framework schema, from which lock keys and the signal channel derive as well.
- **Grants.** The provider ships the permissions each process role needs on framework tables (request-only,
  executing, migrating) as scripts, so roles with their own database logins get least privilege.
- **Domain records** belong to the application. Its own migrations add the lease and reconciliation fields; the EF
  package offers `HasLease(source)` and `HasReconciliation(source)` model helpers. They map the entity to the
  source's table, map each field as a property named after its column in Pascal case (`DueAt` for `due_at`; the
  entity's own property of that name, or a shadow property), and add the stored `LeaseOutstanding` column and the
  filtered indexes the source's claims, expiry settlement and limits need, in one shape that PostgreSQL and SQL Server
  both verify. At startup the provider verifies that every registered source has its fields and the
  indexes its claim statements need, and refuses to start otherwise.

## Conformance

`Corfedo.Work.Conformance` contains the [scenarios](scenarios.md) as executable tests against the provider
contract, including concurrent claimers, holders paused past their lease, store clock checks, lock waits, and process
crashes between steps. A provider is supported only while it passes the whole suite. Provider-specific tests cover
only what the suite cannot see, such as wake signals and index use.

## PostgreSQL

The first provider.

- Claims: candidates are selected and locked with `SELECT ... ORDER BY priority, due LIMIT n FOR UPDATE SKIP
  LOCKED`; a second statement in the same transaction re-evaluates claimability with `clock_timestamp()` and updates
  the rows that still qualify. Store-enforced limits serialize claims of one limit scope with a transaction-scoped
  advisory lock, taken in the order of the lock keys, and count outstanding leases; candidate selection leaves out
  saturated partitions and takes no more records than the owner has free slots. A further scan of the owner index
  adds the owner's own claimable outstanding leases, which a claim replaces without taking a slot, so an owner at its
  limit can still replace them; its cost grows with the owner's outstanding leases. Overlapping scans keep each record
  once. A claim in the caller's transaction
  waits for its scope locks. A claim in the provider's own transaction holds the candidates' row locks by then, so it
  only tries them (`pg_try_advisory_xact_lock`): waiting could deadlock with an application transaction that holds
  the scope and then writes one of those rows. A due claim skips the records of a busy scope and leaves its partition
  out of the call's later passes, so it never waits for an application transaction; a busy owner scope ends the call.
  The skipped records are claimed by a later call. A claim by key in the provider's transaction waits for the busy
  scope at the start of its next attempt, before it locks any row.
- Guard and renewal: `SELECT ... FOR UPDATE` on the record, then an `UPDATE` matched on owner, epoch and
  `LeaseExpiresAt > clock_timestamp()` evaluated after the lock is held. A single `UPDATE ... WHERE` is not enough:
  after waiting on a transaction that locked the row without changing it, PostgreSQL does not re-evaluate the
  condition, so an expiry checked before the wait could pass for a lease that expired during it.
- Generations: one row per reconciliation source in the framework table `reconcile_generations`; invalidation
  increments it with `INSERT ... ON CONFLICT DO UPDATE`, and claims read it as a scalar subquery evaluated once per
  statement.
- Generation fence: a transaction-scoped advisory lock per source, `pg_advisory_xact_lock_shared` in fenced
  transactions and `pg_advisory_xact_lock` in invalidation before the generation row is updated. The framework
  function `generation_fence(keys, exclusive)` takes the locks of one or several sources in byte order of their keys
  and raises SQLSTATE `CWF01` for a shared fence outside READ COMMITTED, so the transaction is aborted; fenced
  statements run it before they lock their record. A row lock would not
  do: a new `FOR SHARE` is granted while an `UPDATE` waits for earlier share lockers, so overlapping guards could keep
  an invalidation waiting indefinitely; the lock manager queues a new shared request behind a waiting exclusive one.
- Jobs: one framework table `jobs` for every definition, keyed by job id. Each definition is a lease source named
  `job:<key>` whose statements all carry `definition = '<key>'`, and every job index leads with `definition`.
  Claimability adds the eligibility filter (accepted argument versions, not below `MinimumArgsVersion`, not paused);
  settlement, cancellation and expiry lookups use the definition scope only. Unheld due jobs come from an index on
  `(definition, args_version, due_at)` with one range per accepted version, so due jobs of versions the process does
  not accept are never read; the locked candidates of all ranges are evaluated together in due-time order, and the
  claim keeps the earliest within its limit. Job sources always settle expiry. A job claim sets the attempt count and the started flag
  in its `UPDATE`: no attempt for a claim that finds the job late under `Late.SkipAfter` before its execution started,
  that finds no attempt left after a lost lease, or that finds a cancellation requested while an earlier holder ran
  it; each ends the job in the claim's run without a handler, and a pending cancellation takes precedence. A graceful release returns the attempt when the job
  records that the released epoch used it.
- Job transitions: `CompleteAsync` locks the job, then reads its attempts, cancellation request and the stored policy
  with the lease's validity evaluated after the lock is held; the transition's `UPDATE` repeats the validity check.
  Every terminal transition is one `UPDATE` that records the outcome, gives up the lease and either makes the job
  terminal or starts a standalone job's pending follow-up, and the outcome hook runs, all in the same transaction. An
  ended occurrence keeps an unconsumed mark for the planner and signals planning. Cancellation locks the
  outstanding jobs of a subject or merge key, standalone jobs and the occurrences of the identity's recurrence,
  together with the steps waiting for their parent in those jobs' chains, in one statement in the order of
  [chains](chains.md#storage-postgresql), waiting, gives a held job a cancellation request and
  signals its holder's process, and ends every other one through the terminal path. Retirement of argument versions
  locks one job per transaction with `SKIP LOCKED` and re-evaluates it against the stored minimum after the lock.
- Merging: a merge identity column `merge_key`, the canonical subject or the explicit merge key, null for
  `RequestMode.New`, for requests without an identity, and for every occurrence, so only standalone jobs merge. The
  arbiter is
  `UNIQUE (definition, merge_key) WHERE merge_key IS NOT NULL AND state = 0`, where state 0 is outstanding;
  `Merge` uses `ON CONFLICT (definition, merge_key) WHERE merge_key IS NOT NULL AND state = 0 DO UPDATE` and
  `IfAbsent` the same target with `DO NOTHING`. `DO UPDATE` locks the existing row before its `SET` is evaluated
  with `clock_timestamp()`; its `WHERE` requires the same partition. A row it leaves unwritten is looked up: another
  partition's job refuses the request. Requests never read `job_recurrences`. `RequestManyAsync` binds the items as arrays to one `INSERT ... SELECT FROM unnest(...)` per
  round, mode and schedule, where a round holds each merge identity once, so items of one identity apply in order. Terminal jobs leave the index, so they are retained and a later request creates a new job;
  a request racing a completion waits for the row lock and then merges into the follow-up, or creates a new job when
  the completion made the old one terminal. A rerun makes a terminal job outstanding again and is refused when
  another job of its identity is outstanding; a request that takes the identity concurrently makes the rerun's update
  fail with a unique violation (SQLSTATE 23505), reported as the identity being held.
- Job measurements, PostgreSQL 18.6, 1M jobs over 10 definitions (70% terminal, 25% due, 5% held, 5 000
  outstanding leases per definition): a due claim pass with the definition and partition limits takes 0.8 ms for 32
  jobs; with 12 500 due jobs of an unaccepted version ahead of the accepted ones, the due scans of a pass accepting
  versions 1 and 2 take 0.56 ms (version 1, 32 jobs) and 0.03 ms (version 2, none) with one range per accepted
  version, against 16 ms for the single scan when one range filters the version. The numbers are of the scan
  statements, not of a whole claim pass with its fence, expiry scan and update. The definition-limit count is an index-only
  scan of the partition index, 0.3 ms at 5 000 outstanding leases. A request merging into an outstanding job takes
  0.1 ms; `RequestManyAsync` of 50 000 new subjects is one statement of 0.7 s. The next due time takes 0.06 ms.
- Recurrences: a framework table `job_recurrences`, one row per recurrence, with the next instant, the occurrence
  waiting under `Overlap.Wait` and `awaited_job`, the occurrence whose end sets an after-completion schedule's next
  instant. Occurrences are `jobs` rows with `recurrence_id` and `occurrence`, unique by
  `(recurrence_id, occurrence) WHERE recurrence_id IS NOT NULL`; planning inserts them with `ON CONFLICT ... DO NOTHING`
  on that index. A planning pass tries the definition fence in shared mode, locks due recurrences, waiting ones and
  those with unconsumed ended occurrences with `FOR UPDATE SKIP LOCKED`, the first two only while neither the
  definition nor the recurrence's subject is paused (`NOT paused` on the recurrence's copied flag, matching the
  partial indexes `job_recurrences_next` and `job_recurrences_waiting`, and `NOT EXISTS` on `job_pauses` for a stale
  copy), and a due after-completion recurrence only while none of its occurrences is outstanding, all of
  which also holds for the next instant it returns. It reads the clock after the locks and plans each
  recurrence in a savepoint with a `lock_timeout` of 50 ms, so a recurrence whose rows are locked is rolled back and
  left for the next pass. It runs no outcome hook: an occurrence the overlap policy skips is inserted with
  `skip_pending` and finalized afterwards through the terminal path, one per transaction with `SKIP LOCKED`.
  Terminal occurrences carry an unconsumed mark that the planner clears; an index over unconsumed terminal occurrences
  finds them. Overlap, waiting and after-completion spacing count the recurrence's own outstanding occurrences
  through `jobs_recurrence (recurrence_id) WHERE recurrence_id IS NOT NULL AND state = 0`. `ScheduleAsync` locks or
  creates the recurrence and touches no job; `TriggerAsync` locks the recurrence and then its outstanding
  occurrences in id order, and `RemoveAsync` locks the recurrence and then its outstanding occurrences with the steps
  of the chains they own, in the cancellation order of [chains](chains.md#storage-postgresql). A trigger during a running occurrence under `Overlap.Wait` sets `waiting_at`
  (kept when already set), which the planner releases once no occurrence is outstanding.
- Definition fence: a transaction-scoped advisory lock per definition, `pg_advisory_xact_lock_shared` in claims and
  `pg_advisory_xact_lock` when a definition is stored or paused; the lock manager queues new shared requests behind a
  waiting exclusive one, so claims cannot starve a definition update. The claim's first statement takes it and reads
  `revision` and `paused` from `job_definitions`.
- Subject pause: rows of `job_pauses` are the authority. `PauseAsync` and `ResumeAsync` take a transaction-scoped
  advisory lock per definition and subject exclusively, store or delete the row and copy it to the `paused` flag of the
  subject's recurrences and then its outstanding jobs, the lock order of recurrence operations. A claim tries the lock
  of each locked record's subject in shared mode, leaves out records whose subject is busy, and checks `job_pauses` in
  its eligibility, a later statement, so it sees every pause committed before the lock was granted. Requests,
  schedules and planning take no lock and copy the flag from `job_pauses`; one that raced a pause or resume stores a
  stale flag, which claims and planning ignore in favour of `job_pauses`. Maintenance repairs the flags on every pass
  with four statements, clearing and then setting the flags of recurrences and then of jobs, each an `UPDATE` of at
  most 100 rows chosen with `FOR UPDATE SKIP LOCKED`; clearing a recurrence flag signals planners and clearing a job
  flag signals claimers. The pass reports the most rows one statement repaired, and maintenance repeats at once only
  when one filled its limit, never because flags stay stale behind locked rows. The flags keep paused jobs out of the
  due index and paused recurrences out of the planner's indexes. Taking the lock in requests was rejected: a
  fan-out takes one lock per subject, and 17 000 advisory locks in one transaction failed with `out of shared memory`
  at default settings, and a request after a domain write deadlocked with a pause.
- Recurrence and pause measurements, PostgreSQL 18.6, 1M jobs over 10 definitions as above, 100 000 recurrences of one
  definition (100 due, 2 000 unconsumed ended occurrences) and 1 000 paused subjects of another with 100 000 due jobs
  due before its 25 000 unpaused ones: the planner's selection of 100 due recurrences takes 0.1 ms, of recurrences with
  unconsumed occurrences 1.5 ms, reading the outstanding occurrences of 100 recurrences 1.3 ms, consuming their ended
  occurrences 2 ms and one occurrence insert 0.13 ms. A merging request with the pause copy takes 0.11 ms;
  `RequestManyAsync` of 50 000 new subjects, 1 000 of them paused, 0.7 s. The due scan of the definition with paused
  subjects takes 0.08 ms with the copied flag in the due index and 150 ms without it, reading every paused due job.
- Pause repair and planning measurements, PostgreSQL 18.6, the 1M jobs above and one definition with 1 000, 50 000 or
  100 000 paused subjects: 100 000 flagged due jobs over them due before 25 000 unpaused ones, and one flagged overdue
  recurrence per paused subject ahead of 100 due unpaused after-completion recurrences and 900 later ones. With no
  stale flag, the worst case since each statement reads every candidate before its `LIMIT` applies, clearing job flags
  takes 19 / 33 / 36 ms and setting them 23 / 43 / 29 ms; clearing recurrence flags takes 0.5 / 17 / 33 ms and
  setting them 0.3 / 4 / 6.6 ms, so a pass costs 43–104 ms. Clearing reads its candidates in a materialized anti join:
  under the `LIMIT` the planner probes `job_pauses` once per flagged job, 167 / 227 / 254 ms. With `jobs_paused` and
  `job_recurrences_paused` ordered by subject, measured on a MacBook (Colima), clearing job flags takes 34 / 23 / 37 ms
  against 37 / 54 / 61 ms on the `(definition, id)` indexes, and clearing recurrence flags 2.9 / 24 / 41 ms against
  1.9 / 22 / 38 ms: the planner hashes `job_pauses` for recurrences, where a merge of the two indexes takes 28 ms at
  100 000. Once vacuum has set the visibility maps of both tables, the planner merges the two indexes for recurrences
  as well: clearing recurrence flags takes 0.21 / 12 / 23 ms and clearing job flags 8.2 / 16 / 23 ms (JIT off,
  medians of 9). Selecting the subjects of flagged rows `EXCEPT` the definition's subjects in `job_pauses` and joining
  back by subject took 0.26 / 13 / 27 ms for recurrences and 6.4 / 17 / 26 ms for jobs, so both statements keep the
  materialized anti join. The planner's due
  selection takes 0.4 / 6.2 / 12 ms, hashing `job_pauses` against the unpaused due recurrences, its waiting selection
  0.015 ms and its next instant 0.03 ms; without the flag the due selection and next instant read past every paused
  overdue recurrence, 97 ms and 90 ms at 50 000. The claim due scan takes 0.07–0.12 ms.
- Calendars: `calendars` holds each calendar's newest revision with its canonical rule and digest, `calendar_dates`
  the newest revision of each date set with its coverage end, and `process_calendars` the calendars of registered
  processes, deleted with them. Outstanding jobs and recurrences reference their calendar by foreign key; the terminal
  transition moves a job's reference to `ended_calendar`, which has none, and a rerun moves it back, so the foreign key
  refuses a rerun after the calendar was removed and holds a removal off until a rerun commits. Removal locks the
  calendar row, which holds off new references, counts the outstanding jobs per definition and the recurrences, parses
  the other stored calendars for composition and deletes the row only when none uses it. Calendars and date sets are
  evaluated in .NET: each statement that needs them reads their revisions in the transaction, and a process cache keeps
  parsed rows by revision. A job with a calendar is stored with `calendar_at`, its requested instant, and no due time,
  so it stays out of `jobs_due`; the policies `calendar_blocked` and `calendar_closed` are copied from its schedule or
  recurrence. A planning pass locks with `FOR UPDATE SKIP LOCKED` up to 100 unheld jobs whose instant came, from
  `jobs_calendar_due (definition, calendar_at) WHERE calendar_at IS NOT NULL AND state = 0 AND NOT
  calendar_unavailable`, and up to 100 whose window closed, from `jobs_window (definition, window_end) WHERE window_end
  IS NOT NULL AND lease_expires_at IS NULL AND state = 0`, reads the clock after the locks, decides each and writes
  every decision in one `UPDATE ... FROM unnest(...)`: a due time with `window_end`, `skip_pending` with `skip_reason`
  for `FinalizeSkippedAsync`, or `calendar_unavailable` with the instant kept. Its next instant is the earlier of one
  probe of each index. Claims add `window_end IS NULL OR window_end > clock_timestamp() OR lease_expires_at IS NOT
  NULL` to their re-evaluation, so the due scans keep their ranges on `jobs_due`; `window_end` is `infinity` for work an
  operator made due ignoring the calendar. Requests with `Blocked.Reject` evaluate the calendar before writing.
  Publishing a newer date-set revision updates, in the publisher's transaction, every outstanding unheld job of the
  calendars using the set through `jobs_calendar (calendar, calendar_at) WHERE calendar IS NOT NULL`, which also serves
  the foreign key, and signals each definition's planner. The coverage report probes `jobs_calendar_unavailable
  (calendar) WHERE calendar_unavailable AND state = 0`, `jobs_calendar` and `job_recurrences_calendar (calendar,
  next_at) WHERE calendar IS NOT NULL` once per calendar with `EXISTS`.
- Calendar measurements, PostgreSQL 18.6 on a MacBook (Colima), the 1M jobs above with two of the ten definitions within
  one calendar, whose 23 880 due jobs per definition carry a window end ahead, 1 000 wait for their instant, 100 have
  an instant that came, 50 a closed window and 20 wait for dates: the claim due scan stays one range of `jobs_due` with
  no row filtered out, 0.55 ms for 32 jobs (0.59 ms on a definition without a calendar). The planner's selection of
  100 instants that came takes 0.81 ms and of the 50 closed windows 0.11 ms, each reading only the rows it locks; its
  next instant takes 0.16 ms. A request with a calendar, one new job and one merging, takes 0.42 ms, of which the
  foreign key check is 0.17 ms. Publishing a revision re-plans the calendar's 47 800 outstanding unheld jobs in one
  `UPDATE` of 0.67 s, reading 60 000 entries of `jobs_calendar` (the other 12 200 are held or wait for their instant);
  the cost is the rewrite of each planned job, in the publisher's transaction.
- Chains: steps are `jobs` rows with chain columns, outside the due indexes while they wait for their parents;
  `job_chains` and `job_chain_groups` hold each chain's top level and groups with one pending count and the worst
  outcome. The terminal transition decides a chain step's children and counts it in its group after the outcome hook;
  children whose condition does not hold are recorded with `skip_pending` and ended by their own definition's
  maintenance, which also ends an occurrence of a recurring chain with its chain's outcome. Layout, locks, retry and
  retention are in [chains](chains.md#storage-postgresql).
- Chain measurements, PostgreSQL 18.6 on a MacBook (Colima), the 1M jobs above plus 20 000 five-step chains with a
  two-branch group (half settled, half with waiting steps) and one chain whose group has 10 000 children: the claim
  due scan stays one range of `jobs_due`, 0.15 ms for 32 jobs. A five-step chain is one `INSERT` of 0.38 ms. A step's
  terminal transition adds deciding its children through `jobs_chain`, 0.17 ms, and counting itself in its group and
  in the chain by primary key, 0.10 ms and 0.06 ms; the 10 000-child group's continuation is decided in 0.06 ms. The
  finalization selection, now also over occurrences whose chain settled, is a `BitmapOr` of `jobs_skip` and
  `jobs_chain_owner`, 0.04 ms. A continuation reading the 10 000-child group's results takes 11 ms for 10 000 rows; a
  chain's steps 0.02 ms. Per-definition retention of 1 000 jobs takes 7.5 ms on `jobs_retention`; chain retention of
  1 000 chains with 5 000 steps 18 ms, reading only the rows it deletes. Joining the steps to the selection instead of
  collecting it into an array, the planner merged it with the whole of `jobs_chain` (109 888 entries), 40 ms.
  Cancelling a subject locks the waiting steps of its jobs' chains in the same statement, collected into an array
  and locked by primary key in the cancellation order: 0.31 ms for a subject with 40 outstanding jobs outside chains
  (0.13 ms when it locked only the subject's jobs from `jobs_subject`), and 0.13 ms for a waiting step's subject,
  locking its chain's four waiting steps through `jobs_chain`. Removing the recurrence of an occurrence that owns a running five-step chain locks the occurrence
  through `jobs_recurrence` and the chain's five steps through `jobs_chain` in one statement, 0.20 ms.
- Rejected designs for the pause flag race with resume (see "Pause flag set after a resume" in [decisions](decisions.md)),
  measured on PostgreSQL 18.6 with the 1M jobs above; the code is on the local branch `pause-race-experiment`. Both
  removed the race, and a racing probe of 480 request and schedule pairs against resumes, without maintenance, left 0
  set flags without a pause against 296–300 today. Neither should be retried without a new idea for the failure below.
  - Copying under a row lock: the copying statements read the pause row `FOR KEY SHARE`, so a resume's delete waits
    for open requests and its later update clears their rows. Request latency and throughput stayed within noise and
    a fan-out to 50 000 subjects with 1 000 paused cost 5–15% more, 21% with all paused. It failed on lock order: a
    resume now waits for every open application transaction that requested work for the subject; a schedule, which
    locks its recurrence before copying, deadlocks with a resume, which deletes the pause before updating the
    recurrence; a transaction resuming two subjects deadlocks with a fan-out requesting them in the other order, which
    sorting the fan-out cannot prevent since requests run in rounds and groups and callers make several calls per
    transaction; and a request after a domain write deadlocks with a resume followed by that write even when the
    request creates a new job.
  - Conservative flag: new jobs, recurrences, occurrences and reruns start unflagged and read no pause, only pausing
    and maintenance set the flag (locking the pause row `FOR KEY SHARE SKIP LOCKED`), resume deletes the pause before
    clearing, and maintenance clears nothing, so a set flag always has a pause behind it. No new locks in application
    transactions and no new deadlocks. It failed on cost: every row of a paused subject sits in the due indexes until
    flagged, and claims and planning read past it. Flagging inside the maintenance loop flagged 50 000 rows in about
    4 minutes, 100 per pass behind planning, finalization and a retire scan of about 240 ms; draining flags inside the
    pass would delay planning without bound under continuous arrivals. Flagging in a loop of its own beside the
    planner flagged a burst of 50 000 jobs in 17 s and 50 000 recurrences in 39 s, but under 100 active requests and
    100 active recurrences per second, sustained requests for paused subjects reached 576/s against 997/s today, the
    unflagged backlog grew to 28 300, active claim latency p50/p99 rose from 23/324 ms to 1 335/2 520 ms and stayed
    at 1 681/2 127 ms after the backlog drained, and WAL per burst doubled. Flagging where claims and planning meet
    paused rows was not tested: claims and planning exclude paused subjects in their candidate predicates and lock
    only eligible rows, so it needs new candidate selection and writes in both, and still pays the flag writes.
- Status, results, progress and run history: `jobs` records the start of a running attempt (`attempt_started_at`,
  set by a claim that runs the handler, cleared by every statement that ends the attempt), its progress, and with the
  outcome the ended execution id and the serialized result with its codec version. `GetManyAsync` is one statement
  over the subjects as arrays: per subject a limited lateral probe for the outstanding job (the running one, otherwise
  the earliest due, standalone job or occurrence) on the index over outstanding jobs by subject, the latest ended execution on `jobs_ended (definition, subject_kind, subject_id,
  ended_at) WHERE ended_at IS NOT NULL AND subject_kind IS NOT NULL`, the recurrence by its identity and the stored
  pause; as plain joins the planner hashed all recurrences and pauses for a large page. Run history entries are rows of
  `job_runs`, keyed by job and the epoch of the claim that used the attempt, inserted from the locked row by an
  `INSERT ... SELECT` joined to the stored definition's `run_history`, with `ON CONFLICT DO NOTHING`: in the terminal
  statement as a data-modifying CTE under the same condition as its update, in a retry and a release before their
  update (a release records only while its lease is valid, `Released` when graceful and `LeaseLost` when failed), in
  expiry settlement, and in the claim statement for an attempt still open, which its lease lost. Pages of
  run history read `job_runs_subject (definition, subject_kind, subject_id, started_at, job_id, epoch)` backwards from
  a row-value cursor bound in UTC. `WaitAsync` marks the job with `UPDATE ... SET waited = true WHERE ... AND state =
  0 AND NOT waited`, which waits for a transition holding the row and writes nothing when a committed mark is already
  there: at 8 clients a repeated mark took 0.12 ms with the predicate and 0.48 ms without, each one a row write; the terminal statement returns the mark and clears it unless a follow-up
  reuses the id, and the transaction then notifies `Ended`.
- Status, result and wait measurements, PostgreSQL 18.6, 1M jobs over 10 definitions with 25 000 subjects each (75%
  terminal, 6 250 held per definition), 20 000 recurrences and 1 000 paused subjects of one definition, and 2M run
  history entries over two definitions: a status page takes 6 ms for 500 subjects and 11 ms for 1 000 (12–16 ms
  cold), and one subject 0.07 ms; with the recurrences and pauses joined instead of probed, 9–17 ms with a full scan
  of both tables. The terminal statement with the run history CTE takes 0.34 ms with history enabled and 0.28 ms with
  it disabled, against 0.14 ms without the CTE; the difference is mostly planning the larger statement. `jobs_ended`
  adds 0.01 ms to it. A page of 50 entries of a subject takes 0.02–0.06 ms, a page of 1 000 0.2 ms from the start or
  after a cursor 3 000 entries in, and the entries of one job 0.015 ms. The waiter's reread takes 0.02 ms for one job,
  0.13–0.21 ms for 100 and 1.8 ms for 1 000. Any notification serializes the commits of notifying transactions: with 8
  clients, single-row terminal transactions commit at 16 300 per second without one and 4 040 with one, and a second
  notification in a transaction that already notifies costs nothing; with one client the difference is 0.02 ms. With
  0%, 1%, 10% and 100% of the ends waited for, they commit at 16 600, 16 700, 15 200 and 3 700–4 100 per second. The
  mark takes 0.34 ms with one client and 0.5 ms with 8.
- Monitoring measurements, PostgreSQL 18.6, 1M jobs over 10 definitions with 25 000 subjects each (75% terminal, 5 000
  held per definition) and 20 000 recurrences of one definition with `CompletesBy`: the terminal statement executes in
  0.044 ms with or without expectations, its join to `job_definitions` reading one buffer; the expectation statements
  of the ending transaction add 0.018 ms on a failure and 0.016 ms on a success, index probes only, and a resolution
  probes `work_incidents_open` with 26 000 incidents stored (9 buffers). End to end over a forwarded port an end takes
  1.5 ms without expectations and 1.9–2.0 ms with them. With 8 clients, ends with the failure statement commit 14 500
  per second on distinct keys against 19 400 without it, and 4 950 per second on one key, whose failures serialize on
  its state row. A deadline batch of 1 000 rows takes 19.4 ms and opens its 1 000 incidents. The job claim's plan is
  unchanged by `due_at` and `trace_parent` in its returned columns (294–295 buffers, 0.6–0.8 ms for 10 claims).
- Running attempts, PostgreSQL 18.6, 1M jobs over 10 definitions with `RunsAtMost` (75% terminal, 5 000 held per
  definition) and 1 000 recurring-chain occurrences of one: the loop's check reads only `jobs_owned_chain` as an index
  range bounded by `(SELECT clock_timestamp())`, 0.055 ms (8 buffers) to open nothing and 0.032 ms (3 buffers) for the
  next limit; a check that also reads held leases filters all 5 000 of them and all 1 000 occurrences with
  `clock_timestamp()` comparisons, 1.10 ms (1 090 buffers) and 0.94 ms (131 buffers). Under `auto_explain` a pass
  touches `jobs_owned_chain` alone. Tracking a held attempt's timer (the set entry, the timer and the token source,
  measured in isolation) costs 0.14–0.18 µs and a few hundred bytes, and no statement for an attempt that ends within
  its limit;
  the fenced check of a firing timer is one primary-key probe, 0.056 ms (7 buffers), 0.84 ms median end to end over a
  forwarded port.
- Expected-key tracking, PostgreSQL 18.6, 20 000 recurrences of one definition with `CompletesBy`: a steady pass runs
  9 statements in 0.33 ms (147 buffers) and scans 3 rows for the 3 it returns; reconciling every key on every pass
  instead took 18.5 ms (4 195 buffers, 318 000 rows scanned), and the due-deadline select scanned the definition's
  40 000 index entries until its clock bound became an initplan parameter. Two passes, one of them applying three
  queued changes (a pause, a removal, a new recurrence), scan 29 rows in 2.0 ms. A reconciliation of all 20 000 keys, once per
  revision or pause change, takes 330 ms over 96 statements, 72 ms of it the guarded update that records it. The
  queue insert shares the recurrence's update statement: scheduling costs 1.97 ms per recurrence in transactions of
  1 000 (1.96 without the queue) and 1.92 ms alone (1.89); pausing a subject takes 0.71 ms, resuming 1.18 ms and
  removing a recurrence 1.49 ms (median), with p90 at 0.89, 1.33 and 1.75 ms against 10.5, 39.7 and 50.2 ms while every
  pass reconciled all keys.
- Operator actions: each runs in the provider's own READ COMMITTED transaction. `RunNowAsync` reads the job's
  recurrence without a lock; for an occurrence of a stored recurrence it locks the recurrence and then its outstanding
  occurrences, the order of recurrence operations, and either evaluates the outstanding occurrence, reports one
  waiting to be recorded skipped as `Waiting` from a read of its state after those locks (they leave it out, and
  locking it could wait for a finalization's outcome hook), or triggers the recurrence when the occurrence has ended.
  Otherwise it locks the job alone. The job is evaluated in a statement after its lock, so validity and due time use a
  clock reading taken once the lock is held, and made due with `due_at = LEAST(due_at, clock_timestamp())`.
  `RerunAsync` locks the job, then reads its state, whether it is an occurrence (`recurrence_id`) and whether another
  standalone job holds its identity (`jobs_merge`) in a new statement. It then updates the row: state 0, due now, a new `execution_id`, attempts, failures and takeovers 0, not
  started, the cancellation request and progress cleared and the pause copied from `job_pauses`. The outcome,
  reason, end time, ended execution and result of the ended execution stay on the row, and `lease_epoch` continues.
- Retention: `job_definitions` stores `job_retention` and `run_history_retention`. A pass reads the stored definitions
  and runs two statements per definition, each in its own transaction: `DELETE ... USING (SELECT ... ORDER BY
  ended_at LIMIT n FOR UPDATE SKIP LOCKED)` over `jobs_retention (definition, ended_at) WHERE state <> 0 AND chain_id IS
  NULL`, which leaves chain steps to their chain's retention, bounded by
  `now()` minus the stored duration as a scalar subquery, keeping unconsumed occurrences and a recurrence's awaited
  job; and the same over `job_runs_retention
  (definition, ended_at)`, which covers entries with and without a subject. A job a rerun made outstanding again
  after the statement's snapshot fails the recheck of `state <> 0` when its lock is taken; a locked one is skipped.
  A third statement per pass deletes settled chains as units (see [chains](chains.md#storage-postgresql)).
- Operator action and retention measurements, PostgreSQL 18.6 on an AMD Ryzen 7 7700X, 1M jobs over 50 definitions
  (600 000 of one, 70% terminal ended over the last 30 days, 80% with a subject) and 2M run history entries ended over
  the last 60 days (20% without a subject), 7 and 30 days retention: a batch of 1 000 expired jobs takes 9.2–9.4 ms and
  of 1 000 entries 8.1–9.3 ms, reading only the deleted rows' index entries; a definition with nothing expired 0.04 ms.
  Without the retention indexes the same batches take 508 ms and 322 ms (a sequential scan and an external sort of every
  terminal job or entry of the definition), and 98 ms for a definition with nothing expired. The indexes take 27 MB
  (`jobs_retention`, 700 000 terminal jobs) and 77 MB (`job_runs_retention`, 2M entries). Draining 532 000 expired jobs
  and 1M expired entries with one transaction per statement took 8.8 s for 60 100 statements, most of them finding
  nothing in already drained definitions. A rerun's lock takes 0.02 ms, its evaluation 0.03 ms and its update
  0.12–0.14 ms; `RunNowAsync`'s read takes 0.02 ms, its lock 0.01 ms, its evaluation 0.02 ms and its update 0.05 ms.
- Process registry: a framework table `processes`, one row per instance keyed by its owner id, with its type, release,
  start, heartbeat and stop times and its own `stale_after`, so every reader judges its heartbeat alike; writes use
  `clock_timestamp()`. A process is live while `stopped_at IS NULL AND heartbeat_at + stale_after > now()`, evaluated
  once per report statement. `process_definitions (owner, definition)` holds the job definitions of a registered
  process, inserted from arrays with `ON CONFLICT DO NOTHING` and deleted with it by `ON DELETE CASCADE`. The unserved
  report is one statement: a loose scan of `jobs_due` finds the argument versions of each unpaused definition's unheld
  jobs with one index probe per version, joined with the versions of expired leases from `jobs_expiry`; a version has
  due work when a probe of that version finds an unheld job due now or an expired lease whose subject has no row in
  `job_pauses`. Definitions no live process runs with such a version, or with a recurrence its planner would act on
  (`job_recurrences_next`, `job_recurrences_waiting`), are reported, and so are versions of run definitions that no live
  runner accepts. Only for a definition no live process runs and that has neither does the report read its flagged
  due jobs and recurrences: the subjects from `jobs_paused` and `job_recurrences_paused`, ordered by subject and covering
  the due columns, `EXCEPT` the definition's subjects in `job_pauses`, a merge of two index-only scans. A flag that a
  request racing a resume left set therefore does not hide work that no maintenance repairs; a definition a live
  process runs has that process's maintenance repair it. The statement turns off JIT for itself with
  `set_config('jit', 'off', true)` in the same batch: its cost estimate charges every unrun definition with the
  flagged-row scans, past the default `jit_above_cost`, and compiling took longer than the report runs. Probing due jobs per definition without a version read past every future due job: the planner chose
  a sequential scan, 96 ms in the setting below. The undeclared report finds each code-defined recurrence by its unique
  identity and reads the definitions live processes declare once. A starting process deletes expired processes, up to
  1 000 per statement with `FOR UPDATE SKIP LOCKED`, so concurrent starts never wait for each other; the table holds
  about one week of starts, so it has no index besides its key.
- Process registry measurements, PostgreSQL 18.6 on an AMD Ryzen 7 7700X, 1M jobs over 10 definitions (70% terminal, 25%
  outstanding unheld of which half due, 5% held with 10 expired leases per definition, one definition with 5 000
  outstanding jobs of an unaccepted version, one with 1 000 paused subjects, one unrun definition with only future
  work), 100 000 recurrences of one definition (100 due), and 300 processes (100 live, 100 stale, 100 stopped)
  registering the 10 definitions each: the unserved report executes in 1.3 ms (1.9–2.0 ms per call from the client), the
  undeclared report in 0.48 ms (0.55 ms), the outdated report over 50 outdated registrations in 0.63 ms, a list of the
  live processes in 0.09 ms (0.3 ms), a heartbeat in 0.2 ms cold (0.08 ms per call), registering a process with 10
  definitions in 0.07 ms and 0.39 ms, and a retention statement with nothing to delete in 0.19 ms.
- Unserved report with flagged rows, PostgreSQL 18.6 on a MacBook (Colima), the registry dataset above plus one
  definition no live process runs with 1 000 / 50 000 / 100 000 paused subjects, 100 000 flagged due jobs over them and
  one flagged overdue recurrence per paused subject: on the registry dataset alone the report executes in 1.6 ms
  (1.5 ms without the flagged-row check; 2.7 ms per call from the client against 2.4 ms). When the definition has other
  due work the check does not run, 1.4–1.7 ms; when a flag is stale the scans stop at it, 1.4 / 6.3 / 12 ms, as without
  the check. The worst case, every flagged row's subject paused and no other due work, executes in 13 / 46 / 80 ms
  (65 ms per call from the client at 100 000), the cost of reading every flagged row once. Rejected forms, at 100 000:
  one `job_pauses` probe per flagged row under `EXISTS`, 545 ms (263 ms on the subject-ordered indexes); a hashed anti
  join over the flagged rows of every definition, 287 ms; `EXCEPT` on the `(definition, id)` indexes, sorting the
  subjects, 323 ms. With JIT left on, the estimate crossed `jit_above_cost` at 50 000 and the report took 63–110 ms.
- JIT measurements, PostgreSQL 18.6 on a MacBook (Colima), the 83 distinct statements the PostgreSQL tests issue, with
  the job and registry dataset above at 1M and 10M jobs plus a definition no live process runs with 100 000 paused
  subjects: cost estimates against `jit_above_cost` (100 000). Below it at both sizes: the unserved report (77 000 /
  61 000, JIT off for itself), clearing job flags (25 000 / 23 000), the status page of 1 000 subjects (34 000), a
  retention batch (19 000 / 21 000), setting job flags (9 600 / 34 000) and every other statement under 2 000. Two
  estimates grow with a backlog. A due claim pass that leaves out busy partitions holding most of a definition's due
  jobs reads past all of them: 44 000 / 510 000, and at 10M it took 150 ms with JIT (inlined and optimized) against
  61 ms without. A claim under a definition limit, whose `LIMIT` is an expression, is charged a fraction of the due
  range: 8 500 / 53 000. A `RequestManyAsync` fan-out costs about 8.5 per item at any table size and crosses at about
  12 000 items: 172 ms against 171 ms at 12 000, within noise at 50 000, 1 636 ms against 1 530 ms at 100 000. A
  `set_config('jit', 'off', true)` command added to the claim batch made no measurable difference.
- Clock: `clock_timestamp()` for every time.
- Signals ([signals](signals.md)): `NOTIFY` in the sending transaction, delivered on commit, on one channel per
  application, `corfedo_work:<schema>`, with the topic, a space and the key as payload. The channel name is at most
  63 bytes, since `pg_notify` refuses longer ones and `LISTEN` truncates them, so the schema is at most 50 bytes and a
  longer one is refused when the store is created. Statements that signal depending on the rows they match send from
  inside the statement through one payload builder. A dedicated session-level connection listens and never runs a
  transaction, so transaction-mode connection poolers need a direct connection for it. The listener probes a quiet
  connection with a query and reconnects when it fails; every subscriber then resyncs, since signals sent meanwhile
  are lost. Schedules, releases, cleared cancellations and settlements send `work.due`; on sources with
  store-enforced limits so do ends, which free a slot. The end of a job a caller waits for sends `work.ended` for its
  definition. A recurrence write that queues an expected key of a definition with `CompletesBy` sends `work.monitor`.
  A cancellation request on a held record sends `work.cancel` with the holder's id in the key, computed from the row,
  so only the holder's process renews its holds at once to observe it.
- Indexes a source needs, checked at startup: a unique index on the key fields; an index led by `DueAt` with the
  predicate `WHERE LeaseExpiresAt IS NULL`, since claims keep `DueAt` and held records would otherwise fill the front
  of a due-time index; an index led by `LeaseExpiresAt` (claims of expired leases and next due time); on sources that
  settle expiry or have store-enforced limits, the stored generated column `LeaseOutstanding`, `CASE WHEN SettledEpoch
  < LeaseEpoch THEN 1 END`, and an index led by it and `LeaseExpiresAt` with the predicate `WHERE LeaseExpiresAt IS NOT
  NULL`, so settlement and the next expiry do not pass expired leases already settled; and, where used, an index led
  by `LeaseOutstanding` and the partition field with the same predicate (partition and spread limits) and an index led
  by `LeaseOwner` (owner limit). The predicate is needed besides the column: a lease that ends clears `LeaseExpiresAt`
  without settling. The column is null once a lease is settled, so statements test it with `= 1 AND ... IS NOT NULL`,
  false rather than unknown under `NOT`. The framework's `jobs` table keeps indexes of its own over the epochs.
  Claim selection counts the
  outstanding leases of every partition once per statement from the partition index, without reading the table, and
  leaves out saturated partitions with a hashed `NOT IN`, keeping records that hold an outstanding lease of their own,
  since claiming them replaces it; its cost grows with outstanding leases, which the processes' capacity bounds, not
  with the table, and stays the same for many small or a few large partitions. Reconciliation sources also need an
  index led by `AttemptedGeneration` without a predicate, from which claims take records of
  an older generation in index order, over a range bounded by the current generation. Claim selection, expiry
  settlement and the next due time read each index over a range bounded by `now()`, which is stable and so usable as
  an index bound, and re-evaluate every locked candidate with `clock_timestamp()`. A claim pass returns its
  transaction's `now()` as its cutoff, and the next due time after a claim counts unheld records due after that cutoff
  and expired leases whose takeover backoff ends after it, besides the next lease to expire: what the claim's scans
  covered and did not claim is locked or held back, and a time already passed gives zero. Partial indexes are accepted except
  for the key. The statements rely on current planner statistics, which autovacuum's analyze keeps: on a table analyzed
  before its first leases, PostgreSQL 18.6 settled each expired lease from the partition index over outstanding leases
  (3.3 ms instead of 0.02 ms) until the table was analyzed again.
- Framework schema: numbered scripts in the provider's schema (`work` by default), applied in order under a
  session-level advisory lock and recorded in `schema_versions`. A baseline script, version 14, creates the current
  tables; later changes are scripts from version 15.

## In-memory

`Corfedo.Work.InMemory` is the provider for tests in one process (R20); it is never used in production. An
`InMemoryWorkDatabase` holds application tables, the framework state of every application that registers a store on
it with `AddInMemoryWorkStore(database, application)`, and the store's clock, a `TimeProvider` such as
`InMemoryClock`.

- Transactions: one store-wide async lock, held from `BeginAsync` until commit or rollback, so transactions are
  serializable by construction. A transaction's writes, the framework's and the application's, go to an overlay that
  commit applies; reads outside a transaction see committed records. Hooks roll back to a savepoint of the overlay.
  `InMemoryTransaction` is a `WorkTransaction`; domain test doubles insert, find, update and delete records of
  application tables through it, and `transaction.InMemory()` returns it from the handle.
- Tables: `CreateTable(location, key, fields)` declares an application table's key fields and fields; writes of
  other fields are refused. Startup verification requires the source's key fields in declared order and its lease,
  partition and reconciliation fields. Numeric fields never written read as 0.
- Jobs: each application's jobs are records of a table at `(application, "jobs")` with the PostgreSQL job columns, so
  job sources claim them as lease records under the same definition scope, eligibility and definition fence; an
  application table cannot use that location. Definitions, recurrences, pauses and run history are typed rows.
- Chains, calendars and monitoring: chains and their groups, calendars, date sets and expectation state are typed
  rows; a chain keeps its steps, which are job records, and its groups, so a step's settlement reads only its own
  chain. Incidents are records of a table at `(application, "incidents")` with the PostgreSQL incident columns, which
  the incident source claims; at most one is open per definition, subject, kind and date set.
- Claims evaluate the same conditions as the PostgreSQL statements, with one clock reading per transaction. Scans
  that order by due time, generation or expiry break ties by write order, as an index returns equal entries.
- Generation fences and limit scopes need no locks: no other transaction runs while one is open.
- Signals: delivered in process to the listeners of the application when the sending transaction commits, or at
  once without a transaction; a rollback, also to a savepoint taken before the send, discards them.
- Isolation: each application has its own generations, process registry and signals; application tables are shared,
  as an application schema is.

Scenarios that keep one transaction open while another runs, to show that claims, renewals, settlements or
maintenance skip or wait for a locked record, or that a row written concurrently with a pause or resume carries a stale
pause flag, need concurrent transactions, which this store does not have. The conformance provider
reports `ConcurrentTransactions` as false, and these scenarios skip:

| Scenario | Needs |
|---|---|
| `L1_ConcurrentClaimersClaimEachRecordOnceWithoutWaiting` | a record locked by an open transaction that claims skip |
| `L5_ExpiredLeaseCannotBeRenewed` | a renewal waiting for an open guarded transaction while a claim skips the record |
| `L6_GuardedTransactionOutlastsOriginalExpiry` | a renewal waiting for an open guarded transaction while a claim skips the record |
| `L7_RenewalWaitingOnALockEvaluatesExpiryAfterTheWait` | a renewal waiting on a record lock while a claim skips the record |
| `L21_SettlementPassesLockedAndFailingRecords` | a record locked by an open transaction that settlement skips |
| `L27_GuardedLeaseKeepsPartitionSlot` | due claims and claims by key while an open guarded transaction holds the partition's slot |
| `L29_DueClaimSkipsLimitScopesAnApplicationTransactionHolds` | due claims while an open application transaction holds a partition or owner scope |
| `L30_LockedAndLimitedRecordsTheClaimConsideredGiveNoImmediateNextDueTime` | a record locked by an open transaction that a due claim considers |
| `C15_InvalidationWaitsForAnOpenGuardAndFailsLaterGuards` | an invalidation waiting for an open guarded transaction, and a guard queued behind it |
| `C22_GuardQueuedBehindAnInvalidationHoldsNoRecordLock` | an invalidation waiting for an open fenced transaction, and a guard queued behind it |
| `ClaimsWaitForAPauseThatHasNotCommitted` | a request and claims while an open transaction pauses the subject |
| `ARequestCommittedBeforeAPauseCommitsStaysUnclaimableAndMaintenanceSetsItsFlag` | a request committed while an open transaction pauses the subject |
| `ARequestThatRacedAResumeIsClaimedWithoutFurtherRequests` | a resume committed while an open transaction requests a job of the subject |
| `AScheduleThatRacedAPauseIsFlaggedByMaintenanceAndWaits` | a pause committed while an open transaction schedules the subject's recurrence |
| `AScheduleCommittedLateAfterAResumeRunsWithoutFurtherRequests` | a resume and maintenance passes while an open transaction schedules the subject's recurrence |
| `MaintenanceSkipsLockedStaleFlagsWithoutRepeatingAndRepairsThemInALaterPass` | a resume while an open transaction writes the subject's rows, and maintenance skipping rows an open transaction locks |
| `J32_DueWorkWithAStalePauseFlagOfADefinitionNoLiveProcessRunsIsReportedUnserved` | a resume committed while an open transaction requests and schedules work of the subject |
| `J35_AJobWithAStalePauseFlagOfAVersionNoLiveProcessAcceptsIsReportedOnceTheRunnerRepairsTheFlag` | a resume committed while an open transaction requests a job of the subject |
| `J24_RetentionSkipsLockedJobsAndKeepsARerun` | retention skipping jobs an open transaction locks |
| `M1_ARecurrenceThatRacedASubjectPauseIsNotCheckedOnceMaintenanceRepairsItsFlag` | a pause committed while an open transaction schedules the subject's recurrence |

Scenarios of `ControlledClockScenarios` advance the store's clock through `IClockConformanceProvider`; the in-memory
provider runs them on an `InMemoryClock`, and providers whose clock follows real time, such as PostgreSQL, skip them.

## Further providers

Providers are added only when they can pass the suite. SQL Server is the second provider. MongoDB is used to
challenge the contract before it is frozen; its notes below are a starting point, not evidence of equivalent
semantics.

- **SQL Server:** investigated with proofs of concept; the translation of each guarantee, the measurements and the
  decisions are in [SQL Server provider](sqlserver.md).
- **MongoDB:** claims with conditional `findOneAndUpdate` per record, sorted by due time; partial unique indexes for
  merging; multi-document transactions, which need a replica set, for fan-out, guard, completion and hooks; the guard
  must write the document, because a read inside a transaction does not protect a later commit; change streams for
  wake-up. Store-enforced limits need counter documents updated in the claim, release, end, supersession and settlement
  transactions, so a crashed holder stops counting exactly when its lease is settled or taken over. The generation
  fence needs guards to write (not only read) the generation document, which serializes guarded transactions of a
  supersede source; whether that is acceptable is part of the challenge.
