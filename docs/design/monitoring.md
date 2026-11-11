# Monitoring

The framework detects when work does not behave as expected and reports it to the application and to telemetry.
Notification channels (chat, mail, paging) belong to the application.

Requirements: M1–M3, R14 in [problems](problems.md).

## Expectations

Expectations are part of a definition's policy, so they are stored with its revision like attempts and retention, and
every process that declares the definition agrees on them:

```csharp
public static readonly JobDefinition<DailyArgs> DailyInvoices = JobDefinition.Create<DailyArgs>(
    key: "invoices.daily",
    revision: 4,
    policy: new JobPolicy
    {
        Expect = Expect.CompletesBy(new TimeOnly(8, 0), "Europe/Warsaw", calendar: "business-days")
            .RunsAtMost(TimeSpan.FromMinutes(30))
            .FailsAtMostInARow(3),
    });
```

`Expect` returns a `JobExpectation` record (`Deadline`, `RunningLimit`, `FailureLimit`) whose methods of the same
names add the others. A definition is refused when the time zone is unknown to `TimeZoneInfo`, the calendar name is
not valid as in `Schedule.Within`, `n` is negative or the duration not positive. The definition's digest includes the
expectations only when they are set, so a definition without them keeps its digest. Expectations apply to the definition's jobs, recurring jobs and recurring chains, and are tracked per subject:
a key is the definition with the subject of the job, or the definition alone for jobs without a subject.

| Expectation | Broken when | Resolved when |
|---|---|---|
| `CompletesBy(time, zone, calendar)` | An expected key has no successful completion in a window that ends at a deadline. | The key's next successful completion. |
| `RunsAtMost(duration)` | A running attempt started more than `duration` ago. | The key's next successful completion. |
| `FailsAtMostInARow(n)` | The key ends more than `n` times in a row as `Failed` or `TimedOut`. | The key's next successful completion. |

A successful completion is a job ending `Completed`; for a recurring chain, its occurrence ending with the chain's
outcome. `Failed` and `TimedOut` count as failures, `Completed` resets the count, and `Cancelled` and `Skipped`
neither count nor reset. Retries do not end the job and do not count. Running is the framework's one definition: a
valid lease, or an attempt its chain holds, so a recurring chain's occurrence runs for as long as its chain does.

### Completion deadlines

A deadline is `time` in `zone` on every day the expectation's calendar allows; a day is allowed when the calendar
allows any instant of it. The window of a deadline starts at the previous deadline, so a run that starts the evening
before and completes before 08:00 counts. When a date set the calendar uses does not cover a day, its deadline waits
until a revision covers it; the [date-set incidents](#date-set-coverage) report the gap.

The keys expected to complete are the definition's recurrences: the code-defined recurrence as the definition alone,
and each subject's recurrence created with `ScheduleAsync` as that subject. A definition with no recurrence is
expected as a whole, and then a completion of any subject counts. A key is first checked at the first deadline after
the monitoring loop sees it, with the window starting at the deadline before.

A completion that resolves a missed deadline belongs to that deadline: the next window starts at the completion, not
at the deadline. A job due at 07:00 that completes at 08:12 opens an incident at 08:00, resolves it at 08:12, and the
next day needs another completion between 08:12 and 08:00.

## Evaluation

Failures, resets and resolutions are recorded by the transaction that ends the job, so they hold with any number of
processes. An attempt held by a lease is checked against `RunsAtMost` by the process that holds it, which is the only
process running it. Deadlines, recurring-chain occurrences, date-set coverage and [unserved work](#unserved-work) are checked by the
monitoring loop of every process that opts in with `work.AddMonitoring()`. It checks the stored expectations of all definitions, also of definitions no live
process runs, so a total outage of a definition's workers is reported. Several processes may run it: every decision
and the incident it opens are one statement over locked rows, so each deadline is processed once. At least one
deployment per store adds it. `JobOperations.GetUnmonitoredAsync()` lists the definitions with expectations while no
live process monitors, from the [process registry](hosting.md#processes), which records whether a process monitors.

### State per key

`job_expectation_states` holds per key:

| Column | Meaning |
|---|---|
| `failures` | Failures in a row; written only when the definition has `FailsAtMostInARow`. |
| `deadline_at` | The next deadline the key must meet; infinity while the calendar's date sets do not cover it; null when the key is not expected to complete. |
| `window_start` | The start of that deadline's window. |
| `met_at` | The first completion in the window. |
| `first_after`, `last_after` | The first and the latest completion after `deadline_at` while the loop has not processed it. |
| `whole` | The row is the definition-alone key of a definition expected as a whole. |

Rows exist only for definitions with expectations. The transaction that ends a job of such a definition, after the
job's own update and in separate statements of one batch:

1. on a failure, increments `failures` (insert or update) and opens a `FailsAtMostInARow` incident when it exceeds
   `n`;
2. on a success, updates the key's row and, for a definition expected as a whole, its `whole` row, only when
   `failures > 0` (reset), when `met_at` is unset before `deadline_at` (sets it), or when `deadline_at` has passed
   (sets `first_after` if unset, and `last_after`); successes therefore write a row once per window, and no row of a
   busy definition becomes a point every completion locks;
3. on a success, resolves the key's open incidents; the statement runs after the row update, so it sees an incident
   the monitoring loop committed while the update waited for the row;
4. when a `CompletesBy` incident was resolved, starts the window at the completion's end and clears `met_at`: a
   completion that resolves a missed deadline belongs to that deadline.

Steps 2–4 cost an index probe when nothing is recorded. A row the transaction does change stays locked until it
commits, also in an application transaction that completes with `run.CompleteAsync(tx, ...)`, so failures of one key
serialize.

**Expected keys.** Which keys are expected depends on recurrences and their copied pause flags, which application
transactions change. Every transaction that creates, replaces or removes a recurrence, or changes its copied flag (a
subject pause or resume, and the maintenance repair of a flag), appends the recurrence's key to
`job_expectation_changes`. The queue is insert-only and no other statement locks its rows, so these transactions never
wait on monitoring and pauses keep not waiting for application transactions. When the definition has `CompletesBy`,
the same statement sends a store signal on commit, one per transaction, and the loop applies the queued keys at once
(see [Monitoring loop](#monitoring-loop)). A definition's revision and pause changes are not queued: `job_definitions`
records in `monitored_revision` and `monitored_paused` the revision and pause the loop last reconciled every key
against, and a difference starts a reconciliation.

### Monitoring loop

Each pass runs in short transactions, and statements that lock jobs or rows take them with `SKIP LOCKED` and leave
them to the next pass. It wakes at the next deadline, at the earliest instant a recurring-chain occurrence reaches its
running limit, on a
signal that expected keys changed (and when signal listening starts or resumes, since signals are lost meanwhile), and
at the polling interval. A removed or paused key's `CompletesBy` incident therefore resolves, and a new key is checked
at its first deadline, without waiting for the polling interval. The loop decides from explicit instants: deadlines are computed from the expectation's
calendar and stored on the rows, and the store's clock is only the current time.

**Deadlines.** The next deadline comes from the .NET calendar evaluator, so the rows whose `deadline_at` has passed
are processed in one short transaction per batch: a statement locks them `FOR UPDATE SKIP LOCKED`, the loop decides
each row deadline by deadline, and one statement stores the rows and inserts the incidents of missed deadlines:

| Recorded | Outcome | Next window |
|---|---|---|
| `met_at` | Met. | Starts at the deadline; `first_after` and `last_after` carry over. |
| `first_after` only | Missed and completed late: an incident opened at the deadline and resolved at `first_after`. | Starts at `first_after`; `last_after` carries over if it is later. |
| Nothing | Missed: an incident opened at the deadline, resolved by the key's next success. | Starts at the deadline. |

Carried-over completions are placed against the next deadline: one before it becomes `met_at`, later ones stay as
completions after it. The same statement moves `deadline_at` to the next deadline, so a replica that reads the row
afterwards finds it no longer due. A row whose next deadline has also passed is processed again in the same pass, deadline by deadline; while no
process monitored for longer than one deadline, completions between `first_after` and `last_after` are not
distinguished, so a deadline among them can report an incident already resolved.

**Expected keys.** Each pass deletes the definition's queued key changes and, in the same transaction and a later
statement, so it sees every change committed before the deletion, reads per key whether an unpaused recurrence exists
and locks its row without waiting. An expected key without a deadline starts the window of the first deadline after
the pass; a key no longer expected has its `deadline_at` cleared and its open `CompletesBy` incident resolved; a key
whose row another transaction holds is queued again. The definition-alone row of a definition without recurrences is
tracked as `whole` in one statement per pass; once the definition has recurrences, it keeps its window only while a
recurrence without subject is unpaused. A steady pass therefore reads the queued keys, not every key.

**Revisions and pauses.** While a definition's revision or pause differs from `monitored_revision` and
`monitored_paused`, each pass reconciles every key: it resolves the incidents of expectations the revision removed
(and resets failures without `FailsAtMostInARow`), clears the deadlines of keys not expected (all of them while the
definition is paused or has no `CompletesBy`), starts windows for expected keys without one, and moves deadlines an
older revision computed to the next one after their window start, placing recorded completions against it. Then one
update records the revision and pause, guarded by reads without locks of what is left; a row another transaction held
keeps its committed state, so the guard finds it and a later pass of any process continues. A paused definition and
paused recurrences are not expected, and resuming starts with the next deadline.

**Running attempts.** An attempt held by a lease is checked by the process holding it: when its handler starts, the
definition's running-limit timers track it and, while the newest stored revision the process knows has `RunsAtMost`,
arm a timer for the limit from the start, so the check costs no statement for an attempt that ends in time. A newer
revision re-arms every tracked attempt with its limit; the process learns of it from its next claim or maintenance pass,
which reads the stored revision, so a lowered limit applies within a polling interval. The tracking ends when the
attempt's outcome is applied. A firing timer runs one statement fenced to the lease: it locks the job in shared mode,
waiting, while the lease is held by its epoch and the attempt is open, compares `attempt_started_at` with the limit of
the stored revision and, once reached, opens the `RunsAtMost` incident naming the job (`ON CONFLICT DO NOTHING` on the
open-incident index). A completion committing meanwhile ends the attempt first, so its resolution never misses the
incident. A limit raised since the timer was armed re-arms it with the time left by the store's clock; a removed
limit or a lost lease ends the checks. Processes that only declare a definition run no attempts and arm nothing.

The monitoring loop checks the recurring-chain occurrences, whose attempts their chains hold without a lease: for each
definition with `RunsAtMost`, a range of `jobs_owned_chain` (`definition`, `attempt_started_at`) bounded by one clock
reading, `(SELECT clock_timestamp())`, finds those started more than the limit ago, locked `FOR SHARE SKIP LOCKED`, and
opens an incident per key naming the earliest; the rest of the range gives the instant the next one reaches the limit,
at which the loop wakes. A pass reads no held leases. No index covers `attempt_started_at` of held jobs, which every
claim writes.

### Date-set coverage

Every date set has two built-in expectations, checked by the monitoring loop at the polling interval from the
[coverage report](calendars.md#coverage-report):

| Incident | Open while the report's state is |
|---|---|
| `DateSetRunningOut` | running out or work waiting |
| `DateSetWorkWaiting` | work waiting |

Work waits for a set only when its coverage ends before an instant planning needs, so work waiting implies running
out, and work starting to wait opens a second incident while the first stays open: the handler sees the escalation. The loop resolves them when their condition no longer holds.
Processes that register calendars keep logging the report's state changes.

### Unserved work

Due work that no process can run is reported without any expectation (R12, R21), checked by the monitoring loop at the
polling interval from the [unserved report](hosting.md#processes) with the host's grace, `work.UnservedGrace(grace)`,
5 minutes by default:

| Incident | Open while |
|---|---|
| `Unserved` | The definition has work that was due `grace` ago, and no process that runs the definition and accepts the work's argument version was live at any instant since then. |

One incident per definition, whatever the argument versions; the report names them. A process counts while it was
live at some instant within the grace, so a restart or a rolling deployment that replaces its workers within the grace,
or a new release whose requests wait for its workers that long, opens nothing. A definition that is paused, or whose
waiting work is paused, is not unserved, and pausing it resolves the incident at the next pass. The condition clears
when processes serve the work or the work ends, and the incident resolves at the next pass. Like the date-set
incidents, several monitoring processes may open and resolve it; one open incident per definition is kept by the
unique index.

## Incidents

An incident records its kind, the definition and subject or the date set, the job that broke the expectation when
there is one, and when it opened and resolved. A key has at most one open incident of each kind: a deadline missed
again while its incident is open adds nothing.

```csharp
public sealed record WorkIncident(
    Guid Id, WorkIncidentKind Kind, string? Definition, Subject? Subject, string? DateSet, JobId? Job,
    DateTimeOffset OpenedAt, DateTimeOffset? ResolvedAt);

public enum WorkIncidentKind { CompletesBy, RunsAtMost, FailsAtMostInARow, DateSetRunningOut, DateSetWorkWaiting, Unserved }
```

A key's success resolves only the incidents of its expectations (`CompletesBy`, `RunsAtMost`, `FailsAtMostInARow`);
date-set and unserved incidents resolve when the monitoring loop finds their condition cleared.

`IncidentOperations.GetIncidentsAsync(openOnly, definition, limit, after)` returns incidents newest first by opening
time, with a keyset cursor on opening time and id; default limit 50, at most 1 000, optionally of one definition.

### Delivery

```csharp
work.OnIncident<InvoiceIncidents>();

public sealed class InvoiceIncidents : IWorkIncidentHandler
{
    public ValueTask OpenedAsync(WorkIncident incident, CancellationToken cancellationToken) { ... }
    public ValueTask ResolvedAsync(WorkIncident incident, CancellationToken cancellationToken) { ... }
}
```

The incident row is a lease source of the framework, `work:incidents` (`IncidentSource`), with leases of 30 seconds
and a failure backoff from one second to five minutes. It is due while one of its events is undelivered, and every
process that registers a handler claims from it, one incident at a time by default. A claim delivers `OpenedAsync`
before `ResolvedAsync`, also for an incident resolved before its opening was delivered, and records each delivery in a
commit fenced to its lease. Its last commit decides in one update, with the row locked, whether a resolution still
waits for delivery and leaves the row due if so; resolving an incident makes it due whether or not it is held. A crash
during delivery lets the lease expire, and another claim delivers the event again (at least once). A handler that
throws is released as failed and retried after a backoff of at most a few minutes, never given up. Opening an
incident, and a resolution that resolved one, signal the source; a success that resolves nothing sends no
notification, since notifications serialize committing transactions.

### Storage and retention

`work_incidents` has one row per incident with the lease fields and the delivery times of both events. A partial
unique index over `(definition, subject, kind, date_set)` with `NULLS NOT DISTINCT` on open incidents keeps one open
incident per key and kind, and serves the probe of a success by its leading columns. An `Unserved` incident is keyed
by its definition alone, so it needs no column of its own and the index stays as earlier releases infer it in their
`ON CONFLICT` clauses, which a rolling deployment runs against the migrated schema.

Resolved incidents are deleted 90 days after they resolved by the [retention job](jobs.md#retention), set with
`work.Retention(jobs, runHistory, incidents)`. It also deletes expectation rows of keys no longer expected
(`deadline_at` null) with no failures and no open incident, and queued key changes of definitions without
`CompletesBy`, which the monitoring loop deletes while a process monitors.

## Telemetry

An `ActivitySource` and a `Meter` named `Corfedo.Work`, `System.Diagnostics` types of the runtime; no OpenTelemetry package (M3).

**Spans.** `corfedo.work.claim` per claim pass, `corfedo.work.run` per run of a claimed record,
`corfedo.work.guard` per guard of a run and `corfedo.work.complete` per application of a job's outcome, by the
dispatcher or in the handler's transaction. A run span starts a trace of its own and links to the trace of the request
that created the job: requests store `Activity.Current`'s W3C id as `trace_parent` on the job, chain steps keep the
activity current when their chain was built (the requester's, or the occurrence's run for a recurring chain), and
occurrences have none. Run spans carry the source, the execution, and for jobs the definition, job, attempt and the
attempt's outcome. A failed span carries status error, the reason code and `error.type`: the exception's type, or the
reason code for a result without an exception, so reason codes are stable identifiers such as `bad_input`, never
messages. An attempt that fails, times out or is retried is a failed run span.

**Counters.**

| Instrument | Unit | Tags |
|---|---|---|
| `corfedo.work.claims` | `{claim}` | source |
| `corfedo.work.takeovers` | `{takeover}` | source |
| `corfedo.work.leases_lost` | `{lease}` | source |
| `corfedo.work.supersessions` | `{supersession}` | source |
| `corfedo.work.job.outcomes` | `{job}` | definition, outcome, reason |
| `corfedo.work.occurrences.skipped` | `{occurrence}` | definition, reason |
| `corfedo.work.incidents` | `{incident}` | kind, definition, event (`opened`, `resolved`) |

**Histograms**, in seconds: `corfedo.work.job.duration` (definition, outcome) per handler attempt, with the
outcome the attempt asked for, and `corfedo.work.job.lateness` (definition) for claims that run a job when due, not by
takeover: the process's clock when the claim returns minus the due time, which job claims return, and zero when clock
skew makes it negative.

Job outcomes and occurrences skipped are counted by the outcome callback every terminal transition runs, and incidents
by the transaction that opened or resolved them. Counts made inside a transaction are made again when it rolls back and
is applied again, for example after a failing outcome hook.

**Gauges.** `corfedo.work.running` per registered source, process-local, so replicas add up.
`corfedo.work.due.age` per job definition, the age of the oldest due job from one index probe, and
`corfedo.work.incidents.open` per kind, both store-wide and reported only by monitoring processes. The running gauge
reads the dispatchers' counts; the store-wide values are read by each monitoring pass while a listener enables either
instrument, and the gauge callbacks report the values of the last pass.

Tags are `corfedo.work.source`, `corfedo.work.definition`, `corfedo.work.outcome`, `corfedo.work.reason`,
`corfedo.work.kind` and `corfedo.work.event`; outcome and kind values are lower snake case (`completed`, `timed_out`,
`retried` for an attempt, `fails_at_most_in_a_row`). Reason codes are chosen by the application (`JobResult.Fail(code)`), so
their number is the tag's cardinality.

**Logs.** Handler logs keep their own pipeline. The framework adds a logging scope with `corfedo.work.source`,
`corfedo.work.definition`, `corfedo.work.job`, `corfedo.work.execution` and `corfedo.work.attempt`, so logs correlate
with spans.

### Data in telemetry (R14)

Arguments, results, subject identifiers and exception messages are not exported. Framework logs describe an exception
by its type chain and stack frames without messages, also for provider exceptions, whose details can contain key
values; spans record `error.type` only. Options extend this:

```csharp
work.Telemetry(telemetry => telemetry.Partition().ExceptionDetails());
```

`Partition()` adds `corfedo.work.partition` to spans and metrics of records with a partition; `ExceptionDetails()`
passes the exception itself to framework logs, whose message then names only its type. The options belong to the
host, so hosts in one process keep their own.
