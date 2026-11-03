# Implementation

The order in which the design is built, and its status. Each stage ends with its conformance scenarios passing on
PostgreSQL; a stage that changes semantics updates the design and the scenarios in the same change.

| Stage | Contents | Scenarios | Status |
|---|---|---|---|
| 1. Repository | Solution, build properties, central package versions, AOT and trimming analyzers, check scripts, PostgreSQL test provisioning. | — | Done |
| 2. Leases | `Abstractions`, `Leases`, `PostgreSql` lease operations and schema scripts, the hosting needed to dispatch and shut down, the conformance suite skeleton. | L1–L30, W-1–W-3 | Done |
| 3. Reconciliation | `Reconcile`, generations, supersession, pools. | C-1–C-22 | Done |
| 4. Jobs | Definitions and revisions, schedules with Cronos, occurrences, merging, attempts, outcome hooks, status reads, operator actions, retention, the process registry and its job coverage reports. | J-1–J-40, D-1–D-9 | Done (the chain part of J-24 is stage 5) |
| 5. Calendars, chains, monitoring | Date sets, calendar planning, chains and groups, expectations and incidents, telemetry. | CAL-1–CAL-7, K-1–K-17, M-1–M-5, H-3 | Done |
| 6. Integration packages | `StartMode.Manual`, application isolation, `InMemory`, `EntityFrameworkCore`, Native AOT smoke application. | H-1, H-2, H-4–H-7 | Done |
| 7. Signals | The signals layer of [signals](signals.md): the signals layer, the PostgreSQL and in-memory transports, framework topics, the EF Core send. | S-1–S-8 (S-6, S-7 as PostgreSQL tests) | Done |
| 8. First public release | Package metadata, SourceLink and symbol packages, CI and release workflows, package readme, public documentation pass; the owner makes the repository public and publishes. | — | Done |
| 9. SQL Server provider | `Corfedo.Work.SqlServer` as designed in [SQL Server provider](sqlserver.md): schema, every store contract, the signals table transport, the EF Core adapter on SQL Server, the guides; the whole conformance suite on SQL Server 2022 and 2025. | All | Done |

The lease, reconciliation, job and process registry scenarios run as `Corfedo.Work.Conformance` tests against
PostgreSQL 18.6 in `tests/Corfedo.Work.PostgreSql.Tests`, and against the in-memory provider in
`tests/Corfedo.Work.InMemory.Tests`.

## Stage 4 steps

Each step adds its conformance scenarios first and shows them failing, then implements them with the design
documents updated in the same change. Before a step is done, its claim, merge and planning statements are measured
at 1M rows with `EXPLAIN ANALYZE` on PostgreSQL 18.6 and the numbers recorded in [providers](providers.md).
The AOT smoke application covers a job definition from 4a, a recurring job from 4b, a definition with a
result type and run history from 4c, the opted-in retention job from 4d, and from 4e a worker that describes itself in
the process registry and a host that only registers its process. Decisions behind the steps
are in [decisions](decisions.md) (job records, started executions and follow-ups, attempt accounting, newest
definition at claim time, recurrences, recurrence lock order, subject pause, missed occurrences, pools, job results,
status by subject, outcomes and waiting, progress, run history entries, stage 4 scope).

| Step | Contents | Framework tables | Status |
|---|---|---|---|
| 4a | Definitions stored by revision with the definition fence; job records as lease sources per definition; `RequestAsync` and `RequestManyAsync` with `Now`, `At`, `After`, `Debounce` in `Merge`, `IfAbsent` and `New`; follow-ups; attempts, exhaustion, `Late.SkipAfter`, timeouts; definition limit; version filters and retirement; `CancelAsync`, `CancelAllAsync`; outcome hooks on every terminal path; pool priority classes; `JobDisposition`. | `job_definitions`, `jobs` | Done |
| 4b | Recurrences with Cronos 0.13.0: `Every` (after completion, fixed rate), `Cron`; the planner; `Missed`, overlap, triggers; `ScheduleAsync`, `RemoveAsync`, `PauseAsync` and `ResumeAsync` for definitions and subjects; `AddRecurring` by revision; request routing against a recurrence's identity; copied pause flags on jobs and recurrences with periodic repair in both directions. | `job_recurrences`, `job_pauses`, occurrence and pause columns | Done |
| 4c | `GetAsync`, `GetManyAsync`; results with a codec, `GetOutcomeAsync`, `WaitAsync` signalled for waited jobs; progress; run history entries and `GetRunsAsync`. | `job_runs`, status, result, progress and wait columns | Done |
| 4d | `RunNowAsync`, `RerunAsync`, `RerunManyAsync`; retention of finished jobs and run history as a recurring job that worker deployments opt into with `AddRetention()`. | Retention durations, retention indexes | Done |
| 4e | Process registry as its own layer: process instances with type, release, heartbeat and stale window, listed live, stale or stopped, usable without other work; every starting process deletes records that stopped or became stale more than 7 days ago. Job definitions of registered processes with their own revision, accepted versions and recurrence declarations; `JobOperations` reports of unserved definitions and versions, undeclared recurrences and outdated processes. | `processes`, `process_definitions` | Done |
| 4f | Separate standalone jobs from recurrences: requests never route into recurrences; occurrences leave the standalone identity index and overlap is tracked through `recurrence_id`; no adoption by `ScheduleAsync` or at a job's end; no requested occurrences, folding or occurrence follow-ups (`after_job`); `TriggerAsync` under `Overlap.Wait` queues the waiting occurrence; identity fence, partition records and keys, request probes, `JobRequestConflictException` and the READ COMMITTED requirement of requests, completions and cancellations removed; schema step 9 (`adoption_ended_at`, never published) dropped. Conformance scenarios for the main behaviors (J-37, J-40), not edge cases. | `jobs` identity index | Done |

Standalone jobs and recurrences are separate lifecycles (see [decisions](decisions.md), "Standalone jobs and
recurrences", owner): occurrences have no merge key, so the outstanding-identity index holds standalone jobs
only, and a recurrence counts its own occurrences through `recurrence_id`.

## Stage 5 steps

Steps follow the stage 4 pattern: conformance scenarios first, the design documents updated in the same change,
claim, planning and request statements measured at 1M rows with `EXPLAIN ANALYZE` and recorded in
[providers](providers.md), and the AOT smoke application extended per step.

| Step | Contents | Framework tables | Status |
|---|---|---|---|
| 5a | Calendars (`WorkCalendar.Create`, time zone, weekdays, windows, annual, single and date-set exclusions, `Except`/`Also`), `AddCalendar` with cycle checks at startup, `Schedule.Within` with `Blocked` and `Closed`, published date sets with revisions and coverage, calendar planning in the definition's maintenance, the window end checked by claims, `JobActionOptions.IgnoreCalendar` on `RunNowAsync`, `TriggerAsync`, `RerunAsync`; removal blocked only by live work, the unregistered-calendar and date-set coverage reports. | `calendars`, `calendar_dates`, `process_calendars`, calendar columns on `jobs` and `job_recurrences` | Done |
| 5b | Chains and groups: steps, continuations, typed results between steps, dynamic groups with one pending count, recurring chains, retry from a step, deletion and retention of chains. | `job_chains`, `job_chain_groups`, chain columns on `jobs` | Done |
| 5c | Monitoring and telemetry: `JobPolicy.Expect` with `CompletesBy`, `RunsAtMost` and `FailsAtMostInARow` tracked per subject; expectation state written by the ending transaction; the `AddMonitoring()` loop for deadlines, recurring-chain occurrences and the two date-set incidents, recorded in the process registry with `GetUnmonitoredAsync` and woken by a signal when expected keys change; running-limit timers held by the attempt's process; incidents with one open per key, delivered to `IWorkIncidentHandler` through a framework lease source, read with `IncidentOperations.GetIncidentsAsync`, retained by the retention job; the `Corfedo.Work` activity source and meter; due time and trace id returned by job claims; exception descriptions without messages in framework logs with `work.Telemetry(...)` options. | `job_expectation_states`, `work_incidents`, expectation columns on `job_definitions`, `monitors` on `processes`, `trace_parent` on `jobs`, an index over `owned_chain` (schema 13); the expected-key change queue `job_expectation_changes` and `monitored_revision`/`monitored_paused` on `job_definitions` (schema 14) | Done |

Deadline and window arithmetic of `CompletesBy` runs as tests with explicit instants in `tests/Corfedo.Work.Jobs.Tests`,
as `CalendarEvaluatorTests` do; M-1 on PostgreSQL covers one day with a deadline seconds ahead of the store's clock in a
fixed-offset time zone. The 5c measurements are the ending transaction with and without expectations against its 0.28 ms
baseline, lock waits of a burst of failures of one key, the recurring-chain occurrence scan and the running-limit timers
at 5 000 held leases per definition, and expected-key tracking at 20 000 recurrences (see [providers](providers.md)).

CAL-1 and CAL-3 also run as evaluator tests in `tests/Corfedo.Work.Jobs.Tests` (`CalendarEvaluatorTests`): allowed
days across weeks, windows crossing midnight and daylight-saving changes need no store. The conformance scenarios scale
days to seconds with windows around the store's clock in a fixed-offset time zone where it is near noon.

## Stage 6 steps

Steps follow the stage 4 pattern. Application isolation comes before the in-memory provider, so that provider
implements the final store contract.

| Step | Contents | Framework tables | Status |
|---|---|---|---|
| 6a | `StartMode.Manual` and `IWorkHost.StartAsync`: the host's start applies the schema, stores definitions, calendars and recurrences and verifies sources; registering the process, dispatchers, the signal listener and background loops wait for `IWorkHost.StartAsync`. A host that is built and never started, as by build-time tools, touches nothing (H-7). | — | Done |
| 6b | Application isolation (R19, H-2) by a framework schema per application: conformance scenarios for two applications in one database with the same definition keys, calendar names and subjects across claims, reads, reports, monitoring, incidents and retention; providers register another application's store for the suite (`AddApplicationStore`). | — | Done |
| 6c | `Corfedo.Work.InMemory` with a controllable clock (R20, H-5, H-6), in the sub-steps below. | — | Done |
| 6d | `Corfedo.Work.EntityFrameworkCore`: the `DbContext` transaction adapter and the mapping of lease and revision fields, in the sub-steps below. | — | Done |
| 6e | Roles (H-1), Native AOT (H-4), stage close. | — | Done |

The in-memory provider serializes transactions behind one store-wide lock and is built in sub-steps toward the whole
store contract. Each sub-step runs its scenario classes in `tests/Corfedo.Work.InMemory.Tests`; scenarios that need
concurrent transactions are skipped, and listed in [providers](providers.md#in-memory).

| Sub-step | Contents | Status |
|---|---|---|
| 6c.1 | The package; `InMemoryWorkDatabase` with application tables and the framework state of each application; `InMemoryTransaction` for work operations and domain test doubles; `InMemoryClock`; in-process signals; the lease, reconciliation and process stores. The lease and reconciliation scenario classes run on it. | Done |
| 6c.2 | Jobs: definitions, requests, transitions, recurrences and planning, pauses, actions, status, results, history, retention, coverage; the process registry scenarios, whose workers run jobs. Requests and schedules with a calendar, chains and expectations are refused until 6c.3; the retention and telemetry scenario classes, which use expectations, run from 6c.3. | Done |
| 6c.3 | Chains, calendars and date sets with calendar planning, and monitoring with incidents claimed from an in-memory incident table, so that the whole conformance suite runs; scenarios that need two interleaving transactions skip, as `providers.md` lists. | Done |
| 6c.4 | Clock tests, a handler-and-chain test as the R20 example, a conformance scenario for a multi-day `CompletesBy` on the clock. | Done |

The EF Core package is an adapter over a store, not a store. Its tests run in `tests/Corfedo.Work.EntityFrameworkCore.Tests`
against PostgreSQL through Npgsql's EF Core provider.

| Sub-step | Contents | Status |
|---|---|---|
| 6d.1 | The implicit `DbTransaction` conversion to `WorkTransaction` and `DatabaseFacade.WorkTransaction()`: requests, guards and completions join a `DbContext`'s transaction and roll back with it (R1); a context without an active transaction is refused. | Done |
| 6d.2 | `HasLease()` and `HasReconciliation()` model helpers mapping lease and revision fields, tested on an EF entity used as a domain lease source. | Done |
| 6d.3 | Wake-up on `SaveChanges` for tracked changes of mapped `DueAt` properties (`AddWorkWakeUps`). | Done |
| 6d.4 | Trimming of the package's own code; the package does not support Native AOT, see [decisions](decisions.md) (H-4). | Done |

| Sub-step | Contents | Status |
|---|---|---|
| 6e.1 | The roles scenario (H-1): two processes on one store each run only the definitions they register, and requests wait for a role that has not started. | Done |
| 6e.2 | The AOT smoke application across all packages except the EF package, with a host that only uses signals (H-4). | Done |
| 6e.3 | Stage close. | Done |

The cron scenarios J-5 and J-34 involve no store: they run as tests of the occurrence planner against Cronos in
`tests/Corfedo.Work.Jobs.Tests`. Scenarios about daily jobs run on PostgreSQL with schedules of seconds.

| Scenario | Requirements | Design | Step |
|---|---|---|---|
| J-1 | R1 | [Common patterns](jobs.md#common-patterns) | 4a |
| J-2 | J1 | [Schedules](jobs.md#schedules) | 4a |
| J-3 | J1 | [Recurring jobs](jobs.md#recurring-jobs-and-occurrences) | 4b |
| J-4 | J1, J15 | Recurring jobs, missed rule | 4b |
| J-5 | J1 | [Cron semantics](jobs.md#cron-semantics) | 4b (planner tests) |
| J-6 | J2 | [Late and missed](jobs.md#late-and-missed-occurrences) | 4b |
| J-7 | J2 | Late and missed, attempts | 4a |
| J-8 | J3, J4, D1 | [Identity and merging](jobs.md#identity-and-merging) | 4a |
| J-9 | J3, R13 | Identity and merging | 4a |
| J-10 | J5 | [Attempts](jobs.md#attempts-and-terminal-states) | 4a |
| J-11 | J5 | Attempts | 4a |
| J-12 | J6, J14 | Recurring jobs | 4b |
| J-13 | J6, J14 | Recurring jobs | 4b |
| J-14 | J14, J15 | Recurring jobs, missed rule | 4b |
| J-15 | J14, J21 | Recurring jobs: triggering | 4b |
| J-16 | J15 | Recurring jobs: overlap | 4b |
| J-17 | J7 | [Cancellation and pause](jobs.md#cancellation-and-pause) | 4a (scheduled and running one-shot jobs); 4b (current occurrence) |
| J-18 | J8 | Cancellation and pause | 4b (definition and subject, one-shot and recurring) |
| J-19 | J9 | [Fan-out](jobs.md#fan-out) | 4a (measured at 50 000) |
| J-20 | J10, R18 | Revisions, [code-defined recurring jobs](jobs.md#code-defined-recurring-jobs) | 4a (equal revision, different content); 4b (recurrence follows the newest revision); 4e (outdated report) |
| J-21 | J10, R12 | Code-defined recurring jobs, [processes](hosting.md#processes) | 4e |
| J-22 | J11, D4 | [Arguments](jobs.md#arguments) | 4a |
| J-23 | J1 | Cron semantics | 4b |
| J-24 | J12 | [Retention](jobs.md#retention) | 4d (jobs, run history); 4e (stopped processes); stage 5 (chains) |
| J-25 | J13 | Identity and merging | 4a |
| J-26 | J17 | [Priority](jobs.md#priority), [pools](hosting.md#pools) | 4a |
| J-27 | J18 | Attempts: timeouts | 4a |
| J-28 | J19 | [Progress](jobs.md#progress-results-and-waiting) | 4c |
| J-29 | J20 | Progress, results and waiting | 4c |
| J-30 | J21 | [Operator actions](jobs.md#operator-actions) | 4d |
| J-31 | J22 | Definitions | 4a |
| J-32 | R10, R12 | [Registration](hosting.md#registration), processes | 4a (never runs); 4e (unserved report) |
| J-33 | D4 | [Outcome hooks](domain-and-jobs.md#outcome-hooks) | 4a |
| J-34 | J1 | Cron semantics | 4b (planner tests) |
| J-35 | R21, J11 | Executable compatibility | 4a (claim filter); 4e (unserved for the version) |
| J-36 | R17, J4, J21 | [Job and execution identity](jobs.md#job-identity-and-execution-identity) | 4a (follow-up); 4b (triggered occurrence); 4d (rerun) |
| J-37 | J3, J13, J14 | [Identity and merging](jobs.md#identity-and-merging) | 4f |
| J-38 | J8, J9 | [Cancellation and pause](jobs.md#cancellation-and-pause) | 4b |
| J-39 | J5, D4 | [Definitions](jobs.md#definitions-and-registrations) | 4c |
| J-40 | J3, J4, J15 | [Common patterns](jobs.md#common-patterns) | 4f |
| D-1 | D2 | [Subject lifecycle](domain-and-jobs.md#the-subjects-lifecycle-in-the-domain-transaction) | 4a |
| D-2 | D3 | [Completion with the domain output](domain-and-jobs.md#completion-with-the-domain-output) | 4a |
| D-3 | D4 | Outcome hooks, attempts | 4a |
| D-4 | D4 | Outcome hooks | 4a (`CompleteAsync`, returned result, cancelling request); 4b (skipped occurrence); 4d (operator action) |
| D-5 | D4 | Outcome hooks | 4a |
| D-6 | D5 | [Status by subject](domain-and-jobs.md#reading-status-by-subject) | 4c |
| D-7 | D6 | Subject lifecycle | 4b |
| D-8 | D7 | [Run history](domain-and-jobs.md#run-history) | 4c (entries); 4d (retention) |
| D-9 | R17 | [External effects](domain-and-jobs.md#external-effects) | 4a (attempts of one execution); 4d (rerun) |
| CAL-1 | J16 | [Applying a calendar](calendars.md#applying-a-calendar), [application-provided dates](calendars.md#application-provided-dates) | 5a |
| CAL-2 | J16 | Applying a calendar | 5a |
| CAL-3 | J16 | [Defining a calendar](calendars.md#defining-a-calendar) | 5a |
| CAL-4 | J16 | Applying a calendar: starting | 5a |
| CAL-5 | J16, J21 | Applying a calendar: retries, operator actions | 5a |
| CAL-6 | J16, M2 | Application-provided dates, [coverage report](calendars.md#coverage-report), [date-set incidents](monitoring.md#date-set-coverage) | 5a (report); 5c (incidents) |
| CAL-7 | J16, R12 | [Storage and removal](calendars.md#storage-and-removal) | 5a |
| M-1 | M1, M2 | [Completion deadlines](monitoring.md#completion-deadlines) | 5c |
| M-2 | M1, M2 | [Expectations](monitoring.md#expectations), [evaluation](monitoring.md#evaluation) | 5c |
| M-3 | M2 | [Delivery](monitoring.md#delivery) | 5c |
| M-4 | M3 | [Telemetry](monitoring.md#telemetry) | 5c |
| M-5 | M2, R12 | [Evaluation](monitoring.md#evaluation) | 5c |
| H-3 | R14 | [Data in telemetry](monitoring.md#data-in-telemetry-r14) | 5c |
| H-1 | R10 | [Registration](hosting.md#registration) | 6e |
| H-2 | R19 | Registration: application isolation | 6b |
| H-5 | R16 | [In-memory](providers.md#in-memory) | 6c |
| H-6 | R20 | [Testing](hosting.md#testing) | 6c.4 |
| H-7 | R10 | [Startup](hosting.md#startup) | 6a |

## Stage 7 steps

Built as designed in [signals](signals.md); 7a and 7b landed together, since the hub's `IWorkSignals` replaces the store's.

| Step | Contents | Status |
|---|---|---|
| 7a | `SignalTopic`, `WorkTopics`, `ISignalTransport` in the contracts; the signals layer with the hub (one listening loop, dispatch by topic, `Resync()` on subscription and reconnection, isolated callback failures, limits), tested against a test transport. | Done |
| 7b | The PostgreSQL transport and `PostgreSqlSignalSql`, every send site of the provider moved to it, the channel length check; the in-memory transport on `InMemoryTransaction`; `IWorkStore.Signals` returns the transport and registration adds the hub; the host, background loops and job waiter subscribe through it; the EF Core interceptor sends `work.due`; `WorkSignal`, `WorkSignalKind`, `PostgreSqlSignalPayload`, `IWorkBackground.Signal` and `ILeaseStore.SignalDueAsync` are removed. One change, so no step keeps both signal paths. | Done |
| 7c | Conformance scenarios S-1–S-5 and S-8, with savepoints in `IConformanceTransaction`; PostgreSQL provider tests S-6 and S-7. | Done |
| 7d | `AddPostgreSqlSignals`, and sends in a `DbContext` transaction, for applications without a store. | Done |
| 7e | Overview, providers and hosting describe the layer; the decision moves to Taken. | Done |

## Stage 8 steps

The first public version is 0.1.0, with every package in `src/`.

| Step | Contents | Status |
|---|---|---|
| 8a | Package metadata in `src/Directory.Build.props`: repository and project URLs, tags, SourceLink, `.snupkg` symbols, deterministic CI builds; exact versions between the packages; a package readme. | Done |
| 8b | `.github/workflows/ci.yml` (build, tests with Docker, AOT smoke) and `release.yml` (on a `v*` tag matching `Version.props`: build, test, pack, push to GitHub Packages and to nuget.org through Trusted Publishing, GitHub release). | Done |
| 8c | Public documentation pass: the README status, measurement notes that name internal hosts. | Done |
| 8d | The owner makes the repository public, adds the nuget.org Trusted Publishing policy, publishes `dev` and tags the release. | Done |

## Stage 9 steps

The provider passes the whole conformance suite before 0.2.0 is released. Each step adds its conformance tests to
`tests/Corfedo.Work.SqlServer.Tests` and keeps the existing suites green.

| Step | Contents | Status |
|---|---|---|
| 9a | Project, package metadata, `Microsoft.Data.SqlClient` and `Testcontainers.MsSql` pins; test provisioning (one container per test process, a database per test class, RCSI on); framework schema baseline with lock escalation disabled; startup checks (RCSI, source columns, indexes, lock escalation); the isolation check recorded on `WorkTransaction`. | Done |
| 9b | Leases: claims, claim by key, claims in the caller's transaction, renew, guard, release, end, schedule, cancel, expiry settlement, limits with scope applocks. | Done |
| 9c | Reconciliation: generations, the generation fence, supersession. | Done |
| 9d | Jobs: definitions and the definition fence, requests and merging, transitions, subject pauses, recurrences and planning, operator actions, status, run history, retention, the process registry. | Done |
| 9e | Calendars, chains, monitoring. | Done |
| 9f | Signals: the signals table transport, its listener and pruning. | Done |
| 9g | EF Core adapter on SQL Server; Native AOT and trimming of the provider; the outstanding-lease column on both providers; guides and README. | Done |
| 9h | Validation: the measurements of [SQL Server provider](sqlserver.md) repeated on the provider at 1M and 10M jobs, the open items there closed. | Done |

