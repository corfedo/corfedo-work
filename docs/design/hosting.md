# Hosting

The hosting layer of `Corfedo.Work` runs registered handlers in a `Microsoft.Extensions.Hosting` process.

## Registration

```csharp
services.AddPostgreSqlWorkStore(dataSource, schema: "orders_work");   // the application's framework schema, R19
services.AddWork(work => work
    .Process(process => process.Type("orders-worker").Release(version))
    .AddJob<SyncFeed, FeedSyncHandler>(FeedSync, job => job.Capacity(2))
    .AddJobDefinition(Cleanup)                               // requested and cancelled here, run elsewhere
    .AddCalendar(BusinessDays)                               // J16, stored by revision at startup
    .AddRecurring(DailyReport, new ReportArgs(), Schedule.Cron("0 3 * * *", "UTC").Within("business-days"))
    .AddRecurringChain(DailyInvoices, new DailyArgs(), Schedule.Cron("0 7 * * *", "UTC"), chain => chain
        .StartWith(FetchOrders, new FetchOrdersArgs()).Then(GenerateInvoices, new InvoiceArgs()))   // K5
    .Retention(jobs: TimeSpan.FromDays(7), runHistory: TimeSpan.FromDays(30), incidents: TimeSpan.FromDays(90))   // J12 defaults
    .AddRetention()                                          // runs the retention job here
    .AddMonitoring()                                         // checks expectations of all definitions (M1)
    .OnIncident<InvoiceIncidents>()                          // delivers incidents here (M2)
    .Telemetry(telemetry => telemetry.Partition())           // R14 opt-ins
    .AddPool("convergence", capacity: 8)
    .AddReconciler<PlacementKey, PlacementReconciler>(Placements, r => r.Pool("convergence"))
    .AddLeaseHandler<InstanceKey, InstanceExecutor>(Instances, l => l.Capacity(16)));
```

A process runs exactly what it registers (R10). `AddCalendar(calendar)` registers a [calendar](calendars.md) that
schedules name with `Within`; `CalendarCoverage(margin)` sets how far ahead of need date sets should be covered.
`AddRecurringChain(definition, args, schedule, chain)` declares a [recurring chain](chains.md#recurring-chains) and runs
its occurrences; the definitions of its steps are registered with `AddJob` or `AddJobDefinition`.
`AddMonitoring()` runs the [monitoring loop](monitoring.md#monitoring-loop), which checks deadlines, recurring-chain
occurrences and date-set coverage for every definition in the store (attempts held by a lease are checked by their
holders); `OnIncident<T>()` delivers [incidents](monitoring.md#delivery) in this process; `Telemetry(...)` sets what [telemetry](monitoring.md#data-in-telemetry-r14) may carry beyond the
defaults. `AddRetention()` registers the framework's retention job (`WorkRetention.Definition`), a code-defined
recurrence every hour with its handler; `Retention(jobs, runHistory, incidents)` sets the job and run-history durations
stored for definitions whose policy leaves them unset, with or without `AddRetention()`, and with its optional third
argument how long the retention job keeps resolved incidents, 90 days by default; see [retention](jobs.md#retention). At least one worker deployment per shared store and application adds `AddRetention()`,
otherwise nothing is deleted; several replicas may, coordinating through the job's lease. Adding it twice is a startup
error, as for any definition registered twice. A process that only requests work registers definitions without
handlers and leaves retention out; it has no dispatcher, and unless it describes itself with `Process(...)`, declares a
code-defined recurrence, registers a calendar or monitors, it is not recorded in the [process registry](#processes). There is no scheduler role: due times are
data, and every process that registers a handler claims from its source.

The PostgreSQL store needs no server or connection settings of its own: the unserved report turns JIT off for its own
statement, and every other statement runs with the server's settings.

Applications sharing one database are isolated by their store (R19): on PostgreSQL each application has its own
framework schema, given to `AddPostgreSqlWorkStore`. Its tables, the keys of its advisory locks (definition fences,
subject pauses, generations) and its signal channel `corfedo_work:<schema>` all derive from the schema, so one
application never claims, reads, reports, monitors, delivers incidents of or retains another's work, and the same
definition keys, calendar names and subjects can mean different things in each (H-2). An application's processes
share its schema; reports and operations cover one application.

## Startup

At startup the host:

1. applies the provider's framework schema scripts, or verifies them when migrations are run separately;
2. stores definitions, calendars and code-defined recurrences by revision (R18);
3. verifies the fields and indexes of every registered domain source;
4. registers the process in the [process registry](#processes) when it is recorded, and starts dispatchers.

`work.StartMode(StartMode.Manual)` defers step 4 until the application calls `IWorkHost.StartAsync`, for hosts that
must finish their own initialization first; the host's start still does steps 1–3, so the application can request
work before it. Until then the process claims nothing, is not in the registry and runs no maintenance, planning or
monitoring. `IWorkHost.StartAsync` refuses to run before the host started, after it stopped, or in a host that starts
automatically, and a second call does nothing (H-7). Hosts built to generate build-time artifacts (for example API
documents) are never started, and building a host and resolving its services touches no store.

## Processes

The process registry records process instances. Each one has an owner id, new on every
start and the same id that holds its leases, a type shared by the replicas of one service (by default the host's
application name), an optional release, a heartbeat interval (15 s by default) and a stale window (60 s by default,
longer than the interval). A process records itself at startup, heartbeats while it runs and is marked stopped on
graceful shutdown, after its dispatchers have stopped. `ProcessRegistry.ListAsync(type, states)` lists instances as
live, stale or stopped, live ones by default, judged with the store's clock: a stale instance crashed, hangs or cannot
reach the store. A live instance has a recent heartbeat, which says nothing about readiness; the registry holds no
endpoints or metadata and does not route, probe health or elect leaders.

```csharp
services.AddPostgreSqlWorkStore(dataSource);
services.AddWorkProcess(process => process.Type("control-plane").Release(version));   // a service that runs no work
```

A process that adds work describes itself with `work.Process(...)` instead, and is recorded when it does, when it runs
a handler, when it declares a code-defined recurrence, when it registers a calendar or when it monitors, and its record
says whether it monitors; a process that only requests work is not recorded, so short-lived tools leave no records. A recorded process also records the job definitions it
registers: the revision of its own code (not the newer stored revision it may apply), whether it runs the handler and
the argument versions it then accepts, and whether it declares the definition's code-defined recurrence; and the
calendars it registers with the revision of its own code. A failed heartbeat is logged and
retried at the next interval; a heartbeat that finds the record deleted records the process and its definitions again
with the same owner id.

`JobOperations` reports from the live processes' definitions, as existence checks that change no record:

- `GetUnservedAsync`: definitions with due work that no live process runs (R12), and argument versions of due jobs
  that no live process running the definition accepts (R21). Due work is an unheld job due now, an expired lease, or a
  recurrence its planner would act on now; paused definitions and subjects, by the stored pauses, and versions below
  `MinimumArgsVersion` are left out. A copied pause flag that a request racing a resume left set does not hide the work
  of a definition no live process runs; for a definition a live process runs, that process's maintenance repairs the
  flag within a polling interval.
- `GetUndeclaredRecurrencesAsync`: code-defined recurrences that no live process declares, whatever revision it
  registers (J10).
- `GetOutdatedProcessesAsync`: live processes whose revision of a definition is older than the stored one (R18).
- `GetUnmonitoredAsync`: definitions with [expectations](monitoring.md#evaluation) while no live process monitors.

`CalendarOperations.GetUnregisteredCalendarsAsync` lists the stored calendars that no live process registers; see
[calendars](calendars.md#storage-and-removal).

Lease validity never depends on the registry; only lease expiry decides ownership. Every starting process, after
recording itself, deletes the records that stopped or became stale more than 7 days ago (`ProcessRegistry.Retention`,
J12), in batches of 1 000 until none is left; a failed deletion is logged and left to the next start. Records are only
added when a process starts, so the registry stays at about one week of starts without a schedule, and a record goes at
the first start after its 7 days.

## Dispatch

Every registered source has a capacity in the process. A dispatcher claims due records up to free capacity, in
priority then due order (J17), runs each handler in a fresh DI scope with its `LeaseHold`, and wakes on:

1. `work.due` signals for its sources, when the provider has signals; the host subscribes through the process's
   `IWorkSignals`, and every dispatcher claims again when the subscription resyncs;
2. the earliest due time it knows of among unclaimed records of its sources;
3. a polling interval, which keeps it correct without signals.

A claim of due records returns its cutoff, the store-clock instant that bounded its scans, also when it claims nothing.
The due time the dispatcher waits for comes from the store after that cutoff: records the claim considered and left
(locked, or held back by limits) are not counted, so they cause no repeated passes; a record that became claimable
after the cutoff counts with zero, and the dispatcher claims again at once. A pass that starts just before a record
becomes due, because a timer fired early or a signal or finished run started it, therefore never leaves that record to
the polling interval. A source whose claim failed asks for no due time; the pool retries after its error delay.

### Pools

Sources can share a pool with one capacity: `AddPool(name, capacity)` defines it and a registration joins it with
`Pool(name)` instead of setting a capacity of its own; joining an undefined pool, or setting both, is a startup
error. A source outside a pool is a pool of one with its own capacity.

Sources belong to priority classes: a job definition's stored `Priority`, and `Normal` for lease and reconciliation
sources. Each claim pass offers the free capacity to the highest class first, then what is left to the next class,
and so on (J17). Within a class the pass starts with the next source in turn and offers every source an equal share
of the free capacity before offering what is left to the sources that used their whole share, so within one class a
source with a large backlog cannot keep another's due records waiting for more than one rotation (C8). Lower classes
may starve while a higher class keeps claimable work; capacity a higher class does not claim, because its work is
held back by limits, paused or of versions this process does not accept, goes on to the lower classes. Claims never
preempt running work. A source whose claim fails is skipped for that pass. The pool honours each source's
store-enforced limits, which its claims apply, and wakes on any of its sources' signals and next due times.

### Job planners

Every process that runs a job definition's handler also runs its planner and maintenance pass, outside the pool's
capacity: it plans due occurrences of the definition's recurrences and consumes ended occurrences, plans jobs whose
[calendar](calendars.md#applying-a-calendar) instant has come and jobs whose allowed window closed before they
started, ends jobs the overlap policy or a calendar skipped through the outcome hook, ends jobs whose argument version
is below the stored `MinimumArgsVersion`, and repairs copied pause flags. It wakes on the definition's planning
signals, which ended occurrences, schedule changes, resumes, requests with a calendar, retries and operator actions
on jobs with a calendar and published date sets send, at the next planned instant, calendar instant or window end, and
at the polling interval. A process that registers a calendar also reads the date-set coverage report at the polling
interval and logs changes of its states. See [recurring jobs](jobs.md#recurring-jobs-and-occurrences).

## Scopes and context

A handler runs in a new DI scope. A registration can bind scope context from the record before the handler is
resolved, for example the partition as the current tenant:

```csharp
.AddJob<SyncFeed, FeedSyncHandler>(FeedSync, job => job.BindScope((services, run) => services.GetRequiredService<TenantScope>().Bind(run.Partition)))
```

## Shutdown

On shutdown, dispatchers stop claiming, running handlers see their tokens cancelled with the shutdown reason, and get
a grace period (R11). Records still held afterwards are released without counting a takeover or an attempt. The
process heartbeats until then and is marked stopped last.

## Telemetry

See [monitoring](monitoring.md#telemetry).

## Testing

`Corfedo.Work.InMemory` is a provider for tests in one process (R20). It runs the same core with an in-memory store,
an in-memory transaction type for domain test doubles, and a controllable clock, and passes every conformance
scenario that one process can observe. It is not a production provider: it has no durability and no cross-process
coordination, and it serializes transactions, so scenarios that keep one transaction open while another runs are
skipped ([providers](providers.md#in-memory)).

A test creates an `InMemoryWorkDatabase` on an `InMemoryClock`, registers the clock as the host's `TimeProvider` and
the store with `AddInMemoryWorkStore(database)`. Time moves only when the test calls `InMemoryClock.Advance`: timers,
delays, lease expiry and backoffs fire then, in due order, and the work they start runs after the call returns.

A background loop arms its next timer from the clock's time when it gets to it, which can be after `Advance` returned,
so a loop busy during a long jump waits one more polling interval. A test advances to the instant it expects
something at, then by the polling interval until the effect shows, and asserts times within that bound. A dispatcher
measures a claim's validity from before its claim query, so a claim in flight while the clock jumps further than the
lease finds the lease used up and does not run its handler: leases in such tests outlast the largest jump.
`FeedImportChainSample` in `tests/Corfedo.Work.InMemory.Tests/Samples` shows the pattern for a chain whose first step
retries with a backoff and whose handlers write domain records in the transactions that complete them.
