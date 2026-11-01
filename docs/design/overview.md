# Overview

## Layers

```
 Chains          jobs that depend on each other: steps, groups, results, recurring chains
   │ are made of
 Jobs            library-owned job records: schedules, occurrences, merging, attempts, calendars
   │ are leasing on library-owned records
   │
   │   Reconciliation  revision requests and convergence on any record
   │     │ is leasing with
   ▼     ▼
 Leases          due time, fenced ownership, renewal, cancellation, takeover, expiry
   │ implemented by
 Store provider  native atomic operations per database (PostgreSQL first)
```

Beside them, every layer above and applications wake through signals:

```
 Signals         commit-time wake hints on topics, one listener per process
   │ carried by
 Store provider  the transport: NOTIFY on PostgreSQL, the transaction's commit in memory
```

- **Leases** work on any record that carries the lease fields. The domain owns the record and its state machine; the
  lease says when the record next needs a run, who holds it and at which epoch. See [leases](leases.md).
- **Reconciliation** adds revision fields: a request increments the requested revision, a run converges the record
  to it. See [reconciliation](reconcile.md).
- **Jobs** are leases on records the library owns, plus arguments, schedules, occurrences, attempts and terminal
  states. They keep each execution's identity across merged requests, which revision-derived reconciliation
  executions cannot. See [jobs](jobs.md) and [calendars](calendars.md).
- **Chains** connect jobs. See [chains](chains.md).
- **Monitoring** watches expectations on any of them. See [monitoring](monitoring.md).
- **Signals** wake processes when committed state they wait for may have changed: the layers above use them, and
  applications can use them alone for their own state. See [signals](signals.md).

Each layer can be used without the ones above it. The choice between them is a question of where the state belongs,
answered in [domain records and jobs](domain-and-jobs.md).

## Terms

| Term | Meaning |
|---|---|
| Record | One row or document that carries work state. |
| Source | A kind of record: where it is stored, how its key maps to fields, which layer's fields it carries. Defined in code. |
| Owner | The identity of one process instance, or of a remote holder. A new identity on every start. |
| Lease | The owner, epoch and expiry of a claimed record. |
| Epoch | Incremented on every claim of a record; the fence that rejects late writes from earlier holders. |
| Due | A record whose due time has come may run now; a record without a due time needs no run. |
| Partition | An optional key, such as a tenant, used for fairness limits and isolation. |
| Subject | The domain record a job acts on, as a kind and an identifier. |
| Definition | The shared, revisioned declaration of a job: identity, arguments, shared policies, hooks. |
| Registration | The local declaration that a process runs a source or definition: handler, capacity, scope binding. |
| Occurrence | One scheduled run of a recurring job, with its own attempts and outcome. |
| Execution id, attempt id | Identity of one deliberate execution, stable across its attempts and new for a rerun, follow-up, occurrence or retried chain step; and identity of one attempt. Used to deduplicate external effects. |

## Packages

| Package | Contents |
|---|---|
| `Corfedo.Work` | The layers, one folder each: contracts shared with providers (sources, keys, leases, transactions, stores); signals (the process's hub over a provider's transport); leases (operations, runner, due-record dispatcher); reconciliation; jobs (definitions, schedules, calendars, occurrences, chains, outcome hooks, status, expectations, incidents, monitoring); the process registry; and hosting (registration, startup, wake-up, shutdown and telemetry for `Microsoft.Extensions.Hosting`). |
| `Corfedo.Work.PostgreSql` | The PostgreSQL provider, over Npgsql. |
| `Corfedo.Work.EntityFrameworkCore` | Mapping of lease and revision fields on EF entities, a transaction adapter for `DbContext` for work and signals, and wake-ups when saved changes make records due. |
| `Corfedo.Work.InMemory` | A single-process provider for tests, with a controllable clock. |
| `Corfedo.Work.Conformance` | The provider conformance suite, runnable by any provider's tests. |

An application installs the provider for its database, which brings in `Corfedo.Work`. Further providers (SQL
Server is planned) are separate packages that pass the conformance suite. The EF package is
optional: the core and the PostgreSQL provider work with plain ADO.NET transactions. Packages target `net10.0` and
are trimming- and Native AOT-compatible, except the EF package: EF Core needs a compiled model under Native AOT and
reports trimming and AOT warnings of its own.

## Design rules

- **Semantics live in the core, atomic operations live in providers.** The provider contract is a set of operations,
  not a query abstraction.
- **The application owns its records.** The framework never subclasses, generates or migrates domain entities. It
  defines the fields a source must carry and verifies them at startup.
- **The library owns its tables.** Framework tables are created and upgraded by the provider's own versioned scripts,
  not by the application's ORM migrations.
- **Shared policy is data.** Anything processes must agree on is stored with a revision; local registrations hold only
  what may differ per process.
- **Closed generics only.** Every registration is a generic method whose type arguments are known at compile time;
  job arguments and results use the host's JSON options, whose reflection resolver exists only while reflection-based
  serialization is enabled, so a Native AOT application registers a source-generated context. No `MakeGenericType`, `Activator`, assembly scanning or attributes
  read at runtime.
- **No hidden infrastructure.** Everything runs against the one store the application already uses.
- **At least once, honestly.** Fencing protects database output; external effects are deduplicated by work identity.
