<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/images/corfedo-logo-dark.svg">
    <img src="docs/images/corfedo-logo-light.svg" alt="Corfedo" width="280">
  </picture>
</p>

<h1 align="center">Corfedo Work</h1>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License: Apache-2.0"></a>
  <a href="https://dotnet.microsoft.com/"><img src="https://img.shields.io/badge/.NET-10-512BD4.svg" alt=".NET 10"></a>
  <a href="https://www.nuget.org/packages/Corfedo.Work"><img src="https://img.shields.io/nuget/v/Corfedo.Work.svg" alt="NuGet"></a>
  <a href="https://github.com/corfedo/corfedo-work/actions/workflows/ci.yml"><img src="https://github.com/corfedo/corfedo-work/actions/workflows/ci.yml/badge.svg" alt="Tests"></a>
</p>

**Background work for .NET that lives in your database and commits in your transactions.** You request work in the
same transaction as the change that needs it, so a rollback leaves nothing behind. There is no broker, no scheduler
process and no second database. PostgreSQL is supported today; SQL Server is planned.

## How it works

Every piece of work is a row with a lease, and every change wakes the processes that wait for it.

- **A row with a lease.** A worker claims a due row and holds it for a short lease, renewed while it runs. If the
  worker dies, the lease expires and another replica picks the row up. No cleanup job resets stuck work.
- **A fence on every write.** Each claim gets a new fence number. A worker that lost its lease can no longer commit,
  even if it keeps running.
- **A wake-up at commit.** When a transaction that makes work due commits, the processes that run it wake at once.
  Polling stays as a slow fallback.

Jobs, cron schedules, leases on your own tables and reconciliation are all built on these three ideas.

## A first job

```sh
dotnet add package Corfedo.Work.PostgreSql
```

**Define the job:** its arguments, a definition with a stable key, and a handler.

```csharp
public sealed record WelcomeArgs(long UserId);

public static class Jobs
{
    public static readonly JobDefinition<WelcomeArgs> Welcome = JobDefinition.Create<WelcomeArgs>("users.welcome");
}

public sealed class SendWelcome : IJobHandler<WelcomeArgs>
{
    public async ValueTask<JobDisposition> RunAsync(WelcomeArgs args, JobRun run, CancellationToken ct)
    {
        // Send the email. A thrown exception retries the job after a backoff, three attempts by default.
        return JobResult.Completed;
    }
}
```

Arguments are stored as JSON. A definition with a policy (attempts, backoff, limits) also takes a revision: increase it
whenever the policy changes, so replicas on different releases agree on one policy.

**Register the store and the job** in `Program.cs`. The store creates its tables in the `work` schema at startup.

```csharp
var dataSource = NpgsqlDataSource.Create(builder.Configuration.GetConnectionString("app")!);
builder.Services.AddPostgreSqlWorkStore(dataSource);
builder.Services.AddWork(work => work.AddJob<WelcomeArgs, SendWelcome>(Jobs.Welcome));
```

**Request it in the transaction that creates the user.** If the transaction rolls back, the job was never requested.

```csharp
await using var transaction = await connection.BeginTransactionAsync(ct);
// INSERT INTO users ...
await jobs.RequestAsync(transaction, Jobs.Welcome, new WelcomeArgs(42), cancellationToken: ct);
await transaction.CommitAsync(ct);
```

`jobs` is `JobOperations` from dependency injection. With EF Core, add `Corfedo.Work.EntityFrameworkCore` and pass
`db.Database.WorkTransaction()` instead; see [transactions](docs/guides/transactions.md).

## Is it for you?

It is a good fit if you have seen any of these:

- A job ran before the row it needed was committed, or a rollback left the job behind.
- A killed pod left a job "processing" until someone reset it.
- A worker whose lock had expired kept writing after another worker took over.
- Two replicas both ran the nightly cron job.
- One tenant's backlog starved every other tenant.
- A status column in your table drifted from the job framework's own state.

It is not a fit for everything:

- Work runs at least once. Fencing protects writes to the same database, but an email or an HTTP call cannot be
  taken back, so handlers deduplicate them with the run's stable execution id.
- It is not a message bus and not a workflow engine that replays code.
- It has no dashboard. Status, history and incidents are APIs and OpenTelemetry signals.

## What it does

| You want to | Use | Guide |
|---|---|---|
| Run jobs now, later or on a cron schedule, with retries, merging, chains and calendars | Jobs | [Jobs](docs/guides/jobs.md) |
| Run long work on a row you own, such as an import or an agent run, with crash recovery and fenced writes | Leases | [Leases](docs/guides/leases.md) |
| Bring a row in line with its latest settings, merging rapid changes into one run | Reconciliation | [Reconciliation](docs/guides/reconciliation.md) |
| Wake `BackgroundService`s in every replica when a transaction commits | Signals | [Signals](docs/guides/signals.md) |

Each part works alone: signals need no framework tables, and leases need no jobs.

A lease on your own table looks like this. The handler holds the row while it runs, and every write checks the fence:

```csharp
public static readonly LeaseSource<long> Imports = LeaseSource.Define("imports",
    StoreLocation.Table("public", "imports"), KeyCodec.Int64("id"),
    new LeasePolicy { Duration = TimeSpan.FromMinutes(2) });

public sealed class ImportRunner(AppDb db) : ILeaseHandler<long>
{
    public async ValueTask RunAsync(LeaseRun<long> run, CancellationToken ct)
    {
        await using var transaction = await db.Database.BeginTransactionAsync(ct);
        await run.GuardAsync(db.Database.WorkTransaction(), ct);   // throws if another worker took over
        // ... import and write the results ...
        await db.SaveChangesAsync(ct);
        await run.EndAsync(db.Database.WorkTransaction(), cancellationToken: ct);
        await transaction.CommitAsync(ct);
    }

    // Called when a row took its worker down LeasePolicy.MaximumTakeovers times, if the policy sets a limit.
    public ValueTask ExhaustedAsync(LeaseRun<long> run, CancellationToken ct) => ValueTask.CompletedTask;
}
```

## Packages

| Package | Contents |
|---|---|
| `Corfedo.Work.PostgreSql` | The PostgreSQL store. Install it, and it brings in `Corfedo.Work`. |
| `Corfedo.Work` | Jobs, leases, reconciliation, signals and the host integration. |
| `Corfedo.Work.EntityFrameworkCore` | Optional: `DbContext` transactions, lease columns on entities, wake-ups on save. |
| `Corfedo.Work.InMemory` | An in-memory store with a controllable clock, for tests. |
| `Corfedo.Work.Conformance` | The test suite a store provider must pass. |

The packages depend on each other's exact version, so update them together.

## Coming from another scheduler

Each guide maps the concepts and shows one job rewritten:

- [Hangfire](docs/guides/migrating/hangfire.md): `Enqueue`, `Schedule` and `RecurringJob` become typed job requests.
- [Quartz.NET](docs/guides/migrating/quartz.md): jobs, triggers and cron expressions, including the differences in cron
  syntax.
- [TickerQ](docs/guides/migrating/tickerq.md): ticker functions, time and cron tickers. TickerQ is source-available
  with paid tiers from its 5.x line; Corfedo Work is Apache-2.0 for everyone.

## Documentation

- [Guides](docs/README.md#guides): getting started, jobs, leases, reconciliation, signals, transactions, testing and
  production.
- [Design](docs/README.md#design): the guarantees, the decisions behind them and the measurements.

## Status and license

This is version 0.2.0, and the API may change before 1.0. The packages are trimming- and Native AOT-compatible, except
the EF Core adapter, since EF Core supports Native AOT only partly, and the conformance suite, which is a test library.
See [trimming and Native AOT](docs/guides/native-aot.md). Contributions are welcome through issues and pull requests;
see [CONTRIBUTING](CONTRIBUTING.md).

Corfedo Work is licensed under [Apache-2.0](LICENSE). See [NOTICE](NOTICE) for the reservation of Corfedo names,
logos and brand assets.
