# Getting started

Corfedo Work needs the .NET 10 SDK and a PostgreSQL or SQL Server 2022 or later database. Install the package for
your database:

```sh
dotnet add package Corfedo.Work.PostgreSql            # or Corfedo.Work.SqlServer
dotnet add package Corfedo.Work.EntityFrameworkCore   # optional, with EF Core
```

## Register the store and the work

```csharp
builder.Services.AddPostgreSqlWorkStore(dataSource);   // framework tables in the "work" schema
// or: builder.Services.AddSqlServerWorkStore(connectionString);
builder.Services.AddWork(work => work
    .AddJob<SyncFeed, FeedSyncHandler>(Jobs.FeedSync)
    .AddRecurring(Jobs.FeedSync, new SyncFeed("all"), Schedule.Cron("0 3 * * *", "Europe/Warsaw")));
```

At startup the host creates or upgrades its framework tables, stores definitions and schedules, and checks that your
own tables have the columns and indexes their sources need. A SQL Server database must have
`READ_COMMITTED_SNAPSHOT` on; see [production](production.md).

A process runs only the work it registers. A process that only requests a job, such as a web API, registers its
definition with `AddJobDefinition(Jobs.FeedSync)`, and the worker processes that register its handler run it.

## Request work in your transaction

Every request, cancellation and completion takes the transaction it belongs to. With EF Core:

```csharp
await using var transaction = await db.Database.BeginTransactionAsync(ct);
db.Feeds.Add(feed);
await db.SaveChangesAsync(ct);
await jobs.RequestAsync(db.Database.WorkTransaction(), Jobs.FeedSync, new SyncFeed(feed.Name), cancellationToken: ct);
await transaction.CommitAsync(ct);
```

`jobs` is `JobOperations` from dependency injection. If the transaction rolls back, the job was never requested. See
[transactions](transactions.md) for plain Npgsql or SqlClient and for the rules.

## Next

- [Jobs](jobs.md): definitions, policies, schedules and results.
- [Leases](leases.md): long work on rows you own.
- [Testing](testing.md): the in-memory store with a controllable clock.
- [Production](production.md): schema, replicas, deployments and telemetry.
