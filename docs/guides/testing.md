# Testing

`Corfedo.Work.InMemory` runs the same semantics as the PostgreSQL and SQL Server stores in one process, without a
database, on a clock your test moves.

```sh
dotnet add package Corfedo.Work.InMemory
```

```csharp
var clock = new InMemoryClock(new DateTimeOffset(2027, 1, 4, 9, 0, 0, TimeSpan.Zero));
var database = new InMemoryWorkDatabase(clock);
var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
builder.Services.AddSingleton<TimeProvider>(clock);
builder.Services.AddInMemoryWorkStore(database);
builder.Services.AddWork(work => work.AddJob<SyncFeed, FeedSyncHandler>(Jobs.FeedSync));
using var host = builder.Build();
await host.StartAsync(ct);
var jobs = host.Services.GetRequiredService<JobOperations>();

using (var transaction = await database.BeginAsync(ct))
{
    await jobs.RequestAsync(transaction, Jobs.FeedSync, new SyncFeed("npm"), Schedule.After(TimeSpan.FromHours(1)), cancellationToken: ct);
    transaction.Commit();
}

clock.Advance(TimeSpan.FromHours(1));   // the job becomes due and runs
```

- **Transactions.** `database.BeginAsync()` returns a transaction that work operations take directly.
- **Your tables.** For lease and reconciliation sources, declare the table with `database.CreateTable(location,
  keyFields, fields)`, and insert and read rows through the database.
- **Waiting.** Background loops arm their timers when they get to them, so advance the clock in steps of the polling
  interval until the state you expect appears.

Tests against a real SQL Server need a database created with `READ_COMMITTED_SNAPSHOT ON`; the store refuses
others.

A provider of another database proves the same behaviour by running the `Corfedo.Work.Conformance` suite in its own
tests.
