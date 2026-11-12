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

## Testing against a real database

The PostgreSQL and SQL Server stores take every time from the database's clock, which a test cannot move. Read the
work state of your records, and move time for one record at a time.

```csharp
var leases = host.Services.GetRequiredService<LeaseOperations>();
var lease = await leases.ReadAsync(Sources.Mirrors, key, ct);   // null when there is no such record
Assert.Equal(1, lease!.Epoch);
Assert.True(lease.ExpiresAt > lease.ReadAt);                     // ReadAt is the database's clock

var reconcile = host.Services.GetRequiredService<ReconcileOperations>();
var record = await reconcile.ReadAsync(Sources.Placements, key, ct);
Assert.Equal(record!.RequestedRevision, record.ConvergedRevision);
```

- **Reads.** `ReadAsync` returns the record's lease fields, and on reconciliation sources its revisions, backoff floor
  and generations. `ReadManyAsync` reads up to 1,000 records with one clock reading. With a transaction, both see
  what it wrote before it commits. Compare times with `ReadAt`, not with the test's clock.
- **Ageing.** `RecordAgeing` moves every time of one record back by a span, as if the span had passed for it. Other
  records and your own columns keep their times.

```csharp
using Corfedo.Work.Testing;

var ageing = new RecordAgeing(host.Services.GetRequiredService<IWorkStore>());
await ageing.AgeAsync(Sources.Mirrors, key, lease.DueAt!.Value - lease.ReadAt, ct);  // due now
await ageing.AgeAsync(Sources.Mirrors, key, TimeSpan.FromMinutes(5), ct);            // a 5-minute lease expired
await ageing.AgeAsync(Sources.Mirrors, key, TimeSpan.FromHours(-1), ct);             // postponed by an hour
```

  An expired lease can be taken over once the source's takeover backoff has also passed, so age it by the lease
  duration plus that backoff. Ageing wakes the source's dispatchers when the record has a due time. Each call commits
  on its own, so call it outside the test's open transactions. It is for tests only: it writes the framework's fields
  outside the rules that keep them consistent.
- **Due times of lease sources.** `LeaseOperations.ScheduleAsync(transaction, source, key, Due.Now)` makes a record
  due without ageing it.

Tests against a real SQL Server need a database created with `READ_COMMITTED_SNAPSHOT ON`; the store refuses
others.

A provider of another database proves the same behaviour by running the `Corfedo.Work.Conformance` suite in its own
tests.
