# Migrating from TickerQ

From its 5.x line, TickerQ is source-available under the TickerQ Software License Agreement, with a free Community
tier below US$5M annual revenue and paid subscriptions above it; earlier releases keep their MIT and Apache-2.0
terms. Corfedo Work is Apache-2.0 for every user. Like TickerQ, it registers work without reflection, and its
packages, apart from the EF Core adapter, support Native AOT.

## What maps to what

| TickerQ | Corfedo Work |
|---|---|
| `[TickerFunction("Name")]` method | A `JobDefinition<TArgs>` with the key `"Name"`, and an `IJobHandler<TArgs>` |
| The function's request object | A typed argument record |
| `TickerFunctionContext` | `JobRun`: ids, attempt, progress and completion |
| `ITimeTickerManager.AddAsync(new TimeTickerEntity { Function, ExecutionTime })` | `jobs.RequestAsync(tx, Jobs.Name, args, Schedule.At(time))` |
| Cron functions and `SeedCronTickers` | `work.AddRecurring(Jobs.Name, args, Schedule.Cron("* * * * *", "UTC"))` |
| A ticker's retries and retry intervals | `JobPolicy.Attempts` and `Backoff`, shared by the definition |
| Job chaining | [Chains](../../design/chains.md): `jobs.RequestChainAsync(tx, c => c.StartWith(...).Then(...))` |
| EF Core operational store | Framework tables in the `work` schema, created at startup; no EF Core migration |
| Dashboard | None. `jobs.GetAsync`, `GetOutcomeAsync` and OpenTelemetry signals |

## Rewrite a job

```csharp
// TickerQ
public class MyJobs
{
    [TickerFunction("HelloWorld")]
    public async Task HelloWorld(TickerFunctionContext context, CancellationToken cancellationToken)
    {
        // ...
    }
}

await manager.AddAsync(new TimeTickerEntity { Function = "HelloWorld", ExecutionTime = DateTime.UtcNow.AddSeconds(10) });
```

```csharp
// Corfedo Work
public sealed record HelloArgs(string Name);

public static class Jobs
{
    public static readonly JobDefinition<HelloArgs> Hello =
        JobDefinition.Create("HelloWorld", 1, JobArgs.Json<HelloArgs>());
}

public sealed class HelloHandler : IJobHandler<HelloArgs>
{
    public async ValueTask<JobDisposition> RunAsync(HelloArgs args, JobRun run, CancellationToken ct)
    {
        // ...
        return JobResult.Completed;
    }
}

await jobs.RequestAsync(db.Database.WorkTransaction(), Jobs.Hello, new HelloArgs("world"),
    Schedule.After(TimeSpan.FromSeconds(10)), cancellationToken: ct);
```

For Native AOT, pass a source-generated `JsonSerializerContext` to `JobArgs.Json`: see
[trimming and Native AOT](../native-aot.md).

## Differences to plan for

- **Transactions.** A request joins your transaction, EF Core or plain Npgsql: if it rolls back, the job does not
  exist.
- **Policy per definition.** Attempts, backoff, timeouts and concurrency limits belong to the definition and are
  stored with its revision, so every replica applies the same policy.
- **Time zones.** Every cron schedule names its time zone.
- **Database.** PostgreSQL is supported today; SQL Server is planned. Redis and MongoDB are not supported.

## Switching over

Move one function at a time: new requests go to Corfedo Work, and TickerQ finishes the tickers it already has. Remove
a cron ticker in the release that adds the job with `AddRecurring`, so only one scheduler runs it.
