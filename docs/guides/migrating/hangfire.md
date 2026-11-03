# Migrating from Hangfire

Hangfire stores a method call and runs it later. Corfedo Work stores a typed request for a named job definition, in
your own PostgreSQL or SQL Server database and in the transaction that needs the work.

## What maps to what

| Hangfire | Corfedo Work |
|---|---|
| `BackgroundJob.Enqueue<T>(x => x.Send(id))` | `jobs.RequestAsync(Jobs.Welcome, new WelcomeArgs(id))`, or in your transaction `jobs.RequestAsync(tx, ...)` |
| `BackgroundJob.Schedule(..., delay)` | `Schedule.After(delay)` or `Schedule.At(time)` |
| `RecurringJob.AddOrUpdate("id", ..., Cron.Daily())` | `work.AddRecurring(Jobs.Cleanup, args, Schedule.Cron("0 0 * * *", "UTC"))` |
| `BackgroundJob.ContinueJobWith(parent, ...)` | A [chain](../../design/chains.md): `jobs.RequestChainAsync(tx, c => c.StartWith(...).Then(...))` |
| The method and its arguments | A `JobDefinition<TArgs>` with a stable key, and an `IJobHandler<TArgs>` |
| `[AutomaticRetry(Attempts = 10)]` | `JobPolicy.Attempts`, which counts every attempt (3 by default), with `Backoff` |
| `[DisableConcurrentExecution]` | `JobPolicy.DefinitionLimit = 1`, or `PartitionLimit` per tenant |
| `[Queue("critical")]` | `JobPolicy.Priority`, or processes that register different jobs |
| `IJobCancellationToken` | The handler's `CancellationToken`: cancellation, timeout, lease loss and shutdown |
| Dashboard | None. `jobs.GetAsync`, `GetOutcomeAsync` and OpenTelemetry signals |

## Rewrite a job

```csharp
// Hangfire
BackgroundJob.Enqueue<IEmailSender>(sender => sender.SendWelcome(userId));
```

```csharp
// Corfedo Work
public sealed record WelcomeArgs(long UserId);

public static class Jobs
{
    public static readonly JobDefinition<WelcomeArgs> Welcome = JobDefinition.Create<WelcomeArgs>("users.welcome");
}

public sealed class SendWelcome(IEmailSender sender) : IJobHandler<WelcomeArgs>
{
    public async ValueTask<JobDisposition> RunAsync(WelcomeArgs args, JobRun run, CancellationToken ct)
    {
        await sender.SendWelcome(args.UserId);
        return JobResult.Completed;
    }
}

await jobs.RequestAsync(Jobs.Welcome, new WelcomeArgs(userId), cancellationToken: ct);
```

Like `Enqueue`, this request commits on its own. Where the code that needs the job has a transaction, pass it first
instead: `jobs.RequestAsync(db.Database.WorkTransaction(), Jobs.Welcome, new WelcomeArgs(userId), cancellationToken: ct)`.
The job then exists only if that transaction commits.

Because the stored job names its definition key and not a CLR method, renaming or moving the handler does not break
jobs that are already queued.

## Differences to plan for

- **Transactions.** A request that takes your transaction joins it: if it rolls back, the job does not exist. Request
  jobs after the change that needs them, in the same transaction. A request without a transaction commits on its own,
  as Hangfire's do.
- **Merging.** Requests with the same subject or merge key merge into one outstanding job. Requests with neither
  create a job each, as in Hangfire.
- **Retries.** Attempts include the first run. Hangfire's default of 10 retries is `Attempts = 11`.
- **Time zones.** Every cron schedule names its time zone.
- **Database.** PostgreSQL and SQL Server 2022 or later are supported. The framework tables live in the `work` schema.

## Switching over

1. Add Corfedo Work next to Hangfire and move one job at a time: new requests go to Corfedo Work, and Hangfire
   finishes the jobs it already has.
2. Move a recurring job by removing it from Hangfire (`RecurringJob.RemoveIfExists`) in the release that adds it with
   `AddRecurring`.
3. Remove Hangfire once its queues are empty.
