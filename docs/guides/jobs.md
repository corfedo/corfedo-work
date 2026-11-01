# Jobs

A job runs a handler with arguments, now, later or on a schedule. The framework keeps it in its own job table, so you
need no table of your own. When the work belongs to a row you already have, use a [lease](leases.md) instead.

## Definitions

A definition has a key, a revision, the arguments' JSON contract and an optional policy:

```csharp
public sealed record SyncFeed(string Name);

public static class Jobs
{
    public static readonly JobDefinition<SyncFeed> FeedSync = JobDefinition.Create("feeds.sync", 1,
        JobArgs.Json<SyncFeed>(),
        new JobPolicy
        {
            Attempts = 5,
            Backoff = Backoff.Exponential(TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(30)),
            PartitionLimit = 2,                    // at most two running per tenant, across all replicas
            Timeout = TimeSpan.FromMinutes(20),
        });
}
```

- **The key** names the job in the database. Keep it stable.
- **The revision** is stored with the policy. Increase it whenever the policy changes: every replica then applies the
  newest stored policy, also during a rolling deployment. Startup fails if the same revision has a different policy.
- **The arguments** are stored as JSON. For a trimmed or Native AOT application, use a source-generated
  `JsonSerializerContext`: see [trimming and Native AOT](native-aot.md).

Without a policy, a job gets three attempts with an exponential backoff from 10 seconds to 10 minutes.

## Handlers

```csharp
public sealed class FeedSyncHandler : IJobHandler<SyncFeed>
{
    public async ValueTask<JobDisposition> RunAsync(SyncFeed args, JobRun run, CancellationToken ct)
    {
        // A thrown exception retries the attempt after the backoff.
        return JobResult.Completed;
    }
}
```

Return `JobResult.Retry(after, code)` to retry on your own terms or `JobResult.Fail(code)` to stop retrying. After the
last attempt the job ends as failed. The token fires when the job is cancelled, times out, loses its lease or the
process stops.

To write the outcome to your own tables in the same transaction, guard and complete inside it:

```csharp
await using var transaction = await db.Database.BeginTransactionAsync(ct);
var work = db.Database.WorkTransaction();
await run.GuardAsync(work, ct);
// ... write the results ...
await db.SaveChangesAsync(ct);
await run.CompleteAsync(work, JobResult.Completed, ct);
await transaction.CommitAsync(ct);
return run.Completed;
```

## Requests and schedules

```csharp
await jobs.RequestAsync(tx, Jobs.FeedSync, new SyncFeed("npm"), Schedule.After(TimeSpan.FromMinutes(5)),
    new JobRequestOptions { Partition = tenantId }, ct);
```

| Schedule | Runs |
|---|---|
| `Schedule.Now` | at once |
| `Schedule.At(time)`, `Schedule.After(delay)` | at a time, or after a delay |
| `Schedule.Debounce(delay, maximum)` | after the last of several requests, at most `maximum` after the first |
| `Schedule.Every(period, interval)` | at a fixed rate or delay |
| `Schedule.Cron(expression, timeZone)` | on a cron schedule in a time zone |

`AddRecurring` registers a recurring job in code. `Within(calendar)` limits any schedule to a
[calendar](../design/calendars.md) such as business days.

Requests with the same subject or merge key merge into one job instead of piling up. `JobRequestOptions.Mode` decides
how: merge, create only if absent, or always create a new job.

## More

- **Results and status.** Definitions created with a result type return a value; `jobs.GetAsync`, `GetOutcomeAsync`
  and `WaitAsync` read status and outcomes.
- **Chains.** Steps, parallel groups and fan-out with fan-in: see [chains](../design/chains.md).
- **Operator actions.** Pause, resume, cancel, run now and rerun, per definition or per subject.
- **Expectations.** "Completes by 06:00 on business days" opens an incident when it is missed: see
  [monitoring](../design/monitoring.md).

The full semantics are in the [jobs design](../design/jobs.md).
