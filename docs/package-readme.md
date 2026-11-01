# Corfedo Work

Background work for .NET that lives in your database and commits in your transactions: jobs and cron schedules,
leases on your own rows with fenced writes, reconciliation and commit-time wake-ups. A rollback leaves no work behind,
and a crashed worker's work is picked up again. There is no broker and no scheduler process. PostgreSQL is supported
today; SQL Server is planned.

Install the package for your database, which brings in `Corfedo.Work`:

```sh
dotnet add package Corfedo.Work.PostgreSql
```

```csharp
public sealed record WelcomeArgs(long UserId);

public static class Jobs
{
    public static readonly JobDefinition<WelcomeArgs> Welcome =
        JobDefinition.Create("users.welcome", 1, JobArgs.Json<WelcomeArgs>());
}

public sealed class SendWelcome : IJobHandler<WelcomeArgs>
{
    public async ValueTask<JobDisposition> RunAsync(WelcomeArgs args, JobRun run, CancellationToken ct)
    {
        // Send the email. A thrown exception retries the job after a backoff.
        return JobResult.Completed;
    }
}

// Program.cs
builder.Services.AddPostgreSqlWorkStore(dataSource);
builder.Services.AddWork(work => work.AddJob<WelcomeArgs, SendWelcome>(Jobs.Welcome));

// In the transaction that creates the user:
await jobs.RequestAsync(WorkTransaction.Of(transaction), Jobs.Welcome, new WelcomeArgs(42), Schedule.Now);
```

The [README](https://github.com/corfedo/corfedo-work#readme) has the full first example, and the
[guides](https://github.com/corfedo/corfedo-work/blob/main/docs/README.md#guides) cover jobs, leases, reconciliation,
signals, transactions, testing and production.

Corfedo Work is licensed under Apache-2.0. Corfedo names, logos and brand assets are reserved; see the NOTICE file.
