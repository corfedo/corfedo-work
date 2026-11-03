# Migrating from Quartz.NET

Quartz.NET separates jobs from triggers and stores both in its own tables. Corfedo Work keeps a job's schedule with
the job, stores it in your PostgreSQL database, and joins the transaction that requests it.

## What maps to what

| Quartz.NET | Corfedo Work |
|---|---|
| `IJob.Execute(IJobExecutionContext)` | `IJobHandler<TArgs>.RunAsync(args, run, ct)` |
| `JobKey` | The definition key, such as `"reports.nightly"` |
| `JobDataMap` | A typed argument record |
| `TriggerBuilder.StartAt(time)` | `Schedule.At(time)` |
| `WithSimpleSchedule(x => x.WithInterval(period).RepeatForever())` | `Schedule.Every(period, Interval.FixedRate)` |
| `WithCronSchedule("0 0 3 * * ?")` | `Schedule.Cron("0 3 * * *", "Europe/Warsaw")` |
| Misfire instructions | `RecurrenceOptions.Missed`: `Skip` (default) or `RunLatest` |
| `[DisallowConcurrentExecution]` | Recurring occurrences wait for the previous one (`Overlap.Wait`, the default); for requested jobs, `JobPolicy.DefinitionLimit = 1` |
| Calendars (`HolidayCalendar`, `DailyCalendar`) | `schedule.Within("business-days")`: see [calendars](../../design/calendars.md) |
| Clustered ADO.NET job store | Nothing to configure: every replica claims due jobs |
| `IJobListener`, `ITriggerListener` | Outcome hooks (`OnEnded<THook>()`) and OpenTelemetry signals |
| Durable jobs without triggers | `work.AddJobDefinition(...)`, requested when needed |

## Rewrite a job

```csharp
// Quartz.NET
public sealed class NightlyReport : IJob
{
    public async Task Execute(IJobExecutionContext context)
    {
        var region = context.MergedJobDataMap.GetString("region");
        // ...
    }
}
```

```csharp
// Corfedo Work
public sealed record ReportArgs(string Region);

public static class Jobs
{
    public static readonly JobDefinition<ReportArgs> NightlyReport = JobDefinition.Create<ReportArgs>("reports.nightly");
}

public sealed class NightlyReportHandler : IJobHandler<ReportArgs>
{
    public async ValueTask<JobDisposition> RunAsync(ReportArgs args, JobRun run, CancellationToken ct)
    {
        // ...
        return JobResult.Completed;
    }
}

builder.Services.AddWork(work => work
    .AddJob<ReportArgs, NightlyReportHandler>(Jobs.NightlyReport)
    .AddRecurring(Jobs.NightlyReport, new ReportArgs("eu"), Schedule.Cron("0 3 * * *", "Europe/Warsaw")));
```

## Cron expressions

- Quartz expressions start with a seconds field. Drop it for a five-field expression, or keep six fields; the seventh
  (year) field is not supported. Replace `?` with `*`.
- Quartz numbers the days of the week from 1 for Sunday; standard cron numbers them from 0 for Sunday. Use names
  (`MON-FRI`) to avoid the difference.
- Quartz uses the server's time zone unless a trigger sets one. Corfedo Work requires the time zone in every cron
  schedule.

## Differences to plan for

- **Transactions.** A request joins your transaction: if it rolls back, the job does not exist.
- **Retries.** Quartz leaves retries to the job. Corfedo Work retries a thrown exception after a backoff, three
  attempts by default (`JobPolicy.Attempts`, `Backoff`).
- **State between runs.** There is no `[PersistJobDataAfterExecution]`. Keep state in your own tables.
- **Database.** PostgreSQL and SQL Server 2022 or later are supported.

## Switching over

Move one job at a time: delete its Quartz trigger in the release that adds the job with `AddRecurring`, so only one
scheduler runs it. Remove Quartz and its tables once no triggers are left.
