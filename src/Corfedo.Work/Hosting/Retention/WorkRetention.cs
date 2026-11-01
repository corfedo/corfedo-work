namespace Corfedo.Work;

/// <summary>
/// The framework's retention job (J12), which a process runs when it registers it with
/// <see cref="WorkBuilder.AddRetention"/>: a code-defined recurrence on an hourly grid that deletes terminal jobs and
/// run history entries older than each definition's stored retention, incidents resolved longer ago than the host's
/// incident retention and expectation state no key needs, in bounded batches. It has no run history of its
/// own; its own ended occurrences follow its retention like any job's. Process records are deleted by starting
/// processes, see <see cref="ProcessRegistry.Retention"/>.
/// Run it at once with <see cref="JobOperations.TriggerAsync(IWorkTransaction, JobDefinition, CancellationToken)"/>.
/// </summary>
public static class WorkRetention
{
    /// <summary>The rows each retention statement deletes at most.</summary>
    public const int BatchSize = 1000;

    /// <summary>The retention job's definition.</summary>
    public static JobDefinition<RetentionArgs> Definition { get; } = JobDefinition.Create(
        "corfedo.work.retention",
        1,
        new RetentionArgsCodec(),
        new JobPolicy { Attempts = 3, Backoff = Backoff.Exponential(TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(10)) });

    /// <summary>The retention job's schedule: every hour on the hour.</summary>
    public static Schedule Schedule { get; } = Schedule.Every(TimeSpan.FromHours(1), Interval.FixedRate);

    /// <summary>The retention job's arguments; it has none.</summary>
    public sealed record RetentionArgs;

    private sealed class RetentionArgsCodec : IJobArgs<RetentionArgs>
    {
        private static readonly RetentionArgs Instance = new();

        public int Version => 1;

        public byte[] Write(RetentionArgs args) => "{}"u8.ToArray();

        public RetentionArgs Read(ReadOnlySpan<byte> data, int version) => Instance;
    }
}

/// <summary>Runs retention passes until one leaves nothing more to delete or the run is cancelled.</summary>
internal sealed class RetentionHandler(IWorkStore store, JobRetention retention) : IJobHandler<WorkRetention.RetentionArgs>
{
    public async ValueTask<JobDisposition> RunAsync(WorkRetention.RetentionArgs args, JobRun run, CancellationToken cancellationToken)
    {
        while ((await store.Jobs.RetainAsync(WorkRetention.BatchSize, cancellationToken)).More)
        {
            cancellationToken.ThrowIfCancellationRequested();
        }
        while (await store.Monitoring.RetainAsync(retention.Incidents, WorkRetention.BatchSize, cancellationToken) == WorkRetention.BatchSize)
        {
            cancellationToken.ThrowIfCancellationRequested();
        }
        return JobResult.Completed;
    }
}
