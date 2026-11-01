using Corfedo.Work.InMemory.Database;

namespace Corfedo.Work.InMemory.Jobs;

/// <summary>The run history of job attempts.</summary>
internal static class JobRuns
{
    /// <summary>
    /// Records the open attempt of job <paramref name="r"/>, as read before the statement that ends it changes it, when
    /// its stored definition keeps run history: an attempt is open while <c>attempt_started_at</c> is set, and every
    /// operation that ends an attempt clears it, so each attempt is recorded once. The entry is keyed by the epoch of the
    /// claim that used the attempt and carries <paramref name="occurrence"/>, the job's occurrence unless the operation
    /// ends it as another.
    /// </summary>
    public static void Record(InMemoryTransaction transaction, ApplicationState application, Record r, JobRunOutcome outcome, string? reason, DateTimeOffset ended, string? occurrence = null)
    {
        if (r.Time(JobColumns.AttemptStartedAt) is not { } started
            || transaction.Get(application.Definitions, r.Text(JobColumns.Definition)!) is not { Stored.Policy.RunHistory: true })
        {
            return;
        }
        var key = new RunKey(r.Uuid(JobColumns.Id)!.Value, r.Int64(JobColumns.AttemptEpoch));
        if (transaction.Get(application.Runs, key) is not null)
        {
            return;
        }
        transaction.Put(application.Runs, key, new RunRow(r.Text(JobColumns.Definition)!, new JobRunEntry(
            r.JobId(),
            key.Epoch,
            r.Subject(),
            occurrence ?? r.Text(JobColumns.Occurrence),
            r.Uuid(LeaseFields.Default.ExecutionId)!.Value,
            r.Int32(JobColumns.Attempts),
            new OwnerId(r.Uuid(LeaseFields.Default.LeaseOwner)!.Value),
            started,
            ended,
            outcome,
            reason)));
    }
}
