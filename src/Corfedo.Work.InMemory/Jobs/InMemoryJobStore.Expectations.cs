namespace Corfedo.Work.InMemory.Jobs;

internal sealed partial class InMemoryJobStore
{
    /// <summary>Records a job's end against its definition's expectations, in the transaction that ends it.</summary>
    private void RecordEnd(InMemoryTransaction work, JobEnd end, JobExpectation expectation, DateTimeOffset now) =>
        monitoring.RecordEnd(work, end.Definition, end.Job, end.Subject, end.Outcome, now, expectation.FailureLimit);
}
