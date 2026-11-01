using System.Diagnostics;

namespace Corfedo.Work;

/// <summary>Spans of job runs.</summary>
internal static class JobSpans
{
    /// <summary>A <c>corfedo.work.complete</c> span of applying <paramref name="transition"/> to the run's job.</summary>
    public static Activity? StartComplete(LeaseRun<JobId> run, JobTransition transition)
    {
        var span = WorkTelemetry.Source.StartActivity("corfedo.work.complete");
        if (span is not null)
        {
            span.SetTag(WorkTelemetry.SourceTag, run.Lease.Source.Name);
            span.SetTag(WorkTelemetry.DefinitionTag, run.Lease.Source.Job!.Definition);
            span.SetTag(WorkTelemetry.OutcomeTag, WorkTelemetry.Value(transition.Kind));
            span.SetTag(WorkTelemetry.ReasonTag, transition.Code);
        }
        return span;
    }
}
