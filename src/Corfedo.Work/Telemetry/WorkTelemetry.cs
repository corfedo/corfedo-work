using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Corfedo.Work;

/// <summary>
/// The <c>Corfedo.Work</c> activity source and meter with their instruments, shared by every layer and provider of the
/// process. Tags never carry arguments, results, subject identifiers or exception messages.
/// </summary>
internal static class WorkTelemetry
{
    public const string Name = "Corfedo.Work";

    public const string SourceTag = "corfedo.work.source";
    public const string DefinitionTag = "corfedo.work.definition";
    public const string OutcomeTag = "corfedo.work.outcome";
    public const string ReasonTag = "corfedo.work.reason";
    public const string KindTag = "corfedo.work.kind";
    public const string EventTag = "corfedo.work.event";
    public const string PartitionTag = "corfedo.work.partition";
    public const string JobTag = "corfedo.work.job";
    public const string ExecutionTag = "corfedo.work.execution";
    public const string AttemptTag = "corfedo.work.attempt";
    public const string ErrorTypeTag = "error.type";

    private static readonly string? Version = typeof(WorkTelemetry).Assembly.GetName().Version?.ToString();

    public static ActivitySource Source { get; } = new(Name, Version);

    public static Meter Meter { get; } = new(Name, Version);

    public static Counter<long> Claims { get; } = Meter.CreateCounter<long>("corfedo.work.claims", "{claim}", "Records claimed by dispatchers.");

    public static Counter<long> Takeovers { get; } = Meter.CreateCounter<long>("corfedo.work.takeovers", "{takeover}", "Claims of records whose previous lease expired without being given back.");

    public static Counter<long> LeasesLost { get; } = Meter.CreateCounter<long>("corfedo.work.leases_lost", "{lease}", "Runs whose lease was lost.");

    public static Counter<long> Supersessions { get; } = Meter.CreateCounter<long>("corfedo.work.supersessions", "{supersession}", "Runs stopped because a newer request or an invalidation made them obsolete.");

    public static Counter<long> JobOutcomes { get; } = Meter.CreateCounter<long>("corfedo.work.job.outcomes", "{job}", "Terminal transitions of job executions.");

    public static Counter<long> OccurrencesSkipped { get; } = Meter.CreateCounter<long>("corfedo.work.occurrences.skipped", "{occurrence}", "Occurrences of recurring jobs recorded skipped.");

    public static Counter<long> Incidents { get; } = Meter.CreateCounter<long>("corfedo.work.incidents", "{incident}", "Incidents opened and resolved.");

    public static Histogram<double> JobDuration { get; } = Meter.CreateHistogram<double>("corfedo.work.job.duration", "s", "Duration of job handler attempts.");

    public static Histogram<double> JobLateness { get; } = Meter.CreateHistogram<double>("corfedo.work.job.lateness", "s", "Time from a job's due time to the claim that runs it.");

    public static GaugeReadings<long> RunningReadings { get; } = new();

    public static GaugeReadings<double> DueAgeReadings { get; } = new();

    public static GaugeReadings<long> OpenIncidentReadings { get; } = new();

    public static ObservableGauge<long> Running { get; } = Meter.CreateObservableGauge("corfedo.work.running", RunningReadings.Read, "{run}", "Records this process runs now, per source.");

    public static ObservableGauge<double> DueAge { get; } = Meter.CreateObservableGauge("corfedo.work.due.age", DueAgeReadings.Read, "s", "Age of each job definition's oldest due job, store-wide.");

    public static ObservableGauge<long> OpenIncidents { get; } = Meter.CreateObservableGauge("corfedo.work.incidents.open", OpenIncidentReadings.Read, "{incident}", "Open incidents per kind, store-wide.");

    /// <summary>Counts the incidents a transaction opened and resolved.</summary>
    public static void Count(IncidentChanges changes)
    {
        if (!Incidents.Enabled)
        {
            return;
        }
        foreach (var incident in changes.Opened)
        {
            Count(incident, "opened");
        }
        foreach (var incident in changes.Resolved)
        {
            Count(incident, "resolved");
        }
    }

    private static void Count(WorkIncident incident, string @event) =>
        Incidents.Add(1, new TagList { { KindTag, Value(incident.Kind) }, { DefinitionTag, incident.Definition }, { EventTag, @event } });

    /// <summary>Marks a span failed with the error's type and the reason code.</summary>
    public static void Fail(Activity? activity, string errorType, string? reason)
    {
        if (activity is null)
        {
            return;
        }
        activity.SetStatus(ActivityStatusCode.Error);
        activity.SetTag(ErrorTypeTag, errorType);
        activity.SetTag(ReasonTag, reason);
    }

    public static string Value(JobOutcome outcome) => outcome switch
    {
        JobOutcome.Completed => "completed",
        JobOutcome.Failed => "failed",
        JobOutcome.Cancelled => "cancelled",
        JobOutcome.TimedOut => "timed_out",
        JobOutcome.Skipped => "skipped",
        _ => "unknown",
    };

    public static string Value(JobTransitionKind kind) => kind switch
    {
        JobTransitionKind.Complete => "completed",
        JobTransitionKind.Retry => "retried",
        JobTransitionKind.Fail => "failed",
        JobTransitionKind.Cancel => "cancelled",
        JobTransitionKind.TimeOut => "timed_out",
        JobTransitionKind.Skip => "skipped",
        _ => "unknown",
    };

    public static string Value(WorkIncidentKind kind) => kind switch
    {
        WorkIncidentKind.CompletesBy => "completes_by",
        WorkIncidentKind.RunsAtMost => "runs_at_most",
        WorkIncidentKind.FailsAtMostInARow => "fails_at_most_in_a_row",
        WorkIncidentKind.DateSetRunningOut => "date_set_running_out",
        WorkIncidentKind.DateSetWorkWaiting => "date_set_work_waiting",
        WorkIncidentKind.Unserved => "unserved",
        _ => "unknown",
    };
}

/// <summary>
/// The readings of one observable gauge, from every component that reports it in this process; each reads values it
/// holds, so a listener's collection makes no store calls.
/// </summary>
internal sealed class GaugeReadings<T>
    where T : struct
{
    private readonly System.Collections.Concurrent.ConcurrentDictionary<Registration, Func<IEnumerable<Measurement<T>>>> readers = new();

    /// <summary>Adds a reader until the returned registration is disposed.</summary>
    public IDisposable Register(Func<IEnumerable<Measurement<T>>> read)
    {
        var registration = new Registration(this);
        readers[registration] = read;
        return registration;
    }

    public IEnumerable<Measurement<T>> Read() => readers.Values.SelectMany(read => read()).ToArray();

    private sealed class Registration(GaugeReadings<T> owner) : IDisposable
    {
        public void Dispose() => owner.readers.TryRemove(this, out _);
    }
}

/// <summary>What a host adds to telemetry; see <c>WorkBuilder.Telemetry</c>.</summary>
internal sealed record WorkTelemetryOptions(bool Partition, bool ExceptionDetails)
{
    public static WorkTelemetryOptions Default { get; } = new(false, false);

    /// <summary>The host's options registered in <paramref name="services"/>, or the defaults.</summary>
    public static WorkTelemetryOptions From(IServiceProvider services) =>
        (WorkTelemetryOptions?)services.GetService(typeof(WorkTelemetryOptions)) ?? Default;

    /// <summary>The exception a framework log passes: the exception itself with <see cref="ExceptionDetails"/>, otherwise none.</summary>
    public Exception? Logged(Exception exception) => ExceptionDetails ? exception : null;

    /// <summary>
    /// How a framework log message names the exception: its type with <see cref="ExceptionDetails"/>, which passes the
    /// exception itself, otherwise its type chain and stack frames without messages.
    /// </summary>
    public string Describe(Exception exception) => ExceptionDetails ? exception.GetType().FullName! : ExceptionDescription.Describe(exception);

    /// <summary>The partition tag when the host adds it and the record has a partition; null otherwise.</summary>
    public string? PartitionOf(string? partition) => Partition && !string.IsNullOrEmpty(partition) ? partition : null;
}
