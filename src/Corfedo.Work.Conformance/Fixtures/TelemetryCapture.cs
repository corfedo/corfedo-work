using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Corfedo.Work.Conformance;

/// <summary>One measurement of a <c>Corfedo.Work</c> instrument with its tags.</summary>
public sealed record CapturedMeasurement(string Instrument, string? Unit, double Value, IReadOnlyDictionary<string, object?> Tags)
{
    public string? Tag(string name) => Tags.GetValueOrDefault(name)?.ToString();
}

/// <summary>
/// Spans and measurements of the <c>Corfedo.Work</c> activity source and meter while it is alive. Listeners are
/// process-wide, so scenarios filter what they read by their own definition keys and sources.
/// </summary>
public sealed class TelemetryCapture : IDisposable
{
    public const string Name = "Corfedo.Work";

    private readonly ActivityListener activities;
    private readonly MeterListener meters;
    private readonly ConcurrentQueue<Activity> spans = new();
    private readonly ConcurrentQueue<CapturedMeasurement> measurements = new();

    public TelemetryCapture()
    {
        activities = new ActivityListener
        {
            ShouldListenTo = source => source.Name == Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Enqueue,
        };
        ActivitySource.AddActivityListener(activities);
        meters = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == Name)
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            },
        };
        meters.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Record(instrument, value, tags));
        meters.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Record(instrument, value, tags));
        meters.Start();
    }

    /// <summary>Stopped spans, in the order they stopped.</summary>
    public IReadOnlyCollection<Activity> Spans => spans;

    /// <summary>Spans named <paramref name="operation"/> with the tag <paramref name="tag"/> set to <paramref name="value"/>.</summary>
    public Activity[] SpansOf(string operation, string tag, string value) =>
        [.. spans.Where(s => s.OperationName == operation && s.GetTagItem(tag)?.ToString() == value)];

    /// <summary>Measurements of <paramref name="instrument"/> with the tag <paramref name="tag"/> set to <paramref name="value"/>.</summary>
    public CapturedMeasurement[] Of(string instrument, string tag, string value) =>
        [.. measurements.Where(m => m.Instrument == instrument && m.Tag(tag) == value)];

    /// <summary>Reads the observable gauges now; their readings are added to the measurements.</summary>
    public void RecordGauges() => meters.RecordObservableInstruments();

    private void Record(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var copy = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var (name, tagValue) in tags)
        {
            copy[name] = tagValue;
        }
        measurements.Enqueue(new CapturedMeasurement(instrument.Name, instrument.Unit, value, copy));
    }

    public void Dispose()
    {
        activities.Dispose();
        meters.Dispose();
    }
}
