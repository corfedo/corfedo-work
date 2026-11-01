namespace Corfedo.Work.Conformance;

/// <summary>
/// A provider whose store clock the scenarios control (R20): time moves only when a scenario advances it, and the
/// scenario's processes use the same clock.
/// </summary>
public interface IClockConformanceProvider : IJobConformanceProvider
{
    /// <summary>The store's clock, the time provider of the scenario's processes.</summary>
    TimeProvider Clock { get; }

    /// <summary>Moves the clock forward by <paramref name="by"/>; timers due on the way fire in due order.</summary>
    void Advance(TimeSpan by);
}
