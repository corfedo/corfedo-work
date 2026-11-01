using Microsoft.Extensions.Logging;

namespace Corfedo.Work;

/// <summary>
/// Reads the date-set coverage report at the polling interval in a process that registers calendars and logs each
/// change of a set's state: running out, work waiting, and covered again after either. The last state of each set is
/// kept in memory, so a process logs a set that is not covered once when it starts and nothing while it stays so.
/// </summary>
internal sealed partial class CalendarCoverageMonitor(CalendarOperations calendars) : IWorkBackground
{
    private readonly CancellationTokenSource stopping = new();
    private readonly Dictionary<string, DateSetCoverageState> states = new(StringComparer.Ordinal);
    private Task loop = Task.CompletedTask;

    public void Start(LeaseDispatchContext context) => loop = Task.Run(() => LoopAsync(context));

    private async Task LoopAsync(LeaseDispatchContext context)
    {
        var token = stopping.Token;
        var logger = context.Logging.CreateLogger("Corfedo.Work.Calendars");
        var telemetry = WorkTelemetryOptions.From(context.Services);
        while (!token.IsCancellationRequested)
        {
            try
            {
                Record(logger, await calendars.GetDateSetCoverageAsync(token));
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                LogCoverageFailed(logger, telemetry.Logged(exception), telemetry.Describe(exception));
            }
            try
            {
                await Task.Delay(context.PollingInterval, context.Time, token);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private void Record(ILogger logger, IReadOnlyList<DateSetCoverage> report)
    {
        foreach (var set in report)
        {
            var previous = states.GetValueOrDefault(set.Name, DateSetCoverageState.Covered);
            states[set.Name] = set.State;
            if (set.State == previous)
            {
                continue;
            }
            var calendarNames = string.Join(", ", set.Calendars);
            switch (set.State)
            {
                case DateSetCoverageState.RunningOut:
                    LogRunningOut(logger, set.Name, set.CoversThrough, calendars.RunningOutMargin, calendarNames);
                    break;
                case DateSetCoverageState.WorkWaiting:
                    LogWorkWaiting(logger, set.Name, set.CoversThrough, calendarNames);
                    break;
                default:
                    LogCovered(logger, set.Name, set.CoversThrough);
                    break;
            }
        }
        foreach (var name in states.Keys.Except(report.Select(s => s.Name), StringComparer.Ordinal).ToArray())
        {
            states.Remove(name);
        }
    }

    public async Task StopAsync()
    {
        await stopping.CancelAsync();
        await loop.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        stopping.Dispose();
    }

    [LoggerMessage(EventId = 40, EventName = "calendar.date_set_running_out", Level = LogLevel.Warning,
        Message = "Date set {DateSet} covers through {CoversThrough}, within {Margin} of the instants planning needs for calendars {Calendars}; publish a revision that covers further")]
    private static partial void LogRunningOut(ILogger logger, string dateSet, DateOnly? coversThrough, TimeSpan margin, string calendars);

    [LoggerMessage(EventId = 41, EventName = "calendar.work_waiting", Level = LogLevel.Error,
        Message = "Work of calendars {Calendars} waits for date set {DateSet}, which covers through {CoversThrough}; publish a revision that covers its dates")]
    private static partial void LogWorkWaiting(ILogger logger, string dateSet, DateOnly? coversThrough, string calendars);

    [LoggerMessage(EventId = 42, EventName = "calendar.date_set_covered", Level = LogLevel.Information, Message = "Date set {DateSet} covers through {CoversThrough}, as planning needs")]
    private static partial void LogCovered(ILogger logger, string dateSet, DateOnly? coversThrough);

    [LoggerMessage(EventId = 43, EventName = "calendar.coverage_failed", Level = LogLevel.Error, Message = "Reading the date-set coverage report failed: {Error}")]
    private static partial void LogCoverageFailed(ILogger logger, Exception? exception, string error);
}
