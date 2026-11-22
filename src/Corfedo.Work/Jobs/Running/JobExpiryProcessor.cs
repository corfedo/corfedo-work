using Microsoft.Extensions.Logging;

namespace Corfedo.Work;

/// <summary>
/// Settles expired leases of every job definition the process runs, as one pass of the process's expiry loop. The
/// definitions share the store's job table, so one statement tells which of them hold an expired, unsettled lease and
/// when the next lease expires; only those definitions run a settlement. Like a source's own expiry processor, the pass
/// is due at the earliest known expiry and at most the polling interval, or the shortest lease duration of the
/// definitions when it is shorter, after its last pass, and every <see cref="ExpiryProcessor{TKey}.ExpiredRetry"/> while
/// a definition's expired lease could not be settled, for example because its record is locked. A definition whose
/// settlement failed is passed over until the polling interval has passed, as a source's own processor
/// waits after a failed pass. Passes of the loop never overlap, so the pass needs no lock.
/// </summary>
internal sealed partial class JobExpiryProcessor
{
    /// <summary>The name of the pass in the expiry loop.</summary>
    public const string Name = "job definitions";

    private const int BatchSize = ExpiryProcessor<JobId>.BatchSize;

    private readonly LeaseSourceRegistration<JobId>[] registrations;
    private readonly string[] definitions;
    private readonly LeaseDispatchContext context;
    private readonly ILogger logger;
    private readonly WorkTelemetryOptions telemetry;
    private readonly Dictionary<string, long> failedUntil = new(StringComparer.Ordinal);
    private readonly TimeSpan cap;

    public JobExpiryProcessor(IReadOnlyList<LeaseSourceRegistration<JobId>> registrations, LeaseDispatchContext context)
    {
        this.registrations = [.. registrations];
        definitions = [.. registrations.Select(Definition)];
        this.context = context;
        logger = context.Logging.CreateLogger("Corfedo.Work.Leases");
        telemetry = WorkTelemetryOptions.From(context.Services);
        cap = this.registrations.Aggregate(context.PollingInterval, (wait, registration) => Min(wait, registration.Source.Policy.Duration));
    }

    private static string Definition(LeaseSourceRegistration<JobId> registration) =>
        registration.Source.Job?.Definition ?? throw new ArgumentException($"Lease source {registration.Name} is not a job source.", nameof(registration));

    public async Task<TimeSpan> SettleAsync(CancellationToken cancellationToken)
    {
        IReadOnlyDictionary<string, TimeSpan> expiries;
        try
        {
            expiries = await context.Store.Jobs.NextExpiriesAsync(definitions, cancellationToken);
        }
        catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
        {
            LogSettlementFailed(logger, telemetry.Logged(exception), Name, telemetry.Describe(exception));
            return context.PollingInterval;
        }
        var time = context.Time;
        var wait = PassLoop.Poll(cap);
        for (var i = 0; i < registrations.Length; i++)
        {
            if (!expiries.TryGetValue(definitions[i], out var until))
            {
                continue;
            }
            if (until > TimeSpan.Zero)
            {
                wait = Min(wait, until);
                continue;
            }
            if (failedUntil.TryGetValue(definitions[i], out var retry))
            {
                var remaining = time.GetElapsedTime(time.GetTimestamp(), retry);
                if (remaining > TimeSpan.Zero)
                {
                    wait = Min(wait, remaining);
                    continue;
                }
                failedUntil.Remove(definitions[i]);
            }
            var registration = registrations[i];
            try
            {
                // After a settlement the pass runs again at once: the batch may have been full, and the statement then
                // reads the next expiry of the definition, which may be sooner than the wait.
                if (await context.Store.Leases.SettleExpiredAsync(registration.Source, registration.Callbacks(context.Services), BatchSize, cancellationToken) > 0)
                {
                    wait = TimeSpan.Zero;
                }
                else
                {
                    // The expired lease stays unsettled, for example because its record is locked.
                    wait = Min(wait, PassLoop.Poll(ExpiryProcessor<JobId>.ExpiredRetry));
                }
            }
            catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
            {
                LogSettlementFailed(logger, telemetry.Logged(exception), registration.Name, telemetry.Describe(exception));
                failedUntil[definitions[i]] = time.GetTimestamp() + (long)Math.Ceiling(context.PollingInterval.TotalSeconds * time.TimestampFrequency);
            }
        }
        return wait;
    }

    private static TimeSpan Min(TimeSpan a, TimeSpan b) => a < b ? a : b;

    [LoggerMessage(EventId = 14, EventName = "lease.settlement_pass_failed", Level = LogLevel.Error, Message = "Settling expired leases of {Source} failed: {Error}")]
    private static partial void LogSettlementFailed(ILogger logger, Exception? exception, string source, string error);
}
