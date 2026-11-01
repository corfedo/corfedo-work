namespace Corfedo.Work;

/// <summary>Reads of incidents (M2): broken expectations of job definitions and date sets, open and resolved.</summary>
public sealed class IncidentOperations(IWorkStore store)
{
    /// <summary>The most incidents one page returns.</summary>
    public const int MaximumIncidents = 1000;

    /// <summary>
    /// Incidents newest first by opening time: up to <paramref name="limit"/> after <paramref name="after"/>, the cursor
    /// of the previous page; only open ones with <paramref name="openOnly"/>, only those of <paramref name="definition"/>
    /// when it is set. Resolved incidents are kept for the retention the host sets, 90 days by default.
    /// </summary>
    public ValueTask<WorkIncidentPage> GetIncidentsAsync(bool openOnly = false, string? definition = null, int limit = 50, WorkIncidentCursor? after = null, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(limit, MaximumIncidents);
        return store.Monitoring.GetIncidentsAsync(openOnly, definition, limit, after, cancellationToken);
    }
}
