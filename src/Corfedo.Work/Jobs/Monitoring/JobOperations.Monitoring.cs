namespace Corfedo.Work;

public sealed partial class JobOperations
{
    /// <summary>
    /// Definitions with expectations, by key, while no live process of the process registry monitors (M2, R12): their
    /// deadlines, running attempts and date sets go unchecked until a process with <c>AddMonitoring()</c> runs.
    /// </summary>
    public ValueTask<IReadOnlyList<string>> GetUnmonitoredAsync(CancellationToken cancellationToken = default) =>
        store.Monitoring.GetUnmonitoredAsync(cancellationToken);
}
