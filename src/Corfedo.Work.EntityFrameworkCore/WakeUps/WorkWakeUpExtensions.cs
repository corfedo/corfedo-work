using Microsoft.EntityFrameworkCore;

namespace Corfedo.Work;

public static class WorkWakeUpExtensions
{
    /// <summary>
    /// Wakes the claimers of the application's sources through <paramref name="signals"/> when the context saves a new record with a due time, or a changed
    /// due time, of a <c>DueAt</c> property mapped with <c>HasLease</c> or <c>HasReconciliation</c>: on commit of the
    /// context's transaction, or after the save without one. Due times written another way are found by polling and
    /// next-due timers.
    /// </summary>
    public static DbContextOptionsBuilder AddWorkWakeUps(this DbContextOptionsBuilder options, IWorkSignals signals)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(signals);
        return options.AddInterceptors(new DueWakeUpInterceptor(signals));
    }
}
