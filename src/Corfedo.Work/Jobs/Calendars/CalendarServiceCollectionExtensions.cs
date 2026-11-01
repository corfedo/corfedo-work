using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Corfedo.Work;

/// <summary>A calendar this process registers, stored by revision at startup before its recurrences.</summary>
public sealed record DeclaredCalendar(WorkCalendar Calendar);

public static class CalendarServiceCollectionExtensions
{
    /// <summary>
    /// Registers <paramref name="calendar"/>: at startup it is stored unless a newer revision is (R18), after the
    /// calendars it composes with are checked for existence and cycles. Schedules name it with <c>Within</c>. The process
    /// records it in the process registry and logs changes of the date-set coverage report.
    /// </summary>
    public static IServiceCollection AddCalendar(this IServiceCollection services, WorkCalendar calendar)
    {
        ArgumentNullException.ThrowIfNull(calendar);
        services.AddSingleton(new DeclaredCalendar(calendar));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IWorkBackground, CalendarCoverageMonitor>(
            static services => new CalendarCoverageMonitor(services.GetRequiredService<CalendarOperations>())));
        return services.AddJobServices();
    }
}
