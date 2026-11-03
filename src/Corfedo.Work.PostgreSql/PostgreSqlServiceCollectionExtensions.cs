using Corfedo.Work.PostgreSql.Signals;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;

namespace Corfedo.Work.PostgreSql;

public static class PostgreSqlServiceCollectionExtensions
{
    /// <summary>
    /// Stores work in PostgreSQL through <paramref name="dataSource"/>, with the store's signals as the process's
    /// <see cref="IWorkSignals"/>. Framework tables live in <paramref name="schema"/>, at most 50 bytes, which also
    /// names the application's signal channel; listening for signals needs a session-level connection, so a
    /// transaction-mode pooler needs a direct data source.
    /// </summary>
    public static IServiceCollection AddPostgreSqlWorkStore(this IServiceCollection services, NpgsqlDataSource dataSource, string schema = "work")
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);
        PostgreSqlSignalSql.For(schema);
        services.AddSingleton<IWorkStore>(provider => new PostgreSqlWorkStore(
            dataSource,
            schema,
            provider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance,
            provider.GetService<TimeProvider>() ?? TimeProvider.System,
            WorkTelemetryOptions.From(provider)));
        services.TryAddSingleton(provider => provider.GetRequiredService<IWorkStore>().Signals!);
        return services.AddWorkSignals();
    }

    /// <summary>
    /// Registers <see cref="IWorkSignals"/> over PostgreSQL for an application that uses signals without the other
    /// layers. <paramref name="application"/> names the signal channel as a framework schema does, at most 50 bytes;
    /// an application that later stores work uses its schema name. Listening needs a session-level connection, so a
    /// transaction-mode pooler needs a direct data source.
    /// </summary>
    public static IServiceCollection AddPostgreSqlSignals(this IServiceCollection services, NpgsqlDataSource dataSource, string application)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(dataSource);
        var sql = PostgreSqlSignalSql.For(application);
        services.TryAddSingleton<ISignalTransport>(provider =>
        {
            var logger = (provider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance).CreateLogger("Corfedo.Work.PostgreSql");
            var telemetry = WorkTelemetryOptions.From(provider);
            return new PostgreSqlSignalTransport(
                dataSource,
                new PostgreSqlSignalSender(dataSource, sql, logger, telemetry),
                provider.GetService<TimeProvider>() ?? TimeProvider.System,
                logger,
                telemetry);
        });
        return services.AddWorkSignals();
    }
}
