using Corfedo.Work.PostgreSql.Signals;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;

namespace Corfedo.Work.PostgreSql;

/// <summary>
/// Registers Work over PostgreSQL. Work never disposes a data source: one the container creates, for example with
/// Npgsql's <c>AddNpgsqlDataSource</c>, is disposed by the container after the store, and one passed as an instance
/// belongs to the caller.
/// </summary>
public static class PostgreSqlServiceCollectionExtensions
{
    /// <summary>
    /// Stores work in PostgreSQL through the container's <see cref="NpgsqlDataSource"/>, or the one registered under
    /// <paramref name="dataSourceKey"/>, with the store's signals as the process's <see cref="IWorkSignals"/>. Framework
    /// tables live in <paramref name="schema"/>, at most 50 bytes, which also names the application's signal channel;
    /// listening for signals needs a session-level connection, so a transaction-mode pooler needs a direct data source.
    /// With <see cref="WorkSchemaMode.Migrate"/> a starting process applies the schema; with
    /// <see cref="WorkSchemaMode.Verify"/> it only checks the schema that the migrate step applied with
    /// <see cref="PostgreSqlWorkSchema"/>.
    /// </summary>
    public static IServiceCollection AddPostgreSqlWorkStore(this IServiceCollection services, string schema = "work", WorkSchemaMode schemaMode = WorkSchemaMode.Migrate, object? dataSourceKey = null) =>
        services.AddPostgreSqlWorkStore(provider => FromContainer(provider, dataSourceKey), schema, schemaMode);

    /// <summary>
    /// Stores work in PostgreSQL through <paramref name="dataSource"/>, which the caller owns; otherwise as
    /// <see cref="AddPostgreSqlWorkStore(IServiceCollection, string, WorkSchemaMode, object?)"/>.
    /// </summary>
    public static IServiceCollection AddPostgreSqlWorkStore(this IServiceCollection services, NpgsqlDataSource dataSource, string schema = "work", WorkSchemaMode schemaMode = WorkSchemaMode.Migrate)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        return services.AddPostgreSqlWorkStore(_ => dataSource, schema, schemaMode);
    }

    /// <summary>
    /// Stores work in PostgreSQL through the data source that <paramref name="dataSource"/> returns, called once when the
    /// container creates the store; otherwise as
    /// <see cref="AddPostgreSqlWorkStore(IServiceCollection, string, WorkSchemaMode, object?)"/>.
    /// </summary>
    public static IServiceCollection AddPostgreSqlWorkStore(this IServiceCollection services, Func<IServiceProvider, NpgsqlDataSource> dataSource, string schema = "work", WorkSchemaMode schemaMode = WorkSchemaMode.Migrate)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);
        PostgreSqlSignalSql.For(schema);
        services.AddSingleton<IWorkStore>(provider => new PostgreSqlWorkStore(
            Resolve(dataSource, provider),
            schema,
            provider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance,
            provider.GetService<TimeProvider>() ?? TimeProvider.System,
            WorkTelemetryOptions.From(provider),
            schemaMode));
        services.TryAddSingleton(provider => provider.GetRequiredService<IWorkStore>().Signals!);
        return services.AddWorkSignals();
    }

    /// <summary>
    /// Registers <see cref="IWorkSignals"/> over PostgreSQL, through the container's <see cref="NpgsqlDataSource"/> or the
    /// one registered under <paramref name="dataSourceKey"/>, for an application that uses signals without the other
    /// layers. <paramref name="application"/> names the signal channel as a framework schema does, at most 50 bytes; an
    /// application that later stores work uses its schema name. Listening needs a session-level connection, so a
    /// transaction-mode pooler needs a direct data source.
    /// </summary>
    public static IServiceCollection AddPostgreSqlSignals(this IServiceCollection services, string application, object? dataSourceKey = null) =>
        services.AddPostgreSqlSignals(provider => FromContainer(provider, dataSourceKey), application);

    /// <summary>
    /// Registers <see cref="IWorkSignals"/> over PostgreSQL through <paramref name="dataSource"/>, which the caller owns;
    /// otherwise as <see cref="AddPostgreSqlSignals(IServiceCollection, string, object?)"/>.
    /// </summary>
    public static IServiceCollection AddPostgreSqlSignals(this IServiceCollection services, NpgsqlDataSource dataSource, string application)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        return services.AddPostgreSqlSignals(_ => dataSource, application);
    }

    /// <summary>
    /// Registers <see cref="IWorkSignals"/> over PostgreSQL through the data source that <paramref name="dataSource"/>
    /// returns, called once when the container creates the signal transport; otherwise as
    /// <see cref="AddPostgreSqlSignals(IServiceCollection, string, object?)"/>.
    /// </summary>
    public static IServiceCollection AddPostgreSqlSignals(this IServiceCollection services, Func<IServiceProvider, NpgsqlDataSource> dataSource, string application)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(dataSource);
        var sql = PostgreSqlSignalSql.For(application);
        services.TryAddSingleton<ISignalTransport>(provider =>
        {
            var source = Resolve(dataSource, provider);
            var logger = (provider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance).CreateLogger("Corfedo.Work.PostgreSql");
            var telemetry = WorkTelemetryOptions.From(provider);
            return new PostgreSqlSignalTransport(
                source,
                new PostgreSqlSignalSender(source, sql, logger, telemetry),
                provider.GetService<TimeProvider>() ?? TimeProvider.System,
                logger,
                telemetry);
        });
        return services.AddWorkSignals();
    }

    private static NpgsqlDataSource Resolve(Func<IServiceProvider, NpgsqlDataSource> dataSource, IServiceProvider provider) =>
        dataSource(provider) ?? throw new InvalidOperationException("The data source factory given to Work returned null.");

    private static NpgsqlDataSource FromContainer(IServiceProvider provider, object? key) =>
        (key is null ? provider.GetService<NpgsqlDataSource>() : provider.GetKeyedService<NpgsqlDataSource>(key))
        ?? throw new InvalidOperationException(key is null
            ? "Work needs an NpgsqlDataSource in the container: register one, for example with AddNpgsqlDataSource, or pass one when registering Work."
            : $"Work needs an NpgsqlDataSource registered under the key '{key}': register one, for example with AddNpgsqlDataSource(connectionString, serviceKey: ...), or pass one when registering Work.");
}
