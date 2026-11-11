using Corfedo.Work.SqlServer.Signals;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Corfedo.Work.SqlServer;

public static class SqlServerServiceCollectionExtensions
{
    /// <summary>
    /// Stores work in SQL Server through <paramref name="connectionString"/>, with the store's signals as the process's
    /// <see cref="IWorkSignals"/>. Framework tables, including the signals table, live in <paramref name="schema"/>.
    /// The database must read committed snapshots (<c>READ_COMMITTED_SNAPSHOT ON</c>); the store refuses to start
    /// otherwise. <paramref name="signals"/> sets how the listener polls the signals table and how long signals are kept.
    /// With <see cref="WorkSchemaMode.Migrate"/> a starting process applies the schema; with
    /// <see cref="WorkSchemaMode.Verify"/> it only checks the schema that the migrate step applied with
    /// <see cref="SqlServerWorkSchema"/>.
    /// </summary>
    public static IServiceCollection AddSqlServerWorkStore(this IServiceCollection services, string connectionString, string schema = "work", SqlServerSignalOptions? signals = null, WorkSchemaMode schemaMode = WorkSchemaMode.Migrate)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);
        var options = signals ?? new SqlServerSignalOptions();
        options.Validate();
        SignalServiceCollectionExtensions.EnsureNoSignals(services);
        services.AddSingleton<IWorkStore>(provider => new SqlServerWorkStore(
            connectionString,
            schema,
            options,
            provider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance,
            provider.GetService<TimeProvider>() ?? TimeProvider.System,
            WorkTelemetryOptions.From(provider),
            schemaMode));
        services.TryAddSingleton(provider => provider.GetRequiredService<IWorkStore>().Signals!);
        return services.AddWorkSignals();
    }

    /// <summary>
    /// Registers <see cref="IWorkSignals"/> over SQL Server for an application that uses signals without the other
    /// layers; a process with a store gets its signals from the store, and registering both fails. <paramref name="application"/> names the schema of the signals table, created when missing, as a
    /// framework schema does; an application that later stores work uses its schema name.
    /// </summary>
    public static IServiceCollection AddSqlServerSignals(this IServiceCollection services, string connectionString, string application, SqlServerSignalOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(application);
        var signalOptions = options ?? new SqlServerSignalOptions();
        signalOptions.Validate();
        SignalServiceCollectionExtensions.EnsureNoStore(services, nameof(AddSqlServerSignals));
        services.TryAddSingleton<ISignalTransport>(provider => new SqlServerSignalTransport(
            connectionString,
            new SqlServerSignalSql(application),
            signalOptions,
            provider.GetService<TimeProvider>() ?? TimeProvider.System,
            (provider.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance).CreateLogger("Corfedo.Work.SqlServer"),
            WorkTelemetryOptions.From(provider)));
        return services.AddWorkSignals();
    }
}
