using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;

[assembly: AssemblyFixture(typeof(Corfedo.Work.SqlServer.Tests.Provisioning.SqlServerInstance))]

namespace Corfedo.Work.SqlServer.Tests.Provisioning;

/// <summary>
/// One SQL Server container per test process, published on a free host port. <c>CORFEDO_WORK_SQLSERVER_IMAGE</c>
/// chooses the image, SQL Server 2022 by default.
/// </summary>
public sealed class SqlServerInstance : IAsyncLifetime
{
    public static string Image { get; } = Environment.GetEnvironmentVariable("CORFEDO_WORK_SQLSERVER_IMAGE") is { Length: > 0 } image
        ? image
        : "mcr.microsoft.com/mssql/server:2022-latest";

    private readonly MsSqlContainer container = new MsSqlBuilder(Image).Build();

    /// <summary>
    /// Starts the container with tempdb's metadata in memory-optimized tables: every provider batch declares table
    /// variables, whose creation otherwise contends on tempdb's system pages under the suite's concurrency. The setting
    /// takes effect after a restart.
    /// </summary>
    public async ValueTask InitializeAsync()
    {
        await container.StartAsync();
        await using (var connection = new SqlConnection(container.GetConnectionString()))
        {
            await connection.OpenAsync();
            await using var command = new SqlCommand("ALTER SERVER CONFIGURATION SET MEMORY_OPTIMIZED TEMPDB_METADATA = ON;", connection);
            await command.ExecuteNonQueryAsync();
        }
        await container.StopAsync();
        await container.StartAsync();
    }

    public ValueTask DisposeAsync() => container.DisposeAsync();

    /// <summary>
    /// Creates an empty database and returns its connection string. It reads committed snapshots, as the store
    /// requires, unless <paramref name="snapshotReads"/> is false, allows snapshot transactions, and commits with
    /// delayed durability, since tests never need a commit to survive a crash.
    /// </summary>
    public async Task<string> CreateDatabaseAsync(bool snapshotReads = true, CancellationToken cancellationToken = default)
    {
        var name = "t_" + Guid.NewGuid().ToString("N");
        await using (var connection = new SqlConnection(container.GetConnectionString()))
        {
            await connection.OpenAsync(cancellationToken);
            await using var command = new SqlCommand($"""
                CREATE DATABASE [{name}];
                ALTER DATABASE [{name}] SET READ_COMMITTED_SNAPSHOT {(snapshotReads ? "ON" : "OFF")};
                ALTER DATABASE [{name}] SET ALLOW_SNAPSHOT_ISOLATION ON;
                ALTER DATABASE [{name}] SET DELAYED_DURABILITY = FORCED;
                """, connection);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        return new SqlConnectionStringBuilder(container.GetConnectionString()) { InitialCatalog = name, MaxPoolSize = 200 }.ConnectionString;
    }
}

/// <summary>A fresh database for one test class.</summary>
public sealed class SqlServerDatabase(SqlServerInstance server) : IAsyncLifetime
{
    public string ConnectionString { get; private set; } = "";

    public async ValueTask InitializeAsync() => ConnectionString = await server.CreateDatabaseAsync();

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    /// <summary>Runs a batch on a new connection.</summary>
    public async Task ExecuteAsync(string text, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = new SqlCommand(text, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>Runs a query on a new connection and returns its first value.</summary>
    public async Task<object?> ScalarAsync(string text, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = new SqlCommand(text, connection);
        return await command.ExecuteScalarAsync(cancellationToken);
    }
}
