using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;

[assembly: AssemblyFixture(typeof(Corfedo.Work.SqlServer.Tests.Provisioning.SqlServerInstance))]

namespace Corfedo.Work.SqlServer.Tests.Provisioning;

/// <summary>
/// The SQL Server of a test process. <c>CORFEDO_WORK_SQLSERVER</c>, a connection string to <c>master</c>, names a
/// running server that test processes share, as <c>dev/check</c> provides; without it the process starts a container
/// of its own, published on a free host port, whose image <c>CORFEDO_WORK_SQLSERVER_IMAGE</c> chooses, SQL Server
/// 2022 by default. A shared server keeps one buffer pool for all processes: SQL Server sizes its memory to the host,
/// and a container per process ran a 30 GiB host out of memory.
/// </summary>
public sealed class SqlServerInstance : IAsyncLifetime
{
    public static string Image { get; } = Environment.GetEnvironmentVariable("CORFEDO_WORK_SQLSERVER_IMAGE") is { Length: > 0 } image
        ? image
        : "mcr.microsoft.com/mssql/server:2022-latest";

    private static readonly string? Shared = Environment.GetEnvironmentVariable("CORFEDO_WORK_SQLSERVER") is { Length: > 0 } shared ? shared : null;

    private readonly MsSqlContainer? container = Shared is null ? new MsSqlBuilder(Image).Build() : null;
    private readonly List<string> databases = [];
    private readonly List<string> logins = [];

    private string Master => Shared ?? container!.GetConnectionString();

    /// <summary>Starts the container, with SQL Server's default configuration; a shared server is run by whoever runs it.</summary>
    public async ValueTask InitializeAsync()
    {
        if (container is not null)
        {
            await container.StartAsync();
        }
    }

    /// <summary>Removes the container, or on a shared server the databases this process created.</summary>
    public async ValueTask DisposeAsync()
    {
        if (container is not null)
        {
            await container.DisposeAsync();
            return;
        }
        SqlConnection.ClearAllPools();
        await using var connection = new SqlConnection(Master);
        await connection.OpenAsync();
        foreach (var name in databases)
        {
            await using var command = new SqlCommand($"IF DB_ID(N'{name}') IS NOT NULL BEGIN ALTER DATABASE [{name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{name}]; END", connection);
            await command.ExecuteNonQueryAsync();
        }
        foreach (var name in logins)
        {
            // A login is dropped once its sessions have ended, which dropping their database or KILL only requests.
            await using var command = new SqlCommand($"""
                DECLARE @kill nvarchar(max) = N'', @waits int = 0;
                SELECT @kill += N'KILL ' + CAST(session_id AS nvarchar(10)) + N';' FROM sys.dm_exec_sessions WHERE login_name = N'{name}';
                EXEC (@kill);
                WHILE EXISTS (SELECT 1 FROM sys.dm_exec_sessions WHERE login_name = N'{name}') AND @waits < 100
                BEGIN
                    WAITFOR DELAY '00:00:00.020';
                    SET @waits += 1;
                END
                IF SUSER_ID(N'{name}') IS NOT NULL DROP LOGIN [{name}];
                """, connection);
            await command.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Creates a login with a user of the same name in the database of <paramref name="connectionString"/>, without
    /// permissions beyond SQL Server's defaults, and returns the name and a connection string to that database under it.
    /// </summary>
    public async Task<(string User, string ConnectionString)> CreateLoginAsync(string connectionString, CancellationToken cancellationToken = default)
    {
        var name = "login_" + Guid.NewGuid().ToString("N")[..12];
        var password = "Cw-" + Guid.NewGuid().ToString("N");
        lock (logins)
        {
            logins.Add(name);
        }
        await using (var connection = new SqlConnection(Master))
        {
            await connection.OpenAsync(cancellationToken);
            await using var command = new SqlCommand($"CREATE LOGIN [{name}] WITH PASSWORD = N'{password}', CHECK_POLICY = OFF", connection);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        await using (var connection = new SqlConnection(connectionString))
        {
            await connection.OpenAsync(cancellationToken);
            await using var command = new SqlCommand($"CREATE USER [{name}] FOR LOGIN [{name}]", connection);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        return (name, new SqlConnectionStringBuilder(connectionString) { UserID = name, Password = password }.ConnectionString);
    }

    /// <summary>
    /// Creates an empty database and returns its connection string. It reads committed snapshots, as the store
    /// requires, unless <paramref name="snapshotReads"/> is false, allows snapshot transactions, and commits with
    /// delayed durability, since tests never need a commit to survive a crash.
    /// </summary>
    public async Task<string> CreateDatabaseAsync(bool snapshotReads = true, CancellationToken cancellationToken = default)
    {
        var name = "t_" + Guid.NewGuid().ToString("N");
        lock (databases)
        {
            databases.Add(name);
        }
        await using (var connection = new SqlConnection(Master))
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
        return new SqlConnectionStringBuilder(Master) { InitialCatalog = name, MaxPoolSize = 200 }.ConnectionString;
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
