using Npgsql;
using Testcontainers.PostgreSql;

[assembly: AssemblyFixture(typeof(Corfedo.Work.PostgreSql.Tests.Provisioning.PostgreSqlServer))]

namespace Corfedo.Work.PostgreSql.Tests.Provisioning;

/// <summary>One PostgreSQL container per test process, published on a free host port.</summary>
public sealed class PostgreSqlServer : IAsyncLifetime
{
    private readonly PostgreSqlContainer container = new PostgreSqlBuilder("postgres:18.6")
        .WithCommand("-c", "max_connections=500", "-c", "fsync=off", "-c", "synchronous_commit=off", "-c", "full_page_writes=off")
        .Build();

    public async ValueTask InitializeAsync() => await container.StartAsync();

    public ValueTask DisposeAsync() => container.DisposeAsync();

    /// <summary>Creates an empty database and returns its connection string.</summary>
    public async Task<string> CreateDatabaseAsync(CancellationToken cancellationToken = default)
    {
        var name = "t_" + Guid.NewGuid().ToString("N");
        await using (var connection = new NpgsqlConnection(container.GetConnectionString()))
        {
            await connection.OpenAsync(cancellationToken);
            await using var command = new NpgsqlCommand($"CREATE DATABASE {name}", connection);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        return new NpgsqlConnectionStringBuilder(container.GetConnectionString()) { Database = name }.ConnectionString;
    }
}

/// <summary>A fresh database for one test class.</summary>
public sealed class PostgreSqlDatabase(PostgreSqlServer server) : IAsyncLifetime
{
    public NpgsqlDataSource DataSource { get; private set; } = null!;

    public string ConnectionString { get; private set; } = "";

    public async ValueTask InitializeAsync()
    {
        ConnectionString = await server.CreateDatabaseAsync();
        DataSource = NpgsqlDataSource.Create(ConnectionString);
    }

    public ValueTask DisposeAsync() => DataSource.DisposeAsync();
}
