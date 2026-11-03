using Corfedo.Work.PostgreSql;
using Corfedo.Work.PostgreSql.Tests.Provisioning;
using Corfedo.Work.SqlServer;
using Corfedo.Work.SqlServer.Tests.Provisioning;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Corfedo.Work.EntityFrameworkCore.Tests.Provisioning;

/// <summary>A fresh database for one test class, with the EF Core provider and the work store of its server.</summary>
public abstract class TestDatabase
{
    public string ConnectionString { get; protected set; } = "";

    /// <summary>Points a context at the database.</summary>
    public abstract void Configure(DbContextOptionsBuilder builder);

    public abstract void AddWorkStore(IServiceCollection services);

    public abstract void AddSignals(IServiceCollection services, string application);
}

public sealed class PostgreSqlTestDatabase(PostgreSqlServer server) : TestDatabase, IAsyncLifetime
{
    private NpgsqlDataSource dataSource = null!;

    public async ValueTask InitializeAsync()
    {
        ConnectionString = await server.CreateDatabaseAsync();
        dataSource = NpgsqlDataSource.Create(ConnectionString);
    }

    public ValueTask DisposeAsync() => dataSource.DisposeAsync();

    public override void Configure(DbContextOptionsBuilder builder) => builder.UseNpgsql(ConnectionString);

    public override void AddWorkStore(IServiceCollection services) => services.AddPostgreSqlWorkStore(dataSource);

    public override void AddSignals(IServiceCollection services, string application) => services.AddPostgreSqlSignals(dataSource, application);
}

public sealed class SqlServerTestDatabase(SqlServerInstance server) : TestDatabase, IAsyncLifetime
{
    public async ValueTask InitializeAsync() => ConnectionString = await server.CreateDatabaseAsync();

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public override void Configure(DbContextOptionsBuilder builder) => builder.UseSqlServer(ConnectionString);

    public override void AddWorkStore(IServiceCollection services) => services.AddSqlServerWorkStore(ConnectionString);

    public override void AddSignals(IServiceCollection services, string application) => services.AddSqlServerSignals(ConnectionString, application);
}
