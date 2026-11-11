using Corfedo.Work.PostgreSql.Tests.Provisioning;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Corfedo.Work.PostgreSql.Tests.Registration;

/// <summary>Where the store and the signals get their data source, and who disposes it.</summary>
public sealed class DataSourceRegistrationTests(PostgreSqlDatabase database) : IClassFixture<PostgreSqlDatabase>
{
    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    private static string Unreachable => new NpgsqlConnectionStringBuilder { Host = "unreachable.invalid" }.ConnectionString;

    [Fact]
    public async Task TheStoreUsesTheContainersDataSourceAndTheContainerDisposesIt()
    {
        var collection = new ServiceCollection();
        collection.AddSingleton(_ => NpgsqlDataSource.Create(database.ConnectionString));
        collection.AddPostgreSqlWorkStore("registration_plain");
        var services = collection.BuildServiceProvider();
        var source = services.GetRequiredService<NpgsqlDataSource>();

        await services.GetRequiredService<IWorkStore>().PrepareAsync(Cancel);
        await services.DisposeAsync();

        Assert.True(await SchemaExistsAsync("registration_plain"));
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await source.OpenConnectionAsync(Cancel));
    }

    [Fact]
    public async Task TheStoreUsesTheDataSourceRegisteredUnderItsKey()
    {
        var collection = new ServiceCollection();
        collection.AddSingleton(_ => NpgsqlDataSource.Create(Unreachable));
        collection.AddKeyedSingleton("work", (_, _) => NpgsqlDataSource.Create(database.ConnectionString));
        collection.AddPostgreSqlWorkStore("registration_keyed", dataSourceKey: "work");
        await using var services = collection.BuildServiceProvider();

        await services.GetRequiredService<IWorkStore>().PrepareAsync(Cancel);

        Assert.True(await SchemaExistsAsync("registration_keyed"));
    }

    [Fact]
    public void AMissingDataSourceFailsWhenTheStoreIsCreated()
    {
        var collection = new ServiceCollection();
        collection.AddPostgreSqlWorkStore(dataSourceKey: "work");
        using var services = collection.BuildServiceProvider();

        var error = Assert.Throws<InvalidOperationException>(() => services.GetRequiredService<IWorkStore>());
        Assert.Contains("'work'", error.Message);
    }

    [Fact]
    public async Task TheFactoryIsCalledOnceForTheStoreAndItsSignals()
    {
        var calls = 0;
        var collection = new ServiceCollection();
        collection.AddPostgreSqlWorkStore(_ =>
        {
            calls++;
            return database.DataSource;
        }, "registration_factory");
        await using var services = collection.BuildServiceProvider();

        services.GetRequiredService<IWorkStore>();
        services.GetRequiredService<IWorkSignals>();

        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task AGivenDataSourceIsNotDisposedWithTheContainer()
    {
        await using var source = NpgsqlDataSource.Create(database.ConnectionString);
        var collection = new ServiceCollection();
        collection.AddPostgreSqlWorkStore(source, "registration_given");
        var services = collection.BuildServiceProvider();
        services.GetRequiredService<IWorkSignals>();

        await services.DisposeAsync();

        await using var connection = await source.OpenConnectionAsync(Cancel);
    }

    [Fact]
    public async Task SignalsUseTheContainersDataSource()
    {
        var collection = new ServiceCollection();
        collection.AddKeyedSingleton("signals", (_, _) => NpgsqlDataSource.Create(database.ConnectionString));
        collection.AddPostgreSqlSignals("registration_signals", dataSourceKey: "signals");
        var services = collection.BuildServiceProvider();
        var source = services.GetRequiredKeyedService<NpgsqlDataSource>("signals");

        await services.GetRequiredService<IWorkSignals>().SendAsync(new SignalTopic("registration.sent"), "1", Cancel);
        await services.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await source.OpenConnectionAsync(Cancel));
    }

    private async Task<bool> SchemaExistsAsync(string schema)
    {
        await using var command = database.DataSource.CreateCommand("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)");
        command.Parameters.Add(new NpgsqlParameter { Value = schema });
        return (bool)(await command.ExecuteScalarAsync(Cancel))!;
    }
}
