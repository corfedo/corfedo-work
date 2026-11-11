using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Corfedo.Work.PostgreSql.Tests.Registration;

/// <summary>A store provides the process's signals, so signals registered alone cannot be combined with it.</summary>
public sealed class SignalRegistrationOrderTests
{
    private static readonly NpgsqlDataSource DataSource = NpgsqlDataSource.Create("Host=unused.invalid");

    [Fact]
    public void SignalsAfterTheStoreAreRefused()
    {
        var services = new ServiceCollection().AddPostgreSqlWorkStore(DataSource);

        var error = Assert.Throws<InvalidOperationException>(() => services.AddPostgreSqlSignals(DataSource, "work"));
        Assert.Contains("AddPostgreSqlSignals", error.Message);
    }

    [Fact]
    public void TheStoreAfterSignalsIsRefused()
    {
        var services = new ServiceCollection().AddPostgreSqlSignals(DataSource, "work");

        var error = Assert.Throws<InvalidOperationException>(() => services.AddPostgreSqlWorkStore(DataSource));
        Assert.Contains("AddPostgreSqlSignals", error.Message);
    }
}
