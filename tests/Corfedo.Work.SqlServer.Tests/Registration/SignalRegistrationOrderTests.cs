using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.SqlServer.Tests.Registration;

/// <summary>A store provides the process's signals, so signals registered alone cannot be combined with it.</summary>
public sealed class SignalRegistrationOrderTests
{
    private const string ConnectionString = "Server=unused.invalid";

    [Fact]
    public void SignalsAfterTheStoreAreRefused()
    {
        var services = new ServiceCollection().AddSqlServerWorkStore(ConnectionString);

        var error = Assert.Throws<InvalidOperationException>(() => services.AddSqlServerSignals(ConnectionString, "work"));
        Assert.Contains("AddSqlServerSignals", error.Message);
    }

    [Fact]
    public void TheStoreAfterSignalsIsRefused()
    {
        var services = new ServiceCollection().AddSqlServerSignals(ConnectionString, "work");

        var error = Assert.Throws<InvalidOperationException>(() => services.AddSqlServerWorkStore(ConnectionString));
        Assert.Contains("AddSqlServerSignals", error.Message);
    }
}
