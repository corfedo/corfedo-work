using Corfedo.Work.PostgreSql.Tests.Provisioning;

namespace Corfedo.Work.PostgreSql.Tests.Transactions;

/// <summary>An Npgsql transaction converts to the work transaction handle operations take.</summary>
public sealed class TransactionConversionTests(PostgreSqlDatabase database) : IClassFixture<PostgreSqlDatabase>
{
    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task EveryConversionOfATransactionGivesOneHandleOverIt()
    {
        await using var connection = await database.DataSource.OpenConnectionAsync(Cancel);
        await using var transaction = await connection.BeginTransactionAsync(Cancel);

        WorkTransaction first = transaction;
        WorkTransaction second = transaction;

        Assert.Same(first, second);
        Assert.Same(transaction, first.Npgsql());
    }
}
