using Corfedo.Work.SqlServer.Tests.Provisioning;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Tests.Transactions;

/// <summary>A SqlClient transaction converts to the work transaction handle operations take.</summary>
public sealed class TransactionConversionTests(SqlServerDatabase database) : IClassFixture<SqlServerDatabase>
{
    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    [Fact]
    public async Task EveryConversionOfATransactionGivesOneHandleOverIt()
    {
        await using var connection = new SqlConnection(database.ConnectionString);
        await connection.OpenAsync(Cancel);
        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(Cancel);

        WorkTransaction first = transaction;
        WorkTransaction second = transaction;

        Assert.Same(first, second);
        Assert.Same(transaction, first.SqlServer());
    }
}
