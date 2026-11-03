using System.Data;
using Corfedo.Work.SqlServer.Tests.Provisioning;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.SqlServer.Tests.Jobs;

/// <summary>An operation whose token is cancelled while SQL Server runs its statement ends as cancelled.</summary>
public sealed class CancellationTests(SqlServerDatabase database) : IClassFixture<SqlServerDatabase>, IAsyncLifetime
{
    private static readonly JobDefinition<string> Definition = JobDefinition.Create("cancellation.feed", 1, new TextArgs(), null, subject: "feed");

    private ServiceProvider services = null!;

    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    private JobOperations Jobs => services.GetRequiredService<JobOperations>();

    public async ValueTask InitializeAsync()
    {
        var collection = new ServiceCollection();
        collection.AddSqlServerWorkStore(database.ConnectionString);
        collection.AddWork(work => work.AddJobDefinition(Definition));
        services = collection.BuildServiceProvider();
        await services.GetRequiredService<IWorkStore>().MigrateAsync(Cancel);
    }

    public async ValueTask DisposeAsync() => await services.DisposeAsync();

    [Fact]
    public async Task AStatementCancelledWhileWaitingForALockThrowsOperationCanceled()
    {
        var subject = Subject.Of("feed", "npm");
        var hourly = Schedule.Every(TimeSpan.FromHours(1), Interval.FixedRate, DateTimeOffset.UnixEpoch);
        await using (var connection = await OpenAsync())
        await using (var setup = await BeginAsync(connection))
        {
            await Jobs.ScheduleAsync(setup, Definition, "s", hourly, new RecurrenceOptions { Subject = subject }, Cancel);
            await setup.CommitAsync(Cancel);
        }

        // Another transaction holds the recurrences table, so removing the recurrence waits for its lock.
        await using var holding = await OpenAsync();
        await using var holder = await BeginAsync(holding);
        await using (var hold = new SqlCommand("SELECT COUNT(*) FROM work.job_recurrences WITH (TABLOCKX, HOLDLOCK);", holding, holder))
        {
            await hold.ExecuteScalarAsync(Cancel);
        }
        await using var calling = await OpenAsync();
        await using var caller = await BeginAsync(calling);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));

        var cancelled = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Jobs.RemoveAsync(caller, Definition, subject, cancellation.Token).AsTask());
        Assert.Equal(cancellation.Token, cancelled.CancellationToken);
    }

    private async Task<SqlConnection> OpenAsync()
    {
        var connection = new SqlConnection(database.ConnectionString);
        await connection.OpenAsync(Cancel);
        return connection;
    }

    private static async Task<SqlTransaction> BeginAsync(SqlConnection connection) =>
        (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, Cancel);

    private sealed class TextArgs : IJobArgs<string>
    {
        public int Version => 1;

        public byte[] Write(string args) => System.Text.Encoding.UTF8.GetBytes(args);

        public string Read(ReadOnlySpan<byte> data, int version) => System.Text.Encoding.UTF8.GetString(data);
    }
}
