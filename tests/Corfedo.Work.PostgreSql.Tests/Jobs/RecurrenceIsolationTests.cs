using System.Data;
using Corfedo.Work.PostgreSql.Tests.Provisioning;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;

namespace Corfedo.Work.PostgreSql.Tests.Jobs;

/// <summary>
/// Recurrence operations in the caller's transaction find the recurrence's occurrences after their locks, so a snapshot
/// taken before the planner committed one must not be used to find them.
/// </summary>
public sealed class RecurrenceIsolationTests(PostgreSqlDatabase database) : IClassFixture<PostgreSqlDatabase>, IAsyncLifetime
{
    private static readonly JobDefinition<string> Definition = JobDefinition.Create("isolation.feed", 1, new TextArgs(), null, subject: "feed");

    private static readonly Schedule Hourly = Schedule.Every(TimeSpan.FromHours(1), Interval.FixedRate, DateTimeOffset.UnixEpoch.AddMinutes(30));

    private ServiceProvider services = null!;

    public enum Operation
    {
        Schedule,
        Remove,
        Trigger,
    }

    private static CancellationToken Cancel => TestContext.Current.CancellationToken;

    private JobOperations Jobs => services.GetRequiredService<JobOperations>();

    public async ValueTask InitializeAsync()
    {
        var collection = new ServiceCollection();
        collection.AddPostgreSqlWorkStore(database.DataSource);
        collection.AddWork(work => work.AddJobDefinition(Definition));
        services = collection.BuildServiceProvider();
        foreach (var hosted in services.GetServices<IHostedService>())
        {
            await hosted.StartAsync(Cancel);
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var hosted in services.GetServices<IHostedService>())
        {
            await hosted.StopAsync(CancellationToken.None);
        }
        await services.DisposeAsync();
    }

    [Theory]
    [InlineData(Operation.Schedule, IsolationLevel.RepeatableRead)]
    [InlineData(Operation.Remove, IsolationLevel.RepeatableRead)]
    [InlineData(Operation.Trigger, IsolationLevel.RepeatableRead)]
    [InlineData(Operation.Schedule, IsolationLevel.Serializable)]
    [InlineData(Operation.Remove, IsolationLevel.Serializable)]
    [InlineData(Operation.Trigger, IsolationLevel.Serializable)]
    public async Task SnapshotIsolatedRecurrenceOperationIsRefusedBeforeItChangesAnything(Operation operation, IsolationLevel level)
    {
        var subject = Subject.Of("feed", $"{operation}-{level}");
        if (operation != Operation.Schedule)
        {
            await InTransactionAsync(t => Jobs.ScheduleAsync(t, Definition, "s", Hourly, new RecurrenceOptions { Subject = subject }).AsTask());
        }
        await using var connection = await database.DataSource.OpenConnectionAsync(Cancel);
        await using var transaction = await connection.BeginTransactionAsync(level, Cancel);
        await SnapshotAsync(connection, transaction);
        var before = await StateAsync(subject);

        var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => RunAsync(operation, transaction, subject));
        Assert.Contains("READ COMMITTED", refused.Message, StringComparison.Ordinal);
        await transaction.RollbackAsync(Cancel);

        Assert.Equal(before, await StateAsync(subject));
        Assert.Equal((operation == Operation.Schedule ? 0L : 1L, 0L), before);
    }

    private Task RunAsync(Operation operation, WorkTransaction transaction, Subject subject) => operation switch
    {
        Operation.Schedule => Jobs.ScheduleAsync(transaction, Definition, "s", Hourly, new RecurrenceOptions { Subject = subject }).AsTask(),
        Operation.Remove => Jobs.RemoveAsync(transaction, Definition, subject).AsTask(),
        _ => Jobs.TriggerAsync(transaction, Definition, subject).AsTask(),
    };

    /// <summary>An earlier read in the caller's transaction, which fixes a snapshot-isolated transaction's snapshot.</summary>
    private static async Task SnapshotAsync(NpgsqlConnection connection, NpgsqlTransaction transaction)
    {
        await using var command = new NpgsqlCommand("SELECT count(*) FROM work.jobs", connection, transaction);
        await command.ExecuteScalarAsync(Cancel);
    }

    private async Task InTransactionAsync(Func<WorkTransaction, Task> operation)
    {
        await using var connection = await database.DataSource.OpenConnectionAsync(Cancel);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, Cancel);
        await operation(transaction);
        await transaction.CommitAsync(Cancel);
    }

    /// <summary>The subject's recurrences and jobs.</summary>
    private async Task<(long Recurrences, long Jobs)> StateAsync(Subject subject)
    {
        await using var command = database.DataSource.CreateCommand(
            "SELECT (SELECT count(*) FROM work.job_recurrences WHERE definition = $1 AND subject_kind = $2 AND subject_id = $3), "
            + "(SELECT count(*) FROM work.jobs WHERE definition = $1 AND subject_kind = $2 AND subject_id = $3)");
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = Definition.Key });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = subject.Kind });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = subject.Id });
        await using var reader = await command.ExecuteReaderAsync(Cancel);
        await reader.ReadAsync(Cancel);
        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    private sealed class TextArgs : IJobArgs<string>
    {
        public int Version => 1;

        public byte[] Write(string args) => System.Text.Encoding.UTF8.GetBytes(args);

        public string Read(ReadOnlySpan<byte> data, int version) => System.Text.Encoding.UTF8.GetString(data);
    }
}
