using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace Corfedo.Work.SqlServer.Signals;

/// <summary>
/// Signals over the application's signals table: a send inserts a row in the sending transaction, so rolled-back
/// changes wake nobody. A rowversion is assigned when a row is written, not when its transaction commits, so the
/// listener reads only below <c>MIN_ACTIVE_ROWVERSION()</c>, where every row is committed or gone, and keeps its
/// position there. It holds one connection that runs a server-side loop: a batch returns as soon as committed rows
/// past the position exist, or after <see cref="SqlServerSignalOptions.MaximumWait"/>, waiting
/// <see cref="SqlServerSignalOptions.PollInterval"/> between checks. An open transaction that wrote any rowversion
/// column of the database holds the watermark back, which delays later signals until it ends. The listener also deletes
/// rows older than the retention; one that has not read for longer than the retention may have missed pruned rows, so
/// it resyncs.
/// </summary>
internal sealed partial class SqlServerSignalTransport(string connectionString, SqlServerSignalSql sql, SqlServerSignalOptions options, TimeProvider time, ILogger logger, WorkTelemetryOptions telemetry) : ISignalTransport
{
    private static readonly TimeSpan Reconnect = TimeSpan.FromSeconds(1);

    /// <summary>Rows one pruning statement deletes at most.</summary>
    private const int PruneBatch = 1000;

    private volatile bool ensured;

    public async ValueTask SendAsync(WorkTransaction? transaction, SignalTopic topic, string key, CancellationToken cancellationToken)
    {
        if (transaction is not null)
        {
            var work = SqlServerTransaction.From(transaction);
            await sql.SendAsync(work.Connection, work.Transaction, topic, key, cancellationToken);
            return;
        }
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await EnsureTableAsync(connection, cancellationToken);
        await sql.SendAsync(connection, null, topic, key, cancellationToken);
    }

    public async Task ListenAsync(Action<SignalTopic, string> received, Action connected, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(received);
        ArgumentNullException.ThrowIfNull(connected);
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await using var connection = new SqlConnection(connectionString);
                await connection.OpenAsync(cancellationToken);
                await EnsureTableAsync(connection, cancellationToken);
                byte[] position;
                await using (var start = Tsql.Command(connection, null, "SELECT CAST(MIN_ACTIVE_ROWVERSION() AS binary(8));"))
                {
                    position = (byte[])(await start.ExecuteScalarAsync(cancellationToken))!;
                }
                connected();
                var lastRead = time.GetTimestamp();
                var lastPruned = lastRead;
                while (true)
                {
                    if (time.GetElapsedTime(lastRead) > options.Retention)
                    {
                        connected();
                    }
                    position = await PollAsync(connection, position, received, cancellationToken);
                    lastRead = time.GetTimestamp();
                    if (time.GetElapsedTime(lastPruned) >= options.PruneInterval)
                    {
                        await PruneAsync(connection, cancellationToken);
                        lastPruned = time.GetTimestamp();
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                LogListenFailed(logger, telemetry.Logged(exception), telemetry.Describe(exception));
                await Task.Delay(Reconnect, time, cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
        }
    }

    /// <summary>
    /// Waits on the server until committed rows at or past <paramref name="from"/> exist below the watermark, or the
    /// maximum wait passes, delivers them in write order and returns the watermark as the next position.
    /// </summary>
    private async Task<byte[]> PollAsync(SqlConnection connection, byte[] from, Action<SignalTopic, string> received, CancellationToken cancellationToken)
    {
        await using var command = Tsql.Command(connection, null, $"""
            DECLARE @until datetime2(7) = DATEADD(millisecond, @wait, SYSUTCDATETIME()), @top binary(8);
            WHILE 1 = 1
            BEGIN
                SET @top = MIN_ACTIVE_ROWVERSION();
                IF EXISTS (SELECT 1 FROM {sql.Table} WHERE rv >= @from AND rv < @top) OR SYSUTCDATETIME() > @until BREAK;
                WAITFOR DELAY @poll;
            END
            SELECT @top;
            SELECT topic, k FROM {sql.Table} WHERE rv >= @from AND rv < @top ORDER BY rv;
            """,
            new SqlParameter("@from", SqlDbType.Binary, 8) { Value = from },
            Tsql.Int32("@wait", (int)options.MaximumWait.TotalMilliseconds),
            new SqlParameter("@poll", SqlDbType.VarChar, 12) { Value = options.PollInterval.ToString(@"hh\:mm\:ss\.fff", System.Globalization.CultureInfo.InvariantCulture) });
        command.CommandTimeout = (int)Math.Ceiling(options.MaximumWait.TotalSeconds) + 30;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        var top = reader.GetFieldValue<byte[]>(0);
        await reader.NextResultAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (SignalTopic.IsValidName(reader.GetString(0)))
            {
                received(new SignalTopic(reader.GetString(0)), reader.GetString(1));
            }
        }
        return top;
    }

    /// <summary>Deletes signals older than the retention, skipping rows of open transactions.</summary>
    private async Task PruneAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = Tsql.Command(connection, null,
            $"DELETE TOP ({PruneBatch}) FROM {sql.Table} WITH (READPAST) WHERE sent_at < DATEADD(millisecond, -@retention, SYSUTCDATETIME());",
            Tsql.Int32("@retention", (int)Math.Min(options.Retention.TotalMilliseconds, int.MaxValue)));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Creates the signals table when an application signals without a store, whose framework schema creates it
    /// otherwise.
    /// </summary>
    private async Task EnsureTableAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        if (ensured)
        {
            return;
        }
        await using var command = Tsql.Command(connection, null, Schema.FrameworkSchema.EnsureSignals(sql.Schema));
        await command.ExecuteNonQueryAsync(cancellationToken);
        ensured = true;
    }

    [LoggerMessage(EventId = 3, EventName = "signals.listen_failed", Level = LogLevel.Warning, Message = "Listening for work signals failed; reconnecting: {Error}")]
    private static partial void LogListenFailed(ILogger logger, Exception? exception, string error);
}

/// <summary>How the SQL Server signal listener polls its signals table and how long signals are kept.</summary>
public sealed record SqlServerSignalOptions
{
    /// <summary>How often the listener's server-side loop checks for committed signals; the wake latency of a signal.</summary>
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(10);

    /// <summary>How long one listening batch waits on the server before it returns without signals.</summary>
    public TimeSpan MaximumWait { get; init; } = TimeSpan.FromSeconds(2);

    /// <summary>How long signals stay in the table; a listener that has not read for longer resyncs.</summary>
    public TimeSpan Retention { get; init; } = TimeSpan.FromMinutes(1);

    /// <summary>How often each listener deletes signals older than the retention.</summary>
    public TimeSpan PruneInterval { get; init; } = TimeSpan.FromSeconds(10);

    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(PollInterval, TimeSpan.FromMilliseconds(1), nameof(PollInterval));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(PollInterval, TimeSpan.FromSeconds(1), nameof(PollInterval));
        ArgumentOutOfRangeException.ThrowIfLessThan(MaximumWait, PollInterval, nameof(MaximumWait));
        ArgumentOutOfRangeException.ThrowIfLessThan(Retention, MaximumWait * 2, nameof(Retention));
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(PruneInterval, TimeSpan.Zero, nameof(PruneInterval));
    }
}
