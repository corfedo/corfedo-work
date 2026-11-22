using System.Data.Common;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Corfedo.Work.PostgreSql.Signals;

/// <summary>
/// Signals over <c>NOTIFY</c>. A signal sent in a transaction is recorded with it and sent by
/// <see cref="PostgreSqlSignalSender"/> once the transaction has committed, so rolled-back changes wake nobody; one
/// sent without a transaction is notified at once. The listener holds a dedicated session-level connection of the listener data source that never runs a transaction, since a listener
/// inside a long transaction holds back the notification queue and, once it is full, notifying commits fail. A
/// connection quiet for <see cref="Probe"/> is checked with a query, so a silently broken connection is replaced
/// instead of losing signals until the process restarts. A listener data source of its own that reaches another
/// database than the store's receives none of the store's signals, which <c>NOTIFY</c> sends per database; the first
/// connection compares the two and logs a warning.
/// </summary>
internal sealed partial class PostgreSqlSignalTransport(NpgsqlDataSource dataSource, NpgsqlDataSource listener, PostgreSqlSignalSender sender, TimeProvider time, ILogger logger, WorkTelemetryOptions telemetry) : ISignalTransport, IAsyncDisposable
{
    private readonly PostgreSqlSignalSql sql = sender.Sql;

    private static readonly TimeSpan Reconnect = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan Probe = TimeSpan.FromSeconds(30);

    private bool compared;

    public async ValueTask SendAsync(WorkTransaction? transaction, SignalTopic topic, string key, CancellationToken cancellationToken)
    {
        if (transaction is not null)
        {
            var work = PostgreSqlTransaction.From(transaction);
            await sender.SendAsync(work.Connection, work.Transaction, topic, key, cancellationToken);
            return;
        }
        await using var command = dataSource.CreateCommand("SELECT pg_notify($1, $2)");
        command.Parameters.Add(sql.ChannelParameter());
        command.Parameters.Add(PostgreSqlSignalSql.PayloadParameter(topic, key));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task ListenAsync(Action<SignalTopic, string> received, Action connected, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(received);
        ArgumentNullException.ThrowIfNull(connected);
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await using var connection = await listener.OpenConnectionAsync(cancellationToken);
                connection.Notification += (_, e) =>
                {
                    if (e.Channel == sql.Channel && PostgreSqlSignalSql.TryParse(e.Payload, out var topic, out var key))
                    {
                        received(topic, key);
                    }
                };
                if (!compared && !ReferenceEquals(listener, dataSource))
                {
                    await CompareDatabasesAsync(connection, cancellationToken);
                }
                await using (var listen = new NpgsqlCommand("LISTEN " + Leases.LeaseSql.Quote(sql.Channel), connection))
                {
                    await listen.ExecuteNonQueryAsync(cancellationToken);
                }
                connected();
                while (true)
                {
                    if (!await connection.WaitAsync(Probe, cancellationToken))
                    {
                        await using var probe = new NpgsqlCommand("SELECT 1", connection);
                        await probe.ExecuteNonQueryAsync(cancellationToken);
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

    /// <summary>Warns once when the listener's database is not the store's.</summary>
    private async Task CompareDatabasesAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        const string text = "SELECT current_database()";
        await using var listened = new NpgsqlCommand(text, connection);
        var listening = (string?)await listened.ExecuteScalarAsync(cancellationToken);
        await using var stored = dataSource.CreateCommand(text);
        var storing = (string?)await stored.ExecuteScalarAsync(cancellationToken);
        compared = true;
        if (!string.Equals(listening, storing, StringComparison.Ordinal))
        {
            LogOtherDatabase(logger, listening, storing);
        }
    }

    /// <summary>Sends the signals of transactions that have committed before the process stops.</summary>
    public ValueTask DisposeAsync() => sender.DisposeAsync();

    [LoggerMessage(EventId = 3, EventName = "signals.listen_failed", Level = LogLevel.Warning, Message = "Listening for work signals failed; reconnecting: {Error}")]
    private static partial void LogListenFailed(ILogger logger, Exception? exception, string error);

    [LoggerMessage(EventId = 50, EventName = "signals.listener_database_differs", Level = LogLevel.Warning, Message = "The signal listener reaches database {Listener}, the store database {Store}; no signals of the store arrive and work is found only by polling")]
    private static partial void LogOtherDatabase(ILogger logger, string? listener, string? store);
}
