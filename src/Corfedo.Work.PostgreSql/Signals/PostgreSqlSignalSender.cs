using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace Corfedo.Work.PostgreSql.Signals;

/// <summary>
/// Sends the signals of transactions after they commit, in batches. A transaction that notifies takes a database-wide
/// lock at commit and holds it until its commit is flushed to disk, so notifying transactions commit one at a time and
/// without group commit. Instead, a signal sent in a transaction is recorded with the transaction's id, and one
/// statement per batch notifies the distinct payloads of committed transactions, drops those of rolled-back ones and
/// returns the ones still running, which are checked again later. Batches start at least <see cref="Interval"/>
/// apart, so a process takes the lock about once per interval however many transactions signal. The transaction's
/// status is all the sender needs, so it also sends for transactions the application commits itself.
/// <para>
/// A transaction id does not tell a rollback to a savepoint apart, so a signal sent after a savepoint that is rolled
/// back to is still sent if the transaction commits, as a spurious wake. Signals recorded in a process that crashes
/// before sending them are lost; subscribers poll as a fallback.
/// </para>
/// </summary>
internal sealed partial class PostgreSqlSignalSender(NpgsqlDataSource dataSource, PostgreSqlSignalSql sql, ILogger logger, WorkTelemetryOptions telemetry) : IAsyncDisposable
{
    /// <summary>The shortest time between the starts of two batches, and the delay before a signal is first checked.</summary>
    internal static readonly TimeSpan Interval = TimeSpan.FromMilliseconds(2);

    /// <summary>The longest wait between two checks of a transaction that is still running; the wait doubles from <see cref="Interval"/>.</summary>
    internal static readonly TimeSpan MaximumRecheck = TimeSpan.FromSeconds(1);

    private readonly Lock gate = new();
    private readonly SemaphoreSlim wake = new(0);
    private List<Entry> pending = [];
    private Task loop = Task.CompletedTask;
    private bool running;
    private bool stopping;

    // The time the loop sleeps until, or 0 while it is not sleeping; a signal due earlier wakes it.
    private long sleepingUntil;

    public PostgreSqlSignalSql Sql => sql;

    /// <summary>Records a signal of the open <paramref name="transaction"/>, sent once it commits; a rollback discards it.</summary>
    public async Task SendAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, SignalTopic topic, string key, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("SELECT pg_current_xact_id()::text::bigint", connection, transaction);
        var xid = (long)(await command.ExecuteScalarAsync(cancellationToken))!;
        Add(xid, PostgreSqlSignalSql.Payload(topic, key));
    }

    /// <summary>Executes a batch whose statements may return signal rows, recording the signals.</summary>
    public async Task ExecuteAsync(NpgsqlBatch batch, CancellationToken cancellationToken)
    {
        await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
        await ReadAsync(reader, cancellationToken);
    }

    /// <summary>Executes a command that may return signal rows, recording the signals.</summary>
    public async Task ExecuteAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await ReadAsync(reader, cancellationToken);
    }

    /// <summary>
    /// Reads the remaining row sets of <paramref name="reader"/>, the current one included, and records the signal rows
    /// among them. Row sets of other statements are skipped, so a caller reads its own first.
    /// </summary>
    public async Task ReadAsync(NpgsqlDataReader reader, CancellationToken cancellationToken)
    {
        do
        {
            if (reader.FieldCount == 2 && reader.GetName(0) == PostgreSqlSignalSql.RowMarker)
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    Add(reader.GetInt64(0), reader.GetString(1));
                }
            }
        }
        while (await reader.NextResultAsync(cancellationToken));
    }

    /// <summary>Sends the recorded signals of committed transactions once more without waiting, and drops the rest.</summary>
    public async ValueTask DisposeAsync()
    {
        Task current;
        lock (gate)
        {
            stopping = true;
            current = loop;
        }
        wake.Release();
        await current.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }

    private void Add(long xid, string payload)
    {
        var due = Stopwatch.GetTimestamp() + Ticks(Interval);
        lock (gate)
        {
            pending.Add(new Entry(xid, payload, due, Interval));
            if (!running)
            {
                running = true;
                loop = Task.Run(RunAsync);
            }
            else if (sleepingUntil > due)
            {
                sleepingUntil = 0;
                wake.Release();
            }
        }
    }

    private async Task RunAsync()
    {
        var lastStart = 0L;
        while (true)
        {
            List<Entry>? batch = null;
            TimeSpan sleep;
            lock (gate)
            {
                if (pending.Count == 0)
                {
                    running = false;
                    return;
                }
                var now = Stopwatch.GetTimestamp();
                var start = stopping ? now : Math.Max(pending.Min(e => e.Due), lastStart + Ticks(Interval));
                if (start <= now)
                {
                    batch = pending;
                    pending = [];
                    sleepingUntil = 0;
                    sleep = TimeSpan.Zero;
                }
                else
                {
                    sleepingUntil = start;
                    // The wait counts whole milliseconds; rounding down would spin until the start.
                    sleep = TimeSpan.FromMilliseconds(Math.Ceiling(Stopwatch.GetElapsedTime(now, start).TotalMilliseconds));
                }
            }
            if (batch is null)
            {
                await wake.WaitAsync(sleep);
                continue;
            }
            lastStart = Stopwatch.GetTimestamp();
            var unfinished = await FlushAsync(batch);
            if (unfinished.Count > 0)
            {
                lock (gate)
                {
                    if (!stopping)
                    {
                        pending.AddRange(unfinished);
                    }
                }
            }
        }
    }

    /// <summary>
    /// Notifies the distinct payloads of committed transactions in one statement and returns the entries whose
    /// transactions are still running, due again after twice their wait. Each status is read once, so a transaction
    /// that commits while the statement runs is either notified or returned. A failed statement drops the batch: the
    /// listeners' connections are likely broken as well, and they resync when they reconnect.
    /// </summary>
    private async Task<List<Entry>> FlushAsync(List<Entry> batch)
    {
        var unfinished = new List<Entry>();
        try
        {
            await using var command = dataSource.CreateCommand(
                "WITH b AS MATERIALIZED (SELECT b.i, b.p, pg_xact_status(b.x::text::xid8) AS s FROM unnest($2::bigint[], $3::text[]) WITH ORDINALITY AS b(x, p, i)), "
                + "n AS MATERIALIZED (SELECT count(pg_notify($1, d.p)) FROM (SELECT DISTINCT b.p FROM b WHERE b.s = 'committed') AS d) "
                + "SELECT array(SELECT b.i::int4 FROM b WHERE b.s = 'in progress') FROM n");
            command.Parameters.Add(sql.ChannelParameter());
            command.Parameters.Add(new NpgsqlParameter<long[]> { TypedValue = [.. batch.Select(e => e.Xid)], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint });
            command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = [.. batch.Select(e => e.Payload)], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text });
            var indexes = (int[])(await command.ExecuteScalarAsync())!;
            var now = Stopwatch.GetTimestamp();
            foreach (var index in indexes)
            {
                var entry = batch[index - 1];
                var wait = TimeSpan.FromTicks(Math.Min(entry.Wait.Ticks * 2, MaximumRecheck.Ticks));
                unfinished.Add(entry with { Due = now + Ticks(wait), Wait = wait });
            }
        }
        catch (Exception exception)
        {
            LogSendFailed(logger, batch.Count, telemetry.Logged(exception), telemetry.Describe(exception));
        }
        return unfinished;
    }

    private static long Ticks(TimeSpan span) => (long)(span.TotalSeconds * Stopwatch.Frequency);

    private readonly record struct Entry(long Xid, string Payload, long Due, TimeSpan Wait);

    [LoggerMessage(EventId = 49, EventName = "signals.send_failed", Level = LogLevel.Warning, Message = "Sending {Count} work signals failed; they are dropped: {Error}")]
    private static partial void LogSendFailed(ILogger logger, int count, Exception? exception, string error);
}
