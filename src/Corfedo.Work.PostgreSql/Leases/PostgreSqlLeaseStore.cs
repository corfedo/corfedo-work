using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Npgsql;
using Corfedo.Work.PostgreSql.Signals;
using NpgsqlTypes;

namespace Corfedo.Work.PostgreSql.Leases;

internal sealed partial class PostgreSqlLeaseStore(NpgsqlDataSource dataSource, string schema, PostgreSqlSignalSender signals, ILogger logger, WorkTelemetryOptions telemetry) : ILeaseStore
{
    private readonly ConcurrentDictionary<object, LeaseSql> statements = new();

    internal LeaseSql Sql<TKey>(LeaseSource<TKey> source)
        where TKey : notnull =>
        statements.GetOrAdd(source, static (_, s) => new LeaseSql(s.Source.Name, s.Source.Store, s.Source.Key.Fields, s.Source.Partition, s.Source.Policy, s.Source.Fields, s.Source.Settles, s.Source.Reconcile, s.Source.Job, s.Schema), (Source: source, Schema: schema));

    internal static void AddKey<TKey>(NpgsqlCommand command, LeaseSource<TKey> source, TKey key)
        where TKey : notnull => source.Key.Write(key, new ParameterKeyWriter(command.Parameters));

    internal static void AddKeys<TKey>(NpgsqlParameterCollection parameters, LeaseSource<TKey> source, IEnumerable<TKey> keys)
        where TKey : notnull
    {
        var writer = new ArrayKeyWriter(source.Key.Fields);
        foreach (var key in keys)
        {
            source.Key.Write(key, writer);
        }
        writer.AddTo(parameters);
    }

    internal static TKey ReadKey<TKey>(LeaseSource<TKey> source, NpgsqlDataReader reader)
        where TKey : notnull => source.Key.Read(new RowKeyReader(reader, 0));

    internal static NpgsqlParameter<Guid> Uuid(Guid value) => new() { TypedValue = value };

    internal static NpgsqlParameter<long> Int64(long value) => new() { TypedValue = value };

    internal static NpgsqlParameter<int> Int32(int value) => new() { TypedValue = value };

    internal static NpgsqlParameter<bool> Bool(bool value) => new() { TypedValue = value };

    internal static NpgsqlParameter<TimeSpan> Interval(TimeSpan value) => new() { TypedValue = value, NpgsqlDbType = NpgsqlDbType.Interval };

    internal static NpgsqlParameter<string> Text(string value) => new() { TypedValue = value, NpgsqlDbType = NpgsqlDbType.Text };

    internal static TimeSpan Seconds(double seconds) => seconds <= 0 ? TimeSpan.Zero : TimeSpan.FromSeconds(seconds);

    /// <summary>The application's signal sender, for statements that signal.</summary>
    internal PostgreSqlSignalSender Signals => signals;

    /// <summary>The payload parameter of a statement's signal rows.</summary>
    internal static NpgsqlParameter Signal(SignalTopic topic, string key) => PostgreSqlSignalSql.PayloadParameter(topic, key);

    /// <summary>Signals listeners when the transaction commits; a rollback discards the signal.</summary>
    internal Task NotifyAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, SignalTopic topic, string key, CancellationToken cancellationToken) =>
        signals.SendAsync(connection, transaction, topic, key, cancellationToken);

    /// <summary>The SQLSTATE of a generation fence refused for the transaction's isolation level.</summary>
    private const string FenceRefused = "CWF01";

    /// <summary>The framework schema, which holds generations and the fence function.</summary>
    internal string Schema => schema;

    /// <summary>
    /// Executes a batch that starts with a generation fence; a fence refused for the transaction's isolation level is
    /// reported as <see cref="InvalidOperationException"/>, and the transaction cannot commit.
    /// </summary>
    internal static async Task<NpgsqlDataReader> ExecuteFencedAsync(NpgsqlBatch batch, CancellationToken cancellationToken)
    {
        try
        {
            return await batch.ExecuteReaderAsync(cancellationToken);
        }
        catch (PostgresException exception) when (exception.SqlState == FenceRefused)
        {
            throw new InvalidOperationException(exception.MessageText, exception);
        }
    }

    internal static NpgsqlBatchCommand Command(string sql, params NpgsqlParameter[] parameters)
    {
        var command = new NpgsqlBatchCommand(sql);
        command.Parameters.AddRange(parameters);
        return command;
    }

    internal static NpgsqlBatchCommand KeyCommand<TKey>(string sql, LeaseSource<TKey> source, TKey key, params NpgsqlParameter[] parameters)
        where TKey : notnull
    {
        var command = new NpgsqlBatchCommand(sql);
        source.Key.Write(key, new ParameterKeyWriter(command.Parameters));
        command.Parameters.AddRange(parameters);
        return command;
    }
}
