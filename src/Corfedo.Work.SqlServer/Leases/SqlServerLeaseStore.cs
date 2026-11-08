using System.Collections.Concurrent;
using System.Data;
using Corfedo.Work.SqlServer.Signals;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace Corfedo.Work.SqlServer.Leases;

internal sealed partial class SqlServerLeaseStore(string connectionString, string schema, SqlServerSignalSql signals, ILogger logger, WorkTelemetryOptions telemetry, bool alterTables = true) : ILeaseStore
{
    /// <summary>Attempts of a provider transaction chosen as a deadlock victim before the error reaches the caller.</summary>
    private const int DeadlockAttempts = 5;

    private readonly ConcurrentDictionary<object, LeaseSql> statements = new();

    /// <summary>The framework schema, which holds generations and the job tables.</summary>
    internal string Schema => schema;

    /// <summary>The application's signal format, for statements that signal.</summary>
    internal SqlServerSignalSql Signals => signals;

    /// <summary>
    /// The statements of <paramref name="source"/>. Job sources are on the framework's job table; the first use of any
    /// other source reads and verifies its table in the catalog, for the key columns' types and the index names.
    /// </summary>
    internal async ValueTask<LeaseSql> SqlAsync<TKey>(LeaseSource<TKey> source, CancellationToken cancellationToken)
        where TKey : notnull
    {
        if (statements.TryGetValue(source, out var sql))
        {
            return sql;
        }
        var shape = source.Job is null ? await SourceCatalog.ReadAsync(connectionString, source, alterTables, cancellationToken) : SourceShape.Jobs;
        return statements.GetOrAdd(source, Build(source, shape));
    }

    private LeaseSql Build<TKey>(LeaseSource<TKey> source, SourceShape shape)
        where TKey : notnull =>
        new(source.Name, source.Store, shape, source.Partition, source.Policy, source.Fields, source.Settles, source.Reconcile, source.Job, schema);

    public async ValueTask VerifyAsync<TKey>(LeaseSource<TKey> source, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var shape = await SourceCatalog.ReadAsync(connectionString, source, alterTables, cancellationToken);
        statements[source] = Build(source, shape);
    }

    internal Task<SqlConnection> OpenAsync(CancellationToken cancellationToken) => Tsql.OpenAsync(connectionString, cancellationToken);

    /// <summary>A provider transaction at read committed on a new connection.</summary>
    internal static async Task<SqlTransaction> BeginAsync(SqlConnection connection, CancellationToken cancellationToken) =>
        (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);

    /// <summary>
    /// Runs <paramref name="operation"/>, a provider transaction of its own, again when SQL Server chose it as a
    /// deadlock victim and rolled it back, up to <see cref="DeadlockAttempts"/> times.
    /// </summary>
    internal static async Task<T> RetryDeadlocksAsync<T>(Func<Task<T>> operation)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await operation();
            }
            catch (SqlException exception) when (attempt < DeadlockAttempts && Tsql.IsDeadlock(exception))
            {
            }
        }
    }

    internal static void AddKey<TKey>(SqlCommand command, LeaseSql sql, LeaseSource<TKey> source, TKey key)
        where TKey : notnull => source.Key.Write(key, new ParameterKeyWriter(command.Parameters, sql.Shape.Keys));

    /// <summary>
    /// Whether the key's fields fit their columns. A key that does not is no record's key, so operations on keys the
    /// caller gives treat it as matching no record instead of binding it, which would truncate it to another record's key.
    /// </summary>
    internal static bool Fits<TKey>(LeaseSql sql, LeaseSource<TKey> source, TKey key)
        where TKey : notnull
    {
        var writer = new FitsKeyWriter(sql.Shape.Keys);
        source.Key.Write(key, writer);
        return writer.Fits;
    }

    internal static SqlParameter[] KeyParameters<TKey>(LeaseSql sql, LeaseSource<TKey> source, TKey key)
        where TKey : notnull
    {
        using var command = new SqlCommand();
        AddKey(command, sql, source, key);
        var parameters = command.Parameters.Cast<SqlParameter>().ToArray();
        command.Parameters.Clear();
        return parameters;
    }

    /// <summary>The <c>@keyset</c> parameter of distinct keys, read by <see cref="LeaseSql.KeyedRows"/>.</summary>
    internal static SqlParameter KeySet<TKey>(LeaseSource<TKey> source, IEnumerable<TKey> keys)
        where TKey : notnull
    {
        using var writer = new JsonKeyWriter(source.Key.Fields.Count);
        foreach (var key in keys.DistinctBy(source.Key.Format))
        {
            source.Key.Write(key, writer);
        }
        return writer.ToParameter();
    }

    internal static TKey ReadKey<TKey>(LeaseSource<TKey> source, SqlDataReader reader)
        where TKey : notnull => source.Key.Read(new RowKeyReader(reader, 0));

    /// <summary>A command for one key in the caller's or a provider transaction, with the key as <c>@k0</c>, ...</summary>
    internal static SqlCommand KeyCommand<TKey>(SqlConnection connection, SqlTransaction? transaction, LeaseSql sql, LeaseSource<TKey> source, TKey key, string text, params SqlParameter[] parameters)
        where TKey : notnull
    {
        var command = Tsql.Command(connection, transaction, text, parameters);
        AddKey(command, sql, source, key);
        return command;
    }

    /// <summary>A command for many keys, as the JSON array <c>@keyset</c>.</summary>
    internal static SqlCommand KeysCommand<TKey>(SqlConnection connection, SqlTransaction? transaction, LeaseSql sql, LeaseSource<TKey> source, IEnumerable<TKey> keys, string text, params SqlParameter[] parameters)
        where TKey : notnull
    {
        var command = Tsql.Command(connection, transaction, text, parameters);
        command.Parameters.Add(KeySet(source, keys));
        return command;
    }

    /// <summary>Signals listeners when the transaction commits; a rollback discards the signal.</summary>
    internal Task NotifyAsync(SqlConnection connection, SqlTransaction transaction, SignalTopic topic, string key, CancellationToken cancellationToken) =>
        signals.SendAsync(connection, transaction, topic, key, cancellationToken);

    /// <summary>
    /// Executes a batch that may refuse the transaction at its fence: a fence refused for the transaction's isolation
    /// level rolled the transaction back and is reported as <see cref="InvalidOperationException"/>; once the batch ran,
    /// the transaction is known to be at read committed.
    /// </summary>
    internal static async Task<SqlDataReader> ExecuteFencedAsync(SqlCommand command, DbWorkTransaction? work, CancellationToken cancellationToken)
    {
        try
        {
            var reader = await command.ReaderAsync(cancellationToken);
            if (work is not null)
            {
                work.ReadCommitted = true;
            }
            return reader;
        }
        catch (SqlException exception) when (Tsql.HasError(exception, LeaseSql.FenceRefused))
        {
            throw new InvalidOperationException(Tsql.Message(exception, LeaseSql.FenceRefused), exception);
        }
    }
}
