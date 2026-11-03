using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Signals;

/// <summary>
/// The provider's signal format: one signals table per application, in its framework schema, with a row per signal of
/// its topic and key. A send is an insert in the sending transaction, so it becomes readable on commit and vanishes
/// with a rollback; statements that signal depending on the rows they match insert from a <c>SELECT</c> over them.
/// </summary>
internal sealed class SqlServerSignalSql(string schema)
{
    public string Schema { get; } = schema;

    public string Table { get; } = Tsql.Quote(schema) + ".signals";

    /// <summary>A statement that inserts one signal with the topic and key in <c>@signal_topic</c> and <c>@signal_key</c>.</summary>
    public string Insert => $"INSERT INTO {Table} (topic, k) VALUES (@signal_topic, @signal_key);\n";

    /// <summary>A statement that inserts a signal of <paramref name="topic"/> with the key expression <paramref name="key"/> for each row <paramref name="from"/> selects.</summary>
    public string InsertFrom(SignalTopic topic, string key, string from) => $"INSERT INTO {Table} (topic, k) SELECT {Topic(topic)}, {key} FROM {from};\n";

    /// <summary>A statement that inserts one signal of <paramref name="topic"/> with the key in <paramref name="key"/> when <paramref name="condition"/> holds.</summary>
    public string InsertIf(SignalTopic topic, string key, string condition) => $"IF {condition} INSERT INTO {Table} (topic, k) VALUES ({Topic(topic)}, {key});\n";

    /// <summary>The key expression of a <see cref="WorkTopics.Cancel"/> signal for the holder in <paramref name="owner"/>.</summary>
    public static string CancelKey(string owner, string source) => $"LOWER(CONVERT(nvarchar(36), {owner})) + N':' + {source}";

    public static string Topic(SignalTopic topic) => "'" + topic.Name + "'";

    public static SqlParameter[] Parameters(SignalTopic topic, string key) =>
        [new SqlParameter("@signal_topic", System.Data.SqlDbType.VarChar, SignalTopic.MaximumLength) { Value = topic.Name }, Tsql.Text("@signal_key", key, 1024)];

    /// <summary>Sends a signal in the transaction, delivered when it commits; a rollback discards it.</summary>
    public async Task SendAsync(SqlConnection connection, SqlTransaction? transaction, SignalTopic topic, string key, CancellationToken cancellationToken)
    {
        await using var command = Tsql.Command(connection, transaction, Insert, Parameters(topic, key));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
