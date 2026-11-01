using System.Text;
using Npgsql;
using NpgsqlTypes;

namespace Corfedo.Work.PostgreSql.Signals;

/// <summary>
/// The provider's signal format: one channel per application, <c>corfedo_work:</c> and the framework schema, and a
/// payload of the topic, a space and the key. Topic names are restricted to characters that need no quoting, so a
/// statement can build a payload from a key expression.
/// </summary>
internal sealed class PostgreSqlSignalSql
{
    /// <summary>The longest channel name: <c>pg_notify</c> refuses longer ones, while <c>LISTEN</c> truncates them.</summary>
    private const int MaximumChannelBytes = 63;

    private const string ChannelPrefix = "corfedo_work:";

    private PostgreSqlSignalSql(string channel) => Channel = channel;

    public string Channel { get; }

    /// <summary>The signal format of the application whose framework schema is <paramref name="schema"/>.</summary>
    /// <exception cref="ArgumentException">The channel name would be longer than 63 bytes.</exception>
    public static PostgreSqlSignalSql For(string schema)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);
        var channel = ChannelPrefix + schema;
        if (Encoding.UTF8.GetByteCount(channel) > MaximumChannelBytes)
        {
            throw new ArgumentException($"The schema name {schema} is longer than the {MaximumChannelBytes - ChannelPrefix.Length} bytes its signal channel allows.", nameof(schema));
        }
        return new PostgreSqlSignalSql(channel);
    }

    public static string Payload(SignalTopic topic, string key) => topic.Name + " " + key;

    /// <summary>The SQL expression of a payload whose key is the SQL expression <paramref name="key"/> of type text.</summary>
    public static string PayloadExpression(SignalTopic topic, string key) => $"'{topic.Name} ' || {key}";

    /// <summary>The SQL expression of a <see cref="WorkTopics.Cancel"/> payload for the holder in <paramref name="owner"/>.</summary>
    public static string CancelExpression(string owner, string source) => PayloadExpression(WorkTopics.Cancel, $"{owner}::text || ':' || {source}");

    public static bool TryParse(string payload, out SignalTopic topic, out string key)
    {
        var space = payload.IndexOf(' ');
        if (space > 0 && SignalTopic.IsValidName(payload.AsSpan(0, space)))
        {
            topic = new SignalTopic(payload[..space]);
            key = payload[(space + 1)..];
            return true;
        }
        topic = default;
        key = "";
        return false;
    }

    public NpgsqlParameter<string> ChannelParameter() => Text(Channel);

    public static NpgsqlParameter<string> PayloadParameter(SignalTopic topic, string key) => Text(Payload(topic, key));

    /// <summary>Sends a signal in the transaction, delivered when it commits; a rollback discards it.</summary>
    public async Task NotifyAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, SignalTopic topic, string key, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("SELECT pg_notify($1, $2)", connection, transaction);
        command.Parameters.Add(ChannelParameter());
        command.Parameters.Add(PayloadParameter(topic, key));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static NpgsqlParameter<string> Text(string value) => new() { TypedValue = value, NpgsqlDbType = NpgsqlDbType.Text };
}
