using System.Text;
using Npgsql;
using NpgsqlTypes;

namespace Corfedo.Work.PostgreSql.Signals;

/// <summary>
/// The provider's signal format: one channel per application, <c>corfedo_work:</c> and the framework schema, and a
/// payload of the topic, a space and the key. Topic names are restricted to characters that need no quoting, so a
/// statement can build a payload from a key expression. Statements that signal return signal rows, the sending
/// transaction's id and a payload, which <see cref="PostgreSqlSignalSender"/> reads and sends once that transaction
/// has committed.
/// </summary>
internal sealed class PostgreSqlSignalSql
{
    /// <summary>The longest channel name: <c>pg_notify</c> refuses longer ones, while <c>LISTEN</c> truncates them.</summary>
    private const int MaximumChannelBytes = 63;

    private const string ChannelPrefix = "corfedo_work:";

    /// <summary>The name of the first column of signal rows, which tells their row sets apart from a statement's own.</summary>
    public const string RowMarker = "corfedo_work_signal";

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

    /// <summary>
    /// The select list of signal rows whose payload is the SQL expression <paramref name="payload"/> of type text; a
    /// statement appends its <c>FROM</c> and <c>WHERE</c>, so it signals once per row it matches. Reading the
    /// transaction id assigns one to a transaction that has none yet. The id is read as <c>bigint</c>, which holds every
    /// 64-bit transaction id below 2^63.
    /// </summary>
    public static string Rows(string payload) => $"SELECT pg_current_xact_id()::text::bigint AS {RowMarker}, {payload}";

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

    private static NpgsqlParameter<string> Text(string value) => new() { TypedValue = value, NpgsqlDbType = NpgsqlDbType.Text };
}
