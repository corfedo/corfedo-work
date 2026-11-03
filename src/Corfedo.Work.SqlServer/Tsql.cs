using System.Data;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer;

/// <summary>T-SQL text and parameters shared by the provider's statements.</summary>
internal static class Tsql
{
    /// <summary>
    /// The options every batch starts with: no row counts, and any error ends the batch and rolls the transaction back,
    /// as an error aborts a PostgreSQL transaction. A batch with parameters runs through <c>sp_executesql</c>, which
    /// restores the session's options when it ends, so a caller's connection keeps its own. <c>@lock_result</c> receives
    /// the results of applock requests.
    /// </summary>
    public const string Prologue = "SET NOCOUNT ON; SET XACT_ABORT ON; DECLARE @lock_result int;\n";

    /// <summary>The collation of key columns: ordinal comparison, as .NET compares keys.</summary>
    public const string Binary = "Latin1_General_100_BIN2";

    /// <summary>The largest <c>datetime2</c> value, which stands for a time that never comes.</summary>
    public static readonly DateTime Never = DateTime.MaxValue;

    /// <summary>The literal of <see cref="Never"/>.</summary>
    public const string NeverLiteral = "CAST('9999-12-31T23:59:59.9999999' AS datetime2(7))";

    public static string Quote(string identifier) => "[" + identifier.Replace("]", "]]", StringComparison.Ordinal) + "]";

    public static string Literal(string value) => "N'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

    public static string Number(long value) => value.ToString(CultureInfo.InvariantCulture);

    /// <summary>A bit expression of a condition; unknown is 0.</summary>
    public static string Bit(string condition) => $"CAST(CASE WHEN {condition} THEN 1 ELSE 0 END AS bit)";

    /// <summary>
    /// The expression of <paramref name="time"/> moved by the bigint tick count <paramref name="ticks"/>. DATEADD takes an
    /// int, so whole seconds and the remaining nanoseconds are added separately.
    /// </summary>
    public static string AddTicks(string time, string ticks) =>
        $"DATEADD(nanosecond, CAST(({ticks}) % 10000000 AS int) * 100, DATEADD(second, CAST(({ticks}) / 10000000 AS int), {time}))";

    /// <summary>The expression of <paramref name="time"/> moved by a constant.</summary>
    public static string Add(string time, TimeSpan value)
    {
        if (value == TimeSpan.Zero)
        {
            return time;
        }
        var seconds = value.Ticks / TimeSpan.TicksPerSecond;
        var nanoseconds = value.Ticks % TimeSpan.TicksPerSecond * 100;
        var result = time;
        if (seconds != 0)
        {
            result = $"DATEADD(second, {Number(seconds)}, {result})";
        }
        if (nanoseconds != 0)
        {
            result = $"DATEADD(nanosecond, {Number(nanoseconds)}, {result})";
        }
        return result;
    }

    /// <summary>
    /// Runs <paramref name="text"/> as a transaction of its own at read committed, within one batch and one round trip;
    /// the prologue's XACT_ABORT rolls it back on any error, a cancellation or a timeout.
    /// </summary>
    public static string OwnTransaction(string text) => "SET TRANSACTION ISOLATION LEVEL READ COMMITTED;\nBEGIN TRANSACTION;\n" + text + "\nCOMMIT;\n";

    /// <summary>
    /// Runs <paramref name="statement"/> and ignores a unique violation (2601, 2627) it raises, which leaves the
    /// transaction active: XACT_ABORT is off around it, since with it on a caught error still dooms the transaction.
    /// Any other error is raised again with XACT_ABORT on, so it rolls the transaction back as every other error does.
    /// </summary>
    public static string IgnoreUnique(string statement) => $"""
        SET XACT_ABORT OFF;
        BEGIN TRY
            {statement}
        END TRY
        BEGIN CATCH
            SET XACT_ABORT ON;
            IF ERROR_NUMBER() NOT IN (2601, 2627) THROW;
        END CATCH
        SET XACT_ABORT ON;

        """;

    /// <summary>
    /// Statements that lock up to <paramref name="top"/> rows of <paramref name="table"/> without waiting, in the order
    /// the query <paramref name="scan"/> returns their keys, and insert the keys of those kept into
    /// <paramref name="into"/>, whose columns are named as the key's. The query reads without locks; each key it returns
    /// is then locked through the key (alias <c>r</c>) and kept when its latest committed version meets
    /// <paramref name="locked"/> and it is not in <paramref name="into"/> yet; a row another transaction holds is
    /// skipped. Rows are locked through their key before any other index, as every statement that writes them does.
    /// The statements declare variables prefixed with <paramref name="name"/>, which names one scan per batch. Keys with
    /// nullable columns are matched with <c>IS NOT DISTINCT FROM</c> when <paramref name="nullable"/> is set.
    /// </summary>
    public static string LockEach(string name, string top, string scan, string table, IReadOnlyList<(string Column, string Type)> key, string locked, string into, bool nullable = false)
    {
        var variables = key.Select((_, i) => $"@{name}_{i}").ToArray();
        var columns = string.Join(", ", key.Select(k => k.Column));
        var equal = nullable ? "IS NOT DISTINCT FROM" : "=";
        var match = string.Join(" AND ", key.Select((k, i) => $"r.{k.Column} {equal} {variables[i]}"));
        var taken = string.Join(" AND ", key.Select((k, i) => $"t.{k.Column} {equal} {variables[i]}"));
        return $"""
            DECLARE {string.Join(", ", key.Select((k, i) => $"{variables[i]} {k.Type}"))}, @{name}_n int = 0;
            DECLARE {name} CURSOR LOCAL FORWARD_ONLY READ_ONLY FOR {scan};
            OPEN {name};
            WHILE @{name}_n < {top}
            BEGIN
                FETCH NEXT FROM {name} INTO {string.Join(", ", variables)};
                IF @@FETCH_STATUS <> 0 BREAK;
                INSERT INTO {into} ({columns}) SELECT {string.Join(", ", key.Select(k => "r." + k.Column))} FROM {table} AS r WITH (UPDLOCK, ROWLOCK, READPAST)
                WHERE {match} AND {locked} AND NOT EXISTS (SELECT 1 FROM {into} AS t WHERE {taken});
                SET @{name}_n += @@ROWCOUNT;
            END
            CLOSE {name};
            DEALLOCATE {name};

            """;
    }

    /// <summary>The key of tables keyed by the single <c>uniqueidentifier</c> column <c>id</c>, for <see cref="LockEach"/>.</summary>
    public static readonly (string Column, string Type)[] IdKey = [("id", "uniqueidentifier")];

    /// <summary>Microseconds from <paramref name="from"/> to <paramref name="to"/>, as a bigint.</summary>
    public static string Micros(string from, string to) => $"DATEDIFF_BIG(microsecond, {from}, {to})";

    public static TimeSpan FromMicros(long micros) => micros <= 0 ? TimeSpan.Zero : TimeSpan.FromTicks(micros * 10);

    /// <summary>A wait in microseconds read from the first column of a scalar result; null when there is none.</summary>
    public static TimeSpan? Wait(object? micros) => micros is long value ? FromMicros(value) : null;

    /// <summary>A store time read from a <c>datetime2</c> or <c>datetimeoffset</c> column.</summary>
    public static DateTimeOffset Time(SqlDataReader reader, int ordinal) => reader.GetValue(ordinal) switch
    {
        DateTimeOffset value => value.ToUniversalTime(),
        DateTime value => new DateTimeOffset(DateTime.SpecifyKind(value, DateTimeKind.Utc)),
        var value => throw new InvalidCastException($"Column {ordinal} holds {value.GetType().Name}, not a time."),
    };

    public static DateTimeOffset? NullableTime(SqlDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : Time(reader, ordinal);

    /// <summary>
    /// A fixed-length applock resource name: <c>cw|</c>, the lock's kind and the SHA-256 of its parts, so a long key
    /// never truncates into another name and two applications' framework schemas never share a lock.
    /// </summary>
    public static string LockName(string kind, params string[] parts)
    {
        var text = string.Join('\n', parts);
        return "cw|" + kind + "|" + Convert.ToHexString(SHA256.HashData(Encoding.Unicode.GetBytes(text)));
    }

    /// <summary>
    /// The SQL expression of <see cref="LockName"/> whose last part is the nvarchar expression <paramref name="last"/>;
    /// HASHBYTES hashes the UTF-16 bytes .NET hashes.
    /// </summary>
    public static string LockNameExpression(string kind, IReadOnlyList<string> constantParts, string last)
    {
        var prefix = string.Concat(constantParts.Select(p => p + "\n"));
        return $"(N'cw|{kind}|' + CONVERT(nvarchar(64), HASHBYTES('SHA2_256', CAST({Literal(prefix)} + {last} AS nvarchar(max))), 2))";
    }

    public static SqlParameter Guid(string name, Guid value) => new(name, SqlDbType.UniqueIdentifier) { Value = value };

    public static SqlParameter Guid(string name, Guid? value) => new(name, SqlDbType.UniqueIdentifier) { Value = value is { } v ? v : DBNull.Value };

    public static SqlParameter Int64(string name, long value) => new(name, SqlDbType.BigInt) { Value = value };

    public static SqlParameter Int64(string name, long? value) => new(name, SqlDbType.BigInt) { Value = value is { } v ? v : DBNull.Value };

    public static SqlParameter Int32(string name, int value) => new(name, SqlDbType.Int) { Value = value };

    public static SqlParameter Int32(string name, int? value) => new(name, SqlDbType.Int) { Value = value is { } v ? v : DBNull.Value };

    public static SqlParameter Int16(string name, short? value) => new(name, SqlDbType.SmallInt) { Value = value is { } v ? v : DBNull.Value };

    public static SqlParameter Bool(string name, bool value) => new(name, SqlDbType.Bit) { Value = value };

    public static SqlParameter Bool(string name, bool? value) => new(name, SqlDbType.Bit) { Value = value is { } v ? v : DBNull.Value };

    /// <summary>A Unicode text parameter with a fixed declared size, so statements reuse one plan whatever the value's length.</summary>
    public static SqlParameter Text(string name, string? value, int size = 4000) => new(name, SqlDbType.NVarChar, size) { Value = (object?)value ?? DBNull.Value };

    public static SqlParameter Bytes(string name, byte[]? value) => new(name, SqlDbType.VarBinary, -1) { Value = (object?)value ?? DBNull.Value };

    public static SqlParameter Ticks(string name, TimeSpan value) => Int64(name, value.Ticks);

    public static SqlParameter Ticks(string name, TimeSpan? value) => Int64(name, value?.Ticks);

    public static SqlParameter Time(string name, DateTimeOffset value) => new(name, SqlDbType.DateTime2) { Scale = 7, Value = value.UtcDateTime };

    public static SqlParameter Time(string name, DateTimeOffset? value) => new(name, SqlDbType.DateTime2) { Scale = 7, Value = value is { } v ? v.UtcDateTime : DBNull.Value };

    /// <summary>A JSON array parameter, read with OPENJSON into a table variable.</summary>
    public static SqlParameter Json(string name, string json) => new(name, SqlDbType.NVarChar, -1) { Value = json };

    /// <summary>A JSON array of strings, read with <c>OPENJSON</c>; its elements are in the column <c>value</c>.</summary>
    public static SqlParameter Strings(string name, IEnumerable<string> values) => Json(name, JsonSerializer.Serialize(values.ToArray(), SqlServerJson.Default.StringArray));

    /// <summary>A JSON array of ids, read with <c>OPENJSON</c>.</summary>
    public static SqlParameter Guids(string name, IEnumerable<Guid> values) => Json(name, JsonSerializer.Serialize(values.ToArray(), SqlServerJson.Default.GuidArray));

    /// <summary>
    /// A command on the connection, in <paramref name="transaction"/> if given, with <see cref="Prologue"/>. A command
    /// without parameters gets one, so it runs through <c>sp_executesql</c> and its options end with it.
    /// </summary>
    public static SqlCommand Command(SqlConnection connection, SqlTransaction? transaction, string text, params SqlParameter[] parameters)
    {
        var command = new SqlCommand(Prologue + text, connection, transaction);
        command.Parameters.AddRange(parameters);
        if (command.Parameters.Count == 0)
        {
            command.Parameters.Add(new SqlParameter("@batch", SqlDbType.Bit) { Value = true });
        }
        return command;
    }

    public static SqlCommand Command(SqlServerTransaction work, string text, params SqlParameter[] parameters) =>
        Command(work.Connection, work.Transaction, text, parameters);

    /// <summary>An open connection; disposed again when opening fails.</summary>
    public static async Task<SqlConnection> OpenAsync(string connectionString, CancellationToken cancellationToken)
    {
        var connection = new SqlConnection(connectionString);
        try
        {
            await connection.OpenAsync(cancellationToken);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }

    /// <summary>Error numbers of a unique index (2601) or unique constraint (2627) violation.</summary>
    public static bool IsUniqueViolation(SqlException exception) => exception.Errors.Cast<SqlError>().Any(e => e.Number is 2601 or 2627);

    /// <summary>A deadlock victim (1205): SQL Server rolled its transaction back.</summary>
    public static bool IsDeadlock(SqlException exception) => exception.Errors.Cast<SqlError>().Any(e => e.Number == 1205);

    public static bool HasError(SqlException exception, int number) => exception.Errors.Cast<SqlError>().Any(e => e.Number == number);

    /// <summary>The message of the first error with <paramref name="number"/>.</summary>
    public static string Message(SqlException exception, int number) => exception.Errors.Cast<SqlError>().First(e => e.Number == number).Message;
}
