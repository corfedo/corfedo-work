using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Monitoring;

/// <summary>
/// Tracking of expected keys. Transactions that change a recurrence or its copied pause flag queue its key in
/// <c>job_expectation_changes</c>; each pass applies the queued keys, so a steady pass reads what changed. A full
/// reconciliation, which reads every recurrence and key of a definition, runs only while the definition's revision or
/// pause differs from the ones it last reconciled with, and is recorded once a read without locks finds nothing left.
/// Rows of several keys are locked in key order; rows locked without waiting may be locked in any order.
/// </summary>
internal sealed partial class SqlServerMonitoringStore
{
    /// <summary>Sets the window of a row inserted or taken up for a deadline: <c>@deadline</c>, <c>@window_start</c>, <c>@revision</c>.</summary>
    private const string StartWindow =
        "deadline_at = COALESCE(@deadline, " + Tsql.NeverLiteral + "), window_start = @window_start, met_at = NULL, first_after = NULL, last_after = NULL, revision = @revision";

    /// <summary>Whether the key of row <paramref name="row"/> has an unpaused recurrence of the definition <c>@definition</c>.</summary>
    private string HasUnpausedRecurrence(string row) =>
        $"(({row}.subject_kind IS NULL AND EXISTS (SELECT 1 FROM {recurrences} AS r WHERE r.[definition] = @definition AND r.subject_kind IS NULL AND r.paused = 0)) "
        + $"OR ({row}.subject_kind IS NOT NULL AND EXISTS (SELECT 1 FROM {recurrences} AS r WHERE r.[definition] = @definition AND r.subject_kind = {row}.subject_kind "
        + $"AND r.subject_id = {row}.subject_id AND r.paused = 0)))";

    /// <summary>
    /// Statements that clear the windows of the rows of the keys in the table variable <c>@gone</c> of
    /// <see cref="StateKeyTable"/>, which the transaction holds, into <c>@cleared</c>, and resolve their keys' open
    /// <see cref="WorkIncidentKind.CompletesBy"/> incidents into <c>@resolved</c>; the definition is <c>@definition</c>.
    /// </summary>
    private string ClearWindows => $"""
        DECLARE @cleared TABLE (subject_kind nvarchar(100) COLLATE {Tsql.Binary}, subject_id nvarchar(400) COLLATE {Tsql.Binary});
        UPDATE s SET deadline_at = NULL, window_start = NULL, met_at = NULL, first_after = NULL, last_after = NULL, whole = 0, revision = NULL
        OUTPUT inserted.subject_kind, inserted.subject_id INTO @cleared
        FROM @gone AS g INNER LOOP JOIN {states} AS s WITH (FORCESEEK) ON s.[definition] = g.[definition] AND {SameKey("s", "g")};
        DECLARE @resolved {IncidentTable};
        {ResolveIncidents($"i.[definition] = @definition AND i.kind = {(int)WorkIncidentKind.CompletesBy} AND EXISTS (SELECT 1 FROM @cleared AS c WHERE {SameKey("i", "c")})")}
        """;

    /// <summary>
    /// Deletes the queued changes locked without waiting and, in the same transaction, reads each key in a new statement,
    /// so it sees every change committed before the deletion: whether it has an unpaused recurrence, and its row locked
    /// without waiting. Then starts, clears or queues again the keys that need it.
    /// </summary>
    public async ValueTask<int> ApplyChangesAsync(string definition, long revision, ExpectationWindow? window, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var (count, applied) = await InTransactionAsync(async work =>
        {
            var deleted = 0;
            var keys = new HashSet<(string? Kind, string? Id)>();
            await using (var delete = Tsql.Command(work, $"""
                DECLARE @queued TABLE (n bigint NOT NULL PRIMARY KEY);
                {Tsql.LockEach("queued_scan", "@limit", $"SELECT q.n FROM {keyChanges} AS q WITH (INDEX(job_expectation_changes_definition)) WHERE q.[definition] = @definition",
                    keyChanges, [("n", "bigint")], "1 = 1", "@queued")}
                DECLARE @deleted TABLE (subject_kind nvarchar(100) COLLATE {Tsql.Binary}, subject_id nvarchar(400) COLLATE {Tsql.Binary});
                DELETE c OUTPUT deleted.subject_kind, deleted.subject_id INTO @deleted FROM @queued AS q INNER LOOP JOIN {keyChanges} AS c WITH (FORCESEEK) ON c.n = q.n;
                SELECT subject_kind, subject_id FROM @deleted;
                """,
                Tsql.Text("@definition", definition, 200),
                Tsql.Int32("@limit", limit)))
            await using (var reader = await delete.ReaderAsync(cancellationToken))
            {
                while (await reader.NextRowAsync(cancellationToken))
                {
                    deleted++;
                    keys.Add((reader.IsDBNull(0) ? null : reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1)));
                }
            }
            var changes = IncidentChanges.None;
            if (window is not null && keys.Count > 0)
            {
                changes = await ApplyKeysAsync(work, definition, revision, window, keys, cancellationToken);
            }
            return (deleted, changes);
        }, cancellationToken);
        WorkTelemetry.Count(applied);
        return count;
    }

    private async Task<IncidentChanges> ApplyKeysAsync(SqlServerTransaction work, string definition, long revision, ExpectationWindow window, IReadOnlyCollection<(string? Kind, string? Id)> keys, CancellationToken cancellationToken)
    {
        var requeue = new List<(string? Kind, string? Id)>();
        var start = new List<(string? Kind, string? Id)>();
        var clear = new List<(string? Kind, string? Id)>();
        await using (var read = Tsql.Command(work, $"""
            DECLARE @k TABLE (n int NOT NULL PRIMARY KEY, subject_kind nvarchar(100) COLLATE {Tsql.Binary}, subject_id nvarchar(400) COLLATE {Tsql.Binary});
            INSERT INTO @k (n, subject_kind, subject_id) SELECT n, k, i FROM OPENJSON(@keys) WITH (n int '$.n', k nvarchar(100) '$.k', i nvarchar(400) '$.i');
            DECLARE @held TABLE (n int NOT NULL PRIMARY KEY, tracked bit NOT NULL, whole bit NOT NULL);
            INSERT INTO @held (n, tracked, whole)
            SELECT k.n, {Tsql.Bit("s.deadline_at IS NOT NULL")}, s.whole FROM @k AS k
            INNER LOOP JOIN {states} AS s WITH (UPDLOCK, ROWLOCK, READPAST, FORCESEEK) ON s.[definition] = @definition AND {SameKey("s", "k")};
            SELECT k.subject_kind, k.subject_id, {Tsql.Bit(HasUnpausedRecurrence("k"))}, {Tsql.Bit("h.n IS NOT NULL")}, h.tracked, h.whole,
                {Tsql.Bit($"EXISTS (SELECT 1 FROM {states} AS s WHERE s.[definition] = @definition AND {SameKey("s", "k")})")}
            FROM @k AS k LEFT JOIN @held AS h ON h.n = k.n ORDER BY k.n;
            """,
            Tsql.Text("@definition", definition, 200),
            Tsql.Json("@keys", KeysJson(keys))))
        await using (var reader = await read.ReaderAsync(cancellationToken))
        {
            while (await reader.NextRowAsync(cancellationToken))
            {
                var key = (reader.IsDBNull(0) ? null : reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1));
                var expected = reader.GetBoolean(2);
                var held = reader.GetBoolean(3);
                if (!held && reader.GetBoolean(6))
                {
                    // Another transaction holds the key's row.
                    requeue.Add(key);
                }
                else if (expected && (!held || !reader.GetBoolean(4)))
                {
                    start.Add(key);
                }
                else if (!expected && held && reader.GetBoolean(4) && !reader.GetBoolean(5))
                {
                    clear.Add(key);
                }
            }
        }
        if (requeue.Count > 0)
        {
            await using var command = Tsql.Command(work,
                $"INSERT INTO {keyChanges} ([definition], subject_kind, subject_id) SELECT @definition, k, i FROM OPENJSON(@keys) WITH (k nvarchar(100) '$.k', i nvarchar(400) '$.i');",
                Tsql.Text("@definition", definition, 200),
                Tsql.Json("@keys", KeysJson(requeue)));
            await command.NonQueryAsync(cancellationToken);
        }
        if (start.Count > 0)
        {
            // A row created meanwhile, by the ending transaction of a failure, takes up the deadline unless it has one.
            await using var command = Tsql.Command(work, $"""
                DECLARE @k TABLE (n int NOT NULL PRIMARY KEY, subject_kind nvarchar(100) COLLATE {Tsql.Binary}, subject_id nvarchar(400) COLLATE {Tsql.Binary});
                INSERT INTO @k (n, subject_kind, subject_id) SELECT n, k, i FROM OPENJSON(@keys) WITH (n int '$.n', k nvarchar(100) '$.k', i nvarchar(400) '$.i');
                DECLARE @n int = 0, @count int = (SELECT COUNT(*) FROM @k), @kind nvarchar(100), @subject nvarchar(400), @upsert_inserted bit;
                WHILE @n < @count
                BEGIN
                    SELECT @kind = subject_kind, @subject = subject_id FROM @k WHERE n = @n;
                    {Upsert(
                        $"UPDATE s SET {StartWindow} FROM {states} AS s WHERE s.[definition] = @definition AND s.subject_kind IS NOT DISTINCT FROM @kind AND s.subject_id IS NOT DISTINCT FROM @subject AND s.deadline_at IS NULL;",
                        $"INSERT INTO {states} ([definition], subject_kind, subject_id, deadline_at, window_start, revision) VALUES (@definition, @kind, @subject, COALESCE(@deadline, {Tsql.NeverLiteral}), @window_start, @revision);")}
                    SET @n += 1;
                END
                """,
                [Tsql.Text("@definition", definition, 200), Tsql.Json("@keys", KeysJson(start)), .. WindowParameters(window, revision)]);
            await command.NonQueryAsync(cancellationToken);
        }
        if (clear.Count == 0)
        {
            return IncidentChanges.None;
        }
        List<WorkIncident> resolved;
        await using (var command = Tsql.Command(work, $"""
            DECLARE @gone {StateKeyTable};
            INSERT INTO @gone ([definition], subject_kind, subject_id) SELECT @definition, k, i FROM OPENJSON(@keys) WITH (k nvarchar(100) '$.k', i nvarchar(400) '$.i');
            {ClearWindows}
            SELECT {IncidentColumns} FROM @resolved AS i;
            """,
            Tsql.Text("@definition", definition, 200),
            Tsql.Json("@keys", KeysJson(clear))))
        await using (var reader = await command.ReaderAsync(cancellationToken))
        {
            resolved = await ReadIncidentsAsync(reader, cancellationToken);
        }
        if (resolved.Count == 0)
        {
            return IncidentChanges.None;
        }
        await NotifyAsync(work.Connection, work.Transaction, cancellationToken);
        return new IncidentChanges([], resolved);
    }

    /// <summary>
    /// Without recurrences, inserts or takes up the definition-alone row as the whole definition unless it has a
    /// deadline, keeping a deadline it has. With them, locks the row if it is marked whole without waiting, then keeps its
    /// window when a recurrence without subject is unpaused and clears it otherwise.
    /// </summary>
    public async ValueTask TrackWholeAsync(string definition, long revision, bool whole, ExpectationWindow window, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(window);
        const string alone = "s.[definition] = @definition AND s.subject_kind IS NULL AND s.subject_id IS NULL";
        if (whole)
        {
            await InTransactionAsync(async work =>
            {
                await using var upsert = Tsql.Command(work, $"""
                    DECLARE @upsert_inserted bit;
                    {Upsert(
                        $"UPDATE s SET whole = 1, deadline_at = COALESCE(s.deadline_at, @deadline, {Tsql.NeverLiteral}), "
                        + "window_start = CASE WHEN s.deadline_at IS NULL THEN @window_start ELSE s.window_start END, "
                        + $"revision = CASE WHEN s.deadline_at IS NULL THEN @revision ELSE s.revision END FROM {states} AS s WHERE {alone} AND NOT (s.whole = 1 AND s.deadline_at IS NOT NULL);",
                        $"IF NOT EXISTS (SELECT 1 FROM {states} AS s WHERE {alone}) "
                        + $"INSERT INTO {states} ([definition], whole, deadline_at, window_start, revision) VALUES (@definition, 1, COALESCE(@deadline, {Tsql.NeverLiteral}), @window_start, @revision);")}
                    """,
                    [Tsql.Text("@definition", definition, 200), .. WindowParameters(window, revision)]);
                await upsert.NonQueryAsync(cancellationToken);
                return 0;
            }, cancellationToken);
            return;
        }
        var changes = await InTransactionAsync(async work =>
        {
            await using var command = Tsql.Command(work, $"""
                DECLARE @held TABLE (kept bit NOT NULL);
                INSERT INTO @held (kept) SELECT {Tsql.Bit(HasUnpausedRecurrence("s"))} FROM {states} AS s WITH (UPDLOCK, ROWLOCK, READPAST) WHERE {alone} AND s.whole = 1;
                DECLARE @gone {StateKeyTable};
                UPDATE s SET whole = 0 FROM {states} AS s WHERE {alone} AND EXISTS (SELECT 1 FROM @held WHERE kept = 1);
                INSERT INTO @gone ([definition], subject_kind, subject_id) SELECT @definition, NULL, NULL FROM @held WHERE kept = 0;
                {ClearWindows}
                SELECT {IncidentColumns} FROM @resolved AS i;
                """,
                Tsql.Text("@definition", definition, 200));
            await using var reader = await command.ReaderAsync(cancellationToken);
            var resolved = await ReadIncidentsAsync(reader, cancellationToken);
            await reader.DisposeAsync();
            if (resolved.Count == 0)
            {
                return IncidentChanges.None;
            }
            await NotifyAsync(work.Connection, work.Transaction, cancellationToken);
            return new IncidentChanges([], resolved);
        }, cancellationToken);
        WorkTelemetry.Count(changes);
    }

    /// <summary>
    /// Keys are found by an anti-join of the definition's unpaused recurrences against its rows with a deadline, in the
    /// order of their subject index, at most <paramref name="limit"/> per call, and the definition alone for a
    /// recurrence without subject, first, as it sorts first in the key. A row the ending transaction created for failures
    /// takes up the deadline.
    /// </summary>
    public async ValueTask<int> InsertExpectedAsync(string definition, long revision, ExpectationWindow window, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(window);
        return await InTransactionAsync(async work =>
        {
            await using var command = Tsql.Command(work, $"""
                DECLARE @k TABLE (n int IDENTITY NOT NULL PRIMARY KEY, subject_kind nvarchar(100) COLLATE {Tsql.Binary}, subject_id nvarchar(400) COLLATE {Tsql.Binary});
                IF EXISTS (SELECT 1 FROM {recurrences} AS r WHERE r.[definition] = @definition AND r.subject_kind IS NULL AND r.paused = 0)
                    AND NOT EXISTS (SELECT 1 FROM {states} AS e WHERE e.[definition] = @definition AND e.subject_kind IS NULL AND e.subject_id IS NULL AND e.deadline_at IS NOT NULL)
                    INSERT INTO @k (subject_kind, subject_id) VALUES (NULL, NULL);
                INSERT INTO @k (subject_kind, subject_id)
                SELECT DISTINCT TOP (@limit) r.subject_kind, r.subject_id FROM {recurrences} AS r WITH (INDEX(job_recurrences_subject))
                WHERE r.[definition] = @definition AND r.subject_kind IS NOT NULL AND r.paused = 0
                AND NOT EXISTS (SELECT 1 FROM {states} AS e WHERE e.[definition] = @definition AND e.subject_kind = r.subject_kind AND e.subject_id = r.subject_id AND e.deadline_at IS NOT NULL)
                ORDER BY r.subject_kind, r.subject_id;
                DECLARE @n int = 1, @count int = (SELECT COUNT(*) FROM @k), @kind nvarchar(100), @subject nvarchar(400), @upsert_inserted bit;
                WHILE @n <= @count
                BEGIN
                    SELECT @kind = subject_kind, @subject = subject_id FROM @k WHERE n = @n;
                    {Upsert(
                        $"UPDATE s SET whole = 0, {StartWindow} FROM {states} AS s WHERE s.[definition] = @definition AND s.subject_kind IS NOT DISTINCT FROM @kind AND s.subject_id IS NOT DISTINCT FROM @subject;",
                        $"INSERT INTO {states} ([definition], subject_kind, subject_id, deadline_at, window_start, revision) VALUES (@definition, @kind, @subject, COALESCE(@deadline, {Tsql.NeverLiteral}), @window_start, @revision);")}
                    SET @n += 1;
                END
                SELECT @count;
                """,
                [Tsql.Text("@definition", definition, 200), Tsql.Int32("@limit", limit), .. WindowParameters(window, revision)]);
            return Convert.ToInt32(await command.ScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        }, cancellationToken);
    }

    /// <summary>
    /// Locks up to <paramref name="limit"/> rows with a deadline that are not expected without waiting, clears their
    /// window and resolves their open completion incidents. While tracking, a row is expected when it is marked whole or
    /// a recurrence of its key is not paused.
    /// </summary>
    public async ValueTask<int> ClearUnexpectedAsync(string definition, bool tracking, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        string Unexpected(string alias) => tracking ? $"NOT ({alias}.whole = 1 OR {HasUnpausedRecurrence(alias)})" : "1 = 1";
        var (count, resolved) = await InTransactionAsync(async work =>
        {
            await using var command = Tsql.Command(work, $"""
                DECLARE @gone {StateKeyTable};
                {Tsql.LockEach("unexpected_scan", "@limit",
                    $"SELECT s.[definition], s.subject_kind, s.subject_id FROM {states} AS s WITH (INDEX(job_expectation_states_deadline)) "
                    + $"WHERE s.[definition] = @definition AND s.deadline_at IS NOT NULL AND {Unexpected("s")}",
                    states, StateKey, $"r.deadline_at IS NOT NULL AND {Unexpected("r")}", "@gone", nullable: true)}
                {ClearWindows}
                SELECT COUNT(*) FROM @cleared;
                SELECT {IncidentColumns} FROM @resolved AS i;
                """,
                Tsql.Text("@definition", definition, 200),
                Tsql.Int32("@limit", limit));
            await using var reader = await command.ReaderAsync(cancellationToken);
            await reader.NextRowAsync(cancellationToken);
            var cleared = reader.GetInt32(0);
            await reader.NextSetAsync(cancellationToken);
            return (cleared, await ReadIncidentsAsync(reader, cancellationToken));
        }, cancellationToken);
        WorkTelemetry.Count(resolved.Count == 0 ? IncidentChanges.None : new IncidentChanges([], resolved));
        return count;
    }

    /// <summary>
    /// One update of the definition row, guarded by its revision and pause and by reads without locks of what a
    /// reconciliation leaves to do; a row another transaction held stays as it was committed, so it is found.
    /// </summary>
    public async ValueTask<bool> MarkReconciledAsync(StoredExpectation definition, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        var expectation = definition.Expectation;
        var tracking = expectation?.Deadline is not null && !definition.Paused;
        var kinds = ExpectationKinds.Unexpected(expectation).Select(k => ((short)k).ToString(CultureInfo.InvariantCulture)).ToArray();
        var left = new List<string>
        {
            kinds.Length == 0 ? "1 = 0" : $"EXISTS (SELECT 1 FROM {incidents} AS i WHERE i.[definition] = @definition AND i.resolved_at IS NULL AND i.kind IN ({string.Join(", ", kinds)}))",
        };
        if (expectation?.FailureLimit is null)
        {
            left.Add($"EXISTS (SELECT 1 FROM {states} AS s WHERE s.[definition] = @definition AND s.failures > 0)");
        }
        if (tracking)
        {
            left.Add($"EXISTS (SELECT 1 FROM {states} AS s WHERE s.[definition] = @definition AND s.deadline_at IS NOT NULL AND (s.revision IS DISTINCT FROM @revision OR NOT (s.whole = 1 OR {HasUnpausedRecurrence("s")})))");
            left.Add($"EXISTS (SELECT 1 FROM {recurrences} AS r WHERE r.[definition] = @definition AND r.subject_kind IS NOT NULL AND r.paused = 0 "
                + $"AND NOT EXISTS (SELECT 1 FROM {states} AS e WHERE e.[definition] = @definition AND e.subject_kind = r.subject_kind AND e.subject_id = r.subject_id AND e.deadline_at IS NOT NULL))");
            left.Add($"(EXISTS (SELECT 1 FROM {recurrences} AS r WHERE r.[definition] = @definition AND r.subject_kind IS NULL AND r.paused = 0) "
                + $"AND NOT EXISTS (SELECT 1 FROM {states} AS e WHERE e.[definition] = @definition AND e.subject_kind IS NULL AND e.subject_id IS NULL AND e.deadline_at IS NOT NULL))");
        }
        else
        {
            left.Add($"EXISTS (SELECT 1 FROM {states} AS s WHERE s.[definition] = @definition AND s.deadline_at IS NOT NULL)");
        }
        await using var connection = await leases.OpenAsync(cancellationToken);
        await using var command = Tsql.Command(connection, null,
            $"UPDATE d SET monitored_revision = @revision, monitored_paused = @paused FROM {definitions} AS d WHERE d.[key] = @definition AND d.revision = @revision AND d.paused = @paused "
            + $"AND NOT ({string.Join(" OR ", left)});\nSELECT @@ROWCOUNT;",
            Tsql.Text("@definition", definition.Definition, 200),
            Tsql.Int64("@revision", definition.Revision),
            Tsql.Bool("@paused", definition.Paused));
        return Convert.ToInt32(await command.ScalarAsync(cancellationToken), CultureInfo.InvariantCulture) == 1;
    }

    /// <summary>The parameters of <see cref="StartWindow"/>.</summary>
    private static SqlParameter[] WindowParameters(ExpectationWindow window, long revision) =>
        [Tsql.Time("@deadline", Deadline(window.Deadline)), Tsql.Time("@window_start", window.WindowStart), Tsql.Int64("@revision", revision)];

    /// <summary>
    /// A deadline as a parameter: null for <see cref="ExpectationWindow.Uncovered"/>, which statements store as the
    /// largest <c>datetime2</c> with <c>COALESCE</c>, and read back as null with <c>NULLIF</c>.
    /// </summary>
    private static DateTimeOffset? Deadline(DateTimeOffset value) => value == ExpectationWindow.Uncovered ? null : value;

    /// <summary>Keys as a JSON array of their position, kind and id, in key order: the definition alone first.</summary>
    private static string KeysJson(IEnumerable<(string? Kind, string? Id)> keys)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            var n = 0;
            foreach (var (kind, id) in keys.OrderBy(k => k.Kind is not null).ThenBy(k => k.Kind, StringComparer.Ordinal).ThenBy(k => k.Id, StringComparer.Ordinal))
            {
                writer.WriteStartObject();
                writer.WriteNumber("n", n++);
                if (kind is not null)
                {
                    writer.WriteString("k", kind);
                    writer.WriteString("i", id);
                }
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    /// <summary>
    /// Runs <paramref name="operation"/> in a provider transaction of its own at read committed, committed when it
    /// returns, and again when SQL Server chose it as a deadlock victim.
    /// </summary>
    private Task<T> InTransactionAsync<T>(Func<SqlServerTransaction, Task<T>> operation, CancellationToken cancellationToken) =>
        Leases.SqlServerLeaseStore.RetryDeadlocksAsync(async () =>
        {
            await using var connection = await leases.OpenAsync(cancellationToken);
            await using var transaction = await Leases.SqlServerLeaseStore.BeginAsync(connection, cancellationToken);
            var result = await operation(new SqlServerTransaction(transaction));
            await transaction.CommitAsync(cancellationToken);
            return result;
        });
}
