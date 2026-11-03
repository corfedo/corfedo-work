using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Corfedo.Work.SqlServer.Jobs;

namespace Corfedo.Work.SqlServer.Monitoring;

internal sealed partial class SqlServerMonitoringStore
{
    /// <summary>
    /// Reads the distinct window starts of the rows to move, computes their deadlines and moves the rows locked without
    /// waiting. Recorded completions are placed against the new deadline: the first before it is the window's, the
    /// first and latest at or after it are completions after the deadline.
    /// </summary>
    public async ValueTask<int> RescheduleAsync(string definition, long revision, Func<DateTimeOffset, DateTimeOffset> next, bool olderRevisions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(next);
        string Selection(string alias) => olderRevisions
            ? $"{alias}.[definition] = @definition AND ({alias}.deadline_at = {Tsql.NeverLiteral} OR ({alias}.deadline_at IS NOT NULL AND {alias}.revision IS DISTINCT FROM @revision))"
            : $"{alias}.[definition] = @definition AND {alias}.deadline_at = {Tsql.NeverLiteral}";
        var starts = new List<DateTimeOffset>();
        await using (var connection = await leases.OpenAsync(cancellationToken))
        await using (var read = Tsql.Command(connection, null, $"SELECT DISTINCT s.window_start FROM {states} AS s WHERE {Selection("s")};",
            Tsql.Text("@definition", definition, 200),
            Tsql.Int64("@revision", revision)))
        await using (var reader = await read.ReaderAsync(cancellationToken))
        {
            while (await reader.NextRowAsync(cancellationToken))
            {
                if (!reader.IsDBNull(0))
                {
                    starts.Add(Tsql.Time(reader, 0));
                }
            }
        }
        var moves = starts.Select(w => (Start: w, Deadline: next(w)))
            .Where(m => olderRevisions || m.Deadline != ExpectationWindow.Uncovered)
            .ToArray();
        if (moves.Length == 0)
        {
            return 0;
        }
        string Completions(string bound, string comparison, string aggregate) =>
            $"(SELECT {aggregate}(x.c) FROM (VALUES (s.met_at), (s.first_after), (s.last_after)) AS x (c) WHERE x.c {comparison} {bound})";
        const string Bound = "COALESCE(v.n, " + Tsql.NeverLiteral + ")";
        const string Moves = "OPENJSON(@moves) WITH (w datetime2(7) '$.w', n datetime2(7) '$.n')";
        return await InTransactionAsync(async work =>
        {
            await using var command = Tsql.Command(work, $"""
                DECLARE @moved nvarchar(max) = N'';
                {Tsql.LockEach("moved_scan", "2147483647",
                    $"SELECT s.[definition], s.subject_kind, s.subject_id FROM {states} AS s WHERE {Selection("s")} AND s.window_start IN (SELECT w FROM {Moves})",
                    states, StateKey, $"{Selection("r")} AND r.window_start IN (SELECT w FROM {Moves})", "@moved", nullable: true)}
                UPDATE s SET deadline_at = {Bound}, revision = @revision,
                    met_at = {Completions(Bound, "<", "MIN")}, first_after = {Completions(Bound, ">=", "MIN")}, last_after = {Completions(Bound, ">=", "MAX")}
                FROM {Tsql.KeyRows("@moved", StateKey)} AS m INNER LOOP JOIN {states} AS s WITH (FORCESEEK) ON s.[definition] = m.[definition] AND {SameKey("s", "m")}
                JOIN {Moves} AS v ON v.w = s.window_start;
                SELECT @@ROWCOUNT;
                """,
                Tsql.Text("@definition", definition, 200),
                Tsql.Int64("@revision", revision),
                Tsql.Json("@moves", Json(writer =>
                {
                    foreach (var (start, deadline) in moves)
                    {
                        writer.WriteStartObject();
                        SqlServerJobStore.WriteTime(writer, "w", start);
                        SqlServerJobStore.WriteTime(writer, "n", Deadline(deadline));
                        writer.WriteEndObject();
                    }
                })));
            return Convert.ToInt32(await command.ScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        }, cancellationToken);
    }

    /// <summary>
    /// Locks the definition's rows whose deadline passed by the store's clock without waiting, found through the deadline
    /// index with the clock read once as its bound, applies <paramref name="process"/> to each with a clock reading taken
    /// after the locks, then stores the processed windows and inserts the incidents of their missed deadlines: an
    /// incident a late completion resolved is inserted resolved, an open one only when the key has no open completion
    /// incident. Then reads the definition's earliest deadline.
    /// </summary>
    public async ValueTask<DeadlinePass> ProcessDeadlinesAsync(string definition, Func<ExpectationWindow, DateTimeOffset, ExpectationStep> process, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(process);
        var pass = await InTransactionAsync(async work =>
        {
            var rows = new List<(Subject? Subject, ExpectationStep Step)>();
            await using (var select = Tsql.Command(work, $"""
                DECLARE @now datetime2(7) = SYSUTCDATETIME();
                DECLARE @due nvarchar(max) = N'';
                {Tsql.LockEach("deadline_scan", "@limit",
                    $"SELECT s.[definition], s.subject_kind, s.subject_id FROM {states} AS s WITH (INDEX(job_expectation_states_deadline)) "
                    + "WHERE s.[definition] = @definition AND s.deadline_at IS NOT NULL AND s.deadline_at <= @now ORDER BY s.deadline_at",
                    states, StateKey, "r.deadline_at <= @now", "@due", nullable: true)}
                SELECT s.subject_kind, s.subject_id, s.deadline_at, s.window_start, s.met_at, s.first_after, s.last_after, SYSUTCDATETIME()
                FROM {Tsql.KeyRows("@due", StateKey)} AS d INNER LOOP JOIN {states} AS s WITH (FORCESEEK) ON s.[definition] = d.[definition] AND {SameKey("s", "d")}
                ORDER BY s.deadline_at;
                """,
                Tsql.Text("@definition", definition, 200),
                Tsql.Int32("@limit", limit)))
            await using (var reader = await select.ReaderAsync(cancellationToken))
            {
                while (await reader.NextRowAsync(cancellationToken))
                {
                    var window = new ExpectationWindow(Tsql.Time(reader, 2), Tsql.Time(reader, 3), Tsql.NullableTime(reader, 4), Tsql.NullableTime(reader, 5), Tsql.NullableTime(reader, 6));
                    var subject = reader.IsDBNull(0) ? null : Subject.Of(reader.GetString(0), reader.GetString(1));
                    rows.Add((subject, process(window, Tsql.Time(reader, 7))));
                }
            }
            var changes = IncidentChanges.None;
            if (rows.Count > 0)
            {
                var missed = rows.SelectMany(r => r.Step.Missed.Select(m => (r.Subject, Missed: m))).ToArray();
                await using var command = Tsql.Command(work, $"""
                    UPDATE s SET deadline_at = COALESCE(v.deadline_at, {Tsql.NeverLiteral}), window_start = v.window_start, met_at = v.met_at, first_after = v.first_after, last_after = v.last_after
                    FROM OPENJSON(@windows) WITH (subject_kind nvarchar(100) COLLATE {Tsql.Binary} '$.k', subject_id nvarchar(400) COLLATE {Tsql.Binary} '$.i', deadline_at datetime2(7) '$.d',
                        window_start datetime2(7) '$.w', met_at datetime2(7) '$.m', first_after datetime2(7) '$.f', last_after datetime2(7) '$.l') AS v
                    INNER LOOP JOIN {states} AS s WITH (FORCESEEK) ON s.[definition] = @definition AND {SameKey("s", "v")};
                    DECLARE @kind nvarchar(100), @subject nvarchar(400), @at datetime2(7), @resolved_at datetime2(7);
                    {Tsql.EachRow("missed", "@missed", [("@kind", "nvarchar(100)", "k"), ("@subject", "nvarchar(400)", "i"), ("@at", "datetime2(7)", "o"), ("@resolved_at", "datetime2(7)", "r")],
                        OpenIncident("id, kind, [definition], subject_kind, subject_id, opened_at, resolved_at, due_at",
                            $"VALUES (NEWID(), {(int)WorkIncidentKind.CompletesBy}, @definition, @kind, @subject, @at, @resolved_at, SYSUTCDATETIME())"))}
                    """,
                    Tsql.Text("@definition", definition, 200),
                    Tsql.Json("@windows", Json(writer =>
                    {
                        foreach (var (subject, step) in rows)
                        {
                            writer.WriteStartObject();
                            WriteSubject(writer, subject);
                            SqlServerJobStore.WriteTime(writer, "d", Deadline(step.Window.Deadline));
                            SqlServerJobStore.WriteTime(writer, "w", step.Window.WindowStart);
                            SqlServerJobStore.WriteTime(writer, "m", step.Window.MetAt);
                            SqlServerJobStore.WriteTime(writer, "f", step.Window.FirstAfter);
                            SqlServerJobStore.WriteTime(writer, "l", step.Window.LastAfter);
                            writer.WriteEndObject();
                        }
                    })),
                    Tsql.Json("@missed", Json(writer =>
                    {
                        foreach (var (subject, deadline) in missed)
                        {
                            writer.WriteStartObject();
                            WriteSubject(writer, subject);
                            SqlServerJobStore.WriteTime(writer, "o", deadline.At);
                            SqlServerJobStore.WriteTime(writer, "r", deadline.ResolvedAt);
                            writer.WriteEndObject();
                        }
                    })));
                List<WorkIncident> inserted;
                await using (var reader = await command.ReaderAsync(cancellationToken))
                {
                    inserted = await ReadIncidentSetsAsync(reader, cancellationToken);
                }
                if (inserted.Count > 0)
                {
                    await NotifyAsync(work.Connection, work.Transaction, cancellationToken);
                    changes = new IncidentChanges(inserted, [.. inserted.Where(i => i.ResolvedAt is not null)]);
                }
            }
            DateTimeOffset? earliest;
            await using (var command = Tsql.Command(work,
                $"SELECT MIN(s.deadline_at) FROM {states} AS s WHERE s.[definition] = @definition AND s.deadline_at IS NOT NULL AND s.deadline_at < {Tsql.NeverLiteral};",
                Tsql.Text("@definition", definition, 200)))
            await using (var reader = await command.ReaderAsync(cancellationToken))
            {
                await reader.NextRowAsync(cancellationToken);
                earliest = Tsql.NullableTime(reader, 0);
            }
            return new DeadlinePass(rows.Count, changes, earliest);
        }, cancellationToken);
        WorkTelemetry.Count(pass.Changes);
        return pass;
    }

    private static void WriteSubject(Utf8JsonWriter writer, Subject? subject)
    {
        if (subject is not null)
        {
            writer.WriteString("k", subject.Kind);
            writer.WriteString("i", subject.Id);
        }
    }

    /// <summary>A JSON array whose elements <paramref name="write"/> writes.</summary>
    private static string Json(Action<Utf8JsonWriter> write)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            write(writer);
            writer.WriteEndArray();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }
}
