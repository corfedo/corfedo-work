using System.Buffers;
using System.Data.SqlTypes;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Corfedo.Work.SqlServer.Leases;

namespace Corfedo.Work.SqlServer.Jobs;

internal sealed partial class SqlServerJobStore
{
    /// <summary>What calendar planning decided for one job.</summary>
    private enum CalendarDecision : short
    {
        /// <summary>Due at the planned time, startable until the window end; a null time keeps the due time.</summary>
        Planned = 0,

        /// <summary>Recorded to be skipped with a reason.</summary>
        Skip = 1,

        /// <summary>Waiting for a date set to cover its instant, planned from that instant once it does.</summary>
        Unavailable = 2,
    }

    /// <summary>
    /// One decision. <see cref="MovesForward"/> says that a job left waiting for dates is planned at the next allowed
    /// instant from <see cref="Instant"/> once they are published, whatever its blocked policy was.
    /// </summary>
    private readonly record struct CalendarPlan(Guid Id, CalendarDecision Decision, DateTimeOffset? Due, DateTimeOffset? WindowEnd, DateTimeOffset? Instant, string? Reason, bool MovesForward = false);

    /// <summary>
    /// Locks through their key, skipping locked rows, the definition's unheld outstanding jobs whose calendar instant has
    /// come (found through <c>jobs_calendar_due</c>) and those whose window closed before they started
    /// (<c>jobs_window</c>), reads the store's clock after the locks and decides each with the stored calendars:
    /// <list type="bullet">
    /// <item>An instant the calendar allows is the due time, with the end of its window. A blocked one is moved to the
    /// next allowed instant under <see cref="Blocked.MoveForward"/>, and otherwise recorded to be skipped with
    /// <c>calendar_blocked</c>.</item>
    /// <item>An instant whose window has already closed, also after moving forward, and a planned job whose window
    /// closed, keep their due time when the calendar allows now, since the window continues; otherwise they follow the
    /// closed policy: planned at the next allowed instant, or recorded to be skipped with <c>window_closed</c>.</item>
    /// <item>A job that needs dates a date set does not cover waits, out of both indexes, until a covering revision is
    /// published; one with no allowed instant within the lookahead is recorded to be skipped.</item>
    /// </list>
    /// One statement writes every decision, in primary-key order. The next instant to wake for is the earliest calendar
    /// instant or window end still to come.
    /// </summary>
    public async ValueTask<CalendarPass> PlanCalendarAsync(LeaseSource<JobId> source, int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var sql = await SqlAsync(source, cancellationToken);
        return await InTransactionAsync(work => PlanCalendarAsync(work, source, sql, limit, cancellationToken), cancellationToken);
    }

    private async Task<CalendarPass> PlanCalendarAsync(SqlServerTransaction work, LeaseSource<JobId> source, LeaseSql sql, int limit, CancellationToken cancellationToken)
    {
        var definition = sql.Job!.Definition;
        var instants = new List<(Guid Id, string Calendar, DateTimeOffset Instant, Blocked Blocked, Closed Closed)>();
        var closed = new List<(Guid Id, string Calendar, Closed Closed)>();
        DateTimeOffset now;
        const string instantDue = "[definition] = @definition AND calendar_at IS NOT NULL AND calendar_at <= @now AND state = 0 AND calendar_unavailable = 0 AND lease_expires_at IS NULL";
        const string windowClosed = "[definition] = @definition AND window_end IS NOT NULL AND window_end <= @now AND lease_expires_at IS NULL AND state = 0";
        static string On(string alias, string condition) => alias + "." + condition.Replace(" AND ", $" AND {alias}.", StringComparison.Ordinal);
        await using (var locks = Tsql.Command(work, $"""
            DECLARE @now datetime2(7) = SYSUTCDATETIME();
            {sql.ScanVariables("@instants", "@closed")}
            {sql.LockScan("instant_scan", "@limit", "jobs_calendar_due", On("x", instantDue), On("r", instantDue), "x.calendar_at", "@instants")}
            {sql.LockScan("window_scan", "@limit", "jobs_window", On("x", windowClosed), On("r", windowClosed), "x.window_end", "@closed")}
            SELECT r.id, r.calendar, r.calendar_at, r.calendar_blocked, r.calendar_closed FROM {sql.KeyedRows(keys: LeaseSql.ListJson("@instants"))} ORDER BY r.calendar_at;
            SELECT r.id, r.calendar, r.calendar_closed FROM {sql.KeyedRows(keys: LeaseSql.ListJson("@closed"))};
            SELECT SYSUTCDATETIME();
            """,
            Tsql.Text("@definition", definition, 200),
            Tsql.Int32("@limit", limit)))
        await using (var reader = await locks.ReaderAsync(cancellationToken))
        {
            while (await reader.NextRowAsync(cancellationToken))
            {
                instants.Add((reader.GetGuid(0), reader.GetString(1), Tsql.Time(reader, 2), (Blocked)reader.GetInt16(3), (Closed)reader.GetInt16(4)));
            }
            await reader.NextSetAsync(cancellationToken);
            while (await reader.NextRowAsync(cancellationToken))
            {
                closed.Add((reader.GetGuid(0), reader.GetString(1), (Closed)reader.GetInt16(2)));
            }
            await reader.NextSetAsync(cancellationToken);
            await reader.NextRowAsync(cancellationToken);
            now = Tsql.Time(reader, 0);
        }

        var plans = new List<CalendarPlan>(instants.Count + closed.Count);
        if (instants.Count + closed.Count > 0)
        {
            var evaluator = await calendars.LoadAsync(work, cancellationToken);
            // Every job whose window closed is decided by the calendar at now, once per calendar.
            var atNow = new Dictionary<string, CalendarSlot>(StringComparer.Ordinal);
            CalendarSlot Now(string calendar) => atNow.TryGetValue(calendar, out var slot) ? slot : atNow[calendar] = evaluator.Next(calendar, now);
            CalendarPlan CloseWindow(Guid id, string calendar, Closed policy, DateTimeOffset? keep)
            {
                var slot = Now(calendar);
                if (slot.Kind == CalendarSlotKind.Allowed && slot.Start == now)
                {
                    return new(id, CalendarDecision.Planned, keep, slot.End, null, null);
                }
                return policy == Closed.Skip ? new(id, CalendarDecision.Skip, null, null, null, JobReasons.WindowClosed) : Plan(id, slot, now);
            }
            foreach (var (id, calendar, instant, blocked, policy) in instants)
            {
                var slot = evaluator.Next(calendar, instant);
                if (slot.Kind == CalendarSlotKind.Allowed && slot.Start == instant)
                {
                    plans.Add(slot.End is { } end && end <= now ? CloseWindow(id, calendar, policy, instant) : new(id, CalendarDecision.Planned, instant, slot.End, null, null));
                }
                else if (slot.Kind == CalendarSlotKind.Unavailable)
                {
                    plans.Add(new(id, CalendarDecision.Unavailable, null, null, instant, null));
                }
                else if (blocked != Blocked.MoveForward || slot.Kind == CalendarSlotKind.None)
                {
                    plans.Add(new(id, CalendarDecision.Skip, null, null, null, JobReasons.CalendarBlocked));
                }
                else
                {
                    // The next allowed window may also have closed already; then the closed policy applies to it.
                    plans.Add(slot.End is { } end && end <= now ? CloseWindow(id, calendar, policy, slot.Start) : Plan(id, slot, instant));
                }
            }
            foreach (var (id, calendar, policy) in closed)
            {
                plans.Add(CloseWindow(id, calendar, policy, null));
            }
            // A job found by both scans is decided once, by its calendar instant.
            await using var write = Tsql.Command(work, $"""
                UPDATE r SET
                    due_at = CASE WHEN u.decision = {(short)CalendarDecision.Planned} THEN COALESCE(u.due, r.due_at) END,
                    window_end = CASE WHEN u.decision = {(short)CalendarDecision.Planned} THEN u.window_end END,
                    calendar_at = CASE WHEN u.decision = {(short)CalendarDecision.Unavailable} THEN u.instant END,
                    calendar_unavailable = CASE WHEN u.decision = {(short)CalendarDecision.Unavailable} THEN 1 ELSE 0 END,
                    calendar_blocked = CASE WHEN u.moves = 1 THEN {(short)Blocked.MoveForward} ELSE r.calendar_blocked END,
                    skip_pending = CASE WHEN u.decision = {(short)CalendarDecision.Skip} THEN 1 ELSE 0 END, skip_reason = u.reason
                FROM OPENJSON(@decisions) WITH (id uniqueidentifier '$.i', decision smallint '$.d', due datetime2(7) '$.t', window_end datetime2(7) '$.w',
                    instant datetime2(7) '$.n', reason nvarchar(max) '$.r', moves bit '$.m') AS u
                INNER LOOP JOIN {sql.Table} AS r WITH (INDEX(jobs_key), FORCESEEK) ON r.id = u.id;
                """,
                Tsql.Json("@decisions", PlansJson(plans.DistinctBy(p => p.Id).OrderBy(p => new SqlGuid(p.Id)))));
            await write.NonQueryAsync(cancellationToken);
            if (plans.Any(p => p.Decision == CalendarDecision.Planned))
            {
                await NotifyDueAsync(work, source, cancellationToken);
            }
        }
        TimeSpan? nextIn;
        await using (var next = Tsql.Command(work, $"""
            DECLARE @now datetime2(7) = SYSUTCDATETIME();
            SELECT {Tsql.Micros("SYSUTCDATETIME()", "MIN(n.t)")} FROM (
                SELECT MIN(r.calendar_at) FROM {sql.Table} AS r WITH (INDEX(jobs_calendar_due))
                WHERE r.[definition] = @definition AND r.calendar_at IS NOT NULL AND r.calendar_at > @now AND r.state = 0 AND r.calendar_unavailable = 0 AND r.lease_expires_at IS NULL
                UNION ALL
                SELECT MIN(r.window_end) FROM {sql.Table} AS r WITH (INDEX(jobs_window))
                WHERE r.[definition] = @definition AND r.window_end IS NOT NULL AND r.window_end > @now AND r.lease_expires_at IS NULL AND r.state = 0) AS n (t);
            """,
            Tsql.Text("@definition", definition, 200)))
        {
            nextIn = Tsql.Wait(await next.ScalarAsync(cancellationToken));
        }
        return new(instants.Count, closed.Count, nextIn);
    }

    /// <summary>Plans a job at the allowed instant of <paramref name="slot"/>, not before <paramref name="from"/>, or as the slot decides.</summary>
    private static CalendarPlan Plan(Guid id, CalendarSlot slot, DateTimeOffset from) => slot.Kind switch
    {
        CalendarSlotKind.Allowed => new(id, CalendarDecision.Planned, slot.Start > from ? slot.Start : from, slot.End, null, null),
        CalendarSlotKind.Unavailable => new(id, CalendarDecision.Unavailable, null, null, from, null, MovesForward: true),
        _ => new(id, CalendarDecision.Skip, null, null, null, JobReasons.CalendarBlocked),
    };

    /// <summary>The decisions as a JSON array; times in UTC as <c>datetime2</c> reads them.</summary>
    private static string PlansJson(IEnumerable<CalendarPlan> plans)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            foreach (var plan in plans)
            {
                writer.WriteStartObject();
                writer.WriteString("i", plan.Id);
                writer.WriteNumber("d", (short)plan.Decision);
                WriteTime(writer, "t", plan.Due);
                WriteTime(writer, "w", plan.WindowEnd);
                WriteTime(writer, "n", plan.Instant);
                if (plan.Reason is { } reason)
                {
                    writer.WriteString("r", reason);
                }
                writer.WriteBoolean("m", plan.MovesForward);
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    /// <summary>Writes a time as <c>datetime2</c> text in UTC, or nothing for null.</summary>
    internal static void WriteTime(Utf8JsonWriter writer, string name, DateTimeOffset? value)
    {
        if (value is { } time)
        {
            writer.WriteString(name, time.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff", CultureInfo.InvariantCulture));
        }
    }
}
