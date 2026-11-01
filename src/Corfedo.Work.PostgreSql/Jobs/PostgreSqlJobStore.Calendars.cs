using Npgsql;
using NpgsqlTypes;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Jobs;

internal sealed partial class PostgreSqlJobStore
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
    /// Locks, skipping locked rows, the definition's unheld outstanding jobs whose calendar instant has come
    /// (<c>jobs_calendar_due</c>) and those whose window closed before they started (<c>jobs_window</c>), reads the
    /// store's clock after the locks and decides each with the stored calendars:
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
    /// One statement writes every decision. The next instant to wake for is the earliest calendar instant or window end
    /// still to come.
    /// </summary>
    public async ValueTask<CalendarPass> PlanCalendarAsync(LeaseSource<JobId> source, int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var sql = Sql(source);
        var definition = sql.Job!.Definition;
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        var work = new PostgreSqlTransaction(transaction);
        var instants = new List<(Guid Id, string Calendar, DateTimeOffset Instant, Blocked Blocked, Closed Closed)>();
        var closed = new List<(Guid Id, string Calendar, Closed Closed)>();
        DateTimeOffset now;
        await using (var batch = new NpgsqlBatch(connection, transaction))
        {
            batch.BatchCommands.Add(Command(
                $"SELECT r.id, r.calendar, r.calendar_at, r.calendar_blocked, r.calendar_closed FROM {sql.Table} AS r "
                + "WHERE r.definition = $1 AND r.calendar_at <= now() AND r.state = 0 AND NOT r.calendar_unavailable AND r.lease_expires_at IS NULL "
                + "ORDER BY r.calendar_at LIMIT $2 FOR UPDATE OF r SKIP LOCKED",
                Text(definition),
                Int32(limit)));
            batch.BatchCommands.Add(Command(
                $"SELECT r.id, r.calendar, r.calendar_closed FROM {sql.Table} AS r "
                + "WHERE r.definition = $1 AND r.window_end <= now() AND r.lease_expires_at IS NULL AND r.state = 0 "
                + "ORDER BY r.window_end LIMIT $2 FOR UPDATE OF r SKIP LOCKED",
                Text(definition),
                Int32(limit)));
            batch.BatchCommands.Add(Command("SELECT clock_timestamp()"));
            await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                instants.Add((reader.GetGuid(0), reader.GetString(1), reader.GetFieldValue<DateTimeOffset>(2), (Blocked)reader.GetInt16(3), (Closed)reader.GetInt16(4)));
            }
            await reader.NextResultAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                closed.Add((reader.GetGuid(0), reader.GetString(1), (Closed)reader.GetInt16(2)));
            }
            await reader.NextResultAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            now = reader.GetFieldValue<DateTimeOffset>(0);
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
            await using var write = new NpgsqlCommand(
                $"UPDATE {sql.Table} AS r SET "
                + $"due_at = CASE WHEN u.decision = {(short)CalendarDecision.Planned} THEN COALESCE(u.due, r.due_at) END, "
                + $"window_end = CASE WHEN u.decision = {(short)CalendarDecision.Planned} THEN u.window_end END, "
                + $"calendar_at = CASE WHEN u.decision = {(short)CalendarDecision.Unavailable} THEN u.instant END, "
                + $"calendar_unavailable = u.decision = {(short)CalendarDecision.Unavailable}, "
                + $"calendar_blocked = CASE WHEN u.moves THEN {(short)Blocked.MoveForward} ELSE r.calendar_blocked END, "
                + $"skip_pending = u.decision = {(short)CalendarDecision.Skip}, skip_reason = u.reason "
                + "FROM unnest($1::uuid[], $2::int2[], $3::timestamptz[], $4::timestamptz[], $5::timestamptz[], $6::text[], $7::bool[]) AS u(id, decision, due, window_end, instant, reason, moves) "
                + "WHERE r.id = u.id",
                connection,
                transaction);
            write.Parameters.Add(Uuids([.. plans.Select(p => p.Id)]));
            write.Parameters.Add(new NpgsqlParameter<short[]> { TypedValue = [.. plans.Select(p => (short)p.Decision)], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Smallint });
            write.Parameters.Add(Instants(plans.Select(p => p.Due)));
            write.Parameters.Add(Instants(plans.Select(p => p.WindowEnd)));
            write.Parameters.Add(Instants(plans.Select(p => p.Instant)));
            write.Parameters.Add(new NpgsqlParameter<string?[]> { TypedValue = [.. plans.Select(p => p.Reason)], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text });
            write.Parameters.Add(new NpgsqlParameter<bool[]> { TypedValue = [.. plans.Select(p => p.MovesForward)], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Boolean });
            await write.ExecuteNonQueryAsync(cancellationToken);
            if (plans.Any(p => p.Decision == CalendarDecision.Planned))
            {
                await NotifyDueAsync(work, source, cancellationToken);
            }
        }
        TimeSpan? nextIn;
        await using (var next = new NpgsqlCommand(
            "SELECT extract(epoch FROM min(t) - clock_timestamp())::float8 FROM ("
            + $"(SELECT r.calendar_at AS t FROM {sql.Table} AS r WHERE r.definition = $1 AND r.calendar_at > now() AND r.state = 0 AND NOT r.calendar_unavailable "
            + "AND r.lease_expires_at IS NULL ORDER BY r.calendar_at LIMIT 1) UNION ALL "
            + $"(SELECT r.window_end FROM {sql.Table} AS r WHERE r.definition = $1 AND r.window_end > now() AND r.lease_expires_at IS NULL AND r.state = 0 "
            + "ORDER BY r.window_end LIMIT 1)) AS next",
            connection,
            transaction))
        {
            next.Parameters.Add(Text(definition));
            nextIn = await next.ExecuteScalarAsync(cancellationToken) is double seconds ? Seconds(seconds) : null;
        }
        await transaction.CommitAsync(cancellationToken);
        return new(instants.Count, closed.Count, nextIn);
    }

    /// <summary>Plans a job at the allowed instant of <paramref name="slot"/>, not before <paramref name="from"/>, or as the slot decides.</summary>
    private static CalendarPlan Plan(Guid id, CalendarSlot slot, DateTimeOffset from) => slot.Kind switch
    {
        CalendarSlotKind.Allowed => new(id, CalendarDecision.Planned, slot.Start > from ? slot.Start : from, slot.End, null, null),
        CalendarSlotKind.Unavailable => new(id, CalendarDecision.Unavailable, null, null, from, null, MovesForward: true),
        _ => new(id, CalendarDecision.Skip, null, null, null, JobReasons.CalendarBlocked),
    };

    private static NpgsqlParameter<DateTimeOffset?[]> Instants(IEnumerable<DateTimeOffset?> values) =>
        new() { TypedValue = [.. values.Select(v => v?.ToUniversalTime())], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.TimestampTz };
}
