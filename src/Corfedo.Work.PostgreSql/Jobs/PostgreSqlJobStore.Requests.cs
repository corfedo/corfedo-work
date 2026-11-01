using Corfedo.Work.PostgreSql.Leases;
using Npgsql;
using NpgsqlTypes;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Jobs;

internal sealed partial class PostgreSqlJobStore
{
    /// <summary>Rounds whose items all had to be retried before the request gives up; each retry follows a concurrent end.</summary>
    private const int RequestRetries = 3;

    public async ValueTask<JobRequestResult> RequestAsync(WorkTransaction transaction, LeaseSource<JobId> source, JobRequestData request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        return (await RequestManyAsync(transaction, source, [request], cancellationToken))[0];
    }

    public async ValueTask<JobRequestResult> RequestAsync(LeaseSource<JobId> source, JobRequestData request, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
        var result = await RequestAsync(new PostgreSqlTransaction(transaction).Work, source, request, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return result;
    }

    /// <summary>
    /// Creates or merges standalone jobs; recurrences of the same identity are not consulted. Applies the requests in
    /// rounds in which each merge identity appears once, so a later request of an identity sees what an earlier one
    /// wrote. Within a round, items with one mode and schedule are one <c>INSERT ... ON CONFLICT</c> statement on the
    /// outstanding-identity index; <c>DO UPDATE</c> locks an existing row before its <c>SET</c> is evaluated with
    /// <c>clock_timestamp()</c>. Items the statement did not write met an outstanding job of another partition, which is
    /// refused, or under <c>IfAbsent</c> an existing one; an item whose conflicting job ended meanwhile is applied again.
    /// </summary>
    public async ValueTask<IReadOnlyList<JobRequestResult>> RequestManyAsync(WorkTransaction transaction, LeaseSource<JobId> source, IReadOnlyList<JobRequestData> requests, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(requests);
        var work = PostgreSqlTransaction.From(transaction);
        var sql = Sql(source);
        await CheckCalendarsAsync(work, requests, cancellationToken);
        var results = new JobRequestResult[requests.Count];
        var pending = Enumerable.Range(0, requests.Count).ToList();
        var stalled = 0;
        while (pending.Count > 0)
        {
            var round = new List<int>();
            var later = new List<int>();
            var identities = new HashSet<string>(StringComparer.Ordinal);
            foreach (var i in pending)
            {
                (requests[i].MergeIdentity is not { } identity || identities.Add(identity) ? round : later).Add(i);
            }
            var retry = new List<int>();
            foreach (var group in round.GroupBy(i => (requests[i].Mode, requests[i].Schedule)))
            {
                retry.AddRange(await RequestGroupAsync(work, sql, requests, [.. group], results, cancellationToken));
            }
            stalled = retry.Count == round.Count ? stalled + 1 : 0;
            if (stalled > RequestRetries)
            {
                throw new InvalidOperationException($"Requests of job definition {sql.Job!.Definition} kept meeting jobs that ended concurrently.");
            }
            pending = [.. retry.Concat(later).Order()];
        }
        if (requests.Any(r => r.Schedule.Calendar is null))
        {
            await NotifyDueAsync(work, source, cancellationToken);
        }
        if (requests.Any(r => r.Schedule.Calendar is not null))
        {
            await NotifyPlanAsync(work, source, cancellationToken);
        }
        return results;
    }

    /// <summary>
    /// Refuses requests whose calendar is not stored, and under <see cref="Blocked.Reject"/> requests whose instant,
    /// computed with the store's clock, the calendar blocks or cannot decide yet; before anything is written.
    /// </summary>
    private async Task CheckCalendarsAsync(PostgreSqlTransaction work, IReadOnlyList<JobRequestData> requests, CancellationToken cancellationToken)
    {
        if (!requests.Any(r => r.Schedule.Calendar is not null))
        {
            return;
        }
        var evaluator = await calendars.LoadAsync(work, cancellationToken);
        DateTimeOffset? now = null;
        foreach (var schedule in requests.Select(r => r.Schedule).Where(s => s.Calendar is not null).Distinct())
        {
            if (!evaluator.Contains(schedule.Calendar!))
            {
                throw new ArgumentException($"Calendar {schedule.Calendar} is not stored; register it with AddCalendar.", nameof(requests));
            }
            if (schedule.Blocked != Blocked.Reject)
            {
                continue;
            }
            now ??= await ClockAsync(work, cancellationToken);
            var instant = schedule.Kind switch
            {
                ScheduleKind.At => schedule.Time,
                ScheduleKind.After or ScheduleKind.Debounce => now.Value + schedule.Delay,
                _ => now.Value,
            };
            var slot = evaluator.Next(schedule.Calendar!, instant);
            if (slot.Kind == CalendarSlotKind.Unavailable)
            {
                throw new CalendarBlockedException($"Calendar {schedule.Calendar} cannot decide {instant:O} yet: a date set does not cover it.", unavailable: true);
            }
            if (slot.Kind != CalendarSlotKind.Allowed || slot.Start != instant)
            {
                throw new CalendarBlockedException($"Calendar {schedule.Calendar} blocks {instant:O}.", unavailable: false);
            }
        }
    }

    /// <summary>The store's clock, read in the transaction.</summary>
    private static async Task<DateTimeOffset> ClockAsync(PostgreSqlTransaction work, CancellationToken cancellationToken)
    {
        await using var clock = new NpgsqlCommand("SELECT clock_timestamp()", work.Connection, work.Transaction);
        await using var reader = await clock.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return reader.GetFieldValue<DateTimeOffset>(0);
    }

    /// <summary>Applies items with distinct merge identities, one mode and one schedule; returns the items to apply again.</summary>
    private async Task<List<int>> RequestGroupAsync(PostgreSqlTransaction work, LeaseSql sql, IReadOnlyList<JobRequestData> requests, int[] items, JobRequestResult[] results, CancellationToken cancellationToken)
    {
        var first = requests[items[0]];
        var schedule = first.Schedule;
        var ids = items.Select(_ => JobId.New().Value).ToArray();
        var parameters = new List<NpgsqlParameter>
        {
            Text(sql.Job!.Definition),
            Uuids(ids),
            TextArray(items.Select(i => requests[i].MergeIdentity)),
            TextArray(items.Select(i => requests[i].Subject?.Kind)),
            TextArray(items.Select(i => requests[i].Subject?.Id)),
            TextArray(items.Select(i => requests[i].Partition ?? "")),
            new NpgsqlParameter<byte[][]> { TypedValue = [.. items.Select(i => requests[i].Args)], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bytea },
            new NpgsqlParameter<int[]> { TypedValue = [.. items.Select(i => requests[i].ArgsVersion)], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer },
            TextArray(items.Select(i => requests[i].TraceParent)),
        };
        string Parameter(NpgsqlParameter parameter)
        {
            parameters.Add(parameter);
            return "$" + parameters.Count;
        }
        var (due, maximum) = Timing(schedule, Parameter);
        string Merged(string current, string firstRequested) => MergedDue(maximum, current, firstRequested);
        // A job with a calendar keeps its requested instant as the calendar instant, without a due time, until its
        // definition's maintenance plans it.
        var (calendar, blocked, closed) = schedule.Calendar is null
            ? ("NULL::text", "NULL::int2", "NULL::int2")
            : (Parameter(Text(schedule.Calendar)), Parameter(new NpgsqlParameter<short> { TypedValue = (short)schedule.Blocked }), Parameter(new NpgsqlParameter<short> { TypedValue = (short)schedule.Closed }));
        var (dueAt, calendarAt) = schedule.Calendar is null ? (due, "NULL::timestamptz") : ("NULL::timestamptz", due);
        // Before the execution started, and while no claim holds or held its lease and it is not to be skipped, the
        // request replaces the arguments, the calendar and its policies and merges the instant; otherwise it becomes the
        // pending follow-up, which runs under the job's calendar. The condition does not depend on the clock, so the
        // outcome reported matches the branch taken.
        const string followUp = "(j.started OR j.lease_expires_at IS NOT NULL OR j.skip_pending)";
        const string merging = "COALESCE(j.calendar_at, j.due_at)";
        var conflict = first.Mode switch
        {
            RequestMode.New => "",
            RequestMode.IfAbsent => "ON CONFLICT (definition, merge_key) WHERE merge_key IS NOT NULL AND state = 0 DO NOTHING ",
            _ => "ON CONFLICT (definition, merge_key) WHERE merge_key IS NOT NULL AND state = 0 DO UPDATE SET "
                + $"args = CASE WHEN {followUp} THEN j.args ELSE EXCLUDED.args END, "
                + $"args_version = CASE WHEN {followUp} THEN j.args_version ELSE EXCLUDED.args_version END, "
                + $"due_at = CASE WHEN {followUp} THEN j.due_at WHEN EXCLUDED.calendar IS NULL THEN {Merged(merging, "j.first_requested_at")} END, "
                + $"calendar_at = CASE WHEN {followUp} THEN j.calendar_at WHEN EXCLUDED.calendar IS NOT NULL THEN {Merged(merging, "j.first_requested_at")} END, "
                + $"window_end = CASE WHEN {followUp} THEN j.window_end END, "
                + $"calendar_unavailable = {followUp} AND j.calendar_unavailable, "
                + $"calendar = CASE WHEN {followUp} THEN j.calendar ELSE EXCLUDED.calendar END, "
                + $"calendar_blocked = CASE WHEN {followUp} THEN j.calendar_blocked ELSE EXCLUDED.calendar_blocked END, "
                + $"calendar_closed = CASE WHEN {followUp} THEN j.calendar_closed ELSE EXCLUDED.calendar_closed END, "
                + $"pending_args = CASE WHEN {followUp} THEN EXCLUDED.args ELSE j.pending_args END, "
                + $"pending_args_version = CASE WHEN {followUp} THEN EXCLUDED.args_version ELSE j.pending_args_version END, "
                + $"pending_due = CASE WHEN NOT {followUp} THEN j.pending_due WHEN j.pending_first_requested_at IS NULL THEN {Requested} "
                + $"ELSE {Merged("j.pending_due", "j.pending_first_requested_at")} END, "
                + $"pending_first_requested_at = CASE WHEN {followUp} THEN COALESCE(j.pending_first_requested_at, clock_timestamp()) ELSE j.pending_first_requested_at END "
                + "WHERE j.partition_key = EXCLUDED.partition_key ",
        };
        // The pause flag is copied from the stored subject pauses; claims check the stored pause itself. A merged
        // request keeps the trace of the request that created the job.
        var text = $"INSERT INTO {sql.Table} AS j (id, definition, merge_key, subject_kind, subject_id, partition_key, args, args_version, "
            + "first_requested_at, created_at, due_at, calendar_at, calendar, calendar_blocked, calendar_closed, execution_id, paused, trace_parent) "
            + $"SELECT u.id, $1, u.merge_key, u.subject_kind, u.subject_id, u.partition_key, u.args, u.args_version, clock_timestamp(), clock_timestamp(), "
            + $"{dueAt}, {calendarAt}, {calendar}, {blocked}, {closed}, gen_random_uuid(), "
            + $"u.subject_kind IS NOT NULL AND EXISTS (SELECT 1 FROM {pauses} AS p WHERE p.definition = $1 AND p.subject_kind = u.subject_kind AND p.subject_id = u.subject_id), u.trace_parent "
            + "FROM unnest($2::uuid[], $3::text[], $4::text[], $5::text[], $6::text[], $7::bytea[], $8::int4[], $9::text[]) "
            + "AS u(id, merge_key, subject_kind, subject_id, partition_key, args, args_version, trace_parent) "
            + $"{conflict}RETURNING j.id, j.merge_key, j.xmax = 0, {followUp}";
        await using (var command = new NpgsqlCommand(text, work.Connection, work.Transaction))
        {
            command.Parameters.AddRange(parameters.ToArray());
            var byId = new Dictionary<Guid, int>(items.Length);
            var byIdentity = new Dictionary<string, int>(StringComparer.Ordinal);
            for (var n = 0; n < items.Length; n++)
            {
                byId[ids[n]] = items[n];
                if (requests[items[n]].MergeIdentity is { } identity)
                {
                    byIdentity[identity] = items[n];
                }
            }
            var written = new HashSet<int>();
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var id = reader.GetGuid(0);
                var item = byId.TryGetValue(id, out var inserted) ? inserted : byIdentity[reader.GetString(1)];
                results[item] = new JobRequestResult(
                    new JobId(id),
                    reader.GetBoolean(2) ? JobRequestOutcome.Created : reader.GetBoolean(3) ? JobRequestOutcome.FollowUp : JobRequestOutcome.Merged);
                written.Add(item);
            }
            items = [.. items.Where(i => !written.Contains(i))];
        }
        return items.Length == 0 ? [] : await ResolveUnwrittenAsync(work, sql, requests, items, results, cancellationToken);
    }

    /// <summary>
    /// The due time of a request as SQL, with parameters added through <paramref name="parameter"/>, and the debounce
    /// ceiling for <see cref="MergedDue"/>, or null when the schedule is no debounce.
    /// </summary>
    private static (string Due, string? Maximum) Timing(Schedule schedule, Func<NpgsqlParameter, string> parameter)
    {
        var due = schedule.Kind switch
        {
            ScheduleKind.Now => "clock_timestamp()",
            ScheduleKind.At => parameter(new NpgsqlParameter<DateTimeOffset> { TypedValue = schedule.Time.ToUniversalTime(), NpgsqlDbType = NpgsqlDbType.TimestampTz }),
            ScheduleKind.After or ScheduleKind.Debounce => "clock_timestamp() + " + parameter(Interval(schedule.Delay)),
            _ => throw new ArgumentException($"A request takes a one-shot schedule, not {schedule.Kind}.", nameof(schedule)),
        };
        return (due, schedule.Kind == ScheduleKind.Debounce ? parameter(Interval(schedule.Maximum)) : null);
    }

    /// <summary>
    /// The instant of an outstanding job <c>j</c> merged with a request's (<see cref="Requested"/>): the earliest, except
    /// that a debounce moves it later up to its <paramref name="maximum"/> from the first outstanding request.
    /// </summary>
    private static string MergedDue(string? maximum, string current, string firstRequested) => maximum is null
        ? $"LEAST({current}, {Requested})"
        : $"LEAST({Requested}, {firstRequested} + {maximum})";

    /// <summary>The instant a request asks for: its calendar instant with a calendar, its due time otherwise.</summary>
    private const string Requested = "COALESCE(EXCLUDED.calendar_at, EXCLUDED.due_at)";

    /// <summary>
    /// Finds the outstanding jobs that kept items from being written: another partition's job is refused and an
    /// <c>IfAbsent</c> item reports the existing job. Items whose job ended meanwhile are returned to be applied again.
    /// </summary>
    private static async Task<List<int>> ResolveUnwrittenAsync(PostgreSqlTransaction work, LeaseSql sql, IReadOnlyList<JobRequestData> requests, int[] items, JobRequestResult[] results, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT merge_key, id, partition_key FROM {sql.Table} WHERE definition = $1 AND merge_key = ANY($2) AND state = 0",
            work.Connection,
            work.Transaction);
        command.Parameters.Add(Text(sql.Job!.Definition));
        command.Parameters.Add(TextArray(items.Select(i => requests[i].MergeIdentity)));
        var outstanding = new Dictionary<string, (Guid Id, string Partition)>(StringComparer.Ordinal);
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                outstanding[reader.GetString(0)] = (reader.GetGuid(1), reader.GetString(2));
            }
        }
        var retry = new List<int>();
        foreach (var item in items)
        {
            var request = requests[item];
            if (!outstanding.TryGetValue(request.MergeIdentity!, out var job))
            {
                retry.Add(item);
                continue;
            }
            if (job.Partition != (request.Partition ?? ""))
            {
                throw new JobPartitionMismatchException(
                    $"The outstanding job {job.Id} of {sql.Job.Definition} with this identity belongs to another partition; a request cannot merge into it.");
            }
            results[item] = new JobRequestResult(new JobId(job.Id), JobRequestOutcome.Existing);
        }
        return retry;
    }

    private static NpgsqlParameter<string?[]> TextArray(IEnumerable<string?> values) =>
        new() { TypedValue = [.. values], NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text };
}
