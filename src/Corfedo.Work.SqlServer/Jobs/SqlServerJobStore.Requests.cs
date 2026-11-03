using System.Buffers;
using System.Text;
using System.Text.Json;
using Corfedo.Work.SqlServer.Leases;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Jobs;

internal sealed partial class SqlServerJobStore
{
    /// <summary>Rounds whose items all had to be retried before the request gives up; each retry follows a concurrent end.</summary>
    private const int RequestRetries = 3;

    /// <summary>The kinds of written items a request batch reports.</summary>
    private const int Created = 0, Merged = 1, FollowUp = 2;

    public async ValueTask<JobRequestResult> RequestAsync(WorkTransaction transaction, LeaseSource<JobId> source, JobRequestData request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        return (await RequestManyAsync(transaction, source, [request], cancellationToken))[0];
    }

    public async ValueTask<JobRequestResult> RequestAsync(LeaseSource<JobId> source, JobRequestData request, CancellationToken cancellationToken) =>
        await InTransactionAsync(async work => await RequestAsync(work.Work, source, request, cancellationToken), cancellationToken);

    /// <summary>
    /// Creates or merges standalone jobs; recurrences of the same identity are not consulted. Applies the requests in
    /// rounds in which each merge identity appears once, so a later request of an identity sees what an earlier one
    /// wrote. Within a round, items with one mode and schedule are one batch (see <see cref="RequestGroupAsync"/>).
    /// Items the batch did not write met an outstanding job of another partition, which is refused, or under
    /// <c>IfAbsent</c> an existing one; an item whose conflicting job ended meanwhile is applied again.
    /// </summary>
    public async ValueTask<IReadOnlyList<JobRequestResult>> RequestManyAsync(WorkTransaction transaction, LeaseSource<JobId> source, IReadOnlyList<JobRequestData> requests, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(requests);
        var work = SqlServerTransaction.From(transaction);
        var sql = await SqlAsync(source, cancellationToken);
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
    private async Task CheckCalendarsAsync(SqlServerTransaction work, IReadOnlyList<JobRequestData> requests, CancellationToken cancellationToken)
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
    private static async Task<DateTimeOffset> ClockAsync(SqlServerTransaction work, CancellationToken cancellationToken)
    {
        await using var clock = Tsql.Command(work, "SELECT SYSUTCDATETIME();");
        await using var reader = await clock.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return Tsql.Time(reader, 0);
    }

    /// <summary>
    /// Applies items with distinct merge identities, one mode and one schedule, in one batch; returns the items to apply
    /// again. Merging items first take an exclusive applock per identity in name order, so requests of one identity
    /// serialize and several requests' identities never interleave. Outstanding jobs of the identities are then found
    /// without locks, locked through their key in primary-key order and evaluated in the next statement, with a clock
    /// reading taken after the locks are held. Under <c>Merge</c> an outstanding job of the same partition merges the
    /// request; one of another partition, and under <c>IfAbsent</c> any outstanding job, is reported for the caller to
    /// refuse or return. The other items are inserted; an insert that meets an outstanding job created meanwhile by a
    /// rerun fails as a whole on the outstanding-identity index, and its items are applied again.
    /// </summary>
    private async Task<List<int>> RequestGroupAsync(SqlServerTransaction work, LeaseSql sql, IReadOnlyList<JobRequestData> requests, int[] items, JobRequestResult[] results, CancellationToken cancellationToken)
    {
        var first = requests[items[0]];
        var schedule = first.Schedule;
        var definition = sql.Job!.Definition;
        var ids = items.Select(_ => JobId.New().Value).ToArray();
        var parameters = new List<SqlParameter>
        {
            Tsql.Text("@definition", definition, 200),
            Tsql.Json("@requests", RequestsJson(definition, requests, items, ids)),
        };
        var due = schedule.Kind switch
        {
            ScheduleKind.Now => "SYSUTCDATETIME()",
            ScheduleKind.At => "@at",
            ScheduleKind.After or ScheduleKind.Debounce => Tsql.AddTicks("SYSUTCDATETIME()", "@delay"),
            _ => throw new ArgumentException($"A request takes a one-shot schedule, not {schedule.Kind}.", nameof(requests)),
        };
        if (schedule.Kind == ScheduleKind.At)
        {
            parameters.Add(Tsql.Time("@at", schedule.Time));
        }
        else if (schedule.Kind is ScheduleKind.After or ScheduleKind.Debounce)
        {
            parameters.Add(Tsql.Ticks("@delay", schedule.Delay));
        }
        string? maximum = null;
        if (schedule.Kind == ScheduleKind.Debounce)
        {
            parameters.Add(Tsql.Ticks("@maximum", schedule.Maximum));
            maximum = "@maximum";
        }
        // The instant of an outstanding job merged with the request's: the earliest, except that a debounce moves it
        // later up to its maximum from the first outstanding request.
        string Merge(string current, string firstRequested) => maximum is null
            ? $"LEAST({current}, {due})"
            : $"LEAST({due}, {Tsql.AddTicks(firstRequested, maximum)})";
        // A job with a calendar keeps its requested instant as the calendar instant, without a due time, until its
        // definition's maintenance plans it.
        var hasCalendar = schedule.Calendar is not null;
        if (hasCalendar)
        {
            parameters.Add(Tsql.Text("@calendar", schedule.Calendar, 128));
            parameters.Add(Tsql.Int16("@blocked", (short)schedule.Blocked));
            parameters.Add(Tsql.Int16("@closed", (short)schedule.Closed));
        }
        var (calendar, blocked, closed) = hasCalendar ? ("@calendar", "@blocked", "@closed") : ("NULL", "NULL", "NULL");
        var (dueAt, calendarAt) = hasCalendar ? ("NULL", due) : (due, "NULL");
        var mode = first.Mode;
        var text = new StringBuilder($"""
            DECLARE @items TABLE (
                ord int NOT NULL PRIMARY KEY, id uniqueidentifier NOT NULL, merge_key nvarchar(512) COLLATE {Tsql.Binary},
                subject_kind nvarchar(100) COLLATE {Tsql.Binary}, subject_id nvarchar(400) COLLATE {Tsql.Binary},
                partition_key nvarchar(400) COLLATE {Tsql.Binary} NOT NULL, args varbinary(max) NOT NULL, args_version int NOT NULL,
                trace_parent nvarchar(100), lock_name nvarchar(255) COLLATE {Tsql.Binary});
            INSERT INTO @items (ord, id, merge_key, subject_kind, subject_id, partition_key, args, args_version, trace_parent, lock_name)
            SELECT o, i, m, sk, si, p, a, v, t, l FROM OPENJSON(@requests) WITH (
                o int '$.o', i uniqueidentifier '$.i', m nvarchar(512) '$.m', sk nvarchar(100) '$.sk', si nvarchar(400) '$.si',
                p nvarchar(400) '$.p', a varbinary(max) '$.a', v int '$.v', t nvarchar(100) '$.t', l nvarchar(255) '$.l');
            DECLARE @done TABLE (ord int NOT NULL PRIMARY KEY, id uniqueidentifier NOT NULL, kind int NOT NULL);
            DECLARE @outstanding TABLE (ord int NOT NULL PRIMARY KEY, id uniqueidentifier NOT NULL, partition_key nvarchar(400) COLLATE {Tsql.Binary} NOT NULL);
            DECLARE @inserted TABLE (id uniqueidentifier NOT NULL PRIMARY KEY);

            """);
        if (mode != RequestMode.New)
        {
            text.Append($"""
                DECLARE @locks TABLE (n int IDENTITY NOT NULL PRIMARY KEY, lock_name nvarchar(255) COLLATE {Tsql.Binary} NOT NULL);
                INSERT INTO @locks (lock_name) SELECT lock_name FROM @items WHERE lock_name IS NOT NULL ORDER BY lock_name;
                DECLARE @lock_count int = @@ROWCOUNT, @lock_index int = 1, @lock nvarchar(255);
                WHILE @lock_index <= @lock_count
                BEGIN
                    SELECT @lock = lock_name FROM @locks WHERE n = @lock_index;
                    {LeaseSql.AppLock("@lock", "Exclusive").Replace("\n", "\n    ", StringComparison.Ordinal).TrimEnd()}
                    SET @lock_index += 1;
                END
                DECLARE @found TABLE (id uniqueidentifier NOT NULL PRIMARY KEY, ord int NOT NULL);
                INSERT INTO @found (id, ord) SELECT j.id, s.ord FROM @items AS s
                INNER LOOP JOIN {sql.Table} AS j WITH (INDEX(jobs_merge), FORCESEEK) ON j.[definition] = @definition AND j.merge_key = s.merge_key
                WHERE j.merge_key IS NOT NULL AND j.state = 0;
                DECLARE @locked int;
                SELECT @locked = COUNT(*) FROM @found AS f INNER LOOP JOIN {sql.Table} AS j WITH (UPDLOCK, ROWLOCK, INDEX(jobs_key), FORCESEEK) ON j.id = f.id;

                """);
        }
        if (mode == RequestMode.Merge)
        {
            // Before the execution started, and while no claim holds or held its lease and it is not to be skipped,
            // the request replaces the arguments, the calendar and its policies and merges the instant; otherwise it
            // becomes the pending follow-up, which runs under the job's calendar. The condition does not depend on the
            // clock, so the outcome reported matches the branch taken.
            const string followUp = "(j.started = 1 OR j.lease_expires_at IS NOT NULL OR j.skip_pending = 1)";
            const string merging = "COALESCE(j.calendar_at, j.due_at)";
            text.Append($"""
                UPDATE j SET
                    args = CASE WHEN {followUp} THEN j.args ELSE s.args END,
                    args_version = CASE WHEN {followUp} THEN j.args_version ELSE s.args_version END,
                    due_at = CASE WHEN {followUp} THEN j.due_at {(hasCalendar ? "" : $"ELSE {Merge(merging, "j.first_requested_at")} ")}END,
                    calendar_at = CASE WHEN {followUp} THEN j.calendar_at {(hasCalendar ? $"ELSE {Merge(merging, "j.first_requested_at")} " : "")}END,
                    window_end = CASE WHEN {followUp} THEN j.window_end END,
                    calendar_unavailable = CASE WHEN {followUp} AND j.calendar_unavailable = 1 THEN 1 ELSE 0 END,
                    calendar = CASE WHEN {followUp} THEN j.calendar ELSE {calendar} END,
                    calendar_blocked = CASE WHEN {followUp} THEN j.calendar_blocked ELSE {blocked} END,
                    calendar_closed = CASE WHEN {followUp} THEN j.calendar_closed ELSE {closed} END,
                    pending_args = CASE WHEN {followUp} THEN s.args ELSE j.pending_args END,
                    pending_args_version = CASE WHEN {followUp} THEN s.args_version ELSE j.pending_args_version END,
                    pending_due = CASE WHEN NOT {followUp} THEN j.pending_due WHEN j.pending_first_requested_at IS NULL THEN {due}
                        ELSE {Merge("j.pending_due", "j.pending_first_requested_at")} END,
                    pending_first_requested_at = CASE WHEN {followUp} THEN COALESCE(j.pending_first_requested_at, SYSUTCDATETIME()) ELSE j.pending_first_requested_at END
                OUTPUT s.ord, inserted.id, CASE WHEN deleted.started = 1 OR deleted.lease_expires_at IS NOT NULL OR deleted.skip_pending = 1 THEN {FollowUp} ELSE {Merged} END INTO @done (ord, id, kind)
                FROM @found AS f INNER LOOP JOIN {sql.Table} AS j WITH (INDEX(jobs_key), FORCESEEK) ON j.id = f.id JOIN @items AS s ON s.ord = f.ord
                WHERE j.state = 0 AND j.[definition] = @definition AND j.merge_key = s.merge_key AND j.partition_key = s.partition_key;

                """);
        }
        if (mode != RequestMode.New)
        {
            text.Append($"""
                INSERT INTO @outstanding (ord, id, partition_key) SELECT f.ord, j.id, j.partition_key
                FROM @found AS f INNER LOOP JOIN {sql.Table} AS j WITH (INDEX(jobs_key), FORCESEEK) ON j.id = f.id JOIN @items AS s ON s.ord = f.ord
                WHERE j.state = 0 AND j.[definition] = @definition AND j.merge_key = s.merge_key AND NOT EXISTS (SELECT 1 FROM @done AS d WHERE d.ord = f.ord);

                """);
        }
        // The pause flag is copied from the stored subject pauses; claims check the stored pause itself. A merged
        // request keeps the trace of the request that created the job.
        text.Append($"""
            SET XACT_ABORT OFF;
            BEGIN TRY
                INSERT INTO {sql.Table} (id, [definition], merge_key, subject_kind, subject_id, partition_key, args, args_version,
                    first_requested_at, created_at, due_at, calendar_at, calendar, calendar_blocked, calendar_closed, execution_id, paused, trace_parent)
                OUTPUT inserted.id INTO @inserted (id)
                SELECT s.id, @definition, s.merge_key, s.subject_kind, s.subject_id, s.partition_key, s.args, s.args_version, SYSUTCDATETIME(), SYSUTCDATETIME(),
                    {dueAt}, {calendarAt}, {calendar}, {blocked}, {closed}, NEWID(),
                    CASE WHEN s.subject_kind IS NOT NULL AND EXISTS (SELECT 1 FROM {pauses} AS p WHERE p.[definition] = @definition AND p.subject_kind = s.subject_kind AND p.subject_id = s.subject_id) THEN 1 ELSE 0 END,
                    s.trace_parent
                FROM @items AS s
                WHERE NOT EXISTS (SELECT 1 FROM @done AS d WHERE d.ord = s.ord) AND NOT EXISTS (SELECT 1 FROM @outstanding AS o WHERE o.ord = s.ord)
                ORDER BY s.ord;
            END TRY
            BEGIN CATCH
                SET XACT_ABORT ON;
                IF ERROR_NUMBER() NOT IN (2601, 2627) THROW;
                DELETE FROM @inserted;
            END CATCH
            SET XACT_ABORT ON;
            SELECT ord, id, kind FROM @done
            UNION ALL SELECT s.ord, s.id, {Created} FROM @items AS s JOIN @inserted AS i ON i.id = s.id;
            SELECT ord, id, partition_key FROM @outstanding;
            """);
        await using var command = Tsql.Command(work, text.ToString(), [.. parameters]);
        var written = new HashSet<int>();
        var unwritten = new List<(int Item, Guid Id, string Partition)>();
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var item = items[reader.GetInt32(0)];
                var outcome = reader.GetInt32(2) switch
                {
                    Created => JobRequestOutcome.Created,
                    FollowUp => JobRequestOutcome.FollowUp,
                    _ => JobRequestOutcome.Merged,
                };
                results[item] = new JobRequestResult(new JobId(reader.GetGuid(1)), outcome);
                written.Add(item);
            }
            await reader.NextResultAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                unwritten.Add((items[reader.GetInt32(0)], reader.GetGuid(1), reader.GetString(2)));
            }
        }
        foreach (var (item, id, partition) in unwritten)
        {
            if (partition != (requests[item].Partition ?? ""))
            {
                throw new JobPartitionMismatchException(
                    $"The outstanding job {id} of {definition} with this identity belongs to another partition; a request cannot merge into it.");
            }
            results[item] = new JobRequestResult(new JobId(id), JobRequestOutcome.Existing);
            written.Add(item);
        }
        return [.. items.Where(i => !written.Contains(i))];
    }

    /// <summary>
    /// The items of one request batch as a JSON array: the position in the batch, the new job's id, the merge identity
    /// with its applock name, subject, partition, base64 arguments, argument version and trace.
    /// </summary>
    private string RequestsJson(string definition, IReadOnlyList<JobRequestData> requests, int[] items, Guid[] ids)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            for (var n = 0; n < items.Length; n++)
            {
                var request = requests[items[n]];
                writer.WriteStartObject();
                writer.WriteNumber("o", n);
                writer.WriteString("i", ids[n]);
                if (request.MergeIdentity is { } identity)
                {
                    writer.WriteString("m", identity);
                    writer.WriteString("l", Tsql.LockName("merge", schema, definition, identity));
                }
                if (request.Subject is { } subject)
                {
                    writer.WriteString("sk", subject.Kind);
                    writer.WriteString("si", subject.Id);
                }
                writer.WriteString("p", request.Partition ?? "");
                writer.WriteBase64String("a", request.Args);
                writer.WriteNumber("v", request.ArgsVersion);
                if (request.TraceParent is { } trace)
                {
                    writer.WriteString("t", trace);
                }
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }
}
