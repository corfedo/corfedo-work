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
        CheckLengths(requests);
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
    /// Refuses values longer than the job table's columns before anything is read or written. Statements read them at
    /// the columns' lengths, so a longer merge identity would otherwise be truncated and could merge into another job.
    /// </summary>
    private static void CheckLengths(IReadOnlyList<JobRequestData> requests)
    {
        foreach (var request in requests)
        {
            Check(request.MergeIdentity, 512, "merge identity");
            Check(request.Subject?.Kind, 100, "subject kind");
            Check(request.Subject?.Id, 400, "subject id");
            Check(request.Partition, 400, "partition");
        }

        static void Check(string? value, int length, string name)
        {
            if (value is not null && value.Length > length)
            {
                throw new ArgumentException($"A job's {name} has {value.Length} characters; SQL Server stores at most {length}.", nameof(requests));
            }
        }
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
        await using var reader = await clock.ReaderAsync(cancellationToken);
        await reader.NextRowAsync(cancellationToken);
        return Tsql.Time(reader, 0);
    }

    /// <summary>
    /// Applies items with distinct merge identities, one mode and one schedule, in one batch; returns the items to apply
    /// again. Merging items first take an exclusive applock per identity in name order, so requests of one identity
    /// serialize and several requests' identities never interleave. Outstanding jobs of the identities are then found
    /// without locks, locked through their key in primary-key order and evaluated in the next statement, with a clock
    /// reading taken after the locks are held. Under <c>Merge</c> an outstanding job of the same partition merges the
    /// request; one of another partition, and under <c>IfAbsent</c> any outstanding job, is reported for the caller to
    /// refuse or return. The other items are inserted by a second command; an insert that meets an outstanding job
    /// created meanwhile by a rerun fails as a whole on the outstanding-identity index, and its items are applied again.
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
            Tsql.Json("@requests", RequestsJson(requests, items, ids)),
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
        // The items of the batch, read from @requests by each statement that needs them, with the job table's types:
        // a wider type would turn each identity seek into a range seek. RequestManyAsync refuses longer values.
        var itemRows = $"""
            OPENJSON(@requests) WITH (ord int '$.o', id uniqueidentifier '$.i', merge_key nvarchar(512) COLLATE {Tsql.Binary} '$.m',
                subject_kind nvarchar(100) COLLATE {Tsql.Binary} '$.sk', subject_id nvarchar(400) COLLATE {Tsql.Binary} '$.si',
                partition_key nvarchar(400) COLLATE {Tsql.Binary} '$.p', args varbinary(max) '$.a', args_version int '$.v', trace_parent nvarchar(max) '$.t')
            """;
        var written = new HashSet<int>();
        var unwritten = new List<(int Item, Guid Id, string Partition)>();
        if (mode != RequestMode.New)
        {
            // Outstanding jobs of the items' identities, read through the identity index without locks.
            var found = $"{itemRows} AS s INNER LOOP JOIN {sql.Table} AS m WITH (INDEX(jobs_merge), FORCESEEK) ON m.[definition] = @definition AND m.merge_key = s.merge_key "
                + "WHERE s.merge_key IS NOT NULL AND m.state = 0";
            var text = new StringBuilder($"""
                {Tsql.AppLocks("merge", "@locks", "Exclusive", "-1")}
                DECLARE @found nvarchar(max) = (SELECT m.id FROM {found} ORDER BY m.id FOR JSON PATH), @locked int;
                SELECT @locked = COUNT(*) FROM OPENJSON(@found) WITH (id uniqueidentifier '$.id') AS f
                INNER LOOP JOIN {sql.Table} AS j WITH (UPDLOCK, ROWLOCK, INDEX(jobs_key), FORCESEEK) ON j.id = f.id;

                """);
            if (mode == RequestMode.Merge)
            {
                // Before the execution started, and while no claim holds or held its lease and it is not to be skipped,
                // the request replaces the arguments, the calendar and its policies and merges the instant; otherwise it
                // becomes the pending follow-up, which runs under the job's calendar. The condition does not depend on the
                // clock, so the outcome reported matches the branch taken. The job table has no triggers, so the merged
                // jobs are returned by the OUTPUT clause.
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
                    OUTPUT s.ord, inserted.id, CASE WHEN deleted.started = 1 OR deleted.lease_expires_at IS NOT NULL OR deleted.skip_pending = 1 THEN {FollowUp} ELSE {Merged} END
                    FROM (SELECT s.ord, m.id, s.merge_key, s.partition_key, s.args, s.args_version FROM {found}) AS s
                    INNER LOOP JOIN {sql.Table} AS j WITH (INDEX(jobs_key), FORCESEEK) ON j.id = s.id
                    WHERE j.state = 0 AND j.[definition] = @definition AND j.merge_key = s.merge_key AND j.partition_key = s.partition_key;

                    """);
            }
            // The outstanding jobs the batch did not merge.
            text.Append($"SELECT s.ord, m.id, m.partition_key FROM {found}{(mode == RequestMode.Merge ? " AND m.partition_key <> s.partition_key" : "")};\n");
            var locks = items.Select(i => requests[i].MergeIdentity).OfType<string>().Select(identity => Tsql.LockName("merge", schema, definition, identity)).Order(StringComparer.Ordinal);
            await using var command = Tsql.Command(work, text.ToString(), [.. parameters.Select(Copy), Tsql.Text("@locks", string.Join(',', locks), -1)]);
            await using var reader = await command.ReaderAsync(cancellationToken);
            if (mode == RequestMode.Merge)
            {
                while (await reader.NextRowAsync(cancellationToken))
                {
                    var item = items[reader.GetInt32(0)];
                    var outcome = reader.GetInt32(2) == FollowUp ? JobRequestOutcome.FollowUp : JobRequestOutcome.Merged;
                    results[item] = new JobRequestResult(new JobId(reader.GetGuid(1)), outcome);
                    written.Add(item);
                }
                await reader.NextSetAsync(cancellationToken);
            }
            while (await reader.NextRowAsync(cancellationToken))
            {
                var n = reader.GetInt32(0);
                unwritten.Add((items[n], reader.GetGuid(1), reader.GetString(2)));
                written.Add(items[n]);
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
        }
        var inserts = Enumerable.Range(0, items.Length).Where(n => !written.Contains(items[n])).ToArray();
        if (inserts.Length == 0)
        {
            return [];
        }
        var insertParameters = parameters.Select(Copy).ToArray();
        if (inserts.Length < items.Length)
        {
            insertParameters[1] = Tsql.Json("@requests", RequestsJson(requests, items, ids, inserts));
        }
        // The pause flag is copied from the stored subject pauses; claims check the stored pause itself. The insert
        // writes every remaining item or, when it meets an outstanding job created meanwhile, none.
        await using (var insert = Tsql.Command(work, $"""
            DECLARE @inserted bit = 0;
            SET XACT_ABORT OFF;
            BEGIN TRY
                INSERT INTO {sql.Table} (id, [definition], merge_key, subject_kind, subject_id, partition_key, args, args_version,
                    first_requested_at, created_at, due_at, calendar_at, calendar, calendar_blocked, calendar_closed, execution_id, paused, trace_parent)
                SELECT s.id, @definition, s.merge_key, s.subject_kind, s.subject_id, s.partition_key, s.args, s.args_version, SYSUTCDATETIME(), SYSUTCDATETIME(),
                    {dueAt}, {calendarAt}, {calendar}, {blocked}, {closed}, NEWID(),
                    CASE WHEN s.subject_kind IS NOT NULL AND EXISTS (SELECT 1 FROM {pauses} AS p WHERE p.[definition] = @definition AND p.subject_kind = s.subject_kind AND p.subject_id = s.subject_id) THEN 1 ELSE 0 END,
                    s.trace_parent
                FROM {itemRows} AS s
                ORDER BY s.ord;
                SET @inserted = 1;
            END TRY
            BEGIN CATCH
                SET XACT_ABORT ON;
                IF ERROR_NUMBER() NOT IN (2601, 2627) THROW;
            END CATCH
            SET XACT_ABORT ON;
            SELECT @inserted;
            """, insertParameters))
        {
            if (!(bool)(await insert.ScalarAsync(cancellationToken))!)
            {
                return [.. inserts.Select(n => items[n])];
            }
        }
        foreach (var n in inserts)
        {
            results[items[n]] = new JobRequestResult(new JobId(ids[n]), JobRequestOutcome.Created);
        }
        return [];
    }

    /// <summary>A parameter like <paramref name="parameter"/> for another command; a parameter belongs to one command.</summary>
    private static SqlParameter Copy(SqlParameter parameter) => (SqlParameter)((ICloneable)parameter).Clone();

    /// <summary>
    /// The items <paramref name="positions"/> of one request batch, all of them by default, as a JSON array: the
    /// position in the batch, the new job's id, the merge identity, subject, partition, base64 arguments, argument
    /// version and trace.
    /// </summary>
    private static string RequestsJson(IReadOnlyList<JobRequestData> requests, int[] items, Guid[] ids, IEnumerable<int>? positions = null)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            foreach (var n in positions ?? Enumerable.Range(0, items.Length))
            {
                var request = requests[items[n]];
                writer.WriteStartObject();
                writer.WriteNumber("o", n);
                writer.WriteString("i", ids[n]);
                if (request.MergeIdentity is { } identity)
                {
                    writer.WriteString("m", identity);
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
