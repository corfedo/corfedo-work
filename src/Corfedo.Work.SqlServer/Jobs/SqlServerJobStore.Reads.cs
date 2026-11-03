using System.Buffers;
using System.Text;
using System.Text.Json;
using Corfedo.Work.SqlServer.Leases;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Jobs;

internal sealed partial class SqlServerJobStore
{
    private const string OutcomeColumns = "j.id, CAST(CASE WHEN j.state <> 0 THEN 1 ELSE 0 END AS bit), j.ended_execution_id, j.outcome, j.reason, j.ended_at, j.result, j.result_version";

    /// <summary>
    /// Locks the job, then records the progress when the lease is valid by a clock reading taken after the lock is
    /// held.
    /// </summary>
    public async ValueTask<bool> ReportProgressAsync(Lease<JobId> lease, JobProgress progress, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(progress);
        var source = lease.Source;
        var sql = await SqlAsync(source, cancellationToken);
        await using var connection = await leases.OpenAsync(cancellationToken);
        await using var command = SqlServerLeaseStore.KeyCommand(connection, null, sql, source, lease.Key, Tsql.OwnTransaction($"""
            DECLARE @held int;
            SELECT @held = 1 FROM {sql.Table} AS r WITH (UPDLOCK, ROWLOCK) WHERE {sql.KeyMatch};
            UPDATE r SET progress_percent = @percent, progress_message = @message FROM {sql.Table} AS r WHERE {sql.KeyMatch} AND r.state = 0 AND {sql.Valid};
            SELECT @@ROWCOUNT;
            """),
            Tsql.Guid("@owner", lease.Owner.Value),
            Tsql.Int64("@epoch", lease.Epoch),
            Tsql.Int16("@percent", (short)progress.Percent),
            Tsql.Text("@message", progress.Message, -1));
        return Convert.ToInt32(await command.ScalarAsync(cancellationToken), System.Globalization.CultureInfo.InvariantCulture) == 1;
    }

    /// <summary>
    /// One statement for the whole page: per subject, the running job, otherwise the earliest due one, standalone job
    /// or occurrence (occurrences waiting to be recorded skipped are not candidates), from the index over outstanding
    /// jobs by subject; the latest ended execution from the index over ended jobs by definition and subject; the
    /// recurrence by its identity; and the stored pause. Each is an index probe per subject.
    /// </summary>
    public async ValueTask<IReadOnlyDictionary<Subject, JobStatus>> GetStatusAsync(string definition, IReadOnlyList<Subject> subjects, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(subjects);
        var statuses = new Dictionary<Subject, JobStatus>();
        if (subjects.Count == 0)
        {
            return statuses;
        }
        const string Order = "COALESCE(r.due_at, r.calendar_at)";
        await using var connection = await leases.OpenAsync(cancellationToken);
        await using var command = Tsql.Command(connection, null, $"""
            DECLARE @s TABLE (n int NOT NULL PRIMARY KEY, kind nvarchar(100) COLLATE {Tsql.Binary} NOT NULL, id nvarchar(400) COLLATE {Tsql.Binary} NOT NULL,
                [identity] nvarchar(512) COLLATE {Tsql.Binary} NOT NULL);
            INSERT INTO @s (n, kind, id, [identity]) SELECT n, k, i, m FROM OPENJSON(@subjects) WITH (n int '$.n', k nvarchar(100) '$.k', i nvarchar(400) '$.i', m nvarchar(512) '$.m');
            SELECT s.n, o.id, o.due_at, o.held, o.attempt_started_at, o.occurrence, o.attempts, o.progress_percent, o.progress_message, o.follow_up, o.chain_waiting,
                e.outcome, e.reason, e.ended_at, c.next_at, {Tsql.Bit("c.id IS NOT NULL")}, {Tsql.Bit("d.paused = 1 OR p.paused IS NOT NULL")}
            FROM @s AS s
            LEFT JOIN {definitions} AS d ON d.[key] = @definition
            OUTER APPLY (
                SELECT TOP (1) r.id, COALESCE(NULLIF(r.due_at, {Tsql.NeverLiteral}), r.calendar_at) AS due_at, {Tsql.Bit(Running)} AS held, r.attempt_started_at,
                    r.occurrence, r.attempts, r.progress_percent, r.progress_message, {Tsql.Bit("r.pending_args IS NOT NULL")} AS follow_up, r.chain_waiting
                FROM {Jobs} AS r
                WHERE r.subject_kind = s.kind AND r.subject_id = s.id AND r.[definition] = @definition AND r.state = 0 AND r.skip_pending = 0
                ORDER BY CASE WHEN {Running} THEN 0 ELSE 1 END, CASE WHEN {Order} IS NULL THEN 1 ELSE 0 END, {Order}, r.id) AS o
            OUTER APPLY (
                SELECT TOP (1) r.outcome, r.reason, r.ended_at FROM {Jobs} AS r
                WHERE r.[definition] = @definition AND r.subject_kind = s.kind AND r.subject_id = s.id AND r.ended_at IS NOT NULL
                ORDER BY r.ended_at DESC) AS e
            OUTER APPLY (SELECT TOP (1) c.id, c.next_at FROM {recurrences} AS c WHERE c.[definition] = @definition AND c.merge_key = s.[identity]) AS c
            OUTER APPLY (SELECT TOP (1) CAST(1 AS bit) AS paused FROM {pauses} AS p WHERE p.[definition] = @definition AND p.subject_kind = s.kind AND p.subject_id = s.id) AS p
            ORDER BY s.n;
            """,
            Tsql.Text("@definition", definition, 200),
            Tsql.Json("@subjects", SubjectsJson(subjects)));
        await using var reader = await command.ReaderAsync(cancellationToken);
        while (await reader.NextRowAsync(cancellationToken))
        {
            var outstanding = !reader.IsDBNull(1);
            var everEnded = !reader.IsDBNull(13);
            if (!outstanding && !everEnded && !reader.GetBoolean(15))
            {
                continue;
            }
            var running = outstanding && reader.GetBoolean(3);
            statuses[subjects[reader.GetInt32(0)]] = new JobStatus(
                !outstanding ? JobStatusState.Idle : running ? JobStatusState.Running : reader.GetBoolean(10) ? JobStatusState.WaitingForParent : JobStatusState.Scheduled,
                outstanding ? new JobId(reader.GetGuid(1)) : null,
                outstanding && !running ? Tsql.NullableTime(reader, 2) : null,
                running ? Tsql.NullableTime(reader, 4) : null,
                outstanding && !reader.IsDBNull(5) ? reader.GetString(5) : null,
                outstanding ? reader.GetInt32(6) : 0,
                outstanding && !reader.IsDBNull(7) ? new JobProgress(reader.GetInt16(7), reader.IsDBNull(8) ? null : reader.GetString(8)) : null,
                everEnded && !reader.IsDBNull(11) ? (JobOutcome)reader.GetInt16(11) : null,
                everEnded && !reader.IsDBNull(12) ? reader.GetString(12) : null,
                everEnded ? Tsql.Time(reader, 13) : null,
                Tsql.NullableTime(reader, 14),
                reader.GetBoolean(16),
                outstanding && reader.GetBoolean(9));
        }
        return statuses;
    }

    /// <summary>The subjects of a status page as a JSON array: position, kind, id and merge identity.</summary>
    private static string SubjectsJson(IReadOnlyList<Subject> subjects)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            for (var n = 0; n < subjects.Count; n++)
            {
                writer.WriteStartObject();
                writer.WriteNumber("n", n);
                writer.WriteString("k", subjects[n].Kind);
                writer.WriteString("i", subjects[n].Id);
                writer.WriteString("m", JobRequestData.Identity(subjects[n], null));
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    /// <summary>
    /// A plain update of the outstanding row: a transition holding its lock commits first, and the update then reads
    /// the latest committed row, so the mark is either seen by the transition that ends the job or finds it ended. A
    /// committed mark is already visible to that transition, so a marked row is not rewritten.
    /// </summary>
    public async ValueTask<bool> WatchAsync(string definition, JobId job, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        await using var connection = await leases.OpenAsync(cancellationToken);
        await using var command = Tsql.Command(connection, null,
            $"UPDATE {Jobs} SET waited = 1 WHERE id = @id AND [definition] = @definition AND state = 0 AND waited = 0;\nSELECT @@ROWCOUNT;",
            Tsql.Text("@definition", definition, 200),
            Tsql.Guid("@id", job.Value));
        return Convert.ToInt32(await command.ScalarAsync(cancellationToken), System.Globalization.CultureInfo.InvariantCulture) == 1;
    }

    public async ValueTask<IReadOnlyList<StoredJobOutcome>> GetOutcomesAsync(string definition, IReadOnlyList<JobId> jobs, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(jobs);
        if (jobs.Count == 0)
        {
            return [];
        }
        await using var connection = await leases.OpenAsync(cancellationToken);
        await using var command = Tsql.Command(connection, null, $"""
            DECLARE @ids TABLE (id uniqueidentifier NOT NULL PRIMARY KEY);
            INSERT INTO @ids (id) SELECT DISTINCT value FROM OPENJSON(@jobs) WITH (value uniqueidentifier '$');
            SELECT {OutcomeColumns} FROM @ids AS i INNER LOOP JOIN {Jobs} AS j ON j.id = i.id WHERE j.[definition] = @definition;
            """,
            Tsql.Text("@definition", definition, 200),
            Tsql.Guids("@jobs", jobs.Select(j => j.Value)));
        var outcomes = new List<StoredJobOutcome>(jobs.Count);
        await using var reader = await command.ReaderAsync(cancellationToken);
        while (await reader.NextRowAsync(cancellationToken))
        {
            outcomes.Add(StoredOutcome(reader));
        }
        return outcomes;
    }

    /// <summary>Reads the outcome columns of <see cref="OutcomeColumns"/>.</summary>
    private static StoredJobOutcome StoredOutcome(SqlDataReader reader) => new(
        new JobId(reader.GetGuid(0)),
        reader.GetBoolean(1),
        reader.IsDBNull(2) ? null : reader.GetGuid(2),
        reader.IsDBNull(3) ? null : (JobOutcome)reader.GetInt16(3),
        reader.IsDBNull(4) ? null : reader.GetString(4),
        Tsql.NullableTime(reader, 5),
        reader.IsDBNull(6) ? null : reader.GetFieldValue<byte[]>(6),
        reader.IsDBNull(7) ? 0 : reader.GetInt32(7));

    /// <summary>
    /// Reads one page newest first in the order of the subject index: by start, job and epoch, continuing after the
    /// cursor. One more entry than the limit is read to tell whether a next page exists.
    /// </summary>
    public async ValueTask<JobRunPage> GetRunsAsync(string definition, Subject? subject, JobId? job, int limit, JobRunCursor? after, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        if ((subject is null) == (job is null))
        {
            throw new ArgumentException("Run history is read by a subject or by a job.");
        }
        var parameters = new List<SqlParameter> { Tsql.Text("@definition", definition, 200), Tsql.Int32("@limit", limit + 1) };
        string filter;
        if (subject is not null)
        {
            filter = "[definition] = @definition AND subject_kind = @kind AND subject_id = @id";
            parameters.Add(Tsql.Text("@kind", subject.Kind, 100));
            parameters.Add(Tsql.Text("@id", subject.Id, 400));
        }
        else
        {
            filter = "[definition] = @definition AND job_id = @job";
            parameters.Add(Tsql.Guid("@job", job!.Value.Value));
        }
        if (after is not null)
        {
            filter += " AND (started_at < @after_start OR (started_at = @after_start AND (job_id < @after_job OR (job_id = @after_job AND epoch < @after_epoch))))";
            parameters.Add(Tsql.Time("@after_start", after.StartedAt));
            parameters.Add(Tsql.Guid("@after_job", after.Job.Value));
            parameters.Add(Tsql.Int64("@after_epoch", after.Epoch));
        }
        await using var connection = await leases.OpenAsync(cancellationToken);
        await using var command = Tsql.Command(connection, null,
            "SELECT TOP (@limit) job_id, epoch, subject_kind, subject_id, occurrence, execution_id, attempt, owner, started_at, ended_at, outcome, reason "
            + $"FROM {runs} WHERE {filter} ORDER BY started_at DESC, job_id DESC, epoch DESC;",
            [.. parameters]);
        var entries = new List<JobRunEntry>(Math.Min(limit + 1, 1001));
        await using (var reader = await command.ReaderAsync(cancellationToken))
        {
            while (await reader.NextRowAsync(cancellationToken))
            {
                entries.Add(new JobRunEntry(
                    new JobId(reader.GetGuid(0)),
                    reader.GetInt64(1),
                    reader.IsDBNull(2) ? null : Subject.Of(reader.GetString(2), reader.GetString(3)),
                    reader.IsDBNull(4) ? null : reader.GetString(4),
                    reader.GetGuid(5),
                    reader.GetInt32(6),
                    new OwnerId(reader.GetGuid(7)),
                    Tsql.Time(reader, 8),
                    Tsql.Time(reader, 9),
                    (JobRunOutcome)reader.GetInt16(10),
                    reader.IsDBNull(11) ? null : reader.GetString(11)));
            }
        }
        if (entries.Count <= limit)
        {
            return new JobRunPage(entries, null);
        }
        entries.RemoveAt(limit);
        var last = entries[^1];
        return new JobRunPage(entries, new JobRunCursor(last.StartedAt, last.Job, last.Epoch));
    }
}
