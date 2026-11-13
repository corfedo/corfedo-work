using Corfedo.Work.PostgreSql.Leases;
using Npgsql;
using NpgsqlTypes;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Jobs;

internal sealed partial class PostgreSqlJobStore
{
    private readonly string runs = LeaseSql.Quote(schema) + ".job_runs";
    private readonly string jobTable = LeaseSql.Quote(schema) + ".jobs";

    public async ValueTask<bool> ReportProgressAsync(Lease<JobId> lease, JobProgress progress, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(progress);
        var source = lease.Source;
        var sql = Sql(source);
        await using var command = dataSource.CreateCommand(
            $"UPDATE {sql.Table} AS r SET progress_percent = $4, progress_message = $5 WHERE {sql.KeyMatch(1)} AND r.state = 0 AND {sql.Valid(2, 3)}");
        AddKey(command, source, lease.Key);
        command.Parameters.Add(Uuid(lease.Owner.Value));
        command.Parameters.Add(Int64(lease.Epoch));
        command.Parameters.Add(new NpgsqlParameter<short> { TypedValue = (short)progress.Percent });
        command.Parameters.Add(new NpgsqlParameter { Value = progress.Message ?? (object)DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    /// <summary>
    /// One statement for the whole page: per subject, the running job, otherwise the earliest due one, standalone job
    /// or occurrence (occurrences waiting to be recorded skipped are not candidates), from the index over
    /// outstanding jobs by subject; the latest ended execution from the index over ended jobs by definition and subject;
    /// the recurrence by its identity; and the stored pause. Each is an index probe per subject: the recurrence and the
    /// pause are limited lateral lookups, since as joins the planner hashed the whole tables for a large page.
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
        await using var command = dataSource.CreateCommand($"""
            SELECT s.n, o.id, o.due_at, o.held, o.attempt_started_at, o.occurrence, o.attempts, o.progress_percent, o.progress_message, o.follow_up, o.chain_waiting,
                   e.outcome, e.reason, e.ended_at, c.next_at, c.id IS NOT NULL, coalesce(d.paused, false) OR p.paused IS NOT NULL
            FROM unnest($2::text[], $3::text[], $4::text[]) WITH ORDINALITY AS s(kind, id, identity, n)
            LEFT JOIN {definitions} AS d ON d.key = $1
            LEFT JOIN LATERAL (
                SELECT r.id, COALESCE(NULLIF(r.due_at, 'infinity'), r.calendar_at) AS due_at, {Running} AS held, r.attempt_started_at,
                       r.occurrence, r.attempts, r.progress_percent, r.progress_message,
                       r.pending_args IS NOT NULL AS follow_up, r.chain_waiting
                FROM {jobTable} AS r
                WHERE r.subject_kind = s.kind AND r.subject_id = s.id AND r.definition = $1 AND r.state = 0 AND NOT r.skip_pending
                ORDER BY {Running} DESC, COALESCE(r.due_at, r.calendar_at) NULLS LAST, r.id
                LIMIT 1) AS o ON true
            LEFT JOIN LATERAL (
                SELECT r.outcome, r.reason, r.ended_at FROM {jobTable} AS r
                WHERE r.definition = $1 AND r.subject_kind = s.kind AND r.subject_id = s.id AND r.ended_at IS NOT NULL
                ORDER BY r.ended_at DESC
                LIMIT 1) AS e ON true
            LEFT JOIN LATERAL (SELECT c.id, c.next_at FROM {recurrences} AS c WHERE c.definition = $1 AND c.merge_key = s.identity LIMIT 1) AS c ON true
            LEFT JOIN LATERAL (SELECT true AS paused FROM {pauses} AS p WHERE p.definition = $1 AND p.subject_kind = s.kind AND p.subject_id = s.id LIMIT 1) AS p ON true
            ORDER BY s.n
            """);
        command.Parameters.Add(Text(definition));
        command.Parameters.Add(Texts([.. subjects.Select(s => s.Kind)]));
        command.Parameters.Add(Texts([.. subjects.Select(s => s.Id)]));
        command.Parameters.Add(Texts([.. subjects.Select(s => JobRequestData.Identity(s, null)!)]));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var outstanding = !reader.IsDBNull(1);
            var everEnded = !reader.IsDBNull(13);
            if (!outstanding && !everEnded && !reader.GetBoolean(15))
            {
                continue;
            }
            var running = outstanding && reader.GetBoolean(3);
            statuses[subjects[(int)reader.GetInt64(0) - 1]] = new JobStatus(
                !outstanding ? JobStatusState.Idle : running ? JobStatusState.Running : reader.GetBoolean(10) ? JobStatusState.WaitingForParent : JobStatusState.Scheduled,
                outstanding ? new JobId(reader.GetGuid(1)) : null,
                outstanding && !running && !reader.IsDBNull(2) ? reader.GetFieldValue<DateTimeOffset>(2) : null,
                running && !reader.IsDBNull(4) ? reader.GetFieldValue<DateTimeOffset>(4) : null,
                outstanding && !reader.IsDBNull(5) ? reader.GetString(5) : null,
                outstanding ? reader.GetInt32(6) : 0,
                outstanding && !reader.IsDBNull(7) ? new JobProgress(reader.GetInt16(7), reader.IsDBNull(8) ? null : reader.GetString(8)) : null,
                everEnded && !reader.IsDBNull(11) ? (JobOutcome)reader.GetInt16(11) : null,
                everEnded && !reader.IsDBNull(12) ? reader.GetString(12) : null,
                everEnded ? reader.GetFieldValue<DateTimeOffset>(13) : null,
                reader.IsDBNull(14) ? null : reader.GetFieldValue<DateTimeOffset>(14),
                reader.GetBoolean(16),
                outstanding && reader.GetBoolean(9));
        }
        return statuses;
    }

    /// <summary>
    /// A plain update of the outstanding row: a transition holding its lock commits first, and the update then sees
    /// whether the job is still outstanding, so the mark is either seen by the transition that ends the job or finds it
    /// ended. A committed mark is already visible to that transition, so a marked row is not rewritten; an uncommitted
    /// one holds the row lock, and the update rechecks the row once it commits.
    /// </summary>
    public async ValueTask<bool> WatchAsync(string definition, JobId job, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        await using var command = dataSource.CreateCommand($"UPDATE {jobTable} SET waited = true WHERE id = $2 AND definition = $1 AND state = 0 AND NOT waited");
        command.Parameters.Add(Text(definition));
        command.Parameters.Add(Uuid(job.Value));
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    public async ValueTask<IReadOnlyList<StoredJobOutcome>> GetOutcomesAsync(string definition, IReadOnlyList<JobId> jobs, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(jobs);
        if (jobs.Count == 0)
        {
            return [];
        }
        await using var command = dataSource.CreateCommand(
            $"SELECT id, state <> 0, ended_execution_id, outcome, reason, ended_at, result, result_version FROM {jobTable} WHERE id = ANY($2) AND definition = $1");
        command.Parameters.Add(Text(definition));
        command.Parameters.Add(Uuids([.. jobs.Select(j => j.Value)]));
        var outcomes = new List<StoredJobOutcome>(jobs.Count);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            outcomes.Add(StoredOutcome(reader));
        }
        return outcomes;
    }

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
        var filter = subject is not null ? "definition = $1 AND subject_kind = $2 AND subject_id = $3" : "definition = $1 AND job_id = $2";
        var next = subject is not null ? 4 : 3;
        await using var command = dataSource.CreateCommand(
            "SELECT job_id, epoch, subject_kind, subject_id, occurrence, execution_id, attempt, owner, started_at, ended_at, outcome, reason "
            + $"FROM {runs} WHERE {filter}"
            + (after is null ? "" : $" AND (started_at, job_id, epoch) < (${next}, ${next + 1}, ${next + 2})")
            + $" ORDER BY started_at DESC, job_id DESC, epoch DESC LIMIT ${(after is null ? next : next + 3)}");
        command.Parameters.Add(Text(definition));
        if (subject is not null)
        {
            command.Parameters.Add(Text(subject.Kind));
            command.Parameters.Add(Text(subject.Id));
        }
        else
        {
            command.Parameters.Add(Uuid(job!.Value.Value));
        }
        if (after is not null)
        {
            command.Parameters.Add(new NpgsqlParameter<DateTimeOffset> { TypedValue = after.StartedAt.ToUniversalTime(), NpgsqlDbType = NpgsqlDbType.TimestampTz });
            command.Parameters.Add(Uuid(after.Job.Value));
            command.Parameters.Add(Int64(after.Epoch));
        }
        command.Parameters.Add(Int32(limit + 1));
        var entries = new List<JobRunEntry>(Math.Min(limit + 1, 1001));
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                entries.Add(new JobRunEntry(
                    new JobId(reader.GetGuid(0)),
                    reader.GetInt64(1),
                    reader.IsDBNull(2) ? null : Subject.Of(reader.GetString(2), reader.GetString(3)),
                    reader.IsDBNull(4) ? null : reader.GetString(4),
                    reader.GetGuid(5),
                    reader.GetInt32(6),
                    new OwnerId(reader.GetGuid(7)),
                    reader.GetFieldValue<DateTimeOffset>(8),
                    reader.GetFieldValue<DateTimeOffset>(9),
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

    /// <summary>
    /// Reads, for each definition, the first entry of <c>jobs_unsettled</c>, which holds only unsettled leases: one
    /// index probe per definition, however many leases are outstanding.
    /// </summary>
    public async ValueTask<IReadOnlyDictionary<string, TimeSpan>> NextExpiriesAsync(IReadOnlyCollection<string> definitions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definitions);
        var expiries = new Dictionary<string, TimeSpan>(StringComparer.Ordinal);
        if (definitions.Count == 0)
        {
            return expiries;
        }
        await using var command = dataSource.CreateCommand(
            "SELECT d.key, extract(epoch FROM e.t - clock_timestamp())::float8 FROM unnest($1) AS d (key) CROSS JOIN LATERAL "
            + $"(SELECT r.lease_expires_at AS t FROM {jobTable} AS r WHERE r.definition = d.key AND r.lease_expires_at IS NOT NULL "
            + "AND r.settled_epoch < r.lease_epoch ORDER BY r.lease_expires_at LIMIT 1) AS e");
        command.Parameters.Add(Texts([.. definitions.Distinct(StringComparer.Ordinal)]));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            expiries[reader.GetString(0)] = Seconds(reader.GetDouble(1));
        }
        return expiries;
    }

    private static NpgsqlParameter<string[]> Texts(string[] values) => new() { TypedValue = values, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text };
}
