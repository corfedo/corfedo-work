using Corfedo.Work.SqlServer.Leases;

namespace Corfedo.Work.SqlServer.Jobs;

internal sealed partial class SqlServerJobStore
{
    /// <summary>
    /// Takes the definition fence exclusively, waiting for claims in flight, then stores the pause state that every
    /// later claim and planning pass reads after its fence. Resuming signals claimers and planners.
    /// </summary>
    public async ValueTask PauseDefinitionAsync(WorkTransaction transaction, LeaseSource<JobId> source, bool paused, CancellationToken cancellationToken)
    {
        var work = SqlServerTransaction.From(transaction);
        var definition = (await SqlAsync(source, cancellationToken)).Job!.Definition;
        await using (var command = Tsql.Command(work,
            LeaseSql.AppLock("@fence", "Exclusive") + $"UPDATE {definitions} SET paused = @paused WHERE [key] = @definition;",
            Tsql.Text("@fence", LeaseSql.DefinitionLockName(schema, definition), 255),
            Tsql.Text("@definition", definition, 200),
            Tsql.Bool("@paused", paused)))
        {
            await command.NonQueryAsync(cancellationToken);
        }
        if (!paused)
        {
            await NotifyDueAsync(work, source, cancellationToken);
            await NotifyPlanAsync(work, source, cancellationToken);
        }
    }

    /// <summary>
    /// Takes the subject's pause lock exclusively, waiting for claims that hold it in shared mode, then stores or deletes
    /// the subject's pause, the authority claims and planning check after taking the lock, and copies it to the flags of
    /// the subject's recurrences and then its outstanding jobs, the lock order of recurrence operations, which keeps
    /// paused work out of the due indexes; rows are found without locks and locked through their key in id order. When
    /// a recurrence's flag changes, the key is queued for the monitoring loop, which is signalled. Requests and
    /// schedules take no pause lock: a row stored concurrently may carry a stale flag, which claims and planning ignore
    /// in favour of the stored pause and maintenance repairs.
    /// </summary>
    public async ValueTask PauseSubjectAsync(WorkTransaction transaction, LeaseSource<JobId> source, Subject subject, bool paused, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(subject);
        var work = SqlServerTransaction.From(transaction);
        var sql = await SqlAsync(source, cancellationToken);
        var definition = sql.Job!.Definition;
        var store = paused
            ? $"INSERT INTO {pauses} ([definition], subject_kind, subject_id, paused_at) SELECT @definition, @kind, @id, SYSUTCDATETIME() "
                + $"WHERE NOT EXISTS (SELECT 1 FROM {pauses} WITH (UPDLOCK, ROWLOCK) WHERE [definition] = @definition AND subject_kind = @kind AND subject_id = @id);"
            : $"DELETE FROM {pauses} WHERE [definition] = @definition AND subject_kind = @kind AND subject_id = @id;";
        await using (var command = Tsql.Command(work, $"""
            {LeaseSql.AppLock("@lock", "Exclusive")}{store}
            DECLARE @recurrences TABLE (id uniqueidentifier NOT NULL PRIMARY KEY);
            INSERT INTO @recurrences (id) SELECT c.id FROM {recurrences} AS c WITH (INDEX(job_recurrences_subject))
            WHERE c.[definition] = @definition AND c.subject_kind IS NOT NULL AND c.subject_kind = @kind AND c.subject_id = @id AND c.paused <> @paused;
            UPDATE c SET paused = @paused FROM @recurrences AS f INNER LOOP JOIN {recurrences} AS c WITH (UPDLOCK, ROWLOCK, FORCESEEK) ON c.id = f.id
            WHERE c.paused <> @paused;
            DECLARE @flagged int = @@ROWCOUNT;
            IF @flagged > 0 INSERT INTO {changes} ([definition], subject_kind, subject_id) VALUES (@definition, @kind, @id);
            {NotifyMonitor("@flagged > 0")}DECLARE @jobs TABLE (id uniqueidentifier NOT NULL PRIMARY KEY);
            INSERT INTO @jobs (id) SELECT r.id FROM {sql.Table} AS r WITH (INDEX(jobs_subject))
            WHERE r.subject_kind = @kind AND r.subject_id = @id AND r.[definition] = @definition AND r.state = 0 AND r.paused <> @paused;
            UPDATE r SET paused = @paused FROM @jobs AS f INNER LOOP JOIN {sql.Table} AS r WITH (UPDLOCK, ROWLOCK, INDEX(jobs_key), FORCESEEK) ON r.id = f.id
            WHERE r.state = 0 AND r.paused <> @paused;
            """,
            Tsql.Text("@lock", LeaseSql.SubjectLockName(schema, definition, subject), 255),
            Tsql.Text("@definition", definition, 200),
            Tsql.Text("@kind", subject.Kind, 100),
            Tsql.Text("@id", subject.Id, 400),
            Tsql.Bool("@paused", paused),
            MonitorSignal(source)))
        {
            await command.NonQueryAsync(cancellationToken);
        }
        if (!paused)
        {
            await NotifyDueAsync(work, source, cancellationToken);
            await NotifyPlanAsync(work, source, cancellationToken);
        }
    }

    /// <summary>
    /// Sets the copied pause flags of recurrences and then outstanding jobs to the stored subject pauses, in both
    /// directions, up to <paramref name="limit"/> rows each: rows are found without locks, locked through their key in
    /// id order without waiting, and evaluated again after the lock is held. The keys of repaired recurrences are queued
    /// for the monitoring loop, which is signalled. A flag left set by a schedule or request that raced a resume is
    /// cleared here, so its recurrence is planned and its job claimed again without further requests. Clearing signals
    /// planners and claimers.
    /// </summary>
    public async ValueTask<int> RepairPausesAsync(LeaseSource<JobId> source, int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var sql = await SqlAsync(source, cancellationToken);
        var definition = sql.Job!.Definition;
        string Stored(string alias) =>
            $"EXISTS (SELECT 1 FROM {pauses} AS p WHERE p.[definition] = {alias}.[definition] AND p.subject_kind = {alias}.subject_kind AND p.subject_id = {alias}.subject_id)";
        string Repair(string table, string alias, string candidates, string still, string set, string output) => $"""
            DELETE FROM @candidates;
            INSERT INTO @candidates (id) {candidates};
            UPDATE {alias} SET paused = {set} {output}
            FROM @candidates AS f INNER LOOP JOIN {table} AS {alias} WITH (UPDLOCK, ROWLOCK, READPAST, FORCESEEK) ON {alias}.id = f.id WHERE {still};
            INSERT INTO @repaired (n) VALUES (@@ROWCOUNT);

            """;
        const string queue = "OUTPUT inserted.[definition], inserted.subject_kind, inserted.subject_id INTO @queued";
        var text = $"""
            DECLARE @candidates TABLE (id uniqueidentifier NOT NULL PRIMARY KEY);
            DECLARE @repaired TABLE (step int IDENTITY NOT NULL PRIMARY KEY, n int NOT NULL);
            DECLARE @queued TABLE ([definition] nvarchar(200) COLLATE {Tsql.Binary} NOT NULL, subject_kind nvarchar(100) COLLATE {Tsql.Binary}, subject_id nvarchar(400) COLLATE {Tsql.Binary});
            {Repair(recurrences, "c",
                $"SELECT TOP (@limit) c.id FROM {recurrences} AS c WITH (INDEX(job_recurrences_paused)) WHERE c.[definition] = @definition AND c.paused = 1 AND NOT {Stored("c")}",
                $"c.paused = 1 AND NOT {Stored("c")}", "0", queue)}{Repair(recurrences, "c",
                $"SELECT TOP (@limit) c.id FROM {pauses} AS p INNER LOOP JOIN {recurrences} AS c WITH (INDEX(job_recurrences_subject)) ON c.[definition] = p.[definition] "
                + "AND c.subject_kind = p.subject_kind AND c.subject_id = p.subject_id WHERE p.[definition] = @definition AND c.subject_kind IS NOT NULL AND c.paused = 0",
                $"c.paused = 0 AND {Stored("c")}", "1", queue)}{Repair(sql.Table, "r",
                $"SELECT TOP (@limit) r.id FROM {sql.Table} AS r WITH (INDEX(jobs_paused)) WHERE r.[definition] = @definition AND r.paused = 1 AND r.state = 0 AND NOT {Stored("r")}",
                $"r.paused = 1 AND r.state = 0 AND NOT {Stored("r")}", "0", "")}{Repair(sql.Table, "r",
                $"SELECT TOP (@limit) r.id FROM {pauses} AS p INNER LOOP JOIN {sql.Table} AS r WITH (INDEX(jobs_subject)) ON r.subject_kind = p.subject_kind "
                + "AND r.subject_id = p.subject_id AND r.[definition] = p.[definition] WHERE p.[definition] = @definition AND r.state = 0 AND r.paused = 0",
                $"r.paused = 0 AND r.state = 0 AND {Stored("r")}", "1", "")}INSERT INTO {changes} ([definition], subject_kind, subject_id) SELECT [definition], subject_kind, subject_id FROM @queued;
            {NotifyMonitor("EXISTS (SELECT 1 FROM @queued)")}{signals.InsertIf(WorkTopics.Plan, "@source", "(SELECT n FROM @repaired WHERE step = 1) > 0")}{signals.InsertIf(WorkTopics.Due, "@source", "(SELECT n FROM @repaired WHERE step = 3) > 0")}SELECT MAX(n) FROM @repaired;
            """;
        return await InTransactionAsync(async work =>
        {
            await using var command = Tsql.Command(work, text,
                Tsql.Text("@definition", definition, 200),
                Tsql.Int32("@limit", limit),
                Tsql.Text("@source", source.Name, 1024),
                MonitorSignal(source));
            return (int)(await command.ScalarAsync(cancellationToken))!;
        }, cancellationToken);
    }
}
