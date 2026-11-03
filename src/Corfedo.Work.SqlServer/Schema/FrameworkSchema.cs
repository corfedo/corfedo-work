using System.Data;
using Corfedo.Work.SqlServer.Leases;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Schema;

/// <summary>
/// The framework's own tables, created by a baseline script and upgraded by numbered scripts after it. Scripts are
/// applied in order under a session applock, so concurrent starts apply each script once. Every table disables lock
/// escalation: claims skip locked rows, and a table lock cannot be skipped.
/// </summary>
internal static class FrameworkSchema
{
    private static readonly (int Version, string Script)[] Scripts =
    [
        (1, """
            CREATE TABLE {schema}.schema_versions (
                version int NOT NULL PRIMARY KEY,
                applied_at datetime2(7) NOT NULL DEFAULT SYSUTCDATETIME()
            );

            -- Reconciliation: the generation of each superseding source.
            CREATE TABLE {schema}.reconcile_generations (
                source nvarchar(400) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY,
                generation bigint NOT NULL
            );

            -- Job definitions by key: the newest stored revision with its shared policy, which every process enforces,
            -- including how long terminal jobs and run history entries are kept and the expectations. Durations are
            -- ticks. monitored_revision and monitored_paused record the revision and pause the monitoring loop last
            -- reconciled every key of the definition against.
            CREATE TABLE {schema}.job_definitions (
                [key] nvarchar(200) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY,
                revision bigint NOT NULL,
                digest varbinary(64) NOT NULL,
                subject_kind nvarchar(100) COLLATE Latin1_General_100_BIN2,
                attempts int NOT NULL,
                backoff_kind smallint NOT NULL,
                backoff_initial bigint NOT NULL,
                backoff_maximum bigint NOT NULL,
                partition_limit int,
                definition_limit int,
                priority smallint NOT NULL,
                attempt_timeout bigint,
                retry_timeouts bit NOT NULL,
                late_grace bigint,
                minimum_args_version int NOT NULL,
                paused bit NOT NULL DEFAULT 0,
                stored_at datetime2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
                run_history bit NOT NULL DEFAULT 0,
                job_retention bigint NOT NULL,
                run_history_retention bigint NOT NULL,
                expect_time time(7),
                expect_zone nvarchar(100),
                expect_calendar nvarchar(128) COLLATE Latin1_General_100_BIN2,
                expect_running bigint,
                expect_failures int,
                monitored_revision bigint,
                monitored_paused bit
            );

            -- Calendars by name with the canonical rule of their newest revision, and published date sets with their data
            -- revision, the last date they cover and their dates as yyyy-MM-dd separated by commas.
            CREATE TABLE {schema}.calendars (
                name nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY,
                revision bigint NOT NULL,
                digest varbinary(64) NOT NULL,
                [rule] nvarchar(max) NOT NULL,
                stored_at datetime2(7) NOT NULL DEFAULT SYSUTCDATETIME()
            );
            CREATE TABLE {schema}.calendar_dates (
                name nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY,
                revision bigint NOT NULL,
                covers_through date NOT NULL,
                dates nvarchar(max) NOT NULL,
                published_at datetime2(7) NOT NULL DEFAULT SYSUTCDATETIME()
            );

            -- Job records of every definition, with the default lease fields. Outstanding jobs have state 0; terminal ones
            -- keep their outcome until retention. Every index the claim statements use leads with the definition; due jobs
            -- are read per accepted argument version.
            --
            -- Identity: merge_key is the merge identity of a standalone job, null for jobs that never merge and for
            -- occurrences, so jobs_merge holds standalone jobs only. An occurrence links its recurrence by recurrence_id,
            -- unique per occurrence identity (jobs_occurrence). An ended occurrence stays unconsumed until the planner
            -- consumed it; skip_pending marks one to be skipped through the terminal path.
            --
            -- Leases: filtered indexes cannot compare two columns, so jobs_expiry and jobs_partition cover every held or
            -- expired lease and carry the epochs that tell outstanding ones apart.
            --
            -- Pauses: paused copies a stored subject pause from job_pauses for the due indexes; maintenance repairs it.
            -- jobs_paused orders the flagged rows by subject like job_pauses and covers the columns that decide whether
            -- they are due.
            --
            -- Status and results: attempt_started_at is set by a claim that runs the handler and cleared when that attempt
            -- ends; while it is set, the attempt has no run history entry yet. ended_execution_id, result and
            -- result_version belong to the outcome of the latest ended execution. waited says a caller waits for the job:
            -- the transition that leaves it without an outstanding execution clears it and signals the end. jobs_ended
            -- finds a subject's latest outcome; jobs_retention a definition's terminal jobs by end time, leaving chain steps
            -- to their chain's retention.
            --
            -- Calendars: an outstanding job references its calendar, so a calendar live work uses cannot be deleted; the
            -- terminal transition moves the reference to ended_calendar, which has no foreign key and is restored by a
            -- rerun. A job with a calendar is planned by its definition's maintenance: calendar_at is the instant still to
            -- be planned, with the policy calendar_blocked for a blocked one; its due time is then null, so it stays out of
            -- the due index. Planning sets due_at with window_end, the end of its allowed window, which claims require to be
            -- in the future; the largest datetime2 marks work an operator made due ignoring the calendar.
            -- calendar_unavailable holds a job whose instant needs dates a date set does not cover until a covering
            -- revision is published. skip_reason is the reason of a job recorded to be skipped by its calendar; overlap
            -- skips have none.
            --
            -- Chains: every step is a job from the request on. chain_parent is the step or group it waits for (null for
            -- the root), chain_group its enclosing group (null at the chain's top level), chain_condition the parent
            -- outcomes under which it runs, and chain_waiting holds while its parent has not settled, without a due time,
            -- so it stays out of the due indexes. jobs_chain finds a step's children and a chain's steps. An occurrence of
            -- a recurring chain points at its chain with owned_chain while it waits for it; chain_outcome is the outcome
            -- its definition's maintenance ends it with once the chain settled, found by jobs_chain_owner, and
            -- jobs_owned_chain finds the running ones.
            --
            -- trace_parent is the W3C trace id of the request that created the job.
            CREATE TABLE {schema}.jobs (
                id uniqueidentifier NOT NULL CONSTRAINT jobs_key PRIMARY KEY,
                definition nvarchar(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
                merge_key nvarchar(512) COLLATE Latin1_General_100_BIN2,
                subject_kind nvarchar(100) COLLATE Latin1_General_100_BIN2,
                subject_id nvarchar(400) COLLATE Latin1_General_100_BIN2,
                partition_key nvarchar(400) COLLATE Latin1_General_100_BIN2 NOT NULL DEFAULT N'',
                state smallint NOT NULL DEFAULT 0,
                args varbinary(max) NOT NULL,
                args_version int NOT NULL,
                first_requested_at datetime2(7) NOT NULL,
                started bit NOT NULL DEFAULT 0,
                attempts int NOT NULL DEFAULT 0,
                attempt_epoch bigint NOT NULL DEFAULT 0,
                pending_args varbinary(max),
                pending_args_version int,
                pending_due datetime2(7),
                pending_first_requested_at datetime2(7),
                outcome smallint,
                reason nvarchar(max),
                ended_at datetime2(7),
                created_at datetime2(7) NOT NULL,
                due_at datetime2(7),
                lease_owner uniqueidentifier,
                lease_epoch bigint NOT NULL DEFAULT 0,
                lease_expires_at datetime2(7),
                takeovers int NOT NULL DEFAULT 0,
                failures int NOT NULL DEFAULT 0,
                settled_epoch bigint NOT NULL DEFAULT 0,
                settle_failures int NOT NULL DEFAULT 0,
                settle_retry_at datetime2(7),
                cancel_requested_at datetime2(7),
                execution_id uniqueidentifier NOT NULL,
                ended_completion_id uniqueidentifier,
                ended_digest varbinary(64),
                recurrence_id uniqueidentifier,
                occurrence nvarchar(200) COLLATE Latin1_General_100_BIN2,
                unconsumed bit NOT NULL DEFAULT 0,
                skip_pending bit NOT NULL DEFAULT 0,
                paused bit NOT NULL DEFAULT 0,
                attempt_started_at datetime2(7),
                progress_percent smallint,
                progress_message nvarchar(max),
                ended_execution_id uniqueidentifier,
                result varbinary(max),
                result_version int,
                waited bit NOT NULL DEFAULT 0,
                calendar nvarchar(128) COLLATE Latin1_General_100_BIN2 REFERENCES {schema}.calendars (name),
                ended_calendar nvarchar(128) COLLATE Latin1_General_100_BIN2,
                calendar_blocked smallint,
                calendar_closed smallint,
                calendar_at datetime2(7),
                window_end datetime2(7),
                calendar_unavailable bit NOT NULL DEFAULT 0,
                skip_reason nvarchar(max),
                chain_id uniqueidentifier,
                chain_generation bigint,
                chain_parent uniqueidentifier,
                chain_group uniqueidentifier,
                chain_condition smallint,
                chain_waiting bit NOT NULL DEFAULT 0,
                owned_chain uniqueidentifier,
                chain_outcome smallint,
                trace_parent nvarchar(100)
            );
            CREATE UNIQUE INDEX jobs_merge ON {schema}.jobs (definition, merge_key) WHERE merge_key IS NOT NULL AND state = 0;
            CREATE INDEX jobs_due ON {schema}.jobs (definition, args_version, due_at) INCLUDE (window_end) WHERE lease_expires_at IS NULL AND due_at IS NOT NULL AND paused = 0;
            CREATE INDEX jobs_expiry ON {schema}.jobs (definition, lease_expires_at) INCLUDE (settled_epoch, lease_epoch, due_at, args_version, paused, settle_retry_at, takeovers) WHERE lease_expires_at IS NOT NULL;
            CREATE INDEX jobs_partition ON {schema}.jobs (definition, partition_key) INCLUDE (lease_owner, lease_expires_at, settled_epoch, lease_epoch) WHERE lease_expires_at IS NOT NULL;
            CREATE INDEX jobs_subject ON {schema}.jobs (subject_kind, subject_id, definition) WHERE state = 0;
            CREATE INDEX jobs_versions ON {schema}.jobs (definition, args_version) WHERE state = 0;
            CREATE UNIQUE INDEX jobs_occurrence ON {schema}.jobs (recurrence_id, occurrence) WHERE recurrence_id IS NOT NULL;
            CREATE INDEX jobs_recurrence ON {schema}.jobs (recurrence_id) WHERE recurrence_id IS NOT NULL AND state = 0;
            CREATE INDEX jobs_unconsumed ON {schema}.jobs (definition, recurrence_id) WHERE unconsumed = 1;
            CREATE INDEX jobs_skip ON {schema}.jobs (definition, id) WHERE skip_pending = 1 AND state = 0;
            CREATE INDEX jobs_paused ON {schema}.jobs (definition, subject_kind, subject_id) INCLUDE (due_at, lease_expires_at) WHERE paused = 1 AND state = 0;
            CREATE INDEX jobs_ended ON {schema}.jobs (definition, subject_kind, subject_id, ended_at) WHERE ended_at IS NOT NULL AND subject_kind IS NOT NULL;
            CREATE INDEX jobs_retention ON {schema}.jobs (definition, ended_at) WHERE state <> 0 AND chain_id IS NULL;
            CREATE INDEX jobs_calendar ON {schema}.jobs (calendar, calendar_at) WHERE calendar IS NOT NULL;
            CREATE INDEX jobs_calendar_unavailable ON {schema}.jobs (calendar) WHERE calendar_unavailable = 1 AND state = 0;
            CREATE INDEX jobs_calendar_due ON {schema}.jobs (definition, calendar_at) WHERE calendar_at IS NOT NULL AND state = 0 AND calendar_unavailable = 0;
            CREATE INDEX jobs_window ON {schema}.jobs (definition, window_end) WHERE window_end IS NOT NULL AND lease_expires_at IS NULL AND state = 0;
            CREATE INDEX jobs_chain ON {schema}.jobs (chain_id, chain_parent) WHERE chain_id IS NOT NULL;
            CREATE INDEX jobs_chain_owner ON {schema}.jobs (definition, id) WHERE chain_outcome IS NOT NULL AND state = 0;
            CREATE INDEX jobs_owned_chain ON {schema}.jobs (definition, attempt_started_at) WHERE owned_chain IS NOT NULL AND state = 0;

            -- Recurrences: one row per definition and merge identity (the subject, the merge key, or U+0002 for the
            -- definition's code-defined recurrence), with its schedule and calendar, the next instant to plan, the instant
            -- waiting under Overlap.Wait, and the occurrence whose end sets the next instant of an after-completion
            -- schedule. paused copies the subject's pause like jobs.paused. period is in ticks.
            CREATE TABLE {schema}.job_recurrences (
                id uniqueidentifier NOT NULL PRIMARY KEY,
                definition nvarchar(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
                merge_key nvarchar(512) COLLATE Latin1_General_100_BIN2 NOT NULL,
                subject_kind nvarchar(100) COLLATE Latin1_General_100_BIN2,
                subject_id nvarchar(400) COLLATE Latin1_General_100_BIN2,
                partition_key nvarchar(400) COLLATE Latin1_General_100_BIN2 NOT NULL DEFAULT N'',
                args varbinary(max) NOT NULL,
                args_version int NOT NULL,
                schedule_kind smallint NOT NULL,
                period bigint,
                interval_kind smallint,
                anchor datetime2(7),
                expression nvarchar(400),
                time_zone nvarchar(100),
                missed smallint NOT NULL,
                overlap smallint NOT NULL,
                awaited_job uniqueidentifier,
                declared_revision bigint,
                declared_digest varbinary(64),
                next_at datetime2(7),
                waiting_at datetime2(7),
                paused bit NOT NULL DEFAULT 0,
                created_at datetime2(7) NOT NULL,
                updated_at datetime2(7) NOT NULL,
                calendar nvarchar(128) COLLATE Latin1_General_100_BIN2 REFERENCES {schema}.calendars (name),
                calendar_closed smallint,
                CONSTRAINT job_recurrences_identity UNIQUE (definition, merge_key)
            );
            CREATE INDEX job_recurrences_next ON {schema}.job_recurrences (definition, next_at) WHERE next_at IS NOT NULL AND paused = 0;
            CREATE INDEX job_recurrences_waiting ON {schema}.job_recurrences (definition) WHERE waiting_at IS NOT NULL AND paused = 0;
            CREATE INDEX job_recurrences_subject ON {schema}.job_recurrences (definition, subject_kind, subject_id) WHERE subject_kind IS NOT NULL;
            CREATE INDEX job_recurrences_paused ON {schema}.job_recurrences (definition, subject_kind, subject_id) INCLUDE (next_at, waiting_at) WHERE paused = 1;
            CREATE INDEX job_recurrences_calendar ON {schema}.job_recurrences (calendar, next_at) WHERE calendar IS NOT NULL;

            -- Subject pauses, the authority claims and planning check.
            CREATE TABLE {schema}.job_pauses (
                definition nvarchar(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
                subject_kind nvarchar(100) COLLATE Latin1_General_100_BIN2 NOT NULL,
                subject_id nvarchar(400) COLLATE Latin1_General_100_BIN2 NOT NULL,
                paused_at datetime2(7) NOT NULL,
                CONSTRAINT job_pauses_key PRIMARY KEY (definition, subject_kind, subject_id)
            );

            -- Run history: one entry per attempt of definitions with run history, keyed by job and the lease epoch of the
            -- claim that used the attempt, found by subject and, for retention, by end time.
            CREATE TABLE {schema}.job_runs (
                job_id uniqueidentifier NOT NULL,
                epoch bigint NOT NULL,
                definition nvarchar(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
                subject_kind nvarchar(100) COLLATE Latin1_General_100_BIN2,
                subject_id nvarchar(400) COLLATE Latin1_General_100_BIN2,
                occurrence nvarchar(200) COLLATE Latin1_General_100_BIN2,
                execution_id uniqueidentifier NOT NULL,
                attempt int NOT NULL,
                owner uniqueidentifier NOT NULL,
                started_at datetime2(7) NOT NULL,
                ended_at datetime2(7) NOT NULL,
                outcome smallint NOT NULL,
                reason nvarchar(max),
                CONSTRAINT job_runs_key PRIMARY KEY (job_id, epoch)
            );
            CREATE INDEX job_runs_subject ON {schema}.job_runs (definition, subject_kind, subject_id, started_at, job_id, epoch) WHERE subject_kind IS NOT NULL;
            CREATE INDEX job_runs_retention ON {schema}.job_runs (definition, ended_at);

            -- The process registry: one row per process instance, keyed by its owner id, with its type, release, its own
            -- stale window in ticks, so every reader judges its heartbeat alike, and whether it monitors.
            -- process_definitions holds the job definitions a registered process registers: the revision of its code,
            -- whether it runs the handler, and whether it declares the code-defined recurrence; process_definition_args
            -- the argument versions it accepts; process_calendars the calendars it registers. Deleting a process deletes
            -- them all.
            CREATE TABLE {schema}.processes (
                owner uniqueidentifier NOT NULL PRIMARY KEY,
                type nvarchar(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
                release nvarchar(200),
                stale_after bigint NOT NULL,
                started_at datetime2(7) NOT NULL,
                heartbeat_at datetime2(7) NOT NULL,
                stopped_at datetime2(7),
                monitors bit NOT NULL DEFAULT 0
            );
            CREATE TABLE {schema}.process_definitions (
                owner uniqueidentifier NOT NULL REFERENCES {schema}.processes ON DELETE CASCADE,
                definition nvarchar(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
                revision bigint NOT NULL,
                runs bit NOT NULL,
                declares_recurrence bit NOT NULL,
                CONSTRAINT process_definitions_key PRIMARY KEY (owner, definition)
            );
            CREATE TABLE {schema}.process_definition_args (
                owner uniqueidentifier NOT NULL,
                definition nvarchar(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
                args_version int NOT NULL,
                CONSTRAINT process_definition_args_key PRIMARY KEY (owner, definition, args_version),
                FOREIGN KEY (owner, definition) REFERENCES {schema}.process_definitions ON DELETE CASCADE
            );
            CREATE TABLE {schema}.process_calendars (
                owner uniqueidentifier NOT NULL REFERENCES {schema}.processes ON DELETE CASCADE,
                calendar nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
                revision bigint NOT NULL,
                CONSTRAINT process_calendars_key PRIMARY KEY (owner, calendar)
            );

            -- Chains, with their top level as a group, and their groups: pending counts the nodes not yet settled and
            -- worst the worst outcome among the steps that ran (0 none, 1 succeeded, 2 cancelled, 3 failed); dynamic marks
            -- a group a running step added. job_chain_definitions holds the definitions of each chain's steps.
            CREATE TABLE {schema}.job_chains (
                id uniqueidentifier NOT NULL PRIMARY KEY,
                generation bigint NOT NULL DEFAULT 1,
                root_job uniqueidentifier NOT NULL,
                owner_job uniqueidentifier,
                owner_definition nvarchar(200) COLLATE Latin1_General_100_BIN2,
                created_at datetime2(7) NOT NULL,
                settled_at datetime2(7),
                outcome smallint,
                pending int NOT NULL,
                worst smallint NOT NULL DEFAULT 0,
                delete_requested bit NOT NULL DEFAULT 0
            );
            CREATE INDEX job_chains_settled ON {schema}.job_chains (settled_at) WHERE settled_at IS NOT NULL;
            CREATE TABLE {schema}.job_chain_definitions (
                chain_id uniqueidentifier NOT NULL REFERENCES {schema}.job_chains ON DELETE CASCADE,
                definition nvarchar(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
                CONSTRAINT job_chain_definitions_key PRIMARY KEY (chain_id, definition)
            );
            CREATE TABLE {schema}.job_chain_groups (
                id uniqueidentifier NOT NULL PRIMARY KEY,
                chain_id uniqueidentifier NOT NULL REFERENCES {schema}.job_chains ON DELETE CASCADE,
                generation bigint NOT NULL,
                parent uniqueidentifier NOT NULL,
                enclosing_group uniqueidentifier,
                pending int NOT NULL,
                worst smallint NOT NULL DEFAULT 0,
                settled_at datetime2(7),
                outcome smallint,
                dynamic bit NOT NULL
            );
            CREATE INDEX job_chain_groups_parent ON {schema}.job_chain_groups (chain_id, parent);

            -- Monitoring. job_expectation_states holds per key (the definition with a subject, or alone): failures in a
            -- row, and for keys expected to complete the next deadline with its window start, the first completion in the
            -- window, the first and latest completion after a passed deadline not yet processed, and the revision the
            -- deadline was computed from; the largest datetime2 holds a deadline whose day a date set does not cover.
            -- whole marks the definition-alone key of a definition expected as a whole, which every subject's success
            -- meets. Unique indexes treat nulls as equal, so the definition-alone key is unique too.
            CREATE TABLE {schema}.job_expectation_states (
                definition nvarchar(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
                subject_kind nvarchar(100) COLLATE Latin1_General_100_BIN2,
                subject_id nvarchar(400) COLLATE Latin1_General_100_BIN2,
                whole bit NOT NULL DEFAULT 0,
                failures int NOT NULL DEFAULT 0,
                deadline_at datetime2(7),
                window_start datetime2(7),
                met_at datetime2(7),
                first_after datetime2(7),
                last_after datetime2(7),
                revision bigint
            );
            CREATE UNIQUE CLUSTERED INDEX job_expectation_states_key ON {schema}.job_expectation_states (definition, subject_kind, subject_id);
            CREATE INDEX job_expectation_states_deadline ON {schema}.job_expectation_states (definition, deadline_at) WHERE deadline_at IS NOT NULL;
            CREATE INDEX job_expectation_states_uncovered ON {schema}.job_expectation_states (definition, window_start) WHERE deadline_at = '9999-12-31T23:59:59.9999999';
            CREATE INDEX job_expectation_states_idle ON {schema}.job_expectation_states (definition) WHERE deadline_at IS NULL AND failures = 0;

            -- Every transaction that creates, replaces or removes a recurrence, or changes its copied pause flag, appends
            -- the recurrence's key to job_expectation_changes, an insert-only queue no other statement locks, so writers
            -- never wait on it; the monitoring loop applies and deletes the queued keys.
            CREATE TABLE {schema}.job_expectation_changes (
                n bigint IDENTITY NOT NULL PRIMARY KEY,
                definition nvarchar(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
                subject_kind nvarchar(100) COLLATE Latin1_General_100_BIN2,
                subject_id nvarchar(400) COLLATE Latin1_General_100_BIN2
            );
            CREATE INDEX job_expectation_changes_definition ON {schema}.job_expectation_changes (definition);

            -- Incidents with the lease fields of the incident lease source and the delivery times of both events.
            -- work_incidents_open keeps one open incident per key and kind and serves the probes of the ending transaction
            -- by its leading columns.
            CREATE TABLE {schema}.work_incidents (
                id uniqueidentifier NOT NULL PRIMARY KEY,
                kind smallint NOT NULL,
                definition nvarchar(200) COLLATE Latin1_General_100_BIN2,
                subject_kind nvarchar(100) COLLATE Latin1_General_100_BIN2,
                subject_id nvarchar(400) COLLATE Latin1_General_100_BIN2,
                date_set nvarchar(128) COLLATE Latin1_General_100_BIN2,
                job uniqueidentifier,
                opened_at datetime2(7) NOT NULL,
                resolved_at datetime2(7),
                opened_delivered_at datetime2(7),
                resolved_delivered_at datetime2(7),
                due_at datetime2(7),
                lease_owner uniqueidentifier,
                lease_epoch bigint NOT NULL DEFAULT 0,
                lease_expires_at datetime2(7),
                takeovers int NOT NULL DEFAULT 0,
                failures int NOT NULL DEFAULT 0,
                settled_epoch bigint NOT NULL DEFAULT 0,
                cancel_requested_at datetime2(7),
                execution_id uniqueidentifier,
                ended_completion_id uniqueidentifier,
                ended_digest varbinary(64)
            );
            CREATE UNIQUE INDEX work_incidents_open ON {schema}.work_incidents (definition, subject_kind, subject_id, kind, date_set) WHERE resolved_at IS NULL;
            CREATE INDEX work_incidents_due ON {schema}.work_incidents (due_at) WHERE lease_expires_at IS NULL;
            CREATE INDEX work_incidents_expiry ON {schema}.work_incidents (lease_expires_at) INCLUDE (settled_epoch, lease_epoch) WHERE lease_expires_at IS NOT NULL;
            CREATE INDEX work_incidents_opened ON {schema}.work_incidents (opened_at, id);
            CREATE INDEX work_incidents_definition ON {schema}.work_incidents (definition, opened_at, id) WHERE definition IS NOT NULL;
            CREATE INDEX work_incidents_retention ON {schema}.work_incidents (resolved_at) WHERE resolved_at IS NOT NULL;

            """),
    ];

    /// <summary>The framework schema's tables, whose lock escalation every script leaves disabled.</summary>
    private const string DisableEscalation = """
        DECLARE @table nvarchar(600);
        DECLARE tables CURSOR LOCAL FAST_FORWARD FOR
            SELECT QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) FROM sys.tables AS t JOIN sys.schemas AS s ON s.schema_id = t.schema_id
            WHERE s.name = @schema AND t.lock_escalation_desc <> N'DISABLE';
        OPEN tables;
        FETCH NEXT FROM tables INTO @table;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            EXEC (N'ALTER TABLE ' + @table + N' SET (LOCK_ESCALATION = DISABLE)');
            FETCH NEXT FROM tables INTO @table;
        END
        CLOSE tables;
        DEALLOCATE tables;
        """;

    /// <summary>
    /// Creates the signals table when it is missing, as dynamic SQL so a batch compiles before its schema exists. A send
    /// is an insert in the sending transaction, so it becomes readable when that transaction commits and disappears with
    /// its rollback. rv orders rows by write; listeners read below MIN_ACTIVE_ROWVERSION(), where every row is
    /// committed or gone, and prune rows older than the retention by sent_at. An application that signals without a
    /// store creates it before the baseline, which then keeps it.
    /// </summary>
    private static string SignalsTable(string schema)
    {
        var table = Tsql.Quote(schema) + ".signals";
        var ddl = $"""
            CREATE TABLE {table} (
                rv rowversion NOT NULL,
                topic varchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL,
                k nvarchar(1024) COLLATE Latin1_General_100_BIN2 NOT NULL,
                sent_at datetime2(7) NOT NULL DEFAULT SYSUTCDATETIME()
            );
            CREATE UNIQUE CLUSTERED INDEX signals_rv ON {table} (rv);
            ALTER TABLE {table} SET (LOCK_ESCALATION = DISABLE);
            """;
        return $"IF OBJECT_ID({Tsql.Literal(table)}, N'U') IS NULL EXEC ({Tsql.Literal(ddl)});\n";
    }

    /// <summary>
    /// A batch that creates the framework schema and its signals table when they are missing, for an application that
    /// signals without a store. It runs in a transaction of its own under the schema's migration lock.
    /// </summary>
    public static string EnsureSignals(string schema) => Tsql.OwnTransaction(
        LeaseSql.AppLock(Tsql.Literal(Tsql.LockName("schema", schema)), "Exclusive")
        + $"IF SCHEMA_ID({Tsql.Literal(schema)}) IS NULL EXEC ({Tsql.Literal("CREATE SCHEMA " + Tsql.Quote(schema))});\n"
        + SignalsTable(schema));

    /// <summary>
    /// Refuses a database without READ_COMMITTED_SNAPSHOT, then applies the scripts not yet recorded, each in its own
    /// transaction, holding a session applock on the schema for the whole run.
    /// </summary>
    public static async Task MigrateAsync(string connectionString, string schema, CancellationToken cancellationToken)
    {
        var quoted = Tsql.Quote(schema);
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await RequireSnapshotReadsAsync(connection, cancellationToken);
        var lockName = Tsql.LockName("schema", schema);
        await using (var lockCommand = Tsql.Command(connection, null,
            "DECLARE @r int; EXEC @r = sp_getapplock @Resource = @lock, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = -1; "
            + "IF @r < 0 THROW 51000, N'The framework schema lock was not granted.', 1;",
            Tsql.Text("@lock", lockName, 255)))
        {
            await lockCommand.ExecuteNonQueryAsync(cancellationToken);
        }
        try
        {
            await using (var create = Tsql.Command(connection, null,
                "IF SCHEMA_ID(@schema) IS NULL EXEC (N'CREATE SCHEMA ' + @quoted);",
                Tsql.Text("@schema", schema, 128),
                Tsql.Text("@quoted", quoted, 300)))
            {
                await create.ExecuteNonQueryAsync(cancellationToken);
            }
            var applied = 0;
            await using (var current = Tsql.Command(connection, null,
                $"IF OBJECT_ID(@table, N'U') IS NULL SELECT 0 ELSE EXEC (N'SELECT COALESCE(MAX(version), 0) FROM ' + @table);",
                Tsql.Text("@table", quoted + ".schema_versions", 400)))
            {
                applied = (int)(await current.ExecuteScalarAsync(cancellationToken))!;
            }
            foreach (var (version, script) in Scripts)
            {
                if (version <= applied)
                {
                    continue;
                }
                await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken);
                var text = script.Replace("{schema}", quoted, StringComparison.Ordinal) + (version == 1 ? "\n" + SignalsTable(schema) : "");
                await using (var apply = Tsql.Command(connection, transaction, text))
                {
                    await apply.ExecuteNonQueryAsync(cancellationToken);
                }
                await using (var record = Tsql.Command(connection, transaction,
                    DisableEscalation + $"\nINSERT INTO {quoted}.schema_versions (version) VALUES (@version);",
                    Tsql.Text("@schema", schema, 128),
                    Tsql.Int32("@version", version)))
                {
                    await record.ExecuteNonQueryAsync(cancellationToken);
                }
                await transaction.CommitAsync(cancellationToken);
            }
        }
        finally
        {
            await using var unlock = Tsql.Command(connection, null,
                "EXEC sp_releaseapplock @Resource = @lock, @LockOwner = 'Session';",
                Tsql.Text("@lock", lockName, 255));
            await unlock.ExecuteNonQueryAsync(CancellationToken.None);
        }
    }

    /// <summary>
    /// Claims and limits read rows they do not lock and rely on reading their committed versions without waiting for
    /// writers, which READ_COMMITTED_SNAPSHOT provides at read committed isolation.
    /// </summary>
    /// <exception cref="InvalidOperationException">The database does not read committed snapshots.</exception>
    private static async Task RequireSnapshotReadsAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = Tsql.Command(connection, null, "SELECT DB_NAME(), is_read_committed_snapshot_on FROM sys.databases WHERE database_id = DB_ID();");
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        if (!reader.GetBoolean(1))
        {
            var name = reader.GetString(0);
            throw new InvalidOperationException(
                $"Database {name} does not use READ_COMMITTED_SNAPSHOT, which Corfedo Work requires; enable it with ALTER DATABASE {Tsql.Quote(name)} SET READ_COMMITTED_SNAPSHOT ON.");
        }
    }
}
