using Npgsql;

namespace Corfedo.Work.PostgreSql.Schema;

/// <summary>
/// The framework's own tables, created by a baseline script and upgraded by numbered scripts after it. Each script keeps
/// the previous release working, so a deployment applies it before that release's processes stop.
/// </summary>
internal static class FrameworkSchema
{
    private static readonly (int Version, string Script)[] Scripts =
    [
        // The baseline is version 14, so a database that already records versions 1 to 14 skips it; later scripts
        // number from 15.
        (14, """
            CREATE TABLE {schema}.schema_versions (
                version integer PRIMARY KEY,
                applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
            );

            -- Reconciliation: the generation of each superseding source.
            CREATE TABLE {schema}.reconcile_generations (
                source text PRIMARY KEY,
                generation bigint NOT NULL
            );

            -- Takes the generation locks of superseding sources in one fixed order: shared for holder writes, which must be
            -- READ COMMITTED, exclusive for invalidation. A refusal raises an error, so the transaction cannot commit.
            CREATE FUNCTION {schema}.generation_fence(keys text[], exclusive boolean) RETURNS void LANGUAGE plpgsql AS $fence$
            DECLARE
                key text;
            BEGIN
                IF NOT exclusive AND current_setting('transaction_isolation') <> 'read committed' THEN
                    RAISE EXCEPTION USING ERRCODE = 'CWF01', MESSAGE = format(
                        'Holder writes on superseding sources need READ COMMITTED isolation; the transaction uses %s.',
                        upper(current_setting('transaction_isolation')));
                END IF;
                FOR key IN SELECT DISTINCT k COLLATE "C" FROM unnest(keys) AS k ORDER BY 1 LOOP
                    IF exclusive THEN
                        PERFORM pg_advisory_xact_lock(hashtextextended(key, 0));
                    ELSE
                        PERFORM pg_advisory_xact_lock_shared(hashtextextended(key, 0));
                    END IF;
                END LOOP;
            END
            $fence$;

            -- Job definitions by key: the newest stored revision with its shared policy, which every process enforces,
            -- including how long terminal jobs and run history entries are kept and the expectations. monitored_revision
            -- and monitored_paused record the revision and pause the monitoring loop last reconciled every key of the
            -- definition against.
            CREATE TABLE {schema}.job_definitions (
                key text PRIMARY KEY,
                revision bigint NOT NULL,
                digest bytea NOT NULL,
                subject_kind text,
                attempts integer NOT NULL,
                backoff_kind smallint NOT NULL,
                backoff_initial interval NOT NULL,
                backoff_maximum interval NOT NULL,
                partition_limit integer,
                definition_limit integer,
                priority smallint NOT NULL,
                attempt_timeout interval,
                retry_timeouts boolean NOT NULL,
                late_grace interval,
                minimum_args_version integer NOT NULL,
                paused boolean NOT NULL DEFAULT false,
                stored_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                run_history boolean NOT NULL DEFAULT false,
                job_retention interval NOT NULL,
                run_history_retention interval NOT NULL,
                expect_time time,
                expect_zone text,
                expect_calendar text,
                expect_running interval,
                expect_failures integer,
                monitored_revision bigint,
                monitored_paused boolean
            );

            -- Calendars by name with the canonical rule of their newest revision, and published date sets with their data
            -- revision and the last date they cover.
            CREATE TABLE {schema}.calendars (
                name text PRIMARY KEY,
                revision bigint NOT NULL,
                digest bytea NOT NULL,
                rule text NOT NULL,
                stored_at timestamptz NOT NULL DEFAULT clock_timestamp()
            );
            CREATE TABLE {schema}.calendar_dates (
                name text PRIMARY KEY,
                revision bigint NOT NULL,
                covers_through date NOT NULL,
                dates date[] NOT NULL,
                published_at timestamptz NOT NULL DEFAULT clock_timestamp()
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
            -- Pauses: paused copies a stored subject pause from job_pauses for the due indexes; maintenance repairs it.
            -- jobs_paused orders the flagged rows by subject like job_pauses and covers the columns that decide whether
            -- they are due, so finding flags no stored pause backs is a merge of two index-only scans.
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
            -- in the future; 'infinity' marks work an operator made due ignoring the calendar. calendar_unavailable holds
            -- a job whose instant needs dates a date set does not cover until a covering revision is published.
            -- skip_reason is the reason of a job recorded to be skipped by its calendar; overlap skips have none.
            -- jobs_window finds unheld jobs whose window closed; jobs_calendar serves the foreign key, the re-plan on
            -- publish and the coverage report; jobs_calendar_unavailable finds the work that waits for dates.
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
                id uuid PRIMARY KEY,
                definition text NOT NULL,
                merge_key text,
                subject_kind text,
                subject_id text,
                partition_key text NOT NULL DEFAULT '',
                state smallint NOT NULL DEFAULT 0,
                args bytea NOT NULL,
                args_version integer NOT NULL,
                first_requested_at timestamptz NOT NULL,
                started boolean NOT NULL DEFAULT false,
                attempts integer NOT NULL DEFAULT 0,
                attempt_epoch bigint NOT NULL DEFAULT 0,
                pending_args bytea,
                pending_args_version integer,
                pending_due timestamptz,
                pending_first_requested_at timestamptz,
                outcome smallint,
                reason text,
                ended_at timestamptz,
                created_at timestamptz NOT NULL,
                due_at timestamptz,
                lease_owner uuid,
                lease_epoch bigint NOT NULL DEFAULT 0,
                lease_expires_at timestamptz,
                takeovers integer NOT NULL DEFAULT 0,
                failures integer NOT NULL DEFAULT 0,
                settled_epoch bigint NOT NULL DEFAULT 0,
                settle_failures integer NOT NULL DEFAULT 0,
                settle_retry_at timestamptz,
                cancel_requested_at timestamptz,
                execution_id uuid NOT NULL,
                ended_completion_id uuid,
                ended_digest bytea,
                recurrence_id uuid,
                occurrence text,
                unconsumed boolean NOT NULL DEFAULT false,
                skip_pending boolean NOT NULL DEFAULT false,
                paused boolean NOT NULL DEFAULT false,
                attempt_started_at timestamptz,
                progress_percent smallint,
                progress_message text,
                ended_execution_id uuid,
                result bytea,
                result_version integer,
                waited boolean NOT NULL DEFAULT false,
                calendar text REFERENCES {schema}.calendars (name),
                ended_calendar text,
                calendar_blocked smallint,
                calendar_closed smallint,
                calendar_at timestamptz,
                window_end timestamptz,
                calendar_unavailable boolean NOT NULL DEFAULT false,
                skip_reason text,
                chain_id uuid,
                chain_generation bigint,
                chain_parent uuid,
                chain_group uuid,
                chain_condition smallint,
                chain_waiting boolean NOT NULL DEFAULT false,
                owned_chain uuid,
                chain_outcome smallint,
                trace_parent text
            );
            CREATE UNIQUE INDEX jobs_merge ON {schema}.jobs (definition, merge_key) WHERE merge_key IS NOT NULL AND state = 0;
            CREATE INDEX jobs_due ON {schema}.jobs (definition, args_version, due_at) WHERE lease_expires_at IS NULL AND due_at IS NOT NULL AND NOT paused;
            CREATE INDEX jobs_expiry ON {schema}.jobs (definition, lease_expires_at) WHERE lease_expires_at IS NOT NULL;
            CREATE INDEX jobs_unsettled ON {schema}.jobs (definition, lease_expires_at) WHERE lease_expires_at IS NOT NULL AND settled_epoch < lease_epoch;
            CREATE INDEX jobs_partition ON {schema}.jobs (definition, partition_key) WHERE lease_expires_at IS NOT NULL AND settled_epoch < lease_epoch;
            CREATE INDEX jobs_subject ON {schema}.jobs (subject_kind, subject_id, definition) WHERE state = 0;
            CREATE INDEX jobs_versions ON {schema}.jobs (definition, args_version) WHERE state = 0;
            CREATE UNIQUE INDEX jobs_occurrence ON {schema}.jobs (recurrence_id, occurrence) WHERE recurrence_id IS NOT NULL;
            CREATE INDEX jobs_recurrence ON {schema}.jobs (recurrence_id) WHERE recurrence_id IS NOT NULL AND state = 0;
            CREATE INDEX jobs_unconsumed ON {schema}.jobs (definition, recurrence_id) WHERE unconsumed;
            CREATE INDEX jobs_skip ON {schema}.jobs (definition, id) WHERE skip_pending AND state = 0;
            CREATE INDEX jobs_paused ON {schema}.jobs (definition, subject_kind, subject_id) INCLUDE (id, due_at, lease_expires_at) WHERE paused AND state = 0;
            CREATE INDEX jobs_ended ON {schema}.jobs (definition, subject_kind, subject_id, ended_at) WHERE ended_at IS NOT NULL AND subject_kind IS NOT NULL;
            CREATE INDEX jobs_retention ON {schema}.jobs (definition, ended_at) WHERE state <> 0 AND chain_id IS NULL;
            CREATE INDEX jobs_calendar ON {schema}.jobs (calendar, calendar_at) WHERE calendar IS NOT NULL;
            CREATE INDEX jobs_calendar_unavailable ON {schema}.jobs (calendar) WHERE calendar_unavailable AND state = 0;
            CREATE INDEX jobs_calendar_due ON {schema}.jobs (definition, calendar_at) WHERE calendar_at IS NOT NULL AND state = 0 AND NOT calendar_unavailable;
            CREATE INDEX jobs_window ON {schema}.jobs (definition, window_end) WHERE window_end IS NOT NULL AND lease_expires_at IS NULL AND state = 0;
            CREATE INDEX jobs_chain ON {schema}.jobs (chain_id, chain_parent) WHERE chain_id IS NOT NULL;
            CREATE INDEX jobs_chain_owner ON {schema}.jobs (definition, id) WHERE chain_outcome IS NOT NULL AND state = 0;
            CREATE INDEX jobs_owned_chain ON {schema}.jobs (definition, attempt_started_at) WHERE owned_chain IS NOT NULL AND state = 0;

            -- Recurrences: one row per definition and merge identity (the subject, the merge key, or U+0002 for the
            -- definition's code-defined recurrence), with its schedule and calendar, the next instant to plan, the instant
            -- waiting under Overlap.Wait, and the occurrence whose end sets the next instant of an after-completion
            -- schedule. paused copies the subject's pause like jobs.paused.
            CREATE TABLE {schema}.job_recurrences (
                id uuid PRIMARY KEY,
                definition text NOT NULL,
                merge_key text NOT NULL,
                subject_kind text,
                subject_id text,
                partition_key text NOT NULL DEFAULT '',
                args bytea NOT NULL,
                args_version integer NOT NULL,
                schedule_kind smallint NOT NULL,
                period interval,
                interval_kind smallint,
                anchor timestamptz,
                expression text,
                time_zone text,
                missed smallint NOT NULL,
                overlap smallint NOT NULL,
                awaited_job uuid,
                declared_revision bigint,
                declared_digest bytea,
                next_at timestamptz,
                waiting_at timestamptz,
                paused boolean NOT NULL DEFAULT false,
                created_at timestamptz NOT NULL,
                updated_at timestamptz NOT NULL,
                calendar text REFERENCES {schema}.calendars (name),
                calendar_closed smallint,
                UNIQUE (definition, merge_key)
            );
            CREATE INDEX job_recurrences_next ON {schema}.job_recurrences (definition, next_at) WHERE next_at IS NOT NULL AND NOT paused;
            CREATE INDEX job_recurrences_waiting ON {schema}.job_recurrences (definition) WHERE waiting_at IS NOT NULL AND NOT paused;
            CREATE INDEX job_recurrences_subject ON {schema}.job_recurrences (definition, subject_kind, subject_id) WHERE subject_kind IS NOT NULL;
            CREATE INDEX job_recurrences_paused ON {schema}.job_recurrences (definition, subject_kind, subject_id) INCLUDE (id, next_at, waiting_at) WHERE paused;
            CREATE INDEX job_recurrences_calendar ON {schema}.job_recurrences (calendar, next_at) WHERE calendar IS NOT NULL;

            -- Subject pauses, the authority claims and planning check.
            CREATE TABLE {schema}.job_pauses (
                definition text NOT NULL,
                subject_kind text NOT NULL,
                subject_id text NOT NULL,
                paused_at timestamptz NOT NULL,
                PRIMARY KEY (definition, subject_kind, subject_id)
            );

            -- Run history: one entry per attempt of definitions with run history, keyed by job and the lease epoch of the
            -- claim that used the attempt, found by subject and, for retention, by end time.
            CREATE TABLE {schema}.job_runs (
                job_id uuid NOT NULL,
                epoch bigint NOT NULL,
                definition text NOT NULL,
                subject_kind text,
                subject_id text,
                occurrence text,
                execution_id uuid NOT NULL,
                attempt integer NOT NULL,
                owner uuid NOT NULL,
                started_at timestamptz NOT NULL,
                ended_at timestamptz NOT NULL,
                outcome smallint NOT NULL,
                reason text,
                PRIMARY KEY (job_id, epoch)
            );
            CREATE INDEX job_runs_subject ON {schema}.job_runs (definition, subject_kind, subject_id, started_at, job_id, epoch) WHERE subject_kind IS NOT NULL;
            CREATE INDEX job_runs_retention ON {schema}.job_runs (definition, ended_at);

            -- The process registry: one row per process instance, keyed by its owner id, with its type, release, its own
            -- stale window, so every reader judges its heartbeat alike, and whether it monitors. process_definitions holds
            -- the job definitions a registered process registers: the revision of its code, whether it runs the handler
            -- with the argument versions it accepts, and whether it declares the code-defined recurrence;
            -- process_calendars the calendars it registers. Deleting a process deletes both.
            CREATE TABLE {schema}.processes (
                owner uuid PRIMARY KEY,
                type text NOT NULL,
                release text,
                stale_after interval NOT NULL,
                started_at timestamptz NOT NULL,
                heartbeat_at timestamptz NOT NULL,
                stopped_at timestamptz,
                monitors boolean NOT NULL DEFAULT false
            );
            CREATE TABLE {schema}.process_definitions (
                owner uuid NOT NULL REFERENCES {schema}.processes ON DELETE CASCADE,
                definition text NOT NULL,
                revision bigint NOT NULL,
                runs boolean NOT NULL,
                accepted_args integer[] NOT NULL,
                declares_recurrence boolean NOT NULL,
                PRIMARY KEY (owner, definition)
            );
            CREATE TABLE {schema}.process_calendars (
                owner uuid NOT NULL REFERENCES {schema}.processes ON DELETE CASCADE,
                calendar text NOT NULL,
                revision bigint NOT NULL,
                PRIMARY KEY (owner, calendar)
            );

            -- Chains, with their top level as a group, and their groups: pending counts the nodes not yet settled and
            -- worst the worst outcome among the steps that ran (0 none, 1 succeeded, 2 cancelled, 3 failed); dynamic marks
            -- a group a running step added.
            CREATE TABLE {schema}.job_chains (
                id uuid PRIMARY KEY,
                generation bigint NOT NULL DEFAULT 1,
                root_job uuid NOT NULL,
                owner_job uuid,
                owner_definition text,
                definitions text[] NOT NULL,
                created_at timestamptz NOT NULL,
                settled_at timestamptz,
                outcome smallint,
                pending integer NOT NULL,
                worst smallint NOT NULL DEFAULT 0,
                delete_requested boolean NOT NULL DEFAULT false
            );
            CREATE INDEX job_chains_settled ON {schema}.job_chains (settled_at) WHERE settled_at IS NOT NULL;
            CREATE TABLE {schema}.job_chain_groups (
                id uuid PRIMARY KEY,
                chain_id uuid NOT NULL REFERENCES {schema}.job_chains ON DELETE CASCADE,
                generation bigint NOT NULL,
                parent uuid NOT NULL,
                enclosing_group uuid,
                pending integer NOT NULL,
                worst smallint NOT NULL DEFAULT 0,
                settled_at timestamptz,
                outcome smallint,
                dynamic boolean NOT NULL
            );
            CREATE INDEX job_chain_groups_parent ON {schema}.job_chain_groups (chain_id, parent);

            -- Monitoring. job_expectation_states holds per key (the definition with a subject, or alone): failures in a
            -- row, and for keys expected to complete the next deadline with its window start, the first completion in the
            -- window, the first and latest completion after a passed deadline not yet processed, and the revision the
            -- deadline was computed from; 'infinity' holds a deadline whose day a date set does not cover. whole marks the
            -- definition-alone key of a definition expected as a whole, which every subject's success meets.
            CREATE TABLE {schema}.job_expectation_states (
                definition text NOT NULL,
                subject_kind text,
                subject_id text,
                whole boolean NOT NULL DEFAULT false,
                failures integer NOT NULL DEFAULT 0,
                deadline_at timestamptz,
                window_start timestamptz,
                met_at timestamptz,
                first_after timestamptz,
                last_after timestamptz,
                revision bigint,
                CONSTRAINT job_expectation_states_key UNIQUE NULLS NOT DISTINCT (definition, subject_kind, subject_id)
            );
            CREATE INDEX job_expectation_states_deadline ON {schema}.job_expectation_states (definition, deadline_at) WHERE deadline_at IS NOT NULL;
            CREATE INDEX job_expectation_states_uncovered ON {schema}.job_expectation_states (definition, window_start) WHERE deadline_at = 'infinity';
            CREATE INDEX job_expectation_states_idle ON {schema}.job_expectation_states (definition) WHERE deadline_at IS NULL AND failures = 0;

            -- Every transaction that creates, replaces or removes a recurrence, or changes its copied pause flag, appends
            -- the recurrence's key to job_expectation_changes, an insert-only queue no other statement locks, so writers
            -- never wait on it; the monitoring loop applies and deletes the queued keys.
            CREATE TABLE {schema}.job_expectation_changes (
                definition text NOT NULL,
                subject_kind text,
                subject_id text
            );
            CREATE INDEX job_expectation_changes_definition ON {schema}.job_expectation_changes (definition);

            -- Incidents with the lease fields of the incident lease source and the delivery times of both events.
            -- work_incidents_open keeps one open incident per key and kind and serves the probes of the ending transaction
            -- by its leading columns.
            CREATE TABLE {schema}.work_incidents (
                id uuid PRIMARY KEY,
                kind smallint NOT NULL,
                definition text,
                subject_kind text,
                subject_id text,
                date_set text,
                job uuid,
                opened_at timestamptz NOT NULL,
                resolved_at timestamptz,
                opened_delivered_at timestamptz,
                resolved_delivered_at timestamptz,
                due_at timestamptz,
                lease_owner uuid,
                lease_epoch bigint NOT NULL DEFAULT 0,
                lease_expires_at timestamptz,
                takeovers integer NOT NULL DEFAULT 0,
                failures integer NOT NULL DEFAULT 0,
                settled_epoch bigint NOT NULL DEFAULT 0,
                cancel_requested_at timestamptz,
                execution_id uuid,
                ended_completion_id uuid,
                ended_digest bytea
            );
            CREATE UNIQUE INDEX work_incidents_open ON {schema}.work_incidents (definition, subject_kind, subject_id, kind, date_set) NULLS NOT DISTINCT WHERE resolved_at IS NULL;
            CREATE INDEX work_incidents_due ON {schema}.work_incidents (due_at) WHERE lease_expires_at IS NULL;
            CREATE INDEX work_incidents_expiry ON {schema}.work_incidents (lease_expires_at) WHERE lease_expires_at IS NOT NULL;
            CREATE INDEX work_incidents_opened ON {schema}.work_incidents (opened_at, id);
            CREATE INDEX work_incidents_definition ON {schema}.work_incidents (definition, opened_at, id) WHERE definition IS NOT NULL;
            CREATE INDEX work_incidents_retention ON {schema}.work_incidents (resolved_at) WHERE resolved_at IS NOT NULL;
            """),
        // The end of the last occurrence that advanced an after-completion schedule, which a replaced schedule measures
        // its first instant from; null when there is none or the schedule is not after-completion.
        (15, """
            ALTER TABLE {schema}.job_recurrences ADD COLUMN last_ended_at timestamptz;
            """),
    ];

    /// <summary>The schema version this release needs: the number of its last script.</summary>
    public static int Version => Scripts[^1].Version;

    /// <summary>
    /// One statement that applies the scripts not yet recorded, in order and in one transaction, under a
    /// transaction-level advisory lock on the schema, so concurrent runs apply each script once. It creates the schema
    /// only when it is missing, so an owner of a schema created for it needs no <c>CREATE</c> on the database. Each of
    /// <paramref name="runtimeRoles"/> then loses every privilege on the schema and its objects and gets the runtime
    /// set: <c>USAGE</c> on the schema, <c>SELECT</c>, <c>INSERT</c>, <c>UPDATE</c> and <c>DELETE</c> on its tables,
    /// <c>USAGE</c> and <c>SELECT</c> on its sequences and <c>EXECUTE</c> on its functions.
    /// </summary>
    public static string Script(string schema, IReadOnlyList<string> runtimeRoles)
    {
        var quoted = Leases.LeaseSql.Quote(schema);
        var text = new System.Text.StringBuilder("\n");
        text.Append($"""
            DECLARE
                applied integer := 0;
            BEGIN
                PERFORM pg_advisory_xact_lock(hashtextextended({Leases.LeaseSql.Literal(LockKey(schema))}, 0));
                IF to_regnamespace({Leases.LeaseSql.Literal(quoted)}) IS NULL THEN
                    CREATE SCHEMA {quoted};
                END IF;
                IF to_regclass({Leases.LeaseSql.Literal(quoted + ".schema_versions")}) IS NOT NULL THEN
                    SELECT coalesce(max(version), 0) INTO applied FROM {quoted}.schema_versions;
                END IF;

            """);
        foreach (var (version, script) in Scripts)
        {
            text.Append($"""
                    IF applied < {version} THEN
                        EXECUTE {DollarQuoted(script.Replace("{schema}", quoted, StringComparison.Ordinal), "corfedo_work_" + version)};
                        INSERT INTO {quoted}.schema_versions (version) VALUES ({version});
                    END IF;

                """);
        }
        foreach (var role in runtimeRoles)
        {
            var grantee = Leases.LeaseSql.Quote(role);
            text.Append($"""
                    REVOKE ALL ON SCHEMA {quoted} FROM {grantee};
                    REVOKE ALL ON ALL TABLES IN SCHEMA {quoted} FROM {grantee};
                    REVOKE ALL ON ALL SEQUENCES IN SCHEMA {quoted} FROM {grantee};
                    REVOKE ALL ON ALL FUNCTIONS IN SCHEMA {quoted} FROM {grantee};
                    GRANT USAGE ON SCHEMA {quoted} TO {grantee};
                    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {quoted} TO {grantee};
                    GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {quoted} TO {grantee};
                    GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {quoted} TO {grantee};

                """);
        }
        text.Append("END\n");
        return "DO " + DollarQuoted(text.ToString(), "corfedo_work") + ";\n";
    }

    /// <summary>
    /// <paramref name="body"/> as a dollar-quoted string whose tag, <paramref name="name"/> or a numbered variant of it,
    /// does not occur in the body, which may hold schema and role names.
    /// </summary>
    private static string DollarQuoted(string body, string name)
    {
        var tag = $"${name}$";
        for (var n = 1; body.Contains(tag, StringComparison.Ordinal); n++)
        {
            tag = $"${name}_x{n}$";
        }
        return tag + body + tag;
    }

    public static async Task MigrateAsync(NpgsqlDataSource dataSource, string schema, IReadOnlyList<string> runtimeRoles, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(Script(schema, runtimeRoles));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Checks, without changing anything, that the schema exists, that the login may use every object in it as the
    /// runtime set allows, and that it is at this release's version or a later one, which a rolling deployment
    /// applies before the last process of this release stops.
    /// </summary>
    /// <exception cref="InvalidOperationException">The schema is missing, older than this release, or not usable by the login.</exception>
    public static async Task VerifyAsync(NpgsqlDataSource dataSource, string schema, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        string user;
        bool exists;
        bool usage;
        string[] tables;
        string[] functions;
        bool versioned;
        await using (var command = new NpgsqlCommand("""
            WITH s AS (SELECT oid FROM pg_namespace WHERE nspname = $1)
            SELECT current_user::text, EXISTS (SELECT FROM s), coalesce((SELECT has_schema_privilege(s.oid, 'USAGE') FROM s), false),
                   ARRAY(SELECT c.relname::text FROM pg_class AS c JOIN s ON c.relnamespace = s.oid
                         WHERE c.relkind IN ('r', 'p') AND NOT (has_table_privilege(c.oid, 'SELECT') AND has_table_privilege(c.oid, 'INSERT')
                               AND has_table_privilege(c.oid, 'UPDATE') AND has_table_privilege(c.oid, 'DELETE'))
                         ORDER BY 1),
                   ARRAY(SELECT p.proname::text FROM pg_proc AS p JOIN s ON p.pronamespace = s.oid
                         WHERE NOT has_function_privilege(p.oid, 'EXECUTE') ORDER BY 1),
                   EXISTS (SELECT FROM pg_class AS c JOIN s ON c.relnamespace = s.oid WHERE c.relname = 'schema_versions')
            """, connection))
        {
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = schema });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            user = reader.GetString(0);
            exists = reader.GetBoolean(1);
            usage = reader.GetBoolean(2);
            tables = reader.GetFieldValue<string[]>(3);
            functions = reader.GetFieldValue<string[]>(4);
            versioned = reader.GetBoolean(5);
        }
        if (!exists)
        {
            throw new InvalidOperationException(
                $"The framework schema {schema} does not exist; this release needs version {Version}. Apply it in the migrate step with "
                + "PostgreSqlWorkSchema.MigrateAsync or the script of PostgreSqlWorkSchema.Script, or register the store with WorkSchemaMode.Migrate.");
        }
        var missing = new List<string>();
        if (!usage)
        {
            missing.Add($"USAGE on schema {schema}");
        }
        if (tables.Length > 0)
        {
            missing.Add($"SELECT, INSERT, UPDATE and DELETE on table{(tables.Length == 1 ? "" : "s")} {string.Join(", ", tables)}");
        }
        if (functions.Length > 0)
        {
            missing.Add($"EXECUTE on function{(functions.Length == 1 ? "" : "s")} {string.Join(", ", functions)}");
        }
        if (missing.Count > 0)
        {
            throw new InvalidOperationException(
                $"Login {user} lacks privileges on the framework schema {schema}: {string.Join("; ", missing)}. "
                + "Grant them by naming its role in the runtime roles of PostgreSqlWorkSchema.MigrateAsync or PostgreSqlWorkSchema.Script.");
        }
        // A schema created for the migrate step that has not run yet has no version table.
        var applied = 0;
        if (versioned)
        {
            await using var command = new NpgsqlCommand($"SELECT coalesce(max(version), 0) FROM {Leases.LeaseSql.Quote(schema)}.schema_versions", connection);
            applied = (int)(await command.ExecuteScalarAsync(cancellationToken))!;
        }
        if (applied < Version)
        {
            throw new InvalidOperationException(
                $"The framework schema {schema} is at version {applied}; this release needs version {Version}. Run this release's migrate step first.");
        }
    }

    private static string LockKey(string schema) => "corfedo-work|schema|" + schema;
}
