using Npgsql;

namespace Corfedo.Work.PostgreSql.Schema;

/// <summary>
/// The framework's own tables, created and upgraded by numbered scripts. Scripts are applied in order under a
/// session-level advisory lock, so concurrent starts apply each script once.
/// </summary>
internal static class FrameworkSchema
{
    private static readonly (int Version, string Script)[] Scripts =
    [
        (1, """
            CREATE TABLE {schema}.schema_versions (
                version integer PRIMARY KEY,
                applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
            );
            """),
        (2, """
            CREATE TABLE {schema}.reconcile_generations (
                source text PRIMARY KEY,
                generation bigint NOT NULL
            );
            """),
        // Takes the generation locks of superseding sources in one fixed order: shared for holder writes, which must be
        // READ COMMITTED, exclusive for invalidation. A refusal raises an error, so the transaction cannot commit.
        (3, """
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
            """),
        // Job definitions by key: the newest stored revision with its shared policy, which every process enforces.
        (4, """
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
                stored_at timestamptz NOT NULL DEFAULT clock_timestamp()
            );
            """),
        // Job records of every definition, with the default lease fields. Outstanding jobs have state 0; terminal ones
        // keep their outcome until retention. merge_key is the merge identity, null for jobs that never merge. Every
        // index the claim statements use leads with the definition; due jobs are read per accepted argument version.
        (5, """
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
                ended_digest bytea
            );
            CREATE UNIQUE INDEX jobs_merge ON {schema}.jobs (definition, merge_key) WHERE merge_key IS NOT NULL AND state = 0;
            CREATE INDEX jobs_due ON {schema}.jobs (definition, args_version, due_at) WHERE lease_expires_at IS NULL AND due_at IS NOT NULL;
            CREATE INDEX jobs_expiry ON {schema}.jobs (definition, lease_expires_at) WHERE lease_expires_at IS NOT NULL;
            CREATE INDEX jobs_unsettled ON {schema}.jobs (definition, lease_expires_at) WHERE lease_expires_at IS NOT NULL AND settled_epoch < lease_epoch;
            CREATE INDEX jobs_partition ON {schema}.jobs (definition, partition_key) WHERE lease_expires_at IS NOT NULL AND settled_epoch < lease_epoch;
            CREATE INDEX jobs_subject ON {schema}.jobs (subject_kind, subject_id, definition) WHERE state = 0;
            CREATE INDEX jobs_versions ON {schema}.jobs (definition, args_version) WHERE state = 0;
            """),
        // Recurrences: one row per definition and merge identity (the subject, the merge key, or U+0002 for the
        // definition's code-defined recurrence), with its schedule, the next instant to plan and the instant waiting
        // under Overlap.Wait, and the occurrence whose end sets the next instant of an after-completion schedule.
        // Occurrences are jobs rows linked by recurrence_id, unique per occurrence identity; they have no merge_key, so
        // the outstanding-identity index holds standalone jobs only. Ended occurrences stay unconsumed until the planner
        // consumed them; occurrences to be skipped wait for finalization through the terminal path. Subject pauses are rows of job_pauses, the authority claims and
        // planning check; jobs.paused and job_recurrences.paused copy them for the due indexes and are repaired by maintenance.
        (6, """
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
                UNIQUE (definition, merge_key)
            );
            CREATE INDEX job_recurrences_next ON {schema}.job_recurrences (definition, next_at) WHERE next_at IS NOT NULL AND NOT paused;
            CREATE INDEX job_recurrences_waiting ON {schema}.job_recurrences (definition) WHERE waiting_at IS NOT NULL AND NOT paused;
            CREATE INDEX job_recurrences_subject ON {schema}.job_recurrences (definition, subject_kind, subject_id) WHERE subject_kind IS NOT NULL;
            CREATE INDEX job_recurrences_paused ON {schema}.job_recurrences (definition, id) WHERE paused;
            CREATE TABLE {schema}.job_pauses (
                definition text NOT NULL,
                subject_kind text NOT NULL,
                subject_id text NOT NULL,
                paused_at timestamptz NOT NULL,
                PRIMARY KEY (definition, subject_kind, subject_id)
            );
            ALTER TABLE {schema}.jobs
                ADD COLUMN recurrence_id uuid,
                ADD COLUMN occurrence text,
                ADD COLUMN unconsumed boolean NOT NULL DEFAULT false,
                ADD COLUMN skip_pending boolean NOT NULL DEFAULT false,
                ADD COLUMN paused boolean NOT NULL DEFAULT false;
            CREATE UNIQUE INDEX jobs_occurrence ON {schema}.jobs (recurrence_id, occurrence) WHERE recurrence_id IS NOT NULL;
            CREATE INDEX jobs_recurrence ON {schema}.jobs (recurrence_id) WHERE recurrence_id IS NOT NULL AND state = 0;
            CREATE INDEX jobs_unconsumed ON {schema}.jobs (definition, recurrence_id) WHERE unconsumed;
            CREATE INDEX jobs_skip ON {schema}.jobs (definition, id) WHERE skip_pending AND state = 0;
            CREATE INDEX jobs_paused ON {schema}.jobs (definition, id) WHERE paused AND state = 0;
            DROP INDEX {schema}.jobs_due;
            CREATE INDEX jobs_due ON {schema}.jobs (definition, args_version, due_at) WHERE lease_expires_at IS NULL AND due_at IS NOT NULL AND NOT paused;
            """),
        // Status, results, progress and run history. attempt_started_at is set by a claim that runs the handler and
        // cleared when that attempt ends; while it is set, the attempt has no run history entry yet. ended_execution_id,
        // result and result_version belong to the outcome of the latest ended execution. waited says a caller waits for
        // the job: the transition that leaves it without an outstanding execution clears it and signals the end.
        // jobs_ended finds a subject's latest outcome. job_runs holds one entry per attempt of definitions with run
        // history, keyed by job and the lease epoch of the claim that used the attempt.
        (7, """
            ALTER TABLE {schema}.job_definitions ADD COLUMN run_history boolean NOT NULL DEFAULT false;
            ALTER TABLE {schema}.jobs
                ADD COLUMN attempt_started_at timestamptz,
                ADD COLUMN progress_percent smallint,
                ADD COLUMN progress_message text,
                ADD COLUMN ended_execution_id uuid,
                ADD COLUMN result bytea,
                ADD COLUMN result_version integer,
                ADD COLUMN waited boolean NOT NULL DEFAULT false;
            CREATE INDEX jobs_ended ON {schema}.jobs (definition, subject_kind, subject_id, ended_at) WHERE ended_at IS NOT NULL AND subject_kind IS NOT NULL;
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
            """),
        // Retention. Each definition stores how long its terminal jobs and its run history entries are kept after they
        // ended. jobs_retention finds a definition's terminal jobs by end time; outstanding jobs, including restarted
        // follow-ups and reruns that keep the end of their previous execution, are not in it. job_runs_retention finds
        // a definition's entries by end time, with and without a subject.
        (8, """
            ALTER TABLE {schema}.job_definitions
                ADD COLUMN job_retention interval NOT NULL DEFAULT '7 days',
                ADD COLUMN run_history_retention interval NOT NULL DEFAULT '30 days';
            ALTER TABLE {schema}.job_definitions
                ALTER COLUMN job_retention DROP DEFAULT,
                ALTER COLUMN run_history_retention DROP DEFAULT;
            CREATE INDEX jobs_retention ON {schema}.jobs (definition, ended_at) WHERE state <> 0;
            CREATE INDEX job_runs_retention ON {schema}.job_runs (definition, ended_at);
            """),
        // The process registry: one row per process instance, keyed by its owner id, with its type, release and its own
        // stale window, so every reader judges its heartbeat alike. process_definitions holds the job definitions a
        // registered process registers: the revision of its code, whether it runs the handler with the argument versions
        // it accepts, and whether it declares the code-defined recurrence. Deleting a process deletes its definitions.
        (9, """
            CREATE TABLE {schema}.processes (
                owner uuid PRIMARY KEY,
                type text NOT NULL,
                release text,
                stale_after interval NOT NULL,
                started_at timestamptz NOT NULL,
                heartbeat_at timestamptz NOT NULL,
                stopped_at timestamptz
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
            """),
        // Rows with a copied pause flag, ordered by subject like job_pauses and covering the columns that decide whether
        // they are due, so finding flags no stored pause backs is a merge of two index-only scans in subject order.
        (10, """
            DROP INDEX {schema}.jobs_paused;
            DROP INDEX {schema}.job_recurrences_paused;
            CREATE INDEX jobs_paused ON {schema}.jobs (definition, subject_kind, subject_id) INCLUDE (id, due_at, lease_expires_at) WHERE paused AND state = 0;
            CREATE INDEX job_recurrences_paused ON {schema}.job_recurrences (definition, subject_kind, subject_id) INCLUDE (id, next_at, waiting_at) WHERE paused;
            """),
        // Calendars by name with the canonical rule of their newest revision, and published date sets with their data
        // revision and the last date they cover. Outstanding jobs and recurrences reference their calendar, so a calendar
        // live work uses cannot be deleted; the terminal transition moves a job's reference to ended_calendar, which has
        // no foreign key and is restored by a rerun. A job with a calendar is planned by its definition's maintenance:
        // calendar_at is the instant still to be planned, with the policy calendar_blocked for a blocked one; its due time
        // is then null, so it stays out of the due index. Planning sets due_at with window_end, the end of its allowed
        // window, which claims require to be in the future; 'infinity' marks work an operator made due ignoring the
        // calendar. calendar_unavailable holds a job whose instant needs dates a date set does not cover until a covering
        // revision is published. skip_reason is the reason of a job recorded to be skipped by its calendar; overlap skips
        // have none. jobs_window finds unheld jobs whose window closed. jobs_calendar serves the foreign key, the re-plan
        // on publish and the instants the coverage report compares with a date set's coverage, like
        // job_recurrences_calendar for recurrences; jobs_calendar_unavailable finds the work that waits for dates.
        // process_calendars holds the calendars a registered process registers.
        (11, """
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
            CREATE TABLE {schema}.process_calendars (
                owner uuid NOT NULL REFERENCES {schema}.processes ON DELETE CASCADE,
                calendar text NOT NULL,
                revision bigint NOT NULL,
                PRIMARY KEY (owner, calendar)
            );
            ALTER TABLE {schema}.job_recurrences
                ADD COLUMN calendar text REFERENCES {schema}.calendars (name),
                ADD COLUMN calendar_closed smallint;
            ALTER TABLE {schema}.jobs
                ADD COLUMN calendar text REFERENCES {schema}.calendars (name),
                ADD COLUMN ended_calendar text,
                ADD COLUMN calendar_blocked smallint,
                ADD COLUMN calendar_closed smallint,
                ADD COLUMN calendar_at timestamptz,
                ADD COLUMN window_end timestamptz,
                ADD COLUMN calendar_unavailable boolean NOT NULL DEFAULT false,
                ADD COLUMN skip_reason text;
            CREATE INDEX job_recurrences_calendar ON {schema}.job_recurrences (calendar, next_at) WHERE calendar IS NOT NULL;
            CREATE INDEX jobs_calendar ON {schema}.jobs (calendar, calendar_at) WHERE calendar IS NOT NULL;
            CREATE INDEX jobs_calendar_unavailable ON {schema}.jobs (calendar) WHERE calendar_unavailable AND state = 0;
            CREATE INDEX jobs_calendar_due ON {schema}.jobs (definition, calendar_at) WHERE calendar_at IS NOT NULL AND state = 0 AND NOT calendar_unavailable;
            CREATE INDEX jobs_window ON {schema}.jobs (definition, window_end) WHERE window_end IS NOT NULL AND lease_expires_at IS NULL AND state = 0;
            """),
        // Chains. Every step is a jobs row from the request on: chain_parent is the step or group it waits for (null for
        // the root), chain_group its enclosing group (null at the chain's top level), chain_condition the parent outcomes
        // under which it runs, and chain_waiting holds while its parent has not settled, without a due time, so it stays
        // out of the due indexes. job_chains holds a chain with its top level as a group, job_chain_groups its groups:
        // pending counts the nodes of the group not yet settled and worst the worst outcome among its steps that ran
        // (0 none, 1 succeeded, 2 cancelled, 3 failed); dynamic marks a group a running step added. jobs_chain finds a step's children and a chain's steps. An
        // occurrence of a recurring chain points at its chain with owned_chain while it waits for it; chain_outcome is
        // the outcome its definition's maintenance ends it with once the chain settled, found by jobs_chain_owner. Job
        // retention leaves chain steps to the retention of their chain.
        (12, """
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
            ALTER TABLE {schema}.jobs
                ADD COLUMN chain_id uuid,
                ADD COLUMN chain_generation bigint,
                ADD COLUMN chain_parent uuid,
                ADD COLUMN chain_group uuid,
                ADD COLUMN chain_condition smallint,
                ADD COLUMN chain_waiting boolean NOT NULL DEFAULT false,
                ADD COLUMN owned_chain uuid,
                ADD COLUMN chain_outcome smallint;
            CREATE INDEX jobs_chain ON {schema}.jobs (chain_id, chain_parent) WHERE chain_id IS NOT NULL;
            CREATE INDEX jobs_chain_owner ON {schema}.jobs (definition, id) WHERE chain_outcome IS NOT NULL AND state = 0;
            DROP INDEX {schema}.jobs_retention;
            CREATE INDEX jobs_retention ON {schema}.jobs (definition, ended_at) WHERE state <> 0 AND chain_id IS NULL;
            """),
        // Monitoring. A definition's expectations are stored with its revision. job_expectation_states holds per key (the
        // definition with a subject, or alone): failures in a row, and for keys expected to complete the next deadline
        // with its window start, the first completion in the window, the first and latest completion after a passed
        // deadline not yet processed, and the revision the deadline was computed from; 'infinity' holds a deadline whose
        // day a date set does not cover. whole marks the definition-alone key of a definition expected as a whole, which
        // every subject's success meets. work_incidents holds incidents with the lease fields of the incident lease
        // source and the delivery times of both events; work_incidents_open keeps one open incident per key and kind
        // and serves the probes of the ending transaction by its leading columns. processes.monitors records monitoring
        // processes; jobs.trace_parent the W3C trace id of the request that created a job; jobs_owned_chain finds
        // running occurrences of recurring chains.
        (13, """
            ALTER TABLE {schema}.job_definitions
                ADD COLUMN expect_time time,
                ADD COLUMN expect_zone text,
                ADD COLUMN expect_calendar text,
                ADD COLUMN expect_running interval,
                ADD COLUMN expect_failures integer;
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
            ALTER TABLE {schema}.processes ADD COLUMN monitors boolean NOT NULL DEFAULT false;
            ALTER TABLE {schema}.jobs ADD COLUMN trace_parent text;
            CREATE INDEX jobs_owned_chain ON {schema}.jobs (definition, attempt_started_at) WHERE owned_chain IS NOT NULL AND state = 0;
            """),
        // Expected-key changes. Every transaction that creates, replaces or removes a recurrence, or changes the copied
        // pause flag of one, appends the recurrence's key to job_expectation_changes, an insert-only queue no other
        // statement locks, so writers never wait on it; the monitoring loop applies and deletes the queued keys.
        // job_definitions.monitored_revision and monitored_paused record the revision and pause the loop last reconciled
        // every key of the definition against.
        (14, """
            CREATE TABLE {schema}.job_expectation_changes (
                definition text NOT NULL,
                subject_kind text,
                subject_id text
            );
            CREATE INDEX job_expectation_changes_definition ON {schema}.job_expectation_changes (definition);
            ALTER TABLE {schema}.job_definitions
                ADD COLUMN monitored_revision bigint,
                ADD COLUMN monitored_paused boolean;
            """),
    ];

    public static async Task MigrateAsync(NpgsqlDataSource dataSource, string schema, CancellationToken cancellationToken)
    {
        var quoted = Leases.LeaseSql.Quote(schema);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using (var lockCommand = new NpgsqlCommand("SELECT pg_advisory_lock(hashtextextended($1, 0))", connection))
        {
            lockCommand.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "corfedo-work|schema|" + schema });
            await lockCommand.ExecuteNonQueryAsync(cancellationToken);
        }
        try
        {
            await using (var create = new NpgsqlCommand($"CREATE SCHEMA IF NOT EXISTS {quoted}", connection))
            {
                await create.ExecuteNonQueryAsync(cancellationToken);
            }
            var applied = 0;
            await using (var exists = new NpgsqlCommand("SELECT to_regclass($1) IS NOT NULL", connection))
            {
                exists.Parameters.Add(new NpgsqlParameter<string> { TypedValue = quoted + ".schema_versions" });
                if ((bool)(await exists.ExecuteScalarAsync(cancellationToken))!)
                {
                    await using var current = new NpgsqlCommand($"SELECT coalesce(max(version), 0) FROM {quoted}.schema_versions", connection);
                    applied = (int)(await current.ExecuteScalarAsync(cancellationToken))!;
                }
            }
            foreach (var (version, script) in Scripts)
            {
                if (version <= applied)
                {
                    continue;
                }
                await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
                await using (var apply = new NpgsqlCommand(script.Replace("{schema}", quoted, StringComparison.Ordinal), connection, transaction))
                {
                    await apply.ExecuteNonQueryAsync(cancellationToken);
                }
                await using (var record = new NpgsqlCommand($"INSERT INTO {quoted}.schema_versions (version) VALUES ($1)", connection, transaction))
                {
                    record.Parameters.Add(new NpgsqlParameter<int> { TypedValue = version });
                    await record.ExecuteNonQueryAsync(cancellationToken);
                }
                await transaction.CommitAsync(cancellationToken);
            }
        }
        finally
        {
            await using var unlock = new NpgsqlCommand("SELECT pg_advisory_unlock(hashtextextended($1, 0))", connection);
            unlock.Parameters.Add(new NpgsqlParameter<string> { TypedValue = "corfedo-work|schema|" + schema });
            await unlock.ExecuteNonQueryAsync(CancellationToken.None);
        }
    }
}
