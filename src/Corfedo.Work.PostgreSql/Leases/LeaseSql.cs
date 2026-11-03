using System.Globalization;
using System.Text;

namespace Corfedo.Work.PostgreSql.Leases;

/// <summary>
/// The statements of one lease source, and its process-local claim turn. Column and table names come from the source
/// definition and are quoted; values are positional parameters, keys first.
/// </summary>
internal sealed class LeaseSql
{
    public LeaseSql(string name, StoreLocation store, IReadOnlyList<KeyField> keyFields, string? partition, LeasePolicy policy, LeaseFields fields, bool settles, ReconcileSettings? reconcile, JobSourceSettings? job, string frameworkSchema)
    {
        Name = name;
        Job = job;
        Reconcile = reconcile;
        Supersedes = reconcile is { Supersedes: true };
        Settles = settles;
        KeyCount = keyFields.Count;
        Table = Quote(store.Schema) + "." + Quote(store.Name);
        Keys = keyFields.Select(f => "r." + Quote(f.Name)).ToArray();
        Partition = partition is null ? null : "r." + Quote(partition);
        Due = "r." + Quote(fields.DueAt);
        Owner = "r." + Quote(fields.LeaseOwner);
        Epoch = "r." + Quote(fields.LeaseEpoch);
        Expires = "r." + Quote(fields.LeaseExpiresAt);
        Takeovers = "r." + Quote(fields.Takeovers);
        Failures = "r." + Quote(fields.Failures);
        Settled = "r." + Quote(fields.SettledEpoch);
        // Application sources mark unsettled leases with their LeaseOutstanding column, which leads their settlement and
        // limit indexes; the framework's job table has indexes of its own over the epochs. The column is null once
        // settled, so the condition tests for it as well and is false then rather than unknown, under NOT and in
        // select lists.
        var outstanding = "r." + Quote(fields.LeaseOutstanding);
        Unsettled = job is null ? $"({outstanding} = 1 AND {outstanding} IS NOT NULL)" : $"{Settled} < {Epoch}";
        SettleFailures = "r." + Quote(fields.SettleFailures);
        SettleRetryAt = "r." + Quote(fields.SettleRetryAt);
        Cancel = "r." + Quote(fields.CancelRequestedAt);
        Execution = "r." + Quote(fields.ExecutionId);
        EndedId = "r." + Quote(fields.EndedCompletionId);
        EndedDigest = "r." + Quote(fields.EndedDigest);
        Unnest = "unnest(" + string.Join(", ", keyFields.Select((f, i) => $"${i + 1}::{KeyTypes.SqlType(f.Type)}[]")) + ") AS u("
            + string.Join(", ", keyFields.Select((_, i) => "k" + i)) + ")";
        // Job sources share the framework's job table: every statement is scoped to the source's definition.
        Scope = job is null ? "true" : $"r.\"definition\" = {Literal(job.Definition)}";
        KeyJoin = string.Join(" AND ", Keys.Select((k, i) => $"{k} = u.k{i}")) + (job is null ? "" : " AND " + Scope);
        KeyList = string.Join(", ", Keys);
        PartitionText = Partition is null ? "NULL::text" : Partition + "::text";
        TakeoverBackoff = Interval(policy.TakeoverBackoff, Takeovers);
        FailureBackoff = Interval(policy.FailureBackoff, Failures);
        SettlementBackoff = Interval(policy.SettlementBackoff, SettleFailures);
        Generations = Quote(frameworkSchema) + ".reconcile_generations";
        GenerationName = Literal(name);
        GenerationLock = Literal(GenerationLockKey(frameworkSchema, name));
        FenceFunction = Quote(frameworkSchema) + ".generation_fence";
        if (reconcile is { Fields: var r })
        {
            RequestedRevision = "r." + Quote(r.RequestedRevision);
            ClaimedRevision = "r." + Quote(r.ClaimedRevision);
            ConvergedRevision = "r." + Quote(r.ConvergedRevision);
            FirstRequestedAt = "r." + Quote(r.FirstRequestedAt);
            NotBefore = "r." + Quote(r.NotBefore);
            AttemptedGeneration = "r." + Quote(r.AttemptedGeneration);
            ConvergedGeneration = "r." + Quote(r.ConvergedGeneration);
            Generation = $"COALESCE((SELECT g.generation FROM {Generations} AS g WHERE g.source = {GenerationName}), 0)";
            Stale = $"({AttemptedGeneration} < {Generation})";
            // The backoff floor beats an invalidation; a stale generation makes every record due once. On superseding
            // sources a lease of an obsolete generation counts as not held.
            Claimable = $"(({NotBefore} IS NULL OR {NotBefore} <= clock_timestamp()) "
                + $"AND ({Due} <= clock_timestamp() OR {Stale}) "
                + $"AND ({Expires} IS NULL OR {Expires} <= clock_timestamp() - {TakeoverBackoff}{(Supersedes ? $" OR {Stale}" : "")}))";
            Superseded = Supersedes ? $"({Expires} IS NOT NULL AND {Stale})" : "false";
        }
        else if (job is not null)
        {
            // Eligibility on top of the definition scope: an accepted argument version not below the stored minimum,
            // and the subject not paused. Terminal jobs have no due time. Only claims and next-due lookups apply it, so
            // settlement, cancellation and finalization also reach jobs no process may claim. The stored subject pause
            // is the authority; the copied flag keeps paused jobs out of the due index.
            var accepted = job.AcceptedArgs.Where(v => v >= job.Policy.MinimumArgsVersion).Select(v => v.ToString(CultureInfo.InvariantCulture)).ToArray();
            JobPauses = Quote(frameworkSchema) + ".job_pauses";
            SubjectPaused = $"EXISTS (SELECT 1 FROM {JobPauses} AS p WHERE p.\"definition\" = r.\"definition\" AND p.\"subject_kind\" = r.\"subject_kind\" AND p.\"subject_id\" = r.\"subject_id\")";
            Eligible = $"({Scope} AND r.\"args_version\" = ANY('{{{string.Join(",", accepted)}}}'::int4[]) AND NOT r.\"paused\" AND NOT {SubjectPaused})";
            SubjectLock = $"hashtextextended({Literal(SubjectLockPrefix(frameworkSchema, job.Definition))} || r.\"subject_kind\" || chr(10) || r.\"subject_id\", 0)";
            // One scan per accepted version on the index led by definition and version, so due jobs of versions this
            // process does not accept are never read.
            DueRanges = [.. accepted.Select(v => $"r.\"args_version\" = {v}")];
            // A job with a calendar starts only inside the window planned with its due time. A takeover of a lost lease
            // continues an execution that started inside it, as a running attempt continues past the window's end.
            Claimable = $"({Eligible} AND {Due} <= clock_timestamp() AND ({Expires} IS NULL OR {Expires} <= clock_timestamp() - {TakeoverBackoff}) "
                + $"AND (r.\"window_end\" IS NULL OR r.\"window_end\" > clock_timestamp() OR {Expires} IS NOT NULL))";
            Superseded = "false";
            JobDefinitions = Quote(frameworkSchema) + ".job_definitions";
            JobRuns = Quote(frameworkSchema) + ".job_runs";
            DefinitionFence = $"SELECT pg_advisory_xact_lock_shared(hashtextextended({Literal(DefinitionLockKey(frameworkSchema, job.Definition))}, 0))";
            DefinitionRead = $"SELECT revision, paused FROM {JobDefinitions} WHERE key = {Literal(job.Definition)}";
            var grace = job.Policy.Late.Grace is { } g ? $"make_interval(secs => {Seconds(g)})" : null;
            JobLate = grace is null ? "false" : $"(NOT r.\"started\" AND clock_timestamp() > {Due} + {grace})";
            JobExhausted = $"(r.\"started\" AND r.\"attempts\" >= {job.Policy.Attempts.ToString(CultureInfo.InvariantCulture)})";
        }
        else
        {
            Claimable = $"({Due} <= clock_timestamp() AND ({Expires} IS NULL OR {Expires} <= clock_timestamp() - {TakeoverBackoff}))";
            Superseded = "false";
        }
        // A lease superseded before it expired has a live holder; only an expired lease needs its expiry settled.
        NeedsSettlement = !settles ? "false"
            : Supersedes ? $"({Expires} IS NOT NULL AND {Expires} <= clock_timestamp() AND {Unsettled})"
            : $"({Expires} IS NOT NULL AND {Unsettled})";
        ClaimableWithoutSettling = $"({Claimable} AND NOT {NeedsSettlement})";
        SettlementWaiting = policy.SettleExpiry ? $"({SettleRetryAt} IS NOT NULL AND {SettleRetryAt} > clock_timestamp())" : "false";
        ClaimableWithSettling = $"({Claimable} AND NOT ({NeedsSettlement} AND {SettlementWaiting}))";
        OutstandingCondition = (settles
            ? $"({Expires} IS NOT NULL AND {Unsettled}"
            : $"({Expires} IS NOT NULL AND {Expires} > clock_timestamp()") + (job is null ? ")" : $" AND {Scope})");
        ScopePrefix = "corfedo-work|" + store + "|" + name + "|";
        Policy = policy;
    }

    public string Name { get; }

    /// <summary>The definition, revision and accepted argument versions on job sources; null otherwise.</summary>
    public JobSourceSettings? Job { get; }

    /// <summary>The condition every statement of the source adds: the definition on job sources, <c>true</c> otherwise.</summary>
    public string Scope { get; }

    /// <summary>On job sources, the scope plus the argument versions this process may claim.</summary>
    public string Eligible { get; } = "true";

    /// <summary>
    /// Conditions that split scans of unheld due records into one index range each: one per accepted argument version
    /// on job sources, a single unrestricted range otherwise.
    /// </summary>
    public IReadOnlyList<string> DueRanges { get; } = ["true"];

    /// <summary>The framework table of job definitions, on job sources.</summary>
    public string JobDefinitions { get; } = "";

    /// <summary>The framework table of run history, on job sources.</summary>
    public string JobRuns { get; } = "";

    /// <summary>
    /// On job sources, a statement that records the open attempt of each row <paramref name="from"/> and
    /// <paramref name="where"/> select (alias <c>r</c>) in the run history, when the stored definition keeps it: an
    /// attempt is open while <c>attempt_started_at</c> is set, and every statement that ends an attempt clears it, so
    /// each attempt is recorded once. The entry is keyed by the epoch of the claim that used the attempt and carries the
    /// <paramref name="occurrence"/> expression, the row's occurrence unless the statement ends it as another.
    /// </summary>
    public string RecordRun(string from, string where, string outcome, string reason, string ended, string occurrence = "r.\"occurrence\"") =>
        $"INSERT INTO {JobRuns} (job_id, epoch, definition, subject_kind, subject_id, occurrence, execution_id, attempt, owner, started_at, ended_at, outcome, reason) "
        + $"SELECT r.\"id\", r.\"attempt_epoch\", r.\"definition\", r.\"subject_kind\", r.\"subject_id\", {occurrence}, r.\"execution_id\", r.\"attempts\", {Owner}, "
        + $"r.\"attempt_started_at\", {ended}, {outcome}, {reason} FROM {from} JOIN {JobDefinitions} AS d ON d.key = r.\"definition\" AND d.run_history "
        + $"WHERE {where} AND r.\"attempt_started_at\" IS NOT NULL ON CONFLICT DO NOTHING";

    /// <summary>
    /// On job sources, the statement that takes the definition fence in shared mode until the claim transaction ends;
    /// storing a definition takes it exclusively, so a claim commits under the revision stored at its commit. It runs
    /// before any record or limit-scope lock.
    /// </summary>
    public string? DefinitionFence { get; }

    /// <summary>On job sources, reads the stored revision and pause state; run after <see cref="DefinitionFence"/>.</summary>
    public string? DefinitionRead { get; }

    /// <summary>The framework table of subject pauses, on job sources.</summary>
    public string JobPauses { get; } = "";

    /// <summary>On job sources, the record's subject has a stored pause.</summary>
    public string SubjectPaused { get; } = "false";

    /// <summary>On job sources, the advisory lock key of the record's definition and subject, as a bigint expression.</summary>
    public string SubjectLock { get; } = "";

    /// <summary>The prefix of a subject's pause lock key; the subject's kind, a newline and its id follow.</summary>
    public static string SubjectLockPrefix(string frameworkSchema, string definition) => "corfedo-work|subject|" + frameworkSchema + "|" + definition + "|";

    /// <summary>On job sources, the claim finds the job late before its execution started; uses no attempt.</summary>
    public string JobLate { get; } = "false";

    /// <summary>On job sources, the claim finds no attempt left after a lost lease; uses no attempt.</summary>
    public string JobExhausted { get; } = "false";

    /// <summary>The key of a definition's fence lock.</summary>
    public static string DefinitionLockKey(string frameworkSchema, string definition) => "corfedo-work|definition|" + frameworkSchema + "|" + definition;

    /// <summary>
    /// Process-local claim state: whether the next due claim starts with records due only because of an invalidation.
    /// Claims of the source alternate it; a lost update only shifts one turn.
    /// </summary>
    public bool GenerationTurn { get; set; }

    public bool Settles { get; }

    /// <summary>The reconciliation policy and fields, on reconciliation sources.</summary>
    public ReconcileSettings? Reconcile { get; }

    /// <summary>Whether requests and invalidations supersede running attempts.</summary>
    public bool Supersedes { get; }

    /// <summary>The framework table holding the generation of each reconciliation source.</summary>
    public string Generations { get; }

    /// <summary>The source's name as a string literal, the key of its generation row.</summary>
    public string GenerationName { get; }

    /// <summary>The source's current generation; 0 before its first invalidation.</summary>
    public string Generation { get; } = "0";

    public string RequestedRevision { get; } = "";

    public string ClaimedRevision { get; } = "";

    public string ConvergedRevision { get; } = "";

    public string FirstRequestedAt { get; } = "";

    public string NotBefore { get; } = "";

    public string AttemptedGeneration { get; } = "";

    public string ConvergedGeneration { get; } = "";

    /// <summary>The record's latest attempt started from an older generation than the current one.</summary>
    public string Stale { get; } = "false";

    /// <summary>The record has an outstanding lease of an obsolete generation, which a claim retires as a supersession.</summary>
    public string Superseded { get; }

    public int KeyCount { get; }

    public LeasePolicy Policy { get; }

    public string Table { get; }

    public string[] Keys { get; }

    public string? Partition { get; }

    public string Due { get; }

    public string Owner { get; }

    public string Epoch { get; }

    public string Expires { get; }

    public string Takeovers { get; }

    public string Failures { get; }

    public string Settled { get; }

    /// <summary>The condition of a lease not settled yet, on the row alias <c>r</c>.</summary>
    public string Unsettled { get; }

    /// <summary>Settlement failures of the expired epoch; only on sources with <c>SettleExpiry</c>.</summary>
    public string SettleFailures { get; }

    /// <summary>When a failed settlement may be retried; only on sources with <c>SettleExpiry</c>.</summary>
    public string SettleRetryAt { get; }

    public string Cancel { get; }

    public string Execution { get; }

    public string EndedId { get; }

    public string EndedDigest { get; }

    /// <summary>The keys of array parameters $1..$k as rows of <c>u(k0, k1, ...)</c>.</summary>
    public string Unnest { get; }

    public string KeyJoin { get; }

    public string KeyList { get; }

    public string PartitionText { get; }

    /// <summary>Due and not held, by the store's clock: claimability without limits.</summary>
    public string Claimable { get; }

    /// <summary>The previous lease expired and its expiry is not settled yet.</summary>
    public string NeedsSettlement { get; }

    /// <summary>Claimability for a claimer that cannot settle: records whose expiry needs settling are left out.</summary>
    public string ClaimableWithoutSettling { get; }

    /// <summary>A failed settlement waits for its retry time.</summary>
    public string SettlementWaiting { get; }

    /// <summary>Claimability for a claimer that settles: an expiry waiting to retry its settlement is left out.</summary>
    public string ClaimableWithSettling { get; }

    /// <summary>Claimability for a claimer with or without the source's expiry handler.</summary>
    public string ClaimableFor<TKey>(LeaseCallbacks<TKey> callbacks)
        where TKey : notnull => !Policy.SettleExpiry ? Claimable : callbacks.Expired is null ? ClaimableWithoutSettling : ClaimableWithSettling;

    /// <summary>Claimed and not yet released, ended, taken over or settled.</summary>
    public string OutstandingCondition { get; }

    public string ScopePrefix { get; }

    /// <summary>The takeover backoff for the record's takeovers, as an interval expression.</summary>
    public string TakeoverBackoff { get; }

    /// <summary>The failure backoff for the record's failures, as an interval expression.</summary>
    public string FailureBackoff { get; }

    /// <summary>The settlement backoff for the expired lease's settlement failures, as an interval expression.</summary>
    public string SettlementBackoff { get; }

    /// <summary>SET clauses that record a settled expiry: the epoch, and on sources with <c>SettleExpiry</c> a clean retry state.</summary>
    public string MarkSettled(string epoch) => $"{Bare(Settled)} = {epoch}"
        + (Policy.SettleExpiry ? $", {Bare(SettleFailures)} = 0, {Bare(SettleRetryAt)} = NULL" : "");

    /// <summary>
    /// The lease is valid for the owner in <c>$owner</c> and the epoch in <c>$epoch</c> by a clock reading taken now;
    /// on superseding sources it must also be of the current generation.
    /// </summary>
    public string Valid(int owner, int epoch) =>
        $"{Owner} = ${owner} AND {Epoch} = ${epoch} AND {Expires} > clock_timestamp()" + (Supersedes ? $" AND {AttemptedGeneration} = {Generation}" : "");

    /// <summary>
    /// The statement that fences a holder's transaction on a superseding source, or null. It runs before the
    /// statement's row locks and holds the source's generation lock in shared mode until the transaction ends.
    /// Invalidation takes the same lock exclusively, so it waits for open fenced transactions, fenced transactions that
    /// start meanwhile queue behind it, and a READ COMMITTED statement that runs after the lock is granted sees the
    /// generation of every invalidation that committed before. A snapshot taken earlier would not, so the fence
    /// refuses other isolation levels with an error that aborts the transaction.
    /// </summary>
    public string? GenerationFence => Supersedes ? $"SELECT {FenceFunction}(ARRAY[{GenerationLock}], false)" : null;

    /// <summary>The statement that takes the source's generation lock exclusively, for invalidation.</summary>
    public string InvalidationFence => $"SELECT {FenceFunction}(ARRAY[{GenerationLock}], true)";

    /// <summary>The framework function that takes generation locks in their fixed order.</summary>
    public string FenceFunction { get; }

    /// <summary>The key of a source's generation lock; every fence orders locks by it.</summary>
    public static string GenerationLockKey(string frameworkSchema, string source) => "corfedo-work|generation|" + frameworkSchema + "|" + source;

    /// <summary>The key of the source's generation lock, as a string literal.</summary>
    public string GenerationLock { get; }

    /// <summary><c>r.k0 = $first AND ...</c> for one key.</summary>
    public string KeyMatch(int first) => string.Join(" AND ", Keys.Select((k, i) => $"{k} = ${first + i}")) + (Job is null ? "" : " AND " + Scope);

    /// <summary>Column name without the row alias, for SET clauses.</summary>
    public static string Bare(string column) => column[2..];

    public static string Literal(string value) => "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

    public static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    /// <summary>An interval expression for a backoff by the given count column.</summary>
    private static string Interval(Backoff backoff, string count) => backoff.Kind switch
    {
        BackoffKind.None => "interval '0'",
        BackoffKind.Constant => $"make_interval(secs => {Seconds(backoff.Initial)})",
        _ => $"make_interval(secs => LEAST({Seconds(backoff.Initial)} * power(2::float8, LEAST({count}, 62)), {Seconds(backoff.Maximum)}))",
    };

    internal static string Seconds(TimeSpan value) => value.TotalSeconds.ToString("R", CultureInfo.InvariantCulture) + "::float8";

    /// <summary>The SQL expression of a due time given its kind in <c>$kind</c> and its delay in <c>$delay</c>.</summary>
    public string DueExpression(int kind, int delay) =>
        $"CASE ${kind} WHEN {(int)DueKind.Unchanged} THEN {Due} WHEN {(int)DueKind.After} THEN clock_timestamp() + ${delay} "
        + "ELSE NULL END";

    public static string Join(params string[] parts) => string.Join(", ", parts);

    public string Select(params string[] columns)
    {
        var builder = new StringBuilder();
        builder.Append(KeyList);
        foreach (var column in columns)
        {
            builder.Append(", ").Append(column);
        }
        return builder.ToString();
    }
}
