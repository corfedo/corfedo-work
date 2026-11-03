using System.Globalization;
using System.Text;

namespace Corfedo.Work.SqlServer.Leases;

/// <summary>
/// The statements of one lease source, and its process-local claim turn. Column and table names come from the source
/// definition and are quoted; values are named parameters, a single key as <c>@k0</c>, <c>@k1</c>, ... and many keys as
/// a JSON array copied into the table variable <c>@keys</c>. Conditions that read the clock call
/// <c>SYSUTCDATETIME()</c>, which a statement evaluates once when it starts: a condition that depends on a lock is
/// evaluated in a statement after the one that took the lock.
/// </summary>
internal sealed class LeaseSql
{
    public LeaseSql(string name, StoreLocation store, SourceShape shape, string? partition, LeasePolicy policy, LeaseFields fields, bool settles, ReconcileSettings? reconcile, JobSourceSettings? job, string frameworkSchema)
    {
        Name = name;
        Shape = shape;
        Job = job;
        Reconcile = reconcile;
        Supersedes = reconcile is { Supersedes: true };
        Settles = settles;
        KeyCount = shape.Keys.Count;
        Table = Tsql.Quote(store.Schema) + "." + Tsql.Quote(store.Name);
        Keys = shape.Keys.Select(k => "r." + Tsql.Quote(k.Name)).ToArray();
        Partition = partition is null ? null : "r." + Tsql.Quote(partition);
        Due = "r." + Tsql.Quote(fields.DueAt);
        Owner = "r." + Tsql.Quote(fields.LeaseOwner);
        Epoch = "r." + Tsql.Quote(fields.LeaseEpoch);
        Expires = "r." + Tsql.Quote(fields.LeaseExpiresAt);
        Takeovers = "r." + Tsql.Quote(fields.Takeovers);
        Failures = "r." + Tsql.Quote(fields.Failures);
        Settled = "r." + Tsql.Quote(fields.SettledEpoch);
        SettleFailures = "r." + Tsql.Quote(fields.SettleFailures);
        SettleRetryAt = "r." + Tsql.Quote(fields.SettleRetryAt);
        Cancel = "r." + Tsql.Quote(fields.CancelRequestedAt);
        Execution = "r." + Tsql.Quote(fields.ExecutionId);
        EndedId = "r." + Tsql.Quote(fields.EndedCompletionId);
        EndedDigest = "r." + Tsql.Quote(fields.EndedDigest);
        KeyTable = "TABLE (" + string.Join(", ", shape.Keys.Select((k, i) => $"k{i} {k.Declaration} NOT NULL"))
            + ", PRIMARY KEY (" + string.Join(", ", shape.Keys.Select((_, i) => "k" + i)) + "))";
        KeyColumns = string.Join(", ", shape.Keys.Select((_, i) => "k" + i));
        LoadKeys = $"DECLARE @keys {KeyTable};\nINSERT INTO @keys ({KeyColumns}) SELECT {KeyColumns} FROM OPENJSON(@keyset) WITH ("
            + string.Join(", ", shape.Keys.Select((k, i) => $"k{i} {JsonType(k)} '$[{i}]'")) + ");\n";
        // Job sources share the framework's job table: every statement is scoped to the source's definition.
        Scope = ScopeOf("r");
        KeyHint = shape.KeyIndex;
        KeyJoin = string.Join(" AND ", Keys.Select((k, i) => $"{k} = u.k{i}")) + (job is null ? "" : " AND " + Scope);
        KeyList = string.Join(", ", Keys);
        PartitionText = Partition is null ? "CAST(NULL AS nvarchar(4000))" : $"CAST({Partition} AS nvarchar(4000))";
        TakeoverBackoff = Interval(policy.TakeoverBackoff, Takeovers);
        FailureBackoff = Interval(policy.FailureBackoff, Failures);
        SettlementBackoff = Interval(policy.SettlementBackoff, SettleFailures);
        Generations = Tsql.Quote(frameworkSchema) + ".reconcile_generations";
        GenerationName = Tsql.Literal(name);
        GenerationLock = Tsql.Literal(GenerationLockName(frameworkSchema, name));
        if (reconcile is { Fields: var r })
        {
            RequestedRevision = "r." + Tsql.Quote(r.RequestedRevision);
            ClaimedRevision = "r." + Tsql.Quote(r.ClaimedRevision);
            ConvergedRevision = "r." + Tsql.Quote(r.ConvergedRevision);
            FirstRequestedAt = "r." + Tsql.Quote(r.FirstRequestedAt);
            NotBefore = "r." + Tsql.Quote(r.NotBefore);
            AttemptedGeneration = "r." + Tsql.Quote(r.AttemptedGeneration);
            ConvergedGeneration = "r." + Tsql.Quote(r.ConvergedGeneration);
            Generation = $"COALESCE((SELECT g.generation FROM {Generations} AS g WHERE g.source = {GenerationName}), 0)";
            Stale = $"({AttemptedGeneration} < {Generation})";
            // The backoff floor beats an invalidation; a stale generation makes every record due once. On superseding
            // sources a lease of an obsolete generation counts as not held.
            Claimable = $"(({NotBefore} IS NULL OR {NotBefore} <= SYSUTCDATETIME()) "
                + $"AND ({Due} <= SYSUTCDATETIME() OR {Stale}) "
                + $"AND ({Expires} IS NULL OR {Tsql.AddTicks(Expires, TakeoverBackoff)} <= SYSUTCDATETIME(){(Supersedes ? $" OR {Stale}" : "")}))";
            Superseded = Supersedes ? $"({Expires} IS NOT NULL AND {Stale})" : "1 = 0";
        }
        else if (job is not null)
        {
            // Eligibility on top of the definition scope: an accepted argument version not below the stored minimum,
            // and the subject not paused. Terminal jobs have no due time. Only claims and next-due lookups apply it, so
            // settlement, cancellation and finalization also reach jobs no process may claim. The stored subject pause
            // is the authority; the copied flag keeps paused jobs out of the due index.
            var accepted = job.AcceptedArgs.Where(v => v >= job.Policy.MinimumArgsVersion).Select(v => v.ToString(CultureInfo.InvariantCulture)).ToArray();
            JobPauses = Tsql.Quote(frameworkSchema) + ".job_pauses";
            SubjectPaused = $"EXISTS (SELECT 1 FROM {JobPauses} AS p WHERE p.[definition] = r.[definition] AND p.[subject_kind] = r.[subject_kind] AND p.[subject_id] = r.[subject_id])";
            var versions = accepted.Length == 0 ? "1 = 0" : $"r.[args_version] IN ({string.Join(", ", accepted)})";
            Eligible = $"({Scope} AND {versions} AND r.[paused] = 0 AND NOT {SubjectPaused})";
            SubjectLock = Tsql.LockNameExpression("subject", [frameworkSchema, job.Definition], "r.[subject_kind] + NCHAR(10) + r.[subject_id]");
            // One scan per accepted version on the index led by definition and version, so due jobs of versions this
            // process does not accept are never read.
            acceptedVersions = accepted;
            DueRanges = DueRangesOf("r");
            // A job with a calendar starts only inside the window planned with its due time. A takeover of a lost lease
            // continues an execution that started inside it, as a running attempt continues past the window's end.
            Claimable = $"({Eligible} AND {Due} <= SYSUTCDATETIME() AND ({Expires} IS NULL OR {Tsql.AddTicks(Expires, TakeoverBackoff)} <= SYSUTCDATETIME()) "
                + $"AND (r.[window_end] IS NULL OR r.[window_end] > SYSUTCDATETIME() OR {Expires} IS NOT NULL))";
            Superseded = "1 = 0";
            JobDefinitions = Tsql.Quote(frameworkSchema) + ".job_definitions";
            JobRuns = Tsql.Quote(frameworkSchema) + ".job_runs";
            DefinitionFence = AppLock(Tsql.Literal(DefinitionLockName(frameworkSchema, job.Definition)), "Shared");
            DefinitionRead = $"SELECT revision, paused FROM {JobDefinitions} WHERE [key] = {Tsql.Literal(job.Definition)};";
            var grace = job.Policy.Late.Grace is { } g ? g : (TimeSpan?)null;
            JobLate = grace is null ? "1 = 0" : $"(r.[started] = 0 AND SYSUTCDATETIME() > {Tsql.Add(Due, grace.Value)})";
            JobExhausted = $"(r.[started] = 1 AND r.[attempts] >= {job.Policy.Attempts.ToString(CultureInfo.InvariantCulture)})";
        }
        else
        {
            Claimable = $"({Due} <= SYSUTCDATETIME() AND ({Expires} IS NULL OR {Tsql.AddTicks(Expires, TakeoverBackoff)} <= SYSUTCDATETIME()))";
            Superseded = "1 = 0";
        }
        // A lease superseded before it expired has a live holder; only an expired lease needs its expiry settled.
        NeedsSettlement = !settles ? "1 = 0"
            : Supersedes ? $"({Expires} IS NOT NULL AND {Expires} <= SYSUTCDATETIME() AND {Settled} < {Epoch})"
            : $"({Expires} IS NOT NULL AND {Settled} < {Epoch})";
        ClaimableWithoutSettling = $"({Claimable} AND NOT {NeedsSettlement})";
        SettlementWaiting = policy.SettleExpiry ? $"({SettleRetryAt} IS NOT NULL AND {SettleRetryAt} > SYSUTCDATETIME())" : "1 = 0";
        ClaimableWithSettling = $"({Claimable} AND NOT ({NeedsSettlement} AND {SettlementWaiting}))";
        OutstandingCondition = (settles
            ? $"({Expires} IS NOT NULL AND {Settled} < {Epoch}"
            : $"({Expires} IS NOT NULL AND {Expires} > SYSUTCDATETIME()") + (job is null ? ")" : $" AND {Scope})");
        ScopePrefix = frameworkSchema + "\n" + store + "\n" + name + "\n";
        Policy = policy;
    }

    private readonly string[]? acceptedVersions;

    public string Name { get; }

    public SourceShape Shape { get; }

    /// <summary>The definition, revision and accepted argument versions on job sources; null otherwise.</summary>
    public JobSourceSettings? Job { get; }

    /// <summary>The condition every statement of the source adds: the definition on job sources, <c>1 = 1</c> otherwise.</summary>
    public string Scope { get; }

    /// <summary><see cref="Scope"/> on the row alias <paramref name="alias"/>.</summary>
    public string ScopeOf(string alias) => Job is null ? "1 = 1" : $"{alias}.[definition] = {Tsql.Literal(Job.Definition)}";

    /// <summary><see cref="DueRanges"/> on the row alias <paramref name="alias"/>.</summary>
    public IReadOnlyList<string> DueRangesOf(string alias) => acceptedVersions is null ? ["1 = 1"] : [.. acceptedVersions.Select(v => $"{alias}.[args_version] = {v}")];

    /// <summary>The column <paramref name="column"/>, given on the row alias <c>r</c>, on <paramref name="alias"/>.</summary>
    public static string On(string alias, string column) => alias + "." + Bare(column);

    /// <summary>The index hint target of the unique index on the key, through which rows are locked.</summary>
    public string KeyHint { get; }

    /// <summary>The variables <see cref="LockScan"/> uses, declared once per batch.</summary>
    public string ScanVariables => "DECLARE " + string.Join(", ", Shape.Keys.Select((k, i) => $"@s{i} {k.VariableDeclaration}")) + ", @scanned int;\n";

    /// <summary>
    /// Statements that lock up to <paramref name="top"/> rows without waiting, in the order of a scan of
    /// <paramref name="index"/>, and insert <paramref name="selected"/> of each into <paramref name="into"/>, whose
    /// first columns are the key as <c>k0</c>, <c>k1</c>, ... A cursor reads the index (alias <c>x</c>) without locks,
    /// in the order <paramref name="order"/>, the rows matching <paramref name="scan"/>; each row it returns is then
    /// locked through its key (alias <c>r</c>) and kept when its latest committed version matches
    /// <paramref name="locked"/> and it is not in <paramref name="into"/> yet; a row another transaction holds is
    /// skipped. The cursor reads only as far as needed, and only kept rows are locked, whatever plan the scan gets.
    /// Rows are locked through their key before any other index, as every statement that writes them does, so a scan
    /// never holds an index entry while it waits for a row that a writer holds. Needs <see cref="ScanVariables"/>.
    /// </summary>
    public string LockScan(string name, string top, string index, string scan, string locked, string order, string into, string columns, string selected)
    {
        var keys = string.Join(", ", Shape.Keys.Select((_, i) => "@s" + i));
        var match = string.Join(" AND ", Shape.Keys.Select((k, i) => $"r.{Tsql.Quote(k.Name)} = @s{i}"));
        var taken = string.Join(" AND ", Shape.Keys.Select((_, i) => $"t.k{i} = @s{i}"));
        return $"""
            DECLARE {name} CURSOR LOCAL FORWARD_ONLY READ_ONLY FOR
                SELECT {string.Join(", ", Shape.Keys.Select(k => "x." + Tsql.Quote(k.Name)))} FROM {Table} AS x WITH (INDEX({Tsql.Quote(index)})) WHERE {scan} ORDER BY {order};
            OPEN {name};
            SET @scanned = 0;
            WHILE @scanned < {top}
            BEGIN
                FETCH NEXT FROM {name} INTO {keys};
                IF @@FETCH_STATUS <> 0 BREAK;
                INSERT INTO {into} ({columns}) SELECT {selected} FROM {Table} AS r WITH (UPDLOCK, ROWLOCK, READPAST)
                WHERE {match} AND {locked} AND NOT EXISTS (SELECT 1 FROM {into} AS t WHERE {taken});
                SET @scanned += @@ROWCOUNT;
            END
            CLOSE {name};
            DEALLOCATE {name};

            """;
    }

    /// <summary>On job sources, the scope plus the argument versions this process may claim.</summary>
    public string Eligible { get; } = "1 = 1";

    /// <summary>
    /// Conditions that split scans of unheld due records into one index range each: one per accepted argument version
    /// on job sources, a single unrestricted range otherwise.
    /// </summary>
    public IReadOnlyList<string> DueRanges { get; } = ["1 = 1"];

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
    public string RecordRun(string from, string where, string outcome, string reason, string ended, string occurrence = "r.[occurrence]") =>
        $"INSERT INTO {JobRuns} (job_id, epoch, definition, subject_kind, subject_id, occurrence, execution_id, attempt, owner, started_at, ended_at, outcome, reason) "
        + $"SELECT r.[id], r.[attempt_epoch], r.[definition], r.[subject_kind], r.[subject_id], {occurrence}, r.[execution_id], r.[attempts], {Owner}, "
        + $"r.[attempt_started_at], {ended}, {outcome}, {reason} FROM {from} JOIN {JobDefinitions} AS d ON d.[key] = r.[definition] AND d.run_history = 1 "
        + $"WHERE {where} AND r.[attempt_started_at] IS NOT NULL "
        + $"AND NOT EXISTS (SELECT 1 FROM {JobRuns} AS e WHERE e.job_id = r.[id] AND e.epoch = r.[attempt_epoch]);";

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
    public string SubjectPaused { get; } = "1 = 0";

    /// <summary>On job sources, the applock name of the record's definition and subject, as an nvarchar expression.</summary>
    public string SubjectLock { get; } = "";

    /// <summary>The applock name of a subject's pause lock.</summary>
    public static string SubjectLockName(string frameworkSchema, string definition, Subject subject) =>
        Tsql.LockName("subject", frameworkSchema, definition, subject.Kind + "\n" + subject.Id);

    /// <summary>On job sources, the claim finds the job late before its execution started; uses no attempt.</summary>
    public string JobLate { get; } = "1 = 0";

    /// <summary>On job sources, the claim finds no attempt left after a lost lease; uses no attempt.</summary>
    public string JobExhausted { get; } = "1 = 0";

    /// <summary>The applock name of a definition's fence.</summary>
    public static string DefinitionLockName(string frameworkSchema, string definition) => Tsql.LockName("definition", frameworkSchema, definition);

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
    public string Stale { get; } = "1 = 0";

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

    /// <summary>Settlement failures of the expired epoch; only on sources with <c>SettleExpiry</c>.</summary>
    public string SettleFailures { get; }

    /// <summary>When a failed settlement may be retried; only on sources with <c>SettleExpiry</c>.</summary>
    public string SettleRetryAt { get; }

    public string Cancel { get; }

    public string Execution { get; }

    public string EndedId { get; }

    public string EndedDigest { get; }

    /// <summary>The declaration of a table variable of keys, <c>(k0, k1, ...)</c>, with the key columns' types.</summary>
    public string KeyTable { get; }

    /// <summary><c>k0, k1, ...</c></summary>
    public string KeyColumns { get; }

    /// <summary>Declares <c>@keys</c> and fills it from the JSON parameter <c>@keyset</c>.</summary>
    public string LoadKeys { get; }

    /// <summary>
    /// The rows (alias <c>r</c>) of the keys in <c>@keys</c> (alias <c>u</c>), each found by a seek on the key, in the
    /// order of <c>@keys</c>, with the table hints <paramref name="hints"/>. Without the seek a statement over a few
    /// keys may scan the table, and a locking scan waits for every row other transactions hold.
    /// </summary>
    public string KeyedRows(string hints = "") =>
        $"@keys AS u INNER LOOP JOIN {Table} AS r WITH ({(hints.Length == 0 ? "" : hints + ", ")}INDEX({KeyHint}), FORCESEEK) ON {KeyJoin}";

    /// <summary>Joins rows of <c>r</c> to <c>u</c>, a table of keys.</summary>
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

    /// <summary>The parts every limit scope's lock name starts with.</summary>
    public string ScopePrefix { get; }

    /// <summary>The takeover backoff for the record's takeovers, as a bigint tick expression.</summary>
    public string TakeoverBackoff { get; }

    /// <summary>The failure backoff for the record's failures, as a bigint tick expression.</summary>
    public string FailureBackoff { get; }

    /// <summary>The settlement backoff for the expired lease's settlement failures, as a bigint tick expression.</summary>
    public string SettlementBackoff { get; }

    /// <summary>SET clauses that record a settled expiry: the epoch, and on sources with <c>SettleExpiry</c> a clean retry state.</summary>
    public string MarkSettled(string epoch) => $"{Bare(Settled)} = {epoch}"
        + (Policy.SettleExpiry ? $", {Bare(SettleFailures)} = 0, {Bare(SettleRetryAt)} = NULL" : "");

    /// <summary>
    /// The lease is valid for the owner in <c>@owner</c> and the epoch in <c>@epoch</c> by a clock reading taken when the
    /// statement starts; on superseding sources it must also be of the current generation.
    /// </summary>
    public string Valid => $"{Owner} = @owner AND {Epoch} = @epoch AND {Expires} > SYSUTCDATETIME()" + (Supersedes ? $" AND {AttemptedGeneration} = {Generation}" : "");

    /// <summary>
    /// The statements that fence a holder's transaction on a superseding source, or null. They run before the
    /// statement's row locks and hold the source's generation lock in shared mode until the transaction ends.
    /// Invalidation takes the same lock exclusively, so it waits for open fenced transactions, fenced transactions that
    /// start meanwhile queue behind it, and a READ COMMITTED statement that runs after the lock is granted sees the
    /// generation of every invalidation that committed before. A snapshot taken earlier would not, so a transaction at
    /// another isolation level is rolled back before the fence is refused; <paramref name="checkIsolation"/> is false
    /// once the transaction was found at read committed.
    /// </summary>
    public string? GenerationFence(bool checkIsolation) => Supersedes ? (checkIsolation ? IsolationFence : "") + AppLock(GenerationLock, "Shared") : null;

    /// <summary>The statement that takes the source's generation lock exclusively, for invalidation.</summary>
    public string InvalidationFence => AppLock(GenerationLock, "Exclusive");

    /// <summary>The error number of a fence refused for the transaction's isolation level.</summary>
    public const int FenceRefused = 51001;

    /// <summary>
    /// Rolls back and refuses a transaction that is not at read committed: under RCSI that level reads each statement's
    /// own snapshot, which the fences need.
    /// </summary>
    public const string IsolationFence = """
        IF (SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID) <> 2
        BEGIN
            DECLARE @isolation nvarchar(100) = (SELECT CASE transaction_isolation_level WHEN 1 THEN N'READ UNCOMMITTED' WHEN 3 THEN N'REPEATABLE READ'
                WHEN 4 THEN N'SERIALIZABLE' WHEN 5 THEN N'SNAPSHOT' ELSE N'an unspecified level' END FROM sys.dm_exec_sessions WHERE session_id = @@SPID);
            IF @@TRANCOUNT > 0 ROLLBACK;
            DECLARE @refusal nvarchar(400) = N'Holder writes on superseding sources need READ COMMITTED isolation; the transaction uses ' + @isolation + N'.';
            THROW 51001, @refusal, 1;
        END

        """;

    /// <summary>The applock name of a source's generation lock; fences of several sources take them in name order.</summary>
    public static string GenerationLockName(string frameworkSchema, string source) => Tsql.LockName("generation", frameworkSchema, source);

    /// <summary>The applock name of the source's generation lock, as a string literal.</summary>
    public string GenerationLock { get; }

    /// <summary>
    /// A statement that takes the applock <paramref name="resource"/> until the transaction ends, waiting for it; a
    /// refusal, such as a deadlock, ends the batch with an error. <c>@lock_result</c> is declared by the prologue.
    /// </summary>
    public static string AppLock(string resource, string mode) =>
        $"EXEC @lock_result = sp_getapplock @Resource = {resource}, @LockMode = '{mode}', @LockOwner = 'Transaction', @LockTimeout = -1;\n"
        + "IF @lock_result < 0 THROW 51205, N'A lock request of the transaction was refused or chosen as a deadlock victim.', 1;\n";

    /// <summary><c>r.k0 = @k0 AND ...</c> for one key.</summary>
    public string KeyMatch => string.Join(" AND ", Keys.Select((k, i) => $"{k} = @k{i}")) + (Job is null ? "" : " AND " + Scope);

    /// <summary>Column name without the row alias, for SET clauses.</summary>
    public static string Bare(string column) => column[2..];

    /// <summary>A bigint tick expression for a backoff by the given count column.</summary>
    private static string Interval(Backoff backoff, string count) => backoff.Kind switch
    {
        BackoffKind.None => "CAST(0 AS bigint)",
        BackoffKind.Constant => $"CAST({Tsql.Number(backoff.Initial.Ticks)} AS bigint)",
        _ => $"CAST(LEAST({Tsql.Number(backoff.Initial.Ticks)} * POWER(CAST(2 AS float), LEAST({count}, 62)), {Tsql.Number(backoff.Maximum.Ticks)}) AS bigint)",
    };

    /// <summary>The SQL expression of a due time given its kind in <c>@due_kind</c> and its delay in ticks in <c>@due_delay</c>.</summary>
    public string DueExpression =>
        $"CASE @due_kind WHEN {(int)DueKind.Unchanged} THEN {Due} WHEN {(int)DueKind.After} THEN {Tsql.AddTicks("SYSUTCDATETIME()", "@due_delay")} ELSE NULL END";

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

    /// <summary>The OPENJSON type a key column's values are read as before they are stored in the key table.</summary>
    private static string JsonType(KeyColumn key) => key.Type switch
    {
        System.Data.SqlDbType.NVarChar or System.Data.SqlDbType.NChar or System.Data.SqlDbType.VarChar or System.Data.SqlDbType.Char => "nvarchar(max)",
        System.Data.SqlDbType.Int => "int",
        System.Data.SqlDbType.BigInt => "bigint",
        _ => "uniqueidentifier",
    };
}
