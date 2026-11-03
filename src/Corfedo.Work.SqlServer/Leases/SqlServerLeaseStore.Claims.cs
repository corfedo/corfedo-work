using System.Globalization;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace Corfedo.Work.SqlServer.Leases;

internal sealed partial class SqlServerLeaseStore
{
    public async ValueTask<DueClaims<TKey>> ClaimDueAsync<TKey>(LeaseSource<TKey> source, LeaseCallbacks<TKey> callbacks, OwnerId owner, int limit, CancellationToken cancellationToken)
        where TKey : notnull
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var sql = await SqlAsync(source, cancellationToken);
        var claims = new List<LeaseClaim<TKey>>();
        DateTimeOffset cutoff = default;
        // A call attempts at most twice its limit: up to limit records whose claim hook or expiry handler fails do not
        // reduce what it claims, and records that keep failing cannot keep it running.
        var budget = 2 * limit;
        var attempts = 0;
        var busy = new HashSet<string>(StringComparer.Ordinal);
        var skips = 0;
        while (claims.Count < limit && attempts < budget)
        {
            ClaimPass<TKey> result;
            try
            {
                var passLimit = Math.Min(limit - claims.Count, budget - attempts);
                string[] passBusy = [.. busy];
                result = await RetryDeadlocksAsync(() => ClaimDuePassAsync(sql, source, callbacks, owner, passLimit, passBusy, cancellationToken));
            }
            catch (Exception exception) when (claims.Count > 0)
            {
                // Earlier passes committed their claims; the caller must receive them to hold or give them back.
                LogClaimPassFailed(logger, telemetry.Logged(exception), source.Name, claims.Count, telemetry.Describe(exception));
                break;
            }
            cutoff = result.Cutoff;
            if (result.Locked == 0)
            {
                break;
            }
            claims.AddRange(result.Claims);
            attempts += result.Claims.Count + result.Failed;
            // Limits can reject candidates beyond a scope's free slots; the next pass excludes the scopes this pass
            // filled. Failed claims moved their records back, so the next pass reaches records due after them, within
            // the attempt budget. A limit scope another transaction holds is not waited for: its records are skipped
            // and later passes of the call leave its partition out, so a long transaction holding one scope delays
            // neither other partitions nor the dispatcher's other sources. A busy owner scope ends the call, since every
            // claim needs it. Skipped records are claimed by a later call. Every pass that continues claims, fails a
            // record or leaves out a new partition, at most limit times, so the loop ends.
            var refill = result.RejectedByLimits > 0 && result.Claims.Count > 0;
            var retry = result.Failed > 0;
            var skip = !result.OwnerBusy && result.BusyPartitions.Count(busy.Add) > 0 && skips++ < limit;
            if (result.OwnerBusy || (!refill && !retry && !skip))
            {
                break;
            }
        }
        return new DueClaims<TKey>(claims, cutoff);
    }

    /// <summary>
    /// One pass of a due claim in its own transaction, leaving out partitions whose limit scope was found busy. Its
    /// cutoff is the store's clock when its scans started, which bounds them.
    /// </summary>
    private async Task<ClaimPass<TKey>> ClaimDuePassAsync<TKey>(LeaseSql sql, LeaseSource<TKey> source, LeaseCallbacks<TKey> callbacks, OwnerId owner, int limit, string[] busy, CancellationToken cancellationToken)
        where TKey : notnull
    {
        await using var connection = await OpenAsync(cancellationToken);
        await using var transaction = await BeginAsync(connection, cancellationToken);
        var work = new SqlServerTransaction(transaction);
        if (!await FenceDefinitionAsync(work, sql, cancellationToken))
        {
            await using var now = Tsql.Command(work, "SELECT SYSUTCDATETIME();");
            await using var reader = await now.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            return new([], 0, 0) { Cutoff = Tsql.Time(reader, 0) };
        }
        var (locked, cutoff) = await LockDueAsync(work, sql, source, sql.ClaimableFor(callbacks), owner, limit, busy, cancellationToken);
        if (locked.Count == 0)
        {
            return new([], 0, 0) { Cutoff = cutoff };
        }
        var result = await ClaimLockedAsync(work, sql, source, callbacks, owner, locked, ClaimReason.Due, limit, rethrow: false, waitForScopes: false, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return result with { Locked = locked.Count, Cutoff = cutoff };
    }

    /// <summary>
    /// Claims one record in its own transaction. When its limit scope is busy, the attempt rolls back and the next one
    /// waits for the scope before it locks the record.
    /// </summary>
    public async ValueTask<LeaseClaim<TKey>?> ClaimAsync<TKey>(LeaseSource<TKey> source, LeaseCallbacks<TKey> callbacks, OwnerId owner, TKey key, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var sql = await SqlAsync(source, cancellationToken);
        string? waitFor = null;
        while (true)
        {
            var (done, claim, busy) = await RetryDeadlocksAsync(async () =>
            {
                await using var connection = await OpenAsync(cancellationToken);
                await using var transaction = await BeginAsync(connection, cancellationToken);
                var work = new SqlServerTransaction(transaction);
                if (!await FenceDefinitionAsync(work, sql, cancellationToken))
                {
                    return (true, (LeaseClaim<TKey>?)null, (string?)null);
                }
                if (waitFor is not null)
                {
                    await WaitForScopeAsync(work, waitFor, cancellationToken);
                }
                var locked = await LockKeysAsync(work, sql, source, [key], callerTransaction: false, cancellationToken);
                if (locked.Count == 0)
                {
                    return (true, null, null);
                }
                var result = await ClaimLockedAsync(work, sql, source, callbacks, owner, locked, ClaimReason.ByKey, 1, rethrow: true, waitForScopes: false, cancellationToken);
                if (result.Busy is not null)
                {
                    return (false, null, result.Busy);
                }
                await transaction.CommitAsync(cancellationToken);
                return (true, result.Claims.Count == 0 ? null : result.Claims[0], null);
            });
            if (done)
            {
                return claim;
            }
            waitFor = busy;
        }
    }

    public async ValueTask<IReadOnlyList<LeaseClaim<TKey>>> ClaimAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, LeaseCallbacks<TKey> callbacks, OwnerId owner, IReadOnlyList<TKey> candidates, int limit, CancellationToken cancellationToken)
        where TKey : notnull
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var work = SqlServerTransaction.From(transaction);
        if (candidates.Count == 0)
        {
            return [];
        }
        var sql = await SqlAsync(source, cancellationToken);
        await RequireReadCommittedAsync(work, "Claims in the caller's transaction", cancellationToken);
        if (!await FenceDefinitionAsync(work, sql, cancellationToken))
        {
            return [];
        }
        var locked = await LockKeysAsync(work, sql, source, candidates, callerTransaction: true, cancellationToken);
        if (locked.Count == 0)
        {
            return [];
        }
        var result = await ClaimLockedAsync(work, sql, source, callbacks, owner, locked, ClaimReason.ByKey, limit, rethrow: false, waitForScopes: true, cancellationToken);
        return result.Claims;
    }

    /// <summary>
    /// Refuses a caller's transaction that is not at read committed, before any lock or write: claims count the
    /// outstanding leases of other records, and a snapshot taken before a scope lock would hide claims committed
    /// meanwhile. The level is read once per transaction and recorded on its handle.
    /// </summary>
    /// <exception cref="InvalidOperationException">The transaction uses another isolation level.</exception>
    internal static async Task RequireReadCommittedAsync(SqlServerTransaction work, string operation, CancellationToken cancellationToken)
    {
        if (work.Work.ReadCommitted)
        {
            return;
        }
        await using var command = Tsql.Command(work,
            "SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID;");
        var level = Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        if (level != 2)
        {
            var name = level switch
            {
                1 => "READ UNCOMMITTED",
                3 => "REPEATABLE READ",
                4 => "SERIALIZABLE",
                5 => "SNAPSHOT",
                _ => "an unspecified level",
            };
            throw new InvalidOperationException($"{operation} need READ COMMITTED isolation; the transaction uses {name}.");
        }
        work.Work.ReadCommitted = true;
    }

    /// <summary>
    /// On job sources, takes the definition fence in shared mode before any record or scope lock and reads the stored
    /// definition after it. False when the definition is paused, so nothing is claimed.
    /// </summary>
    /// <exception cref="JobDefinitionChangedException">The stored revision is not the one the statements were built from.</exception>
    private static async Task<bool> FenceDefinitionAsync(SqlServerTransaction work, LeaseSql sql, CancellationToken cancellationToken)
    {
        if (sql.Job is not { } job)
        {
            return true;
        }
        await using var command = Tsql.Command(work, sql.DefinitionFence + sql.DefinitionRead);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new JobDefinitionChangedException(job.Definition, 0);
        }
        var revision = reader.GetInt64(0);
        if (revision != job.Revision)
        {
            throw new JobDefinitionChangedException(job.Definition, revision);
        }
        return !reader.GetBoolean(1);
    }

    /// <summary>
    /// Locks claimable candidates without waiting, excluding partitions already saturated or in <paramref name="busy"/>
    /// and taking no more than the owner's free slots. Returns them with the store's clock when the scans started.
    /// </summary>
    private static async Task<(List<TKey> Keys, DateTimeOffset Cutoff)> LockDueAsync<TKey>(SqlServerTransaction work, LeaseSql sql, LeaseSource<TKey> source, string claimable, OwnerId owner, int limit, string[] busy, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var policy = sql.Policy;
        var text = new StringBuilder();
        text.Append("DECLARE @now datetime2(7) = SYSUTCDATETIME();\n");
        text.Append($"DECLARE @c {sql.KeyTable};\n");
        if (busy.Length > 0)
        {
            text.Append($"DECLARE @busy TABLE (p {sql.Shape.PartitionDeclaration} NOT NULL PRIMARY KEY);\n");
            text.Append("INSERT INTO @busy (p) SELECT DISTINCT value FROM OPENJSON(@busyset);\n");
            claimable = $"({claimable} AND NOT EXISTS (SELECT 1 FROM @busy AS b WHERE b.p = {sql.Partition}))";
        }
        var saturated = new List<string>();
        var partitionHint = sql.Shape.PartitionIndex is { } p ? $" WITH (INDEX({Tsql.Quote(p)}))" : "";
        if (policy.PartitionLimit is { } partitionLimit)
        {
            saturated.Add($"SELECT {sql.Partition} FROM {sql.Table} AS r{partitionHint} WHERE {sql.OutstandingCondition} GROUP BY {sql.Partition} HAVING COUNT(*) >= {partitionLimit}");
        }
        if (policy.SpreadLimit is { } spreadLimit)
        {
            saturated.Add($"SELECT {sql.Partition} FROM {sql.Table} AS r{partitionHint} WHERE {sql.OutstandingCondition} AND {sql.Owner} = @owner GROUP BY {sql.Partition} HAVING COUNT(*) >= {spreadLimit}");
        }
        if (saturated.Count > 0)
        {
            // Saturated partitions are counted once per statement from the partition index over held leases, and a
            // record with an outstanding lease of its own stays in: claiming it replaces that lease, so it adds nothing
            // to its partition. Partition fields of limited sources are never null.
            text.Append($"DECLARE @saturated TABLE (p {sql.Shape.PartitionDeclaration} NOT NULL PRIMARY KEY);\n");
            text.Append($"INSERT INTO @saturated (p) {string.Join(" UNION ", saturated)};\n");
            claimable = $"({claimable} AND (NOT EXISTS (SELECT 1 FROM @saturated AS s WHERE s.p = {sql.Partition}) OR {sql.OutstandingCondition}))";
        }
        var count = policy.OwnerLimit is { } ownerLimit
            ? $"LEAST(@limit, GREATEST({ownerLimit} - (SELECT COUNT(*) FROM {sql.Table} AS r WHERE {sql.OutstandingCondition} AND {sql.Owner} = @owner), 0))"
            : "@limit";
        if (policy.SourceLimit is { } sourceLimit)
        {
            // A saturated source takes nothing; expired leases that hold its slots are returned by settlement.
            count = $"LEAST({count}, GREATEST({sourceLimit} - (SELECT COUNT(*) FROM {sql.Table} AS r WITH (INDEX({Tsql.Quote(sql.Shape.ExpiryIndex)})) WHERE {sql.OutstandingCondition}), 0))";
        }
        text.Append($"DECLARE @n int = {count};\n");
        // Claims do not clear DueAt, so held records keep their due times. Unheld records come from the index on DueAt
        // that leaves held records out, on job sources one range per accepted argument version, expired leases from the
        // index on LeaseExpiresAt in expiry order; each scan is bounded by @now and read in its index's order, so it
        // locks only the rows it reads, and every candidate is re-evaluated with a later clock reading once locked. Up
        // to the limit is locked from each; the claim keeps the earliest due within the limit. READPAST skips rows other
        // transactions hold. On reconciliation sources, records of an older generation than the current one come from
        // the index on AttemptedGeneration, in index order; claims interleave them with records due by time. The scans
        // overlap (a record can be due by time and of an older generation), so each record is kept once.
        void Scan(string index, string range, string order)
        {
            text.Append($"INSERT INTO @c ({sql.KeyColumns}) SELECT TOP (@n) {sql.KeyList} FROM {sql.Table} AS r WITH (UPDLOCK, ROWLOCK, READPAST, INDEX({Tsql.Quote(index)})) "
                + $"WHERE {range} AND {claimable} AND NOT EXISTS (SELECT 1 FROM @c AS u WHERE {sql.KeyJoin}) ORDER BY {order};\n");
        }
        var unheld = sql.Job is null ? $"{sql.Expires} IS NULL" : $"{sql.Expires} IS NULL AND {sql.Due} IS NOT NULL AND r.[paused] = 0";
        foreach (var range in sql.DueRanges)
        {
            Scan(sql.Shape.DueIndex, $"{range} AND {sql.Scope} AND {unheld} AND {sql.Due} <= @now", sql.Due);
        }
        Scan(sql.Shape.ExpiryIndex, $"{sql.Scope} AND {sql.Expires} IS NOT NULL AND {sql.Expires} <= @now", sql.Expires);
        if (sql.Reconcile is not null)
        {
            Scan(sql.Shape.GenerationIndex!, $"{sql.AttemptedGeneration} < {sql.Generation}", sql.AttemptedGeneration);
        }
        if (policy.OwnerLimit is not null)
        {
            // The owner's own claimable outstanding leases, from the owner index: replacing one takes no free slot, so
            // they are candidates even when the owner has none. Saturated partitions keep them for the same reason.
            text.Append($"INSERT INTO @c ({sql.KeyColumns}) SELECT TOP (@limit) {sql.KeyList} FROM {sql.Table} AS r WITH (UPDLOCK, ROWLOCK, READPAST, INDEX({Tsql.Quote(sql.Shape.OwnerIndex!)})) "
                + $"WHERE {sql.Owner} = @owner AND {sql.OutstandingCondition} AND {claimable} AND NOT EXISTS (SELECT 1 FROM @c AS u WHERE {sql.KeyJoin}) ORDER BY {sql.Due};\n");
        }
        text.Append("SELECT @now;\n");
        text.Append($"SELECT {sql.KeyColumns} FROM @c;\n");
        var parameters = new List<SqlParameter> { Tsql.Guid("@owner", owner.Value), Tsql.Int32("@limit", limit) };
        if (busy.Length > 0)
        {
            parameters.Add(Tsql.Json("@busyset", JsonSerializer.Serialize(busy, SqlServerJson.Default.StringArray)));
        }
        await using var command = Tsql.Command(work, text.ToString(), [.. parameters]);
        var keys = new List<TKey>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        var cutoff = Tsql.Time(reader, 0);
        await reader.NextResultAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            keys.Add(ReadKey(source, reader));
        }
        return (keys, cutoff);
    }

    /// <summary>Locks the given records, each once, that no other transaction has locked, without waiting.</summary>
    private static async Task<List<TKey>> LockKeysAsync<TKey>(SqlServerTransaction work, LeaseSql sql, LeaseSource<TKey> source, IReadOnlyCollection<TKey> keys, bool callerTransaction, CancellationToken cancellationToken)
        where TKey : notnull
    {
        // Rows are locked in key order, the order every multi-row lock of the source takes.
        await using var command = KeysCommand(work.Connection, work.Transaction, sql, source, keys,
            $"SELECT {sql.KeyList} FROM {sql.Table} AS r WITH (UPDLOCK, ROWLOCK, READPAST) JOIN @keys AS u ON {sql.KeyJoin} ORDER BY {sql.KeyList};");
        var locked = new List<TKey>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            locked.Add(ReadKey(source, reader));
        }
        return locked;
    }

    private sealed record Candidate<TKey>(
        TKey Key,
        string? Partition,
        OwnerId? Owner,
        long Epoch,
        bool HadLease,
        bool Claimable,
        bool Outstanding,
        bool NeedsSettlement,
        int Takeovers,
        bool Superseded,
        bool DueByGeneration,
        long Generation)
        where TKey : notnull;

    /// <summary>
    /// The outcome of claiming locked records. <c>Busy</c> is a limit scope another transaction held; with it,
    /// <c>OwnerBusy</c> says whether that included the owner scope, and <c>BusyPartitions</c> lists the partitions
    /// whose scope was held.
    /// </summary>
    private sealed record ClaimPass<TKey>(List<LeaseClaim<TKey>> Claims, int RejectedByLimits, int Failed, string? Busy = null, int Locked = 0)
        where TKey : notnull
    {
        /// <summary>The cutoff of a due claim pass; unset for claims by key.</summary>
        public DateTimeOffset Cutoff { get; init; }

        public bool OwnerBusy { get; init; }

        public IReadOnlyList<string> BusyPartitions { get; init; } = [];
    }

    /// <summary>
    /// Claims locked records: serializes the limit scopes they touch, re-evaluates claimability with a clock reading
    /// taken after every lock is held, counts outstanding leases and claims the records that fit. A record whose
    /// expired lease needs an expiry handler this caller does not have is left to the expiry processor.
    /// <para>
    /// In the caller's transaction limit scopes are waited for. A transaction of the framework holds record locks here,
    /// so it only tries the scopes: waiting would let it deadlock with an application transaction that holds a scope
    /// and then writes one of those records. Records of a scope another transaction holds are skipped and the scope is
    /// reported as <c>Busy</c>.
    /// </para>
    /// </summary>
    private async Task<ClaimPass<TKey>> ClaimLockedAsync<TKey>(
        SqlServerTransaction work,
        LeaseSql sql,
        LeaseSource<TKey> source,
        LeaseCallbacks<TKey> callbacks,
        OwnerId owner,
        List<TKey> locked,
        ClaimReason reason,
        int limit,
        bool rethrow,
        bool waitForScopes,
        CancellationToken cancellationToken)
        where TKey : notnull
    {
        var policy = sql.Policy;
        var claimable = sql.ClaimableFor(callbacks);
        if (sql.Job is not null)
        {
            locked = await LockSubjectsAsync(work, sql, source, locked, cancellationToken);
        }
        var candidates = Interleave(sql, await EvaluateAsync(work, sql, source, locked, claimable, cancellationToken));
        var accepted = new List<Candidate<TKey>>();
        var rejectedByLimits = 0;
        string? busy = null;
        var ownerBusy = false;
        IReadOnlyList<string> busyPartitions = [];
        ClaimPass<TKey> Pass(List<LeaseClaim<TKey>> claimed, int failed) =>
            new(claimed, rejectedByLimits, failed, busy) { OwnerBusy = ownerBusy, BusyPartitions = busyPartitions };
        if (policy.HasStoreLimits)
        {
            var ownerScope = policy.OwnerLimit is null ? null : ScopeName(sql, "o|" + owner);
            var sourceScope = policy.SourceLimit is null ? null : ScopeName(sql, "s");
            var partitioned = policy.PartitionLimit is not null || policy.SpreadLimit is not null;
            string PartitionScope(Candidate<TKey> candidate) => ScopeName(sql, "p|" + candidate.Partition);
            var scopes = new HashSet<string>(StringComparer.Ordinal);
            if (ownerScope is not null)
            {
                scopes.Add(ownerScope);
            }
            if (sourceScope is not null)
            {
                scopes.Add(sourceScope);
            }
            if (partitioned)
            {
                foreach (var candidate in candidates)
                {
                    scopes.Add(PartitionScope(candidate));
                }
            }
            var busyScopes = await LockScopesAsync(work, scopes, waitForScopes, cancellationToken);
            busy = busyScopes.Count == 0 ? null : busyScopes[0];
            // Every claim of the source needs the owner and source scopes, so either busy ends a due claim.
            ownerBusy = (ownerScope is not null && busyScopes.Contains(ownerScope)) || (sourceScope is not null && busyScopes.Contains(sourceScope));
            busyPartitions = partitioned
                ? candidates.Where(c => busyScopes.Contains(PartitionScope(c))).Select(c => c.Partition!).Distinct(StringComparer.Ordinal).ToList()
                : [];
            // The evaluation before the scope locks may predate a lock wait; evaluate again now that all locks are held.
            candidates = Interleave(sql, await EvaluateAsync(work, sql, source, locked, claimable, cancellationToken));
            var counts = await CountOutstandingAsync(work, sql, owner, candidates.Select(c => c.Partition!).Distinct().ToArray(), cancellationToken);
            foreach (var candidate in candidates)
            {
                if (!candidate.Claimable || accepted.Count == limit)
                {
                    continue;
                }
                if (busyScopes.Count > 0 && (ownerBusy || (partitioned && busyScopes.Contains(PartitionScope(candidate)))))
                {
                    continue;
                }
                var addsTotal = candidate.Outstanding ? 0 : 1;
                var addsMine = candidate.Outstanding && candidate.Owner == owner ? 0 : 1;
                var partition = candidate.Partition is null ? default : counts.Partitions.GetValueOrDefault(candidate.Partition);
                if ((policy.PartitionLimit is { } p && partition.Total + addsTotal > p)
                    || (policy.SpreadLimit is { } s && partition.Mine + addsMine > s)
                    || (policy.OwnerLimit is { } o && counts.Owner + addsMine > o)
                    || (policy.SourceLimit is { } d && counts.Source + addsTotal > d))
                {
                    rejectedByLimits++;
                    continue;
                }
                if (candidate.Partition is not null)
                {
                    counts.Partitions[candidate.Partition] = (partition.Total + addsTotal, partition.Mine + addsMine);
                }
                counts.Owner += addsMine;
                counts.Source += addsTotal;
                accepted.Add(candidate);
            }
        }
        else
        {
            accepted.AddRange(candidates.Where(c => c.Claimable).Take(limit));
        }

        var claims = new List<LeaseClaim<TKey>>(accepted.Count);
        if (accepted.Count == 0)
        {
            return Pass(claims, 0);
        }
        var perRecord = callbacks.Claimed is not null || (callbacks.Expired is not null && accepted.Any(c => c.NeedsSettlement));
        if (!perRecord)
        {
            claims.AddRange(await UpdateClaimsAsync(work, sql, source, owner, accepted, reason, cancellationToken));
            TakeTurn(sql, reason, accepted[^1]);
            return Pass(claims, 0);
        }
        // Failures are accounted by their cause. A failing claim hook is a failed attempt of the record: it counts in
        // Failures and moves the record by the failure backoff (a claim by key reports it to the caller instead). A
        // failing expiry handler is a failed settlement of the previous lease, not an attempt: it counts in
        // SettleFailures and waits for the settlement backoff, leaving the record unclaimed. Errors of the provider's
        // own statements are not accounted; they abort the claim.
        var failed = 0;
        foreach (var candidate in accepted)
        {
            var savepoint = work.NextSavepoint();
            await work.Transaction.SaveAsync(savepoint, cancellationToken);
            if (candidate.NeedsSettlement && callbacks.Expired is { } expired)
            {
                try
                {
                    await expired(new LeaseExpiry<TKey>(source, candidate.Key, candidate.Owner!.Value, candidate.Epoch, candidate.Partition), work.Work, cancellationToken);
                }
                catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
                {
                    await work.Transaction.RollbackAsync(savepoint, CancellationToken.None);
                    await RecordSettlementFailureAsync(work.Connection, work.Transaction, sql, source, candidate.Key, cancellationToken);
                    failed++;
                    LogClaimSettlementFailed(logger, telemetry.Logged(exception), source.Name, source.Key.Format(candidate.Key), telemetry.Describe(exception));
                    continue;
                }
            }
            var claim = (await UpdateClaimsAsync(work, sql, source, owner, [candidate], reason, cancellationToken))[0];
            if (callbacks.Claimed is { } claimed)
            {
                try
                {
                    await claimed(claim, work.Work, cancellationToken);
                }
                catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
                {
                    await work.Transaction.RollbackAsync(savepoint, CancellationToken.None);
                    if (rethrow)
                    {
                        throw;
                    }
                    await CountClaimFailureAsync(work, sql, source, candidate.Key, cancellationToken);
                    failed++;
                    LogClaimRolledBack(logger, telemetry.Logged(exception), source.Name, source.Key.Format(candidate.Key), telemetry.Describe(exception));
                    continue;
                }
            }
            claims.Add(claim);
            TakeTurn(sql, reason, candidate);
        }
        return Pass(claims, failed);
    }

    /// <summary>The applock name of one of the source's limit scopes.</summary>
    private static string ScopeName(LeaseSql sql, string scope) => Tsql.LockName("scope", sql.ScopePrefix + scope);

    /// <summary>
    /// On job sources, tries the pause lock of each locked record's subject in shared mode and leaves out records whose
    /// subject another transaction is pausing or resuming. Pausing takes the lock exclusively and holds it until it
    /// commits, so the evaluation that follows, a later statement at read committed, sees every pause committed before
    /// the lock was granted, and a pause waits for the claims that hold it. Locks are only tried: the claim holds record
    /// locks, which a pausing transaction may be waiting for. A pass takes at most one lock per locked record.
    /// </summary>
    private static async Task<List<TKey>> LockSubjectsAsync<TKey>(SqlServerTransaction work, LeaseSql sql, LeaseSource<TKey> source, List<TKey> locked, CancellationToken cancellationToken)
        where TKey : notnull
    {
        if (locked.Count == 0)
        {
            return locked;
        }
        await using var command = KeysCommand(work.Connection, work.Transaction, sql, source, locked, $"""
            DECLARE @subjects TABLE (i int IDENTITY(0, 1) PRIMARY KEY, name nvarchar(255) COLLATE {Tsql.Binary} NOT NULL);
            DECLARE @busy TABLE (name nvarchar(255) COLLATE {Tsql.Binary} NOT NULL PRIMARY KEY);
            INSERT INTO @subjects (name) SELECT s.name FROM (SELECT DISTINCT {sql.SubjectLock} AS name FROM {sql.Table} AS r JOIN @keys AS u ON {sql.KeyJoin}
                WHERE r.[subject_kind] IS NOT NULL) AS s ORDER BY s.name;
            DECLARE @i int = 0, @count int = (SELECT COUNT(*) FROM @subjects), @name nvarchar(255);
            WHILE @i < @count
            BEGIN
                SELECT @name = name FROM @subjects WHERE i = @i;
                EXEC @lock_result = sp_getapplock @Resource = @name, @LockMode = 'Shared', @LockOwner = 'Transaction', @LockTimeout = 0;
                IF @lock_result < 0 INSERT INTO @busy (name) VALUES (@name);
                SET @i += 1;
            END
            SELECT {sql.KeyList} FROM {sql.Table} AS r JOIN @keys AS u ON {sql.KeyJoin} WHERE r.[subject_kind] IS NOT NULL AND {sql.SubjectLock} IN (SELECT name FROM @busy);
            """);
        var busy = new HashSet<string>(StringComparer.Ordinal);
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                busy.Add(source.Key.Format(ReadKey(source, reader)));
            }
        }
        return busy.Count == 0 ? locked : [.. locked.Where(k => !busy.Contains(source.Key.Format(k)))];
    }

    /// <summary>Counts a failed claim of a still locked record and makes it due again after the failure backoff.</summary>
    private static async Task CountClaimFailureAsync<TKey>(SqlServerTransaction work, LeaseSql sql, LeaseSource<TKey> source, TKey key, CancellationToken cancellationToken)
        where TKey : notnull
    {
        // On reconciliation sources the backoff floor is set too, since the record may be due by its generation.
        var retry = Tsql.AddTicks("SYSUTCDATETIME()", sql.FailureBackoff);
        var set = sql.Reconcile is null
            ? $"{LeaseSql.Bare(sql.Due)} = {retry}"
            : $"{LeaseSql.Bare(sql.Due)} = {retry}, {LeaseSql.Bare(sql.NotBefore)} = {retry}";
        await using var command = KeyCommand(work.Connection, work.Transaction, sql, source, key,
            $"UPDATE r SET {LeaseSql.Bare(sql.Failures)} = {sql.Failures} + 1, {set} FROM {sql.Table} AS r WHERE {sql.KeyMatch};");
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<List<Candidate<TKey>>> EvaluateAsync<TKey>(SqlServerTransaction work, LeaseSql sql, LeaseSource<TKey> source, List<TKey> keys, string claimable, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var dueByGeneration = sql.Reconcile is null ? "1 = 0" : $"({sql.Due} IS NULL OR {sql.Due} > SYSUTCDATETIME())";
        var text = $"SELECT {sql.Select(sql.PartitionText, sql.Owner, sql.Epoch, Tsql.Bit($"{sql.Expires} IS NOT NULL"), Tsql.Bit(claimable), Tsql.Bit(sql.OutstandingCondition), Tsql.Bit(sql.NeedsSettlement), sql.Takeovers, Tsql.Bit(sql.Superseded), Tsql.Bit(dueByGeneration), $"CAST({sql.Generation} AS bigint)")} "
            + $"FROM {sql.Table} AS r JOIN @keys AS u ON {sql.KeyJoin} ORDER BY CASE WHEN {sql.Due} IS NULL THEN 1 ELSE 0 END, {sql.Due}, {sql.KeyList};";
        await using var command = KeysCommand(work.Connection, work.Transaction, sql, source, keys, text);
        var candidates = new List<Candidate<TKey>>(keys.Count);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var k = sql.KeyCount;
        while (await reader.ReadAsync(cancellationToken))
        {
            candidates.Add(new Candidate<TKey>(
                ReadKey(source, reader),
                reader.IsDBNull(k) ? null : reader.GetString(k),
                reader.IsDBNull(k + 1) ? null : new OwnerId(reader.GetGuid(k + 1)),
                reader.GetInt64(k + 2),
                reader.GetBoolean(k + 3),
                reader.GetBoolean(k + 4),
                reader.GetBoolean(k + 5),
                reader.GetBoolean(k + 6),
                reader.GetInt32(k + 7),
                reader.GetBoolean(k + 8),
                reader.GetBoolean(k + 9),
                reader.GetInt64(k + 10)));
        }
        return candidates;
    }

    /// <summary>
    /// Alternates records due by time with records due only because of an invalidation, each in their own order, so
    /// that neither a large invalidation nor a stream of requests keeps the other waiting. The alternation continues
    /// across due claims of the source in this process, so claims of one record at a time serve both kinds.
    /// </summary>
    private static List<Candidate<TKey>> Interleave<TKey>(LeaseSql sql, List<Candidate<TKey>> candidates)
        where TKey : notnull
    {
        var byGeneration = candidates.Where(c => c.DueByGeneration).ToList();
        if (byGeneration.Count == 0 || byGeneration.Count == candidates.Count)
        {
            return candidates;
        }
        var byTime = candidates.Where(c => !c.DueByGeneration).ToList();
        var (first, second) = sql.GenerationTurn ? (byGeneration, byTime) : (byTime, byGeneration);
        var merged = new List<Candidate<TKey>>(candidates.Count);
        for (var i = 0; i < Math.Max(first.Count, second.Count); i++)
        {
            if (i < first.Count)
            {
                merged.Add(first[i]);
            }
            if (i < second.Count)
            {
                merged.Add(second[i]);
            }
        }
        return merged;
    }

    /// <summary>After a due claim, the next one starts with the other kind of record than the last one claimed.</summary>
    private static void TakeTurn<TKey>(LeaseSql sql, ClaimReason reason, Candidate<TKey> claimed)
        where TKey : notnull
    {
        if (reason == ClaimReason.Due && sql.Reconcile is not null)
        {
            sql.GenerationTurn = !claimed.DueByGeneration;
        }
    }

    /// <summary>
    /// Takes the applocks of limit scopes in the order of their names, so that claims touching overlapping scopes
    /// cannot deadlock even when two names share a lock. Without <paramref name="wait"/> each lock is only tried, and the
    /// scopes another transaction holds are returned in name order.
    /// </summary>
    internal static async Task<List<string>> LockScopesAsync(SqlServerTransaction work, IEnumerable<string> scopes, bool wait, CancellationToken cancellationToken)
    {
        string[] ordered = [.. scopes.Order(StringComparer.Ordinal)];
        await using var command = Tsql.Command(work, $"""
            DECLARE @scopes TABLE (i int NOT NULL PRIMARY KEY, name nvarchar(255) NOT NULL);
            INSERT INTO @scopes (i, name) SELECT CAST([key] AS int), value FROM OPENJSON(@names);
            DECLARE @busy TABLE (i int NOT NULL PRIMARY KEY, name nvarchar(255) NOT NULL);
            DECLARE @i int = 0, @count int = (SELECT COUNT(*) FROM @scopes), @name nvarchar(255);
            WHILE @i < @count
            BEGIN
                SELECT @name = name FROM @scopes WHERE i = @i;
                EXEC @lock_result = sp_getapplock @Resource = @name, @LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = @timeout;
                IF @lock_result < 0
                BEGIN
                    IF @timeout <> 0 THROW 51205, N'A lock request of the transaction was refused or chosen as a deadlock victim.', 1;
                    INSERT INTO @busy (i, name) VALUES (@i, @name);
                END
                SET @i += 1;
            END
            SELECT name FROM @busy ORDER BY i;
            """,
            Tsql.Json("@names", JsonSerializer.Serialize(ordered, SqlServerJson.Default.StringArray)),
            Tsql.Int32("@timeout", wait ? -1 : 0));
        var busy = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            busy.Add(reader.GetString(0));
        }
        return busy;
    }

    /// <summary>Waits for one limit scope; called while the transaction holds no other lock.</summary>
    private static async Task WaitForScopeAsync(SqlServerTransaction work, string scope, CancellationToken cancellationToken)
    {
        await using var command = Tsql.Command(work, LeaseSql.AppLock("@scope", "Exclusive"), Tsql.Text("@scope", scope, 255));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private sealed class OutstandingCounts
    {
        public Dictionary<string, (int Total, int Mine)> Partitions { get; } = new(StringComparer.Ordinal);

        public int Owner { get; set; }

        public int Source { get; set; }
    }

    private static async Task<OutstandingCounts> CountOutstandingAsync(SqlServerTransaction work, LeaseSql sql, OwnerId owner, string[] partitions, CancellationToken cancellationToken)
    {
        var counts = new OutstandingCounts();
        var text = new StringBuilder();
        var parameters = new List<SqlParameter> { Tsql.Guid("@owner", owner.Value) };
        if (sql.Partition is not null)
        {
            var hint = sql.Shape.PartitionIndex is { } index ? $" WITH (INDEX({Tsql.Quote(index)}))" : "";
            text.Append($"DECLARE @partitions TABLE (p {sql.Shape.PartitionDeclaration} NOT NULL PRIMARY KEY);\n");
            text.Append("INSERT INTO @partitions (p) SELECT DISTINCT value FROM OPENJSON(@partitionset) WHERE value IS NOT NULL;\n");
            text.Append($"SELECT {sql.PartitionText}, COUNT(*), SUM(CASE WHEN {sql.Owner} = @owner THEN 1 ELSE 0 END) FROM {sql.Table} AS r{hint} "
                + $"WHERE {sql.OutstandingCondition} AND {sql.Partition} IN (SELECT p FROM @partitions) GROUP BY {sql.Partition};\n");
            parameters.Add(Tsql.Json("@partitionset", JsonSerializer.Serialize(partitions, SqlServerJson.Default.StringArray)));
        }
        text.Append($"SELECT COUNT(*) FROM {sql.Table} AS r WHERE {sql.OutstandingCondition} AND {sql.Owner} = @owner;\n");
        if (sql.Policy.SourceLimit is not null)
        {
            text.Append($"SELECT COUNT(*) FROM {sql.Table} AS r WHERE {sql.OutstandingCondition};\n");
        }
        await using var command = Tsql.Command(work, text.ToString(), [.. parameters]);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (sql.Partition is not null)
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                counts.Partitions[reader.GetString(0)] = (reader.GetInt32(1), reader.GetInt32(2));
            }
            await reader.NextResultAsync(cancellationToken);
        }
        await reader.ReadAsync(cancellationToken);
        counts.Owner = reader.GetInt32(0);
        if (sql.Policy.SourceLimit is not null)
        {
            await reader.NextResultAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            counts.Source = reader.GetInt32(0);
        }
        return counts;
    }

    private static async Task<List<LeaseClaim<TKey>>> UpdateClaimsAsync<TKey>(SqlServerTransaction work, LeaseSql sql, LeaseSource<TKey> source, OwnerId owner, List<Candidate<TKey>> accepted, ClaimReason reason, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var k = sql.KeyCount;
        var settle = sql.Settles
            ? ", " + sql.MarkSettled($"CASE WHEN {sql.Expires} IS NOT NULL THEN {sql.Epoch} ELSE {sql.Settled} END")
            : "";
        // A claim of a reconciliation record starts from its requested revision and the generation the candidates
        // were evaluated with, retires an obsolete lease without counting a takeover, and moves the due time to now
        // when only the generation made the record due, so a holder that crashes leaves it due. The execution is
        // derived from the revision and the generation instead of stored.
        var reconcile = sql.Reconcile is null
            ? $"{LeaseSql.Bare(sql.Execution)} = COALESCE({sql.Execution}, NEWID())"
            : $"{LeaseSql.Bare(sql.ClaimedRevision)} = {sql.RequestedRevision}, {LeaseSql.Bare(sql.AttemptedGeneration)} = @generation, "
                + $"{LeaseSql.Bare(sql.FirstRequestedAt)} = NULL, {LeaseSql.Bare(sql.Due)} = LEAST({sql.Due}, SYSUTCDATETIME())";
        var takeover = sql.Supersedes
            ? $"{sql.Expires} IS NOT NULL AND {sql.AttemptedGeneration} >= @generation"
            : $"{sql.Expires} IS NOT NULL";
        var generation = accepted.Max(c => c.Generation);
        var keyColumns = string.Join(", ", sql.Shape.Keys.Select((c, i) => $"k{i} {c.Declaration}"));
        var output = $"{string.Join(", ", sql.Shape.Keys.Select(c => "inserted." + Tsql.Quote(c.Name)))}, inserted.{LeaseSql.Bare(sql.Epoch)}, inserted.{LeaseSql.Bare(sql.Takeovers)}, "
            + $"inserted.{LeaseSql.Bare(sql.Failures)}, {Tsql.Bit($"inserted.{LeaseSql.Bare(sql.Cancel)} IS NOT NULL")}, "
            + $"{(sql.Reconcile is null ? "inserted." + LeaseSql.Bare(sql.Execution) : "CAST(NULL AS uniqueidentifier)")}, "
            + $"{Tsql.Micros("SYSUTCDATETIME()", "inserted." + LeaseSql.Bare(sql.Expires))}, "
            + (sql.Reconcile is null ? "CAST(0 AS bigint)" : "inserted." + LeaseSql.Bare(sql.ClaimedRevision));
        var outputTable = $"DECLARE @claimed TABLE ({keyColumns}, epoch bigint, takeovers int, failures int, cancel bit, execution uniqueidentifier, valid_for bigint, claimed_revision bigint";
        // A job claim uses an attempt and starts the execution, unless it finds a cancellation requested while an
        // earlier holder ran it, the job late before its execution started, or no attempt left after a lost lease; those
        // use none and end the job without a handler. A pending cancellation comes first: the application asked for it,
        // and the holder that would have observed it was lost. The epoch that used the attempt is recorded, so only a
        // graceful release of that claim returns it. A claim that runs the handler starts the attempt and clears the
        // progress of the previous one. An attempt still open was lost with its lease: the claim records it in the run
        // history, ended at its lease's expiry, whatever kind of claim it is.
        var text = new StringBuilder();
        var run = (int)JobClaimKind.Run;
        string job = "", from = "";
        if (sql.Job is not null)
        {
            text.Append("DECLARE @j TABLE (jid uniqueidentifier NOT NULL PRIMARY KEY, kind int NOT NULL);\n");
            text.Append($"INSERT INTO @j (jid, kind) SELECT r.[id], CASE WHEN {sql.Cancel} IS NOT NULL THEN {(int)JobClaimKind.Cancelled} WHEN {sql.JobLate} THEN {(int)JobClaimKind.Late} "
                + $"WHEN {sql.JobExhausted} THEN {(int)JobClaimKind.Exhausted} ELSE {run} END FROM {sql.Table} AS r JOIN @keys AS u ON {sql.KeyJoin};\n");
            text.Append(sql.RecordRun($"{sql.Table} AS r JOIN @keys AS u ON {sql.KeyJoin}", "1 = 1", ((int)JobRunOutcome.LeaseLost).ToString(CultureInfo.InvariantCulture), "NULL", $"LEAST({sql.Expires}, SYSUTCDATETIME())")).Append('\n');
            job = $", [attempts] = CASE WHEN j.kind = {run} THEN r.[attempts] + 1 ELSE r.[attempts] END, "
                + $"[started] = CASE WHEN r.[started] = 1 OR j.kind = {run} THEN 1 ELSE 0 END, "
                + $"[attempt_epoch] = CASE WHEN j.kind = {run} THEN {sql.Epoch} + 1 ELSE r.[attempt_epoch] END, "
                + $"[attempt_started_at] = CASE WHEN j.kind = {run} THEN SYSUTCDATETIME() END, "
                + $"[progress_percent] = CASE WHEN j.kind = {run} THEN NULL ELSE r.[progress_percent] END, "
                + $"[progress_message] = CASE WHEN j.kind = {run} THEN NULL ELSE r.[progress_message] END";
            from = " JOIN @j AS j ON j.jid = r.[id]";
            output += ", j.kind, inserted.[attempts], inserted.[args_version], inserted.[args], inserted.[subject_kind], inserted.[subject_id], inserted.[occurrence], inserted.[due_at], inserted.[trace_parent]";
            outputTable += ", kind int, attempts int, args_version int, args varbinary(max), subject_kind nvarchar(100), subject_id nvarchar(400), occurrence nvarchar(200), due_at datetime2(7), trace_parent nvarchar(100)";
        }
        text.Insert(0, outputTable + ");\n");
        text.Append($"UPDATE r SET {LeaseSql.Bare(sql.Owner)} = @owner, {LeaseSql.Bare(sql.Epoch)} = {sql.Epoch} + 1, "
            + $"{LeaseSql.Bare(sql.Expires)} = {Tsql.AddTicks("SYSUTCDATETIME()", "@duration")}, "
            + $"{LeaseSql.Bare(sql.Takeovers)} = CASE WHEN {takeover} THEN {sql.Takeovers} + 1 ELSE {sql.Takeovers} END, "
            + $"{reconcile}{settle}{job} "
            + $"OUTPUT {output} INTO @claimed "
            + $"FROM {sql.Table} AS r JOIN @keys AS u ON {sql.KeyJoin}{from};\n");
        text.Append("SELECT * FROM @claimed;\n");
        var parameters = new List<SqlParameter> { Tsql.Guid("@owner", owner.Value), Tsql.Ticks("@duration", sql.Policy.Duration) };
        if (sql.Reconcile is not null)
        {
            parameters.Add(Tsql.Int64("@generation", generation));
        }
        await using var command = KeysCommand(work.Connection, work.Transaction, sql, source, accepted.Select(c => c.Key), text.ToString(), [.. parameters]);
        var byKey = accepted.ToDictionary(c => source.Key.Format(c.Key), StringComparer.Ordinal);
        var claims = new List<LeaseClaim<TKey>>(accepted.Count);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var key = ReadKey(source, reader);
            var before = byKey[source.Key.Format(key)];
            var takeovers = reader.GetInt32(k + 1);
            var takenOver = before.HadLease && !before.Superseded;
            var attempt = sql.Reconcile is null ? null : new ReconcileAttempt(reader.GetInt64(k + 6), generation);
            claims.Add(new LeaseClaim<TKey>
            {
                Lease = new Lease<TKey>(source, key, owner, reader.GetInt64(k)),
                Reason = takenOver ? ClaimReason.Takeover : reason,
                PreviousOwner = before.Owner,
                PreviousEpoch = before.Epoch,
                Takeovers = takeovers,
                Failures = reader.GetInt32(k + 2),
                Exhausted = takenOver && takeovers > sql.Policy.MaximumTakeovers,
                CancelRequested = reader.GetBoolean(k + 3),
                ExecutionId = attempt is null
                    ? reader.GetGuid(k + 4)
                    : ReconcileExecution.Id(source.Name, source.Key.Format(key), attempt.Revision, attempt.Generation),
                AttemptId = Guid.CreateVersion7(),
                Partition = before.Partition,
                ValidFor = Tsql.FromMicros(reader.GetInt64(k + 5)),
                Reconcile = attempt,
                Job = sql.Job is null ? null : new JobClaim(
                    (JobClaimKind)reader.GetInt32(k + 7),
                    reader.GetInt32(k + 8),
                    reader.GetInt32(k + 9),
                    reader.GetFieldValue<byte[]>(k + 10),
                    reader.IsDBNull(k + 11) ? null : Subject.Of(reader.GetString(k + 11), reader.GetString(k + 12)),
                    reader.IsDBNull(k + 13) ? null : reader.GetString(k + 13),
                    Tsql.NullableTime(reader, k + 14),
                    reader.IsDBNull(k + 15) ? null : reader.GetString(k + 15)),
            });
        }
        return claims;
    }

    [LoggerMessage(EventId = 4, EventName = "lease.claim_pass_failed", Level = LogLevel.Warning, Message = "A claim pass of {Source} failed after {Claimed} records were claimed; returning those: {Error}")]
    private static partial void LogClaimPassFailed(ILogger logger, Exception? exception, string source, int claimed, string error);

    [LoggerMessage(EventId = 5, EventName = "lease.claim_settlement_failed", Level = LogLevel.Warning, Message = "Settling the expired lease on {Source} record {Key} in a claim failed; the record waits for the settlement backoff: {Error}")]
    private static partial void LogClaimSettlementFailed(ILogger logger, Exception? exception, string source, string key, string error);

    [LoggerMessage(EventId = 1, EventName = "lease.claim_rolled_back", Level = LogLevel.Warning, Message = "Claim of {Source} record {Key} rolled back by its claim hook; counted as a failure: {Error}")]
    private static partial void LogClaimRolledBack(ILogger logger, Exception? exception, string source, string key, string error);
}
