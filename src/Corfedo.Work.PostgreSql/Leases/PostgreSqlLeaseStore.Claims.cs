using Microsoft.Extensions.Logging;
using Npgsql;

namespace Corfedo.Work.PostgreSql.Leases;

internal sealed partial class PostgreSqlLeaseStore
{
    public async ValueTask<DueClaims<TKey>> ClaimDueAsync<TKey>(LeaseSource<TKey> source, LeaseCallbacks<TKey> callbacks, OwnerId owner, int limit, CancellationToken cancellationToken)
        where TKey : notnull
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var sql = Sql(source);
        var claims = new List<LeaseClaim<TKey>>();
        DateTimeOffset cutoff = default;
        // A call attempts at most twice its limit: up to limit records whose claim hook or expiry handler fails or finds
        // a row locked do not reduce what it claims, and records that keep failing cannot keep it running.
        var budget = 2 * limit;
        var attempts = 0;
        var busy = new HashSet<string>(StringComparer.Ordinal);
        var passed = new HashSet<string>(StringComparer.Ordinal);
        var skips = 0;
        while (claims.Count < limit && attempts < budget)
        {
            ClaimPass<TKey> result;
            try
            {
                result = await ClaimDuePassAsync(sql, source, callbacks, owner, Math.Min(limit - claims.Count, budget - attempts), [.. busy], passed, cancellationToken);
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
            attempts += result.Claims.Count + result.Failed + result.PassedOver.Count;
            // Limits can reject candidates beyond a scope's free slots; the next pass excludes the scopes this pass
            // filled. Failed claims moved their records back, so the next pass reaches records due after them, within
            // the attempt budget. A limit scope another transaction holds is not waited for: its records are skipped
            // and later passes of the call leave its partition out, so a long transaction holding one scope delays
            // neither other partitions nor the dispatcher's other sources. A busy owner scope ends the call, since every
            // claim needs it. A record whose callback found a row locked without waiting is passed over: later passes
            // of the call leave it out, as they would a record another transaction holds. Skipped and passed-over
            // records are claimed by a later call. Every pass that continues claims, fails or passes over a record, or
            // leaves out a new partition, at most limit times, so the loop ends.
            var refill = result.RejectedByLimits > 0 && result.Claims.Count > 0;
            var passedOver = result.PassedOver.Count(passed.Add) > 0;
            var retry = result.Failed > 0 || passedOver;
            var skip = !result.OwnerBusy && result.BusyPartitions.Count(busy.Add) > 0 && skips++ < limit;
            if (result.OwnerBusy || (!refill && !retry && !skip))
            {
                break;
            }
        }
        return new DueClaims<TKey>(claims, cutoff);
    }

    /// <summary>
    /// One pass of a due claim in its own transaction, leaving out partitions whose limit scope was found busy and the
    /// records <paramref name="passed"/> names, whose callbacks found a row locked in an earlier pass. Its
    /// cutoff is the transaction's <c>now()</c>, which bounds its scans.
    /// </summary>
    private async Task<ClaimPass<TKey>> ClaimDuePassAsync<TKey>(LeaseSql sql, LeaseSource<TKey> source, LeaseCallbacks<TKey> callbacks, OwnerId owner, int limit, string[] busy, IReadOnlySet<string> passed, CancellationToken cancellationToken)
        where TKey : notnull
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var work = new PostgreSqlTransaction(transaction);
        if (!await FenceDefinitionAsync(work, sql, cancellationToken))
        {
            await using var now = new NpgsqlCommand("SELECT now()", connection, transaction);
            await using var reader = await now.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            return new([], 0, 0) { Cutoff = reader.GetFieldValue<DateTimeOffset>(0) };
        }
        // Passed-over records may be among the earliest due, so the scans take as many more; they stay locked unclaimed
        // until the pass commits.
        var (locked, cutoff) = await LockDueAsync(work, sql, source, sql.ClaimableFor(callbacks), owner, limit + passed.Count, busy, cancellationToken);
        if (passed.Count > 0)
        {
            locked = [.. locked.Where(k => !passed.Contains(source.Key.Format(k))).Take(limit)];
        }
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
        var sql = Sql(source);
        string? waitFor = null;
        while (true)
        {
            await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            var work = new PostgreSqlTransaction(transaction);
            if (!await FenceDefinitionAsync(work, sql, cancellationToken))
            {
                return null;
            }
            if (waitFor is not null)
            {
                await WaitForScopeAsync(work, waitFor, cancellationToken);
            }
            var locked = await LockKeysAsync(work, sql, source, [key], callerTransaction: false, cancellationToken);
            if (locked.Count == 0)
            {
                return null;
            }
            var result = await ClaimLockedAsync(work, sql, source, callbacks, owner, locked, ClaimReason.ByKey, 1, rethrow: true, waitForScopes: false, cancellationToken);
            if (result.Busy is not null)
            {
                waitFor = result.Busy;
                continue;
            }
            await transaction.CommitAsync(cancellationToken);
            return result.Claims.Count == 0 ? null : result.Claims[0];
        }
    }

    public async ValueTask<IReadOnlyList<LeaseClaim<TKey>>> ClaimAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, LeaseCallbacks<TKey> callbacks, OwnerId owner, IReadOnlyList<TKey> candidates, int limit, CancellationToken cancellationToken)
        where TKey : notnull
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var work = PostgreSqlTransaction.From(transaction);
        if (candidates.Count == 0)
        {
            return [];
        }
        var sql = Sql(source);
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
    /// On job sources, takes the definition fence in shared mode before any record or scope lock and reads the stored
    /// definition after it. False when the definition is paused, so nothing is claimed.
    /// </summary>
    /// <exception cref="JobDefinitionChangedException">The stored revision is not the one the statements were built from.</exception>
    private static async Task<bool> FenceDefinitionAsync(PostgreSqlTransaction work, LeaseSql sql, CancellationToken cancellationToken)
    {
        if (sql.Job is not { } job)
        {
            return true;
        }
        await using var batch = new NpgsqlBatch(work.Connection, work.Transaction);
        batch.BatchCommands.Add(new NpgsqlBatchCommand(sql.DefinitionFence!));
        batch.BatchCommands.Add(new NpgsqlBatchCommand(sql.DefinitionRead!));
        await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
        await reader.NextResultAsync(cancellationToken);
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
    /// and taking no more than the owner's free slots. Returns them with the transaction's <c>now()</c>.
    /// </summary>
    private static async Task<(List<TKey> Keys, DateTimeOffset Cutoff)> LockDueAsync<TKey>(PostgreSqlTransaction work, LeaseSql sql, LeaseSource<TKey> source, string claimable, OwnerId owner, int limit, string[] busy, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var policy = sql.Policy;
        if (busy.Length > 0)
        {
            claimable = $"({claimable} AND {sql.PartitionText} <> ALL($3))";
        }
        var saturated = new List<string>();
        if (policy.PartitionLimit is { } partitionLimit)
        {
            saturated.Add($"SELECT {sql.Partition} AS p FROM {sql.Table} AS r WHERE {sql.OutstandingCondition} GROUP BY 1 HAVING count(*) >= {partitionLimit}");
        }
        if (policy.SpreadLimit is { } spreadLimit)
        {
            saturated.Add($"SELECT {sql.Partition} AS p FROM {sql.Table} AS r WHERE {sql.OutstandingCondition} AND {sql.Owner} = $1 GROUP BY 1 HAVING count(*) >= {spreadLimit}");
        }
        var count = policy.OwnerLimit is { } ownerLimit
            ? $"LEAST($2, GREATEST({ownerLimit} - (SELECT count(*) FROM {sql.Table} AS r WHERE {sql.OutstandingCondition} AND {sql.Owner} = $1), 0))"
            : "$2";
        if (policy.SourceLimit is { } sourceLimit)
        {
            // A saturated source takes nothing; expired leases that hold its slots are returned by settlement.
            count = $"LEAST({count}, GREATEST({sourceLimit} - (SELECT count(*) FROM {sql.Table} AS r WHERE {sql.OutstandingCondition}), 0))";
        }
        // Claims do not clear DueAt, so held records keep their due times. Unheld records come from the index on DueAt
        // that leaves held records out, on job sources one range per accepted argument version, expired leases from the
        // index on LeaseExpiresAt; each scan is bounded by now(),
        // which is stable and so usable as an index bound, and every candidate is re-evaluated with clock_timestamp()
        // once locked. Up to the limit is locked from each; the claim keeps the earliest due within the limit.
        // Saturated partitions are counted once per statement from the partition index over outstanding leases, an
        // index-only scan whose cost grows with outstanding leases, and excluded with a hashed NOT IN, so each
        // candidate is checked in constant time. A record with an outstanding lease of its own stays in: claiming it
        // replaces that lease, so it adds nothing to its partition. Partition fields of limited sources are never null.
        // On reconciliation sources, records of an older generation than the current one come from the index on
        // AttemptedGeneration over eligible records, in index order; claims interleave them with records due by time.
        string Select(string range, string order) => saturated.Count == 0
            ? $"SELECT {sql.KeyList} FROM {sql.Table} AS r WHERE {range} AND {claimable} ORDER BY {order} LIMIT {count} FOR UPDATE OF r SKIP LOCKED"
            : $"WITH saturated AS MATERIALIZED ({string.Join(" UNION ", saturated)}) SELECT {sql.KeyList} FROM {sql.Table} AS r "
                + $"WHERE {range} AND {claimable} AND ({sql.Partition} NOT IN (SELECT p FROM saturated) OR {sql.OutstandingCondition}) "
                + $"ORDER BY {order} LIMIT {count} FOR UPDATE OF r SKIP LOCKED";
        NpgsqlParameter[] Parameters() => busy.Length == 0
            ? [Uuid(owner.Value), Int32(limit)]
            : [Uuid(owner.Value), Int32(limit), new NpgsqlParameter<string[]> { TypedValue = busy, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text }];
        await using var batch = new NpgsqlBatch(work.Connection, work.Transaction);
        batch.BatchCommands.Add(new NpgsqlBatchCommand("SELECT now()"));
        foreach (var range in sql.DueRanges)
        {
            batch.BatchCommands.Add(Command(Select($"{range} AND {sql.Due} <= now() AND {sql.Expires} IS NULL", sql.Due), Parameters()));
        }
        batch.BatchCommands.Add(Command(Select($"{sql.Expires} <= now()", sql.Due), Parameters()));
        if (sql.Reconcile is not null)
        {
            batch.BatchCommands.Add(Command(Select($"{sql.AttemptedGeneration} < {sql.Generation}", sql.AttemptedGeneration), Parameters()));
        }
        if (policy.OwnerLimit is not null)
        {
            // The owner's own claimable outstanding leases, from the owner index: replacing one takes no free slot, so
            // they are candidates even when the owner has none. Saturated partitions keep them for the same reason.
            batch.BatchCommands.Add(Command(
                $"SELECT {sql.KeyList} FROM {sql.Table} AS r WHERE {sql.Owner} = $1 AND {sql.OutstandingCondition} AND {claimable} "
                + $"ORDER BY {sql.Due} LIMIT $2 FOR UPDATE OF r SKIP LOCKED",
                Parameters()));
        }
        // The scans overlap (a record can be due by time and of an older generation), and SKIP LOCKED does not skip
        // rows this transaction locked, so each record is kept once.
        var keys = new List<TKey>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        var cutoff = reader.GetFieldValue<DateTimeOffset>(0);
        while (await reader.NextResultAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var key = ReadKey(source, reader);
                if (seen.Add(source.Key.Format(key)))
                {
                    keys.Add(key);
                }
            }
        }
        return (keys, cutoff);
    }

    /// <summary>
    /// Locks the given records, each once, that no other transaction has locked, without waiting. In the caller's
    /// transaction it first requires READ COMMITTED: claims count outstanding leases of other records, and a snapshot
    /// taken before the scope lock would hide claims committed meanwhile, so limits could be exceeded.
    /// </summary>
    private static async Task<List<TKey>> LockKeysAsync<TKey>(PostgreSqlTransaction work, LeaseSql sql, LeaseSource<TKey> source, IReadOnlyCollection<TKey> keys, bool callerTransaction, CancellationToken cancellationToken)
        where TKey : notnull
    {
        await using var batch = new NpgsqlBatch(work.Connection, work.Transaction);
        if (callerTransaction)
        {
            batch.BatchCommands.Add(new NpgsqlBatchCommand("SELECT current_setting('transaction_isolation')"));
        }
        var lockCommand = new NpgsqlBatchCommand($"SELECT {sql.KeyList} FROM {sql.Table} AS r JOIN {sql.Unnest} ON {sql.KeyJoin} ORDER BY {sql.Due} FOR UPDATE OF r SKIP LOCKED");
        AddKeys(lockCommand.Parameters, source, keys.DistinctBy(source.Key.Format));
        batch.BatchCommands.Add(lockCommand);
        await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
        if (callerTransaction)
        {
            await reader.ReadAsync(cancellationToken);
            var isolation = reader.GetString(0);
            if (isolation != "read committed")
            {
                throw new InvalidOperationException($"Claims in the caller's transaction need READ COMMITTED isolation; the transaction uses {isolation.ToUpperInvariant()}.");
            }
            await reader.NextResultAsync(cancellationToken);
        }
        var locked = new List<TKey>();
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

        /// <summary>Records whose callback found a row locked without waiting, by their formatted keys.</summary>
        public IReadOnlyList<string> PassedOver { get; init; } = [];
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
        PostgreSqlTransaction work,
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
        var passedOver = new List<string>();
        ClaimPass<TKey> Pass(List<LeaseClaim<TKey>> claimed, int failed) =>
            new(claimed, rejectedByLimits, failed, busy) { OwnerBusy = ownerBusy, BusyPartitions = busyPartitions, PassedOver = passedOver };
        if (policy.HasStoreLimits)
        {
            var ownerScope = policy.OwnerLimit is null ? null : sql.ScopePrefix + "o|" + owner;
            var sourceScope = policy.SourceLimit is null ? null : sql.ScopePrefix + "s";
            var partitioned = policy.PartitionLimit is not null || policy.SpreadLimit is not null;
            string PartitionScope(Candidate<TKey> candidate) => sql.ScopePrefix + "p|" + candidate.Partition;
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
        // own statements are not accounted; they abort the claim. A callback that found a row locked without waiting
        // for it (Busy) is neither: the record is left as it was for a later call, as a record another transaction
        // holds is.
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
                catch (Exception exception) when (!cancellationToken.IsCancellationRequested && Busy(exception))
                {
                    await work.Transaction.RollbackAsync(savepoint, CancellationToken.None);
                    passedOver.Add(source.Key.Format(candidate.Key));
                    LogCallbackBusy(logger, source.Name, source.Key.Format(candidate.Key));
                    continue;
                }
                catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
                {
                    await work.Transaction.RollbackAsync(savepoint, CancellationToken.None);
                    await CountSettlementFailureAsync(work, sql, source, candidate.Key, cancellationToken);
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
                catch (Exception exception) when (!rethrow && !cancellationToken.IsCancellationRequested && Busy(exception))
                {
                    await work.Transaction.RollbackAsync(savepoint, CancellationToken.None);
                    passedOver.Add(source.Key.Format(candidate.Key));
                    LogCallbackBusy(logger, source.Name, source.Key.Format(candidate.Key));
                    continue;
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
            await work.Transaction.ReleaseAsync(savepoint, cancellationToken);
            claims.Add(claim);
            TakeTurn(sql, reason, candidate);
        }
        return Pass(claims, failed);
    }

    /// <summary>
    /// On job sources, tries the pause lock of each locked record's subject in shared mode and leaves out records whose
    /// subject another transaction is pausing or resuming. Pausing takes the lock exclusively and holds it until it
    /// commits, so the evaluation that follows, a new READ COMMITTED statement, sees every pause committed before the
    /// lock was granted, and a pause waits for the claims that hold it. Locks are only tried: the claim holds record
    /// locks, which a pausing transaction may be waiting for. A pass takes at most one lock per locked record.
    /// </summary>
    private static async Task<List<TKey>> LockSubjectsAsync<TKey>(PostgreSqlTransaction work, LeaseSql sql, LeaseSource<TKey> source, List<TKey> locked, CancellationToken cancellationToken)
        where TKey : notnull
    {
        if (locked.Count == 0)
        {
            return locked;
        }
        await using var command = new NpgsqlCommand(
            $"SELECT {sql.KeyList} FROM {sql.Table} AS r JOIN {sql.Unnest} ON {sql.KeyJoin} "
            + $"WHERE r.\"subject_kind\" IS NOT NULL AND NOT pg_try_advisory_xact_lock_shared({sql.SubjectLock})",
            work.Connection,
            work.Transaction);
        AddKeys(command.Parameters, source, locked);
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

    /// <summary>Counts a failed settlement of a still locked record and defers the next one by the settlement backoff.</summary>
    private static Task CountSettlementFailureAsync<TKey>(PostgreSqlTransaction work, LeaseSql sql, LeaseSource<TKey> source, TKey key, CancellationToken cancellationToken)
        where TKey : notnull => RecordSettlementFailureAsync(work.Connection, work.Transaction, sql, source, key, cancellationToken);

    /// <summary>Counts a failed claim of a still locked record and makes it due again after the failure backoff.</summary>
    private static async Task CountClaimFailureAsync<TKey>(PostgreSqlTransaction work, LeaseSql sql, LeaseSource<TKey> source, TKey key, CancellationToken cancellationToken)
        where TKey : notnull
    {
        // On reconciliation sources the backoff floor is set too, since the record may be due by its generation.
        var retry = sql.Reconcile is null
            ? $"{LeaseSql.Bare(sql.Due)} = clock_timestamp() + {sql.FailureBackoff}"
            : $"({LeaseSql.Bare(sql.Due)}, {LeaseSql.Bare(sql.NotBefore)}) = (SELECT t, t FROM (SELECT clock_timestamp() + {sql.FailureBackoff} AS t) AS b)";
        await using var command = new NpgsqlCommand(
            $"UPDATE {sql.Table} AS r SET {LeaseSql.Bare(sql.Failures)} = {sql.Failures} + 1, {retry} WHERE {sql.KeyMatch(1)}",
            work.Connection,
            work.Transaction);
        AddKey(command, source, key);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<List<Candidate<TKey>>> EvaluateAsync<TKey>(PostgreSqlTransaction work, LeaseSql sql, LeaseSource<TKey> source, List<TKey> keys, string claimable, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var dueByGeneration = sql.Reconcile is null ? "false" : $"({sql.Due} IS NULL OR {sql.Due} > clock_timestamp())";
        var text = $"SELECT {sql.Select(sql.PartitionText, sql.Owner, sql.Epoch, $"{sql.Expires} IS NOT NULL", claimable, sql.OutstandingCondition, sql.NeedsSettlement, sql.Takeovers, sql.Superseded, dueByGeneration, sql.Generation)} "
            + $"FROM {sql.Table} AS r JOIN {sql.Unnest} ON {sql.KeyJoin} ORDER BY {sql.Due}, {sql.KeyList}";
        await using var command = new NpgsqlCommand(text, work.Connection, work.Transaction);
        AddKeys(command.Parameters, source, keys);
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
                !reader.IsDBNull(k + 4) && reader.GetBoolean(k + 4),
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
    /// Takes the transaction-scoped advisory locks of limit scopes in the order of their lock keys, so that claims
    /// touching overlapping scopes cannot deadlock even when two scopes hash to one key. A volatile function in the
    /// select list runs after the sort. Without <paramref name="wait"/> each lock is only tried, and the scopes another
    /// transaction holds are returned in lock-key order.
    /// </summary>
    private static async Task<List<string>> LockScopesAsync(PostgreSqlTransaction work, HashSet<string> scopes, bool wait, CancellationToken cancellationToken)
    {
        var function = wait ? "pg_advisory_xact_lock" : "pg_try_advisory_xact_lock";
        await using var command = new NpgsqlCommand(
            $"SELECT s, {function}(h)::text FROM (SELECT s, hashtextextended(s, 0) AS h FROM unnest($1::text[]) AS s) AS scopes ORDER BY h, s",
            work.Connection,
            work.Transaction);
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = [.. scopes], NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text });
        var busy = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (!wait && reader.GetString(1) == "false")
            {
                busy.Add(reader.GetString(0));
            }
        }
        return busy;
    }

    /// <summary>Waits for one limit scope; called while the transaction holds no other lock.</summary>
    private static async Task WaitForScopeAsync(PostgreSqlTransaction work, string scope, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", work.Connection, work.Transaction);
        command.Parameters.Add(Text(scope));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private sealed class OutstandingCounts
    {
        public Dictionary<string, (int Total, int Mine)> Partitions { get; } = new(StringComparer.Ordinal);

        public int Owner { get; set; }

        public int Source { get; set; }
    }

    private static async Task<OutstandingCounts> CountOutstandingAsync(PostgreSqlTransaction work, LeaseSql sql, OwnerId owner, string[] partitions, CancellationToken cancellationToken)
    {
        var counts = new OutstandingCounts();
        await using var batch = new NpgsqlBatch(work.Connection, work.Transaction);
        if (sql.Partition is not null)
        {
            batch.BatchCommands.Add(Command(
                $"SELECT {sql.PartitionText}, count(*)::int4, (count(*) FILTER (WHERE {sql.Owner} = $1))::int4 FROM {sql.Table} AS r "
                + $"WHERE {sql.OutstandingCondition} AND {sql.PartitionText} = ANY($2) GROUP BY 1",
                Uuid(owner.Value),
                new NpgsqlParameter<string[]> { TypedValue = partitions, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text }));
        }
        batch.BatchCommands.Add(Command($"SELECT count(*)::int4 FROM {sql.Table} AS r WHERE {sql.OutstandingCondition} AND {sql.Owner} = $1", Uuid(owner.Value)));
        if (sql.Policy.SourceLimit is not null)
        {
            batch.BatchCommands.Add(Command($"SELECT count(*)::int4 FROM {sql.Table} AS r WHERE {sql.OutstandingCondition}"));
        }
        await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
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

    private static async Task<List<LeaseClaim<TKey>>> UpdateClaimsAsync<TKey>(PostgreSqlTransaction work, LeaseSql sql, LeaseSource<TKey> source, OwnerId owner, List<Candidate<TKey>> accepted, ClaimReason reason, CancellationToken cancellationToken)
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
            ? $"{LeaseSql.Bare(sql.Execution)} = COALESCE({sql.Execution}, gen_random_uuid())"
            : $"{LeaseSql.Bare(sql.ClaimedRevision)} = {sql.RequestedRevision}, {LeaseSql.Bare(sql.AttemptedGeneration)} = ${k + 3}, "
                + $"{LeaseSql.Bare(sql.FirstRequestedAt)} = NULL, {LeaseSql.Bare(sql.Due)} = LEAST({sql.Due}, clock_timestamp())";
        var takeover = sql.Supersedes
            ? $"{sql.Expires} IS NOT NULL AND {sql.AttemptedGeneration} >= ${k + 3}"
            : $"{sql.Expires} IS NOT NULL";
        var generation = accepted.Max(c => c.Generation);
        // A job claim uses an attempt and starts the execution, unless it finds a cancellation requested while an
        // earlier holder ran it, the job late before its execution started, or no attempt left after a lost lease; those
        // use none and end the job without a handler. A pending cancellation comes first: the application asked for it,
        // and the holder that would have observed it was lost. The epoch that used the attempt is recorded, so only a
        // graceful release of that claim returns it. A claim that runs the handler starts the attempt and clears the
        // progress of the previous one. An attempt still open was lost with its lease: the claim records it in the run
        // history, ended at its lease's expiry, whatever kind of claim it is.
        var (with, from, job, returning) = sql.Job is null
            ? ("", "", "", "")
            : ($"WITH j AS (SELECT r.\"id\" AS jid, CASE WHEN {sql.Cancel} IS NOT NULL THEN {(int)JobClaimKind.Cancelled} WHEN {sql.JobLate} THEN {(int)JobClaimKind.Late} "
                + $"WHEN {sql.JobExhausted} THEN {(int)JobClaimKind.Exhausted} ELSE {(int)JobClaimKind.Run} END AS kind FROM {sql.Table} AS r JOIN {sql.Unnest} ON {sql.KeyJoin}), "
                + $"lost AS ({sql.RecordRun($"{sql.Table} AS r JOIN {sql.Unnest} ON {sql.KeyJoin}", "true", ((int)JobRunOutcome.LeaseLost).ToString(System.Globalization.CultureInfo.InvariantCulture), "NULL", $"LEAST({sql.Expires}, clock_timestamp())")}) ",
                ", j",
                $", \"attempts\" = CASE WHEN j.kind = {(int)JobClaimKind.Run} THEN r.\"attempts\" + 1 ELSE r.\"attempts\" END, "
                + $"\"started\" = r.\"started\" OR j.kind = {(int)JobClaimKind.Run}, "
                + $"\"attempt_epoch\" = CASE WHEN j.kind = {(int)JobClaimKind.Run} THEN {sql.Epoch} + 1 ELSE r.\"attempt_epoch\" END, "
                + $"\"attempt_started_at\" = CASE WHEN j.kind = {(int)JobClaimKind.Run} THEN clock_timestamp() END, "
                + $"\"progress_percent\" = CASE WHEN j.kind = {(int)JobClaimKind.Run} THEN NULL ELSE r.\"progress_percent\" END, "
                + $"\"progress_message\" = CASE WHEN j.kind = {(int)JobClaimKind.Run} THEN NULL ELSE r.\"progress_message\" END",
                ", j.kind, r.\"attempts\", r.\"args_version\", r.\"args\", r.\"subject_kind\", r.\"subject_id\", r.\"occurrence\", r.\"due_at\", r.\"trace_parent\"");
        var text = $"{with}UPDATE {sql.Table} AS r SET {LeaseSql.Bare(sql.Owner)} = ${k + 1}, {LeaseSql.Bare(sql.Epoch)} = {sql.Epoch} + 1, "
            + $"{LeaseSql.Bare(sql.Expires)} = clock_timestamp() + ${k + 2}, "
            + $"{LeaseSql.Bare(sql.Takeovers)} = CASE WHEN {takeover} THEN {sql.Takeovers} + 1 ELSE {sql.Takeovers} END, "
            + $"{reconcile}{settle}{job} "
            + $"FROM {sql.Unnest}{from} WHERE {sql.KeyJoin}{(sql.Job is null ? "" : " AND j.jid = r.\"id\"")} "
            + $"RETURNING {sql.Select(sql.Epoch, sql.Takeovers, sql.Failures, $"{sql.Cancel} IS NOT NULL", sql.Reconcile is null ? sql.Execution : "NULL::uuid", $"extract(epoch FROM {sql.Expires} - clock_timestamp())::float8", sql.Reconcile is null ? "0::int8" : sql.ClaimedRevision)}{returning}";
        await using var command = new NpgsqlCommand(text, work.Connection, work.Transaction);
        AddKeys(command.Parameters, source, accepted.Select(c => c.Key));
        command.Parameters.Add(Uuid(owner.Value));
        command.Parameters.Add(Interval(sql.Policy.Duration));
        if (sql.Reconcile is not null)
        {
            command.Parameters.Add(Int64(generation));
        }
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
                ValidFor = Seconds(reader.GetDouble(k + 5)),
                Reconcile = attempt,
                Job = sql.Job is null ? null : new JobClaim(
                    (JobClaimKind)reader.GetInt32(k + 7),
                    reader.GetInt32(k + 8),
                    reader.GetInt32(k + 9),
                    reader.GetFieldValue<byte[]>(k + 10),
                    reader.IsDBNull(k + 11) ? null : Subject.Of(reader.GetString(k + 11), reader.GetString(k + 12)),
                    reader.IsDBNull(k + 13) ? null : reader.GetString(k + 13),
                    reader.IsDBNull(k + 14) ? null : reader.GetFieldValue<DateTimeOffset>(k + 14),
                    reader.IsDBNull(k + 15) ? null : reader.GetString(k + 15)),
            });
        }
        return claims;
    }

    [LoggerMessage(EventId = 4, EventName = "lease.claim_pass_failed", Level = LogLevel.Warning, Message = "A claim pass of {Source} failed after {Claimed} records were claimed; returning those: {Error}")]
    private static partial void LogClaimPassFailed(ILogger logger, Exception? exception, string source, int claimed, string error);

    [LoggerMessage(EventId = 5, EventName = "lease.claim_settlement_failed", Level = LogLevel.Warning, Message = "Settling the expired lease on {Source} record {Key} in a claim failed; the record waits for the settlement backoff: {Error}")]
    private static partial void LogClaimSettlementFailed(ILogger logger, Exception? exception, string source, string key, string error);

    /// <summary>
    /// Whether a callback failed because a row it would have waited for was locked: <c>NOWAIT</c> or
    /// <c>lock_timeout</c> raise 55P03, directly or wrapped by the callback's data access.
    /// </summary>
    internal static bool Busy(Exception exception)
    {
        for (var e = exception; e is not null; e = e.InnerException)
        {
            if (e is PostgresException { SqlState: PostgresErrorCodes.LockNotAvailable })
            {
                return true;
            }
        }
        return false;
    }

    [LoggerMessage(EventId = 6, EventName = "lease.callback_busy", Level = LogLevel.Debug, Message = "A callback of {Source} record {Key} found a row locked; the record is left for a later claim or settlement")]
    internal static partial void LogCallbackBusy(ILogger logger, string source, string key);

    [LoggerMessage(EventId = 1, EventName = "lease.claim_rolled_back", Level = LogLevel.Warning, Message = "Claim of {Source} record {Key} rolled back by its claim hook; counted as a failure: {Error}")]
    private static partial void LogClaimRolledBack(ILogger logger, Exception? exception, string source, string key, string error);
}
