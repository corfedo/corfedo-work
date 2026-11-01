using Corfedo.Work.InMemory.Database;
using Corfedo.Work.InMemory.Jobs;
using Microsoft.Extensions.Logging;

namespace Corfedo.Work.InMemory.Leases;

internal sealed partial class InMemoryLeaseStore
{
    public async ValueTask<DueClaims<TKey>> ClaimDueAsync<TKey>(LeaseSource<TKey> source, LeaseCallbacks<TKey> callbacks, OwnerId owner, int limit, CancellationToken cancellationToken)
        where TKey : notnull
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var rules = Rules(source);
        var claims = new List<LeaseClaim<TKey>>();
        DateTimeOffset cutoff = default;
        // A call attempts at most twice its limit: up to limit records whose claim hook or expiry handler fails do not
        // reduce what it claims, and records that keep failing cannot keep it running.
        var budget = 2 * limit;
        var attempts = 0;
        while (claims.Count < limit && attempts < budget)
        {
            ClaimPass<TKey> result;
            try
            {
                result = await ClaimDuePassAsync(rules, callbacks, owner, Math.Min(limit - claims.Count, budget - attempts), cancellationToken);
            }
            catch (Exception exception) when (claims.Count > 0)
            {
                // Earlier passes committed their claims; the caller must receive them to hold or give them back.
                LogClaimPassFailed(logger, telemetry.Logged(exception), source.Name, claims.Count, telemetry.Describe(exception));
                break;
            }
            cutoff = result.Cutoff;
            if (result.Selected == 0)
            {
                break;
            }
            claims.AddRange(result.Claims);
            attempts += result.Claims.Count + result.Failed;
            // Limits can reject candidates beyond a scope's free slots; the next pass excludes the scopes this pass
            // filled. Failed claims moved their records back, so the next pass reaches records due after them.
            if (!(result.RejectedByLimits > 0 && result.Claims.Count > 0) && result.Failed == 0)
            {
                break;
            }
        }
        return new DueClaims<TKey>(claims, cutoff);
    }

    /// <summary>One pass of a due claim in its own transaction; its cutoff is the pass's clock reading.</summary>
    private async Task<ClaimPass<TKey>> ClaimDuePassAsync<TKey>(LeaseRules<TKey> rules, LeaseCallbacks<TKey> callbacks, OwnerId owner, int limit, CancellationToken cancellationToken)
        where TKey : notnull
    {
        using var transaction = await database.BeginAsync(cancellationToken);
        var now = Now;
        if (!FenceDefinition(transaction, rules))
        {
            return new([], 0, 0) { Cutoff = now };
        }
        var generation = rules.Generation(transaction, application);
        var selected = SelectDue(transaction, rules, callbacks, owner, limit, now, generation);
        if (selected.Count == 0)
        {
            return new([], 0, 0) { Cutoff = now };
        }
        var result = await ClaimSelectedAsync(transaction, rules, callbacks, owner, selected, ClaimReason.Due, limit, rethrow: false, now, generation, cancellationToken);
        transaction.Commit();
        return result with { Selected = selected.Count, Cutoff = now };
    }

    public async ValueTask<LeaseClaim<TKey>?> ClaimAsync<TKey>(LeaseSource<TKey> source, LeaseCallbacks<TKey> callbacks, OwnerId owner, TKey key, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var rules = Rules(source);
        using var transaction = await database.BeginAsync(cancellationToken);
        if (!FenceDefinition(transaction, rules))
        {
            return null;
        }
        var rowKey = rules.RowKey(key);
        if (rules.Find(transaction, rowKey) is null)
        {
            return null;
        }
        var now = Now;
        var result = await ClaimSelectedAsync(transaction, rules, callbacks, owner, [rowKey], ClaimReason.ByKey, 1, rethrow: true, now, rules.Generation(transaction, application), cancellationToken);
        transaction.Commit();
        return result.Claims.Count == 0 ? null : result.Claims[0];
    }

    public async ValueTask<IReadOnlyList<LeaseClaim<TKey>>> ClaimAsync<TKey>(WorkTransaction transaction, LeaseSource<TKey> source, LeaseCallbacks<TKey> callbacks, OwnerId owner, IReadOnlyList<TKey> candidates, int limit, CancellationToken cancellationToken)
        where TKey : notnull
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var work = Join(transaction);
        if (candidates.Count == 0)
        {
            return [];
        }
        var rules = Rules(source);
        if (!FenceDefinition(work, rules))
        {
            return [];
        }
        var keys = candidates.Select(rules.RowKey).Distinct().Where(k => rules.Find(work, k) is not null).ToList();
        if (keys.Count == 0)
        {
            return [];
        }
        var result = await ClaimSelectedAsync(work, rules, callbacks, owner, keys, ClaimReason.ByKey, limit, rethrow: false, Now, rules.Generation(work, application), cancellationToken);
        return result.Claims;
    }

    /// <summary>
    /// On job sources, reads the stored definition, as the claim's definition fence: false when it is paused, so nothing
    /// is claimed. Storing or pausing a definition is its own transaction, so a claim commits under the revision it read.
    /// </summary>
    /// <exception cref="JobDefinitionChangedException">The stored revision is not the one the source was defined from.</exception>
    private bool FenceDefinition<TKey>(InMemoryTransaction transaction, LeaseRules<TKey> rules)
        where TKey : notnull
    {
        if (rules.Job is not { } job)
        {
            return true;
        }
        var stored = transaction.Get(application.Definitions, job.Definition)?.Stored ?? throw new JobDefinitionChangedException(job.Definition, 0);
        if (stored.Revision != job.Revision)
        {
            throw new JobDefinitionChangedException(job.Definition, stored.Revision);
        }
        return !stored.Paused;
    }

    /// <summary>
    /// Selects claimable records the way a due claim's scans find them: unheld records due by the cutoff, expired leases,
    /// on reconciliation sources records of an older generation, and with an owner limit the owner's own claimable
    /// outstanding leases. Each scan takes up to the owner's free slots, saturated partitions are left out unless the
    /// record has an outstanding lease of its own, and each record is kept once. Ties go to the record written first,
    /// as an index scan returns equal entries in the order their rows were written.
    /// </summary>
    private static List<RowKey> SelectDue<TKey>(InMemoryTransaction transaction, LeaseRules<TKey> rules, LeaseCallbacks<TKey> callbacks, OwnerId owner, int limit, DateTimeOffset now, long generation)
        where TKey : notnull
    {
        var policy = rules.Policy;
        var paused = rules.Pauses(transaction);
        var rows = rules.Scan(transaction).ToList();
        var outstanding = rows.Where(r => rules.Outstanding(r.Row, now)).ToList();
        var saturated = new HashSet<string?>(StringComparer.Ordinal);
        if (policy.PartitionLimit is { } partitionLimit)
        {
            saturated.UnionWith(outstanding.GroupBy(r => rules.Partition(r.Row)).Where(g => g.Count() >= partitionLimit).Select(g => g.Key));
        }
        if (policy.SpreadLimit is { } spreadLimit)
        {
            saturated.UnionWith(outstanding.Where(r => Owned(rules, r.Row, owner)).GroupBy(r => rules.Partition(r.Row)).Where(g => g.Count() >= spreadLimit).Select(g => g.Key));
        }
        var count = limit;
        if (policy.OwnerLimit is { } ownerLimit)
        {
            count = Math.Min(count, Math.Max(ownerLimit - outstanding.Count(r => Owned(rules, r.Row, owner)), 0));
        }
        bool Candidate(Record r) =>
            rules.ClaimableFor(callbacks, r, now, generation, paused)
            && (saturated.Count == 0 || !saturated.Contains(rules.Partition(r)) || rules.Outstanding(r, now));
        var byDue = rows.Where(r => Candidate(r.Row)).OrderBy(r => rules.Due(r.Row) ?? DateTimeOffset.MaxValue).ThenBy(r => r.Row.Version).ToList();
        var scans = new List<IEnumerable<RowKey>>
        {
            byDue.Where(r => rules.Due(r.Row) <= now && rules.Expires(r.Row) is null).Take(count).Select(r => r.Key),
            byDue.Where(r => rules.Expires(r.Row) <= now).Take(count).Select(r => r.Key),
        };
        if (rules.Reconcile is { } fields)
        {
            scans.Add(rows
                .Where(r => r.Row.Int64(fields.AttemptedGeneration) < generation && Candidate(r.Row))
                .OrderBy(r => r.Row.Int64(fields.AttemptedGeneration))
                .ThenBy(r => r.Row.Version)
                .Take(count)
                .Select(r => r.Key));
        }
        if (policy.OwnerLimit is not null)
        {
            // The owner's own claimable outstanding leases: replacing one takes no free slot.
            scans.Add(rows
                .Where(r => Owned(rules, r.Row, owner) && rules.Outstanding(r.Row, now) && rules.ClaimableFor(callbacks, r.Row, now, generation, paused))
                .OrderBy(r => rules.Due(r.Row) ?? DateTimeOffset.MaxValue)
                .ThenBy(r => r.Row.Version)
                .Take(limit)
                .Select(r => r.Key));
        }
        var keys = new List<RowKey>();
        var seen = new HashSet<RowKey>();
        foreach (var key in scans.SelectMany(s => s))
        {
            if (seen.Add(key))
            {
                keys.Add(key);
            }
        }
        return keys;
    }

    private static bool Owned<TKey>(LeaseRules<TKey> rules, Record r, OwnerId owner)
        where TKey : notnull => r.Uuid(rules.Fields.LeaseOwner) == owner.Value;

    private sealed record Candidate(
        RowKey Key,
        DateTimeOffset? Due,
        string? Partition,
        OwnerId? Owner,
        long Epoch,
        bool HadLease,
        bool Claimable,
        bool Outstanding,
        bool NeedsSettlement,
        bool Superseded,
        bool DueByGeneration);

    /// <summary>The outcome of claiming selected records.</summary>
    private sealed record ClaimPass<TKey>(List<LeaseClaim<TKey>> Claims, int RejectedByLimits, int Failed)
        where TKey : notnull
    {
        /// <summary>Records a due claim pass selected.</summary>
        public int Selected { get; init; }

        /// <summary>The cutoff of a due claim pass; unset for claims by key.</summary>
        public DateTimeOffset Cutoff { get; init; }
    }

    /// <summary>
    /// Claims selected records: evaluates claimability, counts outstanding leases and claims the records that fit the
    /// limits. A record whose expired lease needs an expiry handler this caller does not have is left to the expiry
    /// processor.
    /// </summary>
    private async Task<ClaimPass<TKey>> ClaimSelectedAsync<TKey>(
        InMemoryTransaction transaction,
        LeaseRules<TKey> rules,
        LeaseCallbacks<TKey> callbacks,
        OwnerId owner,
        IReadOnlyList<RowKey> selected,
        ClaimReason reason,
        int limit,
        bool rethrow,
        DateTimeOffset now,
        long generation,
        CancellationToken cancellationToken)
        where TKey : notnull
    {
        var policy = rules.Policy;
        var candidates = Interleave(rules, Evaluate(transaction, rules, callbacks, selected, now, generation));
        var accepted = new List<Candidate>();
        var rejectedByLimits = 0;
        if (policy.HasStoreLimits)
        {
            var outstanding = rules.Scan(transaction).Select(r => r.Row).Where(r => rules.Outstanding(r, now)).ToList();
            var partitions = outstanding
                .GroupBy(r => rules.Partition(r) ?? "")
                .ToDictionary(g => g.Key, g => (Total: g.Count(), Mine: g.Count(r => Owned(rules, r, owner))), StringComparer.Ordinal);
            var mine = outstanding.Count(r => Owned(rules, r, owner));
            var total = outstanding.Count;
            foreach (var candidate in candidates)
            {
                if (!candidate.Claimable || accepted.Count == limit)
                {
                    continue;
                }
                var addsTotal = candidate.Outstanding ? 0 : 1;
                var addsMine = candidate.Outstanding && candidate.Owner == owner ? 0 : 1;
                var partition = candidate.Partition is null ? default : partitions.GetValueOrDefault(candidate.Partition);
                if ((policy.PartitionLimit is { } p && partition.Total + addsTotal > p)
                    || (policy.SpreadLimit is { } s && partition.Mine + addsMine > s)
                    || (policy.OwnerLimit is { } o && mine + addsMine > o)
                    || (policy.SourceLimit is { } d && total + addsTotal > d))
                {
                    rejectedByLimits++;
                    continue;
                }
                if (candidate.Partition is not null)
                {
                    partitions[candidate.Partition] = (partition.Total + addsTotal, partition.Mine + addsMine);
                }
                mine += addsMine;
                total += addsTotal;
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
            return new(claims, rejectedByLimits, 0);
        }
        var perRecord = callbacks.Claimed is not null || (callbacks.Expired is not null && accepted.Any(c => c.NeedsSettlement));
        if (!perRecord)
        {
            foreach (var candidate in accepted)
            {
                claims.Add(Claim(transaction, rules, owner, candidate, reason, now, generation));
            }
            TakeTurn(rules, reason, accepted[^1]);
            return new(claims, rejectedByLimits, 0);
        }
        // Failures are accounted by their cause. A failing claim hook is a failed attempt of the record: it counts in
        // Failures and moves the record by the failure backoff (a claim by key reports it to the caller instead). A
        // failing expiry handler is a failed settlement of the previous lease, not an attempt: it counts in
        // SettleFailures and waits for the settlement backoff, leaving the record unclaimed.
        var failed = 0;
        foreach (var candidate in accepted)
        {
            var savepoint = transaction.Save();
            if (candidate.NeedsSettlement && callbacks.Expired is { } expired)
            {
                try
                {
                    await expired(new LeaseExpiry<TKey>(rules.Source, rules.Key(candidate.Key), candidate.Owner!.Value, candidate.Epoch, candidate.Partition), transaction, cancellationToken);
                }
                catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
                {
                    transaction.RollbackTo(savepoint);
                    CountSettlementFailure(transaction, rules, candidate.Key, now);
                    failed++;
                    LogClaimSettlementFailed(logger, telemetry.Logged(exception), rules.Source.Name, rules.Format(candidate.Key), telemetry.Describe(exception));
                    continue;
                }
            }
            var claim = Claim(transaction, rules, owner, candidate, reason, now, generation);
            if (callbacks.Claimed is { } claimed)
            {
                try
                {
                    await claimed(claim, transaction, cancellationToken);
                }
                catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
                {
                    transaction.RollbackTo(savepoint);
                    if (rethrow)
                    {
                        throw;
                    }
                    CountClaimFailure(transaction, rules, candidate.Key, now);
                    failed++;
                    LogClaimRolledBack(logger, telemetry.Logged(exception), rules.Source.Name, rules.Format(candidate.Key), telemetry.Describe(exception));
                    continue;
                }
            }
            claims.Add(claim);
            TakeTurn(rules, reason, candidate);
        }
        return new(claims, rejectedByLimits, failed);
    }

    private static List<Candidate> Evaluate<TKey>(InMemoryTransaction transaction, LeaseRules<TKey> rules, LeaseCallbacks<TKey> callbacks, IReadOnlyList<RowKey> keys, DateTimeOffset now, long generation)
        where TKey : notnull
    {
        var candidates = new List<Candidate>(keys.Count);
        var paused = rules.Pauses(transaction);
        foreach (var key in keys)
        {
            if (rules.Find(transaction, key) is not { } r)
            {
                continue;
            }
            var owner = r.Uuid(rules.Fields.LeaseOwner);
            var due = rules.Due(r);
            candidates.Add(new Candidate(
                key,
                due,
                rules.Partition(r),
                owner is { } id ? new OwnerId(id) : null,
                r.Int64(rules.Fields.LeaseEpoch),
                rules.Expires(r) is not null,
                rules.ClaimableFor(callbacks, r, now, generation, paused),
                rules.Outstanding(r, now),
                rules.NeedsSettlement(r, now),
                rules.Superseded(r, generation),
                rules.Reconcile is not null && (due is null || due > now)));
        }
        return [.. candidates.OrderBy(c => c.Due ?? DateTimeOffset.MaxValue).ThenBy(c => c.Key)];
    }

    /// <summary>
    /// Alternates records due by time with records due only because of an invalidation, each in their own order, so
    /// that neither a large invalidation nor a stream of requests keeps the other waiting. The alternation continues
    /// across due claims of the source in this process.
    /// </summary>
    private static List<Candidate> Interleave<TKey>(LeaseRules<TKey> rules, List<Candidate> candidates)
        where TKey : notnull
    {
        var byGeneration = candidates.Where(c => c.DueByGeneration).ToList();
        if (byGeneration.Count == 0 || byGeneration.Count == candidates.Count)
        {
            return candidates;
        }
        var byTime = candidates.Where(c => !c.DueByGeneration).ToList();
        var (first, second) = rules.GenerationTurn ? (byGeneration, byTime) : (byTime, byGeneration);
        var merged = new List<Candidate>(candidates.Count);
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
    private static void TakeTurn<TKey>(LeaseRules<TKey> rules, ClaimReason reason, Candidate claimed)
        where TKey : notnull
    {
        if (reason == ClaimReason.Due && rules.Reconcile is not null)
        {
            rules.GenerationTurn = !claimed.DueByGeneration;
        }
    }

    /// <summary>
    /// Claims one evaluated record. A claim of a reconciliation record starts from its requested revision and the
    /// current generation, retires an obsolete lease without counting a takeover, and moves the due time to now when
    /// only the generation made the record due, so a holder that crashes leaves it due. The execution is derived from
    /// the revision and the generation instead of stored.
    /// <para>
    /// A job claim uses an attempt and starts the execution, unless it finds a cancellation requested while an earlier
    /// holder ran it, the job late before its execution started, or no attempt left after a lost lease; those use none
    /// and end the job without a handler. A pending cancellation comes first. The epoch that used the attempt is
    /// recorded, so only a graceful release of that claim returns it. A claim that runs the handler starts the attempt
    /// and clears the progress of the previous one. An attempt still open was lost with its lease: the claim records it
    /// in the run history, ended at its lease's expiry, whatever kind of claim it is.
    /// </para>
    /// </summary>
    private LeaseClaim<TKey> Claim<TKey>(InMemoryTransaction transaction, LeaseRules<TKey> rules, OwnerId owner, Candidate before, ClaimReason reason, DateTimeOffset now, long generation)
        where TKey : notnull
    {
        var f = rules.Fields;
        var r = rules.Find(transaction, before.Key)!;
        var expired = rules.Expires(r) is not null;
        var takeover = rules.Supersedes ? expired && r.Int64(rules.Reconcile!.AttemptedGeneration) >= generation : expired;
        var epoch = r.Int64(f.LeaseEpoch) + 1;
        var takeovers = r.Int32(f.Takeovers) + (takeover ? 1 : 0);
        var changes = new List<KeyValuePair<string, object?>>
        {
            Set(f.LeaseOwner, owner.Value),
            Set(f.LeaseEpoch, epoch),
            Set(f.LeaseExpiresAt, now + rules.Policy.Duration),
            Set(f.Takeovers, takeovers),
        };
        ReconcileAttempt? attempt = null;
        Guid execution;
        if (rules.Reconcile is { } rf)
        {
            var revision = r.Int64(rf.RequestedRevision);
            attempt = new ReconcileAttempt(revision, generation);
            changes.Add(Set(rf.ClaimedRevision, revision));
            changes.Add(Set(rf.AttemptedGeneration, generation));
            changes.Add(Set(rf.FirstRequestedAt, null));
            changes.Add(Set(f.DueAt, rules.Due(r) is { } due && due < now ? due : now));
            execution = ReconcileExecution.Id(rules.Source.Name, rules.Format(before.Key), revision, generation);
        }
        else
        {
            execution = r.Uuid(f.ExecutionId) ?? Guid.NewGuid();
            changes.Add(Set(f.ExecutionId, execution));
        }
        JobClaim? job = null;
        if (rules.Job is not null)
        {
            var kind = r.Time(f.CancelRequestedAt) is not null ? JobClaimKind.Cancelled
                : rules.Late(r, now) ? JobClaimKind.Late
                : rules.Exhausted(r) ? JobClaimKind.Exhausted
                : JobClaimKind.Run;
            var expiresAt = rules.Expires(r);
            JobRuns.Record(transaction, application, r, JobRunOutcome.LeaseLost, null, expiresAt is { } lost && lost < now ? lost : now);
            var run = kind == JobClaimKind.Run;
            var attempts = r.Int32(JobColumns.Attempts) + (run ? 1 : 0);
            changes.Add(Set(JobColumns.Attempts, attempts));
            changes.Add(Set(JobColumns.Started, r.Bool(JobColumns.Started) || run));
            if (run)
            {
                changes.Add(Set(JobColumns.AttemptEpoch, epoch));
                changes.Add(Set(JobColumns.ProgressPercent, null));
                changes.Add(Set(JobColumns.ProgressMessage, null));
            }
            changes.Add(Set(JobColumns.AttemptStartedAt, run ? now : null));
            job = new JobClaim(
                kind,
                attempts,
                r.Int32(JobColumns.ArgsVersion),
                r.Bytes(JobColumns.Args)!,
                r.Subject(),
                r.Text(JobColumns.Occurrence),
                rules.Due(r),
                r.Text(JobColumns.TraceParent));
        }
        if (rules.Source.Settles)
        {
            if (expired)
            {
                changes.Add(Set(f.SettledEpoch, r.Int64(f.LeaseEpoch)));
            }
            if (rules.Policy.SettleExpiry)
            {
                changes.Add(Set(f.SettleFailures, 0));
                changes.Add(Set(f.SettleRetryAt, null));
            }
        }
        Write(transaction, rules, before.Key, r, changes);
        var takenOver = before.HadLease && !before.Superseded;
        return new LeaseClaim<TKey>
        {
            Lease = new Lease<TKey>(rules.Source, rules.Key(before.Key), owner, epoch),
            Reason = takenOver ? ClaimReason.Takeover : reason,
            PreviousOwner = before.Owner,
            PreviousEpoch = before.Epoch,
            Takeovers = takeovers,
            Failures = r.Int32(f.Failures),
            Exhausted = takenOver && takeovers > rules.Policy.MaximumTakeovers,
            CancelRequested = r.Time(f.CancelRequestedAt) is not null,
            ExecutionId = execution,
            AttemptId = Guid.CreateVersion7(),
            Partition = before.Partition,
            ValidFor = rules.Policy.Duration,
            Reconcile = attempt,
            Job = job,
        };
    }

    /// <summary>Counts a failed claim and makes the record due again after the failure backoff; on reconciliation sources the backoff floor is set too.</summary>
    private void CountClaimFailure<TKey>(InMemoryTransaction transaction, LeaseRules<TKey> rules, RowKey key, DateTimeOffset now)
        where TKey : notnull
    {
        var r = rules.Find(transaction, key)!;
        var retry = now + rules.FailureBackoff(r);
        var changes = new List<KeyValuePair<string, object?>> { Set(rules.Fields.Failures, r.Int32(rules.Fields.Failures) + 1), Set(rules.Fields.DueAt, retry) };
        if (rules.Reconcile is { } rf)
        {
            changes.Add(Set(rf.NotBefore, retry));
        }
        Write(transaction, rules, key, r, changes);
    }

    /// <summary>Counts a failed settlement and defers the next one by the settlement backoff.</summary>
    internal void CountSettlementFailure<TKey>(InMemoryTransaction transaction, LeaseRules<TKey> rules, RowKey key, DateTimeOffset now)
        where TKey : notnull
    {
        var r = rules.Find(transaction, key)!;
        Write(transaction, rules, key, r, [
            Set(rules.Fields.SettleFailures, r.Int32(rules.Fields.SettleFailures) + 1),
            Set(rules.Fields.SettleRetryAt, now + rules.SettlementBackoff(r)),
        ]);
    }

    [LoggerMessage(EventId = 4, EventName = "lease.claim_pass_failed", Level = LogLevel.Warning, Message = "A claim pass of {Source} failed after {Claimed} records were claimed; returning those: {Error}")]
    private static partial void LogClaimPassFailed(ILogger logger, Exception? exception, string source, int claimed, string error);

    [LoggerMessage(EventId = 5, EventName = "lease.claim_settlement_failed", Level = LogLevel.Warning, Message = "Settling the expired lease on {Source} record {Key} in a claim failed; the record waits for the settlement backoff: {Error}")]
    private static partial void LogClaimSettlementFailed(ILogger logger, Exception? exception, string source, string key, string error);

    [LoggerMessage(EventId = 1, EventName = "lease.claim_rolled_back", Level = LogLevel.Warning, Message = "Claim of {Source} record {Key} rolled back by its claim hook; counted as a failure: {Error}")]
    private static partial void LogClaimRolledBack(ILogger logger, Exception? exception, string source, string key, string error);
}
