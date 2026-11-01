using Corfedo.Work.PostgreSql.Leases;
using Npgsql;
using static Corfedo.Work.PostgreSql.Leases.PostgreSqlLeaseStore;

namespace Corfedo.Work.PostgreSql.Jobs;

internal sealed partial class PostgreSqlJobStore
{
    /// <summary>Not held: no lease, or an expired one, by a clock reading taken after the record lock is held.</summary>
    private const string NotHeld = "(r.lease_expires_at IS NULL OR r.lease_expires_at <= clock_timestamp())";

    /// <summary>
    /// Running: an attempt under a valid lease, by a clock reading taken in the statement, or an occurrence of a recurring
    /// chain whose attempt its chain holds until the chain settled. Every read and operation that tells running work
    /// from waiting work uses this.
    /// </summary>
    private const string Running = "(coalesce(r.lease_expires_at > clock_timestamp(), false) OR r.owned_chain IS NOT NULL)";

    /// <summary>
    /// The order cancellations lock outstanding jobs in: chain steps waiting for their parent after the others, and
    /// occurrences that own a recurring chain last, each in id order. A terminal transition holds its own row before it
    /// decides its waiting children, so a cancellation never holds a waiting child while it waits for the parent that
    /// decides it; a chain that settles records its outcome on its occurrence after its steps and the chain row, so a
    /// cancellation never holds the occurrence while it waits for one of its chain's steps.
    /// </summary>
    private const string CancelLockOrder = "r.owned_chain IS NOT NULL, r.chain_waiting, r.id";

    /// <summary>
    /// Clears the occurrence waiting in the identity's recurrence, then locks the definition's outstanding jobs of the
    /// subject or merge key, standalone jobs and occurrences of the identity's recurrence, in <see cref="CancelLockOrder"/>, waiting for
    /// other transactions, and cancels them; the recurrence continues. Occurrences to be skipped are left to
    /// finalization.
    /// </summary>
    public async ValueTask<int> CancelAsync(WorkTransaction transaction, LeaseSource<JobId> source, Subject? subject, string? mergeKey, JobEndedCallback? ended, CancellationToken cancellationToken)
    {
        if ((subject is null) == (mergeKey is null))
        {
            throw new ArgumentException("Cancel by a subject or by a merge key.", nameof(subject));
        }
        var work = PostgreSqlTransaction.From(transaction);
        var sql = Sql(source);
        var identity = JobRequestData.Identity(subject, mergeKey)!;
        await using (var clear = new NpgsqlCommand(
            $"UPDATE {recurrences} SET waiting_at = NULL WHERE definition = $1 AND merge_key = $2 AND waiting_at IS NOT NULL",
            work.Connection,
            work.Transaction))
        {
            clear.Parameters.Add(Text(sql.Job!.Definition));
            clear.Parameters.Add(Text(identity));
            await clear.ExecuteNonQueryAsync(cancellationToken);
        }
        var locked = await LockForCancelAsync(
            work,
            $"SELECT r.id, r.chain_id, r.owned_chain FROM {sql.Table} AS r WHERE {sql.Scope} AND r.state = 0 AND NOT r.skip_pending AND "
            + (subject is null
                ? $"(r.merge_key = $1 OR r.recurrence_id IN (SELECT c.id FROM {recurrences} AS c WHERE c.definition = r.definition AND c.merge_key = $1))"
                : "r.subject_kind = $1 AND r.subject_id = $2"),
            subject is null ? [Text(identity)] : [Text(subject.Kind), Text(subject.Id)],
            cancellationToken);
        await CancelSelectedAsync(work, locked, key => key == sql.Job!.Definition ? (source, ended) : null, cancellationToken);
        return locked.Selected.Count;
    }

    /// <summary>
    /// Clears the occurrences waiting in the subject's recurrences, locks the subject's outstanding jobs of every
    /// definition in <see cref="CancelLockOrder"/>, refuses the cancellation when one belongs to a definition <paramref name="definitions"/>
    /// does not resolve, then cancels them per definition. Occurrences to be skipped are left to finalization.
    /// </summary>
    public async ValueTask<int> CancelAllAsync(WorkTransaction transaction, Subject subject, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(subject);
        ArgumentNullException.ThrowIfNull(definitions);
        var work = PostgreSqlTransaction.From(transaction);
        await using (var clear = new NpgsqlCommand(
            $"UPDATE {recurrences} SET waiting_at = NULL WHERE subject_kind = $1 AND subject_id = $2 AND waiting_at IS NOT NULL",
            work.Connection,
            work.Transaction))
        {
            clear.Parameters.Add(Text(subject.Kind));
            clear.Parameters.Add(Text(subject.Id));
            await clear.ExecuteNonQueryAsync(cancellationToken);
        }
        var locked = await LockForCancelAsync(
            work,
            $"SELECT r.id, r.chain_id, r.owned_chain FROM {jobTable} AS r WHERE r.subject_kind = $1 AND r.subject_id = $2 AND r.state = 0 AND NOT r.skip_pending",
            [Text(subject.Kind), Text(subject.Id)],
            cancellationToken);
        await CancelSelectedAsync(work, locked, definitions, cancellationToken);
        return locked.Selected.Count;
    }

    /// <summary>
    /// Locks the outstanding jobs <paramref name="selection"/> selects (<c>id</c>, <c>chain_id</c> and
    /// <c>owned_chain</c>) together with the steps of their chains that wait for their parent and every outstanding step
    /// of the chains they own, in <see cref="CancelLockOrder"/>, waiting for other transactions. Ending a step decides
    /// its waiting children and the continuations of the groups it settles, so a cancellation holds every waiting step
    /// it may decide before it ends anything; a chain cancellation, which locks all of a chain's outstanding steps, then
    /// never holds one of them while it waits for a step this cancellation holds.
    /// </summary>
    private async Task<LockedForCancel> LockForCancelAsync(PostgreSqlTransaction work, string selection, NpgsqlParameter[] parameters, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"WITH s AS MATERIALIZED ({selection}) "
            + $"SELECT r.id, r.definition, r.id = ANY (ARRAY (SELECT s.id FROM s)), r.owned_chain IS NOT NULL, "
            + "r.chain_id = ANY (ARRAY (SELECT s.owned_chain FROM s WHERE s.owned_chain IS NOT NULL)) "
            + $"FROM {jobTable} AS r WHERE r.id = ANY (ARRAY (SELECT s.id FROM s UNION ALL "
            + $"SELECT w.id FROM {jobTable} AS w WHERE w.chain_id IN (SELECT s.chain_id FROM s WHERE s.chain_id IS NOT NULL) AND w.chain_waiting AND w.state = 0 UNION ALL "
            + $"SELECT w.id FROM {jobTable} AS w WHERE w.chain_id IN (SELECT s.owned_chain FROM s WHERE s.owned_chain IS NOT NULL) AND w.state = 0)) "
            + $"AND r.state = 0 AND NOT r.skip_pending ORDER BY {CancelLockOrder} FOR UPDATE OF r",
            work.Connection,
            work.Transaction);
        command.Parameters.AddRange(parameters);
        var locked = new LockedForCancel([], [], []);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var job = (reader.GetGuid(0), reader.GetString(1));
            if (reader.GetBoolean(2))
            {
                (reader.GetBoolean(3) ? locked.Owners : locked.Selected).Add(job);
            }
            else if (reader.GetBoolean(4))
            {
                locked.OwnedSteps.Add(job);
            }
        }
        locked.Selected.AddRange(locked.Owners);
        return locked;
    }

    /// <summary>
    /// The jobs a cancellation locked: those it selected, the occurrences among them that own a recurring chain, and
    /// the outstanding steps of the chains they own.
    /// </summary>
    private sealed record LockedForCancel(List<(Guid Id, string Definition)> Selected, List<(Guid Id, string Definition)> Owners, List<(Guid Id, string Definition)> OwnedSteps);

    /// <summary>
    /// Cancels what <see cref="LockForCancelAsync"/> locked, per definition: first the steps of the chains selected
    /// occurrences own, so those occurrences stay running until their chains settle and their definition's maintenance
    /// ends them with the chains' outcome, then every other selected job. Refused when a job belongs to a definition
    /// <paramref name="definitions"/> does not resolve, since its outcome hook must run.
    /// </summary>
    private async Task CancelSelectedAsync(PostgreSqlTransaction work, LockedForCancel locked, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken)
    {
        var owners = locked.Owners.Select(j => j.Id).ToHashSet();
        var resolved = new List<((LeaseSource<JobId> Source, JobEndedCallback? Ended) Definition, Guid[] Jobs)>();
        foreach (var batch in new[] { locked.OwnedSteps, locked.Selected.Where(j => !owners.Contains(j.Id)).ToList() })
        {
            foreach (var group in batch.GroupBy(j => j.Definition, StringComparer.Ordinal))
            {
                var definition = definitions(group.Key)
                    ?? throw new InvalidOperationException($"Job definition {group.Key} has jobs this cancellation ends and is not registered in this process, so its outcome hook cannot run; register it with AddJob or AddJobDefinition.");
                resolved.Add((definition, [.. group.Select(j => j.Id)]));
            }
        }
        foreach (var (definition, ids) in resolved)
        {
            await CancelLockedAsync(work, definition.Source, ids, definition.Ended, cancellationToken);
        }
    }

    /// <summary>
    /// Cancels locked outstanding jobs and discards their pending follow-ups. A job whose lease is valid gets a
    /// cancellation request, and its holder's process is signalled; it ends cancelled when its run observes the request.
    /// Every other job ends cancelled now, through the terminal path. Validity is evaluated after the locks are held, and
    /// a job found not held cannot become held while they are.
    /// </summary>
    private async Task CancelLockedAsync(PostgreSqlTransaction work, LeaseSource<JobId> source, Guid[] jobs, JobEndedCallback? ended, CancellationToken cancellationToken)
    {
        if (jobs.Length == 0)
        {
            return;
        }
        var sql = Sql(source);
        var held = new HashSet<Guid>();
        var owners = new List<Guid>();
        await using (var command = new NpgsqlCommand(
            $"UPDATE {sql.Table} AS r SET cancel_requested_at = COALESCE(r.cancel_requested_at, clock_timestamp()), "
            + "pending_args = NULL, pending_args_version = NULL, pending_due = NULL, pending_first_requested_at = NULL "
            + $"WHERE r.id = ANY($1) AND {sql.Scope} AND r.lease_expires_at > clock_timestamp() RETURNING r.id, r.lease_owner",
            work.Connection,
            work.Transaction))
        {
            command.Parameters.Add(Uuids(jobs));
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                held.Add(reader.GetGuid(0));
                owners.Add(reader.GetGuid(1));
            }
        }
        await NotifyCancelAsync(work, source, owners, cancellationToken);
        foreach (var job in jobs.Where(j => !held.Contains(j)))
        {
            await EndJobAsync(work, source, new JobId(job), JobOutcome.Cancelled, JobReasons.Cancelled, discardPending: true, NotHeld, [], ended, cancellationToken);
        }
    }

    /// <summary>
    /// Ends unheld outstanding jobs whose argument version is below the stored minimum, one per transaction: each is
    /// locked without waiting, re-evaluated against the stored definition after the lock is held and ended failed with
    /// <c>args_version</c> through the terminal path, which then starts a pending follow-up. A job whose transition fails, for example in its outcome hook, is
    /// skipped for the rest of the call.
    /// </summary>
    public async ValueTask<int> RetireAsync(LeaseSource<JobId> source, JobEndedCallback? ended, int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var sql = Sql(source);
        var retired = sql.Scope + " AND r.state = 0 AND " + NotHeld
            + $" AND r.args_version < (SELECT d.minimum_args_version FROM {definitions} AS d WHERE d.key = r.definition)";
        var failed = new List<Guid>();
        var count = 0;
        while (count < limit)
        {
            await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted, cancellationToken);
            var work = new PostgreSqlTransaction(transaction);
            Guid job;
            await using (var select = new NpgsqlCommand(
                $"SELECT r.id FROM {sql.Table} AS r WHERE {retired} AND r.id <> ALL($1) ORDER BY r.id LIMIT 1 FOR UPDATE OF r SKIP LOCKED",
                connection,
                transaction))
            {
                select.Parameters.Add(Uuids([.. failed]));
                if (await select.ExecuteScalarAsync(cancellationToken) is not Guid found)
                {
                    return count;
                }
                job = found;
            }
            try
            {
                if (await EndJobAsync(work, source, new JobId(job), JobOutcome.Failed, JobReasons.ArgsVersion, discardPending: false, retired, [], ended, cancellationToken))
                {
                    await transaction.CommitAsync(cancellationToken);
                    count++;
                }
            }
            catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
            {
                failed.Add(job);
                LogRetireFailed(logger, telemetry.Logged(exception), sql.Job!.Definition, job, telemetry.Describe(exception));
            }
        }
        return count;
    }
}
