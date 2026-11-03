using Corfedo.Work.SqlServer.Leases;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Jobs;

internal sealed partial class SqlServerJobStore
{
    /// <summary>Not held: no lease, or an expired one, by a clock reading taken in the statement.</summary>
    private const string NotHeld = "(r.lease_expires_at IS NULL OR r.lease_expires_at <= SYSUTCDATETIME())";

    /// <summary>
    /// Running: an attempt under a valid lease, by a clock reading taken in the statement, or an occurrence of a recurring
    /// chain whose attempt its chain holds until the chain settled. Every read and operation that tells running work
    /// from waiting work uses this.
    /// </summary>
    private const string Running = "(r.lease_expires_at > SYSUTCDATETIME() OR r.owned_chain IS NOT NULL)";

    /// <summary>
    /// The order cancellations lock outstanding jobs in, as the key of a table variable: chain steps waiting for their
    /// parent after the others, and occurrences that own a recurring chain last, each in id order. A terminal
    /// transition holds its own row before it decides its waiting children, so a cancellation never holds a waiting
    /// child while it waits for the parent that decides it; a chain that settles records its outcome on its occurrence
    /// after its steps and the chain row, so a cancellation never holds the occurrence while it waits for one of its
    /// chain's steps.
    /// </summary>
    private const string CancelLockTable = "TABLE (owner bit NOT NULL, waiting bit NOT NULL, id uniqueidentifier NOT NULL, PRIMARY KEY (owner, waiting, id))";

    /// <summary>
    /// Clears the occurrence waiting in the identity's recurrence, then locks the definition's outstanding jobs of the
    /// subject or merge key, standalone jobs and occurrences of the identity's recurrence, in the cancellation order,
    /// waiting for other transactions, and cancels them; the recurrence continues. Occurrences to be skipped are left to
    /// finalization.
    /// </summary>
    public async ValueTask<int> CancelAsync(WorkTransaction transaction, LeaseSource<JobId> source, Subject? subject, string? mergeKey, JobEndedCallback? ended, CancellationToken cancellationToken)
    {
        if ((subject is null) == (mergeKey is null))
        {
            throw new ArgumentException("Cancel by a subject or by a merge key.", nameof(subject));
        }
        var work = SqlServerTransaction.From(transaction);
        var sql = await SqlAsync(source, cancellationToken);
        var identity = JobRequestData.Identity(subject, mergeKey)!;
        var definition = Tsql.Text("@definition", sql.Job!.Definition, 200);
        await using (var clear = Tsql.Command(work,
            ClearWaiting($"c.[definition] = @definition AND c.merge_key = @identity"),
            definition,
            Tsql.Text("@identity", identity, 512)))
        {
            await clear.NonQueryAsync(cancellationToken);
        }
        var locked = await LockForCancelAsync(
            work,
            $"{sql.Scope} AND r.state = 0 AND r.skip_pending = 0 AND "
            + (subject is null
                ? $"(r.merge_key = @identity OR r.recurrence_id IN (SELECT c.id FROM {recurrences} AS c WHERE c.[definition] = r.[definition] AND c.merge_key = @identity))"
                : "r.subject_kind = @kind AND r.subject_id = @id"),
            subject is null ? [Tsql.Text("@identity", identity, 512)] : [Tsql.Text("@kind", subject.Kind, 100), Tsql.Text("@id", subject.Id, 400)],
            cancellationToken);
        await CancelSelectedAsync(work, locked, key => key == sql.Job!.Definition ? (source, ended) : null, cancellationToken);
        return locked.Selected.Count;
    }

    /// <summary>
    /// Clears the occurrences waiting in the subject's recurrences, locks the subject's outstanding jobs of every
    /// definition in the cancellation order, refuses the cancellation when one belongs to a definition
    /// <paramref name="definitions"/> does not resolve, then cancels them per definition. Occurrences to be skipped are
    /// left to finalization.
    /// </summary>
    public async ValueTask<int> CancelAllAsync(WorkTransaction transaction, Subject subject, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(subject);
        ArgumentNullException.ThrowIfNull(definitions);
        var work = SqlServerTransaction.From(transaction);
        await using (var clear = Tsql.Command(work,
            ClearWaiting("c.subject_kind = @kind AND c.subject_id = @id"),
            Tsql.Text("@kind", subject.Kind, 100),
            Tsql.Text("@id", subject.Id, 400)))
        {
            await clear.NonQueryAsync(cancellationToken);
        }
        var locked = await LockForCancelAsync(
            work,
            "r.subject_kind = @kind AND r.subject_id = @id AND r.state = 0 AND r.skip_pending = 0",
            [Tsql.Text("@kind", subject.Kind, 100), Tsql.Text("@id", subject.Id, 400)],
            cancellationToken);
        await CancelSelectedAsync(work, locked, definitions, cancellationToken);
        return locked.Selected.Count;
    }

    /// <summary>
    /// Statements that clear the waiting occurrence of the recurrences <paramref name="where"/> selects (alias
    /// <c>c</c>): found without locks, locked through their key in id order, and cleared when they still wait.
    /// </summary>
    private string ClearWaiting(string where) => $"""
        DECLARE @waiting TABLE (id uniqueidentifier NOT NULL PRIMARY KEY);
        INSERT INTO @waiting (id) SELECT c.id FROM {recurrences} AS c WHERE {where} AND c.waiting_at IS NOT NULL;
        UPDATE c SET waiting_at = NULL FROM @waiting AS w INNER LOOP JOIN {recurrences} AS c WITH (UPDLOCK, ROWLOCK, FORCESEEK) ON c.id = w.id
        WHERE c.waiting_at IS NOT NULL;
        """;

    /// <summary>
    /// Locks the outstanding jobs <paramref name="selection"/> selects (alias <c>r</c>) together with the steps of their
    /// chains that wait for their parent and every outstanding step of the chains they own, in the cancellation order,
    /// waiting for other transactions; they are found without locks and evaluated again after the locks are held.
    /// Ending a step decides its waiting children and the continuations of the groups it settles, so a cancellation
    /// holds every waiting step it may decide before it ends anything; a chain cancellation, which locks all of a
    /// chain's outstanding steps, then never holds one of them while it waits for a step this cancellation holds.
    /// </summary>
    private async Task<LockedForCancel> LockForCancelAsync(SqlServerTransaction work, string selection, SqlParameter[] parameters, CancellationToken cancellationToken)
    {
        await using var command = Tsql.Command(work, $"""
            DECLARE @s TABLE (id uniqueidentifier NOT NULL PRIMARY KEY, chain_id uniqueidentifier, owned_chain uniqueidentifier);
            INSERT INTO @s (id, chain_id, owned_chain) SELECT r.id, r.chain_id, r.owned_chain FROM {Jobs} AS r WHERE {selection};
            DECLARE @c {CancelLockTable};
            INSERT INTO @c (owner, waiting, id)
            SELECT DISTINCT {Tsql.Bit("r.owned_chain IS NOT NULL")}, r.chain_waiting, r.id FROM {Jobs} AS r
            WHERE r.id IN (SELECT s.id FROM @s AS s
                UNION ALL SELECT w.id FROM {Jobs} AS w WHERE w.chain_id IN (SELECT s.chain_id FROM @s AS s WHERE s.chain_id IS NOT NULL) AND w.chain_waiting = 1 AND w.state = 0
                UNION ALL SELECT w.id FROM {Jobs} AS w WHERE w.chain_id IN (SELECT s.owned_chain FROM @s AS s WHERE s.owned_chain IS NOT NULL) AND w.state = 0)
            AND r.state = 0 AND r.skip_pending = 0;
            DECLARE @locked int;
            SELECT @locked = COUNT(*) FROM @c AS c INNER LOOP JOIN {Jobs} AS r WITH (UPDLOCK, ROWLOCK, INDEX(jobs_key), FORCESEEK) ON r.id = c.id;
            SELECT r.id, r.[definition], {Tsql.Bit("EXISTS (SELECT 1 FROM @s AS s WHERE s.id = r.id)")}, {Tsql.Bit("r.owned_chain IS NOT NULL")},
                {Tsql.Bit("r.chain_id IN (SELECT s.owned_chain FROM @s AS s WHERE s.owned_chain IS NOT NULL)")}
            FROM @c AS c INNER LOOP JOIN {Jobs} AS r WITH (INDEX(jobs_key), FORCESEEK) ON r.id = c.id
            WHERE r.state = 0 AND r.skip_pending = 0 ORDER BY c.owner, c.waiting, c.id;
            """, parameters);
        var locked = new LockedForCancel([], [], []);
        await using var reader = await command.ReaderAsync(cancellationToken);
        while (await reader.NextRowAsync(cancellationToken))
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
    private async Task CancelSelectedAsync(SqlServerTransaction work, LockedForCancel locked, Func<string, (LeaseSource<JobId> Source, JobEndedCallback? Ended)?> definitions, CancellationToken cancellationToken)
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
    private async Task CancelLockedAsync(SqlServerTransaction work, LeaseSource<JobId> source, Guid[] jobs, JobEndedCallback? ended, CancellationToken cancellationToken)
    {
        if (jobs.Length == 0)
        {
            return;
        }
        var sql = await SqlAsync(source, cancellationToken);
        var held = new HashSet<Guid>();
        var owners = new List<Guid>();
        await using (var command = Tsql.Command(work, $"""
            DECLARE @ids TABLE (id uniqueidentifier NOT NULL PRIMARY KEY);
            INSERT INTO @ids (id) SELECT DISTINCT value FROM OPENJSON(@jobs) WITH (value uniqueidentifier '$');
            DECLARE @held TABLE (id uniqueidentifier NOT NULL, owner uniqueidentifier);
            UPDATE r SET cancel_requested_at = COALESCE(r.cancel_requested_at, SYSUTCDATETIME()),
                pending_args = NULL, pending_args_version = NULL, pending_due = NULL, pending_first_requested_at = NULL
            OUTPUT inserted.id, inserted.lease_owner INTO @held
            FROM @ids AS i INNER LOOP JOIN {sql.Table} AS r WITH (INDEX(jobs_key), FORCESEEK) ON r.id = i.id
            WHERE {sql.Scope} AND r.lease_expires_at > SYSUTCDATETIME();
            SELECT id, owner FROM @held;
            """, Tsql.Guids("@jobs", jobs)))
        await using (var reader = await command.ReaderAsync(cancellationToken))
        {
            while (await reader.NextRowAsync(cancellationToken))
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
    /// found through the outstanding-version index without a lock, locked through its key without waiting,
    /// re-evaluated against the stored definition after the lock is held and ended failed with <c>args_version</c>
    /// through the terminal path, which then starts a pending follow-up. A job whose transition fails, for example in
    /// its outcome hook, is skipped for the rest of the call.
    /// </summary>
    public async ValueTask<int> RetireAsync(LeaseSource<JobId> source, JobEndedCallback? ended, int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var sql = await SqlAsync(source, cancellationToken);
        var retired = $"{sql.Scope} AND r.state = 0 AND {NotHeld} AND r.args_version < (SELECT d.minimum_args_version FROM {definitions} AS d WHERE d.[key] = r.[definition])";
        var failed = new List<Guid>();
        var count = 0;
        while (count < limit)
        {
            var outcome = await SqlServerLeaseStore.RetryDeadlocksAsync(async () =>
            {
                await using var connection = await leases.OpenAsync(cancellationToken);
                await using var transaction = await SqlServerLeaseStore.BeginAsync(connection, cancellationToken);
                var work = new SqlServerTransaction(transaction);
                Guid job;
                await using (var select = Tsql.Command(work,
                    PickOne(sql, "jobs_versions",
                        $"x.[definition] = @definition AND x.state = 0 AND x.args_version < (SELECT d.minimum_args_version FROM {definitions} AS d WHERE d.[key] = @definition) "
                        + "AND x.id NOT IN (SELECT value FROM OPENJSON(@failed) WITH (value uniqueidentifier '$'))",
                        retired, "x.args_version, x.id"),
                    Tsql.Text("@definition", sql.Job!.Definition, 200),
                    Tsql.Guids("@failed", failed)))
                {
                    if (await select.ScalarAsync(cancellationToken) is not Guid found)
                    {
                        return (Done: true, Job: Guid.Empty, Ended: false);
                    }
                    job = found;
                }
                try
                {
                    if (await EndJobAsync(work, source, new JobId(job), JobOutcome.Failed, JobReasons.ArgsVersion, discardPending: false, retired, [], ended, cancellationToken))
                    {
                        await transaction.CommitAsync(cancellationToken);
                        return (Done: false, Job: job, Ended: true);
                    }
                    return (Done: false, Job: job, Ended: false);
                }
                catch (Exception exception) when (!cancellationToken.IsCancellationRequested && !(exception is SqlException e && Tsql.IsDeadlock(e)))
                {
                    failed.Add(job);
                    LogRetireFailed(logger, telemetry.Logged(exception), sql.Job!.Definition, job, telemetry.Describe(exception));
                    return (Done: false, Job: job, Ended: false);
                }
            });
            if (outcome.Done)
            {
                return count;
            }
            if (outcome.Ended)
            {
                count++;
            }
        }
        return count;
    }

    /// <summary>
    /// A batch that picks one job without waiting: a scan of <paramref name="index"/> without locks (alias <c>x</c>,
    /// rows matching <paramref name="scan"/>, in the order <paramref name="order"/>) locks the first row whose latest
    /// committed version meets <paramref name="locked"/> (alias <c>r</c>) through its key, skipping rows other
    /// transactions hold, and returns its id.
    /// </summary>
    private static string PickOne(LeaseSql sql, string index, string scan, string locked, string order) =>
        sql.ScanVariables
        + "DECLARE @picked TABLE (k0 uniqueidentifier NOT NULL PRIMARY KEY);\n"
        + sql.LockScan("pick", "1", index, scan, locked, order, "@picked", "k0", "r.id")
        + "SELECT k0 FROM @picked;";
}
