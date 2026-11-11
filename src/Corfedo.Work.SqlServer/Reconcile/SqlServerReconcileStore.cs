using System.Globalization;
using Corfedo.Work.SqlServer.Leases;
using Corfedo.Work.SqlServer.Signals;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Reconcile;

/// <summary>Requests, results and invalidation of reconciliation sources, on the statements of their lease sources.</summary>
internal sealed class SqlServerReconcileStore(SqlServerLeaseStore leases, SqlServerSignalSql signals) : IReconcileStore
{
    /// <summary>
    /// Locks the record, then increments the requested revision and schedules it, with a clock reading taken after the
    /// lock is held. The due time is the trailing debounce from now, capped at the first request not yet claimed plus
    /// the maximum; a waiting record keeps an earlier due time; a backoff floor holds unless the request resets it.
    /// While a run holds the record, the due time is recorded for the run's result to schedule the follow-up; on
    /// superseding sources the request instead gives the run's lease back, and the holder's process is signalled to
    /// renew and observe it.
    /// </summary>
    public async ValueTask<bool> RequestAsync<TKey>(WorkTransaction transaction, ReconcileSource<TKey> source, TKey key, ReconcileRequest request, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = SqlServerTransaction.From(transaction);
        var sql = await leases.SqlAsync(source.Leases, cancellationToken);
        if (!SqlServerLeaseStore.Fits(sql, source.Leases, key))
        {
            return false;
        }
        var cancel = sql.Supersedes
            ? signals.InsertIf(WorkTopics.Cancel, SqlServerSignalSql.CancelKey("@owner", "@source"), "@held = 1 AND @owner IS NOT NULL")
            : "";
        var text = $"""
            DECLARE @found int, @held bit, @owner uniqueidentifier;
            SELECT @found = 1 FROM {sql.Table} AS r WITH (UPDLOCK, ROWLOCK) WHERE {sql.KeyMatch};
            SELECT @held = CASE WHEN {sql.Expires} > SYSUTCDATETIME() THEN 1 ELSE 0 END, @owner = {sql.Owner} FROM {sql.Table} AS r WHERE {sql.KeyMatch};
            UPDATE r SET {Schedule(sql, request, "@held = 1")}
            FROM {sql.Table} AS r WHERE {sql.KeyMatch};
            SET @found = @@ROWCOUNT;
            {signals.InsertFrom(WorkTopics.Due, "@source", $"{sql.Table} AS r WHERE {sql.KeyMatch} AND {sql.Expires} IS NULL AND {sql.Due} IS NOT NULL")}{cancel}SELECT @found;
            """;
        await using var command = SqlServerLeaseStore.KeyCommand(work.Connection, work.Transaction, sql, source.Leases, key, text, Parameters(source, request));
        return Convert.ToInt32(await command.ScalarAsync(cancellationToken), CultureInfo.InvariantCulture) == 1;
    }

    /// <summary>
    /// In one command: locks the records in the order of the keys, then schedules them as <see cref="RequestAsync{TKey}"/>
    /// does with a clock reading taken after the locks are held, signals the holders of superseded runs once each,
    /// signals claimers once if any record is due and not held, and returns the keys found. Keys longer than their
    /// columns match no record. The lock statement disables the batch sort of optimized nested loops, which would lock
    /// in index order within each batch instead of the order of the keys. The keys returned are the requested values,
    /// which can differ from the stored ones under the column's collation.
    /// </summary>
    public async ValueTask<IReadOnlyList<TKey>> RequestManyAsync<TKey>(WorkTransaction transaction, ReconcileSource<TKey> source, IReadOnlyList<TKey> keys, ReconcileRequest request, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = SqlServerTransaction.From(transaction);
        var sql = await leases.SqlAsync(source.Leases, cancellationToken);
        List<TKey> fitting = [.. keys.Where(key => SqlServerLeaseStore.Fits(sql, source.Leases, key))];
        if (fitting.Count == 0)
        {
            return [];
        }
        var held = $"{sql.Expires} > @now";
        var cancel = sql.Supersedes
            ? signals.InsertFrom(WorkTopics.Cancel, SqlServerSignalSql.CancelKey("h.owner", "@source"), "(SELECT DISTINCT owner FROM @held WHERE owner IS NOT NULL) AS h")
            : "";
        var text = $"""
            DECLARE @locked int, @now datetime2;
            DECLARE @held TABLE (owner uniqueidentifier);
            SELECT @locked = COUNT(*) FROM {sql.KeyedRows("UPDLOCK, ROWLOCK")} OPTION (USE HINT ('DISABLE_OPTIMIZED_NESTED_LOOP'));
            SET @now = SYSUTCDATETIME();
            UPDATE r SET {Schedule(sql, request, held)}
            OUTPUT CASE WHEN deleted.{LeaseSql.Bare(sql.Expires)} > @now THEN deleted.{LeaseSql.Bare(sql.Owner)} END INTO @held (owner)
            FROM {sql.KeyedRows()};
            {signals.InsertIf(WorkTopics.Due, "@source", $"EXISTS (SELECT 1 FROM {sql.KeyedRows()} WHERE {sql.Expires} IS NULL AND {sql.Due} IS NOT NULL)")}{cancel}SELECT {string.Join(", ", sql.KeyColumns.Split(", ").Select(c => "u." + c))} FROM {sql.KeyedRows()};
            """;
        await using var command = SqlServerLeaseStore.KeysCommand(work.Connection, work.Transaction, sql, source.Leases, fitting, text, Parameters(source, request));
        var found = new HashSet<string>(StringComparer.Ordinal);
        await using (var reader = await command.ReaderAsync(cancellationToken))
        {
            while (await reader.NextRowAsync(cancellationToken))
            {
                found.Add(source.Key.Format(SqlServerLeaseStore.ReadKey(source.Leases, reader)));
            }
        }
        return [.. fitting.Where(key => found.Contains(source.Key.Format(key)))];
    }

    /// <summary>
    /// The assignments of a request to the record <c>r</c>, with whether a run holds it in the condition
    /// <paramref name="held"/>; the debounce is in <c>@delay</c> and <c>@maximum</c>.
    /// </summary>
    private static string Schedule(LeaseSql sql, ReconcileRequest request, string held)
    {
        var first = $"COALESCE({sql.FirstRequestedAt}, SYSUTCDATETIME())";
        var delayed = Tsql.AddTicks("SYSUTCDATETIME()", "@delay");
        var debounced = request.Debounce?.Maximum is null ? delayed : $"LEAST({delayed}, {Tsql.AddTicks(first, "@maximum")})";
        var candidate = $"CASE WHEN {sql.FirstRequestedAt} IS NOT NULL OR {held} THEN {debounced} ELSE LEAST({sql.Due}, {debounced}) END";
        // LEAST and GREATEST ignore nulls: an idle record has no due time, a record without a floor no floor.
        var due = request.ResetBackoff ? candidate : $"GREATEST({candidate}, {sql.NotBefore})";
        var reset = request.ResetBackoff ? $", {LeaseSql.Bare(sql.NotBefore)} = NULL, {LeaseSql.Bare(sql.Failures)} = 0" : "";
        var supersede = sql.Supersedes ? $", {LeaseSql.Bare(sql.Expires)} = CASE WHEN {held} THEN NULL ELSE {sql.Expires} END" : "";
        return $"{LeaseSql.Bare(sql.RequestedRevision)} = {sql.RequestedRevision} + 1, "
            + $"{LeaseSql.Bare(sql.FirstRequestedAt)} = {first}, {LeaseSql.Bare(sql.Due)} = {due}{reset}{supersede}";
    }

    private static SqlParameter[] Parameters<TKey>(ReconcileSource<TKey> source, ReconcileRequest request)
        where TKey : notnull
    {
        var parameters = new List<SqlParameter> { Tsql.Ticks("@delay", request.Debounce?.Delay ?? TimeSpan.Zero), Tsql.Text("@source", source.Name, 1024) };
        if (request.Debounce?.Maximum is { } maximum)
        {
            parameters.Add(Tsql.Ticks("@maximum", maximum));
        }
        return [.. parameters];
    }

    public async ValueTask CompleteAsync<TKey>(WorkTransaction transaction, Lease<TKey> lease, ReconcileResult result, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = SqlServerTransaction.From(transaction);
        if (!await CompleteAsync(work, lease, result, cancellationToken))
        {
            var source = lease.Source;
            throw new LeaseLostException($"The lease on {source.Name} record {source.Key.Format(lease.Key)} at epoch {lease.Epoch} is no longer valid.");
        }
    }

    public async ValueTask<bool> CompleteAsync<TKey>(Lease<TKey> lease, ReconcileResult result, CancellationToken cancellationToken)
        where TKey : notnull
    {
        return await SqlServerLeaseStore.RetryDeadlocksAsync(async () =>
        {
            await using var connection = await leases.OpenAsync(cancellationToken);
            await using var transaction = await SqlServerLeaseStore.BeginAsync(connection, cancellationToken);
            if (!await CompleteAsync(new SqlServerTransaction(transaction), lease, result, cancellationToken))
            {
                return false;
            }
            await transaction.CommitAsync(cancellationToken);
            return true;
        });
    }

    /// <summary>
    /// On superseding sources takes the generation fence, which requires READ COMMITTED, then locks the record and
    /// applies the result if the lease is still valid by a clock reading taken after the locks are held, and gives the
    /// record back. Converged advances the converged revision and generation to what the run claimed and resets
    /// failures; the record is due at the time a request during the run recorded, after the resync interval, or not at
    /// all. Waiting is due at the retry time or the recorded request, whichever is first. Failed counts a failure and
    /// moves the backoff floor and the due time by the failure backoff.
    /// </summary>
    private async Task<bool> CompleteAsync<TKey>(SqlServerTransaction work, Lease<TKey> lease, ReconcileResult result, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var source = lease.Source;
        var sql = await leases.SqlAsync(source, cancellationToken);
        var resync = source.Reconcile!.Policy.ResyncInterval;
        var pending = $"{sql.RequestedRevision} > {sql.ClaimedRevision}";
        var delayed = Tsql.AddTicks("SYSUTCDATETIME()", "@delay");
        var backoff = Tsql.AddTicks("SYSUTCDATETIME()", sql.FailureBackoff);
        var set = result.Outcome switch
        {
            ReconcileOutcome.Converged =>
                $"{LeaseSql.Bare(sql.ConvergedRevision)} = {sql.ClaimedRevision}, {LeaseSql.Bare(sql.ConvergedGeneration)} = {sql.AttemptedGeneration}, "
                + $"{LeaseSql.Bare(sql.Failures)} = 0, {LeaseSql.Bare(sql.NotBefore)} = NULL, "
                + $"{LeaseSql.Bare(sql.Due)} = CASE WHEN {pending} THEN {sql.Due} "
                + (resync is null ? "ELSE NULL END" : $"ELSE {delayed} END"),
            ReconcileOutcome.Waiting =>
                $"{LeaseSql.Bare(sql.Due)} = CASE WHEN {pending} THEN LEAST({sql.Due}, {delayed}) ELSE {delayed} END",
            _ => $"{LeaseSql.Bare(sql.Failures)} = {sql.Failures} + 1, {LeaseSql.Bare(sql.NotBefore)} = {backoff}, {LeaseSql.Bare(sql.Due)} = {backoff}",
        };
        var delay = result.Outcome switch
        {
            ReconcileOutcome.Converged when resync is { } interval => interval,
            ReconcileOutcome.Waiting => result.RetryAfter,
            _ => (TimeSpan?)null,
        };
        var fence = sql.GenerationFence(!work.Work.ReadCommitted);
        // A completed record is claimable again or frees a slot of the source's limits.
        var text = $"""
            {fence}DECLARE @held int, @completed int;
            SELECT @held = 1 FROM {sql.Table} AS r WITH (UPDLOCK, ROWLOCK) WHERE {sql.KeyMatch};
            UPDATE r SET {LeaseSql.Bare(sql.Expires)} = NULL, {LeaseSql.Bare(sql.Takeovers)} = 0, {set}
            FROM {sql.Table} AS r WHERE {sql.KeyMatch} AND {sql.Valid};
            SET @completed = @@ROWCOUNT;
            {signals.InsertFrom(WorkTopics.Due, "@source", $"{sql.Table} AS r WHERE {sql.KeyMatch} AND {sql.Expires} IS NULL AND {sql.Owner} = @owner AND {sql.Epoch} = @epoch{(sql.Policy.HasStoreLimits ? "" : $" AND {sql.Due} IS NOT NULL")}")}SELECT @completed;
            """;
        await using var command = SqlServerLeaseStore.KeyCommand(work.Connection, work.Transaction, sql, source, lease.Key, text,
            Tsql.Guid("@owner", lease.Owner.Value),
            Tsql.Int64("@epoch", lease.Epoch),
            Tsql.Ticks("@delay", delay ?? TimeSpan.Zero),
            Tsql.Text("@source", source.Name, 1024));
        await using var reader = await SqlServerLeaseStore.ExecuteFencedAsync(command, fence is null ? null : work.Work, cancellationToken);
        await reader.NextRowAsync(cancellationToken);
        return reader.GetInt32(0) == 1;
    }

    /// <summary>
    /// Takes the shared generation fences of the superseding sources among <paramref name="sources"/> in the caller's
    /// transaction, in name order, in one batch.
    /// </summary>
    public async ValueTask FenceAsync(WorkTransaction transaction, IReadOnlyCollection<IReconcileSource> sources, CancellationToken cancellationToken)
    {
        var work = SqlServerTransaction.From(transaction);
        var names = sources.Where(s => s.Policy.Requests == RequestPolicy.Supersede)
            .Select(s => LeaseSql.GenerationLockName(leases.Schema, s.Name))
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .ToArray();
        if (names.Length == 0)
        {
            return;
        }
        var text = (work.Work.ReadCommitted ? "" : LeaseSql.IsolationFence)
            + string.Concat(names.Select(n => LeaseSql.AppLock(Tsql.Literal(n), "Shared")));
        await using var command = Tsql.Command(work, text);
        await (await SqlServerLeaseStore.ExecuteFencedAsync(command, work.Work, cancellationToken)).DisposeAsync();
    }

    /// <summary>
    /// Increments the source's generation row in the caller's transaction and signals claimers on commit. On
    /// superseding sources it first takes the generation lock exclusively, waiting for open guarded transactions,
    /// which hold it shared; guarded transactions that start meanwhile wait for the invalidation to end. The row is
    /// created at generation 0 if missing; a concurrent first invalidation that created it meanwhile is waited for.
    /// </summary>
    public async ValueTask<long> InvalidateAllAsync<TKey>(WorkTransaction transaction, ReconcileSource<TKey> source, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = SqlServerTransaction.From(transaction);
        var sql = await leases.SqlAsync(source.Leases, cancellationToken);
        var text = (sql.Supersedes ? sql.InvalidationFence : "")
            + Tsql.IgnoreUnique($"INSERT INTO {sql.Generations} (source, generation) SELECT {sql.GenerationName}, 0 WHERE NOT EXISTS (SELECT 1 FROM {sql.Generations} WITH (UPDLOCK, ROWLOCK) WHERE source = {sql.GenerationName});")
            + $"""
            DECLARE @generation bigint;
            UPDATE {sql.Generations} SET @generation = generation = generation + 1 WHERE source = {sql.GenerationName};
            {signals.Insert}SELECT @generation;
            """;
        await using var command = Tsql.Command(work, text, SqlServerSignalSql.Parameters(WorkTopics.Due, source.Name));
        return (long)(await command.ScalarAsync(cancellationToken))!;
    }
}
