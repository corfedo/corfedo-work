using System.Globalization;
using Corfedo.Work.SqlServer.Signals;
using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer.Leases;

internal sealed partial class SqlServerLeaseStore
{
    /// <summary>
    /// Locks the record unless another transaction holds its lock, then extends the lease if it is still valid by a
    /// clock reading taken in the next statement, both in a transaction of the batch. A locked record is busy if a read
    /// of its committed version, which takes no lock, still finds it, and lost otherwise. A renewal of its own only
    /// compares the generation.
    /// </summary>
    public async ValueTask<RenewResult> RenewAsync<TKey>(Lease<TKey> lease, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var source = lease.Source;
        var sql = await SqlAsync(source, cancellationToken);
        var text = Tsql.OwnTransaction($"""
            DECLARE @held int, @found int, @expires datetime2(7), @cancel bit, @valid_for bigint;
            SELECT @held = 1 FROM {sql.Table} AS r WITH (UPDLOCK, ROWLOCK, READPAST) WHERE {sql.KeyMatch};
            IF @held IS NULL
                SELECT @found = 1 FROM {sql.Table} AS r WHERE {sql.KeyMatch};
            ELSE
                {Update(sql, false)}
            """) + "SELECT CASE WHEN @expires IS NOT NULL THEN 0 WHEN @found = 1 THEN 2 ELSE 1 END, @cancel, @valid_for;";
        return await RetryDeadlocksAsync(async () =>
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var command = KeyCommand(connection, null, sql, source, lease.Key, text, Parameters(lease));
            await using var reader = await command.ReaderAsync(cancellationToken);
            await reader.NextRowAsync(cancellationToken);
            return reader.GetInt32(0) switch
            {
                0 => RenewResult.Renewed(reader.GetBoolean(1), Tsql.FromMicros(reader.GetInt64(2))),
                2 => RenewResult.Busy,
                _ => RenewResult.Lost,
            };
        });
    }

    /// <summary>
    /// Locks the record, waiting for its lock, then extends the lease if it is still valid by a clock reading taken in
    /// the next statement. On a superseding source, the generation fence is taken first, before the row lock, since the
    /// renewal authorizes what the caller's transaction commits, and the transaction must be READ COMMITTED. A renewal
    /// that marks progress also resets the takeovers.
    /// </summary>
    public async ValueTask<RenewResult> RenewAsync<TKey>(WorkTransaction transaction, Lease<TKey> lease, bool progressed, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = SqlServerTransaction.From(transaction);
        var source = lease.Source;
        var sql = await SqlAsync(source, cancellationToken);
        var fence = sql.GenerationFence(!work.Work.ReadCommitted);
        var text = $"""
            DECLARE @held int, @expires datetime2(7), @cancel bit, @valid_for bigint;
            SELECT @held = 1 FROM {sql.Table} AS r WITH (UPDLOCK, ROWLOCK) WHERE {sql.KeyMatch};
            {Update(sql, progressed)}
            SELECT @cancel, @valid_for WHERE @expires IS NOT NULL;
            """;
        await using var command = KeyCommand(work.Connection, work.Transaction, sql, source, lease.Key, fence + text, Parameters(lease));
        await using var reader = fence is null ? await command.ReaderAsync(cancellationToken) : await ExecuteFencedAsync(command, work.Work, cancellationToken);
        return await reader.NextRowAsync(cancellationToken)
            ? RenewResult.Renewed(reader.GetBoolean(0), Tsql.FromMicros(reader.GetInt64(1)))
            : RenewResult.Lost;
    }

    /// <summary>The renewal's update of a locked record; it sets the variables only when the lease was valid.</summary>
    private static string Update(LeaseSql sql, bool progressed) =>
        $"UPDATE r SET @expires = {LeaseSql.Bare(sql.Expires)} = {Tsql.AddTicks("SYSUTCDATETIME()", "@duration")},{(progressed ? $" {LeaseSql.Bare(sql.Takeovers)} = 0," : "")} "
        + $"@cancel = {Tsql.Bit($"{sql.Cancel} IS NOT NULL")}, @valid_for = {Tsql.Micros("SYSUTCDATETIME()", Tsql.AddTicks("SYSUTCDATETIME()", "@duration"))} "
        + $"FROM {sql.Table} AS r WHERE {sql.KeyMatch} AND {sql.Valid};";

    private static SqlParameter[] Parameters<TKey>(Lease<TKey> lease)
        where TKey : notnull => [Tsql.Guid("@owner", lease.Owner.Value), Tsql.Int64("@epoch", lease.Epoch), Tsql.Ticks("@duration", lease.Source.Policy.Duration)];

    public async ValueTask<bool> ReleaseAsync<TKey>(Lease<TKey> lease, Due due, bool failed, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var source = lease.Source;
        var sql = await SqlAsync(source, cancellationToken);
        var run = "";
        if (sql.Job is not null)
        {
            // The released attempt ends in the run history before the release changes the record, and only while the
            // lease is valid: a stale release records nothing, and settlement or a takeover records the loss. A graceful
            // release returns the attempt and is Released; a failed one follows a lost hold and is LeaseLost, ended now.
            var outcome = $"CASE WHEN @failed = 1 THEN {(int)JobRunOutcome.LeaseLost} ELSE {(int)JobRunOutcome.Released} END";
            run = sql.RecordRun($"{sql.Table} AS r", $"{sql.KeyMatch} AND {sql.Valid} AND r.[attempt_epoch] = {sql.Epoch}", outcome, "NULL", "SYSUTCDATETIME()") + "\n";
        }
        var body = $"""
            DECLARE @held int, @released int;
            SELECT @held = 1 FROM {sql.Table} AS r WITH (UPDLOCK, ROWLOCK) WHERE {sql.KeyMatch};
            {run}UPDATE r SET {LeaseSql.Bare(sql.Expires)} = NULL, {LeaseSql.Bare(sql.Due)} = {sql.DueExpression},
                {LeaseSql.Bare(sql.Takeovers)} = 0, {LeaseSql.Bare(sql.Failures)} = CASE WHEN @failed = 1 THEN {sql.Failures} + 1 ELSE 0 END{(sql.Job is null ? "" : $", [attempts] = CASE WHEN @failed = 0 AND r.[attempt_epoch] = {sql.Epoch} THEN r.[attempts] - 1 ELSE r.[attempts] END, [attempt_started_at] = NULL")}
            FROM {sql.Table} AS r WHERE {sql.KeyMatch} AND {sql.Valid};
            SET @released = @@ROWCOUNT;
            {signals.InsertFrom(WorkTopics.Due, "@source", $"{sql.Table} AS r WHERE {sql.KeyMatch} AND {sql.Expires} IS NULL AND {sql.Owner} = @owner AND {sql.Epoch} = @epoch{(sql.Policy.HasStoreLimits ? "" : $" AND {sql.Due} IS NOT NULL")}")}
            """;
        // A graceful release of a job claim returns the attempt that claim used; the execution stays started.
        return await RetryDeadlocksAsync(async () =>
        {
            await using var connection = await OpenAsync(cancellationToken);
            await using var command = KeyCommand(connection, null, sql, source, lease.Key, Tsql.OwnTransaction(body) + "SELECT @released;",
                Tsql.Guid("@owner", lease.Owner.Value),
                Tsql.Int64("@epoch", lease.Epoch),
                Tsql.Int32("@due_kind", (int)due.Kind),
                Tsql.Ticks("@due_delay", due.Delay),
                Tsql.Bool("@failed", failed),
                Tsql.Text("@source", source.Name, 1024));
            return Convert.ToInt32(await command.ScalarAsync(cancellationToken), CultureInfo.InvariantCulture) == 1;
        });
    }

    public async ValueTask<EndResult> EndAsync<TKey>(WorkTransaction transaction, Lease<TKey> lease, LeaseCompletion? completion, CancellationToken cancellationToken)
        where TKey : notnull
    {
        var work = SqlServerTransaction.From(transaction);
        var source = lease.Source;
        var sql = await SqlAsync(source, cancellationToken);
        var fence = sql.GenerationFence(!work.Work.ReadCommitted);
        // An end on a source with store-enforced limits frees a slot, which claimers learn from the signal.
        var signal = source.Policy.HasStoreLimits ? signals.InsertIf(WorkTopics.Due, "@source", "@ended = 1") : "";
        var text = $"""
            {fence}DECLARE @held int, @ended int;
            SELECT @held = 1 FROM {sql.Table} AS r WITH (UPDLOCK, ROWLOCK) WHERE {sql.KeyMatch};
            UPDATE r SET {LeaseSql.Bare(sql.Expires)} = NULL, {LeaseSql.Bare(sql.Due)} = NULL, {LeaseSql.Bare(sql.Cancel)} = NULL,
                {LeaseSql.Bare(sql.Takeovers)} = 0, {LeaseSql.Bare(sql.Failures)} = 0,
                {LeaseSql.Bare(sql.EndedId)} = @completion, {LeaseSql.Bare(sql.EndedDigest)} = @digest
            FROM {sql.Table} AS r WHERE {sql.KeyMatch} AND {sql.Valid};
            SET @ended = @@ROWCOUNT;
            {signal}SELECT @ended;
            SELECT {sql.Owner}, {sql.Epoch}, {sql.EndedId}, {sql.EndedDigest} FROM {sql.Table} AS r WHERE {sql.KeyMatch};
            """;
        await using var command = KeyCommand(work.Connection, work.Transaction, sql, source, lease.Key, text,
            Tsql.Guid("@owner", lease.Owner.Value),
            Tsql.Int64("@epoch", lease.Epoch),
            Tsql.Guid("@completion", completion?.Id),
            Tsql.Bytes("@digest", completion?.Digest),
            Tsql.Text("@source", source.Name, 1024));
        await using var reader = await ExecuteFencedAsync(command, fence is null ? null : work.Work, cancellationToken);
        await reader.NextRowAsync(cancellationToken);
        if (reader.GetInt32(0) == 1)
        {
            return EndResult.Ended;
        }
        await reader.NextSetAsync(cancellationToken);
        if (completion is not null
            && await reader.NextRowAsync(cancellationToken)
            && !reader.IsDBNull(0) && reader.GetGuid(0) == lease.Owner.Value
            && reader.GetInt64(1) == lease.Epoch
            && !reader.IsDBNull(2) && reader.GetGuid(2) == completion.Id)
        {
            if (!reader.IsDBNull(3) && reader.GetFieldValue<byte[]>(3).AsSpan().SequenceEqual(completion.Digest))
            {
                return EndResult.Replayed;
            }
            throw new LeaseLostException($"A different completion {completion.Id} was already recorded for {source.Name} record {source.Key.Format(lease.Key)}.");
        }
        throw new LeaseLostException($"The lease on {source.Name} record {source.Key.Format(lease.Key)} at epoch {lease.Epoch} is no longer valid.");
    }
}
