# Transactions

Work is requested, cancelled and completed in the transaction that makes the change that needs it. A rollback then
leaves no work behind, and a job never runs before its data is committed.

## Getting the handle

Operations take a `WorkTransaction`, a handle to your open transaction. An ADO.NET transaction converts to it, so you
pass your `NpgsqlTransaction` or `SqlTransaction` as it is.

| You use | Handle |
|---|---|
| EF Core | `db.Database.WorkTransaction()`, after `db.Database.BeginTransactionAsync()` |
| Npgsql | the `NpgsqlTransaction` itself |
| SqlClient | the `SqlTransaction` itself |
| Tests on the in-memory store | the transaction from `database.BeginAsync()` |

```csharp
await using var transaction = await connection.BeginTransactionAsync(ct);
// ... your own writes on the same connection ...
await jobs.RequestAsync(transaction, Jobs.FeedSync, args, cancellationToken: ct);
await transaction.CommitAsync(ct);
```

The same handle works for jobs, leases, reconciliation and signals.

## Rules

- **A transaction must be open.** `db.Database.WorkTransaction()` throws when the `DbContext` has none, because the
  work would otherwise commit ahead of your changes. EF Core's implicit transaction in `SaveChanges` does not count:
  begin one.
- **Same database.** The transaction must be on the database the store uses. The store checks that it is a
  transaction of its provider, such as an `NpgsqlTransaction`.
- **Read committed.** Claims and fenced writes inside your transaction require READ COMMITTED, the default isolation
  level, and refuse others. On SQL Server the database reads committed snapshots (`READ_COMMITTED_SNAPSHOT ON`).
- **A refusal ends the transaction.** When an operation refuses your transaction, it is rolled back. On SQL Server the
  server ends it, and so does a deadlock: `Commit` then throws, and a command sent on the connection afterwards runs
  outside any transaction. Start a new transaction instead of continuing. On PostgreSQL the transaction is aborted
  and `Commit` rolls it back without an error.
- **Retry the whole transaction after a deadlock.** Your transaction and another one can wait for each other, for
  example when they request the same jobs in opposite orders or mix requests with writes to the same rows. The
  database ends one of them. Run your unit of work again in a new transaction. The deadlock errors are
  `PostgresException` 40P01, and `SqlException` 1205 or 51205: SQL Server reports a deadlock on the store's locks as
  51205. EF Core's retrying execution strategy runs the unit again when you wrap it in `strategy.ExecuteAsync`; on SQL
  Server add 51205 with `EnableRetryOnFailure(errorNumbersToAdd: [51205])`.
- **Fenced writes.** A handler that writes its results calls `run.GuardAsync(transaction)` first. It locks the row and
  checks that the run still holds it, so a worker that lost its lease cannot commit. See [leases](leases.md).

## Callbacks in Work's transactions

Claim hooks, expiry handlers and outcome hooks run inside the transaction of the claim, settlement or job transition
and receive its handle. Work began that transaction, except for a transition your code makes in its own, such as
`CancelAsync(tx)`. Writes made through the handle commit or roll back with the transition. Turn the handle back into
your data access:

| You use | In the callback |
|---|---|
| Npgsql | `tx.Npgsql()` returns the `NpgsqlTransaction`; run commands on its `Connection` |
| SqlClient | `tx.SqlServer()` returns the `SqlTransaction`; run commands on its `Connection` |
| EF Core | `await db.Database.UseWorkTransactionAsync(tx)` on a `DbContext` that has not opened a connection yet, such as one from the callback's scope |

```csharp
public sealed class ImportEnded(ImportsContext db) : IJobEnded
{
    public async ValueTask EndedAsync(JobEnd end, WorkTransaction tx, CancellationToken ct)
    {
        await db.Database.UseWorkTransactionAsync(tx, ct);
        db.ImportOutcomes.Add(new ImportOutcome(end.Job, end.Outcome));
        await db.SaveChangesAsync(ct);
    }
}
```

- **Only the given connection.** Write through the handle's connection. Work does not see writes made on another
  connection, and they do not roll back with it.
- **Leave the transaction to Work.** Do not commit, roll back or change its isolation level. Work commits it after
  the callback returns and rolls the callback's work back when it throws.
- **No external calls.** A callback can run more than once when its transaction rolls back. Request a job for work
  that must reach outside.
- **Work's record is locked first.** Write rows that your own transactions lock after the work record, or insert new
  rows. A row your transactions lock before it, such as a parent row, belongs in the handler's first guarded
  transaction or in `run.CompleteAsync(tx)`. A callback that must take such a row tries the lock without waiting:
  `FOR UPDATE NOWAIT` on PostgreSQL, `WITH (UPDLOCK, NOWAIT)` on SQL Server. When the row is locked, Work leaves the
  claim or settlement for later without counting a failure, and rolls a job transition back and retries it. On SQL Server keep `XACT_ABORT` off for that statement,
  as SqlClient and EF Core do by default. With it on, the lock error ends Work's whole transaction. See the
  [leases design](../design/leases.md#operations) for the lock order.

## Work that is not in a transaction

An operator action, such as pausing a definition or rerunning a job, runs in its own transaction when the operation
takes no handle.

Code that has no transaction at the call site can request a job without a handle:

```csharp
await jobs.RequestAsync(Jobs.FeedSync, args, cancellationToken: ct);
```

The request commits on its own and wakes the workers after that commit. A rollback of your own transaction does not
remove the job, so prefer the transactional form wherever the job depends on your changes.
