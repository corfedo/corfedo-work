# Transactions

Work is requested, cancelled and completed in the transaction that makes the change that needs it. A rollback then
leaves no work behind, and a job never runs before its data is committed.

## Getting the handle

Operations take a `WorkTransaction`, a handle to your open transaction. An ADO.NET transaction converts to it, so you
pass your `NpgsqlTransaction` as it is.

| You use | Handle |
|---|---|
| EF Core | `db.Database.WorkTransaction()`, after `db.Database.BeginTransactionAsync()` |
| Npgsql | the `NpgsqlTransaction` itself |
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
- **Read committed.** Claims and fenced writes inside your transaction require PostgreSQL's default isolation level,
  READ COMMITTED, and refuse others.
- **Fenced writes.** A handler that writes its results calls `run.GuardAsync(transaction)` first. It locks the row and
  checks that the run still holds it, so a worker that lost its lease cannot commit. See [leases](leases.md).

## Work that is not in a transaction

An operator action, such as pausing a definition or rerunning a job, runs in its own transaction when the operation
takes no handle.

Code that has no transaction at the call site can request a job without a handle:

```csharp
await jobs.RequestAsync(Jobs.FeedSync, args, cancellationToken: ct);
```

The request commits on its own and wakes the workers after that commit. A rollback of your own transaction does not
remove the job, so prefer the transactional form wherever the job depends on your changes.
