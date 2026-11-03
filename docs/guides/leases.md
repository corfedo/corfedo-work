# Leases on your own table

A lease source is a table you own, such as `imports` or `agent_runs`, with lease columns next to your own. Your code
makes a row due. A handler in any replica claims it, holds it while it runs, and ends it in the transaction that
writes the final state. The row's state stays on the row, so there is no second status to keep in sync.

## Define the source

```csharp
public static readonly LeaseSource<long> Imports = LeaseSource.Define("imports",
    StoreLocation.Table("public", "imports"),
    KeyCodec.Int64("id"),
    new LeasePolicy
    {
        Duration = TimeSpan.FromMinutes(2),        // renewed every third of it while the handler runs
        MaximumTakeovers = 3,                      // crashes before ExhaustedAsync decides
    });

work.AddLeaseHandler<long, ImportRunner>(Imports, handler => handler.Capacity(16));
```

The key codec maps the key to its column. `KeyCodec.Int32`, `Int64`, `Guid` and `String` cover keys of one column,
also when your key type wraps the value:

```csharp
KeyCodec.Guid<RunId>("id", key => key.Value, value => new RunId(value))
```

A composite key implements `IKeyCodec<TKey>`.

## The columns

With EF Core, `HasLease` maps the columns onto your entity and your migrations create them:

```csharp
modelBuilder.Entity<Import>(import =>
{
    import.Property(i => i.Id).HasColumnName("id");
    import.HasLease(Imports);
});
```

<details>
<summary>The columns and indexes for PostgreSQL</summary>

```sql
ALTER TABLE imports
    ADD COLUMN due_at timestamptz,
    ADD COLUMN lease_owner uuid,
    ADD COLUMN lease_epoch bigint NOT NULL DEFAULT 0,
    ADD COLUMN lease_expires_at timestamptz,
    ADD COLUMN takeovers integer NOT NULL DEFAULT 0,
    ADD COLUMN failures integer NOT NULL DEFAULT 0,
    ADD COLUMN settled_epoch bigint NOT NULL DEFAULT 0,
    ADD COLUMN settle_failures integer NOT NULL DEFAULT 0,
    ADD COLUMN settle_retry_at timestamptz,
    ADD COLUMN cancel_requested_at timestamptz,
    ADD COLUMN execution_id uuid,
    ADD COLUMN ended_completion_id uuid,
    ADD COLUMN ended_digest bytea;
CREATE INDEX ix_imports_due_at_due ON imports (due_at) WHERE lease_expires_at IS NULL;
CREATE INDEX ix_imports_lease_expires_at_expiry ON imports (lease_expires_at) WHERE lease_expires_at IS NOT NULL;
```

Expiry settlement and limits add the stored generated column `lease_outstanding` and an index each. Startup verifies
the columns and indexes.

</details>

## When a row runs

`due_at` is when the row next needs a run. Null means no run is wanted.

- **Make a row due** by setting `due_at` and saving, or with `leases.ScheduleAsync(tx, Imports, id, Due.Now)`. With
  `AddWorkWakeUps` on the `DbContext` options, saving a due row wakes a worker at once.
- **Claiming does not clear `due_at`.** If the worker crashes, the row is claimable again when the lease expires,
  without a cleanup job.
- **Ending clears it.** `run.EndAsync(tx)` gives the row back with no run wanted.

## The handler

```csharp
public sealed class ImportRunner(AppDb db) : ILeaseHandler<long>
{
    public async ValueTask RunAsync(LeaseRun<long> run, CancellationToken ct)
    {
        // ct fires when the lease is lost, cancellation is requested, the run times out or the process stops.
        await using var transaction = await db.Database.BeginTransactionAsync(ct);
        await run.GuardAsync(db.Database.WorkTransaction(), ct);   // throws if another worker took over
        var import = await db.Imports.FindAsync([run.Key], ct);
        import!.State = ImportState.Finished;
        await db.SaveChangesAsync(ct);
        await run.EndAsync(db.Database.WorkTransaction(), cancellationToken: ct);
        await transaction.CommitAsync(ct);
    }

    // The row took its worker down MaximumTakeovers times: your code decides, for example marking it failed.
    public ValueTask ExhaustedAsync(LeaseRun<long> run, CancellationToken ct) => MarkFailedAsync(run.Key, ct);
}
```

- **Guard every write.** `GuardAsync` locks the row and checks its fence number, so the lock holds until your
  transaction commits and a takeover cannot slip in between. Long work commits in several guarded transactions.
- **Failures and crashes.** A handler that throws gives the row back as failed, due again after the source's
  `FailureBackoff` and counted in `failures`. A crash counts in `takeovers`.
- **Waiting.** A run that waits for something external, such as a tool result, calls
  `run.ReleaseAsync(Due.None)`, and your code sets `due_at` when the result arrives.
- **Cancellation.** `leases.RequestCancelAsync(tx, Imports, id)` from any process fires the holder's token within one
  renewal.

Claims can also take one row by key or a set your code selected, run hooks in the claim transaction, settle
resources held by expired leases, and serve holders in other processes through an opaque token. See the
[leases design](../design/leases.md).
