# SQL Server provider

**Status:** Built (stage 9 of [implementation](implementation.md)). This document records what SQL Server does for
each guarantee in [providers](providers.md#required-guarantees), how the PostgreSQL provider's primitives translate,
the measurements behind each choice and the decisions taken in `Corfedo.Work.SqlServer`.

The proofs of concept are small console programs outside the repository, built on `Microsoft.Data.SqlClient` 6.1.3,
run against SQL Server 2022 (16.0.4305.5, CU27) and SQL Server 2025 (17.0.5035.5, CU10, compatibility level 170) with
identical results. Numbers are from a Linux host (AMD Ryzen 7 7700X, Docker, native x86_64) unless they say Mac (Apple M4 Pro,
colima with Rosetta). The essential statements of each program are reproduced below.

## Decisions

| Decision | Choice |
|---|---|
| Dependencies | `Microsoft.Data.SqlClient` for the provider; `Testcontainers.MsSql` for its tests, as the PostgreSQL and EF Core tests use `Testcontainers.PostgreSql` (owner). |
| Supported versions | SQL Server 2022 and 2025 (owner). Azure SQL Managed Instance and Azure SQL Database are not verified. |
| Isolation | The database must run with `READ_COMMITTED_SNAPSHOT ON` (RCSI); the store refuses to start otherwise (owner). Without RCSI every read of other rows waits for their writers and the limit-scope pattern deadlocks with application transactions (see [isolation](#isolation-and-lock-then-evaluate)). Azure SQL Database runs with RCSI on by default; on SQL Server and Managed Instance it is enabled once per database. |
| Signals | A framework signals table, written in the sending transaction and read below `MIN_ACTIVE_ROWVERSION()` by a server-side short poll (owner). Service Broker is not used: it is unavailable on Azure SQL Database, needs a queue and service per process created at runtime, and every send costs one `SEND` per listening process. See [signals](#signals). |
| Identifiers | Ids are `uniqueidentifier` columns holding the same `Guid` value the application sees, so application tables, tools and EF Core mappings join on them without conversion. SQL Server orders `uniqueidentifier` by its last bytes first, so version-7 ids sort out of time order: inserts spread over the primary key instead of filling its last page. Every rule that locks rows "in id order" means SQL Server's order, produced by the statement's `ORDER BY` or, in .NET, by `SqlGuid` comparison, never by `Guid` comparison. Page splits and fragmentation of `jobs` and `job_runs` are measured at 1M and 10M jobs before the schema baseline is fixed. |
| Lock escalation | Framework tables are created with `LOCK_ESCALATION = DISABLE`. Startup verification refuses a lease or reconciliation source whose table escalates (`sys.tables.lock_escalation_desc` other than `DISABLE`): one application statement locking more than about 5 000 of its rows would otherwise take a table lock, which `READPAST` cannot skip, and every claim of the source would wait for it. Verification disables escalation on such a table itself (owner), waiting at most one second for the schema lock the change takes, and refuses the source, naming the `ALTER TABLE ... SET (LOCK_ESCALATION = DISABLE)` statement, only when its login may not alter the table or the lock is not granted. |
| Native AOT | The provider is not trimming- or Native AOT-compatible (owner): `Microsoft.Data.SqlClient` 7.1.1 reports trimming and AOT analysis warnings of its own. |
| Outstanding leases | Application sources that settle carry a stored computed column `lease_outstanding`, `CASE WHEN settled_epoch < lease_epoch THEN 1 END` (PERSISTED on SQL Server, GENERATED ALWAYS ... STORED on PostgreSQL), and their settlement and partition indexes lead with it, filtered `WHERE lease_expires_at IS NOT NULL`, on both providers (owner, 2026-11-02). One index shape then serves every store, so the EF Core mapping needs no provider knowledge, and settlement and limit counts pass over no expired lease already settled. The framework's job table keeps its own indexes over the epochs. Measured on 1M rows with 100 000 expired, settled, not reclaimed leases: SQL Server settlement scan and limit counts 4.8–5.1 ms with the epochs carried in the index, 0.35–0.43 ms with the column; PostgreSQL unchanged against its partial indexes (0.005 ms settlement, 0.15 ms partition saturation), next expiry 0.022 ms against 1.9 ms. Rejected: epochs as key columns under the same filter on PostgreSQL (settlement 16 ms, partition saturation 2.7 ms at 100 000). |
| No temporary objects | No provider batch declares a table variable or temporary table (owner, 2026-11-03): SQL Server creates them in tempdb on every execution of a parameterized batch, and under concurrent claims their creation contended on tempdb's system pages (see the tempdb finding). Sets pass between statements as JSON text in `nvarchar(max)` variables, read by `OPENJSON` with each key column's exact type and collation, or are read again from their tables; a loop over rows reads its JSON array once (`Tsql.EachRow`). `OUTPUT` returns rows to the client only from framework tables, since application tables may have triggers (334); claims on application sources update, then read the rows again. Memory-optimized tempdb metadata, which takes a restart, is not required. |
| Key length and code page | A key with a string field longer than its column (in characters for `nvarchar` and `nchar`, in bytes of the column's code page for `varchar` and `char`), or with a character a `varchar` or `char` column's code page lacks, matches no record, as on PostgreSQL (owner, 2026-11-03). SQL Server truncates such a value, as a parameter and when `OPENJSON` reads it with the column's declaration, and converts a missing character to a similar one or to `?`, either of which can be the key of another record; operations on keys the caller gives check the key first. Keys read from rows always fit. Key parameters and the variables of claim scans of `varchar` and `char` columns are Unicode and meet the column as `CAST(@k0 COLLATE <column collation> AS <column type>)`, the conversion `OPENJSON` applies: a non-Unicode parameter or variable takes the database's code page and loses the characters of a column in another one. The seeks stay equality seeks (owner, 2026-11-03). |
| Connection pool | The tests run SqlClient's channel-based pool (`Switch.Microsoft.Data.SqlClient.UseConnectionPoolV2`, opt-in since 7.1), and the production guide recommends it (owner); see the login-cost finding. |

## Local setup

SQL Server images are published for `linux/amd64` only; Azure SQL Edge, the former arm64 option, was retired on
30 September 2025. On an Apple silicon Mac the amd64 image runs under Rosetta in a colima VM:

```sh
colima start sqlserver --arch aarch64 --vm-type vz --vz-rosetta --cpu 4 --memory 6 --disk 30
docker context use colima        # colima start makes the new profile the current context
export DOCKER_HOST=unix://$HOME/.colima/sqlserver/docker.sock
docker run -d --name mssql --platform linux/amd64 -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD='<password>' \
  -e MSSQL_PID=Developer -p 127.0.0.1:14331:1433 mcr.microsoft.com/mssql/server:2022-latest
docker exec mssql /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P '<password>' -Q 'SELECT 1'
```

QEMU is not needed: the VM registers Rosetta as the `x86_64` handler. SQL Server needs at least 2 GiB; under load the
container used 2.4 GiB of the VM's 6.

| | Mac (Rosetta) | Linux x86_64 (native) |
|---|---|---|
| Container start to first `SELECT 1` | 6.8 s (2022), 7.1 s (2025) | 2.8 s (2022), 3.4 s (2025) |
| Claim pass, 1 worker, 8 records, p50 | 5.6 ms | 2.1 ms |
| Claim and end, 8 workers | 1 690 claims/s | 5 000 claims/s |
| Race of 16 workers for 180 s (claims, renewals, ends, stalled holders) | 277 000 claims, 0 errors, 0 invariant failures, no restart | |

Full suites run on a native x86_64 host; the Mac is for small runs. Server-side the claim `UPDATE` takes 75 µs; the
client-side pass is mostly round trips (begin, statement, commit) and the compilation of statement text that differs
per call, which the provider avoids with parameterized statements and table-valued input.

`Microsoft.Data.SqlClient` refuses to open connections under `InvariantGlobalization` (`NotSupportedException`). A
Native AOT publish of an application that references the provider reports SqlClient 7.1.1's trimming and AOT analysis
warnings (50 distinct warnings on osx-arm64: diagnostic listener and event source writes, authentication provider and
retry-logic loading by type name, UDT and vector serialization, and `System.Configuration` in
`System.Configuration.ConfigurationManager`, which SqlClient references), so the provider is not marked trimming- or
Native AOT-compatible and the AOT smoke application does not reference it.

## Isolation and lock, then evaluate

Every case below was run with RCSI off and on: a session A holds a lock or an uncommitted change, session B runs the
statement and waits.

| Case | RCSI off | RCSI on |
|---|---|---|
| B's single `UPDATE ... WHERE expires > SYSUTCDATETIME()` waits 3 s on A's lock; the lease expires 1.5 s in | renews the expired lease | renews the expired lease |
| The same `UPDATE` with `READCOMMITTEDLOCK` | renews the expired lease | renews the expired lease |
| B locks the row in one statement and updates in the next statement of the same batch | correct: not renewed | correct: not renewed |
| After A takes over the lease and commits: plain `SELECT` | waits, new owner | no wait, **old owner** |
| `SELECT WITH (UPDLOCK, ROWLOCK)` or `WITH (READCOMMITTEDLOCK)` | waits, new owner | waits, new owner |
| `SELECT WITH (UPDLOCK, ROWLOCK, READPAST)` | no wait, row skipped | no wait, row skipped |
| Single `UPDATE` matched on the old owner | waits, matches nothing | waits, matches nothing |
| B's single `UPDATE` waits on the record while A raises the source's generation; B's subquery reads the generation | correct | **trusts the generation read before the wait** |
| The same subquery `WITH (READCOMMITTEDLOCK)`, or in the statement after the lock | correct | correct |
| Counting a partition's outstanding leases while an application transaction has written one of them | **waits for it** (lock timeout 1222) | no wait, count 4 of 4 |
| The same count `WITH (READPAST)` | no wait, **count 3 of 4** | count 4 of 4 |

So:

- `SYSUTCDATETIME()` is evaluated once per statement, before any lock wait inside it (one value over a statement of
  1M rows). A clock reading taken after the lock is held is the clock of a statement that starts after the lock
  statement. This is the PostgreSQL provider's design already (a lock statement, then the evaluating statement); it
  carries over unchanged.
- Under RCSI a statement reads the committed versions as of its own start, also after it waited on a lock, except
  for the rows it locks itself (`UPDLOCK`, updates) and reads with `READCOMMITTEDLOCK`. Every condition that depends on
  a lock is evaluated in a statement that starts after the lock statement, never in a subquery of the waiting
  statement.
- Without RCSI a read of any row waits for its writer. Claims read rows they do not lock (limit counts, generations,
  pauses, definitions), so a claim would wait for application transactions, and the limit-scope pattern deadlocks:
  the provider claim holds a candidate row and the partition's scope lock and counts the partition; an application
  transaction has written another row of the partition and then claims in the same scope. Reproduced: the provider
  claim was chosen as deadlock victim (1205). Under RCSI the count reads the committed version and the application
  waits for the scope until the claim commits. `READPAST` does not help: it leaves locked rows out of the count. This
  is why RCSI is required.
- A `SNAPSHOT` transaction that locks a row changed after its snapshot started fails with 3960 (update conflict), as
  a PostgreSQL repeatable-read transaction fails on a concurrent update.

The store checks RCSI at startup:

```sql
SELECT is_read_committed_snapshot_on FROM sys.databases WHERE name = DB_NAME();
```

and refuses to start when it is 0, naming `ALTER DATABASE [<db>] SET READ_COMMITTED_SNAPSHOT ON`.

### The caller's isolation level

`sys.dm_exec_sessions.transaction_isolation_level` for `@@SPID` is 1 (read uncommitted), 2 (read committed; under
RCSI this is the statement-snapshot level the provider needs), 3 (repeatable read), 4 (serializable) or 5
(snapshot). A session sees its own row without `VIEW SERVER STATE`.

| Check | Per round trip |
|---|---|
| `SELECT 1` | 0.17 ms |
| `IF (SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID) <> 2 THROW ...` | 0.63–0.72 ms |
| The same on `sys.dm_exec_requests` | 0.72 ms |

The check costs about 0.45 ms of server time, too much for every fenced statement. `SqlTransaction.IsolationLevel`
is free but wrong after `SET TRANSACTION ISOLATION LEVEL` in SQL (it reported `ReadCommitted` for a serializable
transaction). The provider checks once per transaction, in the batch of the first operation that needs read committed,
and records the result on the `WorkTransaction` handle, which exists once per transaction. This needs a provider slot
on the handle in the core (an internal change; providers use the core's internals).

### Claims race

16 workers on 2 000 records with 300 ms leases: each claims up to 8 records, then renews, ends, or stalls past its
lease so another worker takes over. Every transition is recorded with the old expiry and the store clock, and checked
afterwards. With RCSI off and on: 0 claims of an unexpired lease, 0 renewals or ends of an expired lease, 0 gaps or
repeats in each record's epochs, 0 deadlocks; 1 566 and 1 597 claims/s on the Mac.

The claim pass of the POC, one batch in the provider's transaction:

```sql
DECLARE @c TABLE (id int PRIMARY KEY);
INSERT @c SELECT TOP (@n) id FROM leases WITH (UPDLOCK, ROWLOCK, READPAST, INDEX(leases_due))
    WHERE expires IS NULL AND due <= SYSUTCDATETIME() ORDER BY due;
INSERT @c SELECT TOP (@n) id FROM leases WITH (UPDLOCK, ROWLOCK, READPAST, INDEX(leases_expiry))
    WHERE expires IS NOT NULL AND expires <= SYSUTCDATETIME() AND id NOT IN (SELECT id FROM @c) ORDER BY expires;
-- A new statement: its clock is read after the candidates are locked.
UPDATE r SET owner = @owner, epoch = r.epoch + 1, expires = DATEADD(millisecond, @lease, SYSUTCDATETIME())
OUTPUT inserted.id, inserted.epoch INTO @claimed
FROM leases AS r JOIN @c AS c ON c.id = r.id
WHERE r.due <= SYSUTCDATETIME() AND (r.expires IS NULL OR r.expires <= SYSUTCDATETIME());
```

A renewal, guard or end in the caller's transaction:

```sql
SELECT id FROM leases WITH (UPDLOCK, ROWLOCK) WHERE id = @id;
UPDATE leases SET expires = DATEADD(millisecond, @lease, SYSUTCDATETIME())
OUTPUT inserted.epoch INTO @renewed
WHERE id = @id AND owner = @owner AND epoch = @epoch AND expires > SYSUTCDATETIME();
```

`READPAST` skips rows another transaction holds locked and never waits, under RCSI as well.

### Locks a claim holds

| Claim of 32 records | Locks held until commit |
|---|---|
| Index in the claim's order, no residual predicate | 64 key `U` (index and clustered key of each record), 3 page `IU` |
| Same index, a residual predicate that filters half the rows read | 96 key `U`: rows read and filtered out stay locked |
| No index in the claim's order (a sort) | table `X` (one per lock partition): every row read was locked and the locks escalated |

The startup verification of each source's indexes is therefore mandatory, as on PostgreSQL, and claim predicates
are kept in the index (filtered index predicates and key columns) so rows that are read and not claimed stay few:
they remain `U`-locked until the claim commits, and a guard of such a row waits for it.

## Fences and serialization points

PostgreSQL uses transaction-scoped advisory locks for the generation fence, the definition fence, limit scopes,
subject pauses and the schema migration lock. SQL Server's equivalent is `sp_getapplock` with
`@LockOwner = 'Transaction'`.

| Property | Result |
|---|---|
| Queueing | A holds `Shared`, B requests `Exclusive` and waits, C requests `Shared`: C waits behind B (timed out after 1 s) and B is granted when A commits. New shared requests queue behind a waiting exclusive one, which the generation and definition fences need. |
| Outside a transaction | Returns -999; the provider calls it only inside one. |
| Try-lock | `@LockTimeout = 0` returns -1 when busy, 0 or 1 when granted. |
| Savepoints | `ROLLBACK TRANSACTION <savepoint>` releases applocks and row locks taken after the savepoint, as PostgreSQL releases a subtransaction's locks. |
| `SET LOCK_TIMEOUT` | Expiry raises 1222 and ends the statement, not the transaction; rolling back to a savepoint and committing keeps earlier writes. This carries the planner's per-recurrence `lock_timeout` of 50 ms. |
| Resource names | `@Resource` is `nvarchar(255)`; longer names are truncated silently, so two names equal in their first 255 characters share a lock. Names are compared case-sensitively. |
| Hashing | The lock is keyed by a 32-bit hash of the name: of 200 000 random names, 6 merged into the lock of another name. A collision makes unrelated keys share a lock, which costs spurious waits; a deadlock needs two collisions taken in opposite orders. SQL Server's hash is not available, so fences are ordered by name, not by hash as on PostgreSQL. |
| Cost | Inside one batch: 1 lock 0.28 ms per call (mostly the round trip), 10 locks 0.38 ms, 100 locks 1.2 ms, 1 000 locks 9 ms, about 9 µs per lock. As its own round trip 0.18 ms against 0.16 ms for `SELECT 1`. Mac: 20 µs per lock. |

Lock names are fixed-length: a prefix and the SHA-256 of the framework schema and the key, so a long key never
truncates into another one:

```sql
N'cw|merge|' + CONVERT(nvarchar(64), HASHBYTES('SHA2_256', CONVERT(varbinary(800), @definition) + 0x0A + CONVERT(varbinary(800), @merge_key)), 2)
```

### Refusing a transaction

A fence refused for the transaction's isolation level must abort the caller's transaction, as `CWF01` does on
PostgreSQL. `THROW` alone ends the batch but leaves the transaction committable: the caller's commit succeeded and
kept its earlier write. Rolling back first, or `SET XACT_ABORT ON` before the `THROW`, aborts it: `@@TRANCOUNT` is 0
and `SqlTransaction.Commit` fails with "This SqlTransaction has completed; it is no longer usable":

```sql
IF (SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID) <> 2
BEGIN
    IF @@TRANCOUNT > 0 ROLLBACK;
    THROW 51001, N'Holder writes on superseding sources need READ COMMITTED isolation.', 1;
END
EXEC sp_getapplock @Resource = @fence, @LockMode = 'Shared', @LockOwner = 'Transaction';
```

This was tested as a batch. Inside a stored procedure a `ROLLBACK` also raises 266 (transaction count mismatch),
which the provider's error mapping has to expect if the fence becomes a procedure.

The rolled-back transaction cannot be kept doomed past the batch, and SqlClient (7.1.1) treats a `SqlTransaction` the
server ended as completed: `Commit` and `Rollback` throw, and a command given that transaction afterwards runs outside
any transaction, with `@@TRANCOUNT` 0, so a write the caller issues after ignoring the refusal commits on its own. On
PostgreSQL such a write fails in the aborted transaction. Every error that rolls the transaction back behaves the same
way, a deadlock victim (1205 or 51205) included. The provider accepts this: no write of the refused transaction commits and its
commit fails; callers must not continue on a connection after a refusal as if the transaction were still open.

### Per-row try-locks

PostgreSQL's claim tries each locked record's subject pause lock inside one statement
(`pg_try_advisory_xact_lock_shared` in the `WHERE`). `sp_getapplock` is a procedure and cannot be called inside a
statement, so the claim batch loops over the distinct subjects of its locked records, about 9 µs per subject:

```sql
DECLARE @i int = 0, @n int = (SELECT COUNT(*) FROM @subjects), @k nvarchar(255), @r int;
WHILE @i < @n
BEGIN
    SELECT @k = lock_name FROM @subjects WHERE ord = @i;
    EXEC @r = sp_getapplock @Resource = @k, @LockMode = 'Shared', @LockOwner = 'Transaction', @LockTimeout = 0;
    IF @r < 0 INSERT @busy VALUES (@i);
    SET @i += 1;
END
```

## Merging

PostgreSQL merges requests with `INSERT ... ON CONFLICT` on the partial unique index `jobs_merge (definition,
merge_key) WHERE merge_key IS NOT NULL AND state = 0`. SQL Server has filtered unique indexes with the same
definition. Variants were measured with 15 requesters and one completer that ends outstanding jobs, for 6–8 s each;
requests hold each identity once, in key order:

| Variant | 1 identity per request, 20 identities | 10 per request, 20 identities | 10 per request, 2 000 identities |
|---|---|---|---|
| `MERGE ... WITH (HOLDLOCK)` | 3 requests, 15 deadlocks | 105/s, 27 deadlocks | 56/s, 37 deadlocks |
| `UPDATE` then `INSERT ... WHERE NOT EXISTS` under `UPDLOCK, SERIALIZABLE` | 107/s, 56 deadlocks | 15/s, 57 deadlocks | 762/s, 25 deadlocks |
| Row first (below) without identity locks | 343/s, 5 deadlocks | 17/s, 51 deadlocks | 933/s, 0 deadlocks |
| **Identity locks, then row first** | **1 886/s, 0 deadlocks**, p99 26 ms | 129/s, 2 deadlocks (see below) | **1 671/s, 0 deadlocks**, p99 22 ms |

No variant created a second outstanding job of one identity: the filtered unique index guarantees it. The difference
is deadlocks.

- `MERGE ... WITH (HOLDLOCK)` read the clustered index under serializable range locks instead of seeking
  `jobs_merge`, so it serialized every request.
- The others deadlock on lock order across indexes. A request finds the outstanding job through `jobs_merge`, locking
  its index key, and then locks the row; a completion locks the row by primary key and then the `jobs_merge` key it
  removes. PostgreSQL has no such cycle because its row lock is on the tuple and index entries take no locks.
- Without a global order, requests of several identities still deadlock with each other: locks on existing jobs
  (taken in id order) and inserts (taken in key order) interleave.

The working design takes one exclusive applock per identity in merge-key order first, a global order like
PostgreSQL's sorted `ON CONFLICT` rounds. It then finds outstanding jobs without locks (an RCSI read), locks them by
primary key, re-evaluates them in the next statement, and inserts the rest. An insert that meets a job committed in
the meantime fails with 2601, and the round repeats:

```sql
SET XACT_ABORT OFF;
-- Identity locks in merge-key order (@items.ord follows that order).
WHILE @i < @n
BEGIN
    SELECT @lk = N'cw|merge|' + CONVERT(nvarchar(64), HASHBYTES('SHA2_256', CONVERT(varbinary(800), @def) + 0x0A + CONVERT(varbinary(800), merge_key)), 2)
    FROM @items WHERE ord = @i;
    EXEC sp_getapplock @Resource = @lk, @LockMode = 'Exclusive', @LockOwner = 'Transaction';
    SET @i += 1;
END
WHILE @round < 10
BEGIN
    DELETE @found;
    INSERT @found SELECT t.id, s.ord FROM @items AS s
        JOIN jobs AS t WITH (INDEX(jobs_merge)) ON t.definition = @def AND t.merge_key = s.merge_key AND t.merge_key IS NOT NULL AND t.state = 0
        WHERE NOT EXISTS (SELECT 1 FROM @done AS d WHERE d.ord = s.ord);
    SELECT @round = @round FROM jobs AS t WITH (UPDLOCK, ROWLOCK, FORCESEEK) JOIN @found AS f ON t.id = f.id;
    UPDATE t SET requests = t.requests + 1, due = CASE WHEN t.due < SYSUTCDATETIME() THEN t.due ELSE SYSUTCDATETIME() END
    OUTPUT s.ord, 'M' INTO @done
    FROM jobs AS t JOIN @found AS f ON t.id = f.id JOIN @items AS s ON s.ord = f.ord
    WHERE t.state = 0 AND t.part = s.part;
    BEGIN TRY
        INSERT jobs (definition, merge_key, state, part, due)
        SELECT @def, s.merge_key, 0, s.part, SYSUTCDATETIME() FROM @items AS s
        WHERE NOT EXISTS (SELECT 1 FROM @done AS d WHERE d.ord = s.ord) AND NOT EXISTS (SELECT 1 FROM @found AS f WHERE f.ord = s.ord)
        ORDER BY s.merge_key;
        INSERT @done SELECT s.ord, 'I' FROM @items AS s
        WHERE NOT EXISTS (SELECT 1 FROM @done AS d WHERE d.ord = s.ord) AND NOT EXISTS (SELECT 1 FROM @found AS f WHERE f.ord = s.ord);
    END TRY
    BEGIN CATCH
        IF ERROR_NUMBER() NOT IN (2601, 2627) THROW;
    END CATCH
    IF NOT EXISTS (SELECT 1 FROM @items AS s WHERE NOT EXISTS (SELECT 1 FROM @done AS d WHERE d.ord = s.ord)) BREAK;
    SET @round += 1;
END
```

A completion takes no identity lock: it locks the job's row and then its index entry, and a request holding the
identity lock waits for that row and then sees the job ended, so the two never wait for each other in a cycle. The 2
deadlocks left with 10 hot identities per request came from the test's completer, which ended 5 jobs in one
statement in random order: it and a request locked rows of the same jobs in different orders. Hence the rule for the
provider: **every statement that locks several rows of one table locks them in primary-key order**, and the order of
indexes within one row is the same on every path. Cancellation, chain operations and recurrence operations follow it.

Further results:

- A rerun that makes a terminal job outstanding while another job holds its identity fails with 2601, which maps to
  "identity held" as SQLSTATE 23505 does on PostgreSQL.
- `IGNORE_DUP_KEY` (the `DO NOTHING` analogue) is refused on filtered indexes (10618). A persisted computed column
  that is the merge key while outstanding and a per-row value otherwise allows an unfiltered unique index with
  `IGNORE_DUP_KEY`, but it deadlocked like the others and is not used.
- Filtered indexes need `ANSI_NULLS`, `ANSI_PADDING`, `ANSI_WARNINGS`, `CONCAT_NULL_YIELDS_NULL` and
  `QUOTED_IDENTIFIER` on and `NUMERIC_ROUNDABORT` off for writes to the table (an insert with `QUOTED_IDENTIFIER OFF`
  failed with 1934). SqlClient sessions have them; `ARITHABORT` is off in SqlClient and implied by `ANSI_WARNINGS`. A
  query with `state = @s` as a parameter still used `jobs_merge`.
- Batch input: a fan-out of 50 000 new subjects took **68 s** when the multi-statement batch read the table-valued
  parameter directly, with or without `OPTION (RECOMPILE)`; with the parameter first copied into a local table
  variable (`DECLARE @items request_items; INSERT @items SELECT * FROM @p;`) it took **0.33 s**, and through `OPENJSON`
  into a table variable 0.35 s (PostgreSQL: 0.7 s). Batch input is JSON read by `OPENJSON` (decision "No temporary
  objects"). The identity locks add about 9 µs per item, about 0.45 s at 50 000; that sum is
  estimated from the lock cost and still to be measured end to end.

## Signals

SQL Server has no `LISTEN/NOTIFY`. Measured with 4 listening connections, 100–300 signals each sent in a transaction
that also updates a row, 5 ms apart:

| | Service Broker | Signals table |
|---|---|---|
| Commit to delivery | p50 0.6 ms, p99 1.1 ms | poll 10 ms: p50 5.0 ms, p99 10.0 ms; poll 50 ms: p50 25 ms, p99 50 ms |
| Delivered | all | all |
| Rollback, rollback to a savepoint | not delivered | not delivered |
| Short transactions per second, 8 clients, without a signal 6 550 | 804 (`SEND` to 4 listeners over shared dialogs) | 6 716 |
| Listener round trips, 10 ms poll, 4 listeners, about 3 s | blocking `WAITFOR (RECEIVE)` | 735 (one per wake or per 2 s) |
| Every process of the application | one queue and service per process, created at runtime; a send goes to each | every process reads the one table |
| Azure SQL Database | not available | available |

The signals table: a send is an insert in the sending transaction, so it becomes visible exactly when that
transaction commits and disappears with a rollback, including a rollback to a savepoint:

```sql
CREATE TABLE work.signals (rv rowversion NOT NULL, topic varchar(64) NOT NULL, k nvarchar(512) NOT NULL);
CREATE UNIQUE CLUSTERED INDEX signals_rv ON work.signals (rv);

INSERT work.signals (topic, k) VALUES (@topic, @key);
```

A `rowversion` is assigned at write time, not at commit, so a reader that took every row above its last position
would pass over a row whose transaction commits later. `MIN_ACTIVE_ROWVERSION()` is the lowest rowversion of any open
transaction in the database: rows below it are committed or gone. The listener keeps its position and runs one
batch that returns as soon as committed rows exist below the watermark, or after 2 s:

```sql
DECLARE @until datetime2 = DATEADD(second, 2, SYSUTCDATETIME()), @top binary(8);
WHILE 1 = 1
BEGIN
    SET @top = MIN_ACTIVE_ROWVERSION();
    IF EXISTS (SELECT 1 FROM work.signals WHERE rv > @last AND rv < @top) OR SYSUTCDATETIME() > @until BREAK;
    WAITFOR DELAY '00:00:00.010';
END;
SELECT rv, topic, k FROM work.signals WHERE rv > @last AND rv < @top ORDER BY rv;
```

Against [signals](signals.md):

1. On commit, never after a rollback: met.
2. Every process of the application: met. Each application has its own framework schema and so its own table.
3. Lossy, without order: met, and the table loses less than the contract allows. A listener's position survives a
   reconnection; rows are pruned after a retention, and a listener that falls behind the oldest row resyncs.
4. `Resync()` on subscription and reconnection: the listener starts at the current watermark and resyncs.
5. Callbacks on the listener: unchanged.

What it costs against `NOTIFY`: wake latency is the poll interval, and a connection per process sits in the
server-side loop. And an open transaction that has written any `rowversion` column in the database (a signal, or an
application table with a row-version concurrency token, as EF Core maps them) holds the watermark back, so signals
committed after it are delivered only when it ends. They are delayed, never lost. Measured: with an open transaction
that sent a signal first, 0 later committed signals were readable until it committed; with an open application update
of another `rowversion` table, the signal committed after it was held back. The poll interval and the retention are
provider options; pruning is a maintenance statement.

## Statement translation

| PostgreSQL (uses in the provider) | SQL Server |
|---|---|
| `clock_timestamp()` (127) | `SYSUTCDATETIME()` in a statement that starts after the lock statement. Statements that rely on PostgreSQL re-reading the clock after a lock wait inside one statement (`ON CONFLICT DO UPDATE`, the `WaitAsync` mark, single-statement transitions) are split into lock and evaluation. `datetime2(7)` has 100 ns precision. |
| `now()` as a stable index bound and claim cutoff | A variable set from `SYSUTCDATETIME()` at the start of the batch. |
| `FOR UPDATE` (55) / `SKIP LOCKED` (36) | `WITH (UPDLOCK, ROWLOCK)` / `WITH (UPDLOCK, ROWLOCK, READPAST)`, on an index in the claim's order. |
| `RETURNING` (36) | `OUTPUT` to the client on framework tables. `OUTPUT` without `INTO` is refused on tables with enabled triggers (334), and lease sources are application tables: their rows are read again after the update. |
| `unnest` arrays (26) | A JSON parameter read by `OPENJSON` with the key columns' exact types. |
| `ON CONFLICT` (22) | [Merging](#merging): identity applocks, row first, `TRY/CATCH` on 2601. For `occurrence` uniqueness the same pattern, with the recurrence's lock as the identity lock. |
| `'infinity'` (19) | `9999-12-31T23:59:59.9999999` as a sentinel. `DATEADD` past it fails (517), so additions to a value that can be the sentinel are guarded. |
| `pg_notify` (12) | An insert into the signals table, conditional inserts from `INSERT ... SELECT` over the matched rows. |
| Data-modifying CTEs (6) | Several statements in one batch, passing sets as JSON text in variables. |
| `COLLATE "C"` | `Latin1_General_100_BIN2` on key columns. |
| `interval` columns | `bigint` ticks. |
| `uuid` | `uniqueidentifier` (see the decision on identifiers). |
| `NULLS NOT DISTINCT` unique constraints | A plain unique index: SQL Server treats NULLs as equal in unique indexes (a second NULL failed with 2601). PostgreSQL's default (`NULLS DISTINCT`) needs a filtered index `WHERE col IS NOT NULL`. |
| Partial indexes | Filtered indexes: simple comparisons, `IS [NOT] NULL` and `IN`, no `OR`, no functions; the session options above. |
| `INCLUDE` indexes | Supported. |
| `hashtextextended` advisory keys | Fixed-length names with SHA-256, ordered by name. |
| `pg_try_advisory_xact_lock_shared` inside a statement | A T-SQL loop with `@LockTimeout = 0` (see [per-row try-locks](#per-row-try-locks)). |
| `set_config('jit', 'off', true)` | Not applicable. Plans are cached per statement text. |
| `lock_timeout` in a savepoint | `SET LOCK_TIMEOUT 50`, error 1222, rollback to the savepoint. |
| `SQLSTATE 23505` | Errors 2601 (unique index) and 2627 (unique constraint). |
| Deadlock detection | Error 1205; the victim's transaction is rolled back. A victim waiting for an applock gets `sp_getapplock` result -3 instead, without an error and with its transaction still open, and the other transaction keeps waiting; the provider's batch then raises 51205, which rolls the transaction back (`XACT_ABORT`). 51205 means only this; other refused applocks raise 51206. The provider retries its own transactions after either error; in the caller's transaction the error reaches the caller. |

## Rules for the provider

- Require RCSI at startup; check the caller's isolation level once per transaction where read committed is required.
- Lock in one statement, evaluate in the next; no condition that depends on a lock is evaluated inside the statement
  that waits for it.
- Claim and settlement scans read an index in their order without locks and lock each row they keep through its key
  with `READPAST`; rows are locked through the key before any other index on every path.
- Every statement that locks several rows of a table locks them in primary-key order.
- Merge identities are serialized with exclusive applocks in merge-key order before any row lock.
- Fences, limit scopes and subject pauses are applocks with fixed-length names; several of them are taken in name
  order.
- A refused fence rolls back before it throws.
- No table variables or temporary tables; sets pass as JSON text read by `OPENJSON` with the key columns' exact types.
- `OUTPUT` to the client on framework tables only.
- Framework tables disable lock escalation; verification disables it on source tables.
- A command cancelled by its token surfaces as `OperationCanceledException`, through the execution helpers in `Tsql`.
- A refused applock raises 51205 when SQL Server chose it as a deadlock victim (-3), 51206 otherwise.
- Deadlock victims (1205 and 51205) in provider transactions are retried.

## Findings while building the provider

- Filtered indexes cannot compare two columns: `WHERE lease_expires_at IS NOT NULL AND settled_epoch < lease_epoch`
  fails with 10735. Sources that settle therefore carry the persisted computed column `lease_outstanding` (decision
  "Outstanding leases"), and their settlement and partition indexes lead with it under `WHERE lease_expires_at IS NOT
  NULL`; the owner index carries it. With 50 000 expired, settled, not reclaimed leases in 200 000 rows, the
  settlement scan, next expiry, partition and owner counts each seek these indexes (2022, provider statements).
- A locking scan (`UPDLOCK, ROWLOCK, READPAST`) keeps the `U` locks of rows it reads and filters out when the filter is
  evaluated outside the seek: a key lookup or a join (`NOT EXISTS`/`NOT IN` over a table variable) keeps them, a simple
  predicate on a column the index carries releases them. Of 1 000 due rows, 32 claimed: 95 key locks with the filter
  column in the table only, 64 with it included in the index, 88 with a `NOT IN` over a table variable. Claim scans keep
  their predicates on index columns; exclusions by a set (saturated or busy partitions, stale pauses) leave the
  rows they skip locked until the claim commits.
- SET options inside a parameterized SqlClient command (run through `sp_executesql`) are restored when the batch ends
  (`XACT_ABORT`, `LOCK_TIMEOUT` checked): batches set `NOCOUNT` and `XACT_ABORT` themselves without changing a caller's
  session. A command without parameters runs as a plain batch, whose SET options persist, so every provider command
  has at least one parameter.

- Rows are locked through their key, before any other index, on every path. A locking scan of a secondary index
  (`UPDLOCK, ROWLOCK, READPAST` on the expiry index, then a lookup of the row) locks the index entry first and then
  waits for the row when another transaction holds it, which deadlocked against a claim that held the row and
  updated that index entry (reproduced in the racing-settlement scenario). Claim and settlement scans therefore read
  the index without locks through a `DYNAMIC READ_ONLY` cursor and lock each row they find with a single-row
  `UPDLOCK, ROWLOCK, READPAST` statement on the key, which also evaluates the row's latest committed version. A
  set-based form (`TOP (@n)` over the unlocked index joined to the locked key) was rejected: on small tables the
  optimizer sorts after the join, even with `LOOP JOIN` and `FAST 1`, and then locks every candidate row (900 of
  1 000); at 200 000 rows it kept the index order. The cursor reads only as far as needed whatever plan it gets: a
  claim of 32 among 1 000 due rows holds 32 key locks. The cursor is declared `DYNAMIC`: a `FORWARD_ONLY READ_ONLY`
  cursor becomes fast-forward, whose plan read every matching row when it opened (2.8 s for the 250 000 due jobs of a
  definition at 10M jobs, against under 1 ms dynamic). A fetch and its row lock take 0.08 ms at 1M jobs and 0.18 ms
  at 10M on the validation machine described under [Measurements](#measurements).
- Statements over a set of keys (`@keys`) join it to the table with `INNER LOOP JOIN ... WITH (INDEX(<key>),
  FORCESEEK)`. Without it, an update of a few keys on a small table scanned the table and waited for every row other
  claims held, and two such claims deadlocked on each other's rows.
- A query whose predicate implies a filtered index's filter reads the filter's column from the table unless the index
  carries it, so a scan of `<due> WHERE <expires> IS NULL` looks up each row it passes; an index hint on a filtered
  index whose filter the query does not imply fails with 8622.
- In an index without a unique key of its own, rows of equal key value are ordered by the clustered key, where
  PostgreSQL's B-tree orders them by heap position; claims of stale records through the generation index therefore
  reach records of one generation in key order, not in insertion order.
- Batch input (request items, key sets, id lists) is a JSON parameter read with `OPENJSON`, not a framework table type:
  measured 0.35 s against 0.33 s for a table-valued parameter at 50 000 items, and no schema type has to be versioned.
  `OPENJSON ... WITH (a varbinary(max))` decodes base64 values.
- The signals table is a framework table: a database is migrated before clients run, including clients that are
  never started, since lease operations signal in their statements.
- `Microsoft.Data.SqlClient` 7.1.1, like every earlier version, depends on `System.Security.Cryptography.Pkcs` below the
  versions that fix CVE-2026-84008 and CVE-2026-70162 (9.0.18 for 7.1.1); the provider references 10.0.13 directly, so
  restore resolves the fixed version.
- A locking read (`UPDLOCK`) that a covering secondary index serves locks only that index's entries, not the rows: a
  writer that updates the row through its key does not wait for it. Tests that hold rows as a concurrent writer would
  lock them with an update through the key.
- A source's first operation in a process reads its table in the catalog and verifies it as startup does: statements
  bind key parameters to the key columns' exact types and name the source's indexes in `INDEX()` hints, so a source
  without its indexes is refused by every operation, not only at startup.
- Character columns that `OPENJSON ... WITH` returns take the database's default collation, so comparing them with
  framework columns fails with 468 when it differs from `Latin1_General_100_BIN2`; key columns are read with the key
  collation. They are also read with the key column's exact type: with a wider one (`nvarchar(max)`) a seek on a
  two-column key became a range seek (`GetRangeWithMismatchedTypes`) with the second column as a residual predicate,
  so claims took `U` locks on other rows and waited for them (900 s of `LCK_M_U` in a run, failed scenarios).
- `SYSUTCDATETIME()` advances in steps of about 4 ms on Linux (32 distinct values in 20 000 reads), so rows created
  within one step share `created_at`. Orders that must be total add the id.
- Table variables and temporary tables are created in tempdb's system tables on every execution of a batch run through
  `sp_executesql`, which caches no temporary objects. With table variables in every provider batch, under the
  conformance suite's concurrency on 2025 (16 cores, six tempdb data files), `PAGELATCH_EX`/`PAGELATCH_SH` on
  tempdb's system pages dominated the waits: a full run of the SQL Server tests created 119 873 temporary tables and
  had 1.48 million `PAGELATCH` waits (966 s); job claim passes took 20 ms at p50 and 614 ms at p99, and timing-bound
  scenarios failed. Memory-optimized tempdb metadata (`ALTER SERVER CONFIGURATION SET MEMORY_OPTIMIZED
  TEMPDB_METADATA = ON`, after a restart) brought that to 23 000 waits (43 s), p50 13 ms and p99 155 ms. Without
  temporary objects (decision "No temporary objects"), at the default tempdb configuration, the same run created 472
  temporary tables on 2025 and 388 on 2022, with 23 500 `PAGELATCH` waits (14 s) and 25 100 (9 s), and every
  scenario passed. A micro-benchmark of a claim-shaped batch (32 keys from `OPENJSON`, an `UPDATE` with `OUTPUT`, 16
  clients, 2022 and 2025 alike) isolates the cost of the temporary objects themselves:

  | Batch | Default tempdb | Memory-optimized tempdb metadata |
  |---|---|---|
  | Three table variables | 1 450–1 550/s, p50 10 ms | 2 530/s, p50 5 ms |
  | One table variable | 4 000–4 400/s, p50 3.6–4.0 ms | |
  | No temporary object (inline `OPENJSON`, `OUTPUT` to the client) | 12 400–13 300/s, p50 1.2 ms | 12 500/s |
  | The three table variables in a stored procedure | 10 900–11 000/s, p50 1.4 ms | 10 400/s |
  | Memory-optimized table types | 11 200–11 700/s, p50 1.3 ms | 11 500/s |

  One client alone: 570 batches/s with three table variables, 2 075/s without. At 64 clients the setting raised the
  table-variable batch from 1 470/s to 2 540/s while the batch without temporary objects ran 13 700–14 700/s: the
  setting relieves the latch waits but not the cost of creating the objects.
- A SQL authentication login costs about twice as much on 2025 as on 2022 (about 80 ms against 37 ms per `sqlcmd`
  login on an idle container, with `PREEMPTIVE_OS_CRYPTOPS` and `PREEMPTIVE_OS_AUTHENTICATIONOPS` among 2025's top
  waits under the suite), and under the suite's load on 2025 a new connection took 80–830 ms (about 170 per run over
  50 ms, since every test class has a database and so a pool of its own). SqlClient's default pool serves an
  asynchronous open that finds no idle connection from one queue, one request at a time, so every open waits for the
  logins ahead of it: opens that then received an idle connection still waited up to 430 ms, and
  `J29_WaitsRegisteringWhileJobsCompleteAreWokenBySignals` exceeded its 400 ms wake bound in about half of the full
  runs, although signals were delivered within 50 ms of the commit. With the channel-based pool
  (`UseConnectionPoolV2`) a login delays only the open that makes it: the largest wake lag of that scenario fell from
  433 ms to 37 ms, and eight full runs in a row passed.
- SqlClient reports a command that its cancellation token stopped as a `SqlException` ("Operation cancelled by user",
  or "A severe error occurred on the current command"). The provider runs commands and reads results through helpers
  that report it as an `OperationCanceledException` of that token, as core loops and callers expect of a cancellation.
- A read that PostgreSQL serves from one snapshot of several statements (a chain with its steps and groups) is one
  statement on SQL Server, since RCSI gives each statement its own snapshot and snapshot isolation is not required of
  the database: the chain's steps and groups come back as `FOR JSON` columns of the chain's row. The unserved report
  and the monitoring gauges loop over a loose index scan in one batch (recursive CTEs refuse `TOP` and subqueries in
  the recursive part), so each step of the loop reads its own snapshot.
- A correlated `MIN` over a filtered index aggregates the whole index range and looks up every row to recheck the
  filter's column (the unserved report's first version probe: 1.56M logical reads, 977 ms at 1M jobs); `TOP (1) ...
  ORDER BY` in index order seeks once (84 reads, under 1 ms). Probes and joins of the framework batches name their
  index: without a hint the optimizer scanned `jobs` for the calendar branch of the unserved report (2 s at 1M), the
  recurrence probes of planning (2 s at 10M, with no recurrence stored) and the chain steps of a cancellation (6.9 s
  at 10M), because the table variable's estimate and the filtered index's predicate left it no seek.
- SQL Server sizes its buffer pool to the host (80% of memory by default) and keeps it. With a container per test
  process, `dev/check` on a 30 GiB host left 258 MB free and swapped, and SQL Server 2025 stalled: logins timed out
  after 14 s and timing scenarios failed. The test processes therefore share one server (`CORFEDO_WORK_SQLSERVER`),
  which `dev/check` starts; free memory then stayed above 8.6 GB. A 2 GiB memory limit per container instead made
  the suite twice as slow and failed 45 scenarios.

## Measurements

Validation on an AMD Ryzen 7 7700X with 30 GiB, SQL Server 2022 in a container limited to 8 GiB, default tempdb (no
memory-optimized metadata), the jobs of the PostgreSQL measurements in [providers](providers.md): 10 definitions,
70% terminal, 25% due, 5 000 outstanding leases per definition. Times are whole calls from the client unless a
statement is named; statement times are `sys.dm_exec_query_stats` averages.

| Operation | 1M jobs | 10M jobs | PostgreSQL, 1M |
|---|---|---|---|
| Claim pass of a definition (16 jobs, limits, fence, expiry scan, update) | 9.9 ms p50, 13.2 ms p99 | 20.4 ms p50 | scan statements 0.8 ms |
| Claim scan, per job (cursor fetch and row lock) | 0.08 ms | 0.18 ms | |
| Definition-limit count | 0.86 ms | 0.92 ms | 0.3 ms |
| Terminal transition (statements) | 2.0 ms | | |
| Merging request, own transaction | 1.7 ms | 4.8 ms | 0.1 ms (statement) |
| `RequestManyAsync` of 50 000 new subjects | 2.5 s | 8.8 s | 0.7 s |
| Status page of 500 subjects | 5.3 ms | 8.1 ms | 6 ms |
| Retention of 1 000 jobs of a definition | 30 ms (delete 12 ms, locking scan 16 ms) | 145 ms | 7.5–9.4 ms |
| Unserved report | 5.7 ms | 7.2 ms | 1.3 ms |
| Signal, commit to wake | p50 5.1 ms, p99 10.2 ms | | |
| Commits/s at 8 clients without / with a signal | 6 662 / 6 084 | | |

- The 1M values of claim passes, merging requests, `RequestManyAsync`, the status page and the unserved report are
  measured without temporary objects; the 10M column and the other rows with table variables in every batch. The
  same 1M run with table variables: claim passes 17.4 ms p50 and 31.7 ms p99, terminal transitions (whole call)
  18.5 ms against 5.6 ms, merging requests 4.8 ms, status page 7.8 ms, unserved report 7.3 ms. `RequestManyAsync` of
  50 000 took 2.47 s with table variables: its insert reads `OPENJSON`, whose fixed estimate of 50 rows gives a plan
  that maintains each index row by row, where the table variable's row count gave one that sorts each index's rows
  first (225 ms against 142 ms, three times the logical reads, at 10 000 items). The sorted plan needs a plan compiled
  for a large batch: a table variable's plan is compiled for the batch that runs it first and is reused, so after a
  small batch it maintains the indexes row by row as well.
- The narrow insert plan stays (owner, 2026-11-03). SQL Server 2022 and 2025 estimate 50 rows for `OPENJSON`. At 1M
  jobs, with the request rolled back, the whole call took 510–534 ms for 10 000 new subjects and 2.51–2.54 s for
  50 000 (insert statement 228 ms and 1.19–1.25 s, 1.29M logical reads). A table variable with `OPTION (RECOMPILE)`
  sorts each index (insert 169 ms and 0.82–0.85 s, half the reads) but the call took 507–555 ms and 2.37–2.40 s, with
  a tempdb object and a compile per batch, and a single item 9–12 ms against 2–5 ms. Without temporary objects no
  variant gained: trace flag 8790 (per-index plan without sorts, sysadmin only) 530 ms and 2.58 s; nested `OPENJSON`
  arrays, whose estimates multiply, sorted in a parallel plan whose sorts spilled to tempdb, a single item 18 ms; under
  `TOP (@count)` with `OPTION (RECOMPILE)` 501–559 ms and 2.43–2.65 s, a single item 12–14 ms; `ORDER BY` the id no
  gain. The cost is the per-row maintenance of the job indexes (about 25 logical reads per job), which the plan shape
  only reorders.
- Fragmentation of the `uniqueidentifier` keys (ids sort randomly in SQL Server's order): `jobs` at 1M is 99%
  fragmented with pages 66% full (87 064 pages against 58 797 after a rebuild), at 10M 99% and 67% (862 000 pages).
  It costs space, not speed: claim passes 17.7 against 16.9 ms, merging requests 4.9 against 4.7 ms and fan-outs
  2.38 against 2.57 s, fragmented against rebuilt; after a rebuild 150 000 new jobs split 54 000 pages. The id type
  stays.
- Identity locks of a 50 000-item fan-out: 50 000 `sp_getapplock` calls of 0.002 ms, about 0.1 s of the 2.5 s.
- Soak, 10 minutes at 10M jobs: 12 clients requesting into 200 hot identities per definition, cancelling them and
  fanning out 50 at a time, beside 4 job definitions running 16 at a time (10% failing) and a reconciler converging
  2 000 records that the clients request and invalidate: 143 251 request transactions, 30 922 cancellations, 241 916
  job runs, 6 264 invalidations and 184 617 reconcile runs. No error reached a caller, no deadlock was recorded, no
  identity had two outstanding jobs, no terminal job held a lease and no soak job held an expired lease, and every
  record converged within 5 s of the clients stopping.

## Effort and risks

The PostgreSQL provider is 8 154 lines. The SQL Server provider has the same structure and operations; it grows by
splitting statements into lock and evaluation, the T-SQL loops for fences and subject try-locks, the merge rounds, the
signals table with its listener and pruning, the RCSI and isolation checks, lock escalation settings and verification,
deadlock retries and its schema scripts in T-SQL. Estimate: 9 000–11 000 lines, 1.1–1.35 times the PostgreSQL
provider, plus a conformance provider and provisioning on `Testcontainers.MsSql` like the PostgreSQL tests.

Conformance scenarios at risk, to be watched first:

- Requests racing completions and cancellations of the same identities (lock order across indexes and rows).
- Every operation that locks several rows: cancellation by subject, chains, `TriggerAsync`, `RemoveAsync`,
  retention.
- Claims next to large application writes on a source table (lock escalation; refused at startup unless disabled).
- Wake latency in provider tests (poll interval instead of `NOTIFY`).
- Large fan-outs and planner shapes for variable-size input.
- Anything that assumes ids sort by time.
- Planning with savepoints and lock timeouts.

Core changes: none to the public contract. One internal change is needed: a provider slot on `WorkTransaction` to
record the isolation check. Deadlock victims in the caller's transaction surface as `SqlException` 1205 or 51205,
with no framework error type, so retrying execution strategies see the provider's error (owner, 2026-11-03).

Not verified yet: Azure SQL Database and Managed Instance. `Testcontainers.MsSql` runs on the Mac in the colima
profile above; the cost of identity locks and the page splits of `uniqueidentifier` keys are in
[Measurements](#measurements).
