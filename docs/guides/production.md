# Production

- **Schema.** Framework tables live in their own schema per application, `work` by default. The host creates and
  upgrades them at startup. Several applications can share a database.
- **Your tables.** Lease and reconciliation columns live on your tables and come from your migrations. The EF Core
  package maps them, and startup verifies them.
- **Replicas and roles.** Every process claims only up to its free capacity. Different roles register different
  work. Requests for work that no live process runs wait for it and are reported as unserved.
- **Rolling deployments.** Definitions carry revisions: shared policy comes from the newest stored revision, and older
  processes never claim work requested with argument versions they cannot read.
- **Connection poolers.** Work runs through any data source. Listening for signals needs a session-level connection,
  so behind PgBouncer in transaction mode, give the store or `AddPostgreSqlSignals` a direct data source.
- **Shutdown.** Running work gets a grace period. Work still held after it is released for another process.
- **At least once.** A run can repeat after a crash. Writes to the same database are fenced; external effects, such
  as emails or HTTP calls, are deduplicated with the run's stable execution id.
- **Telemetry.** `ActivitySource` and `Meter` named `Corfedo.Work`, compatible with OpenTelemetry. Payloads and error
  messages are not exported unless you enable it. See [monitoring](../design/monitoring.md) for expectations and
  incidents.

## SQL Server

- **Committed snapshots.** The database must read committed snapshots, or the store refuses to start:
  `ALTER DATABASE app SET READ_COMMITTED_SNAPSHOT ON`.
- **Lock escalation.** A claim skips locked rows, but not a table lock, so your source tables must not escalate
  locks. At startup the store runs `ALTER TABLE ... SET (LOCK_ESCALATION = DISABLE)` on each table that does, once,
  waiting at most one second for the schema lock. If it cannot, it refuses to start and names the statement for a
  DBA to run.
- **Permissions.** The application's login creates and upgrades the framework schema at startup, so it needs to
  create the schema, or own one created for it, and create tables in it. On your source tables it needs `SELECT` and
  `UPDATE`, and `ALTER` while lock escalation is still enabled.
- **Connection pool.** Turn on SqlClient's channel-based pool, which keeps a slow login from holding up every open
  queued behind it. A login took 80–830 ms under load on SQL Server 2025, about twice as long as on 2022; with the
  default pool, opens that then got an idle connection still waited up to 430 ms. Set `Min Pool Size` when work
  arrives in bursts, so the pool keeps logins out of the burst.

  ```xml
  <ItemGroup>
    <RuntimeHostConfigurationOption Include="Switch.Microsoft.Data.SqlClient.UseConnectionPoolV2" Value="true" />
  </ItemGroup>
  ```


The process registry, dispatch pools, priorities and shutdown are described in the
[hosting design](../design/hosting.md).
