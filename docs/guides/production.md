# Production

- **Schema.** Framework tables live in their own schema per application, `work` by default. By default the host
  creates and upgrades them at startup; deployments whose serving logins may not change the schema migrate in a
  separate step, below. Several applications can share a database.
- **Your tables.** Lease and reconciliation columns live on your tables and come from your migrations. The EF Core
  package maps them, and startup verifies them.
- **Replicas and roles.** Every process claims only up to its free capacity. Different roles register different
  work. Requests for work that no live process runs wait for it and are reported as unserved; a monitoring process
  opens an incident when such work waits longer than `UnservedGrace` (5 minutes by default).
- **Rolling deployments.** Definitions carry revisions: shared policy comes from the newest stored revision, and older
  processes never claim work requested with argument versions they cannot read.
- **Connections.** Work uses your data source. Besides your own needs, size its pool for: up to 12 connections for
  Work's background passes (dispatch, expiry settlement and job maintenance, each on four workers however many
  sources and definitions you register), one for the signal listener and one for the process heartbeat, and up to the
  sum of your sources' capacities for running work, whose renewals, ends and handlers use connections.
- **Connection poolers.** Work runs through any data source. Listening for signals needs a session-level connection,
  so behind PgBouncer in transaction mode, give the store or `AddPostgreSqlSignals` a direct data source.
- **Shutdown.** Running work gets a grace period. Work still held after it is released for another process.
- **At least once.** A run can repeat after a crash. Writes to the same database are fenced; external effects, such
  as emails or HTTP calls, are deduplicated with the run's stable execution id.
- **Telemetry.** `ActivitySource` and `Meter` named `Corfedo.Work`, compatible with OpenTelemetry. Payloads and error
  messages are not exported unless you enable it. Attempts that fail, time out or retry are logged as `job.failed`
  with their reason code; `work.RunHistory()` records every attempt of every definition that does not opt out. See [monitoring](../design/monitoring.md) for expectations and
  incidents.

## SQL Server

- **Committed snapshots.** The database must read committed snapshots, or the store refuses to start:
  `ALTER DATABASE app SET READ_COMMITTED_SNAPSHOT ON`.
- **Lock escalation.** A claim skips locked rows, but not a table lock, so your source tables must not escalate
  locks. A store that migrates at startup runs `ALTER TABLE ... SET (LOCK_ESCALATION = DISABLE)` on each table that
  does, once, waiting at most one second for the schema lock. If it cannot, or it verifies its schema, it refuses to
  start and names the statement for a DBA to run.
- **Permissions.** A store that migrates at startup needs a login that may create the schema, or owns one created
  for it, and create tables in it. On your source tables the login needs `SELECT`, `UPDATE` and `VIEW DEFINITION`,
  and `ALTER` while lock escalation is still enabled. A store that verifies its schema never alters a table: disable
  escalation in your migrations.
- **Connection pool.** Turn on SqlClient's channel-based pool, which keeps a slow login from holding up every open
  queued behind it. A login took 80–830 ms under load on SQL Server 2025, about twice as long as on 2022; with the
  default pool, opens that then got an idle connection still waited up to 430 ms. Set `Min Pool Size` when work
  arrives in bursts, so the pool keeps logins out of the burst.

  ```xml
  <ItemGroup>
    <RuntimeHostConfigurationOption Include="Switch.Microsoft.Data.SqlClient.UseConnectionPoolV2" Value="true" />
  </ItemGroup>
  ```

## A separate migrate step

When your processes run under logins that may not change the schema, apply the framework schema in your migrate step,
under a login that may create the schema or owns one created for it, and name the roles your runtime logins use:

```csharp
await PostgreSqlWorkSchema.MigrateAsync(ownerDataSource, "work", runtimeRoles: ["app_web", "app_worker"], ct);
```

Then register the store to verify the schema instead of applying it:

```csharp
services.AddPostgreSqlWorkStore("work", WorkSchemaMode.Verify);   // the container's data source, with a runtime login
```

- **Grants.** Each runtime role gets what every process needs on the framework schema and nothing else, and loses
  any other privilege it held there. Running the step again changes nothing, so run it on every deployment: tables a
  release adds get their grants in the same step.
- **Start-up check.** A verifying process refuses to start when the schema is missing or older than its release, or
  its login lacks a privilege, and names what is missing.
- **Rolling deployments.** Migrate first, then roll out. Processes of the previous release keep running against the
  newer schema.
- **SQL files.** `PostgreSqlWorkSchema.Script("work", ["app_web", "app_worker"])` returns the same migration as one
  statement, for tools that run SQL files. It applies only what is missing.
- **Your tables.** The runtime logins also need `SELECT`, `INSERT`, `UPDATE` and `DELETE` on your source tables,
  from your own grants.

On SQL Server use `SqlServerWorkSchema` and `AddSqlServerWorkStore(..., schemaMode: WorkSchemaMode.Verify)`. Runtime
users also need `VIEW DEFINITION` on your source tables, since SQL Server shows index filters only with it.

The process registry, dispatch pools, priorities and shutdown are described in the
[hosting design](../design/hosting.md).
