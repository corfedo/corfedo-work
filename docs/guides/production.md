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

The process registry, dispatch pools, priorities and shutdown are described in the
[hosting design](../design/hosting.md).
