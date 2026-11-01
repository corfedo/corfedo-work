# Documentation

## Guides

How to use Corfedo Work.

| Guide | Contents |
|---|---|
| [Getting started](guides/getting-started.md) | Packages, registration and the first request in your transaction. |
| [Transactions](guides/transactions.md) | Joining your EF Core or Npgsql transaction, and the rules. |
| [Jobs](guides/jobs.md) | Definitions, revisions, handlers, schedules, merging and results. |
| [Leases](guides/leases.md) | Long work on rows you own: columns, key codecs, guarded writes, ending, waiting and cancellation. |
| [Reconciliation](guides/reconciliation.md) | Rows that describe what should exist, and runs that make it so. |
| [Signals](guides/signals.md) | Commit-time wake-ups for your own background services. |
| [Testing](guides/testing.md) | The in-memory store with a controllable clock. |
| [Production](guides/production.md) | Schema, replicas, deployments, poolers, shutdown and telemetry. |
| [Trimming and Native AOT](guides/native-aot.md) | Which packages support it, and source-generated JSON for job arguments and results. |

## Design

How it works and why: the guarantees each layer gives and the measurements behind them, for contributors and for
readers who want the full semantics.

| Document | Contents |
|---|---|
| [Problems](design/problems.md) | The kinds of durable work an application has and the requirements each one places on a framework. |
| [Overview](design/overview.md) | Layers, terminology, packages and design rules. |
| [Leases](design/leases.md) | Due times and claimability, guarded commits, claim hooks, takeovers, expiry settlement, cancellation, limits, remote holders. |
| [Reconciliation](design/reconcile.md) | Revision-based requests, supersession, mass invalidation and convergence on domain records. |
| [Jobs](design/jobs.md) | Definitions and revisions, merging, schedules, occurrences, attempts, timeouts, results, operator actions and common patterns. |
| [Calendars](design/calendars.md) | Named availability rules for when work may start. |
| [Chains](design/chains.md) | Steps, run conditions, groups, dynamic fan-out with fan-in, results, recurring chains, retry from a step. |
| [Domain records and jobs](design/domain-and-jobs.md) | Where work state lives, outcome hooks, status by subject and external effects. |
| [Monitoring](design/monitoring.md) | Expectations, incidents and telemetry. |
| [Signals](design/signals.md) | Commit-time wake hints on topics for the framework and applications. |
| [Hosting](design/hosting.md) | Registration, startup, processes, dispatch, shutdown and testing. |
| [Store providers](design/providers.md) | The provider contract and its required guarantees, transactions, schema ownership, conformance. |
| [Scenarios](design/scenarios.md) | Behaviour every layer and provider must show; the basis of the conformance suite. |
| [Decisions](design/decisions.md) | Decisions taken and those still open. |
| [Implementation](design/implementation.md) | Build order and status. |
