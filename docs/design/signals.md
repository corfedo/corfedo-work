# Signals

**Status:** Accepted and built in [stage 7](implementation.md#stage-7-steps).

Signals wake a process when committed state it waits for may have changed. They are a layer of their own, beside the
store provider, that the other layers and applications use. This document is the decision record for that layer.

## Context

R7 asks for prompt wake-up with polling as the correctness fallback. Today the store owns signals:

- `IWorkStore.Signals` is an `IWorkSignals` with one operation, `ListenAsync(received, connected)`, or null for a store
  without signals. A `WorkSignal` is one of five framework kinds (`Due`, `Plan`, `Ended`, `Monitor`, `Cancel`), a source
  name and, for `Cancel`, an owner.
- Sending has no contract. The PostgreSQL provider formats payloads in `PostgreSqlSignalPayload` and sends them from
  about 35 places: `pg_notify` inside its statements, several of them conditional on the rows a statement matches or
  with a payload computed from a row (the holder named in a cancellation), and `NotifyAsync` calls in the operation's
  transaction. The in-memory provider queues `WorkSignal`s on its transaction. The only public send is
  `ILeaseStore.SignalDueAsync`, which the EF Core wake-up interceptor uses.
- Each `ListenAsync` call listens on its own connection: the host's dispatch and the job waiter each hold one, so a
  process that waits for jobs listens twice.

Applications need the same mechanism for work that is not a CW source, and cannot use it: the kinds are closed, and the
host consumes every signal. Corfedo's platform is the first case. It hand-rolls three wake-ups beside CW, and one of
them has no wake-up at all: its AI execution queue (a Wolverine PostgreSQL queue) polls every 100 ms. On the idle
public demo that poll was about 30 statements per second and, with the connection resets each poll's
connection costs, most of the database's 110 commits per second.

## Decision

### The layer

```
 Leases, Reconciliation, Jobs   wait for due records, planning, ended jobs, monitoring, cancellation
 Applications                   wait for their own state
   │ subscribe and send through
 Signals                        commit-time wake hints on topics, one listener per process
   │ carried by
 Store provider                 the transport: NOTIFY on PostgreSQL, the transaction's commit in memory
```

A signal is a **topic** and a **key**. It says that state named by the key may have changed; it carries no data.

| Term | Meaning |
|---|---|
| Topic | A registered name of what changed: lowercase letters, digits, `.`, `-` and `_`, at most 64 characters. Topics starting with `work.` are the framework's. |
| Key | Which record or group of records, as the topic defines it: a source name, an owner and a source, a domain key. At most 1 024 UTF-8 bytes. May be empty. |
| Subscriber | A local callback for one topic: `Received(key)` for each signal, `Resync()` when signals may have been missed. |

**Contract,** checked by the conformance suite for every provider with signals, and by provider tests where a rule
depends on the transport's connection:

1. A signal sent in a transaction is delivered after that transaction commits and not at all if it rolls back,
   including a rollback to a savepoint taken before the send. A signal sent without a transaction is delivered when
   the send returns.
2. Delivery reaches every subscriber of the topic in every process of the same application (R19), the sender's
   process included, and no other application.
3. Signals may be lost: while a listener is disconnected, when a process crashes after commit, under overload. Signals
   with the same topic and key sent in one transaction may arrive as one. A subscriber may receive a signal for a
   change it cannot see yet or no longer sees. There is no order across transactions.
4. After its subscription starts listening, and after every reconnection, each subscriber gets `Resync()`. A
   subscriber reads the state it waits for after its first `Resync()`, so a change committed before it listened is
   read, and a change after is signalled.
5. Callbacks run on the listener and must not block: they mark work and return, as a dispatcher's wake does. A
   callback that throws is logged; the other subscribers still receive the signal and the listener stays connected.

So a subscriber treats a signal as "read again", never as the change itself, and still polls. Durable delivery of
data remains the application's outbox or messaging; signals are not message delivery, which stays out of scope.

**API,** in the signals layer of `Corfedo.Work`, which depends only on the contracts, so it works without the layers
above it, also without the framework schema:

```csharp
public readonly record struct SignalTopic(string Name);           // validated on construction

public interface IWorkSignals                                     // one per application in a process
{
    ValueTask SendAsync(IWorkTransaction transaction, SignalTopic topic, string key, CancellationToken cancellationToken);
    ValueTask SendAsync(SignalTopic topic, string key, CancellationToken cancellationToken);
    IDisposable Subscribe(SignalTopic topic, ISignalSubscriber subscriber);
    IWorkTransaction Transaction(DbTransaction transaction);       // the transport's, for sends without a store
}

public interface ISignalSubscriber
{
    void Received(string key);
    void Resync();
}
```

`IWorkSignals` is a process-wide hub over the provider's transport: it listens on one connection while it has
subscribers, and dispatches by topic in process. One listening loop runs at a time: the first subscription starts it,
disposing the last stops it, and a subscription made while it stops keeps it running or starts it again once the
previous connection is closed, so a process never holds two listening sessions and no subscriber misses its
`Resync()`. A subscription made while the listener is connected gets `Resync()` at once; one made before the
connection gets it when the connection is established. The provider contract shrinks to the transport:

```csharp
public interface ISignalTransport
{
    ValueTask SendAsync(IWorkTransaction? transaction, SignalTopic topic, string key, CancellationToken cancellationToken);
    Task ListenAsync(Action<SignalTopic, string> received, Action connected, CancellationToken cancellationToken);
    IWorkTransaction Transaction(DbTransaction transaction);
}
```

`IWorkStore.Signals` returns the transport, or null as today. Registration adds the hub with the store
(`AddPostgreSqlWorkStore`, `AddInMemoryWorkStore`); `AddPostgreSqlSignals(dataSource, application)` registers it alone,
for applications that use signals and no other layer. Sends join `WorkTransaction.Of(dbTransaction)` or, with EF Core,
`DatabaseFacade.WorkTransaction()`, the same handle work operations take.

### Framework topics

The five kinds become topics, defined in `WorkTopics` in `Abstractions`, since the providers' statements send them and
the providers depend only on `Abstractions`. The host, the layers' background loops and the job waiter become
subscribers of the hub:

| Topic | Key | Subscriber |
|---|---|---|
| `work.due` | source | the source's dispatcher wakes |
| `work.plan` | job source | the definition's planner |
| `work.ended` | job source | the job waiter rereads that definition's waited jobs |
| `work.monitor` | job source | the monitoring loop |
| `work.cancel` | `<owner>:<source>`, the owner's 36-character id first | the holder's dispatcher checks cancellation when the owner is its process |

The host subscribes to `work.due` and `work.cancel` and finds the dispatcher by the key's source. It no longer passes
signals to the background loops: `IWorkBackground.Signal` is removed, and a background loop subscribes in `Start`
through the hub on `LeaseDispatchContext` and disposes its subscription in `StopAsync`. Job maintenance subscribes to
`work.plan` and ignores other sources' keys; the monitoring loop subscribes to `work.monitor`. `Resync()` replaces the
null signal they received on (re)connection.

`WorkSignalKind`, `WorkSignal` and `PostgreSqlSignalPayload` are removed. `ILeaseStore.SignalDueAsync` is replaced by
sending `work.due` through the hub. Every send site in both providers goes through the layer, so the payload format
exists in one place per provider. The job waiter subscribes through the hub instead of listening on its own
connection.

### PostgreSQL

- One channel per application, `corfedo_work:<schema>` as today; the payload is the topic, a space and the key. One
  `LISTEN` per process. Topics are filtered in process: a channel per topic would run into the 63-byte identifier
  limit and need a `LISTEN` per subscribed topic.
- The channel name is at most 63 bytes, so the schema (or the application of `AddPostgreSqlSignals`) is at most 50
  bytes. The store and the transport refuse a longer one on construction: `pg_notify` refuses a longer channel, which
  would fail every notifying commit, while `LISTEN` truncates it and would listen on another channel.
- `SendAsync` with a transaction runs `pg_notify` in it; without one, on its own connection.
- Statements of the provider that signal keep sending from inside the statement, as today, through one internal SQL
  builder (`PostgreSqlSignalSql`) that produces `pg_notify(channel, topic || ' ' || key)` for a key expression. A
  signal that depends on the rows a statement matches stays in that statement, at no extra round trip.
- PostgreSQL delivers one notification for identical payloads on one channel in one transaction, which is rule 3's
  merging.
- The listening connection is a dedicated session (transaction-mode poolers need a direct connection for it, as
  today), never holds a transaction open, probes a quiet connection every 30 seconds and reconnects after 1 second.
  A listener inside a long transaction would hold back the notification queue, and once the queue is full, notifying
  commits fail.

### In memory

`InMemoryTransaction` keeps queuing signals and delivers them to the application's hub on commit; a savepoint keeps
the signals queued before it. A send without a transaction is delivered at once. The hub calls `Resync()` once on
subscription.

### Sending cost

Any notification serializes the commits of notifying transactions: on PostgreSQL 18.6 with 8 clients, single-row
transactions commit at 16 300 per second without one and 4 040 with one; a second notification in a transaction that
already notifies costs nothing ([providers](providers.md#postgresql)). The layer's documentation states the rule the
framework already follows: send on transitions somebody waits for (for jobs, only the ends a caller waits for), at most
once per topic and key per transaction, and never as a per-row log of changes.

## Expected state

| | Today | After |
|---|---|---|
| Who can send and listen | the providers' own statements; applications only `SignalDueAsync` | framework and application topics through one API |
| Listening connections per process | one per `ListenAsync` (host, job waiter) | one per application |
| Payload format | five kinds formatted in about 35 provider sites | topic and key, formatted by one builder per provider |
| Missed-signal recovery | `connected` wakes every dispatcher | `Resync()` per subscriber, also on subscription |

The conformance scenarios that rely on signals (L11, L12, C-11, the waiting scenarios of J-*, monitoring wake-ups)
keep passing unchanged. New scenarios:

| Scenario | Situation | Expected |
|---|---|---|
| S-1 | A signal is sent in a committed transaction, another in a rolled-back one, a third before a savepoint that is rolled back to. | Subscribers of the topic in two processes receive the first and third; nobody receives the second. |
| S-2 | A signal is sent without a transaction. | Delivered once the send returns. |
| S-3 | Two applications share a database and use the same topic and key. | Each receives only its own. |
| S-4 | The same topic and key are sent several times in one transaction. | At least one delivery, at most as many as sent. |
| S-5 | A subscriber subscribes, then state changes. | It gets `Resync()` before any signal it could miss; the change is signalled. |
| S-6 | The listening connection is broken: the backend is terminated. PostgreSQL provider test. | Every subscriber gets `Resync()` after reconnection; later signals arrive. |
| S-7 | A host with dispatchers and a job waiter run in one process. PostgreSQL provider test. | One `LISTEN` session in `pg_stat_activity`. |
| S-8 | A topic or key outside the limits is sent or subscribed. | Refused with `ArgumentException`; nothing is sent. |

S-6 and S-7 depend on the transport's connection and have no in-memory counterpart, so they are tests of the
PostgreSQL provider rather than conformance scenarios; the conformance fixture needs no hooks to break a listener or
count sessions. S-1 needs savepoints in the fixture's application transaction (`IConformanceTransaction`).

## Alternatives

- **Keep signals inside the store.** Applications keep hand-rolling `LISTEN` or polling, and every listener opens
  its own connection.
- **Wake through the outbox or a message queue.** Durable, but the queue itself has to be polled or woken, which is
  the problem; and a wake-up needs no durability, since polling covers a lost one.
- **Signals carrying data.** Turns a lossy hint into a message bus without delivery guarantees. Applications that need
  the data use their outbox and send a signal so the consumer reads it promptly.
- **Operations returning their signals as data, sent by the layer after the statement.** It would let a transport
  outside the store's database send them after commit, but costs a round trip on claims, ends and settlements, whose
  signals depend on the rows a statement matches. Not taken while every transport is the store's own.
- **A separate transport such as Redis.** The contract (rules 3 and 4) admits a transport that publishes after commit:
  a crash before publishing loses a signal, which polling covers, and a publish for a rolled-back change is a
  spurious wake. It conflicts with the design rule that everything runs against the store the application already
  uses, and needs the alternative above. Reconsidered when a deployment needs transaction-mode pooling for every
  connection, or when measured `NOTIFY` commit serialization limits a workload.
- **A channel per topic.** See [PostgreSQL](#postgresql).
