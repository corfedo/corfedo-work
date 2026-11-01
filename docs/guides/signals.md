# Signals

Signals wake processes when committed state they wait for may have changed. The framework uses them to wake its
workers, and your `BackgroundService`s can use them alone. `AddPostgreSqlSignals` creates no tables and needs no other
registration.

- **Sent at commit.** A signal sent in a transaction is delivered when it commits and never after a rollback.
- **One connection per process.** All subscriptions of a process share one listening connection. The listener starts
  with the first subscription.
- **Reconnects are covered.** After it starts listening and after every reconnection, each subscriber gets
  `Resync()`, so it rereads its state instead of trusting that nothing was missed.
- **Hints, not messages.** A signal is a topic and a key, without data. Signals may be merged or lost, so a subscriber
  reads its state again on every signal and keeps a slow poll as fallback.

```csharp
// Processes registered with the same name hear each other; applications sharing a database use different names.
services.AddPostgreSqlSignals(dataSource, "orders-app");

static readonly SignalTopic Pending = new("orders.pending");

// Send in the transaction that makes the change.
await using var tx = await db.Database.BeginTransactionAsync(ct);
db.Orders.Add(order);
await db.SaveChangesAsync(ct);
await signals.SendAsync(db.Database.WorkTransaction(), Pending, order.TenantId, ct);
await tx.CommitAsync(ct);

// Subscribe for the topic. The callbacks only record the wake; the loop reads the state.
sealed class OrderWorker(IWorkSignals signals) : BackgroundService, ISignalSubscriber
{
    private readonly Channel<bool> wake = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });

    public void Received(string key) => wake.Writer.TryWrite(true);   // every key of the topic

    public void Resync() => wake.Writer.TryWrite(true);

    protected override async Task ExecuteAsync(CancellationToken stop)
    {
        using var subscription = signals.Subscribe(Pending, this);
        while (!stop.IsCancellationRequested)
        {
            await ProcessPendingOrdersAsync(stop);
            using var poll = CancellationTokenSource.CreateLinkedTokenSource(stop);
            poll.CancelAfter(TimeSpan.FromSeconds(30));
            try
            {
                await wake.Reader.ReadAsync(poll.Token);   // a signal, a resync, or the fallback poll
            }
            catch (OperationCanceledException) when (!stop.IsCancellationRequested)
            {
            }
        }
    }
}
```

With Npgsql directly, send with `WorkTransaction.Of(npgsqlTransaction)`.

PostgreSQL commits transactions that send notifications one at a time. Send a signal when something a subscriber waits
for changes, once per topic and key in a transaction, not for every row. Listening needs a session-level connection;
see [production](production.md).

The full semantics are in the [signals design](../design/signals.md).
