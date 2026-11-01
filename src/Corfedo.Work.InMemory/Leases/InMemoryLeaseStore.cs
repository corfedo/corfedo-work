using System.Collections.Concurrent;
using Corfedo.Work.InMemory.Database;
using Microsoft.Extensions.Logging;

namespace Corfedo.Work.InMemory.Leases;

/// <summary>
/// Lease operations on application tables. Every operation runs in one serialized transaction, so the record it
/// evaluates is the latest committed version and its clock reading is taken after the transaction began.
/// </summary>
internal sealed partial class InMemoryLeaseStore(InMemoryWorkDatabase database, ApplicationState application, ILogger logger, WorkTelemetryOptions telemetry) : ILeaseStore
{
    private readonly ConcurrentDictionary<object, object> rules = new();

    internal InMemoryWorkDatabase Database => database;

    internal ApplicationState Application => application;

    internal DateTimeOffset Now => database.Clock.GetUtcNow();

    internal LeaseRules<TKey> Rules<TKey>(LeaseSource<TKey> source)
        where TKey : notnull =>
        (LeaseRules<TKey>)rules.GetOrAdd(source, static (s, store) => new LeaseRules<TKey>((LeaseSource<TKey>)s, store.Database, store.Application), this);

    internal InMemoryTransaction Join(IWorkTransaction transaction) => InMemoryTransaction.From(transaction, database);

    internal void Signal(InMemoryTransaction transaction, SignalTopic topic, string key) => transaction.Signal(application, topic, key);

    /// <summary>Writes <paramref name="changes"/> to the record, which exists.</summary>
    internal Record Write<TKey>(InMemoryTransaction transaction, LeaseRules<TKey> rules, RowKey key, Record record, IEnumerable<KeyValuePair<string, object?>> changes)
        where TKey : notnull
    {
        var written = record.With(changes, database.NextVersion());
        transaction.Put(rules.Rows, key, written);
        return written;
    }

    internal static KeyValuePair<string, object?> Set(string field, object? value) => new(field, value);
}
