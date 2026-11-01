using Corfedo.Work.InMemory.Database;

namespace Corfedo.Work.InMemory;

/// <summary>
/// A transaction of an <see cref="InMemoryWorkDatabase"/>, and the handle work operations join. Domain test doubles
/// read and write application tables through it; its writes, the framework's included, are visible to it at once and
/// to everyone else when it commits. Disposing it without a commit rolls it back.
/// </summary>
public sealed class InMemoryTransaction : IWorkTransaction, IDisposable
{
    private Dictionary<object, ITableChanges> changes = [];
    private List<(ApplicationState Application, SignalTopic Topic, string Key)> signals = [];
    private bool ended;

    internal InMemoryTransaction(InMemoryWorkDatabase database) => Database = database;

    public InMemoryWorkDatabase Database { get; }

    /// <summary>Inserts a record into an application table.</summary>
    /// <exception cref="InMemoryUniqueViolationException">A record with the same key exists.</exception>
    public void Insert(StoreLocation table, IReadOnlyDictionary<string, object?> values)
    {
        ArgumentNullException.ThrowIfNull(values);
        var found = Database.Table(table);
        found.CheckFields(values.Keys);
        var key = found.KeyOf(values);
        if (Get(found.Rows, key) is not null)
        {
            throw new InMemoryUniqueViolationException(table, key.ToString());
        }
        Put(found.Rows, key, new Record(new Dictionary<string, object?>(values, StringComparer.Ordinal), Database.NextVersion()));
    }

    /// <summary>The record with the key field values <paramref name="key"/> as this transaction sees it, or null.</summary>
    public IReadOnlyDictionary<string, object?>? Find(StoreLocation table, IReadOnlyList<object> key) =>
        Get(Database.Table(table).Rows, new RowKey(key))?.Values;

    /// <summary>Changes fields of the record with the key field values <paramref name="key"/>; false if it does not exist.</summary>
    public bool Update(StoreLocation table, IReadOnlyList<object> key, IReadOnlyDictionary<string, object?> changes)
    {
        ArgumentNullException.ThrowIfNull(changes);
        var found = Database.Table(table);
        found.CheckFields(changes.Keys);
        if (changes.Keys.Any(f => found.Key.Contains(f, StringComparer.Ordinal)))
        {
            throw new ArgumentException("Key fields cannot change.", nameof(changes));
        }
        var rowKey = new RowKey(key);
        if (Get(found.Rows, rowKey) is not { } record)
        {
            return false;
        }
        Put(found.Rows, rowKey, record.With(changes, Database.NextVersion()));
        return true;
    }

    /// <summary>Deletes the record with the key field values <paramref name="key"/>; false if it does not exist.</summary>
    public bool Delete(StoreLocation table, IReadOnlyList<object> key)
    {
        var found = Database.Table(table);
        var rowKey = new RowKey(key);
        if (Get(found.Rows, rowKey) is null)
        {
            return false;
        }
        Remove(found.Rows, rowKey);
        return true;
    }

    /// <summary>The records of an application table as this transaction sees them, in key order.</summary>
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Read(StoreLocation table) =>
        [.. Scan(Database.Table(table).Rows).OrderBy(r => r.Key).Select(r => r.Row.Values)];

    /// <summary>Makes the transaction's writes visible and delivers its signals.</summary>
    public void Commit()
    {
        Active();
        ended = true;
        try
        {
            Database.Committed(changes.Values);
        }
        finally
        {
            Database.Release();
        }
        foreach (var (application, topic, key) in signals)
        {
            application.Deliver(topic, key);
        }
    }

    /// <summary>Discards the transaction's writes and signals.</summary>
    public void Rollback()
    {
        Active();
        ended = true;
        Database.Release();
    }

    public void Dispose()
    {
        if (!ended)
        {
            Rollback();
        }
    }

    /// <summary>The transaction behind a handle of this provider that is still open.</summary>
    internal static InMemoryTransaction From(IWorkTransaction transaction, InMemoryWorkDatabase database)
    {
        var found = transaction switch
        {
            InMemoryTransaction memory => memory,
            null => throw new ArgumentNullException(nameof(transaction)),
            _ => throw new ArgumentException("The transaction was not created by the in-memory provider.", nameof(transaction)),
        };
        if (found.Database != database)
        {
            throw new ArgumentException("The transaction belongs to another in-memory database.", nameof(transaction));
        }
        found.Active();
        return found;
    }

    internal void Active()
    {
        if (ended)
        {
            throw new InvalidOperationException("The transaction is no longer active.");
        }
    }

    internal TRow? Get<TKey, TRow>(Table<TKey, TRow> table, TKey key)
        where TKey : notnull
        where TRow : class
    {
        Active();
        if (changes.TryGetValue(table, out var pending) && ((TableChanges<TKey, TRow>)pending).Writes.TryGetValue(key, out var written))
        {
            return written;
        }
        // Only the transaction holding the gate changes committed rows, so reading them needs no lock.
        return table.Committed.GetValueOrDefault(key);
    }

    internal void Put<TKey, TRow>(Table<TKey, TRow> table, TKey key, TRow row)
        where TKey : notnull
        where TRow : class => Changes(table).Writes[key] = row;

    internal void Remove<TKey, TRow>(Table<TKey, TRow> table, TKey key)
        where TKey : notnull
        where TRow : class => Changes(table).Writes[key] = null;

    /// <summary>Every record of the table as this transaction sees it, in no particular order.</summary>
    internal IEnumerable<(TKey Key, TRow Row)> Scan<TKey, TRow>(Table<TKey, TRow> table)
        where TKey : notnull
        where TRow : class
    {
        Active();
        var writes = changes.TryGetValue(table, out var pending) ? ((TableChanges<TKey, TRow>)pending).Writes : null;
        foreach (var (key, row) in table.Committed)
        {
            if (writes is null || !writes.ContainsKey(key))
            {
                yield return (key, row);
            }
        }
        if (writes is not null)
        {
            foreach (var (key, row) in writes)
            {
                if (row is not null)
                {
                    yield return (key, row);
                }
            }
        }
    }

    /// <summary>Delivers <paramref name="signal"/> to the application's listeners when the transaction commits.</summary>
    internal void Signal(ApplicationState application, SignalTopic topic, string key)
    {
        Active();
        signals.Add((application, topic, key));
    }

    /// <summary>The transaction's state, for rolling back the writes and signals made after it.</summary>
    internal Savepoint Save()
    {
        Active();
        return new Savepoint(changes.ToDictionary(c => c.Key, c => c.Value.Clone()), [.. signals]);
    }

    internal void RollbackTo(Savepoint savepoint)
    {
        Active();
        changes = savepoint.Changes.ToDictionary(c => c.Key, c => c.Value.Clone());
        signals = [.. savepoint.Signals];
    }

    private TableChanges<TKey, TRow> Changes<TKey, TRow>(Table<TKey, TRow> table)
        where TKey : notnull
        where TRow : class
    {
        Active();
        if (!changes.TryGetValue(table, out var pending))
        {
            changes[table] = pending = new TableChanges<TKey, TRow>(table);
        }
        return (TableChanges<TKey, TRow>)pending;
    }

    internal sealed record Savepoint(Dictionary<object, ITableChanges> Changes, List<(ApplicationState Application, SignalTopic Topic, string Key)> Signals);
}

/// <summary>An insert found a record with the same key in the table.</summary>
public sealed class InMemoryUniqueViolationException(StoreLocation table, string key)
    : Exception($"Table {table} has a record with the key ({key}).")
{
    public StoreLocation Table { get; } = table;
}

/// <summary>Adapters between in-memory transactions and work operations.</summary>
public static class InMemoryWork
{
    /// <summary>The in-memory transaction behind a handle, for hooks and test doubles that bind their data access to it.</summary>
    public static InMemoryTransaction InMemory(this IWorkTransaction transaction) => transaction switch
    {
        InMemoryTransaction memory => memory,
        null => throw new ArgumentNullException(nameof(transaction)),
        _ => throw new ArgumentException("The transaction was not created by the in-memory provider.", nameof(transaction)),
    };
}
