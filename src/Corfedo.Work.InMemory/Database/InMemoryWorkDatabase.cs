using Corfedo.Work.InMemory.Database;
using Corfedo.Work.InMemory.Jobs;
using Corfedo.Work.InMemory.Monitoring;

namespace Corfedo.Work.InMemory;

/// <summary>
/// An in-memory database for tests in one process: application tables that hold domain records with their lease and
/// reconciliation fields, the framework state of each application that registers a store on it, and the store's clock.
/// Transactions are serialized: one transaction is open at a time, and the next waits until it commits or rolls back.
/// Reads outside a transaction see committed records.
/// </summary>
public sealed class InMemoryWorkDatabase
{
    private readonly SemaphoreSlim gate = new(1, 1);
    private readonly Dictionary<StoreLocation, DomainTable> tables = [];
    private readonly Dictionary<string, ApplicationState> applications = new(StringComparer.Ordinal);
    private long versions;
    private long commits;

    /// <summary>A database on the system clock.</summary>
    public InMemoryWorkDatabase()
        : this(TimeProvider.System)
    {
    }

    /// <summary>A database whose store clock is <paramref name="clock"/>, such as an <see cref="InMemoryClock"/>.</summary>
    public InMemoryWorkDatabase(TimeProvider clock)
    {
        ArgumentNullException.ThrowIfNull(clock);
        Clock = clock;
    }

    /// <summary>The store's clock: every time the store compares or records comes from it.</summary>
    public TimeProvider Clock { get; }

    /// <summary>Guards committed state for readers without the transaction gate and for commits.</summary>
    internal Lock Sync { get; } = new();

    /// <summary>Transactions committed so far.</summary>
    internal long CommittedTransactions => Interlocked.Read(ref commits);

    /// <summary>
    /// Creates an application table whose records are keyed by <paramref name="key"/> and carry
    /// <paramref name="fields"/>, such as a source's lease and reconciliation fields and the application's own.
    /// </summary>
    /// <exception cref="InvalidOperationException">The table exists.</exception>
    public void CreateTable(StoreLocation table, IReadOnlyList<string> key, IEnumerable<string> fields)
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(fields);
        var created = new DomainTable(table, key, fields);
        lock (Sync)
        {
            if (!tables.TryAdd(table, created))
            {
                throw new InvalidOperationException($"Table {table} exists.");
            }
        }
    }

    /// <summary>Begins a transaction, waiting until no other transaction is open.</summary>
    public async ValueTask<InMemoryTransaction> BeginAsync(CancellationToken cancellationToken = default)
    {
        await gate.WaitAsync(cancellationToken);
        return new InMemoryTransaction(this);
    }

    /// <summary>The committed records of a table, in key order.</summary>
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Read(StoreLocation table)
    {
        var found = Table(table);
        lock (Sync)
        {
            return [.. found.Rows.Committed.OrderBy(r => r.Key).Select(r => r.Value.Values)];
        }
    }

    /// <summary>The committed record with the key field values <paramref name="key"/>, or null.</summary>
    public IReadOnlyDictionary<string, object?>? Find(StoreLocation table, IReadOnlyList<object> key)
    {
        var found = Table(table);
        lock (Sync)
        {
            return found.Rows.Committed.GetValueOrDefault(new RowKey(key))?.Values;
        }
    }

    internal DomainTable Table(StoreLocation table)
    {
        ArgumentNullException.ThrowIfNull(table);
        lock (Sync)
        {
            return tables.TryGetValue(table, out var found) ? found : throw new InvalidOperationException($"Table {table} does not exist.");
        }
    }

    internal DomainTable? FindTable(StoreLocation table)
    {
        lock (Sync)
        {
            return tables.GetValueOrDefault(table);
        }
    }

    /// <summary>The framework state of an application, created on first use with its job and incident tables.</summary>
    internal ApplicationState Application(string name)
    {
        lock (Sync)
        {
            if (!applications.TryGetValue(name, out var application))
            {
                var jobs = new DomainTable(StoreLocation.Table(name, "jobs"), [JobColumns.Id], JobColumns.All);
                var incidents = new DomainTable(StoreLocation.Table(name, "incidents"), [IncidentColumns.Id], IncidentColumns.All);
                foreach (var table in new[] { jobs, incidents })
                {
                    if (tables.ContainsKey(table.Location))
                    {
                        throw new InvalidOperationException($"Table {table.Location} is a framework table of application {name}; create application tables elsewhere.");
                    }
                }
                tables.Add(jobs.Location, jobs);
                tables.Add(incidents.Location, incidents);
                applications.Add(name, application = new ApplicationState(name, jobs, incidents));
            }
            return application;
        }
    }

    internal long NextVersion() => Interlocked.Increment(ref versions);

    /// <summary>Reads committed state under the lock that commits take.</summary>
    internal T ReadCommitted<T>(Func<T> read)
    {
        lock (Sync)
        {
            return read();
        }
    }

    internal void Committed(IEnumerable<ITableChanges> changes)
    {
        lock (Sync)
        {
            foreach (var change in changes)
            {
                change.Apply();
            }
        }
        Interlocked.Increment(ref commits);
    }

    internal void Release() => gate.Release();
}
