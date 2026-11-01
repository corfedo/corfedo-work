namespace Corfedo.Work.InMemory.Database;

/// <summary>
/// A table of immutable rows. Only a transaction that holds the database's gate changes the committed rows, under the
/// database's lock, so readers without the gate copy what they need under that lock.
/// </summary>
internal sealed class Table<TKey, TRow>
    where TKey : notnull
    where TRow : class
{
    public Dictionary<TKey, TRow> Committed { get; } = [];
}

/// <summary>A transaction's pending writes to one table; a null row deletes the record.</summary>
internal interface ITableChanges
{
    void Apply();

    ITableChanges Clone();
}

internal sealed class TableChanges<TKey, TRow>(Table<TKey, TRow> table) : ITableChanges
    where TKey : notnull
    where TRow : class
{
    public Dictionary<TKey, TRow?> Writes { get; private init; } = [];

    public void Apply()
    {
        foreach (var (key, row) in Writes)
        {
            if (row is null)
            {
                table.Committed.Remove(key);
            }
            else
            {
                table.Committed[key] = row;
            }
        }
    }

    public ITableChanges Clone() => new TableChanges<TKey, TRow>(table) { Writes = new(Writes) };
}
