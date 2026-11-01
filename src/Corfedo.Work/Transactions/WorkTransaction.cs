using System.Data.Common;
using System.Runtime.CompilerServices;

namespace Corfedo.Work;

/// <summary>
/// A handle to the application's active transaction. Operations that take it commit or roll back with the
/// application's own changes. An ADO.NET transaction, such as an <c>NpgsqlTransaction</c>, converts to it implicitly;
/// the in-memory store's transactions are handles themselves. The store an operation runs on checks that the
/// transaction belongs to its provider.
/// </summary>
public abstract class WorkTransaction
{
    internal WorkTransaction()
    {
    }

    /// <summary>The handle of <paramref name="transaction"/>, the same for every conversion of it.</summary>
    public static implicit operator WorkTransaction(DbTransaction transaction) => DbWorkTransaction.Of(transaction);
}

/// <summary>The handle of an ADO.NET transaction.</summary>
internal sealed class DbWorkTransaction : WorkTransaction
{
    private static readonly ConditionalWeakTable<DbTransaction, DbWorkTransaction> Handles = new();

    private int savepoints;

    private DbWorkTransaction(DbTransaction transaction) => Transaction = transaction;

    public DbTransaction Transaction { get; }

    /// <summary>
    /// The handle of <paramref name="transaction"/>. Every call for the same transaction returns the same handle, so
    /// savepoint names stay unique however many times it is converted.
    /// </summary>
    public static DbWorkTransaction Of(DbTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        return Handles.GetValue(transaction, static t => new DbWorkTransaction(t));
    }

    /// <summary>A savepoint name unique within the transaction.</summary>
    public string NextSavepoint() => "corfedo_work_" + Interlocked.Increment(ref savepoints);
}
