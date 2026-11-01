using System.Data.Common;
using System.Runtime.CompilerServices;

namespace Corfedo.Work;

/// <summary>
/// A handle to the application's active transaction. Operations that take it commit or roll back with the
/// application's own changes. <see cref="WorkTransaction.Of"/> wraps an ADO.NET transaction; the in-memory store
/// creates its own.
/// </summary>
public interface IWorkTransaction;

/// <summary>
/// The application's active ADO.NET transaction, for work operations that join it. The store an operation runs on
/// checks that the transaction belongs to its provider.
/// </summary>
public sealed class WorkTransaction : IWorkTransaction
{
    private static readonly ConditionalWeakTable<DbTransaction, WorkTransaction> Handles = new();

    private int savepoints;

    private WorkTransaction(DbTransaction transaction) => Transaction = transaction;

    /// <summary>The wrapped transaction.</summary>
    public DbTransaction Transaction { get; }

    /// <summary>
    /// Wraps the application's active transaction, such as an <c>NpgsqlTransaction</c>. Every call for the same
    /// transaction returns the same handle, so savepoint names stay unique however many times it is wrapped.
    /// </summary>
    public static WorkTransaction Of(DbTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        return Handles.GetValue(transaction, static t => new WorkTransaction(t));
    }

    /// <summary>A savepoint name unique within the transaction.</summary>
    internal string NextSavepoint() => "corfedo_work_" + Interlocked.Increment(ref savepoints);
}
