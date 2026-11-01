using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace Corfedo.Work;

/// <summary>Adapters between a <c>DbContext</c>'s transactions and work operations.</summary>
public static class EntityFrameworkWork
{
    /// <summary>
    /// Wraps the context's current transaction for work operations and signals that join it, so they commit or roll
    /// back with the context's changes. The context and the store must share a database.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// The context has no active transaction; the work would otherwise commit ahead of the domain changes.
    /// </exception>
    public static WorkTransaction WorkTransaction(this DatabaseFacade database)
    {
        ArgumentNullException.ThrowIfNull(database);
        var current = database.CurrentTransaction
            ?? throw new InvalidOperationException("The DbContext has no active transaction; begin one with Database.BeginTransactionAsync so the work commits with its changes.");
        return current.GetDbTransaction();
    }
}
