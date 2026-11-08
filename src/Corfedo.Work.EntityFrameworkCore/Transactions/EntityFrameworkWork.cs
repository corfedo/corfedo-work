using Microsoft.EntityFrameworkCore;
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

    /// <summary>
    /// Runs the context's commands in <paramref name="transaction"/>, a transaction Work began, such as the one a claim
    /// hook, an expiry handler or an outcome hook receives, so the context's changes commit or roll back with it. The
    /// context must not have opened a connection of its own yet; a context from the callback's scope has not. The
    /// context neither commits nor closes the transaction's connection.
    /// </summary>
    /// <exception cref="ArgumentException">The transaction is not an ADO.NET transaction, as on the in-memory store.</exception>
    public static async Task UseWorkTransactionAsync(this DatabaseFacade database, WorkTransaction transaction, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(database);
        var ado = transaction switch
        {
            DbWorkTransaction work => work.Transaction,
            null => throw new ArgumentNullException(nameof(transaction)),
            _ => throw new ArgumentException("The transaction is not an ADO.NET transaction.", nameof(transaction)),
        };
        var connection = ado.Connection ?? throw new InvalidOperationException("The transaction is no longer active.");
        if (!ReferenceEquals(database.GetDbConnection(), connection))
        {
            database.SetDbConnection(connection, contextOwnsConnection: false);
        }
        await database.UseTransactionAsync(ado, cancellationToken);
    }
}
