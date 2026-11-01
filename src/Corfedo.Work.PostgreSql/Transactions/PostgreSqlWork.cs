using Npgsql;

namespace Corfedo.Work.PostgreSql;

/// <summary>Adapters between Npgsql transactions and work operations.</summary>
public static class PostgreSqlWork
{
    /// <summary>The Npgsql transaction behind a handle, for hooks that bind their own data access to it.</summary>
    public static NpgsqlTransaction Npgsql(this IWorkTransaction transaction) => PostgreSqlTransaction.From(transaction).Transaction;
}

/// <summary>The PostgreSQL view of a <see cref="WorkTransaction"/> over an Npgsql transaction.</summary>
internal sealed class PostgreSqlTransaction
{
    private PostgreSqlTransaction(WorkTransaction work, NpgsqlTransaction transaction)
    {
        Work = work;
        Transaction = transaction;
    }

    /// <summary>Wraps a transaction the framework began, for operations and handlers that join it.</summary>
    public PostgreSqlTransaction(NpgsqlTransaction transaction)
        : this(WorkTransaction.Of(transaction), transaction)
    {
    }

    /// <summary>The handle operations and handlers receive.</summary>
    public WorkTransaction Work { get; }

    public NpgsqlTransaction Transaction { get; }

    public NpgsqlConnection Connection =>
        Transaction.Connection ?? throw new InvalidOperationException("The transaction is no longer active.");

    public string NextSavepoint() => Work.NextSavepoint();

    public static PostgreSqlTransaction From(IWorkTransaction transaction) => transaction switch
    {
        WorkTransaction { Transaction: NpgsqlTransaction npgsql } work => new(work, npgsql),
        WorkTransaction work => throw new ArgumentException($"The PostgreSQL provider joins Npgsql transactions, not {work.Transaction.GetType().Name}.", nameof(transaction)),
        null => throw new ArgumentNullException(nameof(transaction)),
        _ => throw new ArgumentException("The transaction was not created for the PostgreSQL provider.", nameof(transaction)),
    };
}
