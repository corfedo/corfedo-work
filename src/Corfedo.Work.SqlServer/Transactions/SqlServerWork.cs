using Microsoft.Data.SqlClient;

namespace Corfedo.Work.SqlServer;

/// <summary>Adapters between SqlClient transactions and work operations.</summary>
public static class SqlServerWork
{
    /// <summary>The SqlClient transaction behind a handle, for hooks that bind their own data access to it.</summary>
    public static SqlTransaction SqlServer(this WorkTransaction transaction) => SqlServerTransaction.From(transaction).Transaction;
}

/// <summary>The SQL Server view of a work transaction over a SqlClient transaction.</summary>
internal sealed class SqlServerTransaction
{
    private SqlServerTransaction(DbWorkTransaction work, SqlTransaction transaction)
    {
        Work = work;
        Transaction = transaction;
    }

    /// <summary>Wraps a transaction the framework began at read committed, for operations and handlers that join it.</summary>
    public SqlServerTransaction(SqlTransaction transaction)
        : this(DbWorkTransaction.Of(transaction), transaction)
    {
        Work.ReadCommitted = true;
    }

    /// <summary>The handle operations and handlers receive.</summary>
    public DbWorkTransaction Work { get; }

    public SqlTransaction Transaction { get; }

    public SqlConnection Connection =>
        Transaction.Connection ?? throw new InvalidOperationException("The transaction is no longer active.");

    public string NextSavepoint() => Work.NextSavepoint();

    public static SqlServerTransaction From(WorkTransaction transaction) => transaction switch
    {
        DbWorkTransaction { Transaction: SqlTransaction sql } work => new(work, sql),
        DbWorkTransaction work => throw new ArgumentException($"The SQL Server provider joins SqlClient transactions, not {work.Transaction.GetType().Name}.", nameof(transaction)),
        null => throw new ArgumentNullException(nameof(transaction)),
        _ => throw new ArgumentException("The transaction was not created for the SQL Server provider.", nameof(transaction)),
    };
}
