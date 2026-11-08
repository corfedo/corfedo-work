using Corfedo.Work.PostgreSql.Signals;
using Npgsql;

namespace Corfedo.Work.PostgreSql;

/// <summary>
/// The framework schema for a migrate step that runs apart from the serving processes, which then register the store
/// with <see cref="WorkSchemaMode.Verify"/>. Both forms run under a login that may change the schema: one that may
/// create it, or the owner of a schema created for it.
/// </summary>
public static class PostgreSqlWorkSchema
{
    /// <summary>
    /// Applies the scripts of this release not yet recorded in <paramref name="schema"/>, in one transaction. Each of
    /// <paramref name="runtimeRoles"/> then holds exactly the runtime privileges on the schema: <c>USAGE</c> on it,
    /// <c>SELECT</c>, <c>INSERT</c>, <c>UPDATE</c> and <c>DELETE</c> on its tables, <c>USAGE</c> and <c>SELECT</c> on
    /// its sequences and <c>EXECUTE</c> on its functions; any other privilege it held on them is revoked. Running it
    /// again changes nothing.
    /// </summary>
    public static Task MigrateAsync(NpgsqlDataSource dataSource, string schema = "work", IEnumerable<string>? runtimeRoles = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        return Schema.FrameworkSchema.MigrateAsync(dataSource, Checked(schema), Roles(runtimeRoles), cancellationToken);
    }

    /// <summary>
    /// The SQL of <see cref="MigrateAsync"/> as one statement, for migrate steps that run SQL files. It may run against
    /// a database at any earlier version and applies only what is missing.
    /// </summary>
    public static string Script(string schema = "work", IEnumerable<string>? runtimeRoles = null) =>
        Schema.FrameworkSchema.Script(Checked(schema), Roles(runtimeRoles));

    private static string Checked(string schema)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);
        PostgreSqlSignalSql.For(schema);
        return schema;
    }

    private static string[] Roles(IEnumerable<string>? roles)
    {
        var list = roles?.ToArray() ?? [];
        foreach (var role in list)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(role, nameof(roles));
        }
        return list;
    }
}
