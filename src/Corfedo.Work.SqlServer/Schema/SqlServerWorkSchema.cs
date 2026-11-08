namespace Corfedo.Work.SqlServer;

/// <summary>
/// The framework schema for a migrate step that runs apart from the serving processes, which then register the store
/// with <see cref="WorkSchemaMode.Verify"/>. Both forms run under a user that may change the schema: one that may
/// create it, or the owner of a schema created for it.
/// </summary>
public static class SqlServerWorkSchema
{
    /// <summary>
    /// Refuses a database without READ_COMMITTED_SNAPSHOT, then applies the scripts of this release not yet recorded
    /// in <paramref name="schema"/>, in one transaction. Each of <paramref name="runtimeRoles"/>, database users or
    /// roles, then holds exactly the runtime permissions: <c>SELECT</c>, <c>INSERT</c>, <c>UPDATE</c>, <c>DELETE</c>
    /// and <c>VIEW DEFINITION</c> on the schema; any other permission it held on the schema or its objects is revoked.
    /// Running it again changes nothing.
    /// </summary>
    public static Task MigrateAsync(string connectionString, string schema = "work", IEnumerable<string>? runtimeRoles = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);
        return Schema.FrameworkSchema.MigrateAsync(connectionString, schema, Roles(runtimeRoles), cancellationToken);
    }

    /// <summary>
    /// The T-SQL of <see cref="MigrateAsync"/> as one batch without <c>GO</c>, for migrate steps that run SQL files.
    /// It may run against a database at any earlier version and applies only what is missing.
    /// </summary>
    public static string Script(string schema = "work", IEnumerable<string>? runtimeRoles = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);
        return Schema.FrameworkSchema.Script(schema, Roles(runtimeRoles));
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
