namespace Corfedo.Work.SqlServer.Jobs;

/// <summary>
/// Job definitions, requests, transitions and cancellation on the framework's job table. Statements of one definition
/// use the statements of its lease source, so they carry the same definition scope as claims.
/// </summary>
internal sealed partial class SqlServerJobStore(string connectionString, string schema) : IJobStore
{
    private readonly string definitions = Tsql.Quote(schema) + ".job_definitions";
    private readonly string recurrences = Tsql.Quote(schema) + ".job_recurrences";

    public StoreLocation Table { get; } = StoreLocation.Table(schema, "jobs");
}
