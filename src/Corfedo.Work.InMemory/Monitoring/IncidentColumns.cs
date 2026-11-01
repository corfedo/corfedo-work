using Corfedo.Work.InMemory.Database;

namespace Corfedo.Work.InMemory.Monitoring;

/// <summary>
/// The columns of the framework's incident table, named as the PostgreSQL store names them, so the incident source
/// claims incidents with the default lease fields. Kinds are stored as <c>int</c>.
/// </summary>
internal static class IncidentColumns
{
    public const string Id = "id";
    public const string Kind = "kind";
    public const string Definition = "definition";
    public const string SubjectKind = "subject_kind";
    public const string SubjectId = "subject_id";
    public const string DateSet = "date_set";
    public const string Job = "job";
    public const string OpenedAt = "opened_at";
    public const string ResolvedAt = "resolved_at";
    public const string OpenedDeliveredAt = "opened_delivered_at";
    public const string ResolvedDeliveredAt = "resolved_delivered_at";

    /// <summary>Every column; the key is <see cref="Id"/>.</summary>
    public static IEnumerable<string> All
    {
        get
        {
            var f = LeaseFields.Default;
            return
            [
                Kind, Definition, SubjectKind, SubjectId, DateSet, Job, OpenedAt, ResolvedAt, OpenedDeliveredAt, ResolvedDeliveredAt,
                f.DueAt, f.LeaseOwner, f.LeaseEpoch, f.LeaseExpiresAt, f.Takeovers, f.Failures, f.SettledEpoch, f.SettleFailures,
                f.SettleRetryAt, f.CancelRequestedAt, f.ExecutionId, f.EndedCompletionId, f.EndedDigest,
            ];
        }
    }

    public static WorkIncident Incident(this Record r) => new(
        r.Uuid(Id)!.Value,
        (WorkIncidentKind)r.Int32(Kind),
        (string?)r[Definition],
        r[SubjectKind] is string kind ? Subject.Of(kind, (string)r[SubjectId]!) : null,
        (string?)r[DateSet],
        r.Uuid(Job) is { } job ? new JobId(job) : null,
        r.Time(OpenedAt)!.Value,
        r.Time(ResolvedAt));

    public static bool Open(this Record r) => r[ResolvedAt] is null;

    /// <summary>The key of an open incident, of which at most one is open: definition, subject, kind and date set.</summary>
    public static (string?, string?, string?, int, string?) OpenKey(this Record r) =>
        ((string?)r[Definition], (string?)r[SubjectKind], (string?)r[SubjectId], r.Int32(Kind), (string?)r[DateSet]);
}
