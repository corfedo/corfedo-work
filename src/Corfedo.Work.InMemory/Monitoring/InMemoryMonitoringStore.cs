using Corfedo.Work.InMemory.Database;
using Corfedo.Work.InMemory.Leases;
using static Corfedo.Work.InMemory.Leases.InMemoryLeaseStore;

namespace Corfedo.Work.InMemory.Monitoring;

/// <summary>
/// Expectation state and incidents of one application: expectation rows per key, a definition with a subject or alone,
/// and incidents on the application's incident table, which the incident source claims. At most one incident is open
/// per definition, subject, kind and date set.
/// </summary>
internal sealed partial class InMemoryMonitoringStore(InMemoryWorkDatabase database, ApplicationState application, InMemoryLeaseStore leases) : IMonitoringStore
{
    public StoreLocation Incidents => application.Incidents.Location;

    private Table<RowKey, Record> IncidentRows => application.Incidents.Rows;

    private DateTimeOffset Now => database.Clock.GetUtcNow();

    public ValueTask<StoredExpectations> GetExpectationsAsync(CancellationToken cancellationToken)
    {
        var now = Now;
        return ValueTask.FromResult(database.ReadCommitted(() =>
        {
            var recurring = application.Recurrences.Committed.Values.Select(c => c.Definition).ToHashSet(StringComparer.Ordinal);
            var changed = application.ExpectationChanges.Committed.Values.Select(c => c.Definition).ToHashSet(StringComparer.Ordinal);
            var list = application.Definitions.Committed.Values
                .OrderBy(d => d.Stored.Key, StringComparer.Ordinal)
                .Select(d =>
                {
                    var stored = d.Stored;
                    var expectation = Expectation(stored);
                    return new StoredExpectation(
                        stored.Key,
                        stored.Revision,
                        expectation,
                        stored.Paused,
                        expectation?.Deadline is not null && recurring.Contains(stored.Key),
                        d.MonitoredRevision == stored.Revision && d.MonitoredPaused == stored.Paused,
                        changed.Contains(stored.Key));
                })
                .ToList();
            return new StoredExpectations(now, list);
        }));
    }

    /// <summary>One page newest first by opening time and id, continuing after the cursor.</summary>
    public ValueTask<WorkIncidentPage> GetIncidentsAsync(bool openOnly, string? definition, int limit, WorkIncidentCursor? after, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        var list = database.ReadCommitted(() => IncidentRows.Committed.Values
            .Select(r => r.Incident())
            .Where(i => (!openOnly || i.ResolvedAt is null)
                && (definition is null || i.Definition == definition)
                && (after is null || i.OpenedAt < after.OpenedAt || (i.OpenedAt == after.OpenedAt && i.Id.CompareTo(after.Id) < 0)))
            .OrderByDescending(i => i.OpenedAt)
            .ThenByDescending(i => i.Id)
            .Take(limit + 1)
            .ToList());
        if (list.Count <= limit)
        {
            return ValueTask.FromResult(new WorkIncidentPage(list, null));
        }
        list.RemoveAt(limit);
        return ValueTask.FromResult(new WorkIncidentPage(list, new WorkIncidentCursor(list[^1].OpenedAt, list[^1].Id)));
    }

    public ValueTask<IReadOnlyList<string>> GetUnmonitoredAsync(CancellationToken cancellationToken)
    {
        var now = Now;
        return ValueTask.FromResult<IReadOnlyList<string>>(database.ReadCommitted(() =>
            application.Processes.Committed.Values.Any(p => p.Monitors && p.LiveAt(now))
                ? []
                : application.Definitions.Committed.Values
                    .Where(d => Expectation(d.Stored) is not null)
                    .Select(d => d.Stored.Key)
                    .Order(StringComparer.Ordinal)
                    .ToList()));
    }

    public async ValueTask RegisterMonitorAsync(OwnerId owner, CancellationToken cancellationToken)
    {
        using var transaction = await database.BeginAsync(cancellationToken);
        if (transaction.Get(application.Processes, owner) is { Monitors: false } process)
        {
            transaction.Put(application.Processes, owner, process with { Monitors = true });
        }
        transaction.Commit();
    }

    /// <summary>The definition's stored expectations, or null when it has none.</summary>
    internal static JobExpectation? Expectation(StoredJobDefinition stored) => stored.Policy.Expect is { IsEmpty: false } expectation ? expectation : null;

    /// <summary>Every incident as the transaction sees it.</summary>
    private IEnumerable<Record> AllIncidents(InMemoryTransaction work) => work.Scan(IncidentRows).Select(r => r.Row);

    /// <summary>
    /// Inserts an incident unless it is to be open and an incident of its definition, subject, kind and date set is open
    /// already; the incident is due for delivery at once. Returns the inserted incident, or null.
    /// </summary>
    private WorkIncident? Insert(
        InMemoryTransaction work,
        WorkIncidentKind kind,
        string? definition,
        Subject? subject,
        string? dateSet,
        Guid? job,
        DateTimeOffset openedAt,
        DateTimeOffset? resolvedAt = null)
    {
        if (resolvedAt is null && AllIncidents(work).Any(r => r.Open() && r.OpenKey() == (definition, subject?.Kind, subject?.Id, (int)kind, dateSet)))
        {
            return null;
        }
        var id = Guid.NewGuid();
        var record = new Record(
            new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                [IncidentColumns.Id] = id,
                [IncidentColumns.Kind] = (int)kind,
                [IncidentColumns.Definition] = definition,
                [IncidentColumns.SubjectKind] = subject?.Kind,
                [IncidentColumns.SubjectId] = subject?.Id,
                [IncidentColumns.DateSet] = dateSet,
                [IncidentColumns.Job] = job,
                [IncidentColumns.OpenedAt] = openedAt,
                [IncidentColumns.ResolvedAt] = resolvedAt,
                [LeaseFields.Default.DueAt] = Now,
            },
            database.NextVersion());
        work.Put(IncidentRows, new RowKey([id]), record);
        return record.Incident();
    }

    /// <summary>Resolves an open incident at <paramref name="at"/>, not before its opening, due for delivery unless it is.</summary>
    private WorkIncident Resolve(InMemoryTransaction work, Record r, DateTimeOffset at)
    {
        var opened = r.Time(IncidentColumns.OpenedAt)!.Value;
        var written = r.With(
            [
                Set(IncidentColumns.ResolvedAt, at > opened ? at : opened),
                Set(LeaseFields.Default.DueAt, r.Time(LeaseFields.Default.DueAt) ?? Now),
            ],
            database.NextVersion());
        work.Put(IncidentRows, new RowKey([r.Uuid(IncidentColumns.Id)!.Value]), written);
        return written.Incident();
    }

    /// <summary>Writes <paramref name="changes"/> to an incident.</summary>
    private void Write(InMemoryTransaction work, Record r, IEnumerable<KeyValuePair<string, object?>> changes) =>
        work.Put(IncidentRows, new RowKey([r.Uuid(IncidentColumns.Id)!.Value]), r.With(changes, database.NextVersion()));

    /// <summary>Whether the incident belongs to the key of <paramref name="definition"/> and <paramref name="subject"/>.</summary>
    private static bool OfKey(Record r, string definition, string? subjectKind, string? subjectId) =>
        (string?)r[IncidentColumns.Definition] == definition
        && (string?)r[IncidentColumns.SubjectKind] == subjectKind
        && (string?)r[IncidentColumns.SubjectId] == subjectId;

    /// <summary>Signals claimers of the incident source when the transaction commits.</summary>
    private void Notify(InMemoryTransaction work) => leases.Signal(work, WorkTopics.Due, IncidentSource.Name);
}
