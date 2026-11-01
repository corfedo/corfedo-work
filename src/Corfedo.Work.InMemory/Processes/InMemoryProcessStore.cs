using Corfedo.Work.InMemory.Database;

namespace Corfedo.Work.InMemory.Processes;

/// <summary>The process registry of one application. Every time is the store's clock.</summary>
internal sealed class InMemoryProcessStore(InMemoryWorkDatabase database, ApplicationState application) : IProcessStore
{
    private Table<OwnerId, ProcessRow> Processes => application.Processes;

    public async ValueTask RegisterAsync(ProcessRegistration registration, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(registration);
        using var transaction = await database.BeginAsync(cancellationToken);
        if (transaction.Get(Processes, registration.Owner) is null)
        {
            var now = database.Clock.GetUtcNow();
            transaction.Put(Processes, registration.Owner, new ProcessRow(registration.Owner, registration.Type, registration.Release, registration.StaleAfter, now, now, null));
        }
        transaction.Commit();
    }

    public ValueTask<bool> HeartbeatAsync(OwnerId owner, CancellationToken cancellationToken) =>
        UpdateAsync(owner, (p, now) => p with { HeartbeatAt = now }, cancellationToken);

    public async ValueTask StopAsync(OwnerId owner, CancellationToken cancellationToken) =>
        await UpdateAsync(owner, (p, now) => p.StoppedAt is null ? p with { StoppedAt = now } : p, cancellationToken);

    public ValueTask<IReadOnlyList<ProcessInstance>> ListAsync(string? type, IReadOnlyCollection<ProcessState> states, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(states);
        var now = database.Clock.GetUtcNow();
        var processes = database.ReadCommitted(() => Processes.Committed.Values.ToList());
        IReadOnlyList<ProcessInstance> list = [.. processes
            .Where(p => type is null || p.Type == type)
            .Select(p => new ProcessInstance(p.Owner, p.Type, p.Release, p.StateAt(now), p.StartedAt, p.HeartbeatAt, p.StoppedAt))
            .Where(p => states.Contains(p.State))
            .OrderBy(p => p.Type, StringComparer.Ordinal)
            .ThenBy(p => p.StartedAt)
            .ThenBy(p => p.Owner.Value)];
        return ValueTask.FromResult(list);
    }

    /// <summary>Deletes processes stopped, or stale without a stop, longer than <paramref name="retention"/> ago, with their definitions and calendars.</summary>
    public async ValueTask<int> RetainAsync(TimeSpan retention, int limit, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        using var transaction = await database.BeginAsync(cancellationToken);
        var horizon = database.Clock.GetUtcNow() - retention;
        var expired = transaction.Scan(Processes)
            .Where(p => p.Row.StoppedAt < horizon || (p.Row.StoppedAt is null && p.Row.HeartbeatAt + p.Row.StaleAfter < horizon))
            .OrderBy(p => p.Key.Value)
            .Take(limit)
            .Select(p => p.Key)
            .ToList();
        foreach (var owner in expired)
        {
            transaction.Remove(Processes, owner);
        }
        transaction.Commit();
        return expired.Count;
    }

    private async ValueTask<bool> UpdateAsync(OwnerId owner, Func<ProcessRow, DateTimeOffset, ProcessRow> update, CancellationToken cancellationToken)
    {
        using var transaction = await database.BeginAsync(cancellationToken);
        if (transaction.Get(Processes, owner) is not { } process)
        {
            return false;
        }
        transaction.Put(Processes, owner, update(process, database.Clock.GetUtcNow()));
        transaction.Commit();
        return true;
    }
}
