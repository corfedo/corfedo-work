using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace Corfedo.Work;

/// <summary>
/// Collects the sources of due times a save writes before it runs, since the save accepts the changes, and sends
/// <see cref="WorkTopics.Due"/> for them once it succeeded.
/// </summary>
internal sealed class DueWakeUpInterceptor(IWorkSignals signals) : SaveChangesInterceptor
{
    private readonly ConditionalWeakTable<DbContext, string[]> saving = [];

    public override InterceptionResult<int> SavingChanges(DbContextEventData eventData, InterceptionResult<int> result)
    {
        Collect(eventData.Context);
        return result;
    }

    public override ValueTask<InterceptionResult<int>> SavingChangesAsync(DbContextEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
    {
        Collect(eventData.Context);
        return ValueTask.FromResult(result);
    }

    public override int SavedChanges(SaveChangesCompletedEventData eventData, int result)
    {
        SignalAsync(eventData.Context, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        return result;
    }

    public override async ValueTask<int> SavedChangesAsync(SaveChangesCompletedEventData eventData, int result, CancellationToken cancellationToken = default)
    {
        await SignalAsync(eventData.Context, cancellationToken);
        return result;
    }

    public override void SaveChangesFailed(DbContextErrorEventData eventData) => Forget(eventData.Context);

    public override Task SaveChangesFailedAsync(DbContextErrorEventData eventData, CancellationToken cancellationToken = default)
    {
        Forget(eventData.Context);
        return Task.CompletedTask;
    }

    public override void SaveChangesCanceled(DbContextEventData eventData) => Forget(eventData.Context);

    public override Task SaveChangesCanceledAsync(DbContextEventData eventData, CancellationToken cancellationToken = default)
    {
        Forget(eventData.Context);
        return Task.CompletedTask;
    }

    private void Collect(DbContext? context)
    {
        if (context is null)
        {
            return;
        }
        var sources = new HashSet<string>(StringComparer.Ordinal);
        foreach (var entry in context.ChangeTracker.Entries())
        {
            if (entry.State is not (EntityState.Added or EntityState.Modified))
            {
                continue;
            }
            foreach (var property in entry.Properties)
            {
                if (property.Metadata.FindAnnotation(WorkModelExtensions.DueSource)?.Value is string source
                    && (entry.State == EntityState.Added ? property.CurrentValue is not null : property.IsModified))
                {
                    sources.Add(source);
                }
            }
        }
        saving.AddOrUpdate(context, [.. sources]);
    }

    private async ValueTask SignalAsync(DbContext? context, CancellationToken cancellationToken)
    {
        if (context is null || !saving.TryGetValue(context, out var sources))
        {
            return;
        }
        saving.Remove(context);
        if (sources.Length == 0)
        {
            return;
        }
        var transaction = context.Database.CurrentTransaction is null ? null : context.Database.WorkTransaction();
        foreach (var source in sources)
        {
            await (transaction is null
                ? signals.SendAsync(WorkTopics.Due, source, cancellationToken)
                : signals.SendAsync(transaction, WorkTopics.Due, source, cancellationToken));
        }
    }

    private void Forget(DbContext? context)
    {
        if (context is not null)
        {
            saving.Remove(context);
        }
    }
}
