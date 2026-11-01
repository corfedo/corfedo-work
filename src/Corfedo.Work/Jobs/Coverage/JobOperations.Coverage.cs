namespace Corfedo.Work;

/// <summary>
/// Reports of which job definitions the live processes of the process registry cover. A registered process records the
/// definitions it registers: the revision of its code, whether it runs the handler with the argument versions it
/// accepts, and whether it declares the code-defined recurrence. The reports change no record.
/// </summary>
public sealed partial class JobOperations
{
    /// <summary>
    /// Definitions with due work that no live process runs (R12), and argument versions of due jobs that no live process
    /// running the definition accepts (R21). Due work is an unheld job due now, an expired lease, or a recurrence its
    /// planner would act on now; paused definitions and subjects and versions below the stored minimum are left out. A
    /// job or recurrence whose copied pause flag a request racing a resume left set counts as due work of a definition no
    /// live process runs; a live runner repairs such flags in its maintenance.
    /// </summary>
    public ValueTask<UnservedReport> GetUnservedAsync(CancellationToken cancellationToken = default) => Store.GetUnservedAsync(cancellationToken);

    /// <summary>
    /// Code-defined recurrences that no live process declares any longer (J10), whatever revision it registers. They are
    /// kept and planned by processes that run the definition until removed with
    /// <see cref="RemoveAsync(IWorkTransaction, JobDefinition, CancellationToken)"/>.
    /// </summary>
    public ValueTask<IReadOnlyList<UndeclaredRecurrence>> GetUndeclaredRecurrencesAsync(CancellationToken cancellationToken = default) =>
        Store.GetUndeclaredRecurrencesAsync(cancellationToken);

    /// <summary>
    /// Live processes whose own revision of a definition is older than the stored one (R18): they apply the stored
    /// policy and run the argument versions their code accepts.
    /// </summary>
    public ValueTask<IReadOnlyList<OutdatedProcess>> GetOutdatedProcessesAsync(CancellationToken cancellationToken = default) =>
        Store.GetOutdatedProcessesAsync(cancellationToken);
}
