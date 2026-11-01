using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Corfedo.Work;

public static class RecurringChainServiceCollectionExtensions
{
    /// <summary>
    /// Declares a recurring chain (K5): the code-defined recurrence of <paramref name="definition"/>, whose every
    /// occurrence, when it runs, creates a fresh chain from <paramref name="chain"/> with its root due at once. The
    /// occurrence stays outstanding until the chain settled and then ends with the chain's outcome, so the overlap,
    /// missed and late policies and the outcome hook apply to the chain as a whole; an occurrence skipped before it runs
    /// creates no chain. The process runs the definition's occurrences, which have no handler of their own; every step's
    /// definition must be registered.
    /// </summary>
    public static IServiceCollection AddRecurringChain<TArgs>(
        this IServiceCollection services,
        JobDefinition<TArgs> definition,
        TArgs args,
        Schedule schedule,
        Action<ChainBuilder> chain,
        RecurrenceOptions? options = null,
        Action<JobBuilder>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(chain);
        services.AddJob(definition, configure, services => new JobHandler<TArgs, ChainOwnerHandler<TArgs>>(
            new ChainOwnerHandler<TArgs>(chain),
            services.GetRequiredService<JobRegistry>(),
            services.GetRequiredService<IWorkStore>(),
            services.GetService<ILoggerFactory>()));
        return services.AddRecurring(definition, args, schedule, options);
    }
}

/// <summary>Runs an occurrence of a recurring chain: creates its chain and leaves the occurrence waiting for it.</summary>
internal sealed class ChainOwnerHandler<TArgs>(Action<ChainBuilder> chain) : IJobHandler<TArgs>
{
    public async ValueTask<JobDisposition> RunAsync(TArgs args, JobRun run, CancellationToken cancellationToken)
    {
        if (!await run.StartOwnedChainAsync(chain, cancellationToken))
        {
            // The lease was lost, or the occurrence's cancellation was requested before its chain was created: a retry
            // of a job with a cancellation request ends it cancelled, and a lost lease applies nothing.
            return JobResult.Retry(TimeSpan.Zero, JobReasons.Cancelled);
        }
        return run.Completed;
    }
}
