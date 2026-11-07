namespace Corfedo.Work;

/// <summary>Runs a job handler given as a delegate, with the service scope of the attempt.</summary>
internal sealed class DelegateJobHandler<TArgs>(
    Func<TArgs, JobRun, IServiceProvider, CancellationToken, ValueTask<JobDisposition>> handler,
    IServiceProvider services) : IJobHandler<TArgs>
{
    public ValueTask<JobDisposition> RunAsync(TArgs args, JobRun run, CancellationToken cancellationToken) =>
        handler(args, run, services, cancellationToken);
}

/// <summary>Runs a job handler of a definition with a result type given as a delegate, with the service scope of the attempt.</summary>
internal sealed class DelegateJobHandler<TArgs, TResult>(
    Func<TArgs, JobRun<TResult>, IServiceProvider, CancellationToken, ValueTask<JobDisposition<TResult>>> handler,
    IServiceProvider services) : IJobHandler<TArgs, TResult>
{
    public ValueTask<JobDisposition<TResult>> RunAsync(TArgs args, JobRun<TResult> run, CancellationToken cancellationToken) =>
        handler(args, run, services, cancellationToken);
}
