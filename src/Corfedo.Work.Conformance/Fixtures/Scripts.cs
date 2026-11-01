namespace Corfedo.Work.Conformance;

/// <summary>What a scenario's handler does; one instance per process.</summary>
public sealed class HandlerScript
{
    public Func<LeaseRun<TestKey>, CancellationToken, ValueTask> Run { get; set; } = (_, _) => ValueTask.CompletedTask;

    public Func<LeaseRun<TestKey>, CancellationToken, ValueTask> Exhausted { get; set; } = (_, _) => ValueTask.CompletedTask;
}

public sealed class ScriptedHandler(HandlerScript script) : ILeaseHandler<TestKey>
{
    public ValueTask RunAsync(LeaseRun<TestKey> run, CancellationToken cancellationToken) => script.Run(run, cancellationToken);

    public ValueTask ExhaustedAsync(LeaseRun<TestKey> run, CancellationToken cancellationToken) => script.Exhausted(run, cancellationToken);
}

/// <summary>What a scenario's claim hook does.</summary>
public sealed class ClaimHookScript
{
    public Func<LeaseClaim<TestKey>, WorkTransaction, CancellationToken, ValueTask> Claimed { get; set; } = (_, _, _) => ValueTask.CompletedTask;
}

public sealed class ScriptedClaimHook(ClaimHookScript script) : ILeaseClaimHook<TestKey>
{
    public ValueTask ClaimedAsync(LeaseClaim<TestKey> claim, WorkTransaction transaction, CancellationToken cancellationToken) =>
        script.Claimed(claim, transaction, cancellationToken);
}

/// <summary>What a scenario's expiry handler does.</summary>
public sealed class ExpiryScript
{
    public Func<LeaseExpiry<TestKey>, WorkTransaction, CancellationToken, ValueTask> Expired { get; set; } = (_, _, _) => ValueTask.CompletedTask;
}

public sealed class ScriptedExpiryHandler(ExpiryScript script) : ILeaseExpiryHandler<TestKey>
{
    public ValueTask ExpiredAsync(LeaseExpiry<TestKey> expiry, WorkTransaction transaction, CancellationToken cancellationToken) =>
        script.Expired(expiry, transaction, cancellationToken);
}

/// <summary>A clock whose wall time is offset, as on a process with a skewed system clock; timers are unaffected.</summary>
public sealed class SkewedTime(TimeSpan offset) : TimeProvider
{
    public override DateTimeOffset GetUtcNow() => System.GetUtcNow() + offset;
}
