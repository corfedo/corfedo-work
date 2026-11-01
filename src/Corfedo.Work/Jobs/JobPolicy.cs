namespace Corfedo.Work;

/// <summary>
/// The shared policy of a job definition, stored with its revision so every process enforces the newest one.
/// </summary>
public sealed record JobPolicy
{
    /// <summary>Attempts of one execution, counting lost leases; at least 1.</summary>
    public int Attempts { get; init; } = 3;

    /// <summary>Delay before the next attempt by the attempts already used; a handler's retry delay can lengthen it.</summary>
    public Backoff Backoff { get; init; } = Backoff.Exponential(TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(10));

    /// <summary>
    /// Running jobs of one partition across all processes; null for no limit. Jobs requested without a partition share
    /// one default partition of the definition.
    /// </summary>
    public int? PartitionLimit { get; init; }

    /// <summary>Running jobs of the definition across all processes; null for no limit (J22).</summary>
    public int? DefinitionLimit { get; init; }

    /// <summary>The definition's priority class in dispatch pools (J17).</summary>
    public Priority Priority { get; init; } = Priority.Normal;

    /// <summary>Cooperative timeout of one attempt; null for none (J18).</summary>
    public TimeSpan? Timeout { get; init; }

    /// <summary>Whether a timed-out attempt is retried instead of ending the job as timed out.</summary>
    public bool RetryTimeouts { get; init; }

    /// <summary>What happens to a job whose execution would start late (J2).</summary>
    public Late Late { get; init; } = Late.Run;

    /// <summary>The lowest argument version still accepted; jobs with an older version end failed with <c>args_version</c>.</summary>
    public int MinimumArgsVersion { get; init; } = 1;

    /// <summary>Whether every attempt is recorded in the run history (D7); disabled by default.</summary>
    public bool RunHistory { get; init; }

    /// <summary>
    /// How long a terminal job is kept after it ended before retention deletes it (J12); null for the host's default.
    /// Its outcome and result go with it.
    /// </summary>
    public TimeSpan? Retention { get; init; }

    /// <summary>How long a run history entry is kept after its attempt ended (D7); null for the host's default.</summary>
    public TimeSpan? RunHistoryRetention { get; init; }

    /// <summary>What the definition's jobs are expected to do (M1), checked per subject; null for no expectations.</summary>
    public JobExpectation? Expect { get; init; }
}

/// <summary>The priority class of a definition: free capacity of a dispatch pool goes to higher classes first.</summary>
public enum Priority
{
    Low = -1,
    Normal = 0,
    High = 1,
}

/// <summary>Whether a job whose execution starts late still runs.</summary>
public sealed record Late
{
    private Late(TimeSpan? grace) => Grace = grace;

    /// <summary>Run however late.</summary>
    public static Late Run { get; } = new((TimeSpan?)null);

    /// <summary>The longest delay after which a job still starts; null when it always runs.</summary>
    public TimeSpan? Grace { get; }

    /// <summary>End as skipped, with no handler call and no attempt used, when the first claim is later than <paramref name="grace"/> after the due time.</summary>
    public static Late SkipAfter(TimeSpan grace)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(grace, TimeSpan.Zero);
        return new(grace);
    }
}
