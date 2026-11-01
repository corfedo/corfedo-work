namespace Corfedo.Work;

/// <summary>A trailing debounce: due a delay after the latest request, never later than a maximum after the first outstanding one.</summary>
public sealed record Debounce
{
    private Debounce(TimeSpan delay, TimeSpan? maximum)
    {
        Delay = delay;
        Maximum = maximum;
    }

    public TimeSpan Delay { get; }

    /// <summary>The latest due time after the first request not yet claimed; null for no ceiling.</summary>
    public TimeSpan? Maximum { get; }

    public static Debounce Trailing(TimeSpan delay, TimeSpan? maximum = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        if (maximum is { } ceiling)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(ceiling, TimeSpan.Zero, nameof(maximum));
        }
        return new(delay, maximum);
    }
}

/// <summary>Options of one request.</summary>
public sealed record ReconcileRequest
{
    public static ReconcileRequest Default { get; } = new();

    /// <summary>Delays the run; null runs it as soon as the record is claimable.</summary>
    public Debounce? Debounce { get; init; }

    /// <summary>Clears the backoff floor and the failure count, so a deliberate request is not delayed by earlier failures.</summary>
    public bool ResetBackoff { get; init; }
}

/// <summary>What a run reports.</summary>
public enum ReconcileOutcome
{
    /// <summary>The record matches the revision and generation the run started from.</summary>
    Converged,

    /// <summary>The record waits for something outside it until a retry time; not a failure.</summary>
    Waiting,

    /// <summary>The run failed; it is retried after the failure backoff.</summary>
    Failed,
}

/// <summary>The result of a run, applied to the record's work fields.</summary>
public sealed record ReconcileResult
{
    private ReconcileResult(ReconcileOutcome outcome, TimeSpan retryAfter, string? code)
    {
        Outcome = outcome;
        RetryAfter = retryAfter;
        Code = code;
    }

    public ReconcileOutcome Outcome { get; }

    /// <summary>For <see cref="ReconcileOutcome.Waiting"/>: when the record is due again.</summary>
    public TimeSpan RetryAfter { get; }

    /// <summary>
    /// For <see cref="ReconcileOutcome.Failed"/>: why, for logs. The dispatcher logs it when it applies the result; the
    /// domain stores its own error fields.
    /// </summary>
    public string? Code { get; }

    public static ReconcileResult Converged { get; } = new(ReconcileOutcome.Converged, TimeSpan.Zero, null);

    public static ReconcileResult Waiting(TimeSpan retryAfter)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(retryAfter, TimeSpan.Zero);
        return new(ReconcileOutcome.Waiting, retryAfter, null);
    }

    public static ReconcileResult Failed(string code)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(code);
        return new(ReconcileOutcome.Failed, TimeSpan.Zero, code);
    }
}

/// <summary>The requested revision and the source generation a claim of a reconciliation record started from.</summary>
public sealed record ReconcileAttempt(long Revision, long Generation);
