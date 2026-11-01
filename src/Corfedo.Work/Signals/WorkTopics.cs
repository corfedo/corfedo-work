namespace Corfedo.Work;

/// <summary>The framework's signal topics.</summary>
public static class WorkTopics
{
    private const int OwnerLength = 36;

    /// <summary>A record of the source became due or was given back; the key is the source.</summary>
    public static SignalTopic Due { get; } = new("work.due");

    /// <summary>
    /// A recurrence of the job source changed, one of its occurrences ended, or a job of it needs its calendar planned;
    /// its planners should run. The key is the job source.
    /// </summary>
    public static SignalTopic Plan { get; } = new("work.plan");

    /// <summary>A job of the source that a caller waits for has no outstanding execution left; the key is the job source.</summary>
    public static SignalTopic Ended { get; } = new("work.ended");

    /// <summary>
    /// The keys the job source's definition expects to complete changed: a recurrence was created, replaced or removed,
    /// or its copied pause flag changed; monitoring loops should apply them. The key is the job source.
    /// </summary>
    public static SignalTopic Monitor { get; } = new("work.monitor");

    /// <summary>Cancellation was requested for a record of a source held by an owner; the key is <see cref="CancelKey"/>.</summary>
    public static SignalTopic Cancel { get; } = new("work.cancel");

    /// <summary>The key of a <see cref="Cancel"/> signal: the holder's id, a colon and the source.</summary>
    public static string CancelKey(OwnerId owner, string source) => owner.Value.ToString("D") + ":" + source;

    /// <summary>Reads a <see cref="CancelKey"/>; false for a key of another form.</summary>
    public static bool TryParseCancelKey(string key, out OwnerId owner, out string source)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (key.Length > OwnerLength + 1 && key[OwnerLength] == ':' && Guid.TryParseExact(key.AsSpan(0, OwnerLength), "D", out var id))
        {
            owner = new OwnerId(id);
            source = key[(OwnerLength + 1)..];
            return true;
        }
        owner = default;
        source = "";
        return false;
    }
}
