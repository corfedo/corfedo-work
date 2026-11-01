using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Corfedo.Work;

/// <summary>Serializes a job's arguments with a schema version; reads every version its registration accepts.</summary>
public interface IJobArgs<TArgs>
{
    /// <summary>The version this codec writes.</summary>
    int Version { get; }

    byte[] Write(TArgs args);

    TArgs Read(ReadOnlySpan<byte> data, int version);
}

public static class JobArgs
{
    /// <summary>The largest serialized arguments a request may store.</summary>
    public const int MaximumBytes = 256 * 1024;

    /// <summary>JSON arguments through a source-generated <paramref name="typeInfo"/>, written as <paramref name="version"/>.</summary>
    public static IJobArgs<TArgs> Json<TArgs>(JsonTypeInfo<TArgs> typeInfo, int version = 1)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(version);
        return new JsonArgs<TArgs>(typeInfo, version);
    }

    /// <summary>
    /// JSON arguments through reflection with <paramref name="options"/> (<see cref="JsonSerializerOptions.Default"/>
    /// when null), written as <paramref name="version"/>. Trimmed and Native AOT applications pass a source-generated
    /// <see cref="JsonTypeInfo{T}"/> instead; with the same options both write the same JSON.
    /// </summary>
    [RequiresUnreferencedCode(ReflectionJson.Unreferenced)]
    [RequiresDynamicCode(ReflectionJson.Dynamic)]
    public static IJobArgs<TArgs> Json<TArgs>(JsonSerializerOptions? options = null, int version = 1) =>
        Json(ReflectionJson.TypeInfo<TArgs>(options), version);

    private sealed class JsonArgs<TArgs>(JsonTypeInfo<TArgs> typeInfo, int version) : IJobArgs<TArgs>
    {
        public int Version => version;

        public byte[] Write(TArgs args) => JsonSerializer.SerializeToUtf8Bytes(args, typeInfo);

        public TArgs Read(ReadOnlySpan<byte> data, int version) =>
            JsonSerializer.Deserialize(data, typeInfo) ?? throw new JsonException("Job arguments are null.");
    }
}

/// <summary>
/// What every process that requests, cancels or runs a job agrees on: its key, revision, subject kind and shared
/// policy. Stored by revision; the newest stored revision is enforced everywhere.
/// </summary>
public abstract class JobDefinition
{
    private protected JobDefinition(string key, long revision, string? subject, JobPolicy policy)
    {
        Key = key;
        Revision = revision;
        Subject = subject;
        Policy = policy;
        Digest = ComputeDigest(key, subject, policy);
    }

    /// <summary>The stable registered name, never a CLR name.</summary>
    public string Key { get; }

    /// <summary>Increases whenever the shared policy changes.</summary>
    public long Revision { get; }

    /// <summary>The subject kind the definition's jobs act on, or null.</summary>
    public string? Subject { get; }

    public JobPolicy Policy { get; }

    /// <summary>A digest of the shared content; equal revisions must have equal digests.</summary>
    public byte[] Digest { get; }

    /// <summary>The name of the definition's lease source.</summary>
    public string SourceName => SourcePrefix + Key;

    /// <summary>The name of the lease source of the definition with <paramref name="key"/>.</summary>
    public static string SourceNameOf(string key) => SourcePrefix + key;

    internal const string SourcePrefix = "job:";

    public override string ToString() => Key;

    /// <summary>Defines a job whose arguments are <typeparamref name="TArgs"/>.</summary>
    public static JobDefinition<TArgs> Create<TArgs>(string key, long revision, IJobArgs<TArgs> args, JobPolicy? policy = null, string? subject = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        if (key.Any(char.IsControl))
        {
            throw new ArgumentException("A definition key cannot contain control characters.", nameof(key));
        }
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(revision);
        ArgumentNullException.ThrowIfNull(args);
        if (subject is not null)
        {
            ArgumentException.ThrowIfNullOrEmpty(subject);
        }
        policy ??= new JobPolicy();
        Validate(policy);
        return new JobDefinition<TArgs>(key, revision, subject, policy, args);
    }

    /// <summary>Defines a job whose arguments are <typeparamref name="TArgs"/> and whose result is <typeparamref name="TResult"/> (J20).</summary>
    public static JobDefinition<TArgs, TResult> Create<TArgs, TResult>(string key, long revision, IJobArgs<TArgs> args, IJobResults<TResult> result, JobPolicy? policy = null, string? subject = null)
    {
        ArgumentNullException.ThrowIfNull(result);
        var untyped = Create(key, revision, args, policy, subject);
        return new JobDefinition<TArgs, TResult>(untyped.Key, untyped.Revision, untyped.Subject, untyped.Policy, args, result);
    }

    internal static void Validate(JobPolicy policy)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(policy.Attempts, nameof(policy));
        ArgumentNullException.ThrowIfNull(policy.Backoff, nameof(policy));
        ArgumentNullException.ThrowIfNull(policy.Late, nameof(policy));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(policy.MinimumArgsVersion, nameof(policy));
        foreach (var limit in new[] { policy.PartitionLimit, policy.DefinitionLimit })
        {
            if (limit is < 1)
            {
                throw new ArgumentException("Limits must be at least 1.", nameof(policy));
            }
        }
        if (policy.Timeout is { } timeout)
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero, nameof(policy));
        }
        foreach (var retention in new[] { policy.Retention, policy.RunHistoryRetention })
        {
            if (retention is { } kept)
            {
                ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(kept, TimeSpan.Zero, nameof(policy));
            }
        }
        if (!Enum.IsDefined(policy.Priority))
        {
            throw new ArgumentException("Unknown priority.", nameof(policy));
        }
        if (policy.Expect is { IsEmpty: true })
        {
            throw new ArgumentException("An expectation sets at least one of CompletesBy, RunsAtMost and FailsAtMostInARow.", nameof(policy));
        }
    }

    private static byte[] ComputeDigest(string key, string? subject, JobPolicy policy)
    {
        IEnumerable<string> lines =
        [
            key,
            subject ?? "",
            policy.Attempts.ToString(CultureInfo.InvariantCulture),
            ((int)policy.Backoff.Kind).ToString(CultureInfo.InvariantCulture),
            policy.Backoff.Initial.Ticks.ToString(CultureInfo.InvariantCulture),
            policy.Backoff.Maximum.Ticks.ToString(CultureInfo.InvariantCulture),
            policy.PartitionLimit?.ToString(CultureInfo.InvariantCulture) ?? "",
            policy.DefinitionLimit?.ToString(CultureInfo.InvariantCulture) ?? "",
            ((int)policy.Priority).ToString(CultureInfo.InvariantCulture),
            policy.Timeout?.Ticks.ToString(CultureInfo.InvariantCulture) ?? "",
            policy.RetryTimeouts ? "1" : "0",
            policy.Late.Grace?.Ticks.ToString(CultureInfo.InvariantCulture) ?? "",
            policy.MinimumArgsVersion.ToString(CultureInfo.InvariantCulture),
            policy.RunHistory ? "1" : "0",
            policy.Retention?.Ticks.ToString(CultureInfo.InvariantCulture) ?? "",
            policy.RunHistoryRetention?.Ticks.ToString(CultureInfo.InvariantCulture) ?? "",
        ];
        // Expectations are appended only when set, so a definition without them keeps its digest.
        if (policy.Expect is { } expect)
        {
            lines = lines.Concat([
                "expect",
                expect.Deadline is { } deadline ? $"{deadline.Time.Ticks.ToString(CultureInfo.InvariantCulture)} {deadline.TimeZone} {deadline.Calendar}" : "",
                expect.RunningLimit?.Ticks.ToString(CultureInfo.InvariantCulture) ?? "",
                expect.FailureLimit?.ToString(CultureInfo.InvariantCulture) ?? "",
            ]);
        }
        return SHA256.HashData(Encoding.UTF8.GetBytes(string.Join("\n", lines)));
    }
}

/// <summary>A job definition with arguments of type <typeparamref name="TArgs"/>.</summary>
public class JobDefinition<TArgs> : JobDefinition
{
    internal JobDefinition(string key, long revision, string? subject, JobPolicy policy, IJobArgs<TArgs> args)
        : base(key, revision, subject, policy) => Args = args;

    public IJobArgs<TArgs> Args { get; }
}

/// <summary>A job definition with arguments of type <typeparamref name="TArgs"/> and a result of type <typeparamref name="TResult"/>.</summary>
public sealed class JobDefinition<TArgs, TResult> : JobDefinition<TArgs>
{
    internal JobDefinition(string key, long revision, string? subject, JobPolicy policy, IJobArgs<TArgs> args, IJobResults<TResult> results)
        : base(key, revision, subject, policy, args) => Results = results;

    public IJobResults<TResult> Results { get; }
}

/// <summary>
/// The newest definition as stored, which every process enforces. Its policy's retention durations are set: those
/// the declared policy left unset were taken from the defaults of the host that stored the revision.
/// </summary>
public sealed record StoredJobDefinition(string Key, long Revision, string? Subject, JobPolicy Policy, byte[] Digest, bool Paused);

/// <summary>A process declared a definition whose revision equals the stored one but whose content differs.</summary>
public sealed class JobDefinitionConflictException(string message) : Exception(message);

/// <summary>A claim found a newer stored definition than the one its statements were built from; nothing was claimed.</summary>
public sealed class JobDefinitionChangedException(string definition, long revision) : Exception($"Job definition {definition} is stored at revision {revision}.")
{
    public string Definition { get; } = definition;

    public long Revision { get; } = revision;
}
