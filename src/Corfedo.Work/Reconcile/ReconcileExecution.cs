using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace Corfedo.Work;

/// <summary>
/// Execution ids of reconciliation runs. A run's execution is the revision it converges under one source generation:
/// retries of the same revision keep the id, a new request or invalidation produces a new one. Every provider derives
/// it the same way, so external operations deduplicate on it across processes (R17).
/// </summary>
public static class ReconcileExecution
{
    /// <summary>A name-based UUID (version 8) from the source, the canonical key, the revision and the generation.</summary>
    public static Guid Id(string source, string key, long revision, long generation)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(key);
        var text = Encoding.UTF8.GetBytes(source + "\n" + key);
        var input = new byte[text.Length + 16];
        text.CopyTo(input, 0);
        BinaryPrimitives.WriteInt64BigEndian(input.AsSpan(text.Length), revision);
        BinaryPrimitives.WriteInt64BigEndian(input.AsSpan(text.Length + 8), generation);
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(input, hash);
        hash[6] = (byte)((hash[6] & 0x0F) | 0x80);
        hash[8] = (byte)((hash[8] & 0x3F) | 0x80);
        return new Guid(hash[..16], bigEndian: true);
    }
}
