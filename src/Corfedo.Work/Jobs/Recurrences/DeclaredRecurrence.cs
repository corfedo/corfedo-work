using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Corfedo.Work;

/// <summary>
/// A code-defined recurrence of a definition without a subject kind (J10): part of the definition's revision, stored
/// once whatever the number of processes that declare it. A newer revision replaces the stored recurrence, an older
/// one never does, and an equal revision with different content is a startup error.
/// </summary>
public sealed class DeclaredRecurrence
{
    internal DeclaredRecurrence(JobDefinition definition, RecurrenceData data)
    {
        Definition = definition;
        Data = data;
        Digest = ComputeDigest(data);
    }

    public JobDefinition Definition { get; }

    internal RecurrenceData Data { get; }

    /// <summary>A digest of the schedule, policies and arguments; equal revisions must have equal digests.</summary>
    internal byte[] Digest { get; }

    private static byte[] ComputeDigest(RecurrenceData data)
    {
        var schedule = data.Schedule;
        var text = string.Join(
            "\n",
            ((int)schedule.Kind).ToString(CultureInfo.InvariantCulture),
            schedule.Period.Ticks.ToString(CultureInfo.InvariantCulture),
            ((int)schedule.Interval).ToString(CultureInfo.InvariantCulture),
            schedule.Anchor.UtcTicks.ToString(CultureInfo.InvariantCulture),
            schedule.Expression ?? "",
            schedule.TimeZone ?? "",
            ((int)data.Missed).ToString(CultureInfo.InvariantCulture),
            ((int)data.Overlap).ToString(CultureInfo.InvariantCulture),
            data.Partition ?? "",
            data.ArgsVersion.ToString(CultureInfo.InvariantCulture),
            Convert.ToBase64String(data.Args));
        if (schedule.Calendar is { } calendar)
        {
            text += "\n" + calendar + "\n" + ((int)schedule.Closed).ToString(CultureInfo.InvariantCulture);
        }
        return SHA256.HashData(Encoding.UTF8.GetBytes(text));
    }
}
