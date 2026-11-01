using System.Buffers.Text;
using System.Globalization;
using System.Text;

namespace Corfedo.Work;

internal static class LeaseToken
{
    private const string Version = "1";
    private const char Separator = '\n';

    public static string Encode(string source, string key, OwnerId owner, long epoch)
    {
        var text = string.Join(Separator, Version, source, owner.Value.ToString("N"), epoch.ToString(CultureInfo.InvariantCulture), key);
        return Base64Url.EncodeToString(Encoding.UTF8.GetBytes(text));
    }

    public static (string Source, string Key, OwnerId Owner, long Epoch) Decode(string token)
    {
        ArgumentNullException.ThrowIfNull(token);
        string text;
        try
        {
            text = Encoding.UTF8.GetString(Base64Url.DecodeFromChars(token));
        }
        catch (FormatException)
        {
            throw new FormatException("The lease token is malformed.");
        }
        var parts = text.Split(Separator, 5);
        if (parts.Length != 5
            || parts[0] != Version
            || !Guid.TryParseExact(parts[2], "N", out var owner)
            || !long.TryParse(parts[3], NumberStyles.None, CultureInfo.InvariantCulture, out var epoch))
        {
            throw new FormatException("The lease token is malformed.");
        }
        return (parts[1], parts[4], new OwnerId(owner), epoch);
    }
}
