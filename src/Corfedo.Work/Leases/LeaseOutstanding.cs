namespace Corfedo.Work;

/// <summary>
/// The definition of <see cref="LeaseFields.LeaseOutstanding"/>, which providers verify against the definition their
/// catalog reports for the column.
/// </summary>
internal static class LeaseOutstanding
{
    /// <summary>The column's expression with identifiers quoted by <paramref name="quote"/>.</summary>
    public static string Expression(LeaseFields fields, Func<string, string> quote) =>
        $"CASE WHEN {quote(fields.SettledEpoch)} < {quote(fields.LeaseEpoch)} THEN 1 END";

    /// <summary>
    /// Whether a catalog's definition is the expression: compared without case, white space, parentheses and quoting,
    /// and with the <c>ELSE NULL</c> branch catalogs spell out implied.
    /// </summary>
    public static bool Matches(string? definition, LeaseFields fields) =>
        definition is not null && Normalize(definition) == Normalize(Expression(fields, c => c));

    private static string Normalize(string text)
    {
        var bare = new string([.. text.ToLowerInvariant().Where(c => !char.IsWhiteSpace(c) && c is not ('(' or ')' or '[' or ']' or '"'))]);
        return bare.Replace("elsenull::integer", "", StringComparison.Ordinal).Replace("elsenull", "", StringComparison.Ordinal);
    }
}
