using System.Text;

namespace Corfedo.Work;

/// <summary>
/// Describes an exception for framework logs by its type chain and stack frames, without messages or other properties:
/// messages and provider details can carry arguments, subject identifiers and key values.
/// </summary>
internal static class ExceptionDescription
{
    public static string Describe(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        var text = new StringBuilder();
        Append(text, exception, 0);
        return text.ToString();
    }

    private static void Append(StringBuilder text, Exception exception, int depth)
    {
        if (depth > 0)
        {
            text.AppendLine().Append(" ---> ");
        }
        text.Append(exception.GetType().FullName);
        if (exception.StackTrace is { } frames)
        {
            text.AppendLine().Append(frames);
        }
        if (exception is AggregateException aggregate)
        {
            foreach (var inner in aggregate.InnerExceptions)
            {
                Append(text, inner, depth + 1);
            }
        }
        else if (exception.InnerException is { } inner)
        {
            Append(text, inner, depth + 1);
        }
    }
}
