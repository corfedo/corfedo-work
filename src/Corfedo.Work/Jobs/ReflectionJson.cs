using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Corfedo.Work;

/// <summary>Reflection-based JSON type information for job arguments and results.</summary>
internal static class ReflectionJson
{
    public const string Unreferenced = "Reflection-based JSON may need members that trimming removes. Pass the JsonTypeInfo of a source-generated JsonSerializerContext instead.";

    public const string Dynamic = "Reflection-based JSON may need code generated at run time. Pass the JsonTypeInfo of a source-generated JsonSerializerContext instead.";

    [RequiresUnreferencedCode(Unreferenced)]
    [RequiresDynamicCode(Dynamic)]
    public static JsonTypeInfo<T> TypeInfo<T>(JsonSerializerOptions? options)
    {
        options ??= JsonSerializerOptions.Default;
        // Locks the options and adds the reflection resolver if none is set, as the first serialization would.
        options.MakeReadOnly(populateMissingResolver: true);
        return (JsonTypeInfo<T>)options.GetTypeInfo(typeof(T));
    }
}
