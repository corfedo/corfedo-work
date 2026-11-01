using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Corfedo.Work;

/// <summary>
/// The host's JSON options for job arguments and results of definitions without a codec of their own. They start
/// from <see cref="JsonSerializerDefaults.General"/> with the reflection resolver while reflection-based serialization
/// is enabled; under Native AOT only the resolvers the host adds answer.
/// </summary>
internal sealed class JobJson
{
    public static JobJson Default { get; } = new([]);

    public JobJson(IReadOnlyList<Action<JsonSerializerOptions>> configure)
    {
        var options = CreateOptions();
        foreach (var action in configure)
        {
            action(options);
        }
        // Without a resolver every type is reported missing when a definition is registered, not when the options lock.
        options.TypeInfoResolver ??= JsonTypeInfoResolver.Combine();
        options.MakeReadOnly();
        Options = options;
    }

    public JsonSerializerOptions Options { get; }

    /// <summary>The arguments codec of <paramref name="definition"/>: its own, or JSON version 1 with these options.</summary>
    /// <exception cref="InvalidOperationException">The options cannot serialize the argument type.</exception>
    public IJobArgs<TArgs> Args<TArgs>(JobDefinition<TArgs> definition) =>
        definition.Args ?? new JobArgs.JsonArgs<TArgs>(TypeInfo<TArgs>(definition.Key, "arguments"), 1);

    /// <summary>The result codec of <paramref name="definition"/>: its own, or JSON version 1 with these options.</summary>
    /// <exception cref="InvalidOperationException">The options cannot serialize the result type.</exception>
    public IJobResults<TResult> Results<TArgs, TResult>(JobDefinition<TArgs, TResult> definition) =>
        definition.Results ?? new JobResults.JsonResults<TResult>(TypeInfo<TResult>(definition.Key, "results"), 1);

    private JsonTypeInfo<T> TypeInfo<T>(string definition, string role)
    {
        try
        {
            return (JsonTypeInfo<T>)Options.GetTypeInfo(typeof(T));
        }
        catch (NotSupportedException exception)
        {
            throw new InvalidOperationException(
                $"Job definition {definition} has {role} of type {typeof(T).Name}, which the host's JSON options cannot serialize. " +
                $"Add [JsonSerializable(typeof({typeof(T).Name}))] to the application's JsonSerializerContext and register it with UseJson.",
                exception);
        }
    }

    // The reflection resolver is added only while the feature switch enables reflection-based serialization, which
    // trimmed and Native AOT applications turn off; the trimmer then removes it. System.Text.Json and ASP.NET Core
    // build their default options the same way.
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Used only while reflection-based serialization is enabled.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Used only while reflection-based serialization is enabled.")]
    private static JsonSerializerOptions CreateOptions()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.General);
        if (JsonSerializer.IsReflectionEnabledByDefault)
        {
            options.TypeInfoResolver = new DefaultJsonTypeInfoResolver();
        }
        return options;
    }
}
