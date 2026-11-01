using System.Text.Json;
using System.Text.Json.Serialization;

namespace Corfedo.Work.Jobs.Tests;

/// <summary>Reflection-based and source-generated JSON codecs for job arguments and results.</summary>
public sealed partial class JsonCodecTests
{
    public sealed record Args(string Name, int Count);

    [JsonSerializable(typeof(Args))]
    internal sealed partial class Context : JsonSerializerContext;

    [Fact]
    public void ReflectionArgumentsWriteWhatTheSourceGeneratedCodecReads()
    {
        var args = new Args("npm", 3);
        var reflection = JobArgs.Json<Args>(version: 2);
        var generated = JobArgs.Json(Context.Default.Args, 2);

        var written = reflection.Write(args);

        Assert.Equal(2, reflection.Version);
        Assert.Equal(generated.Write(args), written);
        Assert.Equal(args, generated.Read(written, 2));
        Assert.Equal(args, reflection.Read(written, 2));
    }

    [Fact]
    public void ReflectionResultsUseTheGivenOptions()
    {
        var results = JobResults.Json<Args>(new JsonSerializerOptions(JsonSerializerDefaults.Web));

        var written = results.Write(new Args("npm", 3));

        Assert.Equal("""{"name":"npm","count":3}""", System.Text.Encoding.UTF8.GetString(written));
        Assert.Equal(new Args("npm", 3), results.Read(written, 1));
    }

    [Fact]
    public void ReflectionArgumentsRefuseAnInvalidVersion() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => JobArgs.Json<Args>(version: 0));
}
