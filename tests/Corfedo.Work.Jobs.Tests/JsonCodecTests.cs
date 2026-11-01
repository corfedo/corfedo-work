using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace Corfedo.Work.Jobs.Tests;

/// <summary>The host's JSON options for job arguments and results, and codecs of a definition's own.</summary>
public sealed partial class JsonCodecTests
{
    public sealed record Args(string Name, int Count);

    public sealed record Missing(string Name);

    [JsonSerializable(typeof(Args))]
    internal sealed partial class Context : JsonSerializerContext;

    private static readonly JobDefinition<Args, Args> Report = JobDefinition.Create<Args, Args>("reports.build");

    [Fact]
    public void TheDefaultRoundTripsThroughReflection()
    {
        var args = JobJson.Default.Args(Report);

        var written = args.Write(new Args("npm", 3));

        Assert.Equal(1, args.Version);
        Assert.Equal(new Args("npm", 3), args.Read(written, 1));
        Assert.Equal(new Args("npm", 3), JobJson.Default.Results(Report).Read(written, 1));
    }

    [Fact]
    public void TheDefaultWritesWhatAContextWithoutOptionsWrites()
    {
        var args = new Args("npm", 3);

        Assert.Equal(JsonSerializer.SerializeToUtf8Bytes(args, Context.Default.Args), JobJson.Default.Args(Report).Write(args));
    }

    [Fact]
    public void UseJsonResolvesThroughTheContextWithTheConfiguredOptions()
    {
        var json = new JobJson([
            options => options.TypeInfoResolver = JsonTypeInfoResolver.Combine(),
            options => options.TypeInfoResolverChain.Insert(0, Context.Default),
            options => options.PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        ]);

        var written = json.Args(Report).Write(new Args("npm", 3));

        Assert.Equal("""{"name":"npm","count":3}""", Encoding.UTF8.GetString(written));
    }

    [Fact]
    public void ACodecOfTheDefinitionWinsOverTheHostOptions()
    {
        var definition = JobDefinition.Create("reports.versioned", 1, JobArgs.Json(Context.Default.Args, version: 2), JobResults.Json(Context.Default.Args, version: 3));
        var json = new JobJson([options => options.PropertyNamingPolicy = JsonNamingPolicy.CamelCase]);

        var args = json.Args(definition);

        Assert.Equal(2, args.Version);
        Assert.Equal(3, json.Results(definition).Version);
        Assert.Equal("""{"Name":"npm","Count":3}""", Encoding.UTF8.GetString(args.Write(new Args("npm", 3))));
    }

    [Fact]
    public void ATypeTheResolversLackIsNamedWithTheFix()
    {
        var json = new JobJson([options => options.TypeInfoResolver = Context.Default]);

        var refused = Assert.Throws<InvalidOperationException>(() => json.Args(JobDefinition.Create<Missing>("reports.missing")));

        Assert.Contains("reports.missing", refused.Message, StringComparison.Ordinal);
        Assert.Contains("[JsonSerializable(typeof(Missing))]", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ACodecOfTheDefinitionRefusesAnInvalidVersion() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => JobArgs.Json(Context.Default.Args, version: 0));
}
