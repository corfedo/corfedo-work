# Trimming and Native AOT

`Corfedo.Work`, `Corfedo.Work.PostgreSql` and `Corfedo.Work.InMemory` are trimming- and Native AOT-compatible. The EF
Core adapter is not, since EF Core supports Native AOT only partly, and neither is the conformance suite, which is a
test library.

## Job arguments and results

`JobArgs.Json<T>()` and `JobResults.Json<T>()` serialize with reflection. The trimming and AOT analyzers report their
call sites (IL2026, IL3050). For a trimmed or Native AOT application, pass the type information of a source-generated
`JsonSerializerContext` instead:

```csharp
public sealed record SyncFeed(string Name);
public sealed record SyncReport(int Items);

[JsonSerializable(typeof(SyncFeed))]
[JsonSerializable(typeof(SyncReport))]
internal sealed partial class AppJson : JsonSerializerContext;

public static class Jobs
{
    public static readonly JobDefinition<SyncFeed> FeedSync =
        JobDefinition.Create("feeds.sync", 1, JobArgs.Json(AppJson.Default.SyncFeed));

    public static readonly JobDefinition<SyncFeed, SyncReport> FeedReport =
        JobDefinition.Create("feeds.report", 1, JobArgs.Json(AppJson.Default.SyncFeed), JobResults.Json(AppJson.Default.SyncReport));
}
```

Both forms write the same JSON when they use the same options: the defaults of `JsonSerializerOptions` match a context
without `JsonSourceGenerationOptions`. An application can switch from one to the other and still read the arguments
and results it already stored. If you pass options to `Json<T>(options)`, give the context the same settings.
