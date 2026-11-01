# Trimming and Native AOT

`Corfedo.Work`, `Corfedo.Work.PostgreSql` and `Corfedo.Work.InMemory` are trimming- and Native AOT-compatible. The EF
Core adapter is not, since EF Core supports Native AOT only partly, and neither is the conformance suite, which is a
test library.

## Job arguments and results

Job arguments and results are serialized with the host's JSON options. Without further configuration they use
reflection, which a trimmed or Native AOT application does not have. Register a source-generated
`JsonSerializerContext` once for all jobs with `UseJson`:

```csharp
public sealed record SyncFeed(string Name);
public sealed record SyncReport(int Items);

[JsonSerializable(typeof(SyncFeed))]
[JsonSerializable(typeof(SyncReport))]
internal sealed partial class AppJson : JsonSerializerContext;

public static class Jobs
{
    public static readonly JobDefinition<SyncFeed> FeedSync = JobDefinition.Create<SyncFeed>("feeds.sync");

    public static readonly JobDefinition<SyncFeed, SyncReport> FeedReport = JobDefinition.Create<SyncFeed, SyncReport>("feeds.report");
}

builder.Services.AddWork(work => work
    .UseJson(AppJson.Default)
    .AddJob<SyncFeed, FeedSyncHandler>(Jobs.FeedSync)
    .AddJob<SyncFeed, SyncReport, FeedReportHandler>(Jobs.FeedReport));
```

The host resolves the type of every registered definition when it starts. A type missing from the context stops
startup with an error that names it:

```
Job definition feeds.report has results of type SyncReport, which the host's JSON options cannot serialize. Add
[JsonSerializable(typeof(SyncReport))] to the application's JsonSerializerContext and register it with UseJson.
```

`UseJson` puts the context in front of the options' resolvers; `ConfigureJson` changes the options themselves, such as
their naming policy or converters. Give the context the same settings as the options: the defaults of
`JsonSerializerOptions` match a context without `JsonSourceGenerationOptions`, so an application can switch between
reflection and a context and still read the arguments and results it already stored.
