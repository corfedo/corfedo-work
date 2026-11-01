using System.Collections.Concurrent;

namespace Corfedo.Work.Conformance;

/// <summary>Process roles (R10): a process runs exactly what it registers, and no role depends on another running.</summary>
public abstract class ProcessRoleScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    [Fact]
    public async Task H1_TwoRolesOnOneStoreEachClaimOnlyTheirOwnDefinitionsAndNeitherNeedsTheOtherRunning()
    {
        var (exports, imports) = (Definition(), Definition());
        var (exportRuns, importRuns) = (new ConcurrentQueue<(string Definition, OwnerId Owner)>(), new ConcurrentQueue<(string Definition, OwnerId Owner)>());
        JobScript Recording(ConcurrentQueue<(string Definition, OwnerId Owner)> runs) => new()
        {
            Run = (_, run, _) =>
            {
                runs.Enqueue((run.Definition.Key, run.Owner));
                return ValueTask.FromResult<JobDisposition>(JobResult.Completed);
            },
        };
        // The export role runs exports and requests imports, which it does not run.
        var exporter = await WorkerAsync(exports, Recording(exportRuns), also: work => work.AddJobDefinition(imports), pollingInterval: TimeSpan.FromMilliseconds(200));

        var export = await RequestAsync(exporter, exports, "export");
        var import = await RequestAsync(exporter, imports, "import");

        Assert.Equal(JobOutcome.Completed, (await EndedAsync(export.Id, TimeSpan.FromSeconds(10))).State);
        await Task.Delay(1000, Cancel);
        Assert.Null((await ReadJobAsync(import.Id)).State);

        // The import role starts later and runs the import; neither role ran the other's definition.
        var importer = await WorkerAsync(imports, Recording(importRuns), also: work => work.AddJobDefinition(exports), pollingInterval: TimeSpan.FromMilliseconds(200));
        Assert.Equal(JobOutcome.Completed, (await EndedAsync(import.Id, TimeSpan.FromSeconds(10))).State);
        await RequestAsync(importer, exports, "export-from-importer");
        await EventuallyAsync(() => exportRuns.Count == 2, TimeSpan.FromSeconds(10), "the export requested by the import role runs in the export role");

        Assert.Equal([(exports.Key, exporter.Owner), (exports.Key, exporter.Owner)], exportRuns);
        Assert.Equal([(imports.Key, importer.Owner)], importRuns);
    }
}
