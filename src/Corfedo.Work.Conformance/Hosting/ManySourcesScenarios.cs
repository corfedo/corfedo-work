using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>
/// A process with many sources (H-8): its pools, settling sources and job definitions share one loop per kind, and one
/// pass that waits does not stop the others of its kind.
/// </summary>
public abstract class ManySourcesScenarios(IJobConformanceProvider provider) : JobScenarioBase(provider)
{
    [Fact]
    public async Task H8_AProcessWithManyDefinitionsAndPoolsRunsEveryRequestedJobAndPlansItsRecurrence()
    {
        var own = Enumerable.Range(0, 6).Select(_ => Definition()).ToArray();
        var pooled = Enumerable.Range(0, 2).Select(_ => Definition()).ToArray();
        var runs = new ConcurrentQueue<string>();
        var script = new JobScript
        {
            Run = (args, _, _) =>
            {
                runs.Enqueue(args.Value);
                return ValueTask.FromResult<JobDisposition>(JobResult.Completed);
            },
        };
        var process = await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(script);
                work.AddPool("shared", capacity: 2);
                foreach (var definition in own)
                {
                    work.AddJob<TestArgs, ScriptedJobHandler>(definition, job => job.Lease(TimeSpan.FromSeconds(2)));
                }
                foreach (var definition in pooled)
                {
                    work.AddJob<TestArgs, ScriptedJobHandler>(definition, job => job.Lease(TimeSpan.FromSeconds(2)).Pool("shared"));
                }
                work.AddRecurring(own[0], new TestArgs("occurrence"), Schedule.Every(TimeSpan.FromSeconds(1), Interval.FixedRate));
            },
            pollingInterval: TimeSpan.FromMilliseconds(200));

        var requested = new List<JobRequestResult>();
        foreach (var (definition, index) in own.Concat(pooled).Select((d, i) => (d, i)))
        {
            requested.Add(await RequestAsync(process, definition, $"job-{index}"));
        }

        foreach (var request in requested)
        {
            Assert.Equal(JobOutcome.Completed, (await EndedAsync(request.Id, TimeSpan.FromSeconds(10))).State);
        }
        await EventuallyAsync(() => runs.Contains("occurrence"), TimeSpan.FromSeconds(10), "an occurrence of the recurrence runs");
        Assert.Equal(Enumerable.Range(0, 8).Select(i => $"job-{i}").Order(), runs.Where(r => r != "occurrence").Order());
    }

    [Fact]
    public async Task H8_AProcessSettlesTheExpiredLeasesOfItsJobDefinitionsInTheirOnePass()
    {
        var definitions = Enumerable.Range(0, 6).Select(_ => Definition(new JobPolicy { Backoff = Backoff.None, RunHistory = true })).ToArray();
        var client = await ClientAsync(null, definitions);
        var first = await RequestAsync(client, definitions[1], "first");
        var second = await RequestAsync(client, definitions[4], "second");
        // A process that held both jobs crashed: the first lease expires soon, the second a second later.
        var firstClaim = await CrashClaimAsync(client, definitions[1], TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(5));
        var secondClaim = await CrashClaimAsync(client, definitions[4], TimeSpan.FromMilliseconds(1500), TimeSpan.FromSeconds(5));
        // Paused definitions are not claimed, so only expiry settlement, which ignores pauses, ends the attempts.
        await InTransactionAsync(async transaction =>
        {
            await Jobs(client).PauseAsync(transaction, definitions[1], Cancel);
            await Jobs(client).PauseAsync(transaction, definitions[4], Cancel);
        });
        var runs = new ConcurrentQueue<string>();
        var script = new JobScript
        {
            Run = (args, _, _) =>
            {
                runs.Enqueue(args.Value);
                return ValueTask.FromResult<JobDisposition>(JobResult.Completed);
            },
        };
        // The polling interval is longer than the test, so the settlements come from the expiry pass.
        var worker = await ProcessAsync(
            work =>
            {
                work.Services.AddSingleton(script);
                foreach (var definition in definitions)
                {
                    work.AddJob<TestArgs, ScriptedJobHandler>(definition, job => job.Lease(TimeSpan.FromSeconds(2)));
                }
            },
            pollingInterval: TimeSpan.FromMinutes(5));

        foreach (var (job, claim, definition) in new[] { (first, firstClaim, definitions[1]), (second, secondClaim, definitions[4]) })
        {
            JobRunEntry? lost = null;
            await EventuallyAsync(
                async () => (lost = (await Jobs(worker).GetRunsAsync(definition, job.Id, cancellationToken: Cancel)).Runs.SingleOrDefault()) is not null,
                TimeSpan.FromSeconds(10),
                $"the expired lease of {definition.Key} is settled");
            Assert.Equal((JobRunOutcome.LeaseLost, claim.Lease.Epoch), (lost!.Outcome, lost.Epoch));
            Assert.Equal(((JobOutcome?)null, false), ((await ReadJobAsync(job.Id)).State, (await ReadJobAsync(job.Id)).Held));
        }
        Assert.Empty(runs);
    }

    [Fact]
    public async Task H8_AnExpiryHandlerThatWaitsDoesNotStopSettlementOfAnotherSourceInTheProcess()
    {
        RequireConcurrentTransactions();
        var waiting = await SourceAsync(Policy(TimeSpan.FromMilliseconds(300), p => p with { SettleExpiry = true }));
        var other = await SourceAsync(Policy(TimeSpan.FromMilliseconds(1500), p => p with { SettleExpiry = true }));
        var key = new TestKey("t", 1);
        await InsertAsync(waiting, key, TimeSpan.Zero);
        await InsertAsync(other, key, TimeSpan.Zero);
        var handlerWaits = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var expiry = new ExpiryScript
        {
            Expired = async (expired, transaction, cancellationToken) =>
            {
                if (expired.Source.Name == waiting.Name)
                {
                    handlerWaits.TrySetResult();
                    await gate.Task.WaitAsync(cancellationToken);
                }
                await Provider.JournalAsync(transaction, expired.Source.Store, $"release {expired.Key} {expired.Epoch}", cancellationToken);
            },
        };
        // A process that held both records crashed: the waiting source's lease expires first, the other's later.
        var crashed = await ProcessAsync(
            work =>
            {
                work.AddLeaseSource(waiting);
                work.AddLeaseSource(other);
            },
            start: false);
        await using (var transaction = await BeginAsync())
        {
            await crashed.Operations.ClaimAsync(transaction.Work, waiting, [key], 1, Cancel);
            await crashed.Operations.ClaimAsync(transaction.Work, other, [key], 1, Cancel);
            await transaction.CommitAsync(Cancel);
        }
        await ProcessAsync(work =>
        {
            work.Services.AddSingleton(expiry);
            work.AddLeaseSource(waiting, s => s.OnExpired<ScriptedExpiryHandler>());
            work.AddLeaseSource(other, s => s.OnExpired<ScriptedExpiryHandler>());
        });

        try
        {
            await handlerWaits.Task.WaitAsync(TimeSpan.FromSeconds(5), Cancel);
            await EventuallyAsync(async () => (await JournalAsync(other)).Count == 1, TimeSpan.FromSeconds(5), "the other source's lease is settled while the handler waits");
            Assert.Empty(await JournalAsync(waiting));
        }
        finally
        {
            gate.TrySetResult();
        }
        await EventuallyAsync(async () => (await JournalAsync(waiting)).Count == 1, TimeSpan.FromSeconds(5), "the waiting source's lease is settled once its handler returns");
    }
}
