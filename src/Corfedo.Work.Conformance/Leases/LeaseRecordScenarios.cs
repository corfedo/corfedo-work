using Corfedo.Work.Testing;
using Microsoft.Extensions.DependencyInjection;

namespace Corfedo.Work.Conformance;

/// <summary>Typed reads of lease records and ageing of records for tests on the store's clock.</summary>
public abstract class LeaseRecordScenarios(ILeaseConformanceProvider provider) : LeaseScenarioBase(provider)
{
    [Fact]
    public async Task L32_ReadsReturnTheLeaseFieldsWithTheStoreClock()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromMinutes(1)));
        var due = new TestKey("t", 1);
        var later = new TestKey("t", 2);
        var missing = new TestKey("t", 3);
        await InsertAsync(source, due, TimeSpan.FromSeconds(-1));
        await InsertAsync(source, later, TimeSpan.FromHours(1));
        var client = await ClientAsync(source);
        var operations = client.Operations;

        var unclaimed = await operations.ReadAsync(source, due, Cancel);

        Assert.NotNull(unclaimed);
        Assert.True(unclaimed.DueAt <= unclaimed.ReadAt);
        Assert.Equal((null, 0L, null, 0, 0, null), (unclaimed.Owner, unclaimed.Epoch, unclaimed.ExpiresAt, unclaimed.Takeovers, unclaimed.Failures, unclaimed.CancelRequestedAt));
        var waiting = await operations.ReadAsync(source, later, Cancel);
        Assert.NotNull(waiting);
        Assert.InRange(waiting.DueAt!.Value - waiting.ReadAt, TimeSpan.FromMinutes(59), TimeSpan.FromHours(1));
        Assert.Null(await operations.ReadAsync(source, missing, Cancel));

        var owner = OwnerId.New();
        var claim = await operations.ClaimAsync(source, due, Cancel, owner);
        Assert.NotNull(claim);
        var held = await operations.ReadAsync(source, due, Cancel);
        Assert.NotNull(held);
        Assert.Equal((owner, 1L, claim.ExecutionId), (held.Owner, held.Epoch, held.ExecutionId));
        Assert.InRange(held.ExpiresAt!.Value - held.ReadAt, TimeSpan.Zero, TimeSpan.FromMinutes(1));

        // Many keys in one read: duplicates once, missing keys left out.
        var many = await operations.ReadManyAsync(source, [due, later, missing, due], Cancel);
        Assert.Equal([due, later], many.Keys.OrderBy(k => k.Id));
        Assert.Equal(many[due].ReadAt, many[later].ReadAt);
        Assert.Equal(held with { ReadAt = many[due].ReadAt }, many[due]);
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await operations.ReadManyAsync(source, Enumerable.Range(1, LeaseOperations.MaximumKeys + 1).Select(i => new TestKey("t", i)), Cancel));

        // In a transaction, its own uncommitted writes; outside it, the committed record.
        await using (var transaction = await BeginAsync())
        {
            Assert.True(await operations.RequestCancelAsync(transaction.Work, source, due, Cancel));
            var seen = await operations.ReadAsync(transaction.Work, source, due, Cancel);
            Assert.NotNull(seen);
            Assert.NotNull(seen.CancelRequestedAt);
            Assert.True(seen.CancelRequestedAt <= seen.ReadAt);
            Assert.Single(await operations.ReadManyAsync(transaction.Work, source, [due, missing], Cancel));
            Assert.Null((await operations.ReadAsync(source, due, Cancel))!.CancelRequestedAt);
            await transaction.RollbackAsync(Cancel);
        }
        Assert.Null((await operations.ReadAsync(source, due, Cancel))!.CancelRequestedAt);
    }

    [Fact]
    public async Task L33_AgeingARecordMovesItsTimesAsIfTheSpanHadPassed()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromMinutes(1), p => p with { TakeoverBackoff = Backoff.Constant(TimeSpan.FromMinutes(1)) }));
        var later = new TestKey("t", 1);
        var now = new TestKey("t", 2);
        await InsertAsync(source, later, TimeSpan.FromHours(1));
        await InsertAsync(source, now, TimeSpan.Zero);
        var client = await ClientAsync(source);
        var operations = client.Operations;
        var ageing = Ageing(client);
        var first = OwnerId.New();
        var second = OwnerId.New();

        // Due in an hour, aged by an hour: due.
        Assert.Null(await operations.ClaimAsync(source, later, Cancel, first));
        var waiting = (await operations.ReadAsync(source, later, Cancel))!;
        Assert.True(await ageing.AgeAsync(source, later, TimeSpan.FromHours(1), Cancel));
        Assert.Equal(waiting.DueAt - TimeSpan.FromHours(1), (await operations.ReadAsync(source, later, Cancel))!.DueAt);
        Assert.NotNull(await operations.ClaimAsync(source, later, Cancel, first));

        // Held, aged by the lease duration: expired, and not taken over within the takeover backoff.
        await using (var transaction = await BeginAsync())
        {
            Assert.True(await operations.RequestCancelAsync(transaction.Work, source, later, Cancel));
            await transaction.CommitAsync(Cancel);
        }
        var held = (await operations.ReadAsync(source, later, Cancel))!;
        Assert.Null(await operations.ClaimAsync(source, later, Cancel, second));
        Assert.True(await ageing.AgeAsync(source, later, TimeSpan.FromMinutes(1), Cancel));
        var expired = (await operations.ReadAsync(source, later, Cancel))!;
        Assert.Equal(held.ExpiresAt - TimeSpan.FromMinutes(1), expired.ExpiresAt);
        Assert.Equal(held.DueAt - TimeSpan.FromMinutes(1), expired.DueAt);
        Assert.Equal(held.CancelRequestedAt - TimeSpan.FromMinutes(1), expired.CancelRequestedAt);
        Assert.True(expired.ExpiresAt <= expired.ReadAt);
        Assert.Null(await operations.ClaimAsync(source, later, Cancel, second));

        // Aged past the takeover backoff: taken over.
        Assert.True(await ageing.AgeAsync(source, later, TimeSpan.FromMinutes(1), Cancel));
        var takeover = await operations.ClaimAsync(source, later, Cancel, second);
        Assert.NotNull(takeover);
        Assert.Equal((ClaimReason.Takeover, first, 2L), (takeover.Reason, takeover.PreviousOwner, takeover.Lease.Epoch));

        // Due now, aged by minus an hour: postponed by an hour.
        Assert.True(await ageing.AgeAsync(source, now, TimeSpan.FromHours(-1), Cancel));
        Assert.Null(await operations.ClaimAsync(source, now, Cancel, first));
        var postponed = (await operations.ReadAsync(source, now, Cancel))!;
        Assert.InRange(postponed.DueAt!.Value - postponed.ReadAt, TimeSpan.FromMinutes(59), TimeSpan.FromHours(1));

        Assert.False(await ageing.AgeAsync(source, new TestKey("t", 3), TimeSpan.FromHours(1), Cancel));

        // On a source that settles expiry, ageing also moves the settlement retry field.
        var settling = await SourceAsync(Policy(TimeSpan.FromMinutes(1), p => p with { SettleExpiry = true }));
        await InsertAsync(settling, now, TimeSpan.Zero);
        var before = (await operations.ReadAsync(settling, now, Cancel))!;
        Assert.True(await ageing.AgeAsync(settling, now, TimeSpan.FromMinutes(5), Cancel));
        Assert.Equal(before.DueAt - TimeSpan.FromMinutes(5), (await operations.ReadAsync(settling, now, Cancel))!.DueAt);
    }

    [Fact]
    public async Task L34_AgeingARecordDueWakesItsDispatcher()
    {
        if (!Provider.HasSignals)
        {
            Assert.Skip("The store has no signals.");
        }
        var source = await SourceAsync(Policy(TimeSpan.FromMinutes(1)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.FromHours(1));
        var ran = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var script = new HandlerScript
        {
            Run = async (run, cancellationToken) =>
            {
                await EndAsync(run, cancellationToken);
                ran.TrySetResult();
            },
        };
        var worker = await WorkerAsync(source, script, pollingInterval: TimeSpan.FromMinutes(1));
        // Gives the dispatcher's first pass time to find nothing due, so that only the signal wakes it in time.
        await Task.Delay(TimeSpan.FromMilliseconds(500), Cancel);

        Assert.True(await Ageing(worker).AgeAsync(source, key, TimeSpan.FromHours(1), Cancel));

        await ran.Task.WaitAsync(TimeSpan.FromSeconds(10), Cancel);
    }

    private static RecordAgeing Ageing(TestProcess process) => new(process.Services.GetRequiredService<IWorkStore>());
}
