using System.Security.Cryptography;
using System.Text;

namespace Corfedo.Work.Conformance;

/// <summary>Holders outside the process that claim, renew and complete through the application's API with tokens.</summary>
public abstract class RemoteHolderScenarios(ILeaseConformanceProvider provider) : LeaseScenarioBase(provider)
{
    [Fact]
    public async Task W1_VanishedRemoteHolderIsReclaimedAndItsLateResultRefused()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromMilliseconds(600)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var api = await ClientAsync(source);
        var remote = OwnerId.New();
        var token = (await api.Operations.ClaimDueAsync(source, 1, Cancel, remote)).Single().Lease.Token;
        Assert.True((await api.Operations.RenewAsync(source.ParseToken(token), Cancel)).Valid);

        var reclaimed = await ClaimEventuallyAsync(api.Operations, source, key, TimeSpan.FromSeconds(5));
        Assert.Equal((ClaimReason.Takeover, remote), (reclaimed.Reason, reclaimed.PreviousOwner!.Value));

        await using var late = await BeginAsync();
        await late.WriteProgressAsync(source.Store, key, "late result", Cancel);
        await Assert.ThrowsAsync<LeaseLostException>(async () => await api.Operations.EndAsync(source.ParseToken(token), late.Work, cancellationToken: Cancel));
        await late.RollbackAsync(Cancel);
        Assert.False((await api.Operations.RenewAsync(source.ParseToken(token), Cancel)).Valid);
        Assert.Null((await ReadAsync(source, key)).Progress);
    }

    [Fact]
    public async Task W2_OwnerLimitHoldsOnEveryClaimPath()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30), p => p with { OwnerLimit = 2 }));
        var keys = await InsertManyAsync(source, 5);
        var api = await ClientAsync(source);
        var remote = OwnerId.New();

        var due = await api.Operations.ClaimDueAsync(source, 5, Cancel, remote);
        Assert.Equal(2, due.Count);
        var unclaimed = keys.Where(k => due.All(c => c.Key != k)).ToArray();
        Assert.Null(await api.Operations.ClaimAsync(source, unclaimed[0], Cancel, remote));
        await using (var transaction = await BeginAsync())
        {
            Assert.Empty(await api.Operations.ClaimAsync(transaction.Work, source, unclaimed, 5, Cancel, remote));
            await transaction.CommitAsync(Cancel);
        }

        Assert.True(await api.Operations.ReleaseAsync(due[0].Lease, Due.None, cancellationToken: Cancel));
        Assert.NotNull(await api.Operations.ClaimAsync(source, unclaimed[0], Cancel, remote));
        Assert.Null(await api.Operations.ClaimAsync(source, unclaimed[1], Cancel, remote));
        Assert.Equal(2, (await api.Operations.ClaimDueAsync(source, 5, Cancel, OwnerId.New())).Count);
    }

    [Fact]
    public async Task W2_OwnerLimitCountsATakenOverLeaseForItsNewOwner()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromMilliseconds(300), p => p with { OwnerLimit = 1 }));
        var keys = await InsertManyAsync(source, 3);
        var api = await ClientAsync(source);
        var first = OwnerId.New();
        var second = OwnerId.New();
        Assert.NotNull(await api.Operations.ClaimAsync(source, keys[0], Cancel, first));
        await ExpiredAsync(source, keys[0], TimeSpan.FromSeconds(5));

        // Nothing settles the expired lease, so it still counts for its owner, who may take it back but claim no other.
        Assert.Null(await api.Operations.ClaimAsync(source, keys[1], Cancel, first));
        var retaken = await api.Operations.ClaimAsync(source, keys[0], Cancel, first);
        Assert.Equal((ClaimReason.Takeover, first), (retaken!.Reason, retaken.PreviousOwner!.Value));
        await ExpiredAsync(source, keys[0], TimeSpan.FromSeconds(5));

        var takeover = await api.Operations.ClaimAsync(source, keys[0], Cancel, second);
        Assert.Equal((ClaimReason.Takeover, first), (takeover!.Reason, takeover.PreviousOwner!.Value));
        Assert.Null(await api.Operations.ClaimAsync(source, keys[1], Cancel, second));
        Assert.NotNull(await api.Operations.ClaimAsync(source, keys[1], Cancel, first));
        Assert.Null(await api.Operations.ClaimAsync(source, keys[2], Cancel, first));
    }

    [Fact]
    public async Task W3_ReplayedCompletionReturnsTheRecordedOutcome()
    {
        var source = await SourceAsync(Policy(TimeSpan.FromSeconds(30)));
        var key = new TestKey("t", 1);
        await InsertAsync(source, key, TimeSpan.Zero);
        var api = await ClientAsync(source);
        var token = (await api.Operations.ClaimDueAsync(source, 1, Cancel, OwnerId.New())).Single().Lease.Token;
        var completion = new LeaseCompletion(Guid.NewGuid(), SHA256.HashData(Encoding.UTF8.GetBytes("result 42")));

        async Task<EndResult> CompleteAsync(LeaseCompletion sent, string output)
        {
            await using var transaction = await BeginAsync();
            var result = await api.Operations.EndAsync(source.ParseToken(token), transaction.Work, sent, Cancel);
            if (result == EndResult.Ended)
            {
                await transaction.WriteProgressAsync(source.Store, key, output, Cancel);
            }
            await transaction.CommitAsync(Cancel);
            return result;
        }

        Assert.Equal(EndResult.Ended, await CompleteAsync(completion, "result 42"));
        Assert.Equal(EndResult.Replayed, await CompleteAsync(completion, "result 42"));
        var different = completion with { Digest = SHA256.HashData(Encoding.UTF8.GetBytes("result 43")) };
        await Assert.ThrowsAsync<LeaseLostException>(() => CompleteAsync(different, "result 43"));
        Assert.Equal("result 42", (await ReadAsync(source, key)).Progress);
    }

    [Fact]
    public async Task W2_OwnerLimitIsCountedPerSource()
    {
        var first = await SourceAsync(Policy(TimeSpan.FromSeconds(30), p => p with { OwnerLimit = 1 }));
        var second = await SourceAsync(Policy(TimeSpan.FromSeconds(30), p => p with { OwnerLimit = 1 }));
        await InsertManyAsync(first, 2);
        await InsertManyAsync(second, 2);
        var api = await ProcessAsync(work => work.AddLeaseSource(first).AddLeaseSource(second), start: false);
        var remote = OwnerId.New();

        Assert.Single(await api.Operations.ClaimDueAsync(first, 2, Cancel, remote));
        Assert.Single(await api.Operations.ClaimDueAsync(second, 2, Cancel, remote));
    }
}
