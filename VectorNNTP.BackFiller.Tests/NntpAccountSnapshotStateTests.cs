using VectorNNTP.Backfiller.Runtime.Accounts;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

/// <summary>
/// Tests immutable account snapshot state creation semantics.
/// </summary>
public sealed class NntpAccountSnapshotStateTests
{
    [Fact]
    public void Empty_ReturnsServerIdAndNoAccounts()
    {
        const byte serverId = 12;

        NntpAccountSnapshotState snapshotState = NntpAccountSnapshotState.Empty(serverId);

        Assert.Equal(serverId, snapshotState.ServerId);
        Assert.Empty(snapshotState.Accounts);
    }
}
