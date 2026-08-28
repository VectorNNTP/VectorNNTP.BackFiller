using VectorNNTP.Backfiller.Runtime.Accounts;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests immutable account snapshot state creation semantics.
    /// </summary>
    public sealed class NntpAccountSnapshotStateTests
    {
        [Fact]
        public void Empty_ReturnsServerIdAndNoAccounts()
        {
            const byte ServerId = 12;

            NntpAccountSnapshotState snapshotState = NntpAccountSnapshotState.Empty(ServerId);

            Assert.Equal(ServerId, snapshotState.ServerId);
            Assert.Empty(snapshotState.Accounts);
        }
    }
}
