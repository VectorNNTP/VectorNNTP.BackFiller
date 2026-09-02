// <copyright file="NntpAccountSnapshotStateTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Behavior and contract tests for nntp account snapshot state.

using VectorNNTP.Backfiller.Runtime.Accounts;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests immutable account snapshot state creation semantics.
    /// </summary>
    public sealed class NntpAccountSnapshotStateTests
    {
        /// <summary>
        /// Verifies the Empty_ReturnsServerIdAndNoAccounts scenario and expected contract.
        /// </summary>
        [Fact]
        public void Empty_ReturnsServerIdAndNoAccounts()
        {
            /// <summary>
            /// Stores the ServerId fixture value used by these tests.
            /// </summary>
            const byte ServerId = 12;

            NntpAccountSnapshotState snapshotState = NntpAccountSnapshotState.Empty(ServerId);

            Assert.Equal(ServerId, snapshotState.ServerId);
            Assert.Empty(snapshotState.Accounts);
        }
    }
}
