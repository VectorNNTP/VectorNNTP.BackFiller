// <copyright file="NntpAccountSnapshotStateTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for nntp account snapshot state, covering NNTP article and transport behavior.
// Primary responsibility: documents the executable contracts covered by the nntp account snapshot state test suite.

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
        /// Verifies the empty returns server id and no accounts scenario and its documented contract.
        /// </summary>
        [Fact]
        public void Empty_ReturnsServerIdAndNoAccounts()
        {
            /// <summary>
            /// Supplies server id for the fixture or scenario under test.
            /// </summary>
            const byte ServerId = 12;

            NntpAccountSnapshotState snapshotState = NntpAccountSnapshotState.Empty(ServerId);

            Assert.Equal(ServerId, snapshotState.ServerId);
            Assert.Empty(snapshotState.Accounts);
        }
    }
}
