// <copyright file="MySqlNntpAccountSnapshotProviderCancellationTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Behavior and contract tests for my sql nntp account snapshot provider cancellation.

using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Accounts;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests cancellation and publication semantics for account snapshot loading.
    /// </summary>
    public sealed class MySqlNntpAccountSnapshotProviderCancellationTests
    {
        /// <summary>
        /// Verifies the LoadInitialSnapshotAsync_DoesNotApplyProviderOwnedTimeoutAndPreservesSnapshotUntilCallerCancels scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task LoadInitialSnapshotAsync_DoesNotApplyProviderOwnedTimeoutAndPreservesSnapshotUntilCallerCancels()
        {
            NntpAccountSnapshot initial = BuildAccount(Guid.Parse("11111111-1111-1111-1111-111111111111"));

            int queryCallCount = 0;
            MySqlNntpAccountSnapshotProvider provider = new(
                1,
                NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                async cancellationToken =>
                {
                    queryCallCount++;

                    if (queryCallCount == 1)
                    {
                        return [initial];
                    }

                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                    return [];
                });

            await provider.LoadInitialSnapshotAsync(CancellationToken.None);
            NntpAccountSnapshotState baseline = provider.CurrentSnapshot;

            using CancellationTokenSource cts = new();
            Task reloadTask = provider.LoadInitialSnapshotAsync(cts.Token);

            Task completedTask = await Task.WhenAny(reloadTask, Task.Delay(TimeSpan.FromSeconds(6)));
            Assert.NotSame(reloadTask, completedTask);

            cts.Cancel();

            _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reloadTask.ConfigureAwait(false));

            Assert.Same(baseline, provider.CurrentSnapshot);
            _ = Assert.Single(provider.CurrentSnapshot.Accounts);
            Assert.Equal(initial.EntryId, provider.CurrentSnapshot.Accounts[0].EntryId);
        }

        /// <summary>
        /// Verifies the BuildAccount scenario and expected contract.
        /// </summary>
        private static NntpAccountSnapshot BuildAccount(Guid entryId)
        {
            return new NntpAccountSnapshot(
                EntryId: entryId,
                Backbone: "BackboneA",
                Hostname: "news.example.com",
                KeepAliveSeconds: 120,
                MaxConnections: 10,
                Password: "secret",
                Port: 563,
                ServerId: 1,
                Username: "user",
                UseSsl: true);
        }
    }
}


