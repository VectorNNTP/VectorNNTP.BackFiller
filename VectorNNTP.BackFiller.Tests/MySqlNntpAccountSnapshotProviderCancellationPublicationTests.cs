// <copyright file="MySqlNntpAccountSnapshotProviderCancellationPublicationTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Behavior and contract tests for my sql nntp account snapshot provider cancellation publication.

using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Accounts;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Tests cancellation ownership around snapshot publication boundaries.
    /// </summary>
    public sealed class MySqlNntpAccountSnapshotProviderCancellationPublicationTests
    {
        /// <summary>
        /// Verifies the RefreshSnapshotAsync_WhenCanceledAfterQueryCompletes_DoesNotPublishAndThrowsCancellation scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task RefreshSnapshotAsync_WhenCanceledAfterQueryCompletes_DoesNotPublishAndThrowsCancellation()
        {
            NntpAccountSnapshot initial = BuildAccount(Guid.Parse("11111111-1111-1111-1111-111111111111"));
            NntpAccountSnapshot refreshed = BuildAccount(Guid.Parse("22222222-2222-2222-2222-222222222222"));

            TaskCompletionSource secondQueryEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
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

                    secondQueryEntered.SetResult();

                    TaskCompletionSource canceled = new(TaskCreationOptions.RunContinuationsAsynchronously);
                    using CancellationTokenRegistration _ = cancellationToken.Register(
                        static state => ((TaskCompletionSource)state!).TrySetResult(),
                        canceled);

                    await canceled.Task.ConfigureAwait(false);
                    return [refreshed];
                });

            await provider.LoadInitialSnapshotAsync(CancellationToken.None);
            NntpAccountSnapshotState baseline = provider.CurrentSnapshot;

            using CancellationTokenSource cts = new();
            Task<bool> refreshTask = provider.RefreshSnapshotAsync(cts.Token);

            await secondQueryEntered.Task.ConfigureAwait(false);
            cts.Cancel();

            _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await refreshTask.ConfigureAwait(false));

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


