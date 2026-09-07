// <copyright file="ArticleRetentionSweepServiceTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime / Articles / Retention
// Focused tests for retention sweep shutdown ordering relative to article-processing drain completion.

using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Processing;
using VectorNNTP.Backfiller.Runtime.Articles.Retention;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Articles.Retention
{
    /// <summary>
    /// Verifies retention shutdown admission closure waits for the processing-drain barrier.
    /// </summary>
    public sealed class ArticleRetentionSweepServiceTests
    {
        [Fact]
        public async Task StopAsync_WhenProcessingDrainNotCompleted_DoesNotCloseRetentionAdmissionEarly()
        {
            ArticleProcessingDrainBarrier barrier = new();
            TrackingRetentionAuthority retentionAuthority = new();
            using ArticleRetentionSweepService service = new(retentionAuthority, barrier);

            await service.StartAsync(CancellationToken.None).ConfigureAwait(false);

            using CancellationTokenSource stopTimeout = new(TimeSpan.FromSeconds(10));
            Task stopTask = service.StopAsync(stopTimeout.Token);
            await Task.Yield();

            Assert.False(stopTask.IsCompleted);
            Assert.Equal(0, retentionAuthority.BeginShutdownCallCount);

            barrier.SignalProcessingLoopCompleted();
            await stopTask.WaitAsync(stopTimeout.Token).ConfigureAwait(false);

            Assert.Equal(1, retentionAuthority.BeginShutdownCallCount);
        }

        [Fact]
        public async Task StopAsync_WhenProcessingScopeStillInFlight_WaitsForScopeExitBeforeClosingAdmission()
        {
            ArticleProcessingDrainBarrier barrier = new();
            barrier.EnterProcessingScope();
            TrackingRetentionAuthority retentionAuthority = new();
            using ArticleRetentionSweepService service = new(retentionAuthority, barrier);

            await service.StartAsync(CancellationToken.None).ConfigureAwait(false);

            using CancellationTokenSource stopTimeout = new(TimeSpan.FromSeconds(10));
            Task stopTask = service.StopAsync(stopTimeout.Token);

            barrier.SignalProcessingLoopCompleted();
            await Task.Yield();

            Assert.False(stopTask.IsCompleted);
            Assert.Equal(0, retentionAuthority.BeginShutdownCallCount);

            barrier.ExitProcessingScope();
            await stopTask.WaitAsync(stopTimeout.Token).ConfigureAwait(false);

            Assert.Equal(1, retentionAuthority.BeginShutdownCallCount);
        }

        private sealed class TrackingRetentionAuthority : IArticleRetentionAuthority
        {
            public int BeginShutdownCallCount { get; private set; }

            public TimeSpan SweepInterval => TimeSpan.FromMinutes(5);

            public ArticleRetentionAdmissionResult TryRetainSuccessArticle(string messageId, DownloadedArticleBuffer payloadOwner, DateTimeOffset? insertedUtc = null)
            {
                throw new NotSupportedException();
            }

            public ArticleRetentionReadLeaseResult TryAcquireReadLeaseByMessageId(string messageId)
            {
                throw new NotSupportedException();
            }

            public ArticleRetentionReadLeaseResult TryAcquireReadLeaseByMessageIdMd5(string messageIdMd5)
            {
                throw new NotSupportedException();
            }

            public ArticleRetentionCompletionResult MarkTransitCompleted(string messageId)
            {
                throw new NotSupportedException();
            }

            public ArticleRetentionCompletionResult MarkListenerCompleted(string messageId)
            {
                throw new NotSupportedException();
            }

            public long ExpireEligibleArticles(DateTimeOffset nowUtc)
            {
                return 0;
            }

            public void BeginShutdown()
            {
                BeginShutdownCallCount++;
            }

            public ArticleRetentionSnapshot GetSnapshot(DateTimeOffset? nowUtc = null)
            {
                throw new NotSupportedException();
            }
        }
    }
}
