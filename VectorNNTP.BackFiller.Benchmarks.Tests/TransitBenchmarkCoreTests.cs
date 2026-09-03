// <copyright file="TransitBenchmarkCoreTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for transit benchmark core, covering NNTP article and transport behavior; benchmark measurement and runtime identity contracts.
// Primary responsibility: documents the executable contracts covered by the transit benchmark core test suite.

using System.Text;
using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests
{
    /// <summary>
    /// Confirms the transit benchmark core tests behavior.
    /// </summary>
    public sealed class TransitBenchmarkCoreTests
    {
        /// <summary>
        /// Confirms the producer timing from raw produces reconcilable active and blocked buckets behavior.
        /// </summary>
        [Fact]
        public void ProducerTiming_FromRaw_ProducesReconcilableActiveAndBlockedBuckets()
        {
            TransitBenchmarkCore.ProducerTiming timing = TransitBenchmarkCore.ProducerTiming.FromRaw(
                loopTicks: 1_000,
                generationTicks: 300,
                blockedTicks: 700,
                otherActiveTicks: 250);

            Assert.Equal(1_000, timing.LoopTicks);
            Assert.Equal(700, timing.BlockedTicks);
            Assert.Equal(300, timing.GenerationTicks);
            Assert.Equal(0, timing.OtherActiveTicks);
            Assert.Equal(300, timing.ActiveTicks);
            Assert.Equal(timing.LoopTicks, timing.ActiveTicks + timing.BlockedTicks);
        }
        /// <summary>
        /// Confirms the bounded article queue enforces byte budget behavior.
        /// </summary>
        [Fact]
        public async Task BoundedArticleQueue_EnforcesByteBudget()
        {
            TransitBenchmarkCore.ArticlePayload first = TransitBenchmarkCore.ArticlePayload.Create("<a@benchmark.usenet.ninja>", 8);
            TransitBenchmarkCore.ArticlePayload second = TransitBenchmarkCore.ArticlePayload.Create("<b@benchmark.usenet.ninja>", 8);

            long maxResidentBytes = first.Length + second.Length - 1L;

            using TransitBenchmarkCore.BoundedArticleQueue queue = new(maxArticles: 10, maxResidentBytes: maxResidentBytes);

            bool firstReleased = false;
            bool secondReleased = false;

            try
            {
                bool firstQueued = await queue.TryWriteAsync(new TransitBenchmarkCore.QueuedArticle("<a@benchmark.usenet.ninja>", first), CancellationToken.None);
                Assert.True(firstQueued);

                Task<bool> secondAdmissionTask = queue.TryWriteAsync(
                    new TransitBenchmarkCore.QueuedArticle("<b@benchmark.usenet.ninja>", second),
                    CancellationToken.None).AsTask();

                await Task.Yield();
                Assert.False(secondAdmissionTask.IsCompleted);

                Assert.True(queue.TryRead(out TransitBenchmarkCore.QueuedArticle dequeuedFirst));
                queue.ReleaseReservation(dequeuedFirst.Payload.Length);
                dequeuedFirst.Payload.Dispose();
                firstReleased = true;

                bool secondQueued = await secondAdmissionTask;
                Assert.True(secondQueued);

                Assert.True(queue.TryRead(out TransitBenchmarkCore.QueuedArticle dequeuedSecond));
                queue.ReleaseReservation(dequeuedSecond.Payload.Length);
                dequeuedSecond.Payload.Dispose();
                secondReleased = true;
            }
            finally
            {
                if (!firstReleased)
                {
                    first.Dispose();
                }

                if (!secondReleased)
                {
                    second.Dispose();
                }
            }
        }
        /// <summary>
        /// Confirms the byte budget acquire canceled throws operation canceled behavior.
        /// </summary>
        [Fact]
        public async Task ByteBudget_AcquireCanceled_ThrowsOperationCanceled()
        {
            using TransitBenchmarkCore.ByteBudget budget = new(maxBytes: 1);
            await budget.AcquireAsync(1, CancellationToken.None);

            using CancellationTokenSource cts = new(TimeSpan.FromMilliseconds(50));
            _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                await budget.AcquireAsync(1, cts.Token).ConfigureAwait(false);
            });
        }
        /// <summary>
        /// Confirms the byte budget dispose cancels pending acquire behavior.
        /// </summary>
        [Fact]
        public async Task ByteBudget_Dispose_CancelsPendingAcquire()
        {
            using TransitBenchmarkCore.ByteBudget budget = new(maxBytes: 1);
            await budget.AcquireAsync(1, CancellationToken.None);

            ValueTask pending = budget.AcquireAsync(1, CancellationToken.None);
            budget.Dispose();

            _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pending.ConfigureAwait(false));
        }
        /// <summary>
        /// Confirms the build message id produces unique valid message id across worker and sequence behavior.
        /// </summary>
        [Fact]
        public void BuildMessageId_ProducesUniqueValidMessageIdAcrossWorkerAndSequence()
        {
            HashSet<string> seen = new(StringComparer.Ordinal);
            long instance = 123456;

            for (int worker = 0; worker < 4; worker++)
            {
                for (int seq = 1; seq <= 1000; seq++)
                {
                    string id = TransitBenchmarkCore.BuildMessageId(instance, worker, seq, "measure");
                    Assert.StartsWith("<", id, StringComparison.Ordinal);
                    Assert.EndsWith(">", id, StringComparison.Ordinal);
                    Assert.Contains("@benchmark.usenet.ninja>", id, StringComparison.Ordinal);
                    Assert.True(seen.Add(id));
                }
            }
        }
        /// <summary>
        /// Confirms the article payload create produces expected headers and cr lf terminated payload behavior.
        /// </summary>
        [Fact]
        public void ArticlePayload_Create_ProducesExpectedHeadersAndCrLfTerminatedPayload()
        {
            string messageId = "<payload-test@benchmark.usenet.ninja>";
            TransitBenchmarkCore.ArticlePayload payload = TransitBenchmarkCore.ArticlePayload.Create(messageId, 128 * 1024);

            try
            {
                string text = Encoding.ASCII.GetString(payload.AsMemory().Span);

                Assert.Contains($"Message-ID: {messageId}\r\n", text, StringComparison.Ordinal);
                Assert.Contains("Date: ", text, StringComparison.Ordinal);
                Assert.Contains("From: benchmark@usenet.ninja\r\n", text, StringComparison.Ordinal);
                Assert.Contains("Newsgroups: alt.test\r\n", text, StringComparison.Ordinal);
                Assert.Contains("Subject: BackFiller TransitPublisher benchmark workload\r\n", text, StringComparison.Ordinal);
                Assert.Contains("Path: benchmark.usenet.ninja\r\n", text, StringComparison.Ordinal);
                Assert.Contains("\r\n\r\n", text, StringComparison.Ordinal);
                Assert.EndsWith("\r\n", text, StringComparison.Ordinal);
            }
            finally
            {
                payload.Dispose();
            }
        }
        /// <summary>
        /// Confirms the validate int range when within range returns value behavior.
        /// </summary>
        [Theory]
        [InlineData(1, 1, 64, "connections")]
        [InlineData(64, 1, 64, "connections")]
        [InlineData(8, 1, 64, "pipeline-depth")]
        public void ValidateIntRange_WhenWithinRange_ReturnsValue(int value, int min, int max, string option)
        {
            int actual = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(value, min, max, option);
            Assert.Equal(value, actual);
        }
        /// <summary>
        /// Confirms the validate int range when out of range throws behavior.
        /// </summary>
        [Theory]
        [InlineData(0, 1, 64, "connections")]
        [InlineData(65, 1, 64, "connections")]
        public void ValidateIntRange_WhenOutOfRange_Throws(int value, int min, int max, string option)
        {
            _ = Assert.Throws<InvalidOperationException>(() => TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateIntRange(value, min, max, option));
        }
        /// <summary>
        /// Confirms the validate long range when within range returns value behavior.
        /// </summary>
        [Theory]
        [InlineData(64L * 1024 * 1024, 64L * 1024 * 1024, 2L * 1024 * 1024 * 1024, "queue-mib")]
        [InlineData(256L * 1024 * 1024, 64L * 1024 * 1024, 2L * 1024 * 1024 * 1024, "queue-mib")]
        public void ValidateLongRange_WhenWithinRange_ReturnsValue(long value, long min, long max, string option)
        {
            long actual = TransitBenchmarkCore.TransitBenchmarkConfigValidator.ValidateLongRange(value, min, max, option);
            Assert.Equal(value, actual);
        }
    }
}
