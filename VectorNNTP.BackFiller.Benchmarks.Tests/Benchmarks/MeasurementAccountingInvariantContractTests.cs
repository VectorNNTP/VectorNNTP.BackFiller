// <copyright file="MeasurementAccountingInvariantContractTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Focused tests for measurement accounting invariants and mutation guards.

using System.Diagnostics;
using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Validates accounting invariants and mutation-sensitive benchmark throughput contracts.
    /// </summary>
    public sealed class MeasurementAccountingInvariantContractTests
    {
        [Fact]
        public async Task DispatchLoopAsync_WhenPublishThrowsAfterAdmission_AdmittedArticlesStillTerminalizeExactlyOnceAsync()
        {
            const int articleCount = 7;
            string[] messageIds = [.. Enumerable.Range(1, articleCount).Select(static i => $"<throw-{i}@benchmark.usenet.ninja>")];
            byte[] payload = [(byte)'X', (byte)'\n'];

            using BoundedArticleQueue queue = new(maxArticles: 64, maxResidentBytes: 16L * 1024L * 1024L);
            MeasurementMetrics metrics = new(articleBytes: payload.Length);
            metrics.MarkMeasurementStart(Stopwatch.GetTimestamp());
            PreparedBenchmarkWorkload workload = new(messageIds, payload, new WorkloadPreparationSummary(0, 0, messageIds.Length, messageIds.Length, 0, payload.Length));

            foreach (string messageId in messageIds)
            {
                bool queued = await queue.TryWriteAsync(new QueuedArticle(messageId, payload.Length), CancellationToken.None);
                Assert.True(queued);
            }

            queue.StopAdmission();

            ValueTask<TransitPublishResult> ThrowingPublishAsync(string messageId, ReadOnlyMemory<byte> articlePayload, CancellationToken cancellationToken)
            {
                _ = messageId;
                _ = articlePayload;
                _ = cancellationToken;
                throw new InvalidOperationException("forced publish failure");
            }

            Dictionary<string, int> terminalCounts = new(StringComparer.Ordinal);
            Dictionary<string, TransitPublishStatus> terminalStatusByMessageId = new(StringComparer.Ordinal);
            object gate = new();

            void ObserveTerminal(TransitPublishResult result)
            {
                lock (gate)
                {
                    terminalCounts.TryGetValue(result.MessageId, out int count);
                    terminalCounts[result.MessageId] = count + 1;
                    terminalStatusByMessageId[result.MessageId] = result.Status;
                }
            }

            Task[] dispatchers =
            [
                Task.Run(() => MeasurementExecutionEngine.DispatchLoopAsync(queue, null, metrics, workload, null, CancellationToken.None, false, ThrowingPublishAsync, terminalObserver: ObserveTerminal)),
                Task.Run(() => MeasurementExecutionEngine.DispatchLoopAsync(queue, null, metrics, workload, null, CancellationToken.None, false, ThrowingPublishAsync, terminalObserver: ObserveTerminal))
            ];

            await Task.WhenAll(dispatchers);

            MeasurementSnapshot snapshot = metrics.Snapshot();

            Assert.Equal(articleCount, snapshot.AdmittedCount);
            Assert.Equal(articleCount, snapshot.CompletedCount);
            Assert.Equal(articleCount, snapshot.FailedCount);
            Assert.Equal(articleCount, snapshot.AmbiguousCount);
            Assert.Equal(0, snapshot.AcceptedCount);
            Assert.Equal(0, snapshot.RejectedCount);
            Assert.Equal(0, snapshot.UnavailableCount);
            Assert.Equal(0, snapshot.CanceledCount);
            Assert.True(snapshot.CompletedCount <= snapshot.AdmittedCount);

            Assert.Equal(articleCount, terminalCounts.Count);
            Assert.All(messageIds, id => Assert.True(terminalCounts.TryGetValue(id, out int count) && count == 1));
            Assert.All(messageIds, id => Assert.Equal(TransitPublishStatus.Failed, terminalStatusByMessageId[id]));
        }

        [Fact]
        public async Task DispatchLoopAsync_WhenPublishCancellationOccursAfterAdmission_ArticlesAreCountedAsCanceledExactlyOnceAsync()
        {
            const int articleCount = 5;
            string[] messageIds = [.. Enumerable.Range(1, articleCount).Select(static i => $"<cancel-{i}@benchmark.usenet.ninja>")];
            byte[] payload = [(byte)'Y', (byte)'\n'];

            using BoundedArticleQueue queue = new(maxArticles: 64, maxResidentBytes: 16L * 1024L * 1024L);
            MeasurementMetrics metrics = new(articleBytes: payload.Length);
            metrics.MarkMeasurementStart(Stopwatch.GetTimestamp());
            PreparedBenchmarkWorkload workload = new(messageIds, payload, new WorkloadPreparationSummary(0, 0, messageIds.Length, messageIds.Length, 0, payload.Length));

            foreach (string messageId in messageIds)
            {
                bool queued = await queue.TryWriteAsync(new QueuedArticle(messageId, payload.Length), CancellationToken.None);
                Assert.True(queued);
            }

            queue.StopAdmission();

            ValueTask<TransitPublishResult> CanceledPublishAsync(string messageId, ReadOnlyMemory<byte> articlePayload, CancellationToken cancellationToken)
            {
                _ = messageId;
                _ = articlePayload;
                throw new OperationCanceledException("forced cancel", cancellationToken);
            }

            using CancellationTokenSource cts = new();

            Dictionary<string, int> terminalCounts = new(StringComparer.Ordinal);
            Dictionary<string, TransitPublishStatus> terminalStatusByMessageId = new(StringComparer.Ordinal);
            object gate = new();

            void ObserveTerminal(TransitPublishResult result)
            {
                lock (gate)
                {
                    terminalCounts.TryGetValue(result.MessageId, out int count);
                    terminalCounts[result.MessageId] = count + 1;
                    terminalStatusByMessageId[result.MessageId] = result.Status;
                }
            }

            Task[] dispatchers =
            [
                Task.Run(() => MeasurementExecutionEngine.DispatchLoopAsync(queue, null, metrics, workload, null, cts.Token, false, CanceledPublishAsync, terminalObserver: ObserveTerminal)),
                Task.Run(() => MeasurementExecutionEngine.DispatchLoopAsync(queue, null, metrics, workload, null, cts.Token, false, CanceledPublishAsync, terminalObserver: ObserveTerminal))
            ];

            await Task.WhenAll(dispatchers);

            MeasurementSnapshot snapshot = metrics.Snapshot();

            Assert.Equal(articleCount, snapshot.AdmittedCount);
            Assert.Equal(articleCount, snapshot.CompletedCount);
            Assert.Equal(articleCount, snapshot.CanceledCount);
            Assert.True(snapshot.CompletedCount <= snapshot.AdmittedCount);

            Assert.Equal(articleCount, terminalCounts.Count);
            Assert.All(messageIds, id => Assert.True(terminalCounts.TryGetValue(id, out int count) && count == 1));
            Assert.All(messageIds, id => Assert.Equal(TransitPublishStatus.Canceled, terminalStatusByMessageId[id]));
        }

        [Fact]
        public void MutationGuards_AcceptedWindowAndDurationAndClassificationContractsHold()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig(measurementSeconds: 10);
            MeasurementMetrics metrics = new(articleBytes: 1024);

            long startTick = Stopwatch.GetTimestamp();
            long boundaryTick = startTick + 10_000;
            metrics.MarkMeasurementStart(startTick);

            metrics.OnGenerated(1024, startTick + 10, TransitBenchmarkCore.ProducerTiming.FromRaw(10, 3, 7, 0), 7);
            metrics.OnAdmitted(1024, startTick + 11);
            metrics.OnPublishResult(new TransitPublishResult("<pre@benchmark.usenet.ninja>", TransitPublishStatus.Accepted, 239, "ok", 1, 1, 1, 1, 1, 1, 1, 1), 1024, startTick + 11, startTick + 12, startTick + 13, 1, 0, completionTickOverride: boundaryTick);

            metrics.MarkMeasurementBoundary(DateTimeOffset.UtcNow, boundaryTick);

            metrics.OnGenerated(1024, boundaryTick + 1, TransitBenchmarkCore.ProducerTiming.FromRaw(10, 3, 7, 0), 7);
            metrics.OnAdmitted(1024, boundaryTick + 2);
            metrics.OnPublishResult(new TransitPublishResult("<post@benchmark.usenet.ninja>", TransitPublishStatus.Accepted, 239, "ok", 1, 1, 1, 1, 1, 1, 1, 1), 1024, boundaryTick + 2, boundaryTick + 3, boundaryTick + 4, 1, 0, completionTickOverride: boundaryTick + 1);

            metrics.OnGenerated(1024, boundaryTick + 10, TransitBenchmarkCore.ProducerTiming.FromRaw(10, 3, 7, 0), 7);
            metrics.OnAdmitted(1024, boundaryTick + 11);
            metrics.OnPublishResult(new TransitPublishResult("<reject@benchmark.usenet.ninja>", TransitPublishStatus.Rejected, 439, "reject", 1, 1, 1, 1, 1, 1, 1, 1), 1024, boundaryTick + 11, boundaryTick + 12, boundaryTick + 13, 1, 0, completionTickOverride: boundaryTick + 2);

            BenchmarkResult result = BenchmarkContractTestHelper.InvokeCreateBenchmarkResult(
                config,
                metrics.Snapshot(),
                metrics,
                BenchmarkContractTestHelper.CreateRuntimeMetricsWithSnapshotValues(1, 1, 1),
                BenchmarkContractTestHelper.CreateWorkloadPreparation(),
                measurementStartUtc: new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero),
                measurementEndUtc: new DateTimeOffset(2026, 1, 1, 0, 0, 10, TimeSpan.Zero),
                drainDuration: TimeSpan.FromSeconds(5),
                outstandingAtMeasurementEnd: 0,
                drainedAfterMeasurement: 0,
                allocatedStartBytes: 0,
                enableForensicDiagnostics: false);

            double wrongAcceptedWithAllBytes = result.AcceptedBytes * 8d / 1_000_000_000d / Math.Max(0.000001d, result.Boundary.MeasurementWindowDuration.TotalSeconds);
            double wrongAcceptedWithDrainDuration = result.AcceptedWithinWindowBytes * 8d / 1_000_000_000d / Math.Max(0.000001d, result.Boundary.EndToEndDuration.TotalSeconds);

            Assert.NotEqual(wrongAcceptedWithAllBytes, result.AcceptedGbps);
            Assert.NotEqual(wrongAcceptedWithDrainDuration, result.AcceptedGbps);
            Assert.Equal(1, result.AcceptedWithinWindowArticles);
            Assert.Equal(1, result.AcceptedPostMeasurementArticles);
            Assert.Equal(1, result.RejectedArticles);
            Assert.Equal(result.CompletedArticles, result.AcceptedArticles + result.RejectedArticles + result.AmbiguousArticles + result.FailedArticles + result.UnavailableArticles + result.CanceledArticles);
            Assert.True(result.AcceptedArticles <= result.CompletedArticles);
            Assert.True(result.RejectedArticles <= result.CompletedArticles);
            Assert.True(result.AmbiguousArticles <= result.CompletedArticles);
            Assert.True(result.CompletedArticles <= result.AdmittedArticles);
        }
    }
}
