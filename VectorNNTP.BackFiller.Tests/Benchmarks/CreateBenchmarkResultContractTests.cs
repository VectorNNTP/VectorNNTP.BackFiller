// <copyright file="CreateBenchmarkResultContractTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Tests / yEnc
// Corpus-backed and synthetic contract tests for the yEnc article validator,
// covering protocol parsing, integrity classification, malformed input handling,
// and NNTP dot-stuffing interactions.

using System.Diagnostics;
using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    public sealed class CreateBenchmarkResultContractTests
    {
        [Fact]
        public void CreateBenchmarkResult_MapsDeterministicInputsToExpectedContractValues()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig(
                measurementSeconds: 10,
                maxResidentBytes: 100_000_000,
                articleTargetBytes: 1_000_000,
                maxQueuedArticles: 256);

            MeasurementSnapshot snapshot = BenchmarkContractTestHelper.CreateMeasurementSnapshot(
                generatedCount: 100,
                generatedBytes: 100_000_000,
                admittedCount: 90,
                admittedBytes: 90_000_000,
                acceptedCount: 80,
                acceptedBytes: 80_000_000,
                rejectedCount: 5,
                ambiguousCount: 5,
                completedCount: 90,
                blockedTicks: 4000,
                activeTicks: 6000,
                producerQueueWaitTicks: 500,
                articleBytes: 1_000_000);

            MeasurementMetrics metrics = BenchmarkContractTestHelper.CreateMeasurementMetricsWithForensicSample();
            RuntimeMetrics runtime = BenchmarkContractTestHelper.CreateRuntimeMetricsWithSnapshotValues(
                workingSetBytes: 200L * 1024 * 1024,
                gcHeapBytes: 150L * 1024 * 1024,
                allocatedBytes: 50L * 1024 * 1024);

            WorkloadPreparationSummary workload = BenchmarkContractTestHelper.CreateWorkloadPreparation();
            DateTimeOffset measurementStartUtc = new(2026, 1, 2, 3, 4, 5, TimeSpan.Zero);
            DateTimeOffset measurementEndUtc = new(2026, 1, 2, 3, 4, 15, TimeSpan.Zero);
            TimeSpan drainDuration = TimeSpan.FromMilliseconds(3456);

            BenchmarkResult result = BenchmarkContractTestHelper.InvokeCreateBenchmarkResult(
                config,
                snapshot,
                metrics,
                runtime,
                workload,
                measurementStartUtc,
                measurementEndUtc,
                drainDuration,
                outstandingAtMeasurementEnd: 17,
                drainedAfterMeasurement: 13,
                allocatedStartBytes: 10,
                enableForensicDiagnostics: true);

            Assert.Equal(workload, result.WorkloadPreparation);
            Assert.Equal(measurementStartUtc, result.MeasurementStartUtc);
            Assert.Equal(measurementEndUtc, result.MeasurementEndUtc);
            Assert.Equal(drainDuration, result.DrainDuration);
            Assert.Equal(17, result.OutstandingAtMeasurementEnd);
            Assert.Equal(13, result.DrainedAfterMeasurement);
            Assert.Null(result.FixedCountBoundaryTelemetry);

            Assert.All(result.AmbiguityProvenance.Categories, static category =>
            {
                Assert.Equal(0, category.Count);
                Assert.Equal(0, category.BeforeMeasurementEndCount);
                Assert.Equal(0, category.AfterMeasurementEndCount);
            });
            Assert.Empty(result.AmbiguityProvenance.Connections);

            Assert.Equal(0, result.SubmissionPumpFault.TotalFaultCount);
            Assert.Equal(0, result.SubmissionPumpFault.InitiatingFaultCount);
            Assert.Equal(0, result.SubmissionPumpFault.CascadeFaultCount);
            Assert.Null(result.SubmissionPumpFault.InitiatingFault);
            Assert.Null(result.P1GreetingProvenance);

            Assert.Equal(snapshot.GeneratedCount, result.GeneratedArticles);
            Assert.Equal(snapshot.GeneratedBytes, result.GeneratedBytes);
            Assert.Equal(snapshot.AdmittedCount, result.AdmittedArticles);
            Assert.Equal(snapshot.AdmittedBytes, result.AdmittedBytes);
            Assert.Equal(snapshot.AcceptedCount, result.AcceptedArticles);
            Assert.Equal(snapshot.AcceptedBytes, result.AcceptedBytes);
            Assert.Equal(snapshot.RejectedCount, result.RejectedArticles);
            Assert.Equal(snapshot.AmbiguousCount, result.AmbiguousArticles);

            Assert.Equal(snapshot.MinQueueDepth, result.MinQueueDepth);
            Assert.Equal(snapshot.QueueDepthSampleCount, result.QueueDepthSampleCount);
            Assert.Equal(snapshot.AverageQueueDepth, result.AverageQueueDepth);
            Assert.Equal(snapshot.AverageQueueBytes, result.AverageQueuedBytes);
            Assert.Equal(snapshot.PeakQueueDepth, result.PeakQueueDepth);
            Assert.Equal(snapshot.PeakQueueBytes, result.PeakQueuedBytes);
            Assert.Equal(snapshot.PeakInFlight, result.PeakInFlight);
            Assert.Equal(snapshot.PeakActualPending, result.PeakActualPending);

            Assert.Equal(200d, result.WorkingSetMb);
            Assert.Equal(150d, result.GcHeapMb);
            Assert.Equal(50d, result.AllocatedMb);

            Assert.Equal(100, result.EffectiveQueueArticleCapacityFromBytes);

            ForensicSnapshot expectedForensic = metrics.CaptureForensicSnapshot();
            Assert.Equal(expectedForensic.AverageDispatchQueueWaitUs, result.AverageDispatchQueueWaitUs);
            Assert.Equal(expectedForensic.P50DispatchQueueWaitUs, result.P50DispatchQueueWaitUs);
            Assert.Equal(expectedForensic.P95DispatchQueueWaitUs, result.P95DispatchQueueWaitUs);
            Assert.Equal(expectedForensic.P99DispatchQueueWaitUs, result.P99DispatchQueueWaitUs);
            Assert.Equal(expectedForensic.MaxDispatchQueueWaitUs, result.MaxDispatchQueueWaitUs);
            Assert.Equal(expectedForensic.DispatchQueueWaitSampleCount, result.DispatchQueueWaitSampleCount);
            Assert.Equal(expectedForensic.AverageSocketWriteUs, result.AverageSocketWriteUs);
            Assert.Equal(expectedForensic.P50SocketWriteUs, result.P50SocketWriteUs);
            Assert.Equal(expectedForensic.P95SocketWriteUs, result.P95SocketWriteUs);
            Assert.Equal(expectedForensic.P99SocketWriteUs, result.P99SocketWriteUs);
            Assert.Equal(expectedForensic.MaxSocketWriteUs, result.MaxSocketWriteUs);
            Assert.Equal(expectedForensic.SocketWriteSampleCount, result.SocketWriteSampleCount);
            Assert.Equal(expectedForensic.AverageResponseWaitUs, result.AverageResponseWaitUs);
            Assert.Equal(expectedForensic.P50ResponseWaitUs, result.P50ResponseWaitUs);
            Assert.Equal(expectedForensic.P95ResponseWaitUs, result.P95ResponseWaitUs);
            Assert.Equal(expectedForensic.P99ResponseWaitUs, result.P99ResponseWaitUs);
            Assert.Equal(expectedForensic.MaxResponseWaitUs, result.MaxResponseWaitUs);
            Assert.Equal(expectedForensic.ResponseWaitSampleCount, result.ResponseWaitSampleCount);
            Assert.Equal(expectedForensic.AverageParseCorrelationUs, result.AverageParseCorrelationUs);
            Assert.Equal(expectedForensic.P50ParseCorrelationUs, result.P50ParseCorrelationUs);
            Assert.Equal(expectedForensic.P95ParseCorrelationUs, result.P95ParseCorrelationUs);
            Assert.Equal(expectedForensic.P99ParseCorrelationUs, result.P99ParseCorrelationUs);
            Assert.Equal(expectedForensic.MaxParseCorrelationUs, result.MaxParseCorrelationUs);
            Assert.Equal(expectedForensic.ParseCorrelationSampleCount, result.ParseCorrelationSampleCount);
            Assert.Equal(expectedForensic.AverageTotalPublishLatencyUs, result.AverageTotalPublishLatencyUs);
            Assert.Equal(expectedForensic.P50TotalPublishLatencyUs, result.P50TotalPublishLatencyUs);
            Assert.Equal(expectedForensic.P95TotalPublishLatencyUs, result.P95TotalPublishLatencyUs);
            Assert.Equal(expectedForensic.P99TotalPublishLatencyUs, result.P99TotalPublishLatencyUs);
            Assert.Equal(expectedForensic.MaxTotalPublishLatencyUs, result.MaxTotalPublishLatencyUs);
            Assert.Equal(expectedForensic.TotalPublishLatencySampleCount, result.TotalPublishLatencySampleCount);
            Assert.Equal(expectedForensic.AveragePublishLatencyUs, result.AveragePublishLatencyUs);
            Assert.Equal(expectedForensic.MinPublishLatencyUs, result.MinPublishLatencyUs);
            Assert.Equal(expectedForensic.P50PublishLatencyUs, result.P50PublishLatencyUs);
            Assert.Equal(expectedForensic.P95PublishLatencyUs, result.P95PublishLatencyUs);
            Assert.Equal(expectedForensic.P99PublishLatencyUs, result.P99PublishLatencyUs);
            Assert.Equal(expectedForensic.MaxPublishLatencyUs, result.MaxPublishLatencyUs);
            Assert.Equal(expectedForensic.AverageLifecycleLatencyUs, result.AverageLifecycleLatencyUs);
            Assert.Equal(expectedForensic.PendingDepthLatencyBuckets, result.PendingDepthLatencyBuckets);
            Assert.Equal(expectedForensic.ForensicSampleCount, result.ForensicSampleCount);
            Assert.Equal(expectedForensic.ConnectionTimeSeriesSummary, result.ConnectionTimeSeriesSummary);
            Assert.Equal(expectedForensic.DispatcherTimeSeriesSummary, result.DispatcherTimeSeriesSummary);
            Assert.Equal(expectedForensic.ObservabilityNotes, result.ObservabilityNotes);
        }

        [Fact]
        public void CreateBenchmarkResult_UsesExpectedThroughputAndBackpressureFormulas()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig(measurementSeconds: 10);
            MeasurementSnapshot snapshot = BenchmarkContractTestHelper.CreateMeasurementSnapshot(
                generatedBytes: 1_250_000_000,
                admittedBytes: 1_000_000_000,
                acceptedBytes: 500_000_000,
                blockedTicks: 200,
                activeTicks: 800,
                producerQueueWaitTicks: 300,
                articleBytes: 500_000);

            BenchmarkResult result = BenchmarkContractTestHelper.InvokeCreateBenchmarkResult(
                config,
                snapshot,
                BenchmarkContractTestHelper.CreateMeasurementMetricsWithForensicSample(),
                BenchmarkContractTestHelper.CreateRuntimeMetricsWithSnapshotValues(1, 1, 1),
                BenchmarkContractTestHelper.CreateWorkloadPreparation(),
                measurementStartUtc: new DateTimeOffset(2026, 3, 1, 0, 0, 0, TimeSpan.Zero),
                measurementEndUtc: new DateTimeOffset(2026, 3, 1, 0, 0, 10, TimeSpan.Zero),
                drainDuration: TimeSpan.FromSeconds(1),
                outstandingAtMeasurementEnd: 0,
                drainedAfterMeasurement: 0,
                allocatedStartBytes: 0,
                enableForensicDiagnostics: false);

            Assert.Equal(1d, result.GeneratedGbps);
            Assert.Equal(0.8d, result.AdmittedGbps);
            Assert.Equal(0.4d, result.AcceptedGbps);

            Assert.Equal(80d, result.ProducerActivePercent);
            Assert.Equal(20d, result.ProducerBlockedPercent);

            double expectedActiveMs = snapshot.ActiveTicks * 1000d / Stopwatch.Frequency;
            double expectedBlockedMs = snapshot.BlockedTicks * 1000d / Stopwatch.Frequency;
            double expectedQueueWaitMs = snapshot.ProducerQueueWaitTicks * 1000d / Stopwatch.Frequency;

            Assert.Equal(expectedActiveMs, result.ProducerActiveMilliseconds);
            Assert.Equal(expectedBlockedMs, result.ProducerBlockedMilliseconds);
            Assert.Equal(expectedQueueWaitMs, result.ProducerQueueWaitMilliseconds);

            Assert.Equal(config.MaxResidentBytes / snapshot.ArticleBytes, result.EffectiveQueueArticleCapacityFromBytes);
        }

        [Fact]
        public void CreateBenchmarkResult_HandlesZeroObservationInputsAsDefined()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig(measurementSeconds: 10, maxResidentBytes: 10_000_000);
            MeasurementSnapshot snapshot = BenchmarkContractTestHelper.CreateMeasurementSnapshot(
                blockedTicks: 0,
                activeTicks: 0,
                producerQueueWaitTicks: 0,
                articleBytes: 0);

            BenchmarkResult result = BenchmarkContractTestHelper.InvokeCreateBenchmarkResult(
                config,
                snapshot,
                BenchmarkContractTestHelper.CreateMeasurementMetricsWithForensicSample(),
                BenchmarkContractTestHelper.CreateRuntimeMetricsWithSnapshotValues(1, 1, 1),
                BenchmarkContractTestHelper.CreateWorkloadPreparation(),
                measurementStartUtc: DateTimeOffset.UtcNow,
                measurementEndUtc: DateTimeOffset.UtcNow,
                drainDuration: TimeSpan.Zero,
                outstandingAtMeasurementEnd: 9,
                drainedAfterMeasurement: 9,
                allocatedStartBytes: 0,
                enableForensicDiagnostics: true);

            Assert.Equal(0d, result.ProducerActivePercent);
            Assert.Equal(0d, result.ProducerBlockedPercent);
            Assert.Equal(0d, result.ProducerActiveMilliseconds);
            Assert.Equal(0d, result.ProducerBlockedMilliseconds);
            Assert.Equal(0d, result.ProducerQueueWaitMilliseconds);
            Assert.Equal(0, result.EffectiveQueueArticleCapacityFromBytes);
            Assert.Equal(9, result.OutstandingAtMeasurementEnd);
            Assert.Equal(9, result.DrainedAfterMeasurement);
        }

        [Fact]
        public void CreateBenchmarkResult_UsesProcessAndGcFallbacksWhenRuntimeSnapshotMemoryValuesUnavailable()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig(measurementSeconds: 5);
            MeasurementSnapshot snapshot = BenchmarkContractTestHelper.CreateMeasurementSnapshot();
            MeasurementMetrics metrics = BenchmarkContractTestHelper.CreateMeasurementMetricsWithForensicSample();

            RuntimeMetrics runtime = new();
            runtime.Sample(
                cpuPercent: 11,
                hostCpuPercent: 22,
                transitServerCpuPercent: 33,
                workingSet: 0,
                gcHeap: 0,
                allocated: 0);

            long allocatedStartBytes = GC.GetTotalAllocatedBytes(precise: false);

            BenchmarkResult result = BenchmarkContractTestHelper.InvokeCreateBenchmarkResult(
                config,
                snapshot,
                metrics,
                runtime,
                BenchmarkContractTestHelper.CreateWorkloadPreparation(),
                measurementStartUtc: DateTimeOffset.UtcNow,
                measurementEndUtc: DateTimeOffset.UtcNow,
                drainDuration: TimeSpan.Zero,
                outstandingAtMeasurementEnd: 0,
                drainedAfterMeasurement: 0,
                allocatedStartBytes,
                enableForensicDiagnostics: false);

            Assert.True(result.WorkingSetMb > 0);
            Assert.True(result.GcHeapMb > 0);
            Assert.True(result.AllocatedMb >= 0);
        }

        [Fact]
        public void CreateBenchmarkResult_UsesCurrentGcCollectionCountsAtResultCreationTime()
        {
            TransitBenchmarkConfig config = BenchmarkContractTestHelper.CreateConfig();
            MeasurementSnapshot snapshot = BenchmarkContractTestHelper.CreateMeasurementSnapshot();

            int gen0Before = GC.CollectionCount(0);
            int gen1Before = GC.CollectionCount(1);
            int gen2Before = GC.CollectionCount(2);

            BenchmarkResult result = BenchmarkContractTestHelper.InvokeCreateBenchmarkResult(
                config,
                snapshot,
                BenchmarkContractTestHelper.CreateMeasurementMetricsWithForensicSample(),
                BenchmarkContractTestHelper.CreateRuntimeMetricsWithSnapshotValues(1, 1, 1),
                BenchmarkContractTestHelper.CreateWorkloadPreparation(),
                measurementStartUtc: DateTimeOffset.UtcNow,
                measurementEndUtc: DateTimeOffset.UtcNow,
                drainDuration: TimeSpan.Zero,
                outstandingAtMeasurementEnd: 0,
                drainedAfterMeasurement: 0,
                allocatedStartBytes: 0,
                enableForensicDiagnostics: true);

            int gen0After = GC.CollectionCount(0);
            int gen1After = GC.CollectionCount(1);
            int gen2After = GC.CollectionCount(2);

            Assert.InRange(result.Gen0Collections, gen0Before, gen0After);
            Assert.InRange(result.Gen1Collections, gen1Before, gen1After);
            Assert.InRange(result.Gen2Collections, gen2Before, gen2After);
        }
    }
}
