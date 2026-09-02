// <copyright file="BenchmarkContractTestHelper.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Benchmarks
// Focused tests for benchmark contract test helper, covering benchmark measurement and runtime identity contracts.
// Primary responsibility: documents the executable contracts covered by the benchmark contract test helper test suite.

using System.Diagnostics;
using System.Reflection;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.BackFiller.Benchmarks;

namespace VectorNNTP.BackFiller.Tests.Benchmarks
{
    /// <summary>
    /// Covers benchmark contract test helper behavior and invariants exercised by this test suite.
    /// </summary>
    internal static class BenchmarkContractTestHelper
    {
        /// <summary>
        /// Exercises runtime identity behavior, including the expected result and failure semantics.
        /// </summary>
        private static readonly RuntimeExecutionIdentity RuntimeIdentity = RuntimeExecutionIdentityCapture.Capture(typeof(TransitServerStressRunner).Assembly);
        /// <summary>
        /// Supplies benchmark build version for the fixture or scenario under test.
        /// </summary>
        private static readonly string BenchmarkBuildVersion = RuntimeIdentity.AssemblyFileVersion ?? RuntimeIdentity.RuntimeAssemblyVersion;

        /// <summary>
        /// Exercises create benchmark result method behavior, including the expected result and failure semantics.
        /// </summary>
        private static readonly MethodInfo CreateBenchmarkResultMethod = typeof(BenchmarkResultFactory)
            .GetMethod("Create", BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException("BenchmarkResultFactory.Create was not found.");

        /// <summary>
        /// Verifies the create config behavior and expected contract.
        /// </summary>
        internal static TransitBenchmarkConfig CreateConfig(
            double measurementSeconds = 10,
            long maxResidentBytes = 16L * 1024L * 1024L,
            int articleTargetBytes = 1_024 * 1_024,
            int maxQueuedArticles = 64,
            int? measurementArticleCount = null)
        {
            return new TransitBenchmarkConfig(
                Mode: BenchmarkMode.Validation,
                BenchmarkInstanceId: 123456789,
                EndpointType: "TRANSITSERVER",
                EndpointIdentity: "appsettings:BackFiller:TransitServer",
                EndpointHost: "incoming.usenet.ninja",
                EndpointPort: 563,
                EndpointUseSsl: true,
                AppSettingsPath: "appsettings.json",
                WarmupDuration: TimeSpan.FromSeconds(5),
                MeasurementDuration: TimeSpan.FromSeconds(measurementSeconds),
                MeasurementArticleCount: measurementArticleCount,
                ConnectionPoolSize: 4,
                PerConnectionPipelineDepth: 8,
                DispatchWorkerCount: 32,
                GeneratorWorkerCount: 2,
                WriteBatchCoalesceMicroseconds: 250,
                MaxQueuedArticles: maxQueuedArticles,
                MaxResidentBytes: maxResidentBytes,
                ArticleTargetBytes: articleTargetBytes,
                ProducerQueueTargetArticles: Math.Min(maxQueuedArticles, 32),
                ExpectedRuntimeIdentity: default);
        }

        /// <summary>
        /// Verifies the create workload preparation behavior and expected contract.
        /// </summary>
        internal static WorkloadPreparationSummary CreateWorkloadPreparation()
        {
            return new WorkloadPreparationSummary(
                PreGenerationDurationMilliseconds: 123.45,
                PayloadPreparationDurationMilliseconds: 67.89,
                MessageIdPoolSize: 10,
                UniqueMessageIdCount: 8,
                DuplicateMessageIdCount: 2,
                ReusablePayloadBytes: 131072);
        }

        /// <summary>
        /// Verifies the create measurement snapshot behavior and expected contract.
        /// </summary>
        internal static MeasurementSnapshot CreateMeasurementSnapshot(
            long generatedCount = 100,
            long generatedBytes = 100_000_000,
            long admittedCount = 90,
            long admittedBytes = 90_000_000,
            long acceptedCount = 80,
            long acceptedBytes = 80_000_000,
            long rejectedCount = 5,
            long ambiguousCount = 5,
            long completedCount = 90,
            long blockedTicks = 4000,
            long activeTicks = 6000,
            long queueDepthSampleCount = 4,
            long producerQueueWaitTicks = 500,
            int articleBytes = 1_000_000)
        {
            return new MeasurementSnapshot(
                GeneratedCount: generatedCount,
                GeneratedBytes: generatedBytes,
                AdmittedCount: admittedCount,
                AdmittedBytes: admittedBytes,
                AcceptedCount: acceptedCount,
                AcceptedBytes: acceptedBytes,
                RejectedCount: rejectedCount,
                AmbiguousCount: ambiguousCount,
                CompletedCount: completedCount,
                BlockedTicks: blockedTicks,
                GenerationTicks: 4500,
                OtherActiveTicks: 1500,
                ActiveTicks: activeTicks,
                LoopTicks: activeTicks + blockedTicks,
                PeakQueueDepth: 77,
                PeakQueueBytes: 8_388_608,
                PeakInFlight: 19,
                PeakActualPending: 21,
                MinQueueDepth: 2,
                MinQueueBytes: 2048,
                QueueDepthSampleCount: queueDepthSampleCount,
                AverageQueueDepth: 13.25,
                AverageQueueBytes: 2_097_152.5,
                ProducerQueueWaitTicks: producerQueueWaitTicks,
                ArticleBytes: articleBytes);
        }

        /// <summary>
        /// Verifies the create runtime metrics with snapshot values behavior and expected contract.
        /// </summary>
        internal static RuntimeMetrics CreateRuntimeMetricsWithSnapshotValues(long workingSetBytes, long gcHeapBytes, long allocatedBytes)
        {
            RuntimeMetrics runtime = new();
            runtime.Sample(
                cpuPercent: 51.5,
                hostCpuPercent: 42.25,
                transitServerCpuPercent: 27.75,
                workingSet: workingSetBytes,
                gcHeap: gcHeapBytes,
                allocated: allocatedBytes);
            return runtime;
        }

        /// <summary>
        /// Verifies the create measurement metrics with forensic sample behavior and expected contract.
        /// </summary>
        internal static MeasurementMetrics CreateMeasurementMetricsWithForensicSample()
        {
            MeasurementMetrics metrics = new(articleBytes: 1_000_000);

            TransitPublishResult publishResult = new(
                MessageId: "<benchmark-contract@benchmark.usenet.ninja>",
                Status: TransitPublishStatus.Accepted,
                ResponseCode: 239,
                ResponseText: "239 ok",
                T0PublishAsyncEnterTick: 1_000,
                T1DispatcherAssignedTick: 1_100,
                T2SocketWriteBeginTick: 1_200,
                T3SocketWriteEndTick: 1_400,
                T4ResponseAvailableTick: 1_700,
                T5ResponseParsedTick: 1_800,
                T6ResponseCorrelatedTick: 1_900,
                T7PublishAsyncCompleteTick: 2_100);

            metrics.OnPublishResult(
                publishResult,
                bytes: 1_000_000,
                dequeuedTick: 900,
                publishStartTick: 1_000,
                publishEndTick: 2_100,
                pendingAtSubmit: 7,
                pendingAtComplete: 10);

            return metrics;
        }

        /// <summary>
        /// Verifies the invoke create benchmark result behavior and expected contract.
        /// </summary>
        internal static BenchmarkResult InvokeCreateBenchmarkResult(
            TransitBenchmarkConfig config,
            MeasurementSnapshot snapshot,
            MeasurementMetrics metrics,
            RuntimeMetrics runtime,
            WorkloadPreparationSummary workloadPreparation,
            DateTimeOffset measurementStartUtc,
            DateTimeOffset measurementEndUtc,
            TimeSpan drainDuration,
            long outstandingAtMeasurementEnd,
            long drainedAfterMeasurement,
            long allocatedStartBytes,
            bool enableForensicDiagnostics,
            FixedCountBoundaryTelemetry? fixedCountBoundaryTelemetry = null)
        {
            object? value = CreateBenchmarkResultMethod.Invoke(
                obj: null,
                parameters:
                [
                    config,
                    RuntimeIdentity,
                    BenchmarkBuildVersion,
                    snapshot,
                    metrics,
                    runtime,
                    Process.GetCurrentProcess(),
                    workloadPreparation,
                    measurementStartUtc,
                    measurementEndUtc,
                    drainDuration,
                    outstandingAtMeasurementEnd,
                    drainedAfterMeasurement,
                    allocatedStartBytes,
                    enableForensicDiagnostics,
                    fixedCountBoundaryTelemetry,
                    CreatePublisherForContracts()
                ]);

            return value is BenchmarkResult result
                ? result
                : throw new InvalidOperationException("CreateBenchmarkResult invocation did not return BenchmarkResult.");
        }

        /// <summary>
        /// Verifies the create publisher for contracts behavior and expected contract.
        /// </summary>
        private static TransitPublisher CreatePublisherForContracts()
        {
            BackFillerRuntimeOptions runtimeOptions = new(
                CanonicalBackFillerFqdn: "benchmark.backfiller.usenet.ninja",
                BackFillerId: 1,
                CanonicalDnsSuffix: "usenet.ninja",
                ValidatedLogDirectory: Path.GetTempPath(),
                ValidatedCertificateDirectory: Path.GetTempPath(),
                RabbitMqHosts: [],
                RabbitMqPort: 5672,
                RabbitMqEnableSsl: false,
                TransitServerHost: "127.0.0.1",
                TransitServerPort: 119,
                TransitServerUseSsl: false,
                ShutdownGracePeriodSeconds: 120,
                ShutdownDrainQueuedWork: true,
                ShutdownFinishActiveArticles: true,
                RabbitMqMaximumShutdownDrainTimeoutSeconds: 30,
                WriteBatchCoalesceMicroseconds: 250);

            return new TransitPublisher(
                runtimeOptions,
                TimeProvider.System,
                NullLogger<TransitPublisher>.Instance,
                connectionPoolSize: 1,
                perConnectionPipelineDepth: 8);
        }
    }
}
