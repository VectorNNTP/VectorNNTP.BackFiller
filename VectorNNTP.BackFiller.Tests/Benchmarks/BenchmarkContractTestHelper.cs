using System.Diagnostics;
using System.Reflection;
using VectorNNTP.Backfiller.Runtime.Transit;
using VectorNNTP.BackFiller.Benchmarks;

namespace VectorNNTP.Backfiller.Tests;

internal static class BenchmarkContractTestHelper
{
    private static readonly RuntimeExecutionIdentity RuntimeIdentity = RuntimeExecutionIdentityCapture.Capture(typeof(TransitServerStressRunner).Assembly);
    private static readonly string BenchmarkBuildVersion = RuntimeIdentity.AssemblyFileVersion ?? RuntimeIdentity.RuntimeAssemblyVersion;

    private static readonly MethodInfo CreateBenchmarkResultMethod = typeof(BenchmarkResultFactory)
        .GetMethod("Create", BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new InvalidOperationException("BenchmarkResultFactory.Create was not found.");

    internal static TransitBenchmarkConfig CreateConfig(
        double measurementSeconds = 10,
        long maxResidentBytes = 16L * 1024L * 1024L,
        int articleTargetBytes = 1_024 * 1_024,
        int maxQueuedArticles = 64)
    {
        return new TransitBenchmarkConfig(
            Mode: BenchmarkMode.Validation,
            BenchmarkInstanceId: 123456789,
            EndpointHost: "incoming.usenet.ninja",
            EndpointPort: 563,
            EndpointUseSsl: true,
            AppSettingsPath: "appsettings.json",
            WarmupDuration: TimeSpan.FromSeconds(5),
            MeasurementDuration: TimeSpan.FromSeconds(measurementSeconds),
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
            AverageQueueDepth: 13.25,
            AverageQueueBytes: 2_097_152.5,
            ProducerQueueWaitTicks: producerQueueWaitTicks,
            ArticleBytes: articleBytes);
    }

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
        bool enableForensicDiagnostics)
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
                enableForensicDiagnostics
            ]);

        return value is BenchmarkResult result
            ? result
            : throw new InvalidOperationException("CreateBenchmarkResult invocation did not return BenchmarkResult.");
    }
}
