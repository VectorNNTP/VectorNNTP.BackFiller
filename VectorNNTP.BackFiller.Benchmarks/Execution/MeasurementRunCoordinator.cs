// <copyright file="MeasurementRunCoordinator.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Execution/MeasurementRunCoordinator: coordinates bounded benchmark work, transport lifetimes, and deterministic shutdown.

using System.Diagnostics;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks.Execution;

/// <summary>
/// Defines the measurement RunCoordinator class for benchmark or isolated-regression execution.
/// </summary>
internal static class MeasurementRunCoordinator
{
    /// <summary>
    /// Performs the run Async operation.
    /// </summary>
    internal static async Task<BenchmarkResult> RunAsync(
        TransitPublisher publisher,
        TransitBenchmarkConfig config,
        PreparedBenchmarkWorkload workload,
        RuntimeExecutionIdentity runtimeIdentity,
        string benchmarkBuildVersion,
        CancellationToken cancellationToken,
        bool enableForensicDiagnostics)
    {
        using BoundedArticleQueue queue = new(config.MaxQueuedArticles, config.MaxResidentBytes);
        MeasurementMetrics metrics = new(config.ArticleTargetBytes);
        RuntimeMetrics runtime = new();

        using CancellationTokenSource producerStopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        DateTimeOffset measurementStartUtc = DateTimeOffset.UtcNow;
        long measurementStartTick = Stopwatch.GetTimestamp();
        metrics.MarkMeasurementStart(measurementStartTick);
        TransitPublisher.MarkSubmissionPumpFaultMeasurementWindow(measurementStartTick, measurementEndStopwatchTick: 0, measurementBoundaryObserved: false);
        TransitPublisher.MarkSubmissionPumpFaultProducerCompletion(allProducersCompleted: false);
        TransitPublisher.MarkSubmissionPumpFaultDispatchersCompleted(dispatchersCompleted: false);
        Console.WriteLine($"Measurement start UTC: {measurementStartUtc:O}");

        Process process = Process.GetCurrentProcess();
        long allocatedStartBytes = GC.GetTotalAllocatedBytes(precise: false);

        int producerQueueTargetArticles = Math.Clamp(config.ProducerQueueTargetArticles, 1, config.MaxQueuedArticles);
        FixedArticleLimiter? fixedArticleLimiter = config.MeasurementArticleCount is int fixedCount
            ? new FixedArticleLimiter(fixedCount)
            : null;

        Task[] producerTasks = new Task[config.GeneratorWorkerCount];
        for (int producerWorkerId = 0; producerWorkerId < producerTasks.Length; producerWorkerId++)
        {
            int capturedWorkerId = producerWorkerId;
            producerTasks[producerWorkerId] = Task.Run(() => MeasurementExecutionEngine.ProducerLoopAsync(
                queue,
                metrics,
                workload,
                producerQueueTargetArticles,
                capturedWorkerId,
                fixedArticleLimiter,
                producerStopCts.Token), CancellationToken.None);
        }

        Task telemetryTask = Task.Run(() => MeasurementExecutionEngine.TelemetryLoopAsync(
            queue,
            metrics,
            runtime,
            process,
            allocatedStartBytes,
            publisher,
            producerQueueTargetArticles,
            enableForensicDiagnostics,
            producerStopCts.Token), CancellationToken.None);

        Task[] dispatchers = new Task[config.DispatchWorkerCount];
        for (int i = 0; i < dispatchers.Length; i++)
        {
            dispatchers[i] = Task.Run(() => MeasurementExecutionEngine.DispatchLoopAsync(queue, publisher, metrics, workload, cancellationToken, enableForensicDiagnostics), CancellationToken.None);
        }

        if (config.MeasurementArticleCount is null)
        {
            await Task.Delay(config.MeasurementDuration, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await Task.WhenAll(producerTasks).ConfigureAwait(false);
        }

        return await MeasurementExecutionEngine.DrainAndShutdownAsync(
            queue,
            metrics,
            runtime,
            process,
            workload,
            publisher,
            config,
            producerTasks,
            telemetryTask,
            dispatchers,
            producerStopCts,
            measurementStartUtc,
            allocatedStartBytes,
            enableForensicDiagnostics,
            (drainConfig, snapshot, drainMetrics, drainRuntime, drainProcess, workloadPreparation, startUtc, endUtc, drainTime, outstandingAtEnd, drainedAfterEnd, allocatedAtStart, forensicEnabled, fixedCountBoundaryTelemetry) =>
                BenchmarkResultFactory.Create(
                    drainConfig,
                    runtimeIdentity,
                    benchmarkBuildVersion,
                    snapshot,
                    drainMetrics,
                    drainRuntime,
                    drainProcess,
                    workloadPreparation,
                    startUtc,
                    endUtc,
                    drainTime,
                    outstandingAtEnd,
                    drainedAfterEnd,
                    allocatedAtStart,
                    forensicEnabled,
                    fixedCountBoundaryTelemetry,
                    publisher)).ConfigureAwait(false);
    }
}
