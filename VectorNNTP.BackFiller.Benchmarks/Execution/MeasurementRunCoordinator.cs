using System.Diagnostics;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks.Execution;

internal static class MeasurementRunCoordinator
{
    internal static async Task<BenchmarkResult> RunAsync(
        TransitPublisher publisher,
        TransitBenchmarkConfig config,
        PreparedBenchmarkWorkload workload,
        RuntimeExecutionIdentity runtimeIdentity,
        string benchmarkBuildVersion,
        CancellationToken cancellationToken,
        bool enableForensicDiagnostics)
    {
        QueueConsumerForensics? queueConsumerForensics = config.EnableQueueConsumerForensics
            ? new QueueConsumerForensics(config.DispatchWorkerCount)
            : null;

        using BoundedArticleQueue queue = new(config.MaxQueuedArticles, config.MaxResidentBytes, queueConsumerForensics);
        MeasurementMetrics metrics = new(config.ArticleTargetBytes);
        RuntimeMetrics runtime = new();

        using CancellationTokenSource producerStopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        DateTimeOffset measurementStartUtc = DateTimeOffset.UtcNow;
        long measurementStartTick = Stopwatch.GetTimestamp();
        metrics.MarkMeasurementStart(measurementStartTick);
        publisher.MarkSubmissionPumpFaultMeasurementWindow(measurementStartTick, measurementEndStopwatchTick: 0, measurementBoundaryObserved: false);
        publisher.MarkSubmissionPumpFaultProducerCompletion(allProducersCompleted: false);
        publisher.MarkSubmissionPumpFaultDispatchersCompleted(dispatchersCompleted: false);
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
            producerStopCts.Token,
            queueConsumerForensics), CancellationToken.None);

        Task[] dispatchers = new Task[config.DispatchWorkerCount];
        for (int i = 0; i < dispatchers.Length; i++)
        {
            QueueConsumerProbe? consumerProbe = queueConsumerForensics?.GetProbe(i);
            dispatchers[i] = Task.Run(() => MeasurementExecutionEngine.DispatchLoopAsync(queue, publisher, metrics, workload, cancellationToken, enableForensicDiagnostics, consumerProbe), CancellationToken.None);
        }

        if (config.MeasurementArticleCount is null)
        {
            await Task.Delay(config.MeasurementDuration, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await Task.WhenAll(producerTasks).ConfigureAwait(false);
        }

        BenchmarkResult benchmarkResult = await MeasurementExecutionEngine.DrainAndShutdownAsync(
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

        if (queueConsumerForensics is not null)
        {
            ExportQueueConsumerForensics(queueConsumerForensics, queue, publisher);
        }

        return benchmarkResult;
    }

    private static void ExportQueueConsumerForensics(
        QueueConsumerForensics forensics,
        BoundedArticleQueue queue,
        TransitPublisher publisher)
    {
        try
        {
            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot diagnostics = publisher.CaptureConnectionDiagnosticsSnapshot();
            long transportInFlight = diagnostics.Connections.Sum(static entry => (long)entry.Snapshot.CurrentConcurrentSubmissions);

            QueueConsumerForensicsReport report = forensics.BuildReport(
                queue.CurrentQueuedCount,
                queue.CurrentQueuedBytes,
                transportInFlight);

            (string jsonPath, string textPath) = QueueConsumerForensicsWriter.Write(report, AppContext.BaseDirectory);

            Console.WriteLine();
            Console.WriteLine("Queue consumer call-stack forensics written:");
            Console.WriteLine($"JSON: {jsonPath}");
            Console.WriteLine($"TEXT: {textPath}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"WARNING: Failed to write queue consumer forensic artifacts: {ex.GetType().Name}: {ex.Message}");
        }
    }
}
