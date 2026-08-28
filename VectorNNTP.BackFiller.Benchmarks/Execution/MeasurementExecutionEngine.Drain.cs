using System.Diagnostics;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

internal static partial class MeasurementExecutionEngine
{
    internal static async Task<BenchmarkResult> DrainAndShutdownAsync(
        BoundedArticleQueue queue,
        MeasurementMetrics metrics,
        RuntimeMetrics runtime,
        Process process,
        PreparedBenchmarkWorkload workload,
        TransitPublisher publisher,
        TransitBenchmarkConfig config,
        Task[] producerTasks,
        Task telemetryTask,
        Task[] dispatchers,
        CancellationTokenSource producerStopCts,
        DateTimeOffset measurementStartUtc,
        long allocatedStartBytes,
        bool enableForensicDiagnostics,
        Func<TransitBenchmarkConfig, MeasurementSnapshot, MeasurementMetrics, RuntimeMetrics, Process, WorkloadPreparationSummary, DateTimeOffset, DateTimeOffset, TimeSpan, long, long, long, bool, FixedCountBoundaryTelemetry?, BenchmarkResult> createBenchmarkResult)
    {
        DateTimeOffset measurementEndUtc = DateTimeOffset.UtcNow;
        long measurementEndTick = Stopwatch.GetTimestamp();
        Console.WriteLine($"Measurement end UTC:   {measurementEndUtc:O}");
        metrics.MarkMeasurementBoundary(measurementEndUtc, measurementEndTick);
        TransitPublisher.MarkSubmissionPumpFaultMeasurementWindow(measurementStartStopwatchTick: 0, measurementEndStopwatchTick: measurementEndTick, measurementBoundaryObserved: true);

        producerStopCts.Cancel();

        bool producersCompleted = false;
        try
        {
            await Task.WhenAll(producerTasks).ConfigureAwait(false);
            producersCompleted = true;
        }
        catch (OperationCanceledException)
        {
        }

        publisher.MarkSubmissionPumpFaultProducerCompletion(producersCompleted);

        queue.StopAdmission();

        long completedAtMeasurementEnd = metrics.GetCompletedCount();
        TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot diagnosticsAtMeasurementEnd = publisher.CaptureConnectionDiagnosticsSnapshot();
        FixedCountBoundarySnapshot atMeasurementEndSnapshot = BuildBoundarySnapshot("measurement-end", measurementEndUtc, measurementEndTick, metrics, diagnosticsAtMeasurementEnd);

        long outstandingAtMeasurementEnd = atMeasurementEndSnapshot.CurrentOutstandingSubmissions;

        try
        {
            await telemetryTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            Console.WriteLine("[TELEMETRY-WARN] Telemetry loop faulted during drain; continuing shutdown. exceptionType={0} message={1}",
                ex.GetType().Name,
                ex.Message);
        }

        DateTimeOffset postMeasurementPreDrainUtc = DateTimeOffset.UtcNow;
        long postMeasurementPreDrainTick = Stopwatch.GetTimestamp();
        TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot diagnosticsPostMeasurementPreDrain = publisher.CaptureConnectionDiagnosticsSnapshot();
        FixedCountBoundarySnapshot postMeasurementPreDrainSnapshot = BuildBoundarySnapshot("post-measurement-pre-drain", postMeasurementPreDrainUtc, postMeasurementPreDrainTick, metrics, diagnosticsPostMeasurementPreDrain);

        Console.WriteLine("[SHUTDOWN-DIAG] Measurement window expired: outstandingSubmissions={0} queuedSubmissions={1} pendingMessageIds={2} queuedWriteIntents={3}",
            atMeasurementEndSnapshot.CurrentOutstandingSubmissions,
            atMeasurementEndSnapshot.QueuedSubmissionCount,
            atMeasurementEndSnapshot.PendingOperationsCount,
            atMeasurementEndSnapshot.QueuedWriteIntentsCount);

        foreach (BoundaryConnectionSnapshot snapshot in atMeasurementEndSnapshot.Connections
                     .OrderBy(static x => x.SlotIndex)
                     .ThenBy(static x => x.ConnectionId, StringComparer.Ordinal))
        {
            Console.WriteLine("[SHUTDOWN-DIAG] Measurement-end connection snapshot: slot={0} connectionId={1} state={2} inFlight={3} writeQueueDepth={4} pendingMessageIds={5}",
                snapshot.SlotIndex,
                snapshot.ConnectionId,
                snapshot.State,
                snapshot.CurrentConcurrentSubmissions,
                snapshot.CurrentWriteIntentQueueDepth,
                snapshot.OutstandingOperations);
        }

        Console.WriteLine();
        Console.WriteLine("=== Phase 6: Drain ===");
        Stopwatch drainStopwatch = Stopwatch.StartNew();

        Task dispatcherDrainTask = Task.WhenAll(dispatchers);
        Task completedDrainTask = await Task.WhenAny(dispatcherDrainTask, Task.Delay(TimeSpan.FromSeconds(5), CancellationToken.None)).ConfigureAwait(false);
        if (!ReferenceEquals(completedDrainTask, dispatcherDrainTask))
        {
            Console.WriteLine("[SHUTDOWN-DIAG] Dispatcher drain did not complete within grace window; preempting publisher submissions.");
            await publisher.PreemptSubmissionProcessingAsync(CancellationToken.None).ConfigureAwait(false);
        }

        await dispatcherDrainTask.ConfigureAwait(false);
        publisher.MarkSubmissionPumpFaultDispatchersCompleted(dispatchersCompleted: true);
        drainStopwatch.Stop();

        DateTimeOffset postDrainFinalUtc = DateTimeOffset.UtcNow;
        long postDrainFinalTick = Stopwatch.GetTimestamp();
        TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot diagnosticsAfterDrain = publisher.CaptureConnectionDiagnosticsSnapshot();
        FixedCountBoundarySnapshot postDrainFinalSnapshot = BuildBoundarySnapshot("post-drain-final", postDrainFinalUtc, postDrainFinalTick, metrics, diagnosticsAfterDrain);

        Console.WriteLine("[SHUTDOWN-DIAG] Drain completed: outstandingSubmissions={0} queuedSubmissions={1} pendingMessageIds={2} queuedWriteIntents={3}",
            postDrainFinalSnapshot.CurrentOutstandingSubmissions,
            postDrainFinalSnapshot.QueuedSubmissionCount,
            postDrainFinalSnapshot.PendingOperationsCount,
            postDrainFinalSnapshot.QueuedWriteIntentsCount);

        long drainedAfterMeasurement = Math.Max(0, metrics.GetCompletedCount() - completedAtMeasurementEnd);

        PostMeasurementTerminalizationSummary postMeasurementTerminalization = BuildPostMeasurementTerminalizationSummary(
            measurementEndUtc,
            measurementEndTick,
            completedAtMeasurementEnd,
            metrics,
            atMeasurementEndSnapshot,
            postDrainFinalSnapshot);

        FixedCountBoundaryTelemetry? fixedCountBoundaryTelemetry = config.MeasurementArticleCount is null
            ? null
            : new FixedCountBoundaryTelemetry(
                AtMeasurementEnd: atMeasurementEndSnapshot,
                PostMeasurementPreDrain: postMeasurementPreDrainSnapshot,
                PostDrainFinal: postDrainFinalSnapshot,
                PostMeasurementTerminalization: postMeasurementTerminalization);

        return createBenchmarkResult(
            config,
            metrics.Snapshot(),
            metrics,
            runtime,
            process,
            workload.PreparationSummary,
            measurementStartUtc,
            measurementEndUtc,
            drainStopwatch.Elapsed,
            outstandingAtMeasurementEnd,
            drainedAfterMeasurement,
            allocatedStartBytes,
            enableForensicDiagnostics,
            fixedCountBoundaryTelemetry);
    }

    private static FixedCountBoundarySnapshot BuildBoundarySnapshot(
        string phase,
        DateTimeOffset timestampUtc,
        long stopwatchTick,
        MeasurementMetrics metrics,
        TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot diagnostics)
    {
        BoundaryConnectionSnapshot[] connectionSnapshots = diagnostics.Connections
            .OrderBy(static entry => entry.SlotIndex)
            .ThenBy(static entry => entry.Snapshot.ConnectionId, StringComparer.Ordinal)
            .Select(static entry => new BoundaryConnectionSnapshot(
                SlotIndex: entry.SlotIndex,
                ConnectionId: entry.Snapshot.ConnectionId,
                State: entry.Snapshot.CurrentState.ToString(),
                CurrentConcurrentSubmissions: entry.Snapshot.CurrentConcurrentSubmissions,
                OutstandingOperations: entry.Snapshot.OutstandingOperations.Length,
                CurrentWriteIntentQueueDepth: entry.Snapshot.CurrentWriteIntentQueueDepth,
                SubmissionsStarted: entry.Snapshot.SubmissionsStarted,
                SubmissionsAccepted: entry.Snapshot.SubmissionsAccepted,
                SubmissionsRejected: entry.Snapshot.SubmissionsRejected,
                SubmissionsAmbiguous: entry.Snapshot.SubmissionsAmbiguous,
                SubmissionsUnavailable: entry.Snapshot.SubmissionsUnavailable,
                SubmissionsFailed: entry.Snapshot.SubmissionsFailed))
            .ToArray();

        long pendingOperationsCount = connectionSnapshots.Sum(static x => (long)x.OutstandingOperations);
        long queuedWriteIntentsCount = connectionSnapshots.Sum(static x => x.CurrentWriteIntentQueueDepth);
        long currentOutstandingSubmissions = Math.Max(0, metrics.GetAdmittedCount() - metrics.GetCompletedCount());
        int currentConnectionCount = diagnostics.Connections.Length;
        int activeConnectionCount = diagnostics.Connections.Count(static entry => entry.Snapshot.CurrentState is TransitConnectionState.Ready or TransitConnectionState.Publishing);
        int readyConnectionCount = diagnostics.Connections.Count(static entry => entry.Snapshot.ReadyTransitionCount > 0);

        return new FixedCountBoundarySnapshot(
            Phase: phase,
            TimestampUtc: timestampUtc,
            StopwatchTick: stopwatchTick,
            TotalSubmissionsStarted: metrics.GetAdmittedCount(),
            TotalSubmissionsAccepted: metrics.GetAcceptedCount(),
            TotalSubmissionsRejected: metrics.GetRejectedCount(),
            TotalSubmissionsAmbiguous: metrics.GetAmbiguousOnlyCount(),
            TotalSubmissionsFailed: metrics.GetFailedCount(),
            TotalSubmissionsUnavailable: metrics.GetUnavailableCount(),
            TotalSubmissionsCanceled: metrics.GetCanceledCount(),
            CurrentOutstandingSubmissions: currentOutstandingSubmissions,
            QueuedSubmissionCount: diagnostics.QueuedSubmissionCount,
            PendingOperationsCount: pendingOperationsCount,
            QueuedWriteIntentsCount: queuedWriteIntentsCount,
            CurrentConnectionCount: currentConnectionCount,
            ActiveConnectionCount: activeConnectionCount,
            ReadyConnectionCount: readyConnectionCount,
            Connections: connectionSnapshots);
    }

    private static PostMeasurementTerminalizationSummary BuildPostMeasurementTerminalizationSummary(
        DateTimeOffset measurementEndUtc,
        long measurementEndTick,
        long completedAtMeasurementEnd,
        MeasurementMetrics metrics,
        FixedCountBoundarySnapshot atMeasurementEnd,
        FixedCountBoundarySnapshot postDrainFinal)
    {
        long terminalizedBeforeMeasurementEnd = completedAtMeasurementEnd;
        long terminalizedAfterMeasurementEnd = Math.Max(0, metrics.GetCompletedCount() - completedAtMeasurementEnd);

        long postMeasurementAccepted = Math.Max(0, postDrainFinal.TotalSubmissionsAccepted - atMeasurementEnd.TotalSubmissionsAccepted);
        long postMeasurementRejected = Math.Max(0, postDrainFinal.TotalSubmissionsRejected - atMeasurementEnd.TotalSubmissionsRejected);
        long postMeasurementAmbiguous = Math.Max(0, postDrainFinal.TotalSubmissionsAmbiguous - atMeasurementEnd.TotalSubmissionsAmbiguous);
        long postMeasurementFailed = Math.Max(0, postDrainFinal.TotalSubmissionsFailed - atMeasurementEnd.TotalSubmissionsFailed);
        long postMeasurementUnavailable = Math.Max(0, postDrainFinal.TotalSubmissionsUnavailable - atMeasurementEnd.TotalSubmissionsUnavailable);
        long postMeasurementCanceled = Math.Max(0, postDrainFinal.TotalSubmissionsCanceled - atMeasurementEnd.TotalSubmissionsCanceled);

        PostMeasurementTerminalizationReasons reasons = metrics.CapturePostMeasurementReasons();

        ProvenanceOccurrenceBounds postBounds = metrics.CapturePostMeasurementOccurrenceBounds();
        DateTimeOffset? firstPost = postBounds.FirstTick <= 0
            ? null
            : measurementEndUtc.AddSeconds((postBounds.FirstTick - measurementEndTick) / (double)Stopwatch.Frequency);
        DateTimeOffset? lastPost = postBounds.LastTick <= 0
            ? null
            : measurementEndUtc.AddSeconds((postBounds.LastTick - measurementEndTick) / (double)Stopwatch.Frequency);

        return new PostMeasurementTerminalizationSummary(
            TerminalizedBeforeMeasurementEnd: terminalizedBeforeMeasurementEnd,
            TerminalizedAfterMeasurementEnd: terminalizedAfterMeasurementEnd,
            PostMeasurementAccepted: postMeasurementAccepted,
            PostMeasurementRejected: postMeasurementRejected,
            PostMeasurementAmbiguous: postMeasurementAmbiguous,
            PostMeasurementFailed: postMeasurementFailed,
            PostMeasurementUnavailable: postMeasurementUnavailable,
            PostMeasurementCanceled: postMeasurementCanceled,
            FirstPostMeasurementTerminalizationUtc: firstPost,
            LastPostMeasurementTerminalizationUtc: lastPost,
            Reasons: reasons);
    }
}
