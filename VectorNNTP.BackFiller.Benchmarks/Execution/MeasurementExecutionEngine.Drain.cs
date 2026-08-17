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
        Func<TransitBenchmarkConfig, MeasurementSnapshot, MeasurementMetrics, RuntimeMetrics, Process, WorkloadPreparationSummary, DateTimeOffset, DateTimeOffset, TimeSpan, long, long, long, bool, BenchmarkResult> createBenchmarkResult)
    {
        DateTimeOffset measurementEndUtc = DateTimeOffset.UtcNow;
        Console.WriteLine($"Measurement end UTC:   {measurementEndUtc:O}");

        producerStopCts.Cancel();

        try
        {
            await Task.WhenAll(producerTasks).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }

        queue.StopAdmission();

        long outstandingAtMeasurementEnd = metrics.GetAdmittedCount() - metrics.GetCompletedCount();
        long completedAtMeasurementEnd = metrics.GetCompletedCount();

        try
        {
            await telemetryTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }

        TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot diagnosticsAtMeasurementEnd = publisher.CaptureConnectionDiagnosticsSnapshot();
        int pendingMessageIdsAtMeasurementEnd = diagnosticsAtMeasurementEnd.Connections.Sum(static entry => entry.Snapshot.OutstandingOperations.Length);
        long queuedWriteIntentsAtMeasurementEnd = diagnosticsAtMeasurementEnd.Connections.Sum(static entry => entry.Snapshot.CurrentWriteIntentQueueDepth);

        Console.WriteLine("[SHUTDOWN-DIAG] Measurement window expired: outstandingSubmissions={OutstandingSubmissions} queuedSubmissions={QueuedSubmissions} pendingMessageIds={PendingMessageIds} queuedWriteIntents={QueuedWriteIntents}",
            outstandingAtMeasurementEnd,
            diagnosticsAtMeasurementEnd.QueuedSubmissionCount,
            pendingMessageIdsAtMeasurementEnd,
            queuedWriteIntentsAtMeasurementEnd);

        foreach (TransitPublisher.ConnectionDiagnosticsEntry entry in diagnosticsAtMeasurementEnd.Connections
                     .OrderBy(static x => x.SlotIndex)
                     .ThenBy(static x => x.Snapshot.ConnectionId, StringComparer.Ordinal))
        {
            TransitConnection.TransitConnectionDiagnosticsSnapshot snapshot = entry.Snapshot;
            Console.WriteLine("[SHUTDOWN-DIAG] Measurement-end connection snapshot: slot={SlotIndex} connectionId={ConnectionId} state={State} inFlight={InFlight} writeQueueDepth={WriteQueueDepth} pendingMessageIds={PendingMessageIds}",
                entry.SlotIndex,
                snapshot.ConnectionId,
                snapshot.CurrentState,
                snapshot.CurrentConcurrentSubmissions,
                snapshot.CurrentWriteIntentQueueDepth,
                snapshot.OutstandingOperations.Length);

            foreach (TransitConnection.OutstandingPublishOperationSnapshot operation in snapshot.OutstandingOperations)
            {
                Console.WriteLine("[SHUTDOWN-DIAG] Outstanding operation: connectionId={ConnectionId} messageId={MessageId} writeIntentEnqueued={WriteIntentEnqueued} takethisStagedForWrite={TakethisStagedForWrite} flushCompleted={FlushCompleted} waitingFor239Response={WaitingFor239Response} completionTaskStatus={CompletionTaskStatus} completionStatus={CompletionStatus} likelyAwaitingPath={LikelyAwaitingPath} t2Enqueued={T2WriteIntentEnqueuedTick} t6Staged={T6FrameStageEndTick} t8Flush={T8BatchFlushEndTick} t9Correlated={T9ResponseCorrelatedTick}",
                    snapshot.ConnectionId,
                    operation.MessageId,
                    operation.WriteIntentEnqueued,
                    operation.TakethisStagedForWrite,
                    operation.FlushCompleted,
                    operation.WaitingFor239Response,
                    operation.CompletionTaskStatus,
                    operation.CompletionStatus?.ToString() ?? "(null)",
                    operation.LikelyAwaitingPath,
                    operation.T2WriteIntentEnqueuedTick,
                    operation.T6FrameStageEndTick,
                    operation.T8BatchFlushEndTick,
                    operation.T9ResponseCorrelatedTick);
            }
        }

        Console.WriteLine();
        Console.WriteLine("=== Phase 6: Drain ===");
        Stopwatch drainStopwatch = Stopwatch.StartNew();
        await Task.WhenAll(dispatchers).ConfigureAwait(false);
        drainStopwatch.Stop();

        TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot diagnosticsAfterDrain = publisher.CaptureConnectionDiagnosticsSnapshot();
        int pendingMessageIdsAfterDrain = diagnosticsAfterDrain.Connections.Sum(static entry => entry.Snapshot.OutstandingOperations.Length);
        long queuedWriteIntentsAfterDrain = diagnosticsAfterDrain.Connections.Sum(static entry => entry.Snapshot.CurrentWriteIntentQueueDepth);

        Console.WriteLine("[SHUTDOWN-DIAG] Drain completed: outstandingSubmissions={OutstandingSubmissions} queuedSubmissions={QueuedSubmissions} pendingMessageIds={PendingMessageIds} queuedWriteIntents={QueuedWriteIntents}",
            metrics.GetAdmittedCount() - metrics.GetCompletedCount(),
            diagnosticsAfterDrain.QueuedSubmissionCount,
            pendingMessageIdsAfterDrain,
            queuedWriteIntentsAfterDrain);

        long drainedAfterMeasurement = Math.Max(0, metrics.GetCompletedCount() - completedAtMeasurementEnd);

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
            enableForensicDiagnostics);
    }
}
