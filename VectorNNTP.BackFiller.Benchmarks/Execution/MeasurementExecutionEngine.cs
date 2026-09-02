// <copyright file="MeasurementExecutionEngine.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Execution/MeasurementExecutionEngine: coordinates bounded benchmark work, transport lifetimes, and deterministic shutdown.

using System.Diagnostics;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the measurement ExecutionEngine class used by this benchmark or regression-gate component.
/// </summary>
internal static partial class MeasurementExecutionEngine
{
    /// <summary>
    /// Executes the producer LoopAsync operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal static async Task ProducerLoopAsync(
        BoundedArticleQueue queue,
        MeasurementMetrics metrics,
        PreparedBenchmarkWorkload workload,
        int targetQueuedArticles,
        int workerId,
        FixedArticleLimiter? fixedArticleLimiter,
        CancellationToken cancellationToken)
    {
        _ = targetQueuedArticles;
        _ = workerId;

        while (!cancellationToken.IsCancellationRequested)
        {
            if (fixedArticleLimiter is not null && !fixedArticleLimiter.TryReserveNext())
            {
                return;
            }

            if (!workload.TryTakeNextMessageId(out string? messageId))
            {
                return;
            }

            long loopStart = Stopwatch.GetTimestamp();
            long generationEnd = Stopwatch.GetTimestamp();
            Console.WriteLine("[SUBMIT-PATH] stage=generator-created messageId={0} tick={1}", messageId, generationEnd);

            long queueWaitStart = Stopwatch.GetTimestamp();
            bool admittedToQueue = await queue.TryWriteAsync(new QueuedArticle(messageId, workload.PayloadLength), cancellationToken).ConfigureAwait(false);
            long queueWaitEnd = Stopwatch.GetTimestamp();

            if (!admittedToQueue)
            {
                return;
            }

            long loopEnd = Stopwatch.GetTimestamp();
            long loopTicks = Math.Max(0, loopEnd - loopStart);
            long generationTicks = Math.Max(0, generationEnd - loopStart);
            long queueWaitTicks = Math.Max(0, queueWaitEnd - queueWaitStart);
            long activeTicks = Math.Max(0, loopTicks - queueWaitTicks);
            long otherActiveTicks = Math.Max(0, activeTicks - generationTicks);

            TransitBenchmarkCore.ProducerTiming producerTiming = TransitBenchmarkCore.ProducerTiming.FromRaw(
                loopTicks: loopTicks,
                generationTicks: generationTicks,
                blockedTicks: queueWaitTicks,
                otherActiveTicks: otherActiveTicks);

            metrics.OnGenerated(workload.PayloadLength, producerTiming, queueWaitTicks);
        }
    }

    /// <summary>
    /// Executes the dispatch LoopAsync operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal static async Task DispatchLoopAsync(
        BoundedArticleQueue queue,
        TransitPublisher publisher,
        MeasurementMetrics metrics,
        PreparedBenchmarkWorkload workload,
        CancellationToken cancellationToken,
        bool enableForensicDiagnostics)
    {
        bool forensicSnapshotFailureLogged = false;

        while (await queue.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (queue.TryRead(out QueuedArticle queuedArticle))
            {
                long dequeuedTick = Stopwatch.GetTimestamp();
                metrics.OnDequeued(dequeuedTick);
                Interlocked.Increment(ref metrics.InFlightSubmissions);

                try
                {
                    int pendingAtSubmit = 0;
                    if (enableForensicDiagnostics)
                    {
                        try
                        {
                            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot beforeSubmit = publisher.CaptureConnectionDiagnosticsSnapshot();
                            pendingAtSubmit = beforeSubmit.Connections.Sum(static x => x.Snapshot.CurrentConcurrentSubmissions);
                        }
                        catch (Exception ex)
                        {
                            if (!forensicSnapshotFailureLogged)
                            {
                                forensicSnapshotFailureLogged = true;
                                Console.WriteLine("[TELEMETRY-WARN] Forensic pre-submit diagnostics capture failed; continuing. exceptionType={0} message={1}",
                                    ex.GetType().Name,
                                    ex.Message);
                            }
                        }
                    }

                    metrics.OnAdmitted(queuedArticle.PayloadLength, dequeuedTick);
                    long publishStartTick = Stopwatch.GetTimestamp();
                    TransitPublishResult result = await publisher.PublishAsync(queuedArticle.MessageId, workload.ReusableArticlePayload, cancellationToken).ConfigureAwait(false);
                    long publishEndTick = Stopwatch.GetTimestamp();

                    int pendingAtComplete = 0;
                    if (enableForensicDiagnostics)
                    {
                        try
                        {
                            TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot afterSubmit = publisher.CaptureConnectionDiagnosticsSnapshot();
                            pendingAtComplete = afterSubmit.Connections.Sum(static x => x.Snapshot.CurrentConcurrentSubmissions);
                        }
                        catch (Exception ex)
                        {
                            if (!forensicSnapshotFailureLogged)
                            {
                                forensicSnapshotFailureLogged = true;
                                Console.WriteLine("[TELEMETRY-WARN] Forensic post-submit diagnostics capture failed; continuing. exceptionType={0} message={1}",
                                    ex.GetType().Name,
                                    ex.Message);
                            }
                        }
                    }

                    metrics.OnPublishResult(result, queuedArticle.PayloadLength, dequeuedTick, publishStartTick, publishEndTick, pendingAtSubmit, pendingAtComplete);
                }
                finally
                {
                    Interlocked.Decrement(ref metrics.InFlightSubmissions);
                    queue.ReleaseReservation(queuedArticle.PayloadLength);
                }
            }
        }

        TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot dispatcherExitDiagnostics = publisher.CaptureConnectionDiagnosticsSnapshot();
        int dispatcherExitPendingMessageIds = dispatcherExitDiagnostics.Connections.Sum(static entry => entry.Snapshot.OutstandingOperations.Length);
        long dispatcherExitQueuedWriteIntents = dispatcherExitDiagnostics.Connections.Sum(static entry => entry.Snapshot.CurrentWriteIntentQueueDepth);
        Console.WriteLine("[SHUTDOWN-DIAG] DispatchLoop exit: queuedSubmissions={0} pendingMessageIds={1} queuedWriteIntents={2}",
            dispatcherExitDiagnostics.QueuedSubmissionCount,
            dispatcherExitPendingMessageIds,
            dispatcherExitQueuedWriteIntents);
    }

    /// <summary>
    /// Executes the telemetry LoopAsync operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal static async Task TelemetryLoopAsync(
        BoundedArticleQueue queue,
        MeasurementMetrics metrics,
        RuntimeMetrics runtime,
        Process process,
        long allocatedStartBytes,
        TransitPublisher publisher,
        int queueTargetArticles,
        bool enableForensicDiagnostics,
        CancellationToken cancellationToken)
    {
        Console.WriteLine("elapsed_s gen_art_s gen_MB_s gen_Gbps adm_art_s adm_MB_s acc_art_s acc_MB_s acc_Gbps rej_art_s amb_art_s q_depth q_bytes inflight dispatch_pending actual_pending peak_conn_inflight conn_ready active_slots host_cpu_pct transit_cpu_pct cpu_pct ws_mb heap_mb alloc_mb gen0 gen1 gen2 prod_active_pct prod_blocked_pct prod_active_ms prod_blocked_ms queue_wait_ms");
        Console.WriteLine("NOTE: generated/admitted/accepted are distinct throughput classes; accepted is based on definitive TransitServer success responses.");
        Console.WriteLine($"Queue target depth (articles): {queueTargetArticles}");

        Stopwatch elapsed = Stopwatch.StartNew();
        MeasurementSnapshot previous = metrics.Snapshot();
        TimeSpan previousElapsed = TimeSpan.Zero;
        TimeSpan previousCpu = process.TotalProcessorTime;
        bool telemetryFailureLogged = false;

        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);

            try
            {
                TimeSpan now = elapsed.Elapsed;
                double seconds = (now - previousElapsed).TotalSeconds;
                if (seconds <= 0)
                {
                    continue;
                }

                MeasurementSnapshot current = metrics.Snapshot();

                long generatedCountDelta = current.GeneratedCount - previous.GeneratedCount;
                long generatedBytesDelta = current.GeneratedBytes - previous.GeneratedBytes;
                long admittedCountDelta = current.AdmittedCount - previous.AdmittedCount;
                long admittedBytesDelta = current.AdmittedBytes - previous.AdmittedBytes;
                long acceptedCountDelta = current.AcceptedCount - previous.AcceptedCount;
                long acceptedBytesDelta = current.AcceptedBytes - previous.AcceptedBytes;
                long rejectedCountDelta = current.RejectedCount - previous.RejectedCount;
                long ambiguousCountDelta = current.AmbiguousCount - previous.AmbiguousCount;

                int queueDepth = queue.CurrentQueuedCount;
                long queueBytes = queue.CurrentQueuedBytes;
                int inFlight = Volatile.Read(ref metrics.InFlightSubmissions);

                TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot diagnostics = publisher.CaptureConnectionDiagnosticsSnapshot();
                int actualPending = diagnostics.Connections.Sum(static x => x.Snapshot.CurrentConcurrentSubmissions);
                int peakConnectionInFlight = diagnostics.Connections.Length == 0 ? 0 : diagnostics.Connections.Max(static x => x.Snapshot.MaxConcurrentSubmissions);
                int readyConnections = diagnostics.Connections.Count(static x => x.Snapshot.ReadyTransitionCount > 0);
                int activeSlots = diagnostics.Slots.Count(static x => x.TotalSubmissionsRouted > 0);

                metrics.ObservePeaks(queueDepth, queueBytes, inFlight);
                metrics.ObserveActualPending(actualPending);

                TimeSpan cpuNow = process.TotalProcessorTime;
                double cpuPercent = (cpuNow - previousCpu).TotalSeconds / (Environment.ProcessorCount * seconds) * 100d;
                previousCpu = cpuNow;

                double hostCpuPercent = RuntimeMetricSamplingHelpers.ReadHostCpuPercent();
                double transitServerCpuPercent = RuntimeMetricSamplingHelpers.ReadTransitServerCpuPercent();

                long workingSet = process.WorkingSet64;
                long gcHeapBytes = GC.GetTotalMemory(forceFullCollection: false);
                long allocatedBytes = GC.GetTotalAllocatedBytes(precise: false) - allocatedStartBytes;

                runtime.Sample(cpuPercent, hostCpuPercent, transitServerCpuPercent, workingSet, gcHeapBytes, allocatedBytes);

                if (enableForensicDiagnostics)
                {
                    metrics.RecordConnectionSample(diagnostics, now);
                    metrics.RecordDispatcherSample(now, inFlight, current.AdmittedCount - current.CompletedCount, actualPending, queueDepth, queueBytes);
                }

                long activeTicksDelta = current.ActiveTicks - previous.ActiveTicks;
                long blockedTicksDelta = current.BlockedTicks - previous.BlockedTicks;
                long producerObservedTicksDelta = activeTicksDelta + blockedTicksDelta;

                double blockedPercent = producerObservedTicksDelta <= 0
                    ? 0
                    : blockedTicksDelta * 100d / producerObservedTicksDelta;

                double activePercent = producerObservedTicksDelta <= 0
                    ? 0
                    : activeTicksDelta * 100d / producerObservedTicksDelta;

                double activeMilliseconds = TransitBenchmarkCore.StopwatchTicksToMilliseconds(activeTicksDelta);
                double blockedMilliseconds = TransitBenchmarkCore.StopwatchTicksToMilliseconds(blockedTicksDelta);
                long queueWaitTicksDelta = current.ProducerQueueWaitTicks - previous.ProducerQueueWaitTicks;
                double queueWaitMilliseconds = TransitBenchmarkCore.StopwatchTicksToMilliseconds(queueWaitTicksDelta);

                Console.WriteLine($"{now.TotalSeconds:F1} {generatedCountDelta / seconds:F2} {generatedBytesDelta / 1024d / 1024d / seconds:F2} {generatedBytesDelta * 8d / 1_000_000_000d / seconds:F4} {admittedCountDelta / seconds:F2} {admittedBytesDelta / 1024d / 1024d / seconds:F2} {acceptedCountDelta / seconds:F2} {acceptedBytesDelta / 1024d / 1024d / seconds:F2} {acceptedBytesDelta * 8d / 1_000_000_000d / seconds:F4} {rejectedCountDelta / seconds:F2} {ambiguousCountDelta / seconds:F2} {queueDepth} {queueBytes} {inFlight} {current.AdmittedCount - current.CompletedCount} {actualPending} {peakConnectionInFlight} {readyConnections} {activeSlots} {hostCpuPercent:F2} {transitServerCpuPercent:F2} {cpuPercent:F2} {workingSet / 1024d / 1024d:F2} {gcHeapBytes / 1024d / 1024d:F2} {allocatedBytes / 1024d / 1024d:F2} {GC.CollectionCount(0)} {GC.CollectionCount(1)} {GC.CollectionCount(2)} {activePercent:F2} {blockedPercent:F2} {activeMilliseconds:F2} {blockedMilliseconds:F2} {queueWaitMilliseconds:F2}");

                previous = current;
                previousElapsed = now;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                if (!telemetryFailureLogged)
                {
                    telemetryFailureLogged = true;
                    Console.WriteLine("[TELEMETRY-WARN] Telemetry loop iteration failed; continuing. exceptionType={0} message={1}",
                        ex.GetType().Name,
                        ex.Message);
                }
            }
        }
    }
}
