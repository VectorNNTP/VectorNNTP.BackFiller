using System.Diagnostics;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

internal static class MeasurementExecutionEngine
{
    internal static async Task ProducerLoopAsync(
        BoundedArticleQueue queue,
        MeasurementMetrics metrics,
        PreparedBenchmarkWorkload workload,
        int targetQueuedArticles,
        int workerId,
        CancellationToken cancellationToken)
    {
        _ = targetQueuedArticles;
        _ = workerId;

        while (!cancellationToken.IsCancellationRequested)
        {
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

    internal static async Task DispatchLoopAsync(
        BoundedArticleQueue queue,
        TransitPublisher publisher,
        MeasurementMetrics metrics,
        PreparedBenchmarkWorkload workload,
        CancellationToken cancellationToken,
        bool enableForensicDiagnostics)
    {
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
                        TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot beforeSubmit = publisher.CaptureConnectionDiagnosticsSnapshot();
                        pendingAtSubmit = beforeSubmit.Connections.Sum(static x => x.Snapshot.CurrentConcurrentSubmissions);
                    }

                    metrics.OnAdmitted(queuedArticle.PayloadLength, dequeuedTick);
                    long publishStartTick = Stopwatch.GetTimestamp();
                    TransitPublishResult result = await publisher.PublishAsync(queuedArticle.MessageId, workload.ReusableArticlePayload, cancellationToken).ConfigureAwait(false);
                    long publishEndTick = Stopwatch.GetTimestamp();

                    int pendingAtComplete = 0;
                    if (enableForensicDiagnostics)
                    {
                        TransitPublisher.TransitPublisherConnectionDiagnosticsSnapshot afterSubmit = publisher.CaptureConnectionDiagnosticsSnapshot();
                        pendingAtComplete = afterSubmit.Connections.Sum(static x => x.Snapshot.CurrentConcurrentSubmissions);
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
        Console.WriteLine("[SHUTDOWN-DIAG] DispatchLoop exit: queuedSubmissions={QueuedSubmissions} pendingMessageIds={PendingMessageIds} queuedWriteIntents={QueuedWriteIntents}",
            dispatcherExitDiagnostics.QueuedSubmissionCount,
            dispatcherExitPendingMessageIds,
            dispatcherExitQueuedWriteIntents);
    }
}
