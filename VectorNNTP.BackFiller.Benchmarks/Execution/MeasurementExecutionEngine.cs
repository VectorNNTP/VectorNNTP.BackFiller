using System.Diagnostics;

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
}
