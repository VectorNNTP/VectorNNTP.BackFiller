using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

/// <summary>
/// Validates the dispatch consumer queue-read forensic recorder: state tracking, failed-TryRead
/// reconciliation against queue depth accounting, long-wait interval decomposition and artifact export.
/// </summary>
public sealed class QueueConsumerForensicsTests
{
    /// <summary>
    /// Ensures a successful read transfers ownership from the channel accounting to the consumer.
    /// </summary>
    [Fact]
    public void BuildReport_WhenReadSucceeds_ReportsConsumerOwnedArticle()
    {
        QueueConsumerForensics forensics = new(consumerCount: 2);
        QueueConsumerProbe probe = forensics.GetProbe(0);

        probe.RecordWaitStart(queueDepth: 1, queueBytes: 1024, waitCompletedSynchronously: true);
        probe.RecordWaitReturn(waitResult: true, queueDepth: 1, queueBytes: 1024);
        probe.RecordTryReadStart(queueDepth: 1, queueBytes: 1024);
        probe.RecordTryReadEnd(success: true, queueDepthAfter: 0, queueBytesAfter: 0);

        Assert.Equal(QueueConsumerState.ProcessingArticle, probe.State);

        QueueConsumerForensicsReport inFlightReport = forensics.BuildReport(channelQueuedByAccounting: 0, channelQueuedBytesByAccounting: 0, transportInFlightArticles: 3);
        Assert.Equal(1, inFlightReport.Ownership.ConsumerOwnedArticles);
        Assert.Equal(3, inFlightReport.Ownership.TransportInFlightArticles);
        Assert.Equal(4, inFlightReport.Ownership.TotalOutstandingWork);
        Assert.Equal(1, inFlightReport.TryReadSuccessCount);
        Assert.Equal(0, inFlightReport.TryReadFailureCount);
        Assert.Equal(1, inFlightReport.WaitEpisodesCompletedSynchronously);
        Assert.Equal(0, inFlightReport.WaitEpisodesParked);

        probe.RecordProcessingComplete();

        QueueConsumerForensicsReport settledReport = forensics.BuildReport(channelQueuedByAccounting: 0, channelQueuedBytesByAccounting: 0, transportInFlightArticles: 0);
        Assert.Equal(0, settledReport.Ownership.ConsumerOwnedArticles);
    }

    /// <summary>
    /// Ensures failed reads are classified against the queue depth observed immediately before and after the read.
    /// </summary>
    [Theory]
    [InlineData(0, 0, nameof(TryReadFailureClass.CountZeroBefore))]
    [InlineData(992, 992, nameof(TryReadFailureClass.CountPositiveBefore))]
    [InlineData(4, 3, nameof(TryReadFailureClass.CountChangedDuringObservation))]
    public void RecordTryReadEnd_WhenReadFails_ClassifiesAgainstQueueDepth(int depthBefore, int depthAfter, string expectedClassification)
    {
        QueueConsumerForensics forensics = new(consumerCount: 1);
        QueueConsumerProbe probe = forensics.GetProbe(0);

        probe.RecordWaitStart(queueDepth: depthBefore, queueBytes: depthBefore, waitCompletedSynchronously: true);
        probe.RecordWaitReturn(waitResult: true, queueDepth: depthBefore, queueBytes: depthBefore);
        probe.RecordTryReadStart(queueDepth: depthBefore, queueBytes: depthBefore);
        probe.RecordTryReadEnd(success: false, queueDepthAfter: depthAfter, queueBytesAfter: depthAfter);

        QueueConsumerForensicsReport report = forensics.BuildReport(depthAfter, depthAfter, transportInFlightArticles: 0);

        Assert.Equal(1, report.TryReadFailureCount);
        TryReadFailureRecord failure = Assert.Single(report.TryReadFailures);
        Assert.Equal(expectedClassification, failure.Classification);
        Assert.Equal(depthBefore, failure.QueueDepthBefore);
        Assert.Equal(depthAfter, failure.QueueDepthAfter);
        Assert.Equal(
            string.Equals(expectedClassification, nameof(TryReadFailureClass.CountPositiveBefore), StringComparison.Ordinal),
            report.AnyTryReadFailureWithPositiveDepth);
    }

    /// <summary>
    /// Ensures a wait longer than the threshold is decomposed into the A-E intervals and correlated with the first enqueue.
    /// </summary>
    [Fact]
    public async Task RecordWaitReturn_WhenWaitExceedsThreshold_RecordsLongWaitWithIntervals()
    {
        QueueConsumerForensics forensics = new(consumerCount: 1, longWaitThresholdMilliseconds: 1);
        QueueConsumerProbe probe = forensics.GetProbe(0);

        probe.RecordWaitStart(queueDepth: 0, queueBytes: 0, waitCompletedSynchronously: false);
        await Task.Delay(millisecondsDelay: 25).ConfigureAwait(true);
        forensics.RecordEnqueue(System.Diagnostics.Stopwatch.GetTimestamp(), System.Diagnostics.Stopwatch.GetTimestamp());
        await Task.Delay(millisecondsDelay: 5).ConfigureAwait(true);
        probe.RecordWaitReturn(waitResult: true, queueDepth: 1, queueBytes: 4096);
        probe.RecordTryReadStart(queueDepth: 1, queueBytes: 4096);
        probe.RecordTryReadEnd(success: true, queueDepthAfter: 0, queueBytesAfter: 0);

        QueueConsumerForensicsReport report = forensics.BuildReport(0, 0, transportInFlightArticles: 0);

        Assert.Equal(1, report.LongWaitEpisodeCount);
        LongWaitRecord longWait = Assert.Single(report.FirstLongWaits);
        Assert.Equal(1, longWait.Ordinal);
        Assert.False(longWait.WaitCompletedSynchronously);
        Assert.True(longWait.TryReadResult);
        Assert.Equal(nameof(EnqueueCorrelation.Resolved), longWait.EnqueueCorrelation);
        Assert.True(longWait.IntervalAWaitStartToFirstEnqueueUs > 0);
        Assert.True(longWait.IntervalCBatchEligibleToWaitReturnUs > 0);
        Assert.True(longWait.TotalWaitUs >= longWait.IntervalAWaitStartToFirstEnqueueUs);
        Assert.NotNull(longWait.WaitStartStack);
        Assert.Contains(nameof(QueueConsumerProbe.RecordWaitStart), longWait.WaitStartStack, StringComparison.Ordinal);

        IntervalStatistics intervalA = Assert.Single(report.Intervals, statistics => string.Equals(statistics.Interval, "A", StringComparison.Ordinal));
        Assert.Equal(1, intervalA.SampleCount);
        Assert.True(intervalA.MaxMicroseconds > 0);
    }

    /// <summary>
    /// Ensures no enqueue between WAIT_START and WAIT_RETURN is reported as such instead of being invented.
    /// </summary>
    [Fact]
    public async Task RecordWaitReturn_WhenNoEnqueueObserved_ReportsUnresolvedIntervals()
    {
        QueueConsumerForensics forensics = new(consumerCount: 1, longWaitThresholdMilliseconds: 1);
        QueueConsumerProbe probe = forensics.GetProbe(0);

        probe.RecordWaitStart(queueDepth: 0, queueBytes: 0, waitCompletedSynchronously: false);
        await Task.Delay(millisecondsDelay: 20).ConfigureAwait(true);
        probe.RecordWaitReturn(waitResult: false, queueDepth: 0, queueBytes: 0);
        probe.RecordTryReadStart(queueDepth: 0, queueBytes: 0);
        probe.RecordTryReadEnd(success: false, queueDepthAfter: 0, queueBytesAfter: 0);

        QueueConsumerForensicsReport report = forensics.BuildReport(0, 0, transportInFlightArticles: 0);

        LongWaitRecord longWait = Assert.Single(report.FirstLongWaits);
        Assert.Equal(nameof(EnqueueCorrelation.NoEnqueueObserved), longWait.EnqueueCorrelation);
        Assert.True(double.IsNaN(longWait.IntervalAWaitStartToFirstEnqueueUs));
        Assert.True(double.IsNaN(longWait.IntervalBFirstEnqueueToBatchEligibleUs));
        Assert.True(double.IsNaN(longWait.IntervalCBatchEligibleToWaitReturnUs));
        Assert.False(double.IsNaN(longWait.IntervalDWaitReturnToTryReadStartUs));

        IntervalStatistics intervalA = Assert.Single(report.Intervals, statistics => string.Equals(statistics.Interval, "A", StringComparison.Ordinal));
        Assert.Equal(0, intervalA.SampleCount);
    }

    /// <summary>
    /// Ensures representative managed stacks are captured for waiter-count buckets and remain bounded.
    /// </summary>
    [Fact]
    public void RecordWaitStart_WhenManyConsumersWait_CapturesBoundedRepresentativeStacks()
    {
        QueueConsumerForensics forensics = new(consumerCount: 128);

        for (int consumerId = 0; consumerId < 128; consumerId++)
        {
            forensics.GetProbe(consumerId).RecordWaitStart(queueDepth: 992, queueBytes: 992 * 1024, waitCompletedSynchronously: false);
        }

        Assert.Equal(128, forensics.CurrentWaiters);
        Assert.Equal(128, forensics.MaxConcurrentWaiters);

        ConsumerStateCensus census = forensics.CaptureStateCensus();
        Assert.Equal(128, census.WaitingToRead);
        Assert.Equal(0, census.ProcessingArticle);

        QueueConsumerForensicsReport report = forensics.BuildReport(992, 992 * 1024, transportInFlightArticles: 0);

        Assert.NotEmpty(report.StackSamples);
        Assert.True(report.StackSamples.Count <= forensics.WaiterBuckets.Count * 4);
        Assert.All(report.StackSamples, sample => Assert.Equal("WAIT_START", sample.Phase));
        Assert.All(report.StackSamples, sample => Assert.Contains(forensics.WaiterBuckets, bucket => bucket == sample.WaiterBucket));
        Assert.Contains(report.StackSamples, sample => sample.WaiterBucket == 1);
        Assert.Contains(report.StackSamples, sample => sample.WaiterBucket == 100);
    }

    /// <summary>
    /// Ensures the queue reports producer enqueues to the recorder with both the channel-visible and accounting-visible instants.
    /// </summary>
    [Fact]
    public async Task TryWriteAsync_WhenForensicsAttached_RecordsEnqueueTimeline()
    {
        QueueConsumerForensics forensics = new(consumerCount: 1, longWaitThresholdMilliseconds: 1);
        using BoundedArticleQueue queue = new(maxArticles: 8, maxResidentBytes: 1024L * 1024L, forensics);
        QueueConsumerProbe probe = forensics.GetProbe(0);

        probe.RecordWaitStart(queue.CurrentQueuedCount, queue.CurrentQueuedBytes, waitCompletedSynchronously: false);
        await Task.Delay(millisecondsDelay: 5).ConfigureAwait(true);

        Assert.True(await queue.TryWriteAsync(new QueuedArticle("<forensic-1@example.com>", 4096), CancellationToken.None).ConfigureAwait(true));
        Assert.True(await queue.TryWriteAsync(new QueuedArticle("<forensic-2@example.com>", 4096), CancellationToken.None).ConfigureAwait(true));

        probe.RecordWaitReturn(waitResult: true, queue.CurrentQueuedCount, queue.CurrentQueuedBytes);
        probe.RecordTryReadStart(queue.CurrentQueuedCount, queue.CurrentQueuedBytes);
        bool read = queue.TryRead(out QueuedArticle article);
        probe.RecordTryReadEnd(read, queue.CurrentQueuedCount, queue.CurrentQueuedBytes);

        Assert.True(read);
        Assert.Equal("<forensic-1@example.com>", article.MessageId);

        QueueConsumerForensicsReport report = forensics.BuildReport(queue.CurrentQueuedCount, queue.CurrentQueuedBytes, transportInFlightArticles: 0);

        Assert.Equal(2, report.EnqueueCount);
        LongWaitRecord longWait = Assert.Single(report.FirstLongWaits);
        Assert.Equal(nameof(EnqueueCorrelation.Resolved), longWait.EnqueueCorrelation);
        Assert.True(longWait.IntervalAWaitStartToFirstEnqueueUs > 0);
        Assert.True(longWait.IntervalBFirstEnqueueToBatchEligibleUs >= 0);
        Assert.Equal(2, longWait.QueueDepthAtWaitReturn);
        Assert.Equal(1, longWait.QueueDepthAfterTryRead);
    }

    /// <summary>
    /// Ensures the exported artifacts are written with the contracted names and contain the required sections.
    /// </summary>
    [Fact]
    public void Write_ProducesJsonAndTextArtifactsWithRequiredSections()
    {
        QueueConsumerForensics forensics = new(consumerCount: 1);
        QueueConsumerProbe probe = forensics.GetProbe(0);

        probe.RecordWaitStart(queueDepth: 992, queueBytes: 992, waitCompletedSynchronously: false);
        probe.RecordWaitReturn(waitResult: true, queueDepth: 992, queueBytes: 992);
        probe.RecordTryReadStart(queueDepth: 992, queueBytes: 992);
        probe.RecordTryReadEnd(success: false, queueDepthAfter: 992, queueBytesAfter: 992);

        QueueConsumerForensicsReport report = forensics.BuildReport(992, 992, transportInFlightArticles: 7);

        string directory = Path.Combine(Path.GetTempPath(), "queue-consumer-forensics-" + Guid.NewGuid().ToString("N"));
        try
        {
            (string jsonPath, string textPath) = QueueConsumerForensicsWriter.Write(report, directory);

            Assert.Equal(QueueConsumerForensicsWriter.JsonFileName, Path.GetFileName(jsonPath));
            Assert.Equal(QueueConsumerForensicsWriter.TextFileName, Path.GetFileName(textPath));

            string json = File.ReadAllText(jsonPath);
            Assert.Contains("\"TryReadFailuresClassB\"", json, StringComparison.Ordinal);
            Assert.Contains("\"StackSamples\"", json, StringComparison.Ordinal);

            string text = File.ReadAllText(textPath);
            Assert.Contains("=== DISPATCH CONSUMER QUEUE-READ FORENSICS ===", text, StringComparison.Ordinal);
            Assert.Contains("--- FAILED TRYREAD RECONCILIATION AGAINST CurrentQueuedCount ---", text, StringComparison.Ordinal);
            Assert.Contains("--- INTERVAL BREAKDOWN (microseconds, long-wait episodes) ---", text, StringComparison.Ordinal);
            Assert.Contains("--- OUTSTANDING WORK OWNERSHIP ---", text, StringComparison.Ordinal);
            Assert.Contains("--- REPRESENTATIVE MANAGED STACKS ---", text, StringComparison.Ordinal);
            Assert.Contains("TryRead == false DID occur while CurrentQueuedCount > 0", text, StringComparison.Ordinal);
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }
}
