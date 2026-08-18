using VectorNNTP.BackFiller.Benchmarks;
using Xunit;

namespace VectorNNTP.Backfiller.Tests;

/// <summary>
/// Validates queue-depth telemetry aggregation behavior for benchmark measurement snapshots.
/// </summary>
public sealed class MeasurementMetricsQueueTelemetryTests
{
    /// <summary>
    /// Ensures no-sample queue state is represented explicitly by sample count without negative/sentinel depths.
    /// </summary>
    [Fact]
    public void Snapshot_WhenNoQueueSamples_ReportsExplicitNoSampleState()
    {
        MeasurementMetrics metrics = new(articleBytes: 1024);

        MeasurementSnapshot snapshot = metrics.Snapshot();

        Assert.Equal(0, snapshot.QueueDepthSampleCount);
        Assert.Equal(0, snapshot.MinQueueDepth);
        Assert.Equal(0d, snapshot.AverageQueueDepth);
        Assert.Equal(0d, snapshot.AverageQueueBytes);
        Assert.Equal(0, snapshot.PeakQueueDepth);
        Assert.Equal(0, snapshot.PeakQueueBytes);
    }

    /// <summary>
    /// Ensures a single queue sample is preserved exactly in min/avg/peak metrics.
    /// </summary>
    [Fact]
    public void Snapshot_WhenSingleQueueSample_ReportsExactMinAveragePeak()
    {
        MeasurementMetrics metrics = new(articleBytes: 1024);

        metrics.ObservePeaks(queueDepth: 5, queueBytes: 5000, inFlight: 1);

        MeasurementSnapshot snapshot = metrics.Snapshot();

        Assert.Equal(1, snapshot.QueueDepthSampleCount);
        Assert.Equal(5, snapshot.MinQueueDepth);
        Assert.Equal(5d, snapshot.AverageQueueDepth);
        Assert.Equal(5000d, snapshot.AverageQueueBytes);
        Assert.Equal(5, snapshot.PeakQueueDepth);
        Assert.Equal(5000, snapshot.PeakQueueBytes);
    }

    /// <summary>
    /// Ensures multi-sample telemetry computes mathematically correct min/avg/peak values including return-to-zero depth.
    /// </summary>
    [Fact]
    public void Snapshot_WhenMultipleQueueSamplesIncludeZero_ComputesExpectedAggregates()
    {
        MeasurementMetrics metrics = new(articleBytes: 1024);

        metrics.ObservePeaks(queueDepth: 7, queueBytes: 7000, inFlight: 2);
        metrics.ObservePeaks(queueDepth: 0, queueBytes: 0, inFlight: 1);
        metrics.ObservePeaks(queueDepth: 3, queueBytes: 3000, inFlight: 1);

        MeasurementSnapshot snapshot = metrics.Snapshot();

        Assert.Equal(3, snapshot.QueueDepthSampleCount);
        Assert.Equal(0, snapshot.MinQueueDepth);
        Assert.Equal(10d / 3d, snapshot.AverageQueueDepth, precision: 10);
        Assert.Equal(10000d / 3d, snapshot.AverageQueueBytes, precision: 10);
        Assert.Equal(7, snapshot.PeakQueueDepth);
        Assert.Equal(7000, snapshot.PeakQueueBytes);
    }

    /// <summary>
    /// Ensures rapidly changing queue observations never produce negative queue metrics.
    /// </summary>
    [Fact]
    public void Snapshot_WhenQueueDepthChangesRapidly_NeverReportsNegativeMetrics()
    {
        MeasurementMetrics metrics = new(articleBytes: 1024);

        for (int i = 0; i < 1000; i++)
        {
            int depth = i % 32;
            long bytes = depth * 1024L;
            metrics.ObservePeaks(queueDepth: depth, queueBytes: bytes, inFlight: depth);
        }

        MeasurementSnapshot snapshot = metrics.Snapshot();

        Assert.True(snapshot.QueueDepthSampleCount > 0);
        Assert.True(snapshot.MinQueueDepth >= 0);
        Assert.True(snapshot.AverageQueueDepth >= 0);
        Assert.True(snapshot.AverageQueueBytes >= 0);
        Assert.True(snapshot.PeakQueueDepth >= 0);
        Assert.True(snapshot.PeakQueueBytes >= 0);
    }
}
