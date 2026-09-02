// <copyright file="MetricMathHelpers.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Metrics/MetricMathHelpers: supplies numerically stable conversions and summary calculations for measurements.

using System.Diagnostics;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the metric MathHelpers class used by the benchmark or regression gate.
/// </summary>
internal static class MetricMathHelpers
{
    /// <summary>
    /// Computes a nearest-rank latency percentile in microseconds from sorted stopwatch ticks.
    /// </summary>
    /// <param name="sortedLatencyTicks">Latency ticks sorted in ascending order.</param>
    /// <param name="percentile">Percentile in the range 0 through 1; values outside the range are clamped.</param>
    /// <returns>The selected latency in microseconds, or zero when no samples are present.</returns>
    internal static double ComputePercentileMicroseconds(List<long> sortedLatencyTicks, double percentile)
    {
        ArgumentNullException.ThrowIfNull(sortedLatencyTicks);

        if (sortedLatencyTicks.Count == 0)
        {
            return 0;
        }

        percentile = Math.Clamp(percentile, 0d, 1d);
        int index = (int)Math.Clamp(Math.Ceiling(percentile * sortedLatencyTicks.Count) - 1, 0, sortedLatencyTicks.Count - 1);
        long ticks = sortedLatencyTicks[index];
        return TransitBenchmarkCore.StopwatchTicksToMilliseconds(ticks) * 1000d;
    }

    /// <summary>
    /// Implements the classify DepthBucket contract.
    /// </summary>
    internal static int ClassifyDepthBucket(int pending)
    {
        if (pending <= 4) return 0;
        if (pending <= 8) return 1;
        if (pending <= 12) return 2;
        if (pending <= 16) return 3;
        return 4;
    }

    /// <summary>
    /// Implements the percentile Us contract.
    /// </summary>
    internal static double PercentileUs(List<long> samples, double percentile)
    {
        if (samples.Count == 0)
        {
            return 0;
        }

        List<long> sorted = [.. samples];
        sorted.Sort();
        int index = (int)Math.Clamp(Math.Ceiling(percentile * sorted.Count) - 1, 0, sorted.Count - 1);
        return TicksToUs(sorted[index]);
    }

    /// <summary>
    /// Implements the ticks ToUs contract.
    /// </summary>
    internal static double TicksToUs(double ticks)
    {
        if (ticks <= 0)
        {
            return 0;
        }

        return ticks * 1_000_000d / Stopwatch.Frequency;
    }

    /// <summary>
    /// Normalizes Min.

    /// </summary>
    internal static long NormalizeMin(long value)
    {
        return value == long.MaxValue ? 0 : value;
    }

    /// <summary>
    /// Computes Average.

    /// </summary>
    internal static double ComputeAverage(long sum, long count)
    {
        return count <= 0 ? 0 : (double)sum / count;
    }
}
