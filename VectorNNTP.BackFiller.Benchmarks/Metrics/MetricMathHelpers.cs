// <copyright file="MetricMathHelpers.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Metrics/MetricMathHelpers: captures, aggregates, or publishes benchmark throughput, latency, and runtime telemetry.

using System.Diagnostics;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the metric MathHelpers class used by this benchmark or regression-gate component.
/// </summary>
internal static class MetricMathHelpers
{
    /// <summary>
    /// Executes the compute PercentileMicroseconds operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
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
    /// Executes the classify DepthBucket operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the percentile Us operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the ticks ToUs operation while preserving the component's benchmark or test-harness contract.
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
    /// Executes the normalize Min operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal static long NormalizeMin(long value)
    {
        return value == long.MaxValue ? 0 : value;
    }

    /// <summary>
    /// Executes the compute Average operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal static double ComputeAverage(long sum, long count)
    {
        return count <= 0 ? 0 : (double)sum / count;
    }
}
