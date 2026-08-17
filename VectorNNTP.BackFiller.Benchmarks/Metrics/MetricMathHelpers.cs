using System.Diagnostics;

namespace VectorNNTP.BackFiller.Benchmarks;

internal static class MetricMathHelpers
{
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

    internal static int ClassifyDepthBucket(int pending)
    {
        if (pending <= 4) return 0;
        if (pending <= 8) return 1;
        if (pending <= 12) return 2;
        if (pending <= 16) return 3;
        return 4;
    }

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

    internal static double TicksToUs(double ticks)
    {
        if (ticks <= 0)
        {
            return 0;
        }

        return ticks * 1_000_000d / Stopwatch.Frequency;
    }

    internal static long NormalizeMin(long value)
    {
        return value == long.MaxValue ? 0 : value;
    }

    internal static double ComputeAverage(long sum, long count)
    {
        return count <= 0 ? 0 : (double)sum / count;
    }
}
