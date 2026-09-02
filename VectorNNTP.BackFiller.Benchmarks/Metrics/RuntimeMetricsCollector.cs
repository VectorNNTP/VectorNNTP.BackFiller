// <copyright file="RuntimeMetricsCollector.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Metrics/RuntimeMetricsCollector: captures, aggregates, or publishes benchmark throughput, latency, and runtime telemetry.

using System.Diagnostics;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the runtime Metrics class used by the benchmark or regression gate.
/// </summary>
internal sealed class RuntimeMetrics
{
    /// <summary>
    /// Runs the _gate benchmark scenario.
    /// </summary>
    private readonly object _gate = new();
    /// <summary>
    /// Gets or sets the _cpuPercentSum.
    /// </summary>
    private double _cpuPercentSum;
    /// <summary>
    /// Gets or sets the _hostCpuPercentSum.
    /// </summary>
    private double _hostCpuPercentSum;
    /// <summary>
    /// Gets or sets the _transitServerCpuPercentSum.
    /// </summary>
    private double _transitServerCpuPercentSum;
    /// <summary>
    /// Gets or sets the _cpuSampleCount.
    /// </summary>
    private long _cpuSampleCount;
    /// <summary>
    /// Gets or sets the _lastWorkingSet.
    /// </summary>
    private long _lastWorkingSet;
    /// <summary>
    /// Gets or sets the _lastGcHeap.
    /// </summary>
    private long _lastGcHeap;
    /// <summary>
    /// Gets or sets the _lastAllocated.
    /// </summary>
    private long _lastAllocated;
    /// <summary>
    /// Gets or sets the _peakHostCpuPercent.
    /// </summary>
    private double _peakHostCpuPercent;
    /// <summary>
    /// Gets or sets the _peakTransitServerCpuPercent.
    /// </summary>
    private double _peakTransitServerCpuPercent;

    /// <summary>
    /// Runs the sample benchmark scenario.
    /// </summary>
    internal void Sample(double cpuPercent, double hostCpuPercent, double transitServerCpuPercent, long workingSet, long gcHeap, long allocated)
    {
        lock (_gate)
        {
            _cpuPercentSum += cpuPercent;
            _hostCpuPercentSum += hostCpuPercent;
            _transitServerCpuPercentSum += transitServerCpuPercent;
            _cpuSampleCount++;
            _lastWorkingSet = workingSet;
            _lastGcHeap = gcHeap;
            _lastAllocated = allocated;
            _peakHostCpuPercent = Math.Max(_peakHostCpuPercent, hostCpuPercent);
            _peakTransitServerCpuPercent = Math.Max(_peakTransitServerCpuPercent, transitServerCpuPercent);
        }
    }

    /// <summary>
    /// Runs the snapshot benchmark scenario.
    /// </summary>
    internal RuntimeSnapshot Snapshot()
    {
        lock (_gate)
        {
            double avgCpu = _cpuSampleCount == 0 ? 0 : _cpuPercentSum / _cpuSampleCount;
            double avgHostCpu = _cpuSampleCount == 0 ? 0 : _hostCpuPercentSum / _cpuSampleCount;
            double avgTransitCpu = _cpuSampleCount == 0 ? 0 : _transitServerCpuPercentSum / _cpuSampleCount;
            return new RuntimeSnapshot(avgCpu, avgHostCpu, avgTransitCpu, _peakHostCpuPercent, _peakTransitServerCpuPercent, _lastWorkingSet, _lastGcHeap, _lastAllocated);
        }
    }
}

/// <summary>
/// Represents the runtime MetricSamplingHelpers class used by the benchmark or regression gate.
/// </summary>
internal static class RuntimeMetricSamplingHelpers
{
    /// <summary>
    /// Implements the host CpuGate contract.
    /// </summary>
    private static readonly object HostCpuGate = new();
    /// <summary>
    /// Gets or sets the _hostCpuLastSampleUtc.
    /// </summary>
    private static DateTime _hostCpuLastSampleUtc;
    /// <summary>
    /// Gets or sets the _hostCpuLastTotalProcessTicks.
    /// </summary>
    private static long _hostCpuLastTotalProcessTicks;

    /// <summary>
    /// Implements the transit ServerCpuGate contract.
    /// </summary>
    private static readonly object TransitServerCpuGate = new();
    /// <summary>
    /// Gets or sets the _transitServerCpuLastSampleUtc.
    /// </summary>
    private static DateTime _transitServerCpuLastSampleUtc;
    /// <summary>
    /// Gets or sets the _transitServerCpuLastTotalTicks.
    /// </summary>
    private static long _transitServerCpuLastTotalTicks;

    /// <summary>
    /// Reads HostCpuPercent.

    /// </summary>
    internal static double ReadHostCpuPercent()
    {
        lock (HostCpuGate)
        {
            DateTime nowUtc = DateTime.UtcNow;
            long totalTicks = 0;

            Process[] processes = Process.GetProcesses();
            foreach (Process candidate in processes)
            {
                try
                {
                    totalTicks += candidate.TotalProcessorTime.Ticks;
                }
                catch
                {
                }
                finally
                {
                    candidate.Dispose();
                }
            }

            if (_hostCpuLastSampleUtc == default || _hostCpuLastTotalProcessTicks <= 0)
            {
                _hostCpuLastSampleUtc = nowUtc;
                _hostCpuLastTotalProcessTicks = totalTicks;
                return 0;
            }

            double elapsedTicks = Math.Max(1, (nowUtc - _hostCpuLastSampleUtc).Ticks);
            long deltaCpuTicks = Math.Max(0, totalTicks - _hostCpuLastTotalProcessTicks);

            _hostCpuLastSampleUtc = nowUtc;
            _hostCpuLastTotalProcessTicks = totalTicks;

            double percent = deltaCpuTicks * 100d / (elapsedTicks * Environment.ProcessorCount);
            return double.IsFinite(percent) ? Math.Clamp(percent, 0, 100d) : 0;
        }
    }

    /// <summary>
    /// Reads TransitServerCpuPercent.

    /// </summary>
    internal static double ReadTransitServerCpuPercent()
    {
        lock (TransitServerCpuGate)
        {
            DateTime nowUtc = DateTime.UtcNow;
            long totalTicks = 0;

            foreach (Process process in Process.GetProcessesByName("Vector.NNTP.NNTPD"))
            {
                try
                {
                    totalTicks += process.TotalProcessorTime.Ticks;
                }
                catch
                {
                }
                finally
                {
                    process.Dispose();
                }
            }

            if (_transitServerCpuLastSampleUtc == default || _transitServerCpuLastTotalTicks <= 0)
            {
                _transitServerCpuLastSampleUtc = nowUtc;
                _transitServerCpuLastTotalTicks = totalTicks;
                return 0;
            }

            double elapsedTicks = Math.Max(1, (nowUtc - _transitServerCpuLastSampleUtc).Ticks);
            long deltaCpuTicks = Math.Max(0, totalTicks - _transitServerCpuLastTotalTicks);

            _transitServerCpuLastSampleUtc = nowUtc;
            _transitServerCpuLastTotalTicks = totalTicks;

            double percent = deltaCpuTicks * 100d / elapsedTicks;
            return double.IsFinite(percent) ? Math.Max(0, percent) : 0;
        }
    }
}
