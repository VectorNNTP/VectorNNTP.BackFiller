using System.Diagnostics;

namespace VectorNNTP.BackFiller.Benchmarks;

internal sealed class RuntimeMetrics
{
    private readonly object _gate = new();
    private double _cpuPercentSum;
    private double _hostCpuPercentSum;
    private double _transitServerCpuPercentSum;
    private long _cpuSampleCount;
    private long _lastWorkingSet;
    private long _lastGcHeap;
    private long _lastAllocated;
    private double _peakHostCpuPercent;
    private double _peakTransitServerCpuPercent;

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

internal static class RuntimeMetricSamplingHelpers
{
    private static readonly object HostCpuGate = new();
    private static DateTime _hostCpuLastSampleUtc;
    private static long _hostCpuLastTotalProcessTicks;

    private static readonly object TransitServerCpuGate = new();
    private static DateTime _transitServerCpuLastSampleUtc;
    private static long _transitServerCpuLastTotalTicks;

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
