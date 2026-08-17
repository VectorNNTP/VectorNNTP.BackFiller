namespace VectorNNTP.BackFiller.Benchmarks;

internal readonly record struct RuntimeSnapshot(
    double AverageCpuPercent,
    double AverageHostCpuPercent,
    double AverageTransitServerCpuPercent,
    double PeakHostCpuPercent,
    double PeakTransitServerCpuPercent,
    long LastWorkingSetBytes,
    long LastGcHeapBytes,
    long LastAllocatedBytes);
