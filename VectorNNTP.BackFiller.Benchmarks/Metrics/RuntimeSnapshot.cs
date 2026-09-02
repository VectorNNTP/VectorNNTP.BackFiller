// <copyright file="RuntimeSnapshot.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Metrics/RuntimeSnapshot: captures, aggregates, or publishes benchmark throughput, latency, and runtime telemetry.

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Defines the runtime Snapshot record struct for benchmark or isolated-regression execution.
/// </summary>
internal readonly record struct RuntimeSnapshot(
    double AverageCpuPercent,
    double AverageHostCpuPercent,
    double AverageTransitServerCpuPercent,
    double PeakHostCpuPercent,
    double PeakTransitServerCpuPercent,
    long LastWorkingSetBytes,
    long LastGcHeapBytes,
    long LastAllocatedBytes);
