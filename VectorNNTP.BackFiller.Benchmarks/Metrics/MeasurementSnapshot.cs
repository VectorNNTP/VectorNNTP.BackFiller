// <copyright file="MeasurementSnapshot.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Metrics/MeasurementSnapshot: captures, aggregates, or publishes benchmark throughput, latency, and runtime telemetry.

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the measurement Snapshot record struct used by the benchmark or regression gate.
/// </summary>
internal readonly record struct MeasurementSnapshot(
    long GeneratedCount,
    long GeneratedBytes,
    long AdmittedCount,
    long AdmittedBytes,
    long AcceptedCount,
    long AcceptedBytes,
    long RejectedCount,
    long AmbiguousCount,
    long CompletedCount,
    long BlockedTicks,
    long GenerationTicks,
    long OtherActiveTicks,
    long ActiveTicks,
    long LoopTicks,
    long PeakQueueDepth,
    long PeakQueueBytes,
    long PeakInFlight,
    long PeakActualPending,
    long MinQueueDepth,
    long MinQueueBytes,
    long QueueDepthSampleCount,
    double AverageQueueDepth,
    double AverageQueueBytes,
    long ProducerQueueWaitTicks,
    int ArticleBytes);
