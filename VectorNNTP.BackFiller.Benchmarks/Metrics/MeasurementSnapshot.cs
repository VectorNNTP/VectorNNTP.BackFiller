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
    long OfferedCount,
    long OfferedBytes,
    long OfferedWithinWindowCount,
    long OfferedWithinWindowBytes,
    long AdmittedCount,
    long AdmittedBytes,
    long AdmittedWithinWindowCount,
    long AdmittedWithinWindowBytes,
    long SubmittedCount,
    long SubmittedBytes,
    long SubmittedWithinWindowCount,
    long SubmittedWithinWindowBytes,
    long AcceptedCount,
    long AcceptedBytes,
    long AcceptedWithinWindowCount,
    long AcceptedWithinWindowBytes,
    long RejectedCount,
    long RejectedWithinWindowCount,
    long AmbiguousCount,
    long AmbiguousWithinWindowCount,
    long FailedCount,
    long FailedWithinWindowCount,
    long UnavailableCount,
    long UnavailableWithinWindowCount,
    long CanceledCount,
    long CanceledWithinWindowCount,
    long CompletedCount,
    long CompletedWithinWindowCount,
    long CompletedPostMeasurementCount,
    long AcceptedPostMeasurementCount,
    long AcceptedPostMeasurementBytes,
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
