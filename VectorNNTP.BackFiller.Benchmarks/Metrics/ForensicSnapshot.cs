// <copyright file="ForensicSnapshot.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Metrics/ForensicSnapshot: captures, aggregates, or publishes benchmark throughput, latency, and runtime telemetry.

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the forensic Snapshot record struct used by this benchmark or regression-gate component.
/// </summary>
internal readonly record struct ForensicSnapshot(
    double AverageDispatchQueueWaitUs,
    double P50DispatchQueueWaitUs,
    double P95DispatchQueueWaitUs,
    double P99DispatchQueueWaitUs,
    double MaxDispatchQueueWaitUs,
    long DispatchQueueWaitSampleCount,
    double AverageSocketWriteUs,
    double P50SocketWriteUs,
    double P95SocketWriteUs,
    double P99SocketWriteUs,
    double MaxSocketWriteUs,
    long SocketWriteSampleCount,
    double AverageResponseWaitUs,
    double P50ResponseWaitUs,
    double P95ResponseWaitUs,
    double P99ResponseWaitUs,
    double MaxResponseWaitUs,
    long ResponseWaitSampleCount,
    double AverageParseCorrelationUs,
    double P50ParseCorrelationUs,
    double P95ParseCorrelationUs,
    double P99ParseCorrelationUs,
    double MaxParseCorrelationUs,
    long ParseCorrelationSampleCount,
    double AverageTotalPublishLatencyUs,
    double P50TotalPublishLatencyUs,
    double P95TotalPublishLatencyUs,
    double P99TotalPublishLatencyUs,
    double MaxTotalPublishLatencyUs,
    long TotalPublishLatencySampleCount,
    double AveragePublishLatencyUs,
    double MinPublishLatencyUs,
    double P50PublishLatencyUs,
    double P95PublishLatencyUs,
    double P99PublishLatencyUs,
    double MaxPublishLatencyUs,
    double AverageLifecycleLatencyUs,
    string PendingDepthLatencyBuckets,
    int ForensicSampleCount,
    string ConnectionTimeSeriesSummary,
    string DispatcherTimeSeriesSummary,
    string ObservabilityNotes)
{
    /// <summary>
    /// Executes the empty operation while preserving the component's benchmark or test-harness contract.
    /// </summary>
    internal static ForensicSnapshot Empty => new(
        AverageDispatchQueueWaitUs: 0,
        P50DispatchQueueWaitUs: 0,
        P95DispatchQueueWaitUs: 0,
        P99DispatchQueueWaitUs: 0,
        MaxDispatchQueueWaitUs: 0,
        DispatchQueueWaitSampleCount: 0,
        AverageSocketWriteUs: 0,
        P50SocketWriteUs: 0,
        P95SocketWriteUs: 0,
        P99SocketWriteUs: 0,
        MaxSocketWriteUs: 0,
        SocketWriteSampleCount: 0,
        AverageResponseWaitUs: 0,
        P50ResponseWaitUs: 0,
        P95ResponseWaitUs: 0,
        P99ResponseWaitUs: 0,
        MaxResponseWaitUs: 0,
        ResponseWaitSampleCount: 0,
        AverageParseCorrelationUs: 0,
        P50ParseCorrelationUs: 0,
        P95ParseCorrelationUs: 0,
        P99ParseCorrelationUs: 0,
        MaxParseCorrelationUs: 0,
        ParseCorrelationSampleCount: 0,
        AverageTotalPublishLatencyUs: 0,
        P50TotalPublishLatencyUs: 0,
        P95TotalPublishLatencyUs: 0,
        P99TotalPublishLatencyUs: 0,
        MaxTotalPublishLatencyUs: 0,
        TotalPublishLatencySampleCount: 0,
        AveragePublishLatencyUs: 0,
        MinPublishLatencyUs: 0,
        P50PublishLatencyUs: 0,
        P95PublishLatencyUs: 0,
        P99PublishLatencyUs: 0,
        MaxPublishLatencyUs: 0,
        AverageLifecycleLatencyUs: 0,
        PendingDepthLatencyBuckets: "(forensic diagnostics disabled)",
        ForensicSampleCount: 0,
        ConnectionTimeSeriesSummary: "(forensic diagnostics disabled)",
        DispatcherTimeSeriesSummary: "(forensic diagnostics disabled)",
        ObservabilityNotes: "Forensic diagnostics disabled for this run.");
}
