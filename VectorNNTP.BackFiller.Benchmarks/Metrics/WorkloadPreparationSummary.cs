// <copyright file="WorkloadPreparationSummary.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Metrics/WorkloadPreparationSummary: captures, aggregates, or publishes benchmark throughput, latency, and runtime telemetry.

namespace VectorNNTP.BackFiller.Benchmarks
{

    /// <summary>
    /// Represents the workload PreparationSummary record struct used by the benchmark or regression gate.
    /// </summary>
    internal readonly record struct WorkloadPreparationSummary(
        double PreGenerationDurationMilliseconds,
        double PayloadPreparationDurationMilliseconds,
        int MessageIdPoolSize,
        int UniqueMessageIdCount,
        int DuplicateMessageIdCount,
        int ReusablePayloadBytes);
}
