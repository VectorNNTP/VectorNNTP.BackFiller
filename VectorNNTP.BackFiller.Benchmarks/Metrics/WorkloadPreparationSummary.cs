// <copyright file="WorkloadPreparationSummary.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Metrics/WorkloadPreparationSummary: captures, aggregates, or publishes benchmark throughput, latency, and runtime telemetry.

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Defines the workload PreparationSummary record struct for benchmark or isolated-regression execution.
/// </summary>
internal readonly record struct WorkloadPreparationSummary(
    double PreGenerationDurationMilliseconds,
    double PayloadPreparationDurationMilliseconds,
    int MessageIdPoolSize,
    int UniqueMessageIdCount,
    int DuplicateMessageIdCount,
    int ReusablePayloadBytes);
