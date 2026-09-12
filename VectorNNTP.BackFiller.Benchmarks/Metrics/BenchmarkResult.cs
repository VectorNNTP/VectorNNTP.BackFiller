// <copyright file="BenchmarkResult.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// Metrics/BenchmarkResult: captures, aggregates, or publishes benchmark throughput, latency, and runtime telemetry.

using System.Diagnostics;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.BackFiller.Benchmarks;

/// <summary>
/// Represents the boundary ConnectionSnapshot record struct used by the benchmark or regression gate.
/// </summary>
internal readonly record struct BoundaryConnectionSnapshot(
    int SlotIndex,
    string ConnectionId,
    string State,
    int CurrentConcurrentSubmissions,
    int OutstandingOperations,
    long CurrentWriteIntentQueueDepth,
    long SubmissionsStarted,
    long SubmissionsAccepted,
    long SubmissionsRejected,
    long SubmissionsAmbiguous,
    long SubmissionsUnavailable,
    long SubmissionsFailed);

/// <summary>
/// Represents the fixed CountBoundarySnapshot record struct used by the benchmark or regression gate.
/// </summary>
internal readonly record struct FixedCountBoundarySnapshot(
    string Phase,
    DateTimeOffset TimestampUtc,
    long StopwatchTick,
    long TotalSubmissionsStarted,
    long TotalSubmissionsAccepted,
    long TotalSubmissionsRejected,
    long TotalSubmissionsAmbiguous,
    long TotalSubmissionsFailed,
    long TotalSubmissionsUnavailable,
    long TotalSubmissionsCanceled,
    long CurrentOutstandingSubmissions,
    long QueuedSubmissionCount,
    long PendingOperationsCount,
    long QueuedWriteIntentsCount,
    int CurrentConnectionCount,
    int ActiveConnectionCount,
    int ReadyConnectionCount,
    BoundaryConnectionSnapshot[] Connections);

/// <summary>
/// Represents the post MeasurementTerminalizationReasons record struct used by the benchmark or regression gate.
/// </summary>
internal readonly record struct PostMeasurementTerminalizationReasons(
    long Response400,
    long ResponseLoopFailure,
    long ConnectionClose,
    long QueuedWriteDrain,
    long Shutdown,
    long Preemption,
    long Cancellation,
    long Timeout,
    long Unavailable,
    long Failed,
    long OtherOrUnknown);

/// <summary>
/// Represents the post MeasurementTerminalizationSummary record struct used by the benchmark or regression gate.
/// </summary>
internal readonly record struct PostMeasurementTerminalizationSummary(
    long TerminalizedBeforeMeasurementEnd,
    long TerminalizedAfterMeasurementEnd,
    long PostMeasurementAccepted,
    long PostMeasurementRejected,
    long PostMeasurementAmbiguous,
    long PostMeasurementFailed,
    long PostMeasurementUnavailable,
    long PostMeasurementCanceled,
    DateTimeOffset? FirstPostMeasurementTerminalizationUtc,
    DateTimeOffset? LastPostMeasurementTerminalizationUtc,
    PostMeasurementTerminalizationReasons Reasons);

/// <summary>
/// Represents the fixed CountBoundaryTelemetry record struct used by the benchmark or regression gate.
/// </summary>
internal readonly record struct FixedCountBoundaryTelemetry(
    FixedCountBoundarySnapshot AtMeasurementEnd,
    FixedCountBoundarySnapshot PostMeasurementPreDrain,
    FixedCountBoundarySnapshot PostDrainFinal,
    PostMeasurementTerminalizationSummary PostMeasurementTerminalization);

/// <summary>
/// Represents the ambiguity ProvenanceCategorySummary record struct used by the benchmark or regression gate.
/// </summary>
internal readonly record struct AmbiguityProvenanceCategorySummary(
    TransitPublishProvenance Category,
    long Count,
    long BeforeMeasurementEndCount,
    long AfterMeasurementEndCount,
    double? FirstOccurrenceMsFromMeasurementStart,
    double? LastOccurrenceMsFromMeasurementStart);

/// <summary>
/// Represents the provenance ConnectionCategorySummary record struct used by the benchmark or regression gate.
/// </summary>
internal readonly record struct ProvenanceConnectionCategorySummary(
    TransitPublishProvenance Category,
    long Count,
    long BeforeMeasurementEndCount,
    long AfterMeasurementEndCount);

/// <summary>
/// Represents the provenance ConnectionSummary record struct used by the benchmark or regression gate.
/// </summary>
internal readonly record struct ProvenanceConnectionSummary(
    string ConnectionId,
    int? SlotIndex,
    long AmbiguousCount,
    string[] StatesObserved,
    ProvenanceConnectionCategorySummary[] Categories);

/// <summary>
/// Represents the ambiguity ProvenanceSummary record struct used by the benchmark or regression gate.
/// </summary>
internal readonly record struct AmbiguityProvenanceSummary(
    AmbiguityProvenanceCategorySummary[] Categories,
    ProvenanceConnectionSummary[] Connections);

/// <summary>
/// Represents the submission PumpInitiatingFaultSummary record struct used by the benchmark or regression gate.
/// </summary>
internal readonly record struct SubmissionPumpInitiatingFaultSummary(
    long FaultSequence,
    int SlotIndex,
    long CapturedAtTick,
    string ExceptionType,
    string BaseExceptionType,
    int HResult,
    string InvalidOperationMessageClass,
    string SanitizedFirstFaultMessageClass,
    string SanitizedFirstFaultMessage,
    string? FullFirstFaultStackTrace,
    string? TopStackFrameDeclaringType,
    string? TopStackFrameMethodName,
    string Origin,
    double? MillisecondsFromMeasurementStart,
    bool MeasurementBoundaryObserved,
    double? MillisecondsFromMeasurementEnd,
    string MeasurementStateAtFault,
    long QueuedSubmissionCount,
    int InFlightCount,
    long ActiveSubmissionCount,
    int? ChannelImmediateAvailableCount,
    int ActiveConnectionCount,
    int ReadyConnectionCount,
    int FaultedConnectionCount,
    int ReconnectingConnectionCount,
    long OutstandingConnectionOperations,
    string ProducerCompletionState,
    string DispatchersCompletedState);

/// <summary>
/// Represents the submission PumpFaultSummary record struct used by the benchmark or regression gate.
/// </summary>
internal readonly record struct SubmissionPumpFaultSummary(
    long TotalFaultCount,
    long InitiatingFaultCount,
    long CascadeFaultCount,
    SubmissionPumpInitiatingFaultSummary? InitiatingFault);

/// <summary>
/// Represents the P1 GreetingLifecycleEventSummary record struct used by the benchmark or regression gate.
/// </summary>
internal readonly record struct P1GreetingLifecycleEventSummary(
    string Event,
    long Tick,
    int InitializationAttemptId);

/// <summary>
/// Represents the P1 GreetingProvenanceSummary record struct used by the benchmark or regression gate.
/// </summary>
internal readonly record struct P1GreetingProvenanceSummary(
    string ConnectionId,
    string Host,
    int Port,
    int InitializationAttemptId,
    string? LocalIp,
    int LocalPort,
    string? RemoteIp,
    int RemotePort,
    long CapturedAtTick,
    long? ConnectedAtTick,
    long? PipesCreatedAtTick,
    long? AwaitingGreetingAtTick,
    DateTimeOffset? ConnectedAtUtc,
    DateTimeOffset? P1AtUtc,
    bool LocalDisposeAsyncBeforeP1,
    bool LocalResetTransportStateBeforeP1,
    bool LocalDisposeTransportArtifactsBeforeP1,
    bool LocalRebuildPipesBeforeP1,
    bool LocalCleanupFailedInitializationBeforeP1,
    bool InitializationCancellationBeforeP1,
    P1GreetingLifecycleEventSummary[] LifecycleEvents);

/// <summary>
/// Represents the benchmark measurement and drain boundary captured with both UTC and monotonic stopwatch ticks.
/// </summary>
internal readonly record struct MeasurementBoundary(
    DateTimeOffset MeasurementStartUtc,
    long MeasurementStartStopwatchTick,
    DateTimeOffset MeasurementEndUtc,
    long MeasurementEndStopwatchTick,
    DateTimeOffset DrainStartUtc,
    long DrainStartStopwatchTick,
    DateTimeOffset DrainCompletionUtc,
    long DrainCompletionStopwatchTick)
{
    /// <summary>
    /// Gets the monotonic measurement-window duration.
    /// </summary>
    internal TimeSpan MeasurementWindowDuration => StopwatchTicksToTimeSpan(MeasurementEndStopwatchTick - MeasurementStartStopwatchTick);

    /// <summary>
    /// Gets the monotonic post-measurement drain duration.
    /// </summary>
    internal TimeSpan DrainDuration => StopwatchTicksToTimeSpan(DrainCompletionStopwatchTick - DrainStartStopwatchTick);

    /// <summary>
    /// Gets the monotonic end-to-end duration from measurement start through drain completion.
    /// </summary>
    internal TimeSpan EndToEndDuration => StopwatchTicksToTimeSpan(DrainCompletionStopwatchTick - MeasurementStartStopwatchTick);

    private static TimeSpan StopwatchTicksToTimeSpan(long ticks)
    {
        double seconds = Math.Max(0d, ticks) / Stopwatch.Frequency;
        return TimeSpan.FromSeconds(seconds);
    }
}

/// <summary>
/// Represents the benchmark Result record struct used by the benchmark or regression gate.
/// </summary>
/// <remarks>
/// Throughput compatibility contract:
/// <list type="bullet">
/// <item><description><c>OfferedGbps</c> is a compatibility alias of <c>OfferedWithinWindowGbps</c> and they are expected to remain equal by construction.</description></item>
/// <item><description><c>AdmittedGbps</c> is a compatibility alias of <c>AdmittedWithinWindowGbps</c> and they are expected to remain equal by construction.</description></item>
/// <item><description><c>AcceptedGbps</c> is a compatibility alias of <c>AcceptedWithinWindowGbps</c> and they are expected to remain equal by construction.</description></item>
/// <item><description>These compatibility aliases describe measurement-window throughput only and do not include post-boundary drain terminalizations.</description></item>
/// <item><description><c>DrainInclusiveAcceptedGbps</c> is the distinct drain-inclusive accepted-throughput metric and uses end-to-end duration semantics.</description></item>
/// </list>
/// </remarks>
internal readonly record struct BenchmarkResult(
    string BenchmarkBuildVersion,
    RuntimeExecutionIdentity RuntimeIdentity,
    WorkloadPreparationSummary WorkloadPreparation,
    MeasurementBoundary Boundary,
    TimeSpan DrainDuration,
    long OutstandingAtMeasurementEnd,
    long DrainedAfterMeasurement,
    FixedCountBoundaryTelemetry? FixedCountBoundaryTelemetry,
    AmbiguityProvenanceSummary AmbiguityProvenance,
    SubmissionPumpFaultSummary SubmissionPumpFault,
    P1GreetingProvenanceSummary? P1GreetingProvenance,
    long OfferedArticles,
    long OfferedBytes,
    double OfferedGbps,
    long OfferedWithinWindowArticles,
    long OfferedWithinWindowBytes,
    double OfferedWithinWindowGbps,
    long AdmittedArticles,
    long AdmittedBytes,
    double AdmittedGbps,
    long AdmittedWithinWindowArticles,
    long AdmittedWithinWindowBytes,
    double AdmittedWithinWindowGbps,
    long SubmittedArticles,
    long SubmittedBytes,
    long SubmittedWithinWindowArticles,
    long SubmittedWithinWindowBytes,
    long AcceptedArticles,
    long AcceptedBytes,
    double AcceptedGbps,
    long AcceptedWithinWindowArticles,
    long AcceptedWithinWindowBytes,
    double AcceptedWithinWindowGbps,
    long AcceptedPostMeasurementArticles,
    long AcceptedPostMeasurementBytes,
    double DrainInclusiveAcceptedGbps,
    long RejectedArticles,
    long RejectedWithinWindowArticles,
    long AmbiguousArticles,
    long AmbiguousWithinWindowArticles,
    long FailedArticles,
    long FailedWithinWindowArticles,
    long UnavailableArticles,
    long UnavailableWithinWindowArticles,
    long CanceledArticles,
    long CanceledWithinWindowArticles,
    long CompletedArticles,
    long CompletedWithinWindowArticles,
    long CompletedPostMeasurementArticles,
    long MinQueueDepth,
    long QueueDepthSampleCount,
    double AverageQueueDepth,
    double AverageQueuedBytes,
    long PeakQueueDepth,
    long PeakQueuedBytes,
    long PeakInFlight,
    long PeakActualPending,
    double ProducerActivePercent,
    double ProducerBlockedPercent,
    double ProducerActiveMilliseconds,
    double ProducerBlockedMilliseconds,
    double ProducerQueueWaitMilliseconds,
    double AverageCpuPercent,
    double AverageHostCpuPercent,
    double AverageTransitServerCpuPercent,
    double PeakHostCpuPercent,
    double PeakTransitServerCpuPercent,
    double WorkingSetMb,
    double GcHeapMb,
    double AllocatedMb,
    int Gen0Collections,
    int Gen1Collections,
    int Gen2Collections,
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
    string ObservabilityNotes,
    long EffectiveQueueArticleCapacityFromBytes);
