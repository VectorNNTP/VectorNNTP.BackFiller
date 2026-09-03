// <copyright file="TransitTimingCollector.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Transit
// Implements the transit timing collector behavior.

using System.Diagnostics;

namespace VectorNNTP.Backfiller.Runtime.Transit
{
    /// <summary>
    /// Collects lock-free aggregate timing counters for key publish, staging, flush, correlation, and worker-observation phases.
    /// </summary>
    /// <remarks>
    /// The collector records counts, totals, and selected min/max values without owning any pipeline behavior. All values
    /// are captured as raw <see cref="Stopwatch"/> ticks so callers can convert them using the snapshot frequency.
    /// </remarks>
    internal sealed class TransitTimingCollector
    {
        /// <summary>
        /// Number of publish payload-copy measurements recorded.
        /// </summary>
        private long _publishPayloadCopyCount;

        /// <summary>
        /// Total ticks spent copying publish payloads.
        /// </summary>
        private long _publishPayloadCopyTotalTicks;

        /// <summary>
        /// Number of dot-stuff staging measurements recorded.
        /// </summary>
        private long _dotStuffStageCount;

        /// <summary>
        /// Total ticks spent dot-stuffing and staging payloads.
        /// </summary>
        private long _dotStuffStageTotalTicks;

        /// <summary>
        /// Minimum ticks observed for dot-stuff staging.
        /// </summary>
        private long _dotStuffStageMinTicks = long.MaxValue;

        /// <summary>
        /// Maximum ticks observed for dot-stuff staging.
        /// </summary>
        private long _dotStuffStageMaxTicks;

        /// <summary>
        /// Total payload bytes processed by dot-stuff staging.
        /// </summary>
        private long _dotStuffPayloadBytes;

        /// <summary>
        /// Total <c>PipeWriter.GetSpan</c> calls attributed to dot-stuff staging.
        /// </summary>
        private long _dotStuffGetSpanCalls;

        /// <summary>
        /// Total <c>PipeWriter.Advance</c> calls attributed to dot-stuff staging.
        /// </summary>
        private long _dotStuffAdvanceCalls;

        /// <summary>
        /// Total line-leading dot escapes inserted during staging.
        /// </summary>
        private long _dotStuffStuffedDotEvents;

        /// <summary>
        /// Number of flush-wait measurements recorded.
        /// </summary>
        private long _flushCount;

        /// <summary>
        /// Total ticks spent waiting for writer flush completion.
        /// </summary>
        private long _flushTotalTicks;

        /// <summary>
        /// Minimum ticks observed for flush waits.
        /// </summary>
        private long _flushMinTicks = long.MaxValue;

        /// <summary>
        /// Maximum ticks observed for flush waits.
        /// </summary>
        private long _flushMaxTicks;

        /// <summary>
        /// Number of response-line read measurements recorded.
        /// </summary>
        private long _responseLineReadCount;

        /// <summary>
        /// Total ticks spent reading protocol response lines.
        /// </summary>
        private long _responseLineReadTotalTicks;

        /// <summary>
        /// Minimum ticks observed for response-line reads.
        /// </summary>
        private long _responseLineReadMinTicks = long.MaxValue;

        /// <summary>
        /// Maximum ticks observed for response-line reads.
        /// </summary>
        private long _responseLineReadMaxTicks;

        /// <summary>
        /// Number of response-correlation measurements recorded.
        /// </summary>
        private long _responseCorrelationCount;

        /// <summary>
        /// Total ticks spent mapping response lines to pending work.
        /// </summary>
        private long _responseCorrelationTotalTicks;

        /// <summary>
        /// Minimum ticks observed for response correlation.
        /// </summary>
        private long _responseCorrelationMinTicks = long.MaxValue;

        /// <summary>
        /// Maximum ticks observed for response correlation.
        /// </summary>
        private long _responseCorrelationMaxTicks;

        /// <summary>
        /// Number of response-available-to-correlated measurements recorded.
        /// </summary>
        private long _responseAvailableToCorrelatedCount;

        /// <summary>
        /// Total ticks between response availability and successful correlation.
        /// </summary>
        private long _responseAvailableToCorrelatedTotalTicks;

        /// <summary>
        /// Minimum ticks observed between response availability and correlation.
        /// </summary>
        private long _responseAvailableToCorrelatedMinTicks = long.MaxValue;

        /// <summary>
        /// Maximum ticks observed between response availability and correlation.
        /// </summary>
        private long _responseAvailableToCorrelatedMaxTicks;

        /// <summary>
        /// Number of completion-enqueue-to-worker-observation measurements recorded.
        /// </summary>
        private long _completionEnqueueToObserveCount;

        /// <summary>
        /// Total ticks between completion enqueue and worker observation.
        /// </summary>
        private long _completionEnqueueToObserveTotalTicks;

        /// <summary>
        /// Minimum ticks observed between completion enqueue and worker observation.
        /// </summary>
        private long _completionEnqueueToObserveMinTicks = long.MaxValue;

        /// <summary>
        /// Maximum ticks observed between completion enqueue and worker observation.
        /// </summary>
        private long _completionEnqueueToObserveMaxTicks;

        /// <summary>
        /// Number of worker-poll-delay measurements recorded.
        /// </summary>
        private long _workerPollDelayCount;

        /// <summary>
        /// Total ticks spent waiting in worker poll loops.
        /// </summary>
        private long _workerPollDelayTotalTicks;

        /// <summary>
        /// Number of response-to-worker-observation interval measurements recorded.
        /// </summary>
        private long _responseToWorkerObservationCount;

        /// <summary>
        /// Total ticks between the last definitive response correlation and worker observation.
        /// </summary>
        private long _responseToWorkerObservationTotalTicks;

        /// <summary>
        /// Minimum ticks observed between the last definitive response correlation and worker observation.
        /// </summary>
        private long _responseToWorkerObservationMinTicks = long.MaxValue;

        /// <summary>
        /// Maximum ticks observed between the last definitive response correlation and worker observation.
        /// </summary>
        private long _responseToWorkerObservationMaxTicks;

        /// <summary>
        /// Number of worker-observation-to-next-staging interval measurements recorded.
        /// </summary>
        private long _workerObservationToNextStagingCount;

        /// <summary>
        /// Total ticks between worker observation of one completion and the next staging start.
        /// </summary>
        private long _workerObservationToNextStagingTotalTicks;

        /// <summary>
        /// Minimum ticks observed between worker observation and next staging.
        /// </summary>
        private long _workerObservationToNextStagingMinTicks = long.MaxValue;

        /// <summary>
        /// Maximum ticks observed between worker observation and next staging.
        /// </summary>
        private long _workerObservationToNextStagingMaxTicks;

        /// <summary>
        /// Number of response-to-next-staging interval measurements recorded.
        /// </summary>
        private long _responseToNextStagingCount;

        /// <summary>
        /// Total ticks between the last definitive response correlation and the next staging start.
        /// </summary>
        private long _responseToNextStagingTotalTicks;

        /// <summary>
        /// Minimum ticks observed between the last definitive response correlation and the next staging start.
        /// </summary>
        private long _responseToNextStagingMinTicks = long.MaxValue;

        /// <summary>
        /// Maximum ticks observed between the last definitive response correlation and the next staging start.
        /// </summary>
        private long _responseToNextStagingMaxTicks;

        /// <summary>
        /// Tick of the last definitive response correlation.
        /// </summary>
        private long _lastDefinitiveResponseCorrelatedTick;

        /// <summary>
        /// Tick when a worker most recently observed a completion.
        /// </summary>
        private long _lastWorkerObservationTick;

        /// <summary>
        /// Records one publish payload-copy measurement.
        /// </summary>
        /// <param name="elapsedTicks">Elapsed stopwatch ticks spent copying the payload.</param>
        internal void RecordPublishPayloadCopy(long elapsedTicks)
        {
            _ = Interlocked.Increment(ref _publishPayloadCopyCount);
            _ = Interlocked.Add(ref _publishPayloadCopyTotalTicks, elapsedTicks);
        }

        /// <summary>
        /// Records one dot-stuff staging measurement and its related write-shape counters.
        /// </summary>
        /// <param name="elapsedTicks">Elapsed stopwatch ticks spent staging the transformed payload.</param>
        /// <param name="payloadBytes">Payload-byte count processed by the stage.</param>
        /// <param name="getSpanCalls">Number of <c>GetSpan</c> calls attributed to the stage.</param>
        /// <param name="advanceCalls">Number of <c>Advance</c> calls attributed to the stage.</param>
        /// <param name="stuffedDotEvents">Number of line-leading dots duplicated by the transform.</param>
        internal void RecordDotStuffStage(long elapsedTicks, long payloadBytes, long getSpanCalls, long advanceCalls, long stuffedDotEvents)
        {
            _ = Interlocked.Increment(ref _dotStuffStageCount);
            _ = Interlocked.Add(ref _dotStuffStageTotalTicks, elapsedTicks);
            _ = Interlocked.Add(ref _dotStuffPayloadBytes, payloadBytes);
            _ = Interlocked.Add(ref _dotStuffGetSpanCalls, getSpanCalls);
            _ = Interlocked.Add(ref _dotStuffAdvanceCalls, advanceCalls);
            _ = Interlocked.Add(ref _dotStuffStuffedDotEvents, stuffedDotEvents);

            UpdateMin(ref _dotStuffStageMinTicks, elapsedTicks);
            UpdateMax(ref _dotStuffStageMaxTicks, elapsedTicks);
        }

        /// <summary>
        /// Records one writer flush wait measurement.
        /// </summary>
        /// <param name="elapsedTicks">Elapsed stopwatch ticks spent awaiting flush completion.</param>
        internal void RecordFlushWait(long elapsedTicks)
        {
            _ = Interlocked.Increment(ref _flushCount);
            _ = Interlocked.Add(ref _flushTotalTicks, elapsedTicks);
            UpdateMin(ref _flushMinTicks, elapsedTicks);
            UpdateMax(ref _flushMaxTicks, elapsedTicks);
        }

        /// <summary>
        /// Records one response-line read measurement.
        /// </summary>
        /// <param name="elapsedTicks">Elapsed stopwatch ticks spent reading a response line.</param>
        internal void RecordResponseLineRead(long elapsedTicks)
        {
            _ = Interlocked.Increment(ref _responseLineReadCount);
            _ = Interlocked.Add(ref _responseLineReadTotalTicks, elapsedTicks);
            UpdateMin(ref _responseLineReadMinTicks, elapsedTicks);
            UpdateMax(ref _responseLineReadMaxTicks, elapsedTicks);
        }

        /// <summary>
        /// Records one response-correlation measurement and, when applicable, definitive-response progress.
        /// </summary>
        /// <param name="elapsedTicks">Elapsed stopwatch ticks spent correlating the response.</param>
        /// <param name="responseAvailableTick">Tick captured when response bytes became available.</param>
        /// <param name="correlatedTick">Tick captured after the response was correlated.</param>
        /// <param name="definitive"><see langword="true"/> when the response represents definitive server progress.</param>
        internal void RecordResponseCorrelation(long elapsedTicks, long responseAvailableTick, long correlatedTick, bool definitive)
        {
            _ = Interlocked.Increment(ref _responseCorrelationCount);
            _ = Interlocked.Add(ref _responseCorrelationTotalTicks, elapsedTicks);
            UpdateMin(ref _responseCorrelationMinTicks, elapsedTicks);
            UpdateMax(ref _responseCorrelationMaxTicks, elapsedTicks);

            long responseAvailableToCorrelatedTicks = correlatedTick - responseAvailableTick;
            if (responseAvailableToCorrelatedTicks >= 0)
            {
                _ = Interlocked.Increment(ref _responseAvailableToCorrelatedCount);
                _ = Interlocked.Add(ref _responseAvailableToCorrelatedTotalTicks, responseAvailableToCorrelatedTicks);
                UpdateMin(ref _responseAvailableToCorrelatedMinTicks, responseAvailableToCorrelatedTicks);
                UpdateMax(ref _responseAvailableToCorrelatedMaxTicks, responseAvailableToCorrelatedTicks);
            }

            if (definitive)
            {
                _ = Interlocked.Exchange(ref _lastDefinitiveResponseCorrelatedTick, correlatedTick);
            }
        }

        /// <summary>
        /// Records that a worker observed a completion and derives downstream handoff timing from prior definitive progress.
        /// </summary>
        /// <param name="completionEnqueuedTick">Tick captured when the completion was enqueued.</param>
        /// <param name="workerObservedTick">Tick captured when a worker consumed the completion.</param>
        internal void RecordCompletionObserved(long completionEnqueuedTick, long workerObservedTick)
        {
            long enqueueToObserveTicks = workerObservedTick - completionEnqueuedTick;
            if (enqueueToObserveTicks >= 0)
            {
                _ = Interlocked.Increment(ref _completionEnqueueToObserveCount);
                _ = Interlocked.Add(ref _completionEnqueueToObserveTotalTicks, enqueueToObserveTicks);
                UpdateMin(ref _completionEnqueueToObserveMinTicks, enqueueToObserveTicks);
                UpdateMax(ref _completionEnqueueToObserveMaxTicks, enqueueToObserveTicks);
            }

            long lastResponseTick = Volatile.Read(ref _lastDefinitiveResponseCorrelatedTick);
            if (lastResponseTick > 0)
            {
                long responseToObservationTicks = workerObservedTick - lastResponseTick;
                if (responseToObservationTicks >= 0)
                {
                    _ = Interlocked.Increment(ref _responseToWorkerObservationCount);
                    _ = Interlocked.Add(ref _responseToWorkerObservationTotalTicks, responseToObservationTicks);
                    UpdateMin(ref _responseToWorkerObservationMinTicks, responseToObservationTicks);
                    UpdateMax(ref _responseToWorkerObservationMaxTicks, responseToObservationTicks);
                }
            }

            _ = Interlocked.Exchange(ref _lastWorkerObservationTick, workerObservedTick);
        }

        /// <summary>
        /// Records a worker-poll delay interval.
        /// </summary>
        /// <param name="elapsedTicks">Elapsed stopwatch ticks spent in a worker poll wait.</param>
        internal void RecordWorkerPollDelay(long elapsedTicks)
        {
            _ = Interlocked.Increment(ref _workerPollDelayCount);
            _ = Interlocked.Add(ref _workerPollDelayTotalTicks, elapsedTicks);
        }

        /// <summary>
        /// Records a new staging start and derives intervals from the prior worker observation and definitive response.
        /// </summary>
        /// <param name="stageStartTick">Tick captured when staging of the next batch began.</param>
        internal void RecordStagingStarted(long stageStartTick)
        {
            long lastObservationTick = Volatile.Read(ref _lastWorkerObservationTick);
            if (lastObservationTick > 0)
            {
                long workerObservationToStagingTicks = stageStartTick - lastObservationTick;
                if (workerObservationToStagingTicks >= 0)
                {
                    _ = Interlocked.Increment(ref _workerObservationToNextStagingCount);
                    _ = Interlocked.Add(ref _workerObservationToNextStagingTotalTicks, workerObservationToStagingTicks);
                    UpdateMin(ref _workerObservationToNextStagingMinTicks, workerObservationToStagingTicks);
                    UpdateMax(ref _workerObservationToNextStagingMaxTicks, workerObservationToStagingTicks);
                }
            }

            long lastResponseTick = Volatile.Read(ref _lastDefinitiveResponseCorrelatedTick);
            if (lastResponseTick > 0)
            {
                long responseToStagingTicks = stageStartTick - lastResponseTick;
                if (responseToStagingTicks >= 0)
                {
                    _ = Interlocked.Increment(ref _responseToNextStagingCount);
                    _ = Interlocked.Add(ref _responseToNextStagingTotalTicks, responseToStagingTicks);
                    UpdateMin(ref _responseToNextStagingMinTicks, responseToStagingTicks);
                    UpdateMax(ref _responseToNextStagingMaxTicks, responseToStagingTicks);
                }
            }
        }

        /// <summary>
        /// Captures a point-in-time snapshot of all collected timing counters.
        /// </summary>
        /// <returns>A snapshot containing raw stopwatch-frequency data and aggregate timing buckets.</returns>
        internal TransitTimingSnapshot CaptureSnapshot()
        {
            return new TransitTimingSnapshot(
                StopwatchFrequency: Stopwatch.Frequency,
                PublishPayloadCopy: new TransitTimingBucket(
                    Count: Interlocked.Read(ref _publishPayloadCopyCount),
                    TotalTicks: Interlocked.Read(ref _publishPayloadCopyTotalTicks),
                    MinTicks: 0,
                    MaxTicks: 0),
                DotStuffStage: new TransitTimingBucket(
                    Count: Interlocked.Read(ref _dotStuffStageCount),
                    TotalTicks: Interlocked.Read(ref _dotStuffStageTotalTicks),
                    MinTicks: NormalizeMin(Volatile.Read(ref _dotStuffStageMinTicks)),
                    MaxTicks: Volatile.Read(ref _dotStuffStageMaxTicks)),
                DotStuffPayloadBytesProcessed: Interlocked.Read(ref _dotStuffPayloadBytes),
                DotStuffGetSpanCalls: Interlocked.Read(ref _dotStuffGetSpanCalls),
                DotStuffAdvanceCalls: Interlocked.Read(ref _dotStuffAdvanceCalls),
                DotStuffStuffedDotEvents: Interlocked.Read(ref _dotStuffStuffedDotEvents),
                FlushWait: new TransitTimingBucket(
                    Count: Interlocked.Read(ref _flushCount),
                    TotalTicks: Interlocked.Read(ref _flushTotalTicks),
                    MinTicks: NormalizeMin(Volatile.Read(ref _flushMinTicks)),
                    MaxTicks: Volatile.Read(ref _flushMaxTicks)),
                ResponseLineRead: new TransitTimingBucket(
                    Count: Interlocked.Read(ref _responseLineReadCount),
                    TotalTicks: Interlocked.Read(ref _responseLineReadTotalTicks),
                    MinTicks: NormalizeMin(Volatile.Read(ref _responseLineReadMinTicks)),
                    MaxTicks: Volatile.Read(ref _responseLineReadMaxTicks)),
                ResponseCorrelation: new TransitTimingBucket(
                    Count: Interlocked.Read(ref _responseCorrelationCount),
                    TotalTicks: Interlocked.Read(ref _responseCorrelationTotalTicks),
                    MinTicks: NormalizeMin(Volatile.Read(ref _responseCorrelationMinTicks)),
                    MaxTicks: Volatile.Read(ref _responseCorrelationMaxTicks)),
                ResponseAvailableToCorrelated: new TransitTimingBucket(
                    Count: Interlocked.Read(ref _responseAvailableToCorrelatedCount),
                    TotalTicks: Interlocked.Read(ref _responseAvailableToCorrelatedTotalTicks),
                    MinTicks: NormalizeMin(Volatile.Read(ref _responseAvailableToCorrelatedMinTicks)),
                    MaxTicks: Volatile.Read(ref _responseAvailableToCorrelatedMaxTicks)),
                CompletionEnqueueToWorkerObservation: new TransitTimingBucket(
                    Count: Interlocked.Read(ref _completionEnqueueToObserveCount),
                    TotalTicks: Interlocked.Read(ref _completionEnqueueToObserveTotalTicks),
                    MinTicks: NormalizeMin(Volatile.Read(ref _completionEnqueueToObserveMinTicks)),
                    MaxTicks: Volatile.Read(ref _completionEnqueueToObserveMaxTicks)),
                WorkerPollDelay: new TransitTimingBucket(
                    Count: Interlocked.Read(ref _workerPollDelayCount),
                    TotalTicks: Interlocked.Read(ref _workerPollDelayTotalTicks),
                    MinTicks: 0,
                    MaxTicks: 0),
                ResponseToWorkerObservation: new TransitTimingBucket(
                    Count: Interlocked.Read(ref _responseToWorkerObservationCount),
                    TotalTicks: Interlocked.Read(ref _responseToWorkerObservationTotalTicks),
                    MinTicks: NormalizeMin(Volatile.Read(ref _responseToWorkerObservationMinTicks)),
                    MaxTicks: Volatile.Read(ref _responseToWorkerObservationMaxTicks)),
                WorkerObservationToNextStaging: new TransitTimingBucket(
                    Count: Interlocked.Read(ref _workerObservationToNextStagingCount),
                    TotalTicks: Interlocked.Read(ref _workerObservationToNextStagingTotalTicks),
                    MinTicks: NormalizeMin(Volatile.Read(ref _workerObservationToNextStagingMinTicks)),
                    MaxTicks: Volatile.Read(ref _workerObservationToNextStagingMaxTicks)),
                ResponseToNextStaging: new TransitTimingBucket(
                    Count: Interlocked.Read(ref _responseToNextStagingCount),
                    TotalTicks: Interlocked.Read(ref _responseToNextStagingTotalTicks),
                    MinTicks: NormalizeMin(Volatile.Read(ref _responseToNextStagingMinTicks)),
                    MaxTicks: Volatile.Read(ref _responseToNextStagingMaxTicks)));
        }

        /// <summary>
        /// Converts the uninitialized min sentinel into zero for snapshot consumers.
        /// </summary>
        /// <param name="value">Recorded min value or the uninitialized sentinel.</param>
        /// <returns><c>0</c> when no values were recorded; otherwise the supplied minimum.</returns>
        private static long NormalizeMin(long value)
        {
            return value == long.MaxValue ? 0 : value;
        }

        /// <summary>
        /// Atomically lowers a min-tracking field when a smaller candidate is observed.
        /// </summary>
        /// <param name="target">Reference to the tracked minimum field.</param>
        /// <param name="candidate">Candidate value to compare against the current minimum.</param>
        private static void UpdateMin(ref long target, long candidate)
        {
            while (true)
            {
                long observed = Volatile.Read(ref target);
                if (candidate >= observed)
                {
                    return;
                }

                if (Interlocked.CompareExchange(ref target, candidate, observed) == observed)
                {
                    return;
                }
            }
        }

        /// <summary>
        /// Atomically raises a max-tracking field when a larger candidate is observed.
        /// </summary>
        /// <param name="target">Reference to the tracked maximum field.</param>
        /// <param name="candidate">Candidate value to compare against the current maximum.</param>
        private static void UpdateMax(ref long target, long candidate)
        {
            while (true)
            {
                long observed = Volatile.Read(ref target);
                if (candidate <= observed)
                {
                    return;
                }

                if (Interlocked.CompareExchange(ref target, candidate, observed) == observed)
                {
                    return;
                }
            }
        }
    }

    /// <summary>
    /// Immutable snapshot of timing counters captured by <see cref="TransitTimingCollector"/>.
    /// </summary>
    /// <param name="StopwatchFrequency">Stopwatch frequency that callers use to convert ticks to elapsed time.</param>
    /// <param name="PublishPayloadCopy">Aggregate timing for payload-copy work performed before queue admission.</param>
    /// <param name="DotStuffStage">Aggregate timing for dot-stuff staging.</param>
    /// <param name="DotStuffPayloadBytesProcessed">Total payload bytes processed by dot-stuff staging.</param>
    /// <param name="DotStuffGetSpanCalls">Total <c>GetSpan</c> calls attributed to dot-stuff staging.</param>
    /// <param name="DotStuffAdvanceCalls">Total <c>Advance</c> calls attributed to dot-stuff staging.</param>
    /// <param name="DotStuffStuffedDotEvents">Total escaped line-leading dots inserted during staging.</param>
    /// <param name="FlushWait">Aggregate timing for flush waits.</param>
    /// <param name="ResponseLineRead">Aggregate timing for protocol response-line reads.</param>
    /// <param name="ResponseCorrelation">Aggregate timing for response correlation.</param>
    /// <param name="ResponseAvailableToCorrelated">Aggregate timing from response availability to successful correlation.</param>
    /// <param name="CompletionEnqueueToWorkerObservation">Aggregate timing from completion enqueue to worker observation.</param>
    /// <param name="WorkerPollDelay">Aggregate worker poll delay measurements.</param>
    /// <param name="ResponseToWorkerObservation">Aggregate timing from definitive response correlation to worker observation.</param>
    /// <param name="WorkerObservationToNextStaging">Aggregate timing from worker observation to the next staging start.</param>
    /// <param name="ResponseToNextStaging">Aggregate timing from definitive response correlation to the next staging start.</param>
    internal sealed record TransitTimingSnapshot(
        long StopwatchFrequency,
        TransitTimingBucket PublishPayloadCopy,
        TransitTimingBucket DotStuffStage,
        long DotStuffPayloadBytesProcessed,
        long DotStuffGetSpanCalls,
        long DotStuffAdvanceCalls,
        long DotStuffStuffedDotEvents,
        TransitTimingBucket FlushWait,
        TransitTimingBucket ResponseLineRead,
        TransitTimingBucket ResponseCorrelation,
        TransitTimingBucket ResponseAvailableToCorrelated,
        TransitTimingBucket CompletionEnqueueToWorkerObservation,
        TransitTimingBucket WorkerPollDelay,
        TransitTimingBucket ResponseToWorkerObservation,
        TransitTimingBucket WorkerObservationToNextStaging,
        TransitTimingBucket ResponseToNextStaging);

    /// <summary>
    /// Aggregate timing bucket containing count, total, and optional min/max tick measurements.
    /// </summary>
    /// <param name="Count">Number of samples recorded in the bucket.</param>
    /// <param name="TotalTicks">Sum of all recorded sample durations in stopwatch ticks.</param>
    /// <param name="MinTicks">Minimum recorded sample duration in stopwatch ticks, or zero when untracked or absent.</param>
    /// <param name="MaxTicks">Maximum recorded sample duration in stopwatch ticks, or zero when untracked or absent.</param>
    internal sealed record TransitTimingBucket(
        long Count,
        long TotalTicks,
        long MinTicks,
        long MaxTicks);
}
