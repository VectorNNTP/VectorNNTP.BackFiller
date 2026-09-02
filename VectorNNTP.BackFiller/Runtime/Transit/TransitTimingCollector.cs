// <copyright file="TransitTimingCollector.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: transit timing collector in the runtime transit subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

// <copyright file="TransitTimingCollector.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Typed exception model for deterministic internal failure classification without relying
// on exception-message text parsing.

using System.Diagnostics;

namespace VectorNNTP.Backfiller.Runtime.Transit
{
    /// <summary>
    /// Defines the transit timing collector component and its contracts for this subsystem.
    /// </summary>
    internal sealed class TransitTimingCollector
    {
        /// <summary>
        /// Stores the publish payload copy count state used to enforce this component's runtime contract.
        /// </summary>
        private long _publishPayloadCopyCount;
        /// <summary>
        /// Stores the publish payload copy total ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _publishPayloadCopyTotalTicks;

        /// <summary>
        /// Stores the dot stuff stage count state used to enforce this component's runtime contract.
        /// </summary>
        private long _dotStuffStageCount;
        /// <summary>
        /// Stores the dot stuff stage total ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _dotStuffStageTotalTicks;
        /// <summary>
        /// Stores the dot stuff stage min ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _dotStuffStageMinTicks = long.MaxValue;
        /// <summary>
        /// Stores the dot stuff stage max ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _dotStuffStageMaxTicks;
        /// <summary>
        /// Stores the dot stuff payload bytes state used to enforce this component's runtime contract.
        /// </summary>
        private long _dotStuffPayloadBytes;
        /// <summary>
        /// Stores the dot stuff get span calls state used to enforce this component's runtime contract.
        /// </summary>
        private long _dotStuffGetSpanCalls;
        /// <summary>
        /// Stores the dot stuff advance calls state used to enforce this component's runtime contract.
        /// </summary>
        private long _dotStuffAdvanceCalls;
        /// <summary>
        /// Stores the dot stuff stuffed dot events state used to enforce this component's runtime contract.
        /// </summary>
        private long _dotStuffStuffedDotEvents;

        /// <summary>
        /// Stores the flush count state used to enforce this component's runtime contract.
        /// </summary>
        private long _flushCount;
        /// <summary>
        /// Stores the flush total ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _flushTotalTicks;
        /// <summary>
        /// Stores the flush min ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _flushMinTicks = long.MaxValue;
        /// <summary>
        /// Stores the flush max ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _flushMaxTicks;

        /// <summary>
        /// Stores the response line read count state used to enforce this component's runtime contract.
        /// </summary>
        private long _responseLineReadCount;
        /// <summary>
        /// Stores the response line read total ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _responseLineReadTotalTicks;
        /// <summary>
        /// Stores the response line read min ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _responseLineReadMinTicks = long.MaxValue;
        /// <summary>
        /// Stores the response line read max ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _responseLineReadMaxTicks;

        /// <summary>
        /// Stores the response correlation count state used to enforce this component's runtime contract.
        /// </summary>
        private long _responseCorrelationCount;
        /// <summary>
        /// Stores the response correlation total ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _responseCorrelationTotalTicks;
        /// <summary>
        /// Stores the response correlation min ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _responseCorrelationMinTicks = long.MaxValue;
        /// <summary>
        /// Stores the response correlation max ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _responseCorrelationMaxTicks;

        /// <summary>
        /// Stores the response available to correlated count state used to enforce this component's runtime contract.
        /// </summary>
        private long _responseAvailableToCorrelatedCount;
        /// <summary>
        /// Stores the response available to correlated total ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _responseAvailableToCorrelatedTotalTicks;
        /// <summary>
        /// Stores the response available to correlated min ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _responseAvailableToCorrelatedMinTicks = long.MaxValue;
        /// <summary>
        /// Stores the response available to correlated max ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _responseAvailableToCorrelatedMaxTicks;

        /// <summary>
        /// Stores the completion enqueue to observe count state used to enforce this component's runtime contract.
        /// </summary>
        private long _completionEnqueueToObserveCount;
        /// <summary>
        /// Stores the completion enqueue to observe total ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _completionEnqueueToObserveTotalTicks;
        /// <summary>
        /// Stores the completion enqueue to observe min ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _completionEnqueueToObserveMinTicks = long.MaxValue;
        /// <summary>
        /// Stores the completion enqueue to observe max ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _completionEnqueueToObserveMaxTicks;

        /// <summary>
        /// Stores the worker poll delay count state used to enforce this component's runtime contract.
        /// </summary>
        private long _workerPollDelayCount;
        /// <summary>
        /// Stores the worker poll delay total ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _workerPollDelayTotalTicks;

        /// <summary>
        /// Stores the response to worker observation count state used to enforce this component's runtime contract.
        /// </summary>
        private long _responseToWorkerObservationCount;
        /// <summary>
        /// Stores the response to worker observation total ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _responseToWorkerObservationTotalTicks;
        /// <summary>
        /// Stores the response to worker observation min ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _responseToWorkerObservationMinTicks = long.MaxValue;
        /// <summary>
        /// Stores the response to worker observation max ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _responseToWorkerObservationMaxTicks;

        /// <summary>
        /// Stores the worker observation to next staging count state used to enforce this component's runtime contract.
        /// </summary>
        private long _workerObservationToNextStagingCount;
        /// <summary>
        /// Stores the worker observation to next staging total ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _workerObservationToNextStagingTotalTicks;
        /// <summary>
        /// Stores the worker observation to next staging min ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _workerObservationToNextStagingMinTicks = long.MaxValue;
        /// <summary>
        /// Stores the worker observation to next staging max ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _workerObservationToNextStagingMaxTicks;

        /// <summary>
        /// Stores the response to next staging count state used to enforce this component's runtime contract.
        /// </summary>
        private long _responseToNextStagingCount;
        /// <summary>
        /// Stores the response to next staging total ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _responseToNextStagingTotalTicks;
        /// <summary>
        /// Stores the response to next staging min ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _responseToNextStagingMinTicks = long.MaxValue;
        /// <summary>
        /// Stores the response to next staging max ticks state used to enforce this component's runtime contract.
        /// </summary>
        private long _responseToNextStagingMaxTicks;

        /// <summary>
        /// Stores the last definitive response correlated tick state used to enforce this component's runtime contract.
        /// </summary>
        private long _lastDefinitiveResponseCorrelatedTick;
        /// <summary>
        /// Stores the last worker observation tick state used to enforce this component's runtime contract.
        /// </summary>
        private long _lastWorkerObservationTick;

        /// <summary>
        /// Performs the record publish payload copy operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        internal void RecordPublishPayloadCopy(long elapsedTicks)
        {
            _ = Interlocked.Increment(ref _publishPayloadCopyCount);
            _ = Interlocked.Add(ref _publishPayloadCopyTotalTicks, elapsedTicks);
        }

        /// <summary>
        /// Performs the record dot stuff stage operation while preserving this component's lifecycle and state contracts.
        /// </summary>
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
        /// Performs the record flush wait operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        internal void RecordFlushWait(long elapsedTicks)
        {
            _ = Interlocked.Increment(ref _flushCount);
            _ = Interlocked.Add(ref _flushTotalTicks, elapsedTicks);
            UpdateMin(ref _flushMinTicks, elapsedTicks);
            UpdateMax(ref _flushMaxTicks, elapsedTicks);
        }

        /// <summary>
        /// Performs the record response line read operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        internal void RecordResponseLineRead(long elapsedTicks)
        {
            _ = Interlocked.Increment(ref _responseLineReadCount);
            _ = Interlocked.Add(ref _responseLineReadTotalTicks, elapsedTicks);
            UpdateMin(ref _responseLineReadMinTicks, elapsedTicks);
            UpdateMax(ref _responseLineReadMaxTicks, elapsedTicks);
        }

        /// <summary>
        /// Performs the record response correlation operation while preserving this component's lifecycle and state contracts.
        /// </summary>
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
        /// Performs the record completion observed operation while preserving this component's lifecycle and state contracts.
        /// </summary>
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
        /// Performs the record worker poll delay operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        internal void RecordWorkerPollDelay(long elapsedTicks)
        {
            _ = Interlocked.Increment(ref _workerPollDelayCount);
            _ = Interlocked.Add(ref _workerPollDelayTotalTicks, elapsedTicks);
        }

        /// <summary>
        /// Performs the record staging started operation while preserving this component's lifecycle and state contracts.
        /// </summary>
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
        /// Performs the capture snapshot operation while preserving this component's lifecycle and state contracts.
        /// </summary>
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
        /// Performs the normalize min operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static long NormalizeMin(long value)
        {
            return value == long.MaxValue ? 0 : value;
        }

        /// <summary>
        /// Performs the update min operation while preserving this component's lifecycle and state contracts.
        /// </summary>
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
        /// Performs the update max operation while preserving this component's lifecycle and state contracts.
        /// </summary>
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
    /// Performs the transit timing snapshot operation while preserving this component's lifecycle and state contracts.
    /// </summary>
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
    /// Performs the transit timing bucket operation while preserving this component's lifecycle and state contracts.
    /// </summary>
    internal sealed record TransitTimingBucket(
        long Count,
        long TotalTicks,
        long MinTicks,
        long MaxTicks);
}
