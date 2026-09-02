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
    /// Defines transit timing collector and its transit timing collector contract.
    /// </summary>
    internal sealed class TransitTimingCollector
    {
        /// <summary>
        /// Limits publish payload copy count for transit timing collector.
        /// </summary>
        private long _publishPayloadCopyCount;
        /// <summary>
        /// Stores publish payload copy total ticks for transit timing collector.
        /// </summary>
        private long _publishPayloadCopyTotalTicks;

        /// <summary>
        /// Limits dot stuff stage count for transit timing collector.
        /// </summary>
        private long _dotStuffStageCount;
        /// <summary>
        /// Tracks dot stuff stage total ticks for transit timing collector.
        /// </summary>
        private long _dotStuffStageTotalTicks;
        /// <summary>
        /// Tracks dot stuff stage min ticks for transit timing collector.
        /// </summary>
        private long _dotStuffStageMinTicks = long.MaxValue;
        /// <summary>
        /// Limits dot stuff stage max ticks for transit timing collector.
        /// </summary>
        private long _dotStuffStageMaxTicks;
        /// <summary>
        /// Stores dot stuff payload bytes for transit timing collector.
        /// </summary>
        private long _dotStuffPayloadBytes;
        /// <summary>
        /// Tracks dot stuff get span calls for transit timing collector.
        /// </summary>
        private long _dotStuffGetSpanCalls;
        /// <summary>
        /// Tracks dot stuff advance calls for transit timing collector.
        /// </summary>
        private long _dotStuffAdvanceCalls;
        /// <summary>
        /// Tracks dot stuff stuffed dot events for transit timing collector.
        /// </summary>
        private long _dotStuffStuffedDotEvents;

        /// <summary>
        /// Limits flush count for transit timing collector.
        /// </summary>
        private long _flushCount;
        /// <summary>
        /// Tracks flush total ticks for transit timing collector.
        /// </summary>
        private long _flushTotalTicks;
        /// <summary>
        /// Tracks flush min ticks for transit timing collector.
        /// </summary>
        private long _flushMinTicks = long.MaxValue;
        /// <summary>
        /// Limits flush max ticks for transit timing collector.
        /// </summary>
        private long _flushMaxTicks;

        /// <summary>
        /// Limits response line read count for transit timing collector.
        /// </summary>
        private long _responseLineReadCount;
        /// <summary>
        /// Tracks response line read total ticks for transit timing collector.
        /// </summary>
        private long _responseLineReadTotalTicks;
        /// <summary>
        /// Tracks response line read min ticks for transit timing collector.
        /// </summary>
        private long _responseLineReadMinTicks = long.MaxValue;
        /// <summary>
        /// Limits response line read max ticks for transit timing collector.
        /// </summary>
        private long _responseLineReadMaxTicks;

        /// <summary>
        /// Limits response correlation count for transit timing collector.
        /// </summary>
        private long _responseCorrelationCount;
        /// <summary>
        /// Tracks response correlation total ticks for transit timing collector.
        /// </summary>
        private long _responseCorrelationTotalTicks;
        /// <summary>
        /// Tracks response correlation min ticks for transit timing collector.
        /// </summary>
        private long _responseCorrelationMinTicks = long.MaxValue;
        /// <summary>
        /// Limits response correlation max ticks for transit timing collector.
        /// </summary>
        private long _responseCorrelationMaxTicks;

        /// <summary>
        /// Limits response available to correlated count for transit timing collector.
        /// </summary>
        private long _responseAvailableToCorrelatedCount;
        /// <summary>
        /// Tracks response available to correlated total ticks for transit timing collector.
        /// </summary>
        private long _responseAvailableToCorrelatedTotalTicks;
        /// <summary>
        /// Tracks response available to correlated min ticks for transit timing collector.
        /// </summary>
        private long _responseAvailableToCorrelatedMinTicks = long.MaxValue;
        /// <summary>
        /// Limits response available to correlated max ticks for transit timing collector.
        /// </summary>
        private long _responseAvailableToCorrelatedMaxTicks;

        /// <summary>
        /// Limits completion enqueue to observe count for transit timing collector.
        /// </summary>
        private long _completionEnqueueToObserveCount;
        /// <summary>
        /// Tracks completion enqueue to observe total ticks for transit timing collector.
        /// </summary>
        private long _completionEnqueueToObserveTotalTicks;
        /// <summary>
        /// Tracks completion enqueue to observe min ticks for transit timing collector.
        /// </summary>
        private long _completionEnqueueToObserveMinTicks = long.MaxValue;
        /// <summary>
        /// Limits completion enqueue to observe max ticks for transit timing collector.
        /// </summary>
        private long _completionEnqueueToObserveMaxTicks;

        /// <summary>
        /// Limits worker poll delay count for transit timing collector.
        /// </summary>
        private long _workerPollDelayCount;
        /// <summary>
        /// Configures worker poll delay total ticks for transit timing collector.
        /// </summary>
        private long _workerPollDelayTotalTicks;

        /// <summary>
        /// Limits response to worker observation count for transit timing collector.
        /// </summary>
        private long _responseToWorkerObservationCount;
        /// <summary>
        /// Tracks response to worker observation total ticks for transit timing collector.
        /// </summary>
        private long _responseToWorkerObservationTotalTicks;
        /// <summary>
        /// Tracks response to worker observation min ticks for transit timing collector.
        /// </summary>
        private long _responseToWorkerObservationMinTicks = long.MaxValue;
        /// <summary>
        /// Limits response to worker observation max ticks for transit timing collector.
        /// </summary>
        private long _responseToWorkerObservationMaxTicks;

        /// <summary>
        /// Limits worker observation to next staging count for transit timing collector.
        /// </summary>
        private long _workerObservationToNextStagingCount;
        /// <summary>
        /// Tracks worker observation to next staging total ticks for transit timing collector.
        /// </summary>
        private long _workerObservationToNextStagingTotalTicks;
        /// <summary>
        /// Tracks worker observation to next staging min ticks for transit timing collector.
        /// </summary>
        private long _workerObservationToNextStagingMinTicks = long.MaxValue;
        /// <summary>
        /// Limits worker observation to next staging max ticks for transit timing collector.
        /// </summary>
        private long _workerObservationToNextStagingMaxTicks;

        /// <summary>
        /// Limits response to next staging count for transit timing collector.
        /// </summary>
        private long _responseToNextStagingCount;
        /// <summary>
        /// Tracks response to next staging total ticks for transit timing collector.
        /// </summary>
        private long _responseToNextStagingTotalTicks;
        /// <summary>
        /// Tracks response to next staging min ticks for transit timing collector.
        /// </summary>
        private long _responseToNextStagingMinTicks = long.MaxValue;
        /// <summary>
        /// Limits response to next staging max ticks for transit timing collector.
        /// </summary>
        private long _responseToNextStagingMaxTicks;

        /// <summary>
        /// Tracks last definitive response correlated tick for transit timing collector.
        /// </summary>
        private long _lastDefinitiveResponseCorrelatedTick;
        /// <summary>
        /// Tracks last worker observation tick for transit timing collector.
        /// </summary>
        private long _lastWorkerObservationTick;

        /// <summary>
        /// Coordinates record publish payload copy for transit timing collector.
        /// </summary>
        internal void RecordPublishPayloadCopy(long elapsedTicks)
        {
            _ = Interlocked.Increment(ref _publishPayloadCopyCount);
            _ = Interlocked.Add(ref _publishPayloadCopyTotalTicks, elapsedTicks);
        }

        /// <summary>
        /// Coordinates record dot stuff stage for transit timing collector.
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
        /// Coordinates record flush wait for transit timing collector.
        /// </summary>
        internal void RecordFlushWait(long elapsedTicks)
        {
            _ = Interlocked.Increment(ref _flushCount);
            _ = Interlocked.Add(ref _flushTotalTicks, elapsedTicks);
            UpdateMin(ref _flushMinTicks, elapsedTicks);
            UpdateMax(ref _flushMaxTicks, elapsedTicks);
        }

        /// <summary>
        /// Coordinates record response line read for transit timing collector.
        /// </summary>
        internal void RecordResponseLineRead(long elapsedTicks)
        {
            _ = Interlocked.Increment(ref _responseLineReadCount);
            _ = Interlocked.Add(ref _responseLineReadTotalTicks, elapsedTicks);
            UpdateMin(ref _responseLineReadMinTicks, elapsedTicks);
            UpdateMax(ref _responseLineReadMaxTicks, elapsedTicks);
        }

        /// <summary>
        /// Coordinates record response correlation for transit timing collector.
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
        /// Coordinates record completion observed for transit timing collector.
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
        /// Coordinates record worker poll delay for transit timing collector.
        /// </summary>
        internal void RecordWorkerPollDelay(long elapsedTicks)
        {
            _ = Interlocked.Increment(ref _workerPollDelayCount);
            _ = Interlocked.Add(ref _workerPollDelayTotalTicks, elapsedTicks);
        }

        /// <summary>
        /// Coordinates record staging started for transit timing collector.
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
        /// Coordinates capture snapshot for transit timing collector.
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
        /// Coordinates normalize min for transit timing collector.
        /// </summary>
        private static long NormalizeMin(long value)
        {
            return value == long.MaxValue ? 0 : value;
        }

        /// <summary>
        /// Coordinates update min for transit timing collector.
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
        /// Coordinates update max for transit timing collector.
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
    /// Defines transit timing snapshot and its transit timing collector contract.
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
    /// Defines transit timing bucket and its transit timing collector contract.
    /// </summary>
    internal sealed record TransitTimingBucket(
        long Count,
        long TotalTicks,
        long MinTicks,
        long MaxTicks);
}
