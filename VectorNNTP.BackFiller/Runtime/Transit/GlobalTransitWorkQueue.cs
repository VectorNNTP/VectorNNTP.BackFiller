// <copyright file="GlobalTransitWorkQueue.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Transit
// Implements the global transit work queue behavior.

using System.Collections.Concurrent;
using System.Threading.Channels;

namespace VectorNNTP.Backfiller.Runtime.Transit
{
    /// <summary>
    /// Single bounded global transit work queue with item-count and payload-byte admission limits.
    /// </summary>
    internal sealed class GlobalTransitWorkQueue
    {
        /// <summary>
        /// Tracks ready queue for global transit work queue.
        /// </summary>
        private readonly Channel<TransitWorkItem> _readyQueue;
        /// <summary>
        /// Tracks scheduled retries for global transit work queue.
        /// </summary>
        private readonly ConcurrentQueue<ScheduledRetry> _scheduledRetries = new();
        /// <summary>
        /// Tracks retry scheduled signal for global transit work queue.
        /// </summary>
        private readonly SemaphoreSlim _retryScheduledSignal = new(0);
        /// <summary>
        /// Tracks admission gate for global transit work queue.
        /// </summary>
        private readonly object _admissionGate = new();
        /// <summary>
        /// Tracks claim gate for global transit work queue.
        /// </summary>
        private readonly object _claimGate = new();

        /// <summary>
        /// Limits max queued item count for global transit work queue.
        /// </summary>
        private readonly int _maxQueuedItemCount;
        /// <summary>
        /// Limits max queued payload bytes for global transit work queue.
        /// </summary>
        private readonly long _maxQueuedPayloadBytes;

        /// <summary>
        /// Limits queued item count for global transit work queue.
        /// </summary>
        private long _queuedItemCount;
        /// <summary>
        /// Stores queued payload bytes for global transit work queue.
        /// </summary>
        private long _queuedPayloadBytes;
        /// <summary>
        /// Limits retry pending count for global transit work queue.
        /// </summary>
        private long _retryPendingCount;
        /// <summary>
        /// Limits in flight count for global transit work queue.
        /// </summary>
        private long _inFlightCount;
        /// <summary>
        /// Limits admission wait count for global transit work queue.
        /// </summary>
        private long _admissionWaitCount;

        /// <summary>
        /// Tracks admission frozen for global transit work queue.
        /// </summary>
        private volatile bool _admissionFrozen;

        /// <summary>
        /// Coordinates global transit work queue for global transit work queue.
        /// </summary>
        internal GlobalTransitWorkQueue(int maxQueuedItemCount, long maxQueuedPayloadBytes)
        {
            if (maxQueuedItemCount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxQueuedItemCount), maxQueuedItemCount, "Max queued item count must be greater than zero.");
            }

            if (maxQueuedPayloadBytes <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxQueuedPayloadBytes), maxQueuedPayloadBytes, "Max queued payload bytes must be greater than zero.");
            }

            _maxQueuedItemCount = maxQueuedItemCount;
            _maxQueuedPayloadBytes = maxQueuedPayloadBytes;
            _readyQueue = Channel.CreateUnbounded<TransitWorkItem>(new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
            });
        }

        /// <summary>
        /// Limits queued item count for global transit work queue.
        /// </summary>
        internal long QueuedItemCount => Interlocked.Read(ref _queuedItemCount);

        /// <summary>
        /// Stores queued payload bytes for global transit work queue.
        /// </summary>
        internal long QueuedPayloadBytes => Interlocked.Read(ref _queuedPayloadBytes);

        /// <summary>
        /// Limits retry pending count for global transit work queue.
        /// </summary>
        internal long RetryPendingCount => Interlocked.Read(ref _retryPendingCount);

        /// <summary>
        /// Limits in flight count for global transit work queue.
        /// </summary>
        internal long InFlightCount => Interlocked.Read(ref _inFlightCount);

        /// <summary>
        /// Limits admission wait count for global transit work queue.
        /// </summary>
        internal long AdmissionWaitCount => Interlocked.Read(ref _admissionWaitCount);

        /// <summary>
        /// Tracks is admission frozen for global transit work queue.
        /// </summary>
        internal bool IsAdmissionFrozen => _admissionFrozen;

        /// <summary>
        /// Coordinates enqueue async for global transit work queue.
        /// </summary>
        internal async ValueTask EnqueueAsync(TransitWorkItem item, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(item);

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (item.IsTerminal)
                {
                    return;
                }

                if (_admissionFrozen)
                {
                    throw new InvalidOperationException("Global transit queue admission is frozen.");
                }

                bool reserved = false;
                lock (_admissionGate)
                {
                    if (CanAdmit(item.PayloadBytes))
                    {
                        _ = Interlocked.Increment(ref _queuedItemCount);
                        _ = Interlocked.Add(ref _queuedPayloadBytes, item.PayloadBytes);
                        reserved = true;
                    }
                }

                if (reserved)
                {
                    await _readyQueue.Writer.WriteAsync(item, CancellationToken.None).ConfigureAwait(false);
                    return;
                }

                if (item.IsTerminal)
                {
                    return;
                }

                _ = Interlocked.Increment(ref _admissionWaitCount);
                await WaitForCapacityAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Coordinates try claim for global transit work queue.
        /// </summary>
        internal bool TryClaim(string connectionId, out TransitWorkItem? item)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(connectionId);

            lock (_claimGate)
            {
                while (_readyQueue.Reader.TryRead(out TransitWorkItem? candidate))
                {
                    if (candidate is null)
                    {
                        continue;
                    }

                    if (!candidate.TryMarkClaimed(connectionId, DateTimeOffset.UtcNow))
                    {
                        continue;
                    }

                    _ = Interlocked.Decrement(ref _queuedItemCount);
                    _ = Interlocked.Add(ref _queuedPayloadBytes, -candidate.PayloadBytes);
                    _ = Interlocked.Increment(ref _inFlightCount);
                    item = candidate;
                    return true;
                }

                item = null;
                return false;
            }
        }

        /// <summary>
        /// Coordinates wait for work async for global transit work queue.
        /// </summary>
        internal async ValueTask<bool> WaitForWorkAsync(CancellationToken cancellationToken)
        {
            while (true)
            {
                await DrainEligibleRetriesAsync(cancellationToken).ConfigureAwait(false);

                if (_readyQueue.Reader.TryPeek(out _))
                {
                    return true;
                }

                ValueTask<bool> waitForRead = _readyQueue.Reader.WaitToReadAsync(cancellationToken);
                if (!_scheduledRetries.TryPeek(out ScheduledRetry scheduled))
                {
                    return await waitForRead.ConfigureAwait(false);
                }

                TimeSpan delayUntilEligible = scheduled.NotBeforeUtc - DateTimeOffset.UtcNow;
                if (delayUntilEligible <= TimeSpan.Zero)
                {
                    continue;
                }

                Task<bool> readableTask = waitForRead.AsTask();
                Task delayTask = Task.Delay(delayUntilEligible, cancellationToken);
                Task retrySignalTask = _retryScheduledSignal.WaitAsync(cancellationToken);
                Task completed = await Task.WhenAny(readableTask, delayTask, retrySignalTask).ConfigureAwait(false);
                if (ReferenceEquals(completed, readableTask))
                {
                    return await readableTask.ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Coordinates schedule retry async for global transit work queue.
        /// </summary>
        internal async ValueTask<bool> ScheduleRetryAsync(
            TransitWorkItem item,
            TransitWorkFailureClass failureClass,
            TransitTransmissionUncertainty uncertainty,
            TimeSpan retryDelay,
            bool transferOwnershipFromInFlight,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(item);

            if (!item.HasAttemptsRemaining())
            {
                if (transferOwnershipFromInFlight)
                {
                    MarkInFlightTerminal();
                }

                return false;
            }

            DateTimeOffset now = DateTimeOffset.UtcNow;
            if (!item.TryMoveToRetryPending(failureClass, uncertainty, now, retryDelay))
            {
                return false;
            }

            if (transferOwnershipFromInFlight)
            {
                MarkInFlightTerminal();
            }

            _ = Interlocked.Increment(ref _retryPendingCount);
            _scheduledRetries.Enqueue(new ScheduledRetry(item, item.NextEligibleUtc ?? now));
            _ = _retryScheduledSignal.Release();
            await DrainEligibleRetriesAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }

        /// <summary>
        /// Coordinates drain eligible retries async for global transit work queue.
        /// </summary>
        internal async ValueTask DrainEligibleRetriesAsync(CancellationToken cancellationToken)
        {
            while (_scheduledRetries.TryPeek(out ScheduledRetry scheduled))
            {
                cancellationToken.ThrowIfCancellationRequested();

                DateTimeOffset now = DateTimeOffset.UtcNow;
                if (scheduled.NotBeforeUtc > now)
                {
                    break;
                }

                if (!_scheduledRetries.TryDequeue(out ScheduledRetry dequeued))
                {
                    continue;
                }

                if (!dequeued.Item.TryMarkQueued(DateTimeOffset.UtcNow))
                {
                    continue;
                }

                _ = Interlocked.Decrement(ref _retryPendingCount);
                try
                {
                    await EnqueueAsync(dequeued.Item, cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    if (dequeued.Item.TryRevertQueuedToRetryPending(DateTimeOffset.UtcNow))
                    {
                        _ = Interlocked.Increment(ref _retryPendingCount);
                        _scheduledRetries.Enqueue(dequeued);
                    }

                    throw;
                }
            }
        }

        /// <summary>
        /// Coordinates mark in flight terminal for global transit work queue.
        /// </summary>
        internal void MarkInFlightTerminal()
        {
            while (true)
            {
                long observed = Interlocked.Read(ref _inFlightCount);
                if (observed <= 0)
                {
                    throw new InvalidOperationException("Global transit queue in-flight accounting invariant violated: decrement attempted with no in-flight ownership.");
                }

                if (Interlocked.CompareExchange(ref _inFlightCount, observed - 1, observed) == observed)
                {
                    return;
                }
            }
        }

        /// <summary>
        /// Coordinates mark queued terminal for global transit work queue.
        /// </summary>
        internal void MarkQueuedTerminal(int payloadBytes)
        {
            DecrementQueuedOwnership(payloadBytes);
        }

        /// <summary>
        /// Coordinates mark retry pending terminal for global transit work queue.
        /// </summary>
        internal void MarkRetryPendingTerminal()
        {
            DecrementRetryPendingOwnership();
        }

        /// <summary>
        /// Coordinates freeze admission for global transit work queue.
        /// </summary>
        internal void FreezeAdmission()
        {
            _admissionFrozen = true;
        }

        /// <summary>
        /// Coordinates capture snapshot for global transit work queue.
        /// </summary>
        internal GlobalTransitWorkQueueSnapshot CaptureSnapshot()
        {
            return new GlobalTransitWorkQueueSnapshot(
                MaxQueuedItemCount: _maxQueuedItemCount,
                MaxQueuedPayloadBytes: _maxQueuedPayloadBytes,
                QueuedItemCount: Interlocked.Read(ref _queuedItemCount),
                QueuedPayloadBytes: Interlocked.Read(ref _queuedPayloadBytes),
                RetryPendingCount: Interlocked.Read(ref _retryPendingCount),
                InFlightCount: Interlocked.Read(ref _inFlightCount),
                AdmissionWaitCount: Interlocked.Read(ref _admissionWaitCount),
                IsAdmissionFrozen: _admissionFrozen);
        }

        /// <summary>
        /// Coordinates can admit for global transit work queue.
        /// </summary>
        private bool CanAdmit(int payloadBytes)
        {
            long currentCount = Interlocked.Read(ref _queuedItemCount);
            long currentBytes = Interlocked.Read(ref _queuedPayloadBytes);
            return currentCount + 1 <= _maxQueuedItemCount && currentBytes + payloadBytes <= _maxQueuedPayloadBytes;
        }

        /// <summary>
        /// Coordinates wait for capacity async for global transit work queue.
        /// </summary>
        private static Task WaitForCapacityAsync(CancellationToken cancellationToken)
        {
            return Task.Delay(TimeSpan.FromMilliseconds(5), cancellationToken);
        }

        /// <summary>
        /// Coordinates decrement queued ownership for global transit work queue.
        /// </summary>
        private void DecrementQueuedOwnership(int payloadBytes)
        {
            while (true)
            {
                long observedQueued = Interlocked.Read(ref _queuedItemCount);
                if (observedQueued <= 0)
                {
                    throw new InvalidOperationException("Global transit queue queued-item accounting invariant violated: decrement attempted with no queued ownership.");
                }

                if (Interlocked.CompareExchange(ref _queuedItemCount, observedQueued - 1, observedQueued) == observedQueued)
                {
                    break;
                }
            }

            while (true)
            {
                long observedPayloadBytes = Interlocked.Read(ref _queuedPayloadBytes);
                if (observedPayloadBytes < payloadBytes)
                {
                    throw new InvalidOperationException("Global transit queue queued-payload accounting invariant violated: decrement exceeds queued payload ownership.");
                }

                if (Interlocked.CompareExchange(ref _queuedPayloadBytes, observedPayloadBytes - payloadBytes, observedPayloadBytes) == observedPayloadBytes)
                {
                    return;
                }
            }
        }

        /// <summary>
        /// Coordinates decrement retry pending ownership for global transit work queue.
        /// </summary>
        private void DecrementRetryPendingOwnership()
        {
            while (true)
            {
                long observed = Interlocked.Read(ref _retryPendingCount);
                if (observed <= 0)
                {
                    throw new InvalidOperationException("Global transit queue retry-pending accounting invariant violated: decrement attempted with no retry ownership.");
                }

                if (Interlocked.CompareExchange(ref _retryPendingCount, observed - 1, observed) == observed)
                {
                    return;
                }
            }
        }

        /// <summary>
        /// Defines struct and its global transit work queue contract.
        /// </summary>
        private readonly record struct ScheduledRetry(TransitWorkItem Item, DateTimeOffset NotBeforeUtc);
    }

    /// <summary>
    /// Defines global transit work queue snapshot and its global transit work queue contract.
    /// </summary>
    internal sealed record GlobalTransitWorkQueueSnapshot(
        int MaxQueuedItemCount,
        long MaxQueuedPayloadBytes,
        long QueuedItemCount,
        long QueuedPayloadBytes,
        long RetryPendingCount,
        long InFlightCount,
        long AdmissionWaitCount,
        bool IsAdmissionFrozen);
}
