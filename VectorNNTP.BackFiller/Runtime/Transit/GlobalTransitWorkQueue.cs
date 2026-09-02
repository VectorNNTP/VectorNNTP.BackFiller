// <copyright file="GlobalTransitWorkQueue.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: global transit work queue in the runtime transit subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

// <copyright file="GlobalTransitWorkQueue.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Typed exception model for deterministic internal failure classification without relying
// on exception-message text parsing.

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
        /// Stores the ready queue state used to enforce this component's runtime contract.
        /// </summary>
        private readonly Channel<TransitWorkItem> _readyQueue;
        /// <summary>
        /// Stores the scheduled retries state used to enforce this component's runtime contract.
        /// </summary>
        private readonly ConcurrentQueue<ScheduledRetry> _scheduledRetries = new();
        /// <summary>
        /// Stores the retry scheduled signal state used to enforce this component's runtime contract.
        /// </summary>
        private readonly SemaphoreSlim _retryScheduledSignal = new(0);
        /// <summary>
        /// Stores the admission gate state used to enforce this component's runtime contract.
        /// </summary>
        private readonly object _admissionGate = new();
        /// <summary>
        /// Stores the claim gate state used to enforce this component's runtime contract.
        /// </summary>
        private readonly object _claimGate = new();

        /// <summary>
        /// Stores the max queued item count state used to enforce this component's runtime contract.
        /// </summary>
        private readonly int _maxQueuedItemCount;
        /// <summary>
        /// Stores the max queued payload bytes state used to enforce this component's runtime contract.
        /// </summary>
        private readonly long _maxQueuedPayloadBytes;

        /// <summary>
        /// Stores the queued item count state used to enforce this component's runtime contract.
        /// </summary>
        private long _queuedItemCount;
        /// <summary>
        /// Stores the queued payload bytes state used to enforce this component's runtime contract.
        /// </summary>
        private long _queuedPayloadBytes;
        /// <summary>
        /// Stores the retry pending count state used to enforce this component's runtime contract.
        /// </summary>
        private long _retryPendingCount;
        /// <summary>
        /// Stores the in flight count state used to enforce this component's runtime contract.
        /// </summary>
        private long _inFlightCount;
        /// <summary>
        /// Stores the admission wait count state used to enforce this component's runtime contract.
        /// </summary>
        private long _admissionWaitCount;

        /// <summary>
        /// Stores the admission frozen state used to enforce this component's runtime contract.
        /// </summary>
        private volatile bool _admissionFrozen;

        /// <summary>
        /// Performs the global transit work queue operation while preserving this component's lifecycle and state contracts.
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
        /// Stores the queued item count state used to enforce this component's runtime contract.
        /// </summary>
        internal long QueuedItemCount => Interlocked.Read(ref _queuedItemCount);

        /// <summary>
        /// Stores the queued payload bytes state used to enforce this component's runtime contract.
        /// </summary>
        internal long QueuedPayloadBytes => Interlocked.Read(ref _queuedPayloadBytes);

        /// <summary>
        /// Stores the retry pending count state used to enforce this component's runtime contract.
        /// </summary>
        internal long RetryPendingCount => Interlocked.Read(ref _retryPendingCount);

        /// <summary>
        /// Stores the in flight count state used to enforce this component's runtime contract.
        /// </summary>
        internal long InFlightCount => Interlocked.Read(ref _inFlightCount);

        /// <summary>
        /// Stores the admission wait count state used to enforce this component's runtime contract.
        /// </summary>
        internal long AdmissionWaitCount => Interlocked.Read(ref _admissionWaitCount);

        /// <summary>
        /// Stores the is admission frozen state used to enforce this component's runtime contract.
        /// </summary>
        internal bool IsAdmissionFrozen => _admissionFrozen;

        /// <summary>
        /// Performs the enqueue operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the try claim operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the wait for work operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the schedule retry operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the drain eligible retries operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the mark in flight terminal operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the mark queued terminal operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        internal void MarkQueuedTerminal(int payloadBytes)
        {
            DecrementQueuedOwnership(payloadBytes);
        }

        /// <summary>
        /// Performs the mark retry pending terminal operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        internal void MarkRetryPendingTerminal()
        {
            DecrementRetryPendingOwnership();
        }

        /// <summary>
        /// Performs the freeze admission operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        internal void FreezeAdmission()
        {
            _admissionFrozen = true;
        }

        /// <summary>
        /// Performs the capture snapshot operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the can admit operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private bool CanAdmit(int payloadBytes)
        {
            long currentCount = Interlocked.Read(ref _queuedItemCount);
            long currentBytes = Interlocked.Read(ref _queuedPayloadBytes);
            return currentCount + 1 <= _maxQueuedItemCount && currentBytes + payloadBytes <= _maxQueuedPayloadBytes;
        }

        /// <summary>
        /// Performs the wait for capacity operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static Task WaitForCapacityAsync(CancellationToken cancellationToken)
        {
            return Task.Delay(TimeSpan.FromMilliseconds(5), cancellationToken);
        }

        /// <summary>
        /// Performs the decrement queued ownership operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the decrement retry pending ownership operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the scheduled retry operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private readonly record struct ScheduledRetry(TransitWorkItem Item, DateTimeOffset NotBeforeUtc);
    }

    /// <summary>
    /// Performs the global transit work queue snapshot operation while preserving this component's lifecycle and state contracts.
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
