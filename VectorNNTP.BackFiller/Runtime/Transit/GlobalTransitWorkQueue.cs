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
    /// Global bounded transit work queue that tracks queued, retry-pending, and in-flight ownership separately.
    /// </summary>
    /// <remarks>
    /// Admission is bounded by both item count and payload bytes. Work remains globally owned by the queue until a
    /// connection claims it, and explicit accounting helpers enforce exactly-once transfer between queued,
    /// retry-pending, in-flight, and terminal states.
    /// </remarks>
    internal sealed class GlobalTransitWorkQueue : IDisposable
    {
        /// <summary>
        /// Ready queue from which connections claim immediately publishable work items.
        /// </summary>
        private readonly Channel<TransitWorkItem> _readyQueue;

        /// <summary>
        /// Ensures disposal of owned synchronization resources is idempotent.
        /// </summary>
        private int _disposeSignaled;

        /// <summary>
        /// FIFO schedule of retries waiting for their eligibility time.
        /// </summary>
        private readonly ConcurrentQueue<ScheduledRetry> _scheduledRetries = new();

        /// <summary>
        /// Signal used to wake waiters when new retry work is scheduled.
        /// </summary>
        private readonly SemaphoreSlim _retryScheduledSignal = new(0);

        /// <summary>
        /// Lock that serializes admission-capacity reservation updates.
        /// </summary>
        private readonly object _admissionGate = new();

        /// <summary>
        /// Lock that serializes claim operations across competing connections.
        /// </summary>
        private readonly object _claimGate = new();

        /// <summary>
        /// Maximum number of work items allowed to remain queued at once.
        /// </summary>
        private readonly int _maxQueuedItemCount;

        /// <summary>
        /// Maximum aggregate payload bytes allowed to remain queued at once.
        /// </summary>
        private readonly long _maxQueuedPayloadBytes;

        /// <summary>
        /// Current count of work items still owned by the ready queue.
        /// </summary>
        private long _queuedItemCount;

        /// <summary>
        /// Current payload-byte total still owned by the ready queue.
        /// </summary>
        private long _queuedPayloadBytes;

        /// <summary>
        /// Current count of work items waiting for a retry eligibility deadline.
        /// </summary>
        private long _retryPendingCount;

        /// <summary>
        /// Current count of work items claimed by connections and not yet terminally settled.
        /// </summary>
        private long _inFlightCount;

        /// <summary>
        /// Count of admission attempts that had to wait for capacity.
        /// </summary>
        private long _admissionWaitCount;

        /// <summary>
        /// Indicates that no further queue admissions should be accepted.
        /// </summary>
        private volatile bool _admissionFrozen;

        /// <summary>
        /// Initializes a global transit work queue with bounded item-count and payload-byte capacity.
        /// </summary>
        /// <param name="maxQueuedItemCount">Maximum number of ready-queue items that may be buffered at once.</param>
        /// <param name="maxQueuedPayloadBytes">Maximum aggregate payload bytes that may be buffered at once.</param>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when either bound is zero or negative.</exception>
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
        /// Gets the number of work items still owned by the ready queue.
        /// </summary>
        internal long QueuedItemCount => Interlocked.Read(ref _queuedItemCount);

        /// <summary>
        /// Gets the number of payload bytes still owned by the ready queue.
        /// </summary>
        internal long QueuedPayloadBytes => Interlocked.Read(ref _queuedPayloadBytes);

        /// <summary>
        /// Gets the number of work items parked in retry-pending state.
        /// </summary>
        internal long RetryPendingCount => Interlocked.Read(ref _retryPendingCount);

        /// <summary>
        /// Gets the number of work items currently owned by connections.
        /// </summary>
        internal long InFlightCount => Interlocked.Read(ref _inFlightCount);

        /// <summary>
        /// Gets the number of admission attempts that had to wait for queue capacity.
        /// </summary>
        internal long AdmissionWaitCount => Interlocked.Read(ref _admissionWaitCount);

        /// <summary>
        /// Gets whether queue admission has been frozen for shutdown or preemption.
        /// </summary>
        internal bool IsAdmissionFrozen => _admissionFrozen;

        /// <summary>
        /// Enqueues a work item once item-count and payload-byte capacity are both available.
        /// </summary>
        /// <param name="item">Work item to admit into the ready queue.</param>
        /// <param name="cancellationToken">Cancellation token for blocked admission waits.</param>
        /// <returns>A value task that completes after the item is admitted or the operation is canceled.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="item"/> is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">Thrown when admission has been frozen before the item can be admitted.</exception>
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
        /// Attempts to claim the next ready work item for a specific connection.
        /// </summary>
        /// <param name="connectionId">Stable connection identifier that will own the claimed work item.</param>
        /// <param name="item">When successful, receives the claimed work item.</param>
        /// <returns><see langword="true"/> when a ready item was claimed; otherwise <see langword="false"/>.</returns>
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
        /// Waits until ready work becomes readable or a scheduled retry reaches eligibility.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token for the wait.</param>
        /// <returns><see langword="true"/> when work is available to claim; otherwise <see langword="false"/> if the ready channel completes.</returns>
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
        /// Moves a claimed work item into retry-pending state when attempt budget remains.
        /// </summary>
        /// <param name="item">Work item to schedule for retry.</param>
        /// <param name="failureClass">Failure classification recorded on the work item.</param>
        /// <param name="uncertainty">Transmission certainty classification recorded on the work item.</param>
        /// <param name="retryDelay">Delay before the item becomes eligible for requeue.</param>
        /// <param name="transferOwnershipFromInFlight"><see langword="true"/> when in-flight ownership should be released as part of scheduling the retry.</param>
        /// <param name="cancellationToken">Cancellation token for immediate follow-up draining.</param>
        /// <returns><see langword="true"/> when retry scheduling succeeded; otherwise <see langword="false"/> if the item should be terminalized instead.</returns>
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
        /// Re-enqueues all retry-pending items whose <c>NotBeforeUtc</c> deadline has elapsed.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token for requeue work.</param>
        /// <returns>A value task that completes after currently eligible retries have been drained.</returns>
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
        /// Releases one in-flight ownership slot after a claimed item reaches a terminal outcome.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when in-flight accounting would underflow.</exception>
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
        /// Releases queued ownership for an item that is terminalized before being claimed.
        /// </summary>
        /// <param name="payloadBytes">Payload-byte contribution previously counted for the queued item.</param>
        internal void MarkQueuedTerminal(int payloadBytes)
        {
            DecrementQueuedOwnership(payloadBytes);
        }

        /// <summary>
        /// Releases retry-pending ownership for an item that is terminalized before requeue.
        /// </summary>
        internal void MarkRetryPendingTerminal()
        {
            DecrementRetryPendingOwnership();
        }

        /// <summary>
        /// Prevents any future queue admissions.
        /// </summary>
        internal void FreezeAdmission()
        {
            _admissionFrozen = true;
        }

        /// <summary>
        /// Captures a point-in-time queue-accounting snapshot.
        /// </summary>
        /// <returns>Current queue limits and ownership counters.</returns>
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
        /// Determines whether a new item of the specified size can be admitted under current queue bounds.
        /// </summary>
        /// <param name="payloadBytes">Payload-byte contribution of the candidate item.</param>
        /// <returns><see langword="true"/> when both item-count and payload-byte limits allow admission.</returns>
        private bool CanAdmit(int payloadBytes)
        {
            long currentCount = Interlocked.Read(ref _queuedItemCount);
            long currentBytes = Interlocked.Read(ref _queuedPayloadBytes);
            return currentCount + 1 <= _maxQueuedItemCount && currentBytes + payloadBytes <= _maxQueuedPayloadBytes;
        }

        /// <summary>
        /// Waits briefly before re-checking queue admission capacity.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token for the delay.</param>
        /// <returns>A task representing the capacity wait.</returns>
        private static Task WaitForCapacityAsync(CancellationToken cancellationToken)
        {
            return Task.Delay(TimeSpan.FromMilliseconds(5), cancellationToken);
        }

        /// <summary>
        /// Releases queued ownership counters for one item.
        /// </summary>
        /// <param name="payloadBytes">Payload-byte contribution previously counted for the item.</param>
        /// <exception cref="InvalidOperationException">Thrown when queued-item or queued-byte accounting would underflow.</exception>
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
        /// Releases retry-pending ownership for one item.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when retry-pending accounting would underflow.</exception>
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
        /// Retry entry pairing a work item with the time it becomes eligible for requeue.
        /// </summary>
        /// <param name="Item">Retry-pending work item.</param>
        /// <param name="NotBeforeUtc">UTC time before which the item must not be re-enqueued.</param>
        private readonly record struct ScheduledRetry(TransitWorkItem Item, DateTimeOffset NotBeforeUtc);

        /// <summary>
        /// Disposes synchronization resources owned by the global transit queue.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposeSignaled, 1) != 0)
            {
                return;
            }

            _retryScheduledSignal.Dispose();
        }
    }

    /// <summary>
    /// Immutable point-in-time snapshot of global transit queue capacity and ownership counters.
    /// </summary>
    /// <param name="MaxQueuedItemCount">Configured maximum ready-queue item count.</param>
    /// <param name="MaxQueuedPayloadBytes">Configured maximum ready-queue payload bytes.</param>
    /// <param name="QueuedItemCount">Current number of items in the ready queue.</param>
    /// <param name="QueuedPayloadBytes">Current number of payload bytes in the ready queue.</param>
    /// <param name="RetryPendingCount">Current number of retry-pending items.</param>
    /// <param name="InFlightCount">Current number of items owned by connections.</param>
    /// <param name="AdmissionWaitCount">Number of admissions that had to wait for capacity.</param>
    /// <param name="IsAdmissionFrozen">Indicates whether new admissions are currently blocked.</param>
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
