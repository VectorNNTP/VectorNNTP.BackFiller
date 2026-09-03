// <copyright file="TransitWorkItem.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Transit
// Implements the transit work item behavior.

using System.Diagnostics;

namespace VectorNNTP.Backfiller.Runtime.Transit
{
    /// <summary>
    /// Mutable work item that tracks one article as ownership moves through queueing, connection claim, retry, and terminal settlement.
    /// </summary>
    /// <remarks>
    /// State transitions are coordinated with atomic fields so queue and connection workers can enforce exactly-once
    /// terminal completion and explicit ownership accounting.
    /// </remarks>
    internal sealed class TransitWorkItem
    {
        /// <summary>
        /// Indicates whether any terminal completion path has already won.
        /// </summary>
        private int _terminalCompletionObserved;

        /// <summary>
        /// Integer-backed state field used for atomic transition operations.
        /// </summary>
        private int _stateValue = (int)TransitWorkItemState.Queued;

        /// <summary>
        /// Completion source used to signal the caller-facing publish result exactly once.
        /// </summary>
        private readonly TaskCompletionSource<TransitPublishResult> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>
        /// Initializes a transit work item for one article payload.
        /// </summary>
        /// <param name="workItemId">Unique publisher-assigned work-item identifier.</param>
        /// <param name="messageId">Article Message-ID used for protocol framing and response correlation.</param>
        /// <param name="payload">Owned payload bytes that remain associated with this work item for its full lifetime.</param>
        /// <param name="maxAttempts">Maximum number of transmission attempts allowed before failure terminalization.</param>
        internal TransitWorkItem(long workItemId, string messageId, byte[] payload, int maxAttempts = 3)
        {
            if (string.IsNullOrWhiteSpace(messageId))
            {
                throw new ArgumentException("Message-ID is required.", nameof(messageId));
            }

            ArgumentNullException.ThrowIfNull(payload);

            if (payload.Length == 0)
            {
                throw new ArgumentException("Payload must not be empty.", nameof(payload));
            }

            if (maxAttempts <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxAttempts), maxAttempts, "Max attempts must be greater than zero.");
            }

            WorkItemId = workItemId;
            MessageId = messageId;
            Payload = payload;
            PayloadBytes = payload.Length;
            MaxAttempts = maxAttempts;
            FirstEnqueuedUtc = DateTimeOffset.UtcNow;
            LastEnqueuedUtc = FirstEnqueuedUtc;
        }

        /// <summary>
        /// Gets the publisher-assigned identifier for this work item.
        /// </summary>
        internal long WorkItemId { get; }

        /// <summary>
        /// Gets the article Message-ID used for correlation and diagnostics.
        /// </summary>
        internal string MessageId { get; }

        /// <summary>
        /// Gets the owned article payload bytes.
        /// </summary>
        internal byte[] Payload { get; }

        /// <summary>
        /// Gets the payload size in bytes.
        /// </summary>
        internal int PayloadBytes { get; }

        /// <summary>
        /// Gets how many transmission attempts have been claimed so far.
        /// </summary>
        internal int AttemptCount { get; private set; }

        /// <summary>
        /// Gets the maximum number of transmission attempts allowed.
        /// </summary>
        internal int MaxAttempts { get; }

        /// <summary>
        /// Gets the UTC time when the item first entered the queue.
        /// </summary>
        internal DateTimeOffset FirstEnqueuedUtc { get; }

        /// <summary>
        /// Gets the UTC time when the item most recently entered the queue.
        /// </summary>
        internal DateTimeOffset LastEnqueuedUtc { get; private set; }

        /// <summary>
        /// Gets the UTC time when a connection most recently claimed the item.
        /// </summary>
        internal DateTimeOffset? LastClaimedUtc { get; private set; }

        /// <summary>
        /// Gets the UTC time when the item most recently failed.
        /// </summary>
        internal DateTimeOffset? LastFailureUtc { get; private set; }

        /// <summary>
        /// Gets the UTC time before which retry requeue must not occur.
        /// </summary>
        internal DateTimeOffset? NextEligibleUtc { get; private set; }

        /// <summary>
        /// Gets the most recent failure classification recorded for the item.
        /// </summary>
        internal TransitWorkFailureClass? LastFailureClass { get; private set; }

        /// <summary>
        /// Gets the most recent transmission-certainty classification recorded for the item.
        /// </summary>
        internal TransitTransmissionUncertainty? LastTransmissionUncertainty { get; private set; }

        /// <summary>
        /// Gets the current atomic state of the work item.
        /// </summary>
        internal TransitWorkItemState State => (TransitWorkItemState)Volatile.Read(ref _stateValue);

        /// <summary>
        /// Gets the connection currently owning the item, if any.
        /// </summary>
        internal string? OwnerConnectionId { get; private set; }

        /// <summary>
        /// Gets whether caller cancellation has been requested for the work item.
        /// </summary>
        internal bool CancelRequested { get; private set; }

        /// <summary>
        /// Gets the terminal publish status once the item has completed.
        /// </summary>
        internal TransitPublishStatus? TerminalStatus { get; private set; }

        /// <summary>
        /// Gets the terminal provenance once the item has completed.
        /// </summary>
        internal TransitPublishProvenance? TerminalProvenance { get; private set; }

        /// <summary>
        /// Gets the stopwatch tick captured for the most recent state transition.
        /// </summary>
        internal long LastStateTransitionTick { get; private set; }

        /// <summary>
        /// Gets the caller-facing completion task for the terminal publish result.
        /// </summary>
        internal Task<TransitPublishResult> CompletionTask => _completion.Task;

        /// <summary>
        /// Attempts to place the item in queued state.
        /// </summary>
        /// <param name="utcNow">UTC timestamp to record for the requeue operation.</param>
        /// <returns><see langword="true"/> when the item is queued or already queued; otherwise <see langword="false"/>.</returns>
        internal bool TryMarkQueued(DateTimeOffset utcNow)
        {
            while (true)
            {
                TransitWorkItemState current = State;
                if (IsTerminalState(current))
                {
                    return false;
                }

                if (current == TransitWorkItemState.Queued)
                {
                    OwnerConnectionId = null;
                    LastEnqueuedUtc = utcNow;
                    LastStateTransitionTick = Stopwatch.GetTimestamp();
                    return true;
                }

                if (current != TransitWorkItemState.RetryPending)
                {
                    return false;
                }

                if (Interlocked.CompareExchange(ref _stateValue, (int)TransitWorkItemState.Queued, (int)TransitWorkItemState.RetryPending)
                    == (int)TransitWorkItemState.RetryPending)
                {
                    OwnerConnectionId = null;
                    LastEnqueuedUtc = utcNow;
                    LastStateTransitionTick = Stopwatch.GetTimestamp();
                    return true;
                }
            }
        }

        /// <summary>
        /// Reverts a queued retry back to retry-pending when re-admission fails.
        /// </summary>
        /// <param name="utcNow">UTC timestamp to record for the failure.</param>
        /// <returns><see langword="true"/> when the queued-to-retry-pending transition succeeded; otherwise <see langword="false"/>.</returns>
        internal bool TryRevertQueuedToRetryPending(DateTimeOffset utcNow)
        {
            if (Interlocked.CompareExchange(ref _stateValue, (int)TransitWorkItemState.RetryPending, (int)TransitWorkItemState.Queued)
                != (int)TransitWorkItemState.Queued)
            {
                return false;
            }

            OwnerConnectionId = null;
            LastFailureUtc = utcNow;
            LastStateTransitionTick = Stopwatch.GetTimestamp();
            return true;
        }

        /// <summary>
        /// Queues the item or throws when queue ownership cannot be reacquired.
        /// </summary>
        /// <param name="utcNow">UTC timestamp to record for the queue transition.</param>
        /// <exception cref="InvalidOperationException">Thrown when the item is already terminal.</exception>
        internal void MarkQueued(DateTimeOffset utcNow)
        {
            if (!TryMarkQueued(utcNow))
            {
                throw new InvalidOperationException("Cannot queue a terminal transit work item.");
            }
        }

        /// <summary>
        /// Attempts to claim the item for a connection when it is currently queued.
        /// </summary>
        /// <param name="connectionId">Connection identifier that will own the item.</param>
        /// <param name="utcNow">UTC timestamp to record for the claim.</param>
        /// <returns><see langword="true"/> when the item was claimed; otherwise <see langword="false"/>.</returns>
        internal bool TryMarkClaimed(string connectionId, DateTimeOffset utcNow)
        {
            if (string.IsNullOrWhiteSpace(connectionId))
            {
                throw new ArgumentException("Connection id is required.", nameof(connectionId));
            }

            if (Interlocked.CompareExchange(ref _stateValue, (int)TransitWorkItemState.Claimed, (int)TransitWorkItemState.Queued)
                != (int)TransitWorkItemState.Queued)
            {
                return false;
            }

            AttemptCount++;
            OwnerConnectionId = connectionId;
            LastClaimedUtc = utcNow;
            LastStateTransitionTick = Stopwatch.GetTimestamp();
            return true;
        }

        /// <summary>
        /// Claims the item for a connection or throws when it is not queued.
        /// </summary>
        /// <param name="connectionId">Connection identifier that will own the item.</param>
        /// <param name="utcNow">UTC timestamp to record for the claim.</param>
        /// <exception cref="InvalidOperationException">Thrown when the item is not currently queued.</exception>
        internal void MarkClaimed(string connectionId, DateTimeOffset utcNow)
        {
            if (!TryMarkClaimed(connectionId, utcNow))
            {
                throw new InvalidOperationException("Cannot claim a transit work item that is not queued.");
            }
        }

        /// <summary>
        /// Marks the item as staged after TAKETHIS frame materialization begins.
        /// </summary>
        internal void MarkStaged()
        {
            _ = TryTransitionState(TransitWorkItemState.Claimed, TransitWorkItemState.Staged);
        }

        /// <summary>
        /// Marks the item as flushed after staged bytes have been flushed to the transport.
        /// </summary>
        internal void MarkFlushed()
        {
            _ = TryTransitionState(TransitWorkItemState.Staged, TransitWorkItemState.Flushed);
        }

        /// <summary>
        /// Marks the item as awaiting a server response after flush completion.
        /// </summary>
        internal void MarkAwaitingResponse()
        {
            if (State == TransitWorkItemState.AwaitingResponse)
            {
                return;
            }

            _ = TryTransitionState(TransitWorkItemState.Flushed, TransitWorkItemState.AwaitingResponse);
        }

        /// <summary>
        /// Attempts to move the item into retry-pending state from a connection-owned phase.
        /// </summary>
        /// <param name="failureClass">Failure classification to record.</param>
        /// <param name="uncertainty">Transmission-certainty classification to record.</param>
        /// <param name="utcNow">UTC timestamp to record for the failure.</param>
        /// <param name="retryDelay">Delay before the item becomes eligible for requeue.</param>
        /// <returns><see langword="true"/> when the transition succeeded; otherwise <see langword="false"/>.</returns>
        internal bool TryMoveToRetryPending(
            TransitWorkFailureClass failureClass,
            TransitTransmissionUncertainty uncertainty,
            DateTimeOffset utcNow,
            TimeSpan retryDelay)
        {
            while (true)
            {
                TransitWorkItemState current = State;
                if (IsTerminalState(current))
                {
                    return false;
                }

                if (current is not (TransitWorkItemState.Claimed
                    or TransitWorkItemState.Staged
                    or TransitWorkItemState.Flushed
                    or TransitWorkItemState.AwaitingResponse))
                {
                    return false;
                }

                if (Interlocked.CompareExchange(ref _stateValue, (int)TransitWorkItemState.RetryPending, (int)current) == (int)current)
                {
                    LastFailureClass = failureClass;
                    LastTransmissionUncertainty = uncertainty;
                    LastFailureUtc = utcNow;
                    NextEligibleUtc = utcNow.Add(retryDelay);
                    OwnerConnectionId = null;
                    LastStateTransitionTick = Stopwatch.GetTimestamp();
                    return true;
                }
            }
        }

        /// <summary>
        /// Determines whether another transmission attempt is still permitted.
        /// </summary>
        /// <returns><see langword="true"/> when <see cref="AttemptCount"/> is still below <see cref="MaxAttempts"/>.</returns>
        internal bool HasAttemptsRemaining()
        {
            return AttemptCount < MaxAttempts;
        }

        /// <summary>
        /// Attempts to transition the item into a terminal state exactly once.
        /// </summary>
        /// <param name="status">Terminal publish status to record.</param>
        /// <param name="terminalProvenance">Terminal provenance to record.</param>
        /// <param name="priorState">Receives the state observed immediately before terminalization.</param>
        /// <returns><see langword="true"/> when this call won terminalization; otherwise <see langword="false"/>.</returns>
        internal bool TryTransitionToTerminal(TransitPublishStatus status, TransitPublishProvenance terminalProvenance, out TransitWorkItemState priorState)
        {
            if (Interlocked.CompareExchange(ref _terminalCompletionObserved, 1, 0) != 0)
            {
                priorState = State;
                return false;
            }

            priorState = (TransitWorkItemState)Interlocked.Exchange(ref _stateValue, (int)MapTerminalState(status));
            TerminalStatus = status;
            TerminalProvenance = terminalProvenance;
            LastStateTransitionTick = Stopwatch.GetTimestamp();
            return true;
        }

        /// <summary>
        /// Records that the caller requested cancellation for this work item.
        /// </summary>
        internal void MarkCancelRequested()
        {
            CancelRequested = true;
        }

        /// <summary>
        /// Attempts to terminally complete the item and publish the supplied result.
        /// </summary>
        /// <param name="result">Terminal result to publish to the caller.</param>
        /// <param name="terminalProvenance">Provenance to stamp on the terminal transition.</param>
        /// <returns><see langword="true"/> when this call completed the item; otherwise <see langword="false"/>.</returns>
        internal bool TryComplete(TransitPublishResult result, TransitPublishProvenance terminalProvenance)
        {
            ArgumentNullException.ThrowIfNull(result);

            return TryTransitionToTerminal(result.Status, terminalProvenance, out _)
                && _completion.TrySetResult(result);
        }

        /// <summary>
        /// Attempts to terminally complete the item and also reports the prior state.
        /// </summary>
        /// <param name="result">Terminal result to publish to the caller.</param>
        /// <param name="terminalProvenance">Provenance to stamp on the terminal transition.</param>
        /// <param name="priorState">Receives the state observed immediately before terminalization.</param>
        /// <returns><see langword="true"/> when this call completed the item; otherwise <see langword="false"/>.</returns>
        internal bool TryComplete(TransitPublishResult result, TransitPublishProvenance terminalProvenance, out TransitWorkItemState priorState)
        {
            ArgumentNullException.ThrowIfNull(result);

            return TryTransitionToTerminal(result.Status, terminalProvenance, out priorState) && _completion.TrySetResult(result);
        }

        /// <summary>
        /// Attempts to complete the caller-facing task without changing state.
        /// </summary>
        /// <param name="result">Result to publish to the caller.</param>
        /// <returns><see langword="true"/> when the completion task accepted the result; otherwise <see langword="false"/>.</returns>
        internal bool TrySetCompletionResult(TransitPublishResult result)
        {
            ArgumentNullException.ThrowIfNull(result);
            return _completion.TrySetResult(result);
        }

        /// <summary>
        /// Attempts an atomic state transition between two non-terminal states.
        /// </summary>
        /// <param name="expected">Expected current state.</param>
        /// <param name="next">State to apply when the expectation matches.</param>
        /// <returns><see langword="true"/> when the transition succeeded; otherwise <see langword="false"/>.</returns>
        private bool TryTransitionState(TransitWorkItemState expected, TransitWorkItemState next)
        {
            if (Interlocked.CompareExchange(ref _stateValue, (int)next, (int)expected) != (int)expected)
            {
                return false;
            }

            LastStateTransitionTick = Stopwatch.GetTimestamp();
            return true;
        }

        /// <summary>
        /// Determines whether the supplied state is already terminal.
        /// </summary>
        /// <param name="state">State to classify.</param>
        /// <returns><see langword="true"/> for completed accepted, rejected, canceled, or failed states.</returns>
        private static bool IsTerminalState(TransitWorkItemState state)
        {
            return state is TransitWorkItemState.CompletedAccepted
                or TransitWorkItemState.CompletedRejected
                or TransitWorkItemState.CompletedCanceled
                or TransitWorkItemState.CompletedFailed;
        }

        /// <summary>
        /// Maps a terminal publish status to the internal terminal work-item state.
        /// </summary>
        /// <param name="status">Terminal publish status.</param>
        /// <returns>The internal terminal state used to represent the supplied publish status.</returns>
        private static TransitWorkItemState MapTerminalState(TransitPublishStatus status)
        {
            return status switch
            {
                TransitPublishStatus.Accepted => TransitWorkItemState.CompletedAccepted,
                TransitPublishStatus.Rejected => TransitWorkItemState.CompletedRejected,
                TransitPublishStatus.Canceled => TransitWorkItemState.CompletedCanceled,
                _ => TransitWorkItemState.CompletedFailed,
            };
        }

        /// <summary>
        /// Gets whether any terminal completion path has already succeeded.
        /// </summary>
        internal bool IsTerminal => Volatile.Read(ref _terminalCompletionObserved) == 1;
    }

    /// <summary>
    /// Internal lifecycle states for a transit work item.
    /// </summary>
    internal enum TransitWorkItemState
    {
        /// <summary>
        /// The item is queued and available for claim.
        /// </summary>
        Queued = 0,

        /// <summary>
        /// A connection has claimed the item.
        /// </summary>
        Claimed = 1,

        /// <summary>
        /// TAKETHIS frame staging has begun.
        /// </summary>
        Staged = 2,

        /// <summary>
        /// Staged bytes have been flushed to the transport.
        /// </summary>
        Flushed = 3,

        /// <summary>
        /// The item is waiting for a definitive server response.
        /// </summary>
        AwaitingResponse = 4,

        /// <summary>
        /// The item is waiting for its retry deadline.
        /// </summary>
        RetryPending = 5,

        /// <summary>
        /// The item completed with an accepted result.
        /// </summary>
        CompletedAccepted = 6,

        /// <summary>
        /// The item completed with a rejected result.
        /// </summary>
        CompletedRejected = 7,

        /// <summary>
        /// The item completed with a failed or ambiguous terminal result.
        /// </summary>
        CompletedFailed = 8,

        /// <summary>
        /// The item completed because caller cancellation won.
        /// </summary>
        CompletedCanceled = 9,
    }

    /// <summary>
    /// Classifies the failure mode that caused retry scheduling or terminalization.
    /// </summary>
    internal enum TransitWorkFailureClass
    {
        /// <summary>
        /// The connection reset unexpectedly.
        /// </summary>
        ConnectionReset = 0,

        /// <summary>
        /// The connection was disposed while the item was active.
        /// </summary>
        ConnectionDisposed = 1,

        /// <summary>
        /// Writing the submission frame failed.
        /// </summary>
        WriteFailure = 2,

        /// <summary>
        /// Flushing staged bytes failed.
        /// </summary>
        FlushFailure = 3,

        /// <summary>
        /// Reading the response line failed.
        /// </summary>
        ResponseReadFailure = 4,

        /// <summary>
        /// The response was lost after transmission.
        /// </summary>
        ResponseLost = 5,

        /// <summary>
        /// Local validation failed before safe transmission.
        /// </summary>
        LocalValidationFailure = 6,

        /// <summary>
        /// The server definitively rejected the submission for a permanent protocol reason.
        /// </summary>
        PermanentProtocolRejection = 7,

        /// <summary>
        /// Caller or lifecycle cancellation interrupted processing.
        /// </summary>
        Cancellation = 8,

        /// <summary>
        /// Shutdown deadlines were reached before completion.
        /// </summary>
        ShutdownDeadline = 9,

        /// <summary>
        /// No more specific failure classification was available.
        /// </summary>
        Unknown = 10,
    }

    /// <summary>
    /// Describes how certain the system is about whether bytes reached the remote server.
    /// </summary>
    internal enum TransitTransmissionUncertainty
    {
        /// <summary>
        /// The item was definitely not sent.
        /// </summary>
        DefinitelyNotSent = 0,

        /// <summary>
        /// Bytes were sent, but the response was lost.
        /// </summary>
        SentResponseLost = 1,

        /// <summary>
        /// The connection failed while bytes may have been in flight.
        /// </summary>
        ConnectionFailedDuringSend = 2,

        /// <summary>
        /// The item is being retried and has not yet reached a terminal outcome.
        /// </summary>
        Retrying = 3,
    }
}
