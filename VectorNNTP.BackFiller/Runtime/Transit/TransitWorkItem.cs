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
    /// Represents one outbound transit article owned by producer, global queue, or a connection until terminal completion.
    /// </summary>
    internal sealed class TransitWorkItem
    {
        /// <summary>
        /// Tracks terminal completion observed for transit work item.
        /// </summary>
        private int _terminalCompletionObserved;
        /// <summary>
        /// Tracks state value for transit work item.
        /// </summary>
        private int _stateValue = (int)TransitWorkItemState.Queued;

        /// <summary>
        /// Coordinates transit work item for transit work item.
        /// </summary>
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
        /// Tracks work item id for transit work item.
        /// </summary>
        internal long WorkItemId { get; }

        /// <summary>
        /// Tracks message id for transit work item.
        /// </summary>
        internal string MessageId { get; }

        /// <summary>
        /// Stores payload for transit work item.
        /// </summary>
        internal byte[] Payload { get; }

        /// <summary>
        /// Stores payload bytes for transit work item.
        /// </summary>
        internal int PayloadBytes { get; }

        /// <summary>
        /// Limits attempt count for transit work item.
        /// </summary>
        internal int AttemptCount { get; private set; }

        /// <summary>
        /// Limits max attempts for transit work item.
        /// </summary>
        internal int MaxAttempts { get; }

        /// <summary>
        /// Tracks first enqueued utc for transit work item.
        /// </summary>
        internal DateTimeOffset FirstEnqueuedUtc { get; }

        /// <summary>
        /// Tracks last enqueued utc for transit work item.
        /// </summary>
        internal DateTimeOffset LastEnqueuedUtc { get; private set; }

        /// <summary>
        /// Tracks last claimed utc for transit work item.
        /// </summary>
        internal DateTimeOffset? LastClaimedUtc { get; private set; }

        /// <summary>
        /// Tracks last failure utc for transit work item.
        /// </summary>
        internal DateTimeOffset? LastFailureUtc { get; private set; }

        /// <summary>
        /// Tracks next eligible utc for transit work item.
        /// </summary>
        internal DateTimeOffset? NextEligibleUtc { get; private set; }

        /// <summary>
        /// Tracks last failure class for transit work item.
        /// </summary>
        internal TransitWorkFailureClass? LastFailureClass { get; private set; }

        /// <summary>
        /// Tracks last transmission uncertainty for transit work item.
        /// </summary>
        internal TransitTransmissionUncertainty? LastTransmissionUncertainty { get; private set; }

        /// <summary>
        /// Tracks state for transit work item.
        /// </summary>
        internal TransitWorkItemState State => (TransitWorkItemState)Volatile.Read(ref _stateValue);

        /// <summary>
        /// Tracks owner connection id for transit work item.
        /// </summary>
        internal string? OwnerConnectionId { get; private set; }

        /// <summary>
        /// Tracks cancel requested for transit work item.
        /// </summary>
        internal bool CancelRequested { get; private set; }

        /// <summary>
        /// Tracks terminal status for transit work item.
        /// </summary>
        internal TransitPublishStatus? TerminalStatus { get; private set; }

        /// <summary>
        /// Tracks terminal provenance for transit work item.
        /// </summary>
        internal TransitPublishProvenance? TerminalProvenance { get; private set; }

        /// <summary>
        /// Tracks last state transition tick for transit work item.
        /// </summary>
        internal long LastStateTransitionTick { get; private set; }

        /// <summary>
        /// Tracks completion task for transit work item.
        /// </summary>
        internal Task<TransitPublishResult> CompletionTask => _completion.Task;

        /// <summary>
        /// Tracks completion for transit work item.
        /// </summary>
        private readonly TaskCompletionSource<TransitPublishResult> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>
        /// Coordinates try mark queued for transit work item.
        /// </summary>
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
        /// Coordinates try revert queued to retry pending for transit work item.
        /// </summary>
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
        /// Coordinates mark queued for transit work item.
        /// </summary>
        internal void MarkQueued(DateTimeOffset utcNow)
        {
            if (!TryMarkQueued(utcNow))
            {
                throw new InvalidOperationException("Cannot queue a terminal transit work item.");
            }
        }

        /// <summary>
        /// Coordinates try mark claimed for transit work item.
        /// </summary>
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
        /// Coordinates mark claimed for transit work item.
        /// </summary>
        internal void MarkClaimed(string connectionId, DateTimeOffset utcNow)
        {
            if (!TryMarkClaimed(connectionId, utcNow))
            {
                throw new InvalidOperationException("Cannot claim a transit work item that is not queued.");
            }
        }

        /// <summary>
        /// Coordinates mark staged for transit work item.
        /// </summary>
        internal void MarkStaged()
        {
            _ = TryTransitionState(TransitWorkItemState.Claimed, TransitWorkItemState.Staged);
        }

        /// <summary>
        /// Coordinates mark flushed for transit work item.
        /// </summary>
        internal void MarkFlushed()
        {
            _ = TryTransitionState(TransitWorkItemState.Staged, TransitWorkItemState.Flushed);
        }

        /// <summary>
        /// Coordinates mark awaiting response for transit work item.
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
        /// Coordinates try move to retry pending for transit work item.
        /// </summary>
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
        /// Coordinates has attempts remaining for transit work item.
        /// </summary>
        internal bool HasAttemptsRemaining()
        {
            return AttemptCount < MaxAttempts;
        }

        /// <summary>
        /// Coordinates try transition to terminal for transit work item.
        /// </summary>
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
        /// Coordinates mark cancel requested for transit work item.
        /// </summary>
        internal void MarkCancelRequested()
        {
            CancelRequested = true;
        }

        /// <summary>
        /// Coordinates try complete for transit work item.
        /// </summary>
        internal bool TryComplete(TransitPublishResult result, TransitPublishProvenance terminalProvenance)
        {
            ArgumentNullException.ThrowIfNull(result);

            return TryTransitionToTerminal(result.Status, terminalProvenance, out _)
                && _completion.TrySetResult(result);
        }

        /// <summary>
        /// Coordinates try complete for transit work item.
        /// </summary>
        internal bool TryComplete(TransitPublishResult result, TransitPublishProvenance terminalProvenance, out TransitWorkItemState priorState)
        {
            ArgumentNullException.ThrowIfNull(result);

            return TryTransitionToTerminal(result.Status, terminalProvenance, out priorState) && _completion.TrySetResult(result);
        }

        /// <summary>
        /// Coordinates try set completion result for transit work item.
        /// </summary>
        internal bool TrySetCompletionResult(TransitPublishResult result)
        {
            ArgumentNullException.ThrowIfNull(result);
            return _completion.TrySetResult(result);
        }

        /// <summary>
        /// Coordinates try transition state for transit work item.
        /// </summary>
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
        /// Coordinates is terminal state for transit work item.
        /// </summary>
        private static bool IsTerminalState(TransitWorkItemState state)
        {
            return state is TransitWorkItemState.CompletedAccepted
                or TransitWorkItemState.CompletedRejected
                or TransitWorkItemState.CompletedCanceled
                or TransitWorkItemState.CompletedFailed;
        }

        /// <summary>
        /// Coordinates map terminal state for transit work item.
        /// </summary>
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
        /// Tracks is terminal for transit work item.
        /// </summary>
        internal bool IsTerminal => Volatile.Read(ref _terminalCompletionObserved) == 1;
    }

    /// <summary>
    /// Defines transit work item state and its transit work item contract.
    /// </summary>
    internal enum TransitWorkItemState
    {
        Queued = 0,
        Claimed = 1,
        Staged = 2,
        Flushed = 3,
        AwaitingResponse = 4,
        RetryPending = 5,
        CompletedAccepted = 6,
        CompletedRejected = 7,
        CompletedFailed = 8,
        CompletedCanceled = 9,
    }

    /// <summary>
    /// Defines transit work failure class and its transit work item contract.
    /// </summary>
    internal enum TransitWorkFailureClass
    {
        ConnectionReset = 0,
        ConnectionDisposed = 1,
        WriteFailure = 2,
        FlushFailure = 3,
        ResponseReadFailure = 4,
        ResponseLost = 5,
        LocalValidationFailure = 6,
        PermanentProtocolRejection = 7,
        Cancellation = 8,
        ShutdownDeadline = 9,
        Unknown = 10,
    }

    /// <summary>
    /// Defines transit transmission uncertainty and its transit work item contract.
    /// </summary>
    internal enum TransitTransmissionUncertainty
    {
        DefinitelyNotSent = 0,
        SentResponseLost = 1,
        ConnectionFailedDuringSend = 2,
        Retrying = 3,
    }
}
