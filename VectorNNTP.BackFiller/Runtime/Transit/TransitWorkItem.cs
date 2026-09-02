// <copyright file="TransitWorkItem.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Transit
// Implements the transit work item responsibilities for this subsystem boundary.

using System.Diagnostics;

namespace VectorNNTP.Backfiller.Runtime.Transit
{
    /// <summary>
    /// Represents one outbound transit article owned by producer, global queue, or a connection until terminal completion.
    /// </summary>
    internal sealed class TransitWorkItem
    {
        /// <summary>
        /// Stores the terminal completion observed state used to enforce this component's runtime contract.
        /// </summary>
        private int _terminalCompletionObserved;
        /// <summary>
        /// Stores the state value state used to enforce this component's runtime contract.
        /// </summary>
        private int _stateValue = (int)TransitWorkItemState.Queued;

        /// <summary>
        /// Stores the max attempts state used to enforce this component's runtime contract.
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
        /// Stores the work item id state used to enforce this component's runtime contract.
        /// </summary>
        internal long WorkItemId { get; }

        /// <summary>
        /// Stores the message id state used to enforce this component's runtime contract.
        /// </summary>
        internal string MessageId { get; }

        /// <summary>
        /// Stores the payload state used to enforce this component's runtime contract.
        /// </summary>
        internal byte[] Payload { get; }

        /// <summary>
        /// Stores the payload bytes state used to enforce this component's runtime contract.
        /// </summary>
        internal int PayloadBytes { get; }

        /// <summary>
        /// Stores the attempt count state used to enforce this component's runtime contract.
        /// </summary>
        internal int AttemptCount { get; private set; }

        /// <summary>
        /// Stores the max attempts state used to enforce this component's runtime contract.
        /// </summary>
        internal int MaxAttempts { get; }

        /// <summary>
        /// Stores the first enqueued utc state used to enforce this component's runtime contract.
        /// </summary>
        internal DateTimeOffset FirstEnqueuedUtc { get; }

        /// <summary>
        /// Stores the last enqueued utc state used to enforce this component's runtime contract.
        /// </summary>
        internal DateTimeOffset LastEnqueuedUtc { get; private set; }

        /// <summary>
        /// Stores the last claimed utc state used to enforce this component's runtime contract.
        /// </summary>
        internal DateTimeOffset? LastClaimedUtc { get; private set; }

        /// <summary>
        /// Stores the last failure utc state used to enforce this component's runtime contract.
        /// </summary>
        internal DateTimeOffset? LastFailureUtc { get; private set; }

        /// <summary>
        /// Stores the next eligible utc state used to enforce this component's runtime contract.
        /// </summary>
        internal DateTimeOffset? NextEligibleUtc { get; private set; }

        /// <summary>
        /// Stores the last failure class state used to enforce this component's runtime contract.
        /// </summary>
        internal TransitWorkFailureClass? LastFailureClass { get; private set; }

        /// <summary>
        /// Stores the last transmission uncertainty state used to enforce this component's runtime contract.
        /// </summary>
        internal TransitTransmissionUncertainty? LastTransmissionUncertainty { get; private set; }

        /// <summary>
        /// Stores the state state used to enforce this component's runtime contract.
        /// </summary>
        internal TransitWorkItemState State => (TransitWorkItemState)Volatile.Read(ref _stateValue);

        /// <summary>
        /// Stores the owner connection id state used to enforce this component's runtime contract.
        /// </summary>
        internal string? OwnerConnectionId { get; private set; }

        /// <summary>
        /// Stores the cancel requested state used to enforce this component's runtime contract.
        /// </summary>
        internal bool CancelRequested { get; private set; }

        /// <summary>
        /// Stores the terminal status state used to enforce this component's runtime contract.
        /// </summary>
        internal TransitPublishStatus? TerminalStatus { get; private set; }

        /// <summary>
        /// Stores the terminal provenance state used to enforce this component's runtime contract.
        /// </summary>
        internal TransitPublishProvenance? TerminalProvenance { get; private set; }

        /// <summary>
        /// Stores the last state transition tick state used to enforce this component's runtime contract.
        /// </summary>
        internal long LastStateTransitionTick { get; private set; }

        /// <summary>
        /// Stores the completion task state used to enforce this component's runtime contract.
        /// </summary>
        internal Task<TransitPublishResult> CompletionTask => _completion.Task;

        /// <summary>
        /// Stores the completion state used to enforce this component's runtime contract.
        /// </summary>
        private readonly TaskCompletionSource<TransitPublishResult> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>
        /// Performs the try mark queued operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the try revert queued to retry pending operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the mark queued operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        internal void MarkQueued(DateTimeOffset utcNow)
        {
            if (!TryMarkQueued(utcNow))
            {
                throw new InvalidOperationException("Cannot queue a terminal transit work item.");
            }
        }

        /// <summary>
        /// Performs the try mark claimed operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the mark claimed operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        internal void MarkClaimed(string connectionId, DateTimeOffset utcNow)
        {
            if (!TryMarkClaimed(connectionId, utcNow))
            {
                throw new InvalidOperationException("Cannot claim a transit work item that is not queued.");
            }
        }

        /// <summary>
        /// Performs the mark staged operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        internal void MarkStaged()
        {
            _ = TryTransitionState(TransitWorkItemState.Claimed, TransitWorkItemState.Staged);
        }

        /// <summary>
        /// Performs the mark flushed operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        internal void MarkFlushed()
        {
            _ = TryTransitionState(TransitWorkItemState.Staged, TransitWorkItemState.Flushed);
        }

        /// <summary>
        /// Performs the mark awaiting response operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the try move to retry pending operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the has attempts remaining operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        internal bool HasAttemptsRemaining()
        {
            return AttemptCount < MaxAttempts;
        }

        /// <summary>
        /// Performs the try transition to terminal operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the mark cancel requested operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        internal void MarkCancelRequested()
        {
            CancelRequested = true;
        }

        /// <summary>
        /// Performs the try complete operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        internal bool TryComplete(TransitPublishResult result, TransitPublishProvenance terminalProvenance)
        {
            ArgumentNullException.ThrowIfNull(result);

            return TryTransitionToTerminal(result.Status, terminalProvenance, out _)
                && _completion.TrySetResult(result);
        }

        /// <summary>
        /// Performs the try complete operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        internal bool TryComplete(TransitPublishResult result, TransitPublishProvenance terminalProvenance, out TransitWorkItemState priorState)
        {
            ArgumentNullException.ThrowIfNull(result);

            return TryTransitionToTerminal(result.Status, terminalProvenance, out priorState) && _completion.TrySetResult(result);
        }

        /// <summary>
        /// Performs the try set completion result operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        internal bool TrySetCompletionResult(TransitPublishResult result)
        {
            ArgumentNullException.ThrowIfNull(result);
            return _completion.TrySetResult(result);
        }

        /// <summary>
        /// Performs the try transition state operation while preserving this component's lifecycle and state contracts.
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
        /// Performs the is terminal state operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static bool IsTerminalState(TransitWorkItemState state)
        {
            return state is TransitWorkItemState.CompletedAccepted
                or TransitWorkItemState.CompletedRejected
                or TransitWorkItemState.CompletedCanceled
                or TransitWorkItemState.CompletedFailed;
        }

        /// <summary>
        /// Performs the map terminal state operation while preserving this component's lifecycle and state contracts.
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
        /// Stores the is terminal state used to enforce this component's runtime contract.
        /// </summary>
        internal bool IsTerminal => Volatile.Read(ref _terminalCompletionObserved) == 1;
    }

    /// <summary>
    /// Defines the transit work item state component and its contracts for this subsystem.
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
    /// Defines the transit work failure class component and its contracts for this subsystem.
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
    /// Defines the transit transmission uncertainty component and its contracts for this subsystem.
    /// </summary>
    internal enum TransitTransmissionUncertainty
    {
        DefinitelyNotSent = 0,
        SentResponseLost = 1,
        ConnectionFailedDuringSend = 2,
        Retrying = 3,
    }
}
