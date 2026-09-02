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
        private int _terminalCompletionObserved;
        private int _stateValue = (int)TransitWorkItemState.Queued;

        /// <summary>
        /// Handles transit work item for transit work item.
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
        internal long WorkItemId { get; }
        internal string MessageId { get; }
        internal byte[] Payload { get; }
        internal int PayloadBytes { get; }

        /// <summary>
        /// Limits attempt count for transit work item.
        /// </summary>
        internal int AttemptCount { get; private set; }

        /// <summary>
        /// Limits max attempts for transit work item.
        /// </summary>
        internal int MaxAttempts { get; }
        internal DateTimeOffset FirstEnqueuedUtc { get; }
        internal DateTimeOffset LastEnqueuedUtc { get; private set; }
        internal DateTimeOffset? LastClaimedUtc { get; private set; }
        internal DateTimeOffset? LastFailureUtc { get; private set; }
        internal DateTimeOffset? NextEligibleUtc { get; private set; }
        internal TransitWorkFailureClass? LastFailureClass { get; private set; }
        internal TransitTransmissionUncertainty? LastTransmissionUncertainty { get; private set; }
        internal TransitWorkItemState State => (TransitWorkItemState)Volatile.Read(ref _stateValue);
        internal string? OwnerConnectionId { get; private set; }
        internal bool CancelRequested { get; private set; }
        internal TransitPublishStatus? TerminalStatus { get; private set; }
        internal TransitPublishProvenance? TerminalProvenance { get; private set; }
        internal long LastStateTransitionTick { get; private set; }
        internal Task<TransitPublishResult> CompletionTask => _completion.Task;
        private readonly TaskCompletionSource<TransitPublishResult> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>
        /// Handles try mark queued for transit work item.
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
        /// Handles try revert queued to retry pending for transit work item.
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
        /// Handles mark queued for transit work item.
        /// </summary>
        internal void MarkQueued(DateTimeOffset utcNow)
        {
            if (!TryMarkQueued(utcNow))
            {
                throw new InvalidOperationException("Cannot queue a terminal transit work item.");
            }
        }

        /// <summary>
        /// Handles try mark claimed for transit work item.
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
        /// Handles mark claimed for transit work item.
        /// </summary>
        internal void MarkClaimed(string connectionId, DateTimeOffset utcNow)
        {
            if (!TryMarkClaimed(connectionId, utcNow))
            {
                throw new InvalidOperationException("Cannot claim a transit work item that is not queued.");
            }
        }

        /// <summary>
        /// Handles mark staged for transit work item.
        /// </summary>
        internal void MarkStaged()
        {
            _ = TryTransitionState(TransitWorkItemState.Claimed, TransitWorkItemState.Staged);
        }

        /// <summary>
        /// Handles mark flushed for transit work item.
        /// </summary>
        internal void MarkFlushed()
        {
            _ = TryTransitionState(TransitWorkItemState.Staged, TransitWorkItemState.Flushed);
        }

        /// <summary>
        /// Handles mark awaiting response for transit work item.
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
        /// Handles try move to retry pending for transit work item.
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
        /// Handles has attempts remaining for transit work item.
        /// </summary>
        internal bool HasAttemptsRemaining()
        {
            return AttemptCount < MaxAttempts;
        }

        /// <summary>
        /// Handles try transition to terminal for transit work item.
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
        /// Handles mark cancel requested for transit work item.
        /// </summary>
        internal void MarkCancelRequested()
        {
            CancelRequested = true;
        }

        /// <summary>
        /// Handles try complete for transit work item.
        /// </summary>
        internal bool TryComplete(TransitPublishResult result, TransitPublishProvenance terminalProvenance)
        {
            ArgumentNullException.ThrowIfNull(result);

            return TryTransitionToTerminal(result.Status, terminalProvenance, out _)
                && _completion.TrySetResult(result);
        }

        /// <summary>
        /// Handles try complete for transit work item.
        /// </summary>
        internal bool TryComplete(TransitPublishResult result, TransitPublishProvenance terminalProvenance, out TransitWorkItemState priorState)
        {
            ArgumentNullException.ThrowIfNull(result);

            return TryTransitionToTerminal(result.Status, terminalProvenance, out priorState) && _completion.TrySetResult(result);
        }

        /// <summary>
        /// Handles try set completion result for transit work item.
        /// </summary>
        internal bool TrySetCompletionResult(TransitPublishResult result)
        {
            ArgumentNullException.ThrowIfNull(result);
            return _completion.TrySetResult(result);
        }

        /// <summary>
        /// Handles try transition state for transit work item.
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
        /// Handles is terminal state for transit work item.
        /// </summary>
        private static bool IsTerminalState(TransitWorkItemState state)
        {
            return state is TransitWorkItemState.CompletedAccepted
                or TransitWorkItemState.CompletedRejected
                or TransitWorkItemState.CompletedCanceled
                or TransitWorkItemState.CompletedFailed;
        }

        /// <summary>
        /// Handles map terminal state for transit work item.
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

