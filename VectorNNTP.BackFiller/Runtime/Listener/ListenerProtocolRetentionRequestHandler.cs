// <copyright file="ListenerProtocolRetentionRequestHandler.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Listener
// Retention-backed GetRequest handler that acquires read leases by MessageIdMd5 and coordinates completion ownership with session lifecycle callbacks.

using System.Collections.Concurrent;
using System.Text;
using VectorNNTP.Backfiller.Runtime.Articles.Retention;

namespace VectorNNTP.Backfiller.Runtime.Listener
{
    /// <summary>
    /// Handles Listener GetRequest retrieval using the shared article retention authority.
    /// </summary>
    /// <remarks>
    /// <para>For acquired entries this handler returns the retained lease payload memory without copying article bytes.</para>
    /// <para>Retention listener completion is marked only after a successful Found transfer and a correlated receipt acknowledgement.</para>
    /// <para>Lease ownership remains with this handler until terminal lifecycle callbacks release the request state.</para>
    /// </remarks>
    internal sealed class ListenerProtocolRetentionRequestHandler : IListenerProtocolRequestHandler
    {
        private readonly IArticleRetentionAuthority _retentionAuthority;
        private readonly ConcurrentDictionary<uint, PendingFoundRequest> _pendingFoundRequests = new();

        /// <summary>
        /// Initializes a new retention-backed listener request handler.
        /// </summary>
        /// <param name="retentionAuthority">Shared retention authority used for MD5 lookup, read leases, and listener completion.</param>
        internal ListenerProtocolRetentionRequestHandler(IArticleRetentionAuthority retentionAuthority)
        {
            _retentionAuthority = retentionAuthority ?? throw new ArgumentNullException(nameof(retentionAuthority));
        }

        /// <inheritdoc/>
        public ValueTask<ListenerSessionRequestDispatchResult> HandleGetRequestAsync(
            uint requestId,
            ReadOnlyMemory<byte> messageIdMd5Payload,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            string messageIdMd5 = Encoding.ASCII.GetString(messageIdMd5Payload.Span);
            ArticleRetentionReadLeaseResult leaseResult = _retentionAuthority.TryAcquireReadLeaseByMessageIdMd5(messageIdMd5);
            if (!leaseResult.IsAcquired || leaseResult.Lease is not IArticleRetentionReadLease lease)
            {
                return ValueTask.FromResult(ListenerSessionRequestDispatchResult.NotFound());
            }

            PendingFoundRequest pending = new(lease);
            if (!_pendingFoundRequests.TryAdd(requestId, pending))
            {
                pending.DisposeLease();
                return ValueTask.FromResult(ListenerSessionRequestDispatchResult.Error(ListenerProtocolErrorCode.InternalError));
            }

            return ValueTask.FromResult(ListenerSessionRequestDispatchResult.Found(lease.Payload));
        }

        /// <summary>
        /// Observes terminal transport status for one Found response transfer.
        /// </summary>
        /// <param name="transferEvent">Found transfer terminal status correlated by RequestId.</param>
        internal void OnFoundTransferTerminal(ListenerFoundTransferEvent transferEvent)
        {
            if (!_pendingFoundRequests.TryGetValue(transferEvent.RequestId, out PendingFoundRequest? pending))
            {
                return;
            }

            bool shouldMarkCompletion = pending.RegisterFoundTransferTerminal(transferEvent.Status);
            if (shouldMarkCompletion)
            {
                MarkListenerCompletedAndRelease(transferEvent.RequestId, pending);
                return;
            }

            if (transferEvent.Status != ListenerTransferCompletionStatus.Completed)
            {
                ReleasePendingRequest(transferEvent.RequestId, pending);
            }
        }

        /// <summary>
        /// Observes one correlated receipt acknowledgement for a tracked request.
        /// </summary>
        /// <param name="receiptAckEvent">Receipt acknowledgement event correlated by RequestId.</param>
        internal void OnReceiptAcknowledged(ListenerReceiptAckEvent receiptAckEvent)
        {
            if (!_pendingFoundRequests.TryGetValue(receiptAckEvent.RequestId, out PendingFoundRequest? pending))
            {
                return;
            }

            bool shouldMarkCompletion = pending.RegisterReceiptAcknowledged();
            if (shouldMarkCompletion)
            {
                MarkListenerCompletedAndRelease(receiptAckEvent.RequestId, pending);
            }
        }

        /// <summary>
        /// Releases any tracked lease state when the session terminalizes a request.
        /// </summary>
        /// <param name="requestId">Terminalized request identifier.</param>
        internal void OnRequestTerminalized(uint requestId)
        {
            if (_pendingFoundRequests.TryRemove(requestId, out PendingFoundRequest? pending))
            {
                pending.DisposeLease();
            }
        }

        private void MarkListenerCompletedAndRelease(uint requestId, PendingFoundRequest pending)
        {
            if (!pending.TryMarkListenerCompleted())
            {
                return;
            }

            _ = _retentionAuthority.MarkListenerCompleted(pending.MessageId);
            ReleasePendingRequest(requestId, pending);
        }

        private void ReleasePendingRequest(uint requestId, PendingFoundRequest pending)
        {
            if (_pendingFoundRequests.TryRemove(requestId, out PendingFoundRequest? removed))
            {
                removed.DisposeLease();
                return;
            }

            pending.DisposeLease();
        }

        private sealed class PendingFoundRequest(IArticleRetentionReadLease lease)
        {
            private readonly object _sync = new();
            private int _leaseDisposed;
            private bool _receiptAcknowledged;
            private bool _foundTransferCompleted;
            private bool _listenerCompleted;

            public string MessageId { get; } = lease.MessageId;

            public bool RegisterReceiptAcknowledged()
            {
                lock (_sync)
                {
                    _receiptAcknowledged = true;
                    return _foundTransferCompleted && !_listenerCompleted;
                }
            }

            public bool RegisterFoundTransferTerminal(ListenerTransferCompletionStatus status)
            {
                lock (_sync)
                {
                    if (status != ListenerTransferCompletionStatus.Completed)
                    {
                        return false;
                    }

                    _foundTransferCompleted = true;
                    return _receiptAcknowledged && !_listenerCompleted;
                }
            }

            public bool TryMarkListenerCompleted()
            {
                lock (_sync)
                {
                    if (_listenerCompleted)
                    {
                        return false;
                    }

                    _listenerCompleted = true;
                    return true;
                }
            }

            public void DisposeLease()
            {
                if (Interlocked.Exchange(ref _leaseDisposed, 1) == 0)
                {
                    lease.Dispose();
                }
            }
        }
    }
}
