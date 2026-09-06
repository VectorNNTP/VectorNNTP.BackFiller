// <copyright file="ListenerProtocolSessionContracts.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Listener
// Per-connection protocol session contracts for request dispatch and lifecycle tracking.

namespace VectorNNTP.Backfiller.Runtime.Listener
{
    /// <summary>
    /// Defines lifecycle states for one connected Listener protocol session.
    /// </summary>
    internal enum ListenerProtocolSessionState
    {
        /// <summary>
        /// Session is admitting and processing inbound protocol traffic.
        /// </summary>
        Running = 0,

        /// <summary>
        /// Session has begun graceful shutdown and no longer admits new requests.
        /// </summary>
        GracefulShutdown = 1,

        /// <summary>
        /// Session is in forced shutdown and cancellation has been signaled for pending work.
        /// </summary>
        ForcedShutdown = 2,

        /// <summary>
        /// Session has completed and released owned resources.
        /// </summary>
        Completed = 3,
    }

    /// <summary>
    /// Defines terminal transfer outcomes observed by the session writer.
    /// </summary>
    internal enum ListenerTransferCompletionStatus
    {
        /// <summary>
        /// Full frame payload was written to the transport.
        /// </summary>
        Completed = 0,

        /// <summary>
        /// Transfer did not finish because cancellation was observed.
        /// </summary>
        Cancelled = 1,

        /// <summary>
        /// Transfer stopped due to transport failure.
        /// </summary>
        Failed = 2,

        /// <summary>
        /// Transfer stopped before completion due to shutdown.
        /// </summary>
        Incomplete = 3,
    }

    /// <summary>
    /// Correlation lifecycle for one outstanding RequestId within a connection session.
    /// </summary>
    internal enum ListenerRequestState
    {
        /// <summary>
        /// Request is tracked but has not started handler execution.
        /// </summary>
        Queued = 0,

        /// <summary>
        /// Request handler execution has started.
        /// </summary>
        Processing = 1,

        /// <summary>
        /// Response transfer completed and session is awaiting receipt acknowledgement.
        /// </summary>
        AwaitingReceiptAck = 2,

        /// <summary>
        /// Request reached terminal completion and may be removed from outstanding tracking.
        /// </summary>
        Terminal = 3,
    }

    /// <summary>
    /// Result contract returned by request dispatch handlers for inbound GetRequest operations.
    /// </summary>
    /// <param name="Kind">Dispatch category consumed by the session writer to choose Found, NotFound, or Error wire-response handling.</param>
    /// <param name="FoundPayload">Payload memory returned to the session only when <paramref name="Kind"/> is <see cref="ListenerSessionRequestDispatchKind.Found"/>; ignored for other kinds. The handler must keep this memory valid until Found transfer terminalization callbacks have completed.</param>
    /// <param name="ErrorCode">Protocol error code used only when <paramref name="Kind"/> is <see cref="ListenerSessionRequestDispatchKind.Error"/>; ignored for Found and NotFound responses.</param>
    /// <remarks>
    /// This contract intentionally excludes retention completion semantics; it only describes which wire response
    /// the session should send and whether receipt acknowledgement tracking is required.
    /// </remarks>
    internal readonly record struct ListenerSessionRequestDispatchResult(
        ListenerSessionRequestDispatchKind Kind,
        ReadOnlyMemory<byte> FoundPayload,
        ListenerProtocolErrorCode ErrorCode)
    {
        /// <summary>
        /// Creates a Found dispatch result that carries article payload memory.
        /// </summary>
        internal static ListenerSessionRequestDispatchResult Found(ReadOnlyMemory<byte> payload)
        {
            return new ListenerSessionRequestDispatchResult(ListenerSessionRequestDispatchKind.Found, payload, default);
        }

        /// <summary>
        /// Creates a NotFound dispatch result.
        /// </summary>
        internal static ListenerSessionRequestDispatchResult NotFound()
        {
            return new ListenerSessionRequestDispatchResult(ListenerSessionRequestDispatchKind.NotFound, ReadOnlyMemory<byte>.Empty, default);
        }

        /// <summary>
        /// Creates an Error dispatch result.
        /// </summary>
        internal static ListenerSessionRequestDispatchResult Error(ListenerProtocolErrorCode errorCode)
        {
            return new ListenerSessionRequestDispatchResult(ListenerSessionRequestDispatchKind.Error, ReadOnlyMemory<byte>.Empty, errorCode);
        }
    }

    /// <summary>
    /// Dispatch result category for inbound GetRequest operations.
    /// </summary>
    internal enum ListenerSessionRequestDispatchKind
    {
        /// <summary>
        /// A payload is available and should be returned via GetResponseFound.
        /// </summary>
        Found = 0,

        /// <summary>
        /// Requested article cannot currently be served and should return GetResponseNotFound.
        /// </summary>
        NotFound = 1,

        /// <summary>
        /// Request should return GetResponseError with supplied protocol error code.
        /// </summary>
        Error = 2,
    }

    /// <summary>
    /// Handles accepted GetRequest operations for a Listener protocol session.
    /// </summary>
    /// <remarks>
    /// Implementations may perform asynchronous work and return a deterministic wire response contract.
    /// Handler implementations must not write directly to the transport.
    /// </remarks>
    internal interface IListenerProtocolRequestHandler
    {
        /// <summary>
        /// Handles one validated GetRequest payload.
        /// </summary>
        /// <param name="requestId">Request correlation identifier for the current connection.</param>
        /// <param name="messageIdMd5Payload">Validated 32-byte canonical MessageIdMd5 payload bytes.</param>
        /// <param name="cancellationToken">Connection/session cancellation token.</param>
        /// <returns>
        /// A deterministic dispatch result that instructs the session which protocol response to emit.
        /// </returns>
        public abstract ValueTask<ListenerSessionRequestDispatchResult> HandleGetRequestAsync(
            uint requestId,
            ReadOnlyMemory<byte> messageIdMd5Payload,
            CancellationToken cancellationToken);
    }

    /// <summary>
    /// Session event emitted when a Found response transfer reaches a terminal transport state.
    /// </summary>
    /// <param name="RequestId">Correlated request identifier.</param>
    /// <param name="Status">Terminal transfer status.</param>
    internal readonly record struct ListenerFoundTransferEvent(
        uint RequestId,
        ListenerTransferCompletionStatus Status);

    /// <summary>
    /// Session event emitted when a receipt acknowledgement frame is correlated to a request.
    /// </summary>
    /// <param name="RequestId">Correlated request identifier.</param>
    internal readonly record struct ListenerReceiptAckEvent(uint RequestId);
}
