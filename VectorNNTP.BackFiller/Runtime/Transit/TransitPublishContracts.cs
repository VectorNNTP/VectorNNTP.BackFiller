// <copyright file="TransitPublishContracts.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Transit
// Implements the transit publish contracts behavior.

namespace VectorNNTP.Backfiller.Runtime.Transit
{
    /// <summary>
    /// Terminal and intermediate status values for one outbound transit publish submission.
    /// </summary>
    internal enum TransitPublishStatus
    {
        /// <summary>
        /// The remote server accepted the article.
        /// </summary>
        Accepted,

        /// <summary>
        /// The remote server definitively rejected the article.
        /// </summary>
        Rejected,

        /// <summary>
        /// The submission has been accepted into local buffering but not yet terminally settled.
        /// </summary>
        Queued,

        /// <summary>
        /// The publish path was unavailable before the article could be admitted to a live connection.
        /// </summary>
        Unavailable,

        /// <summary>
        /// The submission exhausted retry budget or failed due to a local non-ambiguous error.
        /// </summary>
        Failed,

        /// <summary>
        /// The transport lost definitive knowledge of the article outcome.
        /// </summary>
        Ambiguous,

        /// <summary>
        /// The caller canceled the submission before a definitive terminal result was returned.
        /// </summary>
        Canceled,
    }

    /// <summary>
    /// Classifies the production-path origin of a publish result's terminalization.
    /// </summary>
    internal enum TransitPublishProvenance
    {
        /// <summary>
        /// Terminalization originated from a <c>400</c> server response.
        /// </summary>
        Response400,

        /// <summary>
        /// Terminalization originated from a response-loop failure before definitive settlement.
        /// </summary>
        ResponseLoopFailure,

        /// <summary>
        /// Terminalization originated from connection closure before definitive settlement.
        /// </summary>
        ConnectionClose,

        /// <summary>
        /// Terminalization originated while draining queued completions.
        /// </summary>
        QueuedWriteDrain,

        /// <summary>
        /// Terminalization originated during shutdown handling.
        /// </summary>
        Shutdown,

        /// <summary>
        /// Terminalization originated from submission preemption or caller cancellation handling.
        /// </summary>
        Preemption,

        /// <summary>
        /// Terminalization originated from an explicit cancellation path.
        /// </summary>
        Cancellation,

        /// <summary>
        /// Terminalization originated from a timeout path.
        /// </summary>
        Timeout,

        /// <summary>
        /// Terminalization originated because the publish path was unavailable.
        /// </summary>
        Unavailable,

        /// <summary>
        /// Terminalization originated from a non-ambiguous failure path.
        /// </summary>
        Failed,

        /// <summary>
        /// No more specific provenance classification was available.
        /// </summary>
        OtherOrUnknown,
    }

    /// <summary>
    /// Immutable publish result returned for each submission attempt.
    /// </summary>
    /// <param name="MessageId">Article Message-ID metadata.</param>
    /// <param name="Status">Submission outcome status.</param>
    /// <param name="ResponseCode">NNTP response code when available.</param>
    /// <param name="ResponseText">Sanitized protocol response text when available.</param>
    /// <param name="T0PublishAsyncEnterTick">Tick when <c>PublishAsync</c> was entered.</param>
    /// <param name="T1DispatcherAssignedTick">Tick when dispatcher assigned the submission to a connection slot.</param>
    /// <param name="T2SocketWriteBeginTick">Tick when socket write staging began.</param>
    /// <param name="T3SocketWriteEndTick">Tick when socket write staging completed.</param>
    /// <param name="T4ResponseAvailableTick">Tick when response bytes became available for parsing.</param>
    /// <param name="T5ResponseParsedTick">Tick when response parsing completed.</param>
    /// <param name="T6ResponseCorrelatedTick">Tick when the parsed response was correlated to pending submission state.</param>
    /// <param name="T7PublishAsyncCompleteTick">Tick when the caller-facing publish API completed.</param>
    /// <param name="Provenance">Root production-path provenance for this terminalization.</param>
    /// <param name="ProvenanceConnectionId">Connection identifier associated with the provenance when available.</param>
    /// <param name="ProvenanceConnectionState">Connection state associated with the provenance when available.</param>
    /// <param name="ProvenanceSlotIndex">Publisher slot index associated with the provenance when available.</param>
    /// <param name="ProvenanceTick">Timestamp tick captured at the provenance origin when available.</param>
    internal sealed record TransitPublishResult(
        string MessageId,
        TransitPublishStatus Status,
        int? ResponseCode,
        string? ResponseText,
        long T0PublishAsyncEnterTick = 0,
        long T1DispatcherAssignedTick = 0,
        long T2SocketWriteBeginTick = 0,
        long T3SocketWriteEndTick = 0,
        long T4ResponseAvailableTick = 0,
        long T5ResponseParsedTick = 0,
        long T6ResponseCorrelatedTick = 0,
        long T7PublishAsyncCompleteTick = 0,
        TransitPublishProvenance Provenance = TransitPublishProvenance.OtherOrUnknown,
        string? ProvenanceConnectionId = null,
        TransitConnectionState? ProvenanceConnectionState = null,
        int? ProvenanceSlotIndex = null,
        long ProvenanceTick = 0);

    /// <summary>
    /// Immutable transit publish request payload.
    /// </summary>
    /// <param name="MessageId">Article Message-ID used for TAKETHIS framing and response correlation.</param>
    /// <param name="ArticlePayload">Opaque binary article payload that must remain byte-for-byte intact.</param>
    internal sealed record TransitPublishRequest(
        string MessageId,
        ReadOnlyMemory<byte> ArticlePayload);

    /// <summary>
    /// Capability flags discovered during NNTP <c>CAPABILITIES</c> negotiation.
    /// </summary>
    /// <param name="SupportsStartTls"><see langword="true"/> when the server advertises STARTTLS.</param>
    /// <param name="SupportsStreaming"><see langword="true"/> when the server advertises STREAMING or MODE STREAM support.</param>
    internal sealed record TransitCapabilitySnapshot(
        bool SupportsStartTls,
        bool SupportsStreaming);

    /// <summary>
    /// Lifecycle states for a single outbound transit connection.
    /// </summary>
    internal enum TransitConnectionState
    {
        /// <summary>
        /// No active transport exists.
        /// </summary>
        Disconnected,

        /// <summary>
        /// TCP connection establishment is in progress.
        /// </summary>
        Connecting,

        /// <summary>
        /// The transport is connected and the greeting line is being awaited.
        /// </summary>
        AwaitingGreeting,

        /// <summary>
        /// CAPABILITIES negotiation is in progress.
        /// </summary>
        CapabilitiesNegotiation,

        /// <summary>
        /// STARTTLS negotiation or immediate TLS activation is in progress.
        /// </summary>
        StartingTls,

        /// <summary>
        /// TLS has been established for the transport.
        /// </summary>
        TlsEstablished,

        /// <summary>
        /// MODE STREAM activation is in progress.
        /// </summary>
        StartingStreaming,

        /// <summary>
        /// The connection is ready to publish TAKETHIS frames.
        /// </summary>
        Ready,

        /// <summary>
        /// The connection is actively publishing work.
        /// </summary>
        Publishing,

        /// <summary>
        /// Shutdown and resource teardown are in progress.
        /// </summary>
        Disconnecting,

        /// <summary>
        /// The connection has faulted and cannot safely continue publishing.
        /// </summary>
        Faulted,
    }

    /// <summary>
    /// Aggregate transport counters captured for periodic runtime reporting.
    /// </summary>
    /// <param name="TotalBytesTransmitted">Total bytes written to all transport streams.</param>
    /// <param name="TotalBytesReceived">Total bytes read from all transport streams.</param>
    /// <param name="TotalArticlesSubmitted">Total articles admitted for publish processing.</param>
    /// <param name="TotalArticlesAccepted">Total articles definitively accepted by remote servers.</param>
    /// <param name="TotalArticlesRejected">Total articles definitively rejected by remote servers.</param>
    /// <param name="TotalArticlesAmbiguous">Total articles whose outcome became ambiguous.</param>
    /// <param name="TotalReconnects">Total connection replacement attempts recorded by the publisher.</param>
    /// <param name="ActiveConnections">Current number of active connection instances.</param>
    /// <param name="OutstandingSubmissions">Current number of queued or in-flight submissions.</param>
    internal sealed record TransitTransportSnapshot(
        long TotalBytesTransmitted,
        long TotalBytesReceived,
        long TotalArticlesSubmitted,
        long TotalArticlesAccepted,
        long TotalArticlesRejected,
        long TotalArticlesAmbiguous,
        long TotalReconnects,
        int ActiveConnections,
        int OutstandingSubmissions);
}
