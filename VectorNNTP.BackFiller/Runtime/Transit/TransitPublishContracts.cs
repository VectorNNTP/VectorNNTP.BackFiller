// <copyright file="TransitPublishContracts.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Transit
// Implements the transit publish contracts behavior.

namespace VectorNNTP.Backfiller.Runtime.Transit
{
    /// <summary>
    /// Result status for an outbound NNTP Transit publish submission.
    /// </summary>
    internal enum TransitPublishStatus
    {
        Accepted,
        Rejected,
        Queued,
        Unavailable,
        Failed,
        Ambiguous,
        Canceled,
    }

    /// <summary>
    /// Defines transit publish provenance and its transit publish contracts contract.
    /// </summary>
    internal enum TransitPublishProvenance
    {
        Response400,
        ResponseLoopFailure,
        ConnectionClose,
        QueuedWriteDrain,
        Shutdown,
        Preemption,
        Cancellation,
        Timeout,
        Unavailable,
        Failed,
        OtherOrUnknown,
    }

    /// <summary>
    /// Immutable publish result returned to callers for each submission.
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
    /// <param name="T6ResponseCorrelatedTick">Tick when parsed response was correlated to a pending submission.</param>
    /// <param name="T7PublishAsyncCompleteTick">Tick when <c>PublishAsync</c> completed for the caller.</param>
    /// <param name="Provenance">Root production-path provenance for this terminalization.</param>
    /// <param name="ProvenanceConnectionId">Connection id associated with provenance when available.</param>
    /// <param name="ProvenanceConnectionState">Connection state associated with provenance when available.</param>
    /// <param name="ProvenanceSlotIndex">Publisher slot index associated with provenance when available.</param>
    /// <param name="ProvenanceTick">Timestamp tick captured at provenance origin when available.</param>
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
    /// Publish request metadata and byte payload contract.
    /// </summary>
    /// <param name="MessageId">Article Message-ID used for TAKETHIS and response correlation.</param>
    /// <param name="ArticlePayload">Opaque binary article payload. Payload bytes must never be converted to string.</param>
    internal sealed record TransitPublishRequest(
        string MessageId,
        ReadOnlyMemory<byte> ArticlePayload);

    /// <summary>
    /// Capability snapshot discovered during NNTP CAPABILITIES negotiation.
    /// </summary>
    /// <param name="SupportsStartTls">Whether STARTTLS is advertised.</param>
    /// <param name="SupportsStreaming">Whether STREAMING/MODE STREAM is advertised.</param>
    internal sealed record TransitCapabilitySnapshot(
        bool SupportsStartTls,
        bool SupportsStreaming);

    /// <summary>
    /// Transit connection protocol state.
    /// </summary>
    internal enum TransitConnectionState
    {
        Disconnected,
        Connecting,
        AwaitingGreeting,
        CapabilitiesNegotiation,
        StartingTls,
        TlsEstablished,
        StartingStreaming,
        Ready,
        Publishing,
        Disconnecting,
        Faulted,
    }

    /// <summary>
    /// Transport statistics snapshot for periodic runtime reporting.
    /// </summary>
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
