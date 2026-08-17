namespace VectorNNTP.Backfiller.Runtime.Transit;

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
/// Immutable publish result returned to callers for each submission.
/// </summary>
/// <param name="MessageId">Article Message-ID metadata.</param>
/// <param name="Status">Submission outcome status.</param>
/// <param name="ResponseCode">NNTP response code when available.</param>
/// <param name="ResponseText">Sanitized protocol response text when available.</param>
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
    long T7PublishAsyncCompleteTick = 0);

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
/// <param name="SupportsCompressDeflate">Whether COMPRESS DEFLATE is advertised.</param>
/// <param name="SupportsStreaming">Whether STREAMING/MODE STREAM is advertised.</param>
internal sealed record TransitCapabilitySnapshot(
    bool SupportsStartTls,
    bool SupportsCompressDeflate,
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
    StartingCompression,
    CompressionEstablished,
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
