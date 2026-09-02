// <copyright file="NntpArticleAcquisitionContracts.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Contracts for NNTP article acquisition result classification, endpoint/options configuration,
// and explicit pooled-buffer ownership for downloaded raw article bytes.

using System.Buffers;

namespace VectorNNTP.Backfiller.Runtime.Articles.Acquisition
{
    /// <summary>
    /// Represents terminal acquisition outcomes for one ARTICLE request.
    /// </summary>
    internal enum NntpArticleAcquisitionFailureCode
    {
        /// <summary>
        /// ARTICLE completed successfully and article bytes are available.
        /// </summary>
        None = 0,

        /// <summary>
        /// Message-ID argument failed NNTP/INN grammar validation.
        /// </summary>
        InvalidMessageId = 1,

        /// <summary>
        /// Remote server reported article-not-found (<c>430</c>).
        /// </summary>
        ArticleNotFound = 2,

        /// <summary>
        /// Remote server explicitly rejected request with non-success 4xx/5xx response other than not found.
        /// </summary>
        RemoteRejected = 3,

        /// <summary>
        /// Connection establishment, TLS/auth setup, or transport I/O failed.
        /// </summary>
        ConnectionFailure = 4,

        /// <summary>
        /// Operation exceeded configured timeout.
        /// </summary>
        Timeout = 5,

        /// <summary>
        /// Server response line was syntactically malformed.
        /// </summary>
        MalformedResponse = 6,

        /// <summary>
        /// Article stream ended before RFC terminator line.
        /// </summary>
        TruncatedArticle = 7,

        /// <summary>
        /// Article exceeded configured maximum size guardrail.
        /// </summary>
        ArticleTooLarge = 8,

        /// <summary>
        /// Caller cancellation requested operation termination.
        /// </summary>
        Cancelled = 9,

        /// <summary>
        /// Response sequence was syntactically valid but protocol-unexpected.
        /// </summary>
        ProtocolFailure = 10,

        /// <summary>
        /// Authentication to the remote server failed and prevented ARTICLE operations.
        /// </summary>
        AuthenticationFailure = 11,
    }

    /// <summary>
    /// Immutable endpoint settings for one NNTP acquisition session.
    /// </summary>
    /// <param name="Host">Remote NNTP host name or IP literal.</param>
    /// <param name="Port">Remote NNTP server port.</param>
    /// <param name="UseSsl">Whether implicit TLS is required at connect time.</param>
    /// <param name="Username">Optional NNTP username for AUTHINFO flow.</param>
    /// <param name="Password">Optional NNTP password for AUTHINFO flow.</param>
    internal readonly record struct NntpArticleAcquisitionEndpoint(
        string Host,
        int Port,
        bool UseSsl,
        string? Username,
        string? Password);

    /// <summary>
    /// Immutable runtime guardrails for acquisition session operations.
    /// </summary>
    /// <param name="MaxArticleBytes">Maximum article bytes accepted before deterministic rejection.</param>
    /// <param name="ReceiveBufferBytes">Socket receive-buffer size for network stream reads.</param>
    /// <param name="MaxStatusLineBytes">Maximum allowed NNTP status-line bytes.</param>
    /// <param name="ConnectTimeout">Timeout for connect and handshake operations.</param>
    /// <param name="CommandTimeout">Timeout for command transmit and status-line receive.</param>
    /// <param name="ReceiveTimeout">Timeout for multiline article receive operations.</param>
    internal readonly record struct NntpArticleAcquisitionOptions(
        int MaxArticleBytes,
        int ReceiveBufferBytes,
        int MaxStatusLineBytes,
        TimeSpan ConnectTimeout,
        TimeSpan CommandTimeout,
        TimeSpan ReceiveTimeout)
    {
        /// <summary>
        /// Gets default acquisition guardrails.
        /// </summary>
        /// <returns>The operation result.</returns>
        internal static NntpArticleAcquisitionOptions Default => new(
            MaxArticleBytes: 256 * 1024 * 1024,
            ReceiveBufferBytes: 64 * 1024,
            MaxStatusLineBytes: 16 * 1024,
            ConnectTimeout: TimeSpan.FromSeconds(30),
            CommandTimeout: TimeSpan.FromSeconds(30),
            ReceiveTimeout: TimeSpan.FromMinutes(2));
    }

    /// <summary>
    /// Represents explicit ownership of a pooled article byte buffer.
    /// </summary>
    internal sealed class DownloadedArticleBuffer : IDisposable
    {
        /// <summary>
        /// Owned rented buffer.
        /// </summary>
        private byte[]? _buffer;

        /// <summary>
        /// Initializes a new pooled article buffer owner.
        /// </summary>
        /// <param name="buffer">Rented backing array.</param>
        /// <param name="length">Valid byte count in rented array.</param>
        internal DownloadedArticleBuffer(byte[] buffer, int length)
        {
            _buffer = buffer ?? throw new ArgumentNullException(nameof(buffer));
            Length = length;
        }

        /// <summary>
        /// Gets valid byte length in owned buffer.
        /// </summary>
        internal int Length { get; }

        /// <summary>
        /// Gets article bytes as read-only memory.
        /// </summary>
        internal ReadOnlyMemory<byte> Memory
        {
            get
            {
                byte[] buffer = _buffer ?? throw new ObjectDisposedException(nameof(DownloadedArticleBuffer));
                return buffer.AsMemory(0, Length);
            }
        }

        /// <summary>
        /// Returns rented buffer to shared pool exactly once.
        /// </summary>
        public void Dispose()
        {
            byte[]? buffer = Interlocked.Exchange(ref _buffer, null);
            if (buffer is not null)
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }

    /// <summary>
    /// Represents one deterministic article acquisition result.
    /// </summary>
    internal sealed class NntpArticleAcquisitionResult : IDisposable
    {
        /// <summary>
        /// Initializes a new acquisition result instance.
        /// </summary>
        /// <param name="failureCode">Terminal acquisition code.</param>
        /// <param name="responseCode">Optional NNTP response code.</param>
        /// <param name="responseText">Protocol/local result detail text.</param>
        /// <param name="articleBuffer">Owned article buffer when successful.</param>
        internal NntpArticleAcquisitionResult(
            NntpArticleAcquisitionFailureCode failureCode,
            int? responseCode,
            string responseText,
            DownloadedArticleBuffer? articleBuffer)
        {
            FailureCode = failureCode;
            ResponseCode = responseCode;
            ResponseText = responseText;
            ArticleBuffer = articleBuffer;
        }

        /// <summary>
        /// Gets a value indicating whether acquisition succeeded.
        /// </summary>
        internal bool IsSuccess => FailureCode == NntpArticleAcquisitionFailureCode.None && ArticleBuffer is not null;

        /// <summary>
        /// Gets terminal acquisition code.
        /// </summary>
        internal NntpArticleAcquisitionFailureCode FailureCode { get; }

        /// <summary>
        /// Gets remote NNTP response code when available.
        /// </summary>
        internal int? ResponseCode { get; }

        /// <summary>
        /// Gets protocol/local detail text.
        /// </summary>
        internal string ResponseText { get; }

        /// <summary>
        /// Gets owned article buffer for successful acquisitions.
        /// </summary>
        internal DownloadedArticleBuffer? ArticleBuffer { get; }

        /// <summary>
        /// Gets article bytes when successful, otherwise empty memory.
        /// </summary>
        internal ReadOnlyMemory<byte> ArticleBytes => ArticleBuffer?.Memory ?? ReadOnlyMemory<byte>.Empty;

        /// <summary>
        /// Gets article byte count when successful, otherwise zero.
        /// </summary>
        internal int ArticleLength => ArticleBuffer?.Length ?? 0;

        /// <summary>
        /// Creates a success result.
        /// </summary>
        /// <param name="responseCode">Remote response code.</param>
        /// <param name="responseText">Remote response text.</param>
        /// <param name="articleBuffer">Owned article buffer.</param>
        /// <returns>Success result.</returns>
        internal static NntpArticleAcquisitionResult Success(int responseCode, string responseText, DownloadedArticleBuffer articleBuffer)
        {
            return new(NntpArticleAcquisitionFailureCode.None, responseCode, responseText, articleBuffer);
        }

        /// <summary>
        /// Creates a failure result.
        /// </summary>
        /// <param name="failureCode">Failure code.</param>
        /// <param name="responseCode">Optional response code.</param>
        /// <param name="responseText">Detail text.</param>
        /// <returns>Failure result.</returns>
        internal static NntpArticleAcquisitionResult Failure(NntpArticleAcquisitionFailureCode failureCode, int? responseCode, string responseText)
        {
            return new(failureCode, responseCode, responseText, articleBuffer: null);
        }

        /// <summary>
        /// Disposes owned article buffer when present.
        /// </summary>
        public void Dispose()
        {
            ArticleBuffer?.Dispose();
        }
    }
}
