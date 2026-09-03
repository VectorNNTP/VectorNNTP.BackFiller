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
    /// Classifies deterministic outcomes observed while connecting, authenticating, issuing commands, or receiving one article payload.
    /// </summary>
    internal enum NntpArticleAcquisitionFailureCode
    {
        /// <summary>
        /// The operation reached its command-specific success condition.
        /// </summary>
        None = 0,

        /// <summary>
        /// The supplied Message-ID does not satisfy the NNTP/INN grammar accepted by the acquisition path.
        /// </summary>
        InvalidMessageId = 1,

        /// <summary>
        /// The remote server reported that the requested article does not exist (<c>430</c>).
        /// </summary>
        ArticleNotFound = 2,

        /// <summary>
        /// The server rejected the command with a non-success 4xx/5xx status other than article-not-found.
        /// </summary>
        RemoteRejected = 3,

        /// <summary>
        /// Connection establishment, TLS negotiation, socket I/O, or stream completion failed before the operation could finish.
        /// </summary>
        ConnectionFailure = 4,

        /// <summary>
        /// The operation exceeded its configured timeout.
        /// </summary>
        Timeout = 5,

        /// <summary>
        /// A protocol line was received but did not satisfy the expected NNTP status-line syntax.
        /// </summary>
        MalformedResponse = 6,

        /// <summary>
        /// The multiline article body ended before the NNTP terminator line was observed.
        /// </summary>
        TruncatedArticle = 7,

        /// <summary>
        /// The received article exceeded the configured maximum byte budget.
        /// </summary>
        ArticleTooLarge = 8,

        /// <summary>
        /// Cancellation was requested by the caller before the operation completed.
        /// </summary>
        Cancelled = 9,

        /// <summary>
        /// The protocol exchange was syntactically readable but semantically unexpected for the active command.
        /// </summary>
        ProtocolFailure = 10,

        /// <summary>
        /// Authentication could not be completed, preventing authenticated commands from proceeding.
        /// </summary>
        AuthenticationFailure = 11,
    }

    /// <summary>
    /// Captures the immutable endpoint and credential settings used to establish one acquisition session.
    /// </summary>
    /// <param name="Host">Remote NNTP host name or IP literal.</param>
    /// <param name="Port">Remote NNTP server port.</param>
    /// <param name="UseSsl">Whether the session uses implicit TLS from connect time.</param>
    /// <param name="Username">Optional username supplied to <c>AUTHINFO USER</c>.</param>
    /// <param name="Password">Optional password supplied to <c>AUTHINFO PASS</c>.</param>
    internal readonly record struct NntpArticleAcquisitionEndpoint(
        string Host,
        int Port,
        bool UseSsl,
        string? Username,
        string? Password);

    /// <summary>
    /// Captures immutable guardrails that bound socket, protocol-line, and article-payload work for one acquisition session.
    /// </summary>
    /// <param name="MaxArticleBytes">Maximum article payload bytes accepted before the receive path fails deterministically.</param>
    /// <param name="ReceiveBufferBytes">Socket send and receive buffer size applied to the underlying <see cref="System.Net.Sockets.TcpClient"/>.</param>
    /// <param name="MaxStatusLineBytes">Maximum allowed byte length for a received NNTP status line.</param>
    /// <param name="ConnectTimeout">Timeout applied to connect and TLS/authentication handshake work.</param>
    /// <param name="CommandTimeout">Timeout applied to command writes and single-line status reads.</param>
    /// <param name="ReceiveTimeout">Timeout applied to multiline article payload reads.</param>
    internal readonly record struct NntpArticleAcquisitionOptions(
        int MaxArticleBytes,
        int ReceiveBufferBytes,
        int MaxStatusLineBytes,
        TimeSpan ConnectTimeout,
        TimeSpan CommandTimeout,
        TimeSpan ReceiveTimeout)
    {
        /// <summary>
        /// Gets the repository default guardrails for acquisition sessions.
        /// </summary>
        /// <value>
        /// A configuration that allows large articles, uses 64 KiB socket buffers, caps status lines at 16 KiB,
        /// and applies 30-second connect/command timeouts with a 2-minute payload receive timeout.
        /// </value>
        internal static NntpArticleAcquisitionOptions Default => new(
            MaxArticleBytes: 256 * 1024 * 1024,
            ReceiveBufferBytes: 64 * 1024,
            MaxStatusLineBytes: 16 * 1024,
            ConnectTimeout: TimeSpan.FromSeconds(30),
            CommandTimeout: TimeSpan.FromSeconds(30),
            ReceiveTimeout: TimeSpan.FromMinutes(2));
    }

    /// <summary>
    /// Owns a rented pooled buffer containing one successfully received raw article.
    /// </summary>
    /// <remarks>
    /// The acquisition path transfers buffer ownership into this wrapper so downstream parsing can read the article without copying it.
    /// Disposing the wrapper returns the rented array to <see cref="ArrayPool{T}.Shared"/> exactly once.
    /// </remarks>
    internal sealed class DownloadedArticleBuffer : IDisposable
    {
        /// <summary>
        /// Owned rented buffer.
        /// </summary>
        private byte[]? _buffer;

        /// <summary>
        /// Initializes a new pooled article-buffer owner.
        /// </summary>
        /// <param name="buffer">Rented backing array now owned by this instance.</param>
        /// <param name="length">Number of valid article bytes stored at the start of <paramref name="buffer"/>.</param>
        internal DownloadedArticleBuffer(byte[] buffer, int length)
        {
            _buffer = buffer ?? throw new ArgumentNullException(nameof(buffer));
            Length = length;
        }

        /// <summary>
        /// Gets the valid byte count in the owned buffer.
        /// </summary>
        /// <value>The length of the article prefix that callers may read from <see cref="Memory"/>.</value>
        internal int Length { get; }

        /// <summary>
        /// Gets a read-only view over the valid article bytes.
        /// </summary>
        /// <value>The first <see cref="Length"/> bytes of the rented array.</value>
        /// <exception cref="ObjectDisposedException">Thrown when the pooled buffer has already been returned.</exception>
        internal ReadOnlyMemory<byte> Memory
        {
            get
            {
                byte[] buffer = _buffer ?? throw new ObjectDisposedException(nameof(DownloadedArticleBuffer));
                return buffer.AsMemory(0, Length);
            }
        }

        /// <summary>
        /// Returns the rented buffer to the shared pool if ownership has not already been transferred or released.
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
    /// Carries the typed outcome of one acquisition-related operation.
    /// </summary>
    /// <remarks>
    /// <para><see cref="IsSuccess"/> is reserved for operations that returned article bytes and therefore own a <see cref="DownloadedArticleBuffer"/>.</para>
    /// <para>Some session operations, such as connect and DATE keepalive, use <see cref="NntpArticleAcquisitionFailureCode.None"/> to signal protocol success while intentionally leaving <see cref="ArticleBuffer"/> absent.</para>
    /// </remarks>
    internal sealed class NntpArticleAcquisitionResult : IDisposable
    {
        /// <summary>
        /// Initializes a new acquisition result instance.
        /// </summary>
        /// <param name="failureCode">Typed outcome classification.</param>
        /// <param name="responseCode">NNTP status code when the outcome was driven by a server response.</param>
        /// <param name="responseText">Server status text or local diagnostic detail describing the outcome.</param>
        /// <param name="articleBuffer">Owned raw article bytes when the operation produced a payload.</param>
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
        /// Gets a value indicating whether this result owns successfully downloaded article bytes.
        /// </summary>
        /// <value><see langword="true"/> only when <see cref="FailureCode"/> is <see cref="NntpArticleAcquisitionFailureCode.None"/> and <see cref="ArticleBuffer"/> is present.</value>
        internal bool IsSuccess => FailureCode == NntpArticleAcquisitionFailureCode.None && ArticleBuffer is not null;

        /// <summary>
        /// Gets the typed outcome classification.
        /// </summary>
        /// <value>The deterministic result code for the completed operation.</value>
        internal NntpArticleAcquisitionFailureCode FailureCode { get; }

        /// <summary>
        /// Gets the NNTP status code returned by the remote server when one was available.
        /// </summary>
        /// <value>The raw server response code, or <see langword="null"/> when the failure was local.</value>
        internal int? ResponseCode { get; }

        /// <summary>
        /// Gets server status text or local detail associated with the outcome.
        /// </summary>
        /// <value>A diagnostic string preserved for higher-level logging and failure mapping.</value>
        internal string ResponseText { get; }

        /// <summary>
        /// Gets the owned article buffer for payload-producing successes.
        /// </summary>
        /// <value>The buffer owner transferred from the receive path, or <see langword="null"/> when no payload was produced.</value>
        internal DownloadedArticleBuffer? ArticleBuffer { get; }

        /// <summary>
        /// Gets the article bytes when a payload was acquired.
        /// </summary>
        /// <value>The payload memory for successful article downloads; otherwise <see cref="ReadOnlyMemory{T}.Empty"/>.</value>
        internal ReadOnlyMemory<byte> ArticleBytes => ArticleBuffer?.Memory ?? ReadOnlyMemory<byte>.Empty;

        /// <summary>
        /// Gets the article length when a payload was acquired.
        /// </summary>
        /// <value>The byte length of <see cref="ArticleBytes"/>, or <c>0</c> when no payload is present.</value>
        internal int ArticleLength => ArticleBuffer?.Length ?? 0;

        /// <summary>
        /// Creates a result that owns successfully downloaded article bytes.
        /// </summary>
        /// <param name="responseCode">NNTP status code associated with the successful ARTICLE command.</param>
        /// <param name="responseText">NNTP status text associated with the successful ARTICLE command.</param>
        /// <param name="articleBuffer">Owned article buffer transferred to the result.</param>
        /// <returns>A result whose <see cref="IsSuccess"/> property is <see langword="true"/>.</returns>
        internal static NntpArticleAcquisitionResult Success(int responseCode, string responseText, DownloadedArticleBuffer articleBuffer)
        {
            return new(NntpArticleAcquisitionFailureCode.None, responseCode, responseText, articleBuffer);
        }

        /// <summary>
        /// Creates a result that records a non-payload outcome.
        /// </summary>
        /// <param name="failureCode">Deterministic outcome classification.</param>
        /// <param name="responseCode">Optional NNTP response code.</param>
        /// <param name="responseText">Server status text or local diagnostic detail.</param>
        /// <returns>A result without an owned article buffer.</returns>
        internal static NntpArticleAcquisitionResult Failure(NntpArticleAcquisitionFailureCode failureCode, int? responseCode, string responseText)
        {
            return new(failureCode, responseCode, responseText, articleBuffer: null);
        }

        /// <summary>
        /// Disposes the owned article buffer, if this result currently owns one.
        /// </summary>
        public void Dispose()
        {
            ArticleBuffer?.Dispose();
        }
    }
}
