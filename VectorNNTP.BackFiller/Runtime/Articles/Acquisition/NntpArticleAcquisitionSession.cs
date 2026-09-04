// <copyright file="NntpArticleAcquisitionSession.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Long-lived NNTP ARTICLE acquisition session with byte-preserving multiline framing,
// deterministic failure classification, redacted protocol logging, and pooled ownership semantics.

using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.IO.Pipelines;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Text;
using VectorNNTP.Backfiller.Runtime.Articles.Validation;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.Backfiller.Runtime.Articles.Acquisition
{
    /// <summary>
    /// Owns one connected NNTP transport and executes sequential ARTICLE and DATE commands against it.
    /// </summary>
    /// <remarks>
    /// Repository usage routes this type through <see cref="VectorNNTP.Backfiller.Runtime.Articles.Grabber.NntpArticleExecutionSessionManager"/>, which ensures that one active article workflow or keepalive probe uses a session at a time.
    /// The session serializes command writes to avoid QUIT racing active command emission during shutdown, but it is not a general-purpose concurrent NNTP command multiplexer.
    /// </remarks>
    internal sealed class NntpArticleAcquisitionSession : IAsyncDisposable
    {
        /// <summary>
        /// Endpoint settings.
        /// </summary>
        private readonly NntpArticleAcquisitionEndpoint _endpoint;

        /// <summary>
        /// Acquisition guardrails.
        /// </summary>
        private readonly NntpArticleAcquisitionOptions _options;

        /// <summary>
        /// Session logger.
        /// </summary>
        private readonly ILogger<NntpArticleAcquisitionSession> _logger;

        /// <summary>
        /// Connected client.
        /// </summary>
        private readonly TcpClient _tcpClient;

        /// <summary>
        /// Active transport stream.
        /// </summary>
        private readonly Stream _stream;

        /// <summary>
        /// Connection-scoped logging context.
        /// </summary>
        private readonly IDisposable? _connectionLoggingScope;

        /// <summary>
        /// Connection-scoped logging metadata.
        /// </summary>
        private readonly NntpConnectionLogContext? _connectionLoggingContext;

        /// <summary>
        /// Reader over transport stream.
        /// </summary>
        private readonly PipeReader _reader;

        /// <summary>
        /// Disposal marker.
        /// </summary>
        private bool _disposed;

        /// <summary>
        /// Indicates whether authentication completed and lifecycle commands such as QUIT are valid.
        /// </summary>
        private bool _protocolReadyForCommands;

        /// <summary>
        /// Stores whether transport/protocol failures make further command writes unsafe used by graceful lifecycle shutdown.
        /// </summary>
        private bool _transportFailed;

        /// <summary>
        /// Serializes command writes during shutdown so QUIT cannot race with in-flight ARTICLE/DATE command emissions.
        /// </summary>
        private readonly SemaphoreSlim _commandWriteGate = new(1, 1);

        /// <summary>
        /// Initializes a new acquisition session.
        /// </summary>
        /// <param name="endpoint">Endpoint settings.</param>
        /// <param name="options">Acquisition options.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="tcpClient">Connected client.</param>
        /// <param name="stream">Transport stream.</param>
        /// <param name="connectionLoggingScope">Optional disposable scope that owns connection log properties.</param>
        /// <param name="connectionLoggingContext">Optional connection metadata used to create the logging scope.</param>
        private NntpArticleAcquisitionSession(
            NntpArticleAcquisitionEndpoint endpoint,
            NntpArticleAcquisitionOptions options,
            ILogger<NntpArticleAcquisitionSession> logger,
            TcpClient tcpClient,
            Stream stream,
            IDisposable? connectionLoggingScope,
            NntpConnectionLogContext? connectionLoggingContext)
        {
            _endpoint = endpoint;
            _options = options;
            _logger = logger;
            _tcpClient = tcpClient;
            _stream = stream;
            _connectionLoggingScope = connectionLoggingScope;
            _connectionLoggingContext = connectionLoggingContext;
            _reader = PipeReader.Create(stream, new StreamPipeReaderOptions(leaveOpen: true));
        }

        /// <summary>
        /// Connects, negotiates TLS when required, validates the greeting, and performs optional AUTHINFO setup for a reusable acquisition session.
        /// </summary>
        /// <param name="endpoint">Endpoint settings.</param>
        /// <param name="options">Acquisition guardrails.</param>
        /// <param name="logger">Session logger.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <param name="serverCertificateValidationCallback">Optional per-session TLS server-certificate validation callback. When <see langword="null"/>, platform default certificate validation semantics remain in effect.</param>
        /// <param name="connectionLoggingContext">Optional connection metadata used to enrich session logging.</param>
        /// <returns>
        /// A tuple whose <c>Session</c> element is non-null only when the reusable session is ready for later commands,
        /// and whose <c>Result</c> element preserves the greeting or failure status for diagnostics.
        /// </returns>
        /// <remarks>
        /// Successful connect results report <see cref="NntpArticleAcquisitionFailureCode.None"/> in the returned result, but they intentionally do not carry article bytes, so <see cref="NntpArticleAcquisitionResult.IsSuccess"/> remains reserved for successful ARTICLE payload acquisition.
        /// </remarks>
        internal static async ValueTask<(NntpArticleAcquisitionSession? Session, NntpArticleAcquisitionResult Result)> ConnectAsync(
            NntpArticleAcquisitionEndpoint endpoint,
            NntpArticleAcquisitionOptions options,
            ILogger<NntpArticleAcquisitionSession> logger,
            CancellationToken cancellationToken,
            RemoteCertificateValidationCallback? serverCertificateValidationCallback = null,
            NntpConnectionLogContext? connectionLoggingContext = null)
        {
            ArgumentNullException.ThrowIfNull(logger);

            NntpArticleAcquisitionResult? optionsFailure = ValidateOptions(endpoint, options);
            if (optionsFailure is not null)
            {
                return (null, optionsFailure);
            }

            TcpClient tcpClient = new()
            {
                ReceiveBufferSize = options.ReceiveBufferBytes,
                SendBufferSize = options.ReceiveBufferBytes
            };
            Stream? stream = null;
            IDisposable? connectionLoggingScope = connectionLoggingContext?.Push();

            try
            {
                LogSessionConnecting(logger, endpoint.Host, endpoint.Port, endpoint.UseSsl);

                _ = await ExecuteWithTimeoutAsync(
                    options.ConnectTimeout,
                    cancellationToken,
                    async token =>
                    {
                        await tcpClient.ConnectAsync(endpoint.Host, endpoint.Port, token).ConfigureAwait(false);
                        return true;
                    }).ConfigureAwait(false);

                stream = tcpClient.GetStream();
                if (endpoint.UseSsl)
                {
                    SslStream sslStream = serverCertificateValidationCallback is null
                        ? new SslStream(stream, leaveInnerStreamOpen: false)
                        : new SslStream(stream, leaveInnerStreamOpen: false, serverCertificateValidationCallback);

                    SslClientAuthenticationOptions sslOptions = new()
                    {
                        TargetHost = endpoint.Host,
                        EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                    };

                    if (serverCertificateValidationCallback is not null)
                    {
                        sslOptions.RemoteCertificateValidationCallback = serverCertificateValidationCallback;
                    }

                    _ = await ExecuteWithTimeoutAsync(
                        options.ConnectTimeout,
                        cancellationToken,
                        async token =>
                        {
                            await sslStream.AuthenticateAsClientAsync(sslOptions, token).ConfigureAwait(false);

                            return true;
                        }).ConfigureAwait(false);

                    stream = sslStream;
                }

                NntpArticleAcquisitionSession session = new(endpoint, options, logger, tcpClient, stream, connectionLoggingScope, connectionLoggingContext);

                NntpArticleAcquisitionTraceContext greetingContext = new(NntpArticleAcquisitionOperation.Connect, MessageId: null, MaximumValue: null, ActualValue: null);
                string greetingLine = await session.ReadProtocolLineAsync(options.CommandTimeout, cancellationToken, greetingContext).ConfigureAwait(false);
                if (!TryParseStatusLine(greetingLine, out int greetingCode, out string greetingText))
                {
                    throw new NntpArticleAcquisitionException(
                        NntpArticleAcquisitionFailureCode.MalformedResponse,
                        greetingContext,
                        "Malformed NNTP greeting status line.");
                }

                if (greetingCode is not 200 and not 201)
                {
                    await session.DisposeAsync().ConfigureAwait(false);
                    return (null, NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.ProtocolFailure, greetingCode, greetingText));
                }

                NntpArticleAcquisitionResult? authFailure = await session.AuthenticateIfConfiguredAsync(cancellationToken).ConfigureAwait(false);
                if (authFailure is not null)
                {
                    await session.DisposeAsync().ConfigureAwait(false);
                    return (null, authFailure);
                }

                session._protocolReadyForCommands = true;
                LogSessionConnected(logger, endpoint.Host, endpoint.Port, endpoint.UseSsl);
                return (session, NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.None, greetingCode, greetingText));
            }
            catch (Exception ex) when (TryMapFailure(ex, cancellationToken, out NntpArticleAcquisitionResult failure))
            {
                connectionLoggingScope?.Dispose();

                if (stream is not null)
                {
                    await stream.DisposeAsync().ConfigureAwait(false);
                }

                tcpClient.Dispose();
                return (null, failure);
            }
        }

        /// <summary>
        /// Issues <c>ARTICLE</c> for one validated Message-ID and, on success, returns the downloaded raw article bytes.
        /// </summary>
        /// <param name="messageId">Message-ID argument.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A deterministic acquisition result whose payload buffer is present only when the ARTICLE command completed successfully.</returns>
        /// <remarks>
        /// Invalid Message-IDs are rejected before any protocol write. On payload-producing success the caller assumes ownership of the returned pooled buffer and should dispose the result after parsing.
        /// </remarks>
        internal async ValueTask<NntpArticleAcquisitionResult> DownloadArticleAsync(string messageId, CancellationToken cancellationToken)
        {
            if (_disposed)
            {
                return NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.ConnectionFailure, null, "Session has been disposed.");
            }

            if (!NntpMessageIdValidation.IsValidMessageId(messageId.AsSpan()))
            {
                return NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.InvalidMessageId, null, "Message-ID does not satisfy NNTP/INN grammar.");
            }

            Stopwatch stopwatch = Stopwatch.StartNew();
            NntpArticleAcquisitionTraceContext writeContext = new(NntpArticleAcquisitionOperation.CommandWrite, messageId, null, null);
            NntpArticleAcquisitionTraceContext statusContext = new(NntpArticleAcquisitionOperation.StatusRead, messageId, null, null);
            NntpArticleAcquisitionTraceContext payloadContext = new(NntpArticleAcquisitionOperation.ArticleReceive, messageId, _options.MaxArticleBytes, null);

            try
            {
                string command = string.Create(CultureInfo.InvariantCulture, $"ARTICLE {messageId}");
                await WriteCommandAsync(command, _options.CommandTimeout, cancellationToken, writeContext, redactCredentials: false).ConfigureAwait(false);

                string statusLine = await ReadProtocolLineAsync(_options.CommandTimeout, cancellationToken, statusContext).ConfigureAwait(false);
                if (!TryParseStatusLine(statusLine, out int statusCode, out string statusText))
                {
                    throw new NntpArticleAcquisitionException(
                        NntpArticleAcquisitionFailureCode.MalformedResponse,
                        statusContext,
                        "Malformed NNTP ARTICLE status line.");
                }

                NntpArticleAcquisitionResult statusResult = ClassifyArticleStatus(statusCode, statusText);
                if (statusResult.FailureCode == NntpArticleAcquisitionFailureCode.None)
                {
                    DownloadedArticleBuffer buffer = await ReadArticlePayloadAsync(cancellationToken, payloadContext).ConfigureAwait(false);
                    NntpArticleAcquisitionResult success = NntpArticleAcquisitionResult.Success(statusCode, statusText, buffer);
                    LogArticleOutcome(_logger, messageId, "downloaded", stopwatch.Elapsed, success.ArticleLength, failureReason: null);
                    return success;
                }

                if (statusResult.FailureCode == NntpArticleAcquisitionFailureCode.ArticleNotFound)
                {
                    LogArticleOutcome(_logger, messageId, "not found", stopwatch.Elapsed, articleSizeBytes: null, NntpArticleAcquisitionFailureCode.ArticleNotFound);
                }
                else if (statusResult.FailureCode == NntpArticleAcquisitionFailureCode.RemoteRejected)
                {
                    LogArticleOutcome(_logger, messageId, "remote rejection", stopwatch.Elapsed, articleSizeBytes: null, NntpArticleAcquisitionFailureCode.RemoteRejected);
                }
                else
                {
                    LogArticleFailure(_logger, messageId, statusResult.FailureCode, stopwatch.Elapsed, articleSizeBytes: null);
                }

                return statusResult;
            }
            catch (Exception ex) when (TryMapFailure(ex, cancellationToken, out NntpArticleAcquisitionResult failure))
            {
                LogArticleFailure(_logger, messageId, failure.FailureCode, stopwatch.Elapsed, articleSizeBytes: null);
                return failure;
            }
        }

        /// <summary>
        /// Sends a <c>DATE</c> keepalive command on the existing session without performing article retrieval.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A deterministic result describing DATE keepalive health with command-specific status semantics.</returns>
        /// <remarks>
        /// A successful DATE response is reported through <see cref="NntpArticleAcquisitionFailureCode.None"/>, but no article buffer is produced.
        /// </remarks>
        internal async ValueTask<NntpArticleAcquisitionResult> KeepAliveWithDateAsync(CancellationToken cancellationToken)
        {
            if (_disposed)
            {
                return NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.ConnectionFailure, null, "Session has been disposed.");
            }

            using IDisposable? connectionLoggingScope = _connectionLoggingContext?.Push();

            NntpArticleAcquisitionTraceContext writeContext = new(NntpArticleAcquisitionOperation.CommandWrite, MessageId: null, MaximumValue: null, ActualValue: null);
            NntpArticleAcquisitionTraceContext statusContext = new(NntpArticleAcquisitionOperation.StatusRead, MessageId: null, MaximumValue: null, ActualValue: null);

            try
            {
                await WriteCommandAsync("DATE", _options.CommandTimeout, cancellationToken, writeContext, redactCredentials: false).ConfigureAwait(false);
                string statusLine = await ReadProtocolLineAsync(_options.CommandTimeout, cancellationToken, statusContext).ConfigureAwait(false);
                return !TryParseStatusLine(statusLine, out int statusCode, out string statusText)
                    ? throw new NntpArticleAcquisitionException(
                        NntpArticleAcquisitionFailureCode.MalformedResponse,
                        statusContext,
                        "Malformed NNTP DATE status line.")
                    : ClassifyDateStatus(statusCode, statusText);
            }
            catch (Exception ex) when (TryMapFailure(ex, cancellationToken, out NntpArticleAcquisitionResult failure))
            {
                return failure;
            }
        }

        /// <summary>
        /// Shuts down the session, attempting a bounded <c>QUIT</c> exchange when the protocol state still permits it.
        /// </summary>
        /// <returns>A task that completes after reader, stream, socket, and logging-scope cleanup finish.</returns>
        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            using IDisposable? connectionLoggingScope = _connectionLoggingContext?.Push();

            _disposed = true;

            await TrySendQuitBeforeTransportDisposeAsync().ConfigureAwait(false);

            try
            {
                await _reader.CompleteAsync().ConfigureAwait(false);
            }
            catch
            {
            }

            try
            {
                await _stream.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
            }

            _tcpClient.Dispose();
            _connectionLoggingScope?.Dispose();
        }

        /// <summary>
        /// Performs the NNTP <c>AUTHINFO USER</c>/<c>AUTHINFO PASS</c> exchange when credentials are configured.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A failure result when authentication cannot complete; otherwise <see langword="null"/>.</returns>
        /// <remarks>
        /// The method requires username and password to be configured together; a half-configured credential set is rejected locally without sending protocol commands.
        /// </remarks>
        private async Task<NntpArticleAcquisitionResult?> AuthenticateIfConfiguredAsync(CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(_endpoint.Username) && string.IsNullOrWhiteSpace(_endpoint.Password))
            {
                return null;
            }

            if (string.IsNullOrWhiteSpace(_endpoint.Username) || string.IsNullOrWhiteSpace(_endpoint.Password))
            {
                return NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.AuthenticationFailure, null, "Both Username and Password must be configured together.");
            }

            NntpArticleAcquisitionTraceContext userWrite = new(NntpArticleAcquisitionOperation.CommandWrite, null, null, null);
            NntpArticleAcquisitionTraceContext userRead = new(NntpArticleAcquisitionOperation.StatusRead, null, null, null);
            await WriteCommandAsync($"AUTHINFO USER {_endpoint.Username}", _options.CommandTimeout, cancellationToken, userWrite, redactCredentials: true).ConfigureAwait(false);
            string userLine = await ReadProtocolLineAsync(_options.CommandTimeout, cancellationToken, userRead).ConfigureAwait(false);
            if (!TryParseStatusLine(userLine, out int userCode, out string userText))
            {
                return NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.MalformedResponse, null, "Malformed AUTHINFO USER status line.");
            }

            if (userCode == 281)
            {
                return null;
            }

            if (userCode != 381)
            {
                return ClassifyAuthInfoUserFailureStatus(userCode, userText);
            }

            NntpArticleAcquisitionTraceContext passWrite = new(NntpArticleAcquisitionOperation.CommandWrite, null, null, null);
            NntpArticleAcquisitionTraceContext passRead = new(NntpArticleAcquisitionOperation.StatusRead, null, null, null);
            await WriteCommandAsync($"AUTHINFO PASS {_endpoint.Password}", _options.CommandTimeout, cancellationToken, passWrite, redactCredentials: true).ConfigureAwait(false);
            string passLine = await ReadProtocolLineAsync(_options.CommandTimeout, cancellationToken, passRead).ConfigureAwait(false);
            return !TryParseStatusLine(passLine, out int passCode, out string passText)
                ? NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.MalformedResponse, null, "Malformed AUTHINFO PASS status line.")
                : passCode == 281 ? null : ClassifyAuthInfoPassFailureStatus(passCode, passText);
        }

        /// <summary>
        /// Reads one NNTP protocol line with timeout, byte-length guardrails, and debug logging.
        /// </summary>
        /// <param name="timeout">Operation timeout.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <param name="context">Trace context describing the active protocol phase.</param>
        /// <returns>The received status line text.</returns>
        private async ValueTask<string> ReadProtocolLineAsync(
            TimeSpan timeout,
            CancellationToken cancellationToken,
            NntpArticleAcquisitionTraceContext context)
        {
            try
            {
                (string? line, _, bool completedWithoutLine) = await ExecuteWithTimeoutAsync(
                    timeout,
                    cancellationToken,
                    token => TransitProtocolParser.ReadNntpLineWithByteCountAndCompletionAsync(_reader, token)).ConfigureAwait(false);

                if (completedWithoutLine)
                {
                    throw new EndOfStreamException("NNTP connection closed while awaiting line response.");
                }

                string statusLine = line!;
                int bytes = Encoding.ASCII.GetByteCount(statusLine);
                if (bytes > _options.MaxStatusLineBytes)
                {
                    throw new NntpArticleAcquisitionException(
                        NntpArticleAcquisitionFailureCode.MalformedResponse,
                        context with { MaximumValue = _options.MaxStatusLineBytes, ActualValue = bytes },
                        string.Create(CultureInfo.InvariantCulture, $"NNTP status line exceeded configured maximum length ({bytes}>{_options.MaxStatusLineBytes})."));
                }

                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    if (string.IsNullOrWhiteSpace(context.MessageId))
                    {
                        _logger.LogDebug("RX: {StatusLine}", statusLine);
                    }
                    else
                    {
                        _logger.LogDebug("RX: {StatusLine} MessageId={MessageId}", statusLine, context.MessageId);
                    }
                }

                return statusLine;
            }
            catch (Exception ex) when (MarkTransportFailureForException(ex, cancellationToken))
            {
                throw;
            }
        }

        /// <summary>
        /// Writes one NNTP command line with timeout handling and optional credential redaction in debug logs.
        /// </summary>
        /// <param name="command">Command text.</param>
        /// <param name="timeout">Operation timeout.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <param name="context">Trace context describing the active command phase.</param>
        /// <param name="redactCredentials">Whether authentication arguments should be redacted in logs.</param>
        /// <returns>A task that completes when the command has been written and flushed.</returns>
        private async Task WriteCommandAsync(
            string command,
            TimeSpan timeout,
            CancellationToken cancellationToken,
            NntpArticleAcquisitionTraceContext context,
            bool redactCredentials)
        {
            await _commandWriteGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                bool isAuthInfoUser = command.StartsWith("AUTHINFO USER ", StringComparison.OrdinalIgnoreCase);
                bool isAuthInfoPass = command.StartsWith("AUTHINFO PASS ", StringComparison.OrdinalIgnoreCase);
    
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    if (isAuthInfoUser)
                    {
                        _logger.LogDebug("TX: AUTHINFO USER ***");
                    }
                    else if (isAuthInfoPass)
                    {
                        _logger.LogDebug("TX: AUTHINFO PASS ***");
                    }
                    else
                    {
                        // Normal NNTP commands remain fully visible in debug logging.
                        if (string.IsNullOrWhiteSpace(context.MessageId))
                        {
                            _logger.LogDebug("TX: {Command}", command);
                        }
                        else
                        {
                            _logger.LogDebug("TX: {Command} MessageId={MessageId}", command, context.MessageId);
                        }
                    }
                }

                byte[] bytes = Encoding.ASCII.GetBytes(command + "\r\n");
                _ = await ExecuteWithTimeoutAsync(
                    timeout,
                    cancellationToken,
                    async token =>
                    {
                        await _stream.WriteAsync(bytes, token).ConfigureAwait(false);
                        await _stream.FlushAsync(token).ConfigureAwait(false);
                        return true;
                    }).ConfigureAwait(false);
            }
            catch (Exception ex) when (MarkTransportFailureForException(ex, cancellationToken))
            {
                throw;
            }
            finally
            {
                _ = _commandWriteGate.Release();
            }
        }

        /// <summary>
        /// Reads the multiline ARTICLE payload, removes one level of NNTP dot-stuffing, and transfers pooled ownership to the caller.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <param name="context">Trace context describing the payload receive phase.</param>
        /// <returns>An owned buffer containing the raw article bytes exactly as the parser should consume them.</returns>
        /// <remarks>
        /// The receive loop accepts payload fragmentation across pipe reads and terminates only on the NNTP terminator line. Any failure before <see cref="PooledArticleBuilder.Build"/> disposes the in-progress pooled buffer.
        /// </remarks>
        private async ValueTask<DownloadedArticleBuffer> ReadArticlePayloadAsync(
            CancellationToken cancellationToken,
            NntpArticleAcquisitionTraceContext context)
        {
            PooledArticleBuilder builder = new(_options.MaxArticleBytes, context);
            bool atLineStart = true;

            try
            {
                while (true)
                {
                    ReadResult readResult = await ExecuteWithTimeoutAsync(
                        _options.ReceiveTimeout,
                        cancellationToken,
                        token => _reader.ReadAsync(token)).ConfigureAwait(false);

                    ReadOnlySequence<byte> sequence = readResult.Buffer;
                    SequenceReader<byte> reader = new(sequence);

                    while (reader.TryPeek(out byte current))
                    {
                        if (atLineStart && current == (byte)'.')
                        {
                            SequenceReader<byte> lookAhead = reader;
                            lookAhead.Advance(1);
                            if (!lookAhead.TryPeek(out byte next))
                            {
                                break;
                            }

                            if (next == (byte)'.')
                            {
                                reader.Advance(2);
                                builder.WriteByte((byte)'.');
                                atLineStart = false;
                                continue;
                            }

                            if (next == (byte)'\n')
                            {
                                reader.Advance(2);
                                _reader.AdvanceTo(reader.Position, sequence.End);
                                return builder.Build();
                            }

                            if (next == (byte)'\r')
                            {
                                SequenceReader<byte> afterCarriageReturn = lookAhead;
                                afterCarriageReturn.Advance(1);
                                if (!afterCarriageReturn.TryPeek(out byte lineFeed))
                                {
                                    break;
                                }

                                if (lineFeed == (byte)'\n')
                                {
                                    reader.Advance(3);
                                    _reader.AdvanceTo(reader.Position, sequence.End);
                                    return builder.Build();
                                }

                                reader.Advance(1);
                                builder.WriteByte((byte)'.');
                                atLineStart = false;
                                continue;
                            }

                            reader.Advance(1);
                            builder.WriteByte((byte)'.');
                            atLineStart = false;
                            continue;
                        }

                        reader.Advance(1);
                        builder.WriteByte(current);
                        atLineStart = current is (byte)'\r' or (byte)'\n';
                    }

                    _reader.AdvanceTo(reader.Position, sequence.End);
                    if (readResult.IsCompleted)
                    {
                        throw new NntpArticleAcquisitionException(
                            NntpArticleAcquisitionFailureCode.TruncatedArticle,
                            context,
                            "NNTP article response ended before terminator line.");
                    }
                }
            }
            catch
            {
                builder.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Parses one status line into code/text tuple.
        /// </summary>
        /// <param name="line">Status line text.</param>
        /// <param name="code">Parsed status code.</param>
        /// <param name="text">Parsed status text.</param>
        /// <returns><see langword="true"/> when parse succeeded.</returns>
        private static bool TryParseStatusLine(string line, out int code, out string text)
        {
            try
            {
                (code, text) = TransitProtocolParser.ParseStatusCodeAndText(line);
                return true;
            }
            catch (InvalidOperationException)
            {
                code = 0;
                text = string.Empty;
                return false;
            }
        }

        /// <summary>
        /// Classifies ARTICLE command response status according to command-specific NNTP semantics.
        /// </summary>
        /// <remarks>
        /// Authoritative reference: RFC 3977 ARTICLE command definition and its documented response set
        /// (<c>220</c>, <c>430</c>, <c>412</c>, <c>420</c>, <c>423</c>) plus standard command/session
        /// rejection responses (<c>500</c>, <c>501</c>, <c>502</c>, <c>503</c>).
        /// </remarks>
        /// <param name="statusCode">Parsed NNTP status code.</param>
        /// <param name="statusText">Parsed NNTP status text.</param>
        /// <returns>Typed acquisition result preserving raw status code/text.</returns>
        private static NntpArticleAcquisitionResult ClassifyArticleStatus(int statusCode, string statusText)
        {
            return statusCode switch
            {
                220 => NntpArticleAcquisitionResult.StatusSuccess(statusCode, statusText),
                430 => NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.ArticleNotFound, statusCode, statusText),
                480 or 481 or 482 => NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.AuthenticationFailure, statusCode, statusText),
                500 or 501 or 502 or 503 => NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.RemoteRejected, statusCode, statusText),
                412 or 420 or 423 => NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.ProtocolFailure, statusCode, statusText),
                _ => NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.ProtocolFailure, statusCode, statusText),
            };
        }

        /// <summary>
        /// Classifies DATE command response status according to command-specific NNTP semantics.
        /// </summary>
        /// <remarks>
        /// Authoritative reference: RFC 3977 DATE command semantics where <c>111</c> is the expected
        /// successful DATE response; other command responses are interpreted explicitly and unexpected
        /// statuses remain protocol-level failures.
        /// </remarks>
        /// <param name="statusCode">Parsed NNTP status code.</param>
        /// <param name="statusText">Parsed NNTP status text.</param>
        /// <returns>Typed keepalive result preserving raw status code/text.</returns>
        private static NntpArticleAcquisitionResult ClassifyDateStatus(int statusCode, string statusText)
        {
            return statusCode switch
            {
                111 => NntpArticleAcquisitionResult.StatusSuccess(statusCode, statusText),
                480 or 481 or 482 => NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.AuthenticationFailure, statusCode, statusText),
                500 or 501 or 502 or 503 => NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.RemoteRejected, statusCode, statusText),
                _ => NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.ProtocolFailure, statusCode, statusText),
            };
        }

        /// <summary>
        /// Classifies AUTHINFO USER failure statuses according to command-specific NNTP semantics.
        /// </summary>
        /// <remarks>
        /// Authoritative reference: RFC 4643 AUTHINFO USER/PASS authentication extension semantics,
        /// combined with RFC 3977 base command-rejection responses.
        /// </remarks>
        /// <param name="statusCode">Parsed NNTP status code.</param>
        /// <param name="statusText">Parsed NNTP status text.</param>
        /// <returns>Failure classification preserving raw status details.</returns>
        private static NntpArticleAcquisitionResult ClassifyAuthInfoUserFailureStatus(int statusCode, string statusText)
        {
            return statusCode switch
            {
                480 or 481 or 482 => NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.AuthenticationFailure, statusCode, statusText),
                500 or 501 or 502 or 503 => NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.RemoteRejected, statusCode, statusText),
                _ => NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.ProtocolFailure, statusCode, statusText),
            };
        }

        /// <summary>
        /// Classifies AUTHINFO PASS failure statuses according to command-specific NNTP semantics.
        /// </summary>
        /// <remarks>
        /// Authoritative reference: RFC 4643 AUTHINFO USER/PASS authentication extension semantics,
        /// combined with RFC 3977 base command-rejection responses.
        /// </remarks>
        /// <param name="statusCode">Parsed NNTP status code.</param>
        /// <param name="statusText">Parsed NNTP status text.</param>
        /// <returns>Failure classification preserving raw status details.</returns>
        private static NntpArticleAcquisitionResult ClassifyAuthInfoPassFailureStatus(int statusCode, string statusText)
        {
            return statusCode switch
            {
                480 or 481 or 482 => NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.AuthenticationFailure, statusCode, statusText),
                500 or 501 or 502 or 503 => NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.RemoteRejected, statusCode, statusText),
                _ => NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.ProtocolFailure, statusCode, statusText),
            };
        }

        /// <summary>
        /// Validates endpoint and option guardrails before any network work begins.
        /// </summary>
        /// <param name="endpoint">Endpoint settings.</param>
        /// <param name="options">Acquisition guardrails.</param>
        /// <returns>A deterministic failure result when validation fails; otherwise <see langword="null"/>.</returns>
        private static NntpArticleAcquisitionResult? ValidateOptions(
            NntpArticleAcquisitionEndpoint endpoint,
            NntpArticleAcquisitionOptions options)
        {
            return string.IsNullOrWhiteSpace(endpoint.Host)
                ? NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.ConnectionFailure, null, "NNTP host is required.")
                : endpoint.Port is <= 0 or > 65535
                ? NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.ConnectionFailure, null, "NNTP port must be between 1 and 65535.")
                : options.MaxArticleBytes <= 0 || options.ReceiveBufferBytes < 1024 || options.MaxStatusLineBytes < 256
                ? NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.ProtocolFailure, null, "Acquisition options are out of range.")
                : options.ConnectTimeout <= TimeSpan.Zero || options.CommandTimeout <= TimeSpan.Zero || options.ReceiveTimeout <= TimeSpan.Zero
                ? NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.ProtocolFailure, null, "Acquisition timeouts must be greater than zero.")
                : null;
        }

        /// <summary>
        /// Executes an asynchronous operation with a linked timeout and caller-cancellation token.
        /// </summary>
        /// <typeparam name="T">Operation result type.</typeparam>
        /// <param name="timeout">Timeout budget for the operation.</param>
        /// <param name="cancellationToken">Caller cancellation token.</param>
        /// <param name="operation">Operation delegate that receives the linked token.</param>
        /// <returns>The delegate result.</returns>
        /// <exception cref="TimeoutException">Thrown when the linked timeout fires before the caller token is cancelled.</exception>
        private static async ValueTask<T> ExecuteWithTimeoutAsync<T>(
            TimeSpan timeout,
            CancellationToken cancellationToken,
            Func<CancellationToken, ValueTask<T>> operation)
        {
            using CancellationTokenSource timeoutSource = new(timeout);
            using CancellationTokenSource linkedSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutSource.Token);

            try
            {
                return await operation(linkedSource.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && timeoutSource.IsCancellationRequested)
            {
                throw new TimeoutException("NNTP operation timed out.");
            }
        }

        /// <summary>
        /// Attempts a bounded protocol-level <c>QUIT</c> exchange during disposal when the session reached command-ready state and the transport still appears writable.
        /// </summary>
        /// <returns>A task that completes after the QUIT attempt path finishes.</returns>
        private async Task TrySendQuitBeforeTransportDisposeAsync()
        {
            if (!_protocolReadyForCommands || _transportFailed || !CanAttemptQuitTransportWrite())
            {
                return;
            }

            using CancellationTokenSource quitTimeout = new(_options.CommandTimeout);
            NntpArticleAcquisitionTraceContext quitWriteContext = new(NntpArticleAcquisitionOperation.CommandWrite, MessageId: null, MaximumValue: null, ActualValue: null);
            NntpArticleAcquisitionTraceContext quitReadContext = new(NntpArticleAcquisitionOperation.StatusRead, MessageId: null, MaximumValue: null, ActualValue: null);

            try
            {
                await WriteCommandAsync("QUIT", _options.CommandTimeout, quitTimeout.Token, quitWriteContext, redactCredentials: false).ConfigureAwait(false);
                _ = await ReadProtocolLineAsync(_options.CommandTimeout, quitTimeout.Token, quitReadContext).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (quitTimeout.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                _ = MarkTransportFailureForException(ex, quitTimeout.Token);
            }
        }

        /// <summary>
        /// Determines whether the current transport appears usable for a bounded QUIT emission attempt.
        /// </summary>
        /// <returns><see langword="true"/> when QUIT write can be attempted; otherwise <see langword="false"/>.</returns>
        private bool CanAttemptQuitTransportWrite()
        {
            return _tcpClient.Client is Socket socket && socket.Connected;
        }

        /// <summary>
        /// Marks the transport as failed when an observed exception means later command writes are no longer trustworthy.
        /// </summary>
        /// <param name="exception">Exception observed by protocol read or write flow.</param>
        /// <param name="callerCancellation">Caller cancellation token used to distinguish cooperative cancellation from transport failure.</param>
        /// <returns>Always <see langword="false"/> so exception filters preserve the original control flow.</returns>
        private bool MarkTransportFailureForException(Exception exception, CancellationToken callerCancellation)
        {
            if (exception is TimeoutException
                or SocketException
                or IOException
                or AuthenticationException
                or EndOfStreamException)
            {
                _transportFailed = true;
                return false;
            }

            if (exception is OperationCanceledException && !callerCancellation.IsCancellationRequested)
            {
                _transportFailed = true;
            }

            return false;
        }

        /// <summary>
        /// Maps observed runtime exceptions into deterministic acquisition results.
        /// </summary>
        /// <param name="exception">Thrown exception.</param>
        /// <param name="callerCancellation">Caller cancellation token.</param>
        /// <param name="failure">Mapped acquisition failure result.</param>
        /// <returns><see langword="true"/> because every exception reaching this helper is translated into a deterministic result.</returns>
        private static bool TryMapFailure(
            Exception exception,
            CancellationToken callerCancellation,
            out NntpArticleAcquisitionResult failure)
        {
            if (exception is NntpArticleAcquisitionException acquisitionException)
            {
                failure = NntpArticleAcquisitionResult.Failure(acquisitionException.FailureCode, null, acquisitionException.Message);
                return true;
            }

            if (exception is TimeoutException)
            {
                failure = NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.Timeout, null, exception.Message);
                return true;
            }

            if (exception is OperationCanceledException)
            {
                failure = NntpArticleAcquisitionResult.Failure(
                    callerCancellation.IsCancellationRequested ? NntpArticleAcquisitionFailureCode.Cancelled : NntpArticleAcquisitionFailureCode.Timeout,
                    null,
                    exception.Message);
                return true;
            }

            if (exception is SocketException or IOException or AuthenticationException or EndOfStreamException)
            {
                failure = NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.ConnectionFailure, null, exception.Message);
                return true;
            }

            if (exception is InvalidOperationException invalidOperationException)
            {
                failure = NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.MalformedResponse, null, invalidOperationException.Message);
                return true;
            }

            failure = NntpArticleAcquisitionResult.Failure(NntpArticleAcquisitionFailureCode.ProtocolFailure, null, exception.Message);
            return true;
        }

        /// <summary>
        /// Formats elapsed duration with invariant machine-facing representation.
        /// </summary>
        /// <param name="elapsed">Elapsed duration.</param>
        /// <returns>Formatted duration.</returns>
        private static string FormatElapsed(TimeSpan elapsed)
        {
            return elapsed.TotalSeconds.ToString("F2", CultureInfo.InvariantCulture) + "s";
        }

        /// <summary>
        /// Logs session connect start.
        /// </summary>
        /// <param name="logger">Logger.</param>
        /// <param name="host">Host.</param>
        /// <param name="port">Port.</param>
        /// <param name="useSsl">SSL flag.</param>
        private static void LogSessionConnecting(ILogger logger, string host, int port, bool useSsl)
        {
            logger.LogInformation("Connecting article acquisition session to {Host}:{Port} (SSL={UseSsl})", host, port, useSsl);
        }

        /// <summary>
        /// Logs session connect completion.
        /// </summary>
        /// <param name="logger">Logger.</param>
        /// <param name="host">Host.</param>
        /// <param name="port">Port.</param>
        /// <param name="useSsl">SSL flag.</param>
        private static void LogSessionConnected(ILogger logger, string host, int port, bool useSsl)
        {
            logger.LogInformation("Connected article acquisition session to {Host}:{Port} (SSL={UseSsl})", host, port, useSsl);
        }

        /// <summary>
        /// Logs successful/non-fatal article outcomes.
        /// </summary>
        /// <param name="logger">Logger.</param>
        /// <param name="messageId">Message-ID.</param>
        /// <param name="outcome">Outcome text.</param>
        /// <param name="elapsed">Elapsed operation duration measured by monotonic stopwatch.</param>
        /// <param name="articleSizeBytes">Optional downloaded article payload size in bytes.</param>
        /// <param name="failureReason">Optional failure reason classification for non-success outcomes.</param>
        private static void LogArticleOutcome(
            ILogger logger,
            string messageId,
            string outcome,
            TimeSpan elapsed,
            int? articleSizeBytes,
            NntpArticleAcquisitionFailureCode? failureReason)
        {
            if (!logger.IsEnabled(LogLevel.Information))
            {
                return;
            }

            string duration = FormatElapsed(elapsed);
            string failureReasonText = failureReason?.ToString() ?? string.Empty;
            logger.LogInformation(
                "Article {MessageId} {Outcome} in {Duration} (FailureReason={FailureReason}, ArticleSize={ArticleSize})",
                messageId,
                outcome,
                duration,
                failureReasonText,
                articleSizeBytes);
        }

        /// <summary>
        /// Logs failed article outcomes.
        /// </summary>
        /// <param name="logger">Logger.</param>
        /// <param name="messageId">Message-ID.</param>
        /// <param name="failureCode">Failure classification.</param>
        /// <param name="elapsed">Elapsed operation duration measured by monotonic stopwatch.</param>
        /// <param name="articleSizeBytes">Optional article size associated with the failed operation.</param>
        private static void LogArticleFailure(
            ILogger logger,
            string messageId,
            NntpArticleAcquisitionFailureCode failureCode,
            TimeSpan elapsed,
            int? articleSizeBytes)
        {
            if (!logger.IsEnabled(LogLevel.Information))
            {
                return;
            }

            string duration = FormatElapsed(elapsed);
            logger.LogInformation(
                "Article {MessageId} failed in {Duration}: {FailureCode} (FailureReason={FailureReason}, ArticleSize={ArticleSize})",
                messageId,
                duration,
                failureCode,
                failureCode,
                articleSizeBytes);
        }

        /// <summary>
        /// Accumulates received article bytes in a pooled buffer that can grow up to the configured article-size ceiling.
        /// </summary>
        private sealed class PooledArticleBuilder : IDisposable
        {
            /// <summary>
            /// Initial rented buffer size.
            /// </summary>
            private const int InitialBufferBytes = 4096;

            /// <summary>
            /// Current rented buffer.
            /// </summary>
            private byte[] _buffer;

            /// <summary>
            /// Current valid length.
            /// </summary>
            private int _length;

            /// <summary>
            /// Maximum allowed bytes.
            /// </summary>
            private readonly int _maximumArticleBytes;

            /// <summary>
            /// Exception trace context.
            /// </summary>
            private readonly NntpArticleAcquisitionTraceContext _context;

            /// <summary>
            /// Initializes a new pooled article builder.
            /// </summary>
            /// <param name="maximumArticleBytes">Maximum allowed article size in bytes.</param>
            /// <param name="context">Trace context used when guardrail failures are raised.</param>
            internal PooledArticleBuilder(int maximumArticleBytes, NntpArticleAcquisitionTraceContext context)
            {
                _maximumArticleBytes = maximumArticleBytes;
                _context = context;
                _buffer = ArrayPool<byte>.Shared.Rent(Math.Min(maximumArticleBytes, InitialBufferBytes));
            }

            /// <summary>
            /// Appends one received byte, growing the rented buffer as needed within the configured limit.
            /// </summary>
            /// <param name="value">Received byte value.</param>
            internal void WriteByte(byte value)
            {
                if (_length >= _maximumArticleBytes)
                {
                    throw new NntpArticleAcquisitionException(
                        NntpArticleAcquisitionFailureCode.ArticleTooLarge,
                        _context with { ActualValue = _length, MaximumValue = _maximumArticleBytes },
                        string.Create(CultureInfo.InvariantCulture, $"Article exceeded configured maximum of {_maximumArticleBytes} bytes."));
                }

                if (_length == _buffer.Length)
                {
                    Grow();
                }

                _buffer[_length++] = value;
            }

            /// <summary>
            /// Transfers ownership of the rented buffer into a <see cref="DownloadedArticleBuffer"/> result wrapper.
            /// </summary>
            /// <returns>A buffer owner for the accumulated article bytes.</returns>
            internal DownloadedArticleBuffer Build()
            {
                byte[] owned = Interlocked.Exchange(ref _buffer, []);
                return new DownloadedArticleBuffer(owned, _length);
            }

            /// <summary>
            /// Returns the rented buffer to the shared pool when ownership has not yet been transferred.
            /// </summary>
            public void Dispose()
            {
                byte[] owned = Interlocked.Exchange(ref _buffer, []);
                if (owned.Length > 0)
                {
                    ArrayPool<byte>.Shared.Return(owned);
                }
            }

            /// <summary>
            /// Grows the rented buffer toward the configured maximum while preserving the bytes already received.
            /// </summary>
            private void Grow()
            {
                int currentLength = _buffer.Length;
                int candidateLength = currentLength <= int.MaxValue / 2 ? currentLength * 2 : _maximumArticleBytes;
                int nextLength = Math.Min(_maximumArticleBytes, candidateLength);
                if (nextLength <= currentLength)
                {
                    throw new NntpArticleAcquisitionException(
                        NntpArticleAcquisitionFailureCode.ArticleTooLarge,
                        _context with { ActualValue = _length, MaximumValue = _maximumArticleBytes },
                        string.Create(CultureInfo.InvariantCulture, $"Article exceeded configured maximum of {_maximumArticleBytes} bytes."));
                }

                byte[] next = ArrayPool<byte>.Shared.Rent(nextLength);
                _buffer.AsSpan(0, _length).CopyTo(next);
                ArrayPool<byte>.Shared.Return(_buffer);
                _buffer = next;
            }
        }
    }
}
