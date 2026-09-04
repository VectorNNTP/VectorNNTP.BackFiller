// <copyright file="TransitConnection.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Transit
// Implements the transit connection behavior.

using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.ExceptionServices;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Channels;

namespace VectorNNTP.Backfiller.Runtime.Transit
{
    /// <summary>
    /// Owns one outbound NNTP transit session, including transport establishment, protocol negotiation,
    /// TAKETHIS submission, and response correlation for work assigned to this connection.
    /// </summary>
    /// <remarks>
    /// The connection transitions through explicit lifecycle states from TCP connect, greeting, capability discovery,
    /// optional TLS activation, and MODE STREAM enablement before entering publishing readiness.
    /// In-flight submissions are tracked by Message-ID and send order so asynchronous response lines can be mapped
    /// back to the originating work item. Disposal settles unresolved owned work as ambiguous and releases transport,
    /// pipe, channel, and synchronization resources owned by this instance.
    /// </remarks>
    internal sealed partial class TransitConnection : IAsyncDisposable
    {
        /// <summary>
        /// CRLF bytes appended to NNTP protocol lines.
        /// </summary>
        private static readonly byte[] CrLfBytes = "\r\n"u8.ToArray();
        /// <summary>
        /// NNTP dot-terminator sequence appended after staged article bodies.
        /// </summary>
        private static readonly byte[] DotTerminatorBytes = ".\r\n"u8.ToArray();
        /// <summary>
        /// ASCII prefix written before each TAKETHIS Message-ID token.
        /// </summary>
        private static readonly byte[] TakethisPrefixBytes = "TAKETHIS "u8.ToArray();
        /// <summary>
        /// Configures default response progress timeout for transit connection.
        /// </summary>
        private static readonly TimeSpan DefaultResponseProgressTimeout = TimeSpan.FromSeconds(30);
        /// <summary>
        /// Configures default response progress check interval for transit connection.
        /// </summary>
        private static readonly TimeSpan DefaultResponseProgressCheckInterval = TimeSpan.FromMilliseconds(250);

        /// <summary>
        /// Remote transit server host name or IP address.
        /// </summary>
        private readonly string _host;
        /// <summary>
        /// Remote transit server TCP port.
        /// </summary>
        private readonly int _port;
        /// <summary>
        /// Indicates whether the connection starts with TLS already enabled.
        /// </summary>
        private readonly bool _useSsl;
        /// <summary>
        /// Supplies the logger used by transit connection.
        /// </summary>
        private readonly ILogger _logger;
        /// <summary>
        /// Optional TLS certificate validation callback used when STARTTLS or implicit TLS is negotiated.
        /// </summary>
        private readonly RemoteCertificateValidationCallback? _serverCertificateValidationCallback;
        /// <summary>
        /// Configures response progress timeout for transit connection.
        /// </summary>
        private readonly TimeSpan _responseProgressTimeout;
        /// <summary>
        /// Configures response progress check interval for transit connection.
        /// </summary>
        private readonly TimeSpan _responseProgressCheckInterval;
        /// <summary>
        /// Optional collector for staging, flush, read, and response-correlation timing metrics.
        /// </summary>
        private readonly TransitTimingCollector? _timingCollector;

        /// <summary>
        /// Gate that serializes staging and flushing on the shared transport writer.
        /// </summary>
        private readonly SemaphoreSlim _writeGate = new(1, 1);
        /// <summary>
        /// Gate that serializes the narrow tokenless-correlation fallback path.
        /// </summary>
        private readonly SemaphoreSlim _tokenlessCorrelationGate = new(1, 1);

        /// <summary>
        /// Owned TCP client for the live transport session.
        /// </summary>
        private TcpClient? _tcpClient;
        /// <summary>
        /// Currently active base transport stream, either raw network or negotiated TLS.
        /// </summary>
        private Stream? _transportStream;
        /// <summary>
        /// Stream used by the pipe reader for protocol response consumption.
        /// </summary>
        private Stream? _readStream;
        /// <summary>
        /// Stream used by the pipe writer for TAKETHIS submissions.
        /// </summary>
        private Stream? _writeStream;
        /// <summary>
        /// Pipe reader that frames protocol response lines.
        /// </summary>
        private PipeReader? _reader;
        /// <summary>
        /// Pipe writer that stages TAKETHIS payloads before flush.
        /// </summary>
        private PipeWriter? _writer;

        /// <summary>
        /// Cancellation source for the background response loop.
        /// </summary>
        private CancellationTokenSource? _responseLoopCancellation;
        /// <summary>
        /// Background task that reads and correlates response lines.
        /// </summary>
        private Task? _responseLoopTask;
        /// <summary>
        /// Cancellation source for the definitive-response progress watchdog.
        /// </summary>
        private CancellationTokenSource? _responseProgressWatchdogCancellation;
        /// <summary>
        /// Background watchdog task that faults stalled response progress.
        /// </summary>
        private Task? _responseProgressWatchdogTask;
        /// <summary>
        /// Captured response-loop fault rethrown to later callers when needed.
        /// </summary>
        private ExceptionDispatchInfo? _responseLoopFault;
        /// <summary>
        /// Single-bit guard indicating that the response loop has faulted.
        /// </summary>
        private int _responseLoopFaulted;

        /// <summary>
        /// Pending connection-owned work keyed by Message-ID for normal response correlation.
        /// </summary>
        private readonly ConcurrentDictionary<string, PendingOwnedWork> _pendingByMessageId = new(StringComparer.Ordinal);
        /// <summary>
        /// FIFO send-order queue used by the narrow tokenless success fallback.
        /// </summary>
        private readonly ConcurrentQueue<string> _pendingBySendOrder = new();
        /// <summary>
        /// Internal completion queue consumed by the connection worker after responses are correlated.
        /// </summary>
        private readonly Channel<CompletedWork> _completedQueue = Channel.CreateUnbounded<CompletedWork>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = false,
        });
        /// <summary>
        /// Completion sources used by direct submit helpers keyed by work-item identifier.
        /// </summary>
        private readonly ConcurrentDictionary<long, TaskCompletionSource<TransitPublishResult>> _directSubmitCompletions = new();
        /// <summary>
        /// Stopwatch ticks captured when completions are enqueued for downstream observation.
        /// </summary>
        private readonly ConcurrentDictionary<long, long> _completionEnqueuedTicks = new();

        /// <summary>
        /// Indicates whether MODE STREAM negotiation completed successfully.
        /// </summary>
        private bool _streamingModeNegotiated;

        /// <summary>
        /// Single-bit guard indicating that shutdown or disposal has started.
        /// </summary>
        private int _shutdownRequested;
        /// <summary>
        /// Indicates that tokenless <c>239 Article transferred OK</c> correlation may be attempted.
        /// </summary>
        private int _tokenlessSuccessModeEnabled;
        /// <summary>
        /// Total bytes written to the remote server by this connection.
        /// </summary>
        private long _bytesTransmitted;
        /// <summary>
        /// Total bytes read from the remote server by this connection.
        /// </summary>
        private long _bytesReceived;
        /// <summary>
        /// Count of successful socket-open transitions observed for this connection instance.
        /// </summary>
        private long _socketOpenCount;
        /// <summary>
        /// Count of times the connection reached ready state.
        /// </summary>
        private long _readyTransitionCount;
        /// <summary>
        /// Count of submissions whose TAKETHIS send path has started on this connection.
        /// </summary>
        private long _submissionsStarted;
        /// <summary>
        /// Count of submissions accepted by definitive server response.
        /// </summary>
        private long _submissionsAccepted;
        /// <summary>
        /// Count of submissions rejected by definitive server response.
        /// </summary>
        private long _submissionsRejected;
        /// <summary>
        /// Count of submissions that failed locally before a definitive accept or reject.
        /// </summary>
        private long _submissionsFailed;
        /// <summary>
        /// Count of submissions settled as ambiguous because transmission certainty could not be resolved.
        /// </summary>
        private long _submissionsAmbiguous;
        /// <summary>
        /// Count of submissions rejected as unavailable due to remote capability or lifecycle state.
        /// </summary>
        private long _submissionsUnavailable;
        /// <summary>
        /// Highest concurrent submission depth observed on this connection.
        /// </summary>
        private int _maxConcurrentSubmissions;
        /// <summary>
        /// Monotonic send-order sequence used for diagnostics and tokenless fallback ordering.
        /// </summary>
        private long _sendSequence;
        /// <summary>
        /// Number of processed submission batches.
        /// </summary>
        private long _batchCount;
        /// <summary>
        /// Aggregate count of work items processed across all batches.
        /// </summary>
        private long _batchSizeTotal;
        /// <summary>
        /// Largest batch size written by this connection.
        /// </summary>
        private int _maxWriterBatchSize;
        /// <summary>
        /// Stopwatch tick of the last definitive response progress observed by the connection.
        /// </summary>
        private long _lastDefinitiveResponseProgressTick;

        /// <summary>
        /// Formats a trace stamp used by low-level diagnostic console output.
        /// </summary>
        /// <returns>An ISO-8601 UTC timestamp with managed thread and task identifiers.</returns>
        private static string TraceStamp()
        {
            return $"{DateTimeOffset.UtcNow:O}|tid={Environment.CurrentManagedThreadId}|task={Task.CurrentId?.ToString() ?? "-"}";
        }

        /// <summary>
        /// Initializes a transit connection configuration using platform-default server certificate validation.
        /// </summary>
        /// <param name="host">DNS name or IP address of the remote transit server.</param>
        /// <param name="port">TCP port used for the outbound transit session.</param>
        /// <param name="useSsl">
        /// <see langword="true"/> to start the session over TLS immediately; otherwise plaintext is used until
        /// optional STARTTLS negotiation.
        /// </param>
        /// <param name="logger">Logger used for state, capability, and fault events emitted by this connection.</param>
        /// <param name="perConnectionPipelineDepth">Maximum number of work items expected to be pipelined per batch on this connection.</param>
        /// <param name="writeBatchCoalesceMicroseconds">Validation input for configured write coalescing window in microseconds.</param>
        /// <param name="expectedBatchIntentCountProvider">Reserved provider for batch-intent accounting in the global queue architecture.</param>
        /// <param name="responseProgressTimeout">Optional watchdog timeout for definitive response progress; defaults to <c>30s</c>.</param>
        /// <param name="responseProgressCheckInterval">Optional interval for watchdog checks; defaults to <c>250ms</c>.</param>
        /// <param name="timingCollector">Optional collector that receives staging, flush, and response timing events.</param>
        internal TransitConnection(
            string host,
            int port,
            bool useSsl,
            ILogger logger,
            int perConnectionPipelineDepth = 8,
            int writeBatchCoalesceMicroseconds = 250,
            Func<int>? expectedBatchIntentCountProvider = null,
            TimeSpan? responseProgressTimeout = null,
            TimeSpan? responseProgressCheckInterval = null,
            TransitTimingCollector? timingCollector = null)
            : this(
                host,
                port,
                useSsl,
                logger,
                serverCertificateValidationCallback: null,
                perConnectionPipelineDepth,
                writeBatchCoalesceMicroseconds,
                expectedBatchIntentCountProvider,
                responseProgressTimeout,
                responseProgressCheckInterval,
                timingCollector)
        {
        }

        /// <summary>
        /// Initializes a transit connection with explicit server certificate validation policy.
        /// </summary>
        /// <param name="host">DNS name or IP address of the remote transit server.</param>
        /// <param name="port">TCP port used for the outbound transit session.</param>
        /// <param name="useSsl">
        /// <see langword="true"/> to start the session over TLS immediately; otherwise plaintext is used until
        /// optional STARTTLS negotiation.
        /// </param>
        /// <param name="logger">Logger used for state, capability, and fault events emitted by this connection.</param>
        /// <param name="serverCertificateValidationCallback">
        /// Optional TLS certificate validator. When <see langword="null"/>, platform-default certificate and host-name
        /// validation is used.
        /// </param>
        /// <param name="perConnectionPipelineDepth">Maximum number of work items expected to be pipelined per batch on this connection.</param>
        /// <param name="writeBatchCoalesceMicroseconds">Validation input for configured write coalescing window in microseconds.</param>
        /// <param name="expectedBatchIntentCountProvider">Reserved provider for batch-intent accounting in the global queue architecture.</param>
        /// <param name="responseProgressTimeout">Optional watchdog timeout for definitive response progress; defaults to <c>30s</c>.</param>
        /// <param name="responseProgressCheckInterval">Optional interval for watchdog checks; defaults to <c>250ms</c>.</param>
        /// <param name="timingCollector">Optional collector that receives staging, flush, and response timing events.</param>
        /// <exception cref="ArgumentException">Thrown when <paramref name="host"/> is null, empty, or whitespace.</exception>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="logger"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when <paramref name="port"/>, <paramref name="perConnectionPipelineDepth"/>,
        /// <paramref name="writeBatchCoalesceMicroseconds"/>, <paramref name="responseProgressTimeout"/>, or
        /// <paramref name="responseProgressCheckInterval"/> is outside the accepted range.
        /// </exception>
        /// <remarks>
        /// This constructor validates configuration values and stores immutable connection settings. Network I/O,
        /// protocol negotiation, and state transition to publishing readiness occur in <see cref="InitializeAsync(CancellationToken)"/>.
        /// </remarks>
        internal TransitConnection(
            string host,
            int port,
            bool useSsl,
            ILogger logger,
            RemoteCertificateValidationCallback? serverCertificateValidationCallback,
            int perConnectionPipelineDepth = 8,
            int writeBatchCoalesceMicroseconds = 250,
            Func<int>? expectedBatchIntentCountProvider = null,
            TimeSpan? responseProgressTimeout = null,
            TimeSpan? responseProgressCheckInterval = null,
            TransitTimingCollector? timingCollector = null)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Transit host is required.", nameof(host));
            }

            ArgumentNullException.ThrowIfNull(logger);

            if (port is <= 0 or > 65535)
            {
                throw new ArgumentOutOfRangeException(nameof(port), port, "Transit port must be between 1 and 65535.");
            }

            if (perConnectionPipelineDepth <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(perConnectionPipelineDepth), perConnectionPipelineDepth, "Per-connection pipeline depth must be greater than zero.");
            }

            if (writeBatchCoalesceMicroseconds <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(writeBatchCoalesceMicroseconds), writeBatchCoalesceMicroseconds, "Write batch coalescing window must be greater than zero microseconds.");
            }

            TimeSpan effectiveResponseProgressTimeout = responseProgressTimeout ?? DefaultResponseProgressTimeout;
            if (effectiveResponseProgressTimeout <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(responseProgressTimeout), effectiveResponseProgressTimeout, "Response progress timeout must be greater than zero.");
            }

            TimeSpan effectiveResponseProgressCheckInterval = responseProgressCheckInterval ?? DefaultResponseProgressCheckInterval;
            if (effectiveResponseProgressCheckInterval <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(responseProgressCheckInterval), effectiveResponseProgressCheckInterval, "Response progress check interval must be greater than zero.");
            }

            _host = host.Trim();
            _port = port;
            _useSsl = useSsl;
            _logger = logger;
            _serverCertificateValidationCallback = serverCertificateValidationCallback;
            PipelineDepth = perConnectionPipelineDepth;
            _responseProgressTimeout = effectiveResponseProgressTimeout;
            _responseProgressCheckInterval = effectiveResponseProgressCheckInterval;
            _timingCollector = timingCollector;
            _ = expectedBatchIntentCountProvider;
        }

        /// <summary>
        /// Gets a stable identifier used to correlate logs and diagnostics for this connection instance.
        /// </summary>
        /// <value>A random GUID formatted as a lowercase 32-character hex string without separators.</value>
        internal string ConnectionId { get; } = Guid.NewGuid().ToString("N");

        /// <summary>
        /// Gets the current lifecycle state of the connection state machine.
        /// </summary>
        /// <value>The most recent <see cref="TransitConnectionState"/> assigned by internal transition logic.</value>
        internal TransitConnectionState CurrentState { get; private set; } = TransitConnectionState.Disconnected;

        /// <summary>
        /// Gets a value indicating whether transport I/O currently runs over an authenticated <see cref="SslStream"/>.
        /// </summary>
        /// <value><see langword="true"/> after TLS activation succeeds; otherwise <see langword="false"/>.</value>
        internal bool IsTlsActive { get; private set; }

        /// <summary>
        /// Gets the most recently negotiated capability snapshot for this session.
        /// </summary>
        /// <value>
        /// Capability flags discovered during CAPABILITIES negotiation. The value is reset during failed initialization
        /// cleanup and refreshed after STARTTLS renegotiation.
        /// </value>
        internal TransitCapabilitySnapshot Capabilities { get; private set; } = new(SupportsStartTls: false, SupportsStreaming: false);

        /// <summary>
        /// Gets the number of in-flight submissions currently awaiting definitive settlement on this connection.
        /// </summary>
        /// <value>The current count of entries in the pending Message-ID map.</value>
        internal int OutstandingSubmissionCount => _pendingByMessageId.Count;

        /// <summary>
        /// Gets the configured per-connection pipeline depth used by upstream scheduling and diagnostics.
        /// </summary>
        /// <value>A positive integer validated at construction time.</value>
        internal int PipelineDepth { get; }

        /// <summary>
        /// Gets a value indicating whether the response loop has recorded a terminal fault.
        /// </summary>
        /// <value><see langword="true"/> once fault signaling succeeds; otherwise <see langword="false"/>.</value>
        internal bool IsResponseLoopFaulted => Volatile.Read(ref _responseLoopFaulted) == 1;

        /// <summary>
        /// Throws when the response loop has faulted and pending responses can no longer be completed normally.
        /// </summary>
        /// <exception cref="IOException">Thrown when a response-loop fault was recorded for this connection.</exception>
        internal void ThrowIfResponseLoopFaulted()
        {
            if (!IsResponseLoopFaulted)
            {
                return;
            }

            Exception? inner = _responseLoopFault?.SourceException;
            throw new IOException("Transit response loop faulted before pending responses completed.", inner);
        }

        /// <summary>
        /// Determines whether an observed exception corresponds to the response-loop fault captured by this connection.
        /// </summary>
        /// <param name="exception">Exception to compare against the recorded response-loop failure.</param>
        /// <returns>
        /// <see langword="true"/> when the supplied exception is the recorded fault instance, one of its inner exceptions,
        /// or an <see cref="InvalidOperationException"/> with the same message as the recorded invalid-operation fault.
        /// </returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="exception"/> is <see langword="null"/>.</exception>
        internal bool IsRecordedResponseLoopFault(Exception exception)
        {
            ArgumentNullException.ThrowIfNull(exception);

            if (!IsResponseLoopFaulted)
            {
                return false;
            }

            Exception? recorded = _responseLoopFault?.SourceException;
            if (recorded is null)
            {
                return false;
            }

            if (ReferenceEquals(exception, recorded))
            {
                return true;
            }

            for (Exception? current = exception.InnerException; current is not null; current = current.InnerException)
            {
                if (ReferenceEquals(current, recorded))
                {
                    return true;
                }
            }

            return exception is InvalidOperationException candidate
                && recorded is InvalidOperationException recordedInvalidOperation
                && string.Equals(candidate.Message, recordedInvalidOperation.Message, StringComparison.Ordinal);
        }

        /// <summary>
        /// Receives reservation-change notifications from the dispatcher integration point.
        /// </summary>
        /// <remarks>
        /// In the current global-queue architecture this callback is intentionally a no-op and exists only to preserve
        /// compatibility with higher-level publisher orchestration hooks.
        /// </remarks>
        internal static void NotifyMaterializationReservationChanged()
        {
            // Intentionally no-op in global queue architecture.
        }

        /// <summary>
        /// Receives reconnect notifications from publisher orchestration.
        /// </summary>
        /// <remarks>
        /// The method is intentionally a no-op for the current architecture where reconnect accounting is maintained
        /// outside this connection class.
        /// </remarks>
        internal static void RecordReconnectEvent()
        {
            // Intentionally no-op in global queue architecture.
        }

        /// <summary>
        /// Establishes transport connectivity and negotiates protocol readiness for TAKETHIS publishing.
        /// </summary>
        /// <param name="cancellationToken">Token used to cancel connect, negotiation, and initialization stage waits.</param>
        /// <returns>A task that completes when the connection reaches <see cref="TransitConnectionState.Ready"/>.</returns>
        /// <exception cref="OperationCanceledException">Thrown when <paramref name="cancellationToken"/> is canceled.</exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown when greeting/capability/mode-stream protocol responses are invalid for a publishing-ready session.
        /// </exception>
        /// <exception cref="TransitConnectionLifecycleException">
        /// Thrown when initialization exceeds the configured response-progress timeout.
        /// </exception>
        /// <remarks>
        /// The method performs TCP connect, optional immediate TLS, greeting validation, CAPABILITIES negotiation,
        /// optional STARTTLS upgrade with capability renegotiation, and MODE STREAM activation. On any failure,
        /// partially initialized resources are cleaned up and state is reset to <see cref="TransitConnectionState.Disconnected"/>.
        /// </remarks>
        internal async Task InitializeAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Console.WriteLine($"[TRACE-RI-60] {TraceStamp()} Connection.Initialize START connectionId={ConnectionId} host={_host} port={_port} timeoutMs={_responseProgressTimeout.TotalMilliseconds:F0}");

            try
            {
                TransitionState(TransitConnectionState.Connecting);
                _tcpClient = new TcpClient();
                await AwaitInitializationStageAsync(
                    token => _tcpClient.ConnectAsync(_host, _port, token).AsTask(),
                    "TCP connect",
                    cancellationToken).ConfigureAwait(false);
                Console.WriteLine($"[TRACE-RI-61] {TraceStamp()} Connection.Initialize TCP-CONNECT-COMPLETE connectionId={ConnectionId}");
                _ = Interlocked.Increment(ref _socketOpenCount);

                _transportStream = _tcpClient.GetStream();
                _readStream = _transportStream;
                _writeStream = _transportStream;
                IsTlsActive = false;
                _streamingModeNegotiated = false;

                if (_useSsl)
                {
                    TransitionState(TransitConnectionState.StartingTls);
                    await UpgradeToTlsAsync(cancellationToken).ConfigureAwait(false);
                    TransitionState(TransitConnectionState.TlsEstablished);
                }

                _reader = PipeReader.Create(_readStream, new StreamPipeReaderOptions(leaveOpen: true));
                _writer = PipeWriter.Create(_writeStream, new StreamPipeWriterOptions(leaveOpen: true));

                TransitionState(TransitConnectionState.AwaitingGreeting);
                Console.WriteLine($"[TRACE-RI-62] {TraceStamp()} Connection.Initialize GREETING-READ-START connectionId={ConnectionId}");
                string greetingLine = await AwaitInitializationStageAsync(
                    token => ReadLineAsync(token).AsTask(),
                    "greeting response",
                    cancellationToken).ConfigureAwait(false);
                Console.WriteLine($"[TRACE-RI-63] {TraceStamp()} Connection.Initialize GREETING-READ-COMPLETE connectionId={ConnectionId} line='{greetingLine}'");
                TransitProtocolParser.ValidateGreeting(greetingLine);

                TransitionState(TransitConnectionState.CapabilitiesNegotiation);
                Console.WriteLine($"[TRACE-RI-64] {TraceStamp()} Connection.Initialize CAPABILITIES-START connectionId={ConnectionId}");
                Capabilities = await AwaitInitializationStageAsync(
                    ReadCapabilitiesAsync,
                    "CAPABILITIES exchange",
                    cancellationToken).ConfigureAwait(false);
                Console.WriteLine($"[TRACE-RI-65] {TraceStamp()} Connection.Initialize CAPABILITIES-COMPLETE connectionId={ConnectionId} supportsStreaming={Capabilities.SupportsStreaming}");
                LogTransitCapabilities(_logger, ConnectionId, Capabilities.SupportsStartTls, Capabilities.SupportsStreaming);

                if (!_useSsl && Capabilities.SupportsStartTls)
                {
                    TransitionState(TransitConnectionState.StartingTls);
                    await AwaitInitializationStageAsync(
                        StartTlsAsync,
                        "STARTTLS negotiation",
                        cancellationToken).ConfigureAwait(false);
                    TransitionState(TransitConnectionState.TlsEstablished);

                    _reader = PipeReader.Create(_readStream ?? throw new InvalidOperationException("Transit transport read stream is not initialized."), new StreamPipeReaderOptions(leaveOpen: true));
                    _writer = PipeWriter.Create(_writeStream ?? throw new InvalidOperationException("Transit transport write stream is not initialized."), new StreamPipeWriterOptions(leaveOpen: true));

                    TransitionState(TransitConnectionState.CapabilitiesNegotiation);
                    Console.WriteLine($"[TRACE-RI-64A] {TraceStamp()} Connection.Initialize CAPABILITIES-RENEGOTIATE-START connectionId={ConnectionId}");
                    Capabilities = await AwaitInitializationStageAsync(
                        ReadCapabilitiesAsync,
                        "CAPABILITIES exchange (post-STARTTLS)",
                        cancellationToken).ConfigureAwait(false);
                    Console.WriteLine($"[TRACE-RI-65A] {TraceStamp()} Connection.Initialize CAPABILITIES-RENEGOTIATE-COMPLETE connectionId={ConnectionId} supportsStreaming={Capabilities.SupportsStreaming}");
                    LogTransitCapabilities(_logger, ConnectionId, Capabilities.SupportsStartTls, Capabilities.SupportsStreaming);
                }

                if (!Capabilities.SupportsStreaming)
                {
                    throw new InvalidOperationException("Transit server does not advertise STREAMING capability.");
                }

                TransitionState(TransitConnectionState.StartingStreaming);
                Console.WriteLine($"[TRACE-RI-66] {TraceStamp()} Connection.Initialize MODE-STREAM-START connectionId={ConnectionId}");
                await WriteCommandAsync("MODE STREAM", cancellationToken).ConfigureAwait(false);
                string streamResponse = await AwaitInitializationStageAsync(
                    token => ReadLineAsync(token).AsTask(),
                    "MODE STREAM response",
                    cancellationToken).ConfigureAwait(false);
                Console.WriteLine($"[TRACE-RI-67] {TraceStamp()} Connection.Initialize MODE-STREAM-COMPLETE connectionId={ConnectionId} line='{streamResponse}'");
                (int streamCode, _) = TransitProtocolParser.ParseStatusCodeAndText(streamResponse);
                if (streamCode != 203)
                {
                    throw new InvalidOperationException($"Unexpected MODE STREAM response code: {streamCode}.");
                }

                _streamingModeNegotiated = true;
                TransitionState(TransitConnectionState.Ready);
                Console.WriteLine($"[TRACE-RI-68] {TraceStamp()} Connection.Initialize SUCCESS connectionId={ConnectionId} state={CurrentState}");
                _ = Interlocked.Increment(ref _readyTransitionCount);
                LogTransitConnectionReady(_logger, ConnectionId, IsTlsActive);

                _responseLoopCancellation = new CancellationTokenSource();
                _responseProgressWatchdogCancellation = CancellationTokenSource.CreateLinkedTokenSource(_responseLoopCancellation.Token);
                _responseLoopFault = null;
                Volatile.Write(ref _responseLoopFaulted, 0);
                Volatile.Write(ref _lastDefinitiveResponseProgressTick, Stopwatch.GetTimestamp());
                _responseLoopTask = Task.Run(() => ResponseLoopAsync(_responseLoopCancellation.Token), CancellationToken.None);
                _responseProgressWatchdogTask = Task.Run(() => ResponseProgressWatchdogLoopAsync(_responseProgressWatchdogCancellation.Token), CancellationToken.None);
            }
            catch
            {
                try
                {
                    await CleanupInitializationFailureAsync().ConfigureAwait(false);
                }
                catch
                {
                }

                throw;
            }
        }

        /// <summary>
        /// Best-effort cleanup path used after initialization failures.
        /// </summary>
        /// <returns>A task that completes after transport, pipe, loop, and state artifacts are reset.</returns>
        /// <remarks>
        /// This method intentionally swallows expected disposal and completion exceptions while tearing down partially
        /// initialized resources so the caller can rethrow the original initialization failure.
        /// </remarks>
        private async Task CleanupInitializationFailureAsync()
        {
            try
            {
                _responseLoopCancellation?.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }

            try
            {
                _responseProgressWatchdogCancellation?.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }

            try
            {
                if (_reader is not null)
                {
                    await _reader.CompleteAsync().ConfigureAwait(false);
                }
            }
            catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException)
            {
            }

            try
            {
                if (_writer is not null)
                {
                    await _writer.CompleteAsync().ConfigureAwait(false);
                }
            }
            catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException)
            {
            }

            _readStream?.Dispose();
            _writeStream?.Dispose();
            _transportStream?.Dispose();
            _tcpClient?.Dispose();

            _reader = null;
            _writer = null;
            _readStream = null;
            _writeStream = null;
            _transportStream = null;
            _tcpClient = null;

            _responseLoopTask = null;
            _responseProgressWatchdogTask = null;
            _responseLoopFault = null;
            Volatile.Write(ref _responseLoopFaulted, 0);
            Volatile.Write(ref _lastDefinitiveResponseProgressTick, 0);

            _responseLoopCancellation?.Dispose();
            _responseProgressWatchdogCancellation?.Dispose();
            _responseLoopCancellation = null;
            _responseProgressWatchdogCancellation = null;

            IsTlsActive = false;
            _streamingModeNegotiated = false;
            Capabilities = new TransitCapabilitySnapshot(SupportsStartTls: false, SupportsStreaming: false);

            TransitionState(TransitConnectionState.Disconnected);
        }

        /// <summary>
        /// Stages and flushes a batch of claimed work items to the active transport and marks them awaiting responses.
        /// </summary>
        /// <param name="items">Claimed work items to register as pending and serialize as TAKETHIS frames.</param>
        /// <param name="cancellationToken">Token used to cancel admission, write-gate wait, and flush operations.</param>
        /// <returns>A value task that completes after all items are staged, flushed, and registered for response correlation.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="items"/> is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the connection is not in a publishing-ready state or when duplicate Message-IDs are submitted in flight.
        /// </exception>
        /// <exception cref="TransitConnectionLifecycleException">
        /// Thrown when the protocol writer is unavailable, completed, or disposed during frame staging/flush.
        /// </exception>
        /// <remarks>
        /// Pending registration and frame writes are serialized with <c>_writeGate</c>. When tokenless correlation mode is active,
        /// admission and send-order mutation are additionally protected by <c>_tokenlessCorrelationGate</c>.
        /// </remarks>
        internal async ValueTask ProcessBatchAsync(IReadOnlyList<TransitWorkItem> items, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(items);

            if (items.Count == 0)
            {
                return;
            }

            if (Volatile.Read(ref _shutdownRequested) == 1 || !_streamingModeNegotiated)
            {
                throw new InvalidOperationException("Transit connection is not ready for publishing.");
            }

            bool tokenlessModeEnabled = Volatile.Read(ref _tokenlessSuccessModeEnabled) == 1;
            if (tokenlessModeEnabled)
            {
                await _tokenlessCorrelationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            try
            {
                foreach (TransitWorkItem item in items)
                {
                    PendingOwnedWork pending = new(item);
                    if (!_pendingByMessageId.TryAdd(item.MessageId, pending))
                    {
                        throw new InvalidOperationException("Duplicate in-flight Message-ID on same connection.");
                    }

                    _pendingBySendOrder.Enqueue(item.MessageId);
                    _ = Interlocked.Increment(ref _submissionsStarted);
                    ObserveMaxConcurrentSubmissions(_pendingByMessageId.Count);
                }

                PipeWriter writer = _writer ?? throw new TransitConnectionLifecycleException(TransitConnectionLifecycleFailure.WriterNotInitialized);
                await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);

                try
                {
                    long stageStartTick = Stopwatch.GetTimestamp();
                    long batchBytesStaged = 0;

                    _timingCollector?.RecordStagingStarted(stageStartTick);

                    try
                    {
                        foreach (TransitWorkItem item in items)
                        {
                            item.MarkStaged();
                            batchBytesStaged += StageTakethisFrame(writer, item.MessageId, item.Payload);
                            item.MarkFlushed();
                        }

                        long flushStartTick = Stopwatch.GetTimestamp();
                        FlushResult flushResult = await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                        _timingCollector?.RecordFlushWait(Stopwatch.GetTimestamp() - flushStartTick);
                        if (flushResult.IsCompleted)
                        {
                            throw new TransitConnectionLifecycleException(TransitConnectionLifecycleFailure.WriterCompletedDuringTakethisSubmission);
                        }
                    }
                    catch (ObjectDisposedException)
                    {
                        throw new TransitConnectionLifecycleException(TransitConnectionLifecycleFailure.WriterDisposedDuringTakethisSubmission);
                    }
                    catch (InvalidOperationException ex) when (ex.Message.Contains("Writing is not allowed after writer was completed.", StringComparison.Ordinal))
                    {
                        throw new TransitConnectionLifecycleException(TransitConnectionLifecycleFailure.WriterDisposedDuringTakethisSubmission);
                    }

                    _ = Interlocked.Add(ref _bytesTransmitted, batchBytesStaged);
                    long stageEndTick = Stopwatch.GetTimestamp();

                    foreach (TransitWorkItem item in items)
                    {
                        item.MarkAwaitingResponse();
                        if (_pendingByMessageId.TryGetValue(item.MessageId, out PendingOwnedWork? pending))
                        {
                            pending.T2SocketWriteBeginTick = stageStartTick;
                            pending.T3SocketWriteEndTick = stageEndTick;
                            pending.SendSequence = Interlocked.Increment(ref _sendSequence);
                        }
                    }
                }
                finally
                {
                    _ = _writeGate.Release();
                }

                _ = Interlocked.Increment(ref _batchCount);
                _ = Interlocked.Add(ref _batchSizeTotal, items.Count);
                UpdateMaxBatchSize(items.Count);
            }
            finally
            {
                if (tokenlessModeEnabled)
                {
                    _ = _tokenlessCorrelationGate.Release();
                }
            }
        }

        /// <summary>
        /// Attempts to dequeue one completed work/result pair from the completion channel.
        /// </summary>
        /// <param name="item">When this method returns <see langword="true"/>, receives the completed work item.</param>
        /// <param name="result">When this method returns <see langword="true"/>, receives the publish result for <paramref name="item"/>.</param>
        /// <returns><see langword="true"/> when a completion was available; otherwise <see langword="false"/>.</returns>
        internal bool TryTakeCompleted(out TransitWorkItem item, out TransitPublishResult result)
        {
            if (_completedQueue.Reader.TryRead(out CompletedWork? completed) && completed is not null)
            {
                item = completed.WorkItem;
                result = completed.Result;

                if (_timingCollector is not null
                    && _completionEnqueuedTicks.TryRemove(completed.WorkItem.WorkItemId, out long completionEnqueuedTick))
                {
                    _timingCollector.RecordCompletionObserved(completionEnqueuedTick, Stopwatch.GetTimestamp());
                }

                return true;
            }

            item = null!;
            result = null!;
            return false;
        }

        /// <summary>
        /// Asynchronously waits until a completion is available to read from the completion channel.
        /// </summary>
        /// <param name="cancellationToken">Token used to cancel the wait operation.</param>
        /// <returns>
        /// A value indicating whether data is available to read. The value is <see langword="false"/> when the channel
        /// has completed and no further completions will arrive.
        /// </returns>
        /// <exception cref="OperationCanceledException">Thrown when <paramref name="cancellationToken"/> is canceled.</exception>
        /// <exception cref="IOException">Thrown when the underlying response loop faulted before completions were drained.</exception>
        internal async ValueTask<bool> WaitForCompletedAsync(CancellationToken cancellationToken)
        {
            try
            {
                return await _completedQueue.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (IsRecordedResponseLoopFault(ex))
            {
                throw new IOException("Transit response loop faulted before pending responses completed.", ex);
            }
        }

        /// <summary>
        /// Submits one article for TAKETHIS publication on this connection and awaits correlation with a server response.
        /// </summary>
        /// <param name="messageId">RFC-style Message-ID token used for protocol framing and response correlation.</param>
        /// <param name="articlePayload">Article payload bytes ending with LF; framing and dot-stuffing are applied during staging.</param>
        /// <param name="publishAsyncEnterTick">Caller-provided timing marker for publish-entry telemetry.</param>
        /// <param name="dispatcherAssignedTick">Caller-provided timing marker for dispatcher-assignment telemetry.</param>
        /// <param name="onWriteIntentMaterialized">Optional callback invoked after write intent has been staged for this submission.</param>
        /// <param name="cancellationToken">Token used to cancel submission admission and completion wait.</param>
        /// <returns>
        /// A publish result containing protocol status, response metadata, provenance, and timing stamps for this submission.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="messageId"/> is invalid or when <paramref name="articlePayload"/> is empty or does not end with LF.
        /// </exception>
        /// <exception cref="OperationCanceledException">May be observed from underlying operations when cancellation races with settlement.</exception>
        /// <remarks>
        /// If the connection is not ready, the method returns an <see cref="TransitPublishStatus.Unavailable"/> result instead of throwing.
        /// If cancellation occurs after a definitive settlement has already completed, the settled result is returned.
        /// </remarks>
        internal async ValueTask<TransitPublishResult> SubmitTakethisAsync(
            string messageId,
            ReadOnlyMemory<byte> articlePayload,
            long publishAsyncEnterTick,
            long dispatcherAssignedTick,
            Action? onWriteIntentMaterialized = null,
            CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(messageId);

            if (messageId.AsSpan().Contains('\r') || messageId.AsSpan().Contains('\n'))
            {
                throw new ArgumentException("Message-ID must not contain CR or LF characters.", nameof(messageId));
            }

            if (articlePayload.IsEmpty)
            {
                throw new ArgumentException("Article payload must not be empty.", nameof(articlePayload));
            }

            if (articlePayload.Span[^1] != (byte)'\n')
            {
                throw new ArgumentException("Article payload must end with LF to preserve byte integrity during TAKETHIS framing.", nameof(articlePayload));
            }

            if (Volatile.Read(ref _shutdownRequested) == 1 || (CurrentState != TransitConnectionState.Ready && CurrentState != TransitConnectionState.Publishing) || !_streamingModeNegotiated)
            {
                _ = Interlocked.Increment(ref _submissionsUnavailable);
                return new TransitPublishResult(
                    MessageId: messageId,
                    Status: TransitPublishStatus.Unavailable,
                    ResponseCode: null,
                    ResponseText: "Transit connection is not ready for publishing.",
                    T0PublishAsyncEnterTick: publishAsyncEnterTick,
                    T1DispatcherAssignedTick: dispatcherAssignedTick,
                    Provenance: TransitPublishProvenance.Unavailable,
                    ProvenanceConnectionId: ConnectionId,
                    ProvenanceConnectionState: CurrentState,
                    ProvenanceTick: Stopwatch.GetTimestamp());
            }

            byte[] payloadCopy = articlePayload.ToArray();
            TransitWorkItem item = new(Interlocked.Increment(ref _sendSequence), messageId, payloadCopy, maxAttempts: 3);
            item.MarkClaimed(ConnectionId, DateTimeOffset.UtcNow);

            TaskCompletionSource<TransitPublishResult> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _directSubmitCompletions[item.WorkItemId] = completion;

            try
            {
                try
                {
                    await ProcessBatchAsync([item], cancellationToken).ConfigureAwait(false);
                }
                catch (InvalidOperationException ex) when (string.Equals(ex.Message, "Duplicate in-flight Message-ID on same connection.", StringComparison.Ordinal))
                {
                    _ = Interlocked.Increment(ref _submissionsFailed);
                    return new TransitPublishResult(
                        MessageId: messageId,
                        Status: TransitPublishStatus.Failed,
                        ResponseCode: null,
                        ResponseText: "Duplicate in-flight Message-ID on same connection.",
                        T0PublishAsyncEnterTick: publishAsyncEnterTick,
                        T1DispatcherAssignedTick: dispatcherAssignedTick,
                        Provenance: TransitPublishProvenance.Failed,
                        ProvenanceConnectionId: ConnectionId,
                        ProvenanceConnectionState: CurrentState,
                        ProvenanceTick: Stopwatch.GetTimestamp());
                }

                onWriteIntentMaterialized?.Invoke();

                TransitPublishResult result = await completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                return result with
                {
                    T0PublishAsyncEnterTick = publishAsyncEnterTick,
                    T1DispatcherAssignedTick = dispatcherAssignedTick,
                };
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                if (completion.Task.IsCompletedSuccessfully)
                {
                    TransitPublishResult completedResult = completion.Task.Result;
                    return completedResult with
                    {
                        T0PublishAsyncEnterTick = publishAsyncEnterTick,
                        T1DispatcherAssignedTick = dispatcherAssignedTick,
                    };
                }

                if (_pendingByMessageId.TryGetValue(messageId, out PendingOwnedWork? pending)
                    && pending.T2SocketWriteBeginTick == 0
                    && _pendingByMessageId.TryRemove(messageId, out _))
                {
                    AcknowledgeSendOrder(messageId);
                }

                return new TransitPublishResult(
                    MessageId: messageId,
                    Status: TransitPublishStatus.Canceled,
                    ResponseCode: null,
                    ResponseText: "Transit publisher canceled.",
                    T0PublishAsyncEnterTick: publishAsyncEnterTick,
                    T1DispatcherAssignedTick: dispatcherAssignedTick,
                    Provenance: TransitPublishProvenance.Preemption,
                    ProvenanceConnectionId: ConnectionId,
                    ProvenanceConnectionState: CurrentState,
                    ProvenanceTick: Stopwatch.GetTimestamp());
            }
            finally
            {
                _ = _directSubmitCompletions.TryRemove(item.WorkItemId, out _);
            }
        }

        /// <summary>
        /// Drains all currently pending work items owned by this connection for upstream retry orchestration.
        /// </summary>
        /// <returns>A snapshot list of drained work items removed from local pending-correlation state.</returns>
        /// <remarks>
        /// Draining acknowledges send-order entries for removed items and transfers ownership of retry decisions
        /// to the caller.
        /// </remarks>
        internal IReadOnlyList<TransitWorkItem> DrainOutstandingOwnedWorkForRetry()
        {
            List<TransitWorkItem> drained = [.. DrainOwnedPendingWork(static _ => true).Select(static pending => pending.WorkItem)];
            // Console.WriteLine($"[TRACE-RI-79] {TraceStamp()} DrainOutstandingOwnedWorkForRetry connectionId={ConnectionId} count={drained.Count} items=[{string.Join(",", drained.Select(static x => $"{x.WorkItemId}:{x.State}:{x.AttemptCount}"))}]");
            return drained;
        }

        /// <summary>
        /// Drains only pending entries that still have registered direct-submit completion sources.
        /// </summary>
        /// <returns>Removed pending entries that were associated with direct-submit callers.</returns>
        private List<PendingOwnedWork> DrainOutstandingDirectSubmitPendingWork()
        {
            return DrainOwnedPendingWork(pending => _directSubmitCompletions.ContainsKey(pending.WorkItem.WorkItemId));
        }

        /// <summary>
        /// Drains pending entries that match a supplied predicate and removes their send-order tracking.
        /// </summary>
        /// <param name="shouldDrain">Predicate that determines whether a pending entry should be removed.</param>
        /// <returns>Removed pending entries in the order they were discovered during dictionary traversal.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="shouldDrain"/> is <see langword="null"/>.</exception>
        private List<PendingOwnedWork> DrainOwnedPendingWork(Func<PendingOwnedWork, bool> shouldDrain)
        {
            ArgumentNullException.ThrowIfNull(shouldDrain);

            List<PendingOwnedWork> unresolved = [];
            foreach ((string messageId, PendingOwnedWork pending) in _pendingByMessageId)
            {
                if (!shouldDrain(pending))
                {
                    continue;
                }

                if (_pendingByMessageId.TryRemove(messageId, out PendingOwnedWork? removed) && removed is not null)
                {
                    AcknowledgeSendOrder(messageId);
                    unresolved.Add(removed);
                }
            }

            return unresolved;
        }

        /// <summary>
        /// Captures a point-in-time diagnostics snapshot for this connection instance.
        /// </summary>
        /// <returns>
        /// A diagnostics record containing lifecycle state, transport endpoints, aggregate counters, and sampled
        /// outstanding publish-operation data.
        /// </returns>
        internal TransitConnectionDiagnosticsSnapshot CaptureDiagnosticsSnapshot()
        {
            OutstandingPublishOperationSnapshot[] outstanding = [.. _pendingByMessageId.Values
                .Select(static x => new OutstandingPublishOperationSnapshot(
                    MessageId: x.WorkItem.MessageId,
                    T2WriteIntentEnqueuedTick: 0,
                    T6FrameStageEndTick: x.T3SocketWriteEndTick,
                    T8BatchFlushEndTick: x.T3SocketWriteEndTick,
                    T9ResponseCorrelatedTick: x.T6ResponseCorrelatedTick,
                    WriteIntentEnqueued: true,
                    TakethisStagedForWrite: x.WorkItem.State >= TransitWorkItemState.Staged,
                    FlushCompleted: x.WorkItem.State >= TransitWorkItemState.Flushed,
                    WaitingFor239Response: x.WorkItem.State == TransitWorkItemState.AwaitingResponse,
                    CompletionTaskIsCompleted: false,
                    CompletionTaskStatus: "Waiting",
                    CompletionStatus: null,
                    LikelyAwaitingPath: "ResponseLoop"))];

            long numberOfBatches = Interlocked.Read(ref _batchCount);
            double avgBatch = numberOfBatches == 0 ? 0 : (double)Interlocked.Read(ref _batchSizeTotal) / numberOfBatches;

            string? localEndpoint = null;
            string? remoteEndpoint = null;
            TcpClient? tcpClient = _tcpClient;
            Socket? socket = tcpClient?.Client;
            if (socket is not null)
            {
                try
                {
                    localEndpoint = socket.LocalEndPoint?.ToString();
                    remoteEndpoint = socket.RemoteEndPoint?.ToString();
                }
                catch (ObjectDisposedException)
                {
                    localEndpoint = null;
                    remoteEndpoint = null;
                }
            }

            return new TransitConnectionDiagnosticsSnapshot(
                ConnectionId: ConnectionId,
                Host: _host,
                Port: _port,
                CurrentState: CurrentState,
                IsTlsActive: IsTlsActive,
                SocketOpenCount: Interlocked.Read(ref _socketOpenCount),
                ReadyTransitionCount: Interlocked.Read(ref _readyTransitionCount),
                SubmissionsStarted: Interlocked.Read(ref _submissionsStarted),
                SubmissionsAccepted: Interlocked.Read(ref _submissionsAccepted),
                SubmissionsRejected: Interlocked.Read(ref _submissionsRejected),
                SubmissionsAmbiguous: Interlocked.Read(ref _submissionsAmbiguous),
                SubmissionsUnavailable: Interlocked.Read(ref _submissionsUnavailable),
                SubmissionsFailed: Interlocked.Read(ref _submissionsFailed),
                BytesTransmitted: Interlocked.Read(ref _bytesTransmitted),
                BytesReceived: Interlocked.Read(ref _bytesReceived),
                MaxConcurrentSubmissions: Volatile.Read(ref _maxConcurrentSubmissions),
                CurrentConcurrentSubmissions: _pendingByMessageId.Count,
                CurrentWriteIntentQueueDepth: 0,
                LocalEndpoint: localEndpoint,
                RemoteEndpoint: remoteEndpoint,
                DiagnosticsSummary: new PipeliningDiagnosticSummary(
                    MaxPendingDepth: Volatile.Read(ref _maxConcurrentSubmissions),
                    MaxWriteQueueDepth: 0,
                    MaxWriterBatchSize: Volatile.Read(ref _maxWriterBatchSize),
                    AverageWriterBatchSize: avgBatch,
                    P50WriterBatchSize: avgBatch,
                    P95WriterBatchSize: avgBatch,
                    P99WriterBatchSize: avgBatch,
                    NumberOfBatches: numberOfBatches,
                    BatchSizeHistogram: string.Empty,
                    BatchSizeCounts: [],
                    AverageCoalescingWaitMicroseconds: 0,
                    P50CoalescingWaitMicroseconds: 0,
                    P95CoalescingWaitMicroseconds: 0,
                    P99CoalescingWaitMicroseconds: 0,
                    MaxLogicalOutstandingAheadAtResponse: 0,
                    CapturedOperationCount: outstanding.Length,
                    SampledOperationCount: outstanding.Length),
                DiagnosticSampleRecords: [],
                OutstandingOperations: outstanding);
        }

        /// <summary>
        /// Captures provenance data for the first greeting phase (P1) when enabled by instrumentation.
        /// </summary>
        /// <returns>
        /// The captured provenance snapshot, or <see langword="null"/> when no snapshot is available in the current build/runtime path.
        /// </returns>
        internal static P1GreetingProvenanceSnapshot? CaptureFirstP1GreetingProvenanceSnapshot()
        {
            return null;
        }

        /// <summary>
        /// Reads response lines from the server and settles pending submissions as responses arrive.
        /// </summary>
        /// <param name="cancellationToken">Token used to stop the background response loop.</param>
        /// <returns>A task representing the response-loop lifetime.</returns>
        /// <remarks>
        /// This loop maps NNTP response lines to Message-IDs, records timing and counters, enqueues completion notifications,
        /// and signals terminal faults through <see cref="TrySignalResponseLoopFault(Exception, bool)"/>.
        /// </remarks>
        private async Task ResponseLoopAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    long responseReadStartTick = Stopwatch.GetTimestamp();
                    string responseLine = await ReadLineAsync(cancellationToken).ConfigureAwait(false);
                    long responseReadEndTick = Stopwatch.GetTimestamp();
                    _timingCollector?.RecordResponseLineRead(responseReadEndTick - responseReadStartTick);

                    long responseAvailableTick = responseReadEndTick;
                    long responseCorrelationStartTick = Stopwatch.GetTimestamp();
                    TransitPublishResult? mapped = MapTakethisResponse(responseLine, responseAvailableTick);
                    if (mapped is null)
                    {
                        continue;
                    }

                    if (_pendingByMessageId.TryRemove(mapped.MessageId, out PendingOwnedWork? pendingCandidate) && pendingCandidate is not null)
                    {
                        pendingCandidate.T6ResponseCorrelatedTick = Stopwatch.GetTimestamp();
                        _timingCollector?.RecordResponseCorrelation(
                            elapsedTicks: pendingCandidate.T6ResponseCorrelatedTick - responseCorrelationStartTick,
                            responseAvailableTick: responseAvailableTick,
                            correlatedTick: pendingCandidate.T6ResponseCorrelatedTick,
                            definitive: mapped.ResponseCode is 239 or 439);
                        AcknowledgeSendOrder(mapped.MessageId);
                        TransitPublishResult correlatedResult = mapped with
                        {
                            T2SocketWriteBeginTick = pendingCandidate.T2SocketWriteBeginTick,
                            T3SocketWriteEndTick = pendingCandidate.T3SocketWriteEndTick,
                            T6ResponseCorrelatedTick = pendingCandidate.T6ResponseCorrelatedTick,
                        };

                        RecordSubmissionResult(correlatedResult.Status);
                        if (_timingCollector is not null)
                        {
                            _completionEnqueuedTicks[pendingCandidate.WorkItem.WorkItemId] = Stopwatch.GetTimestamp();
                        }

                        _ = _completedQueue.Writer.TryWrite(new CompletedWork(pendingCandidate.WorkItem, correlatedResult));
                        TryCompleteDirectSubmit(pendingCandidate.WorkItem.WorkItemId, correlatedResult);

                        if (correlatedResult.ResponseCode is 239 or 439)
                        {
                            Volatile.Write(ref _lastDefinitiveResponseProgressTick, Stopwatch.GetTimestamp());
                        }
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                TrySignalResponseLoopFault(ex, cancelResponseLoop: false);
            }
        }

        /// <summary>
        /// Monitors definitive-response progress while submissions are outstanding and faults the connection on prolonged stalls.
        /// </summary>
        /// <param name="cancellationToken">Token used to stop the watchdog loop.</param>
        /// <returns>A task representing the watchdog-loop lifetime.</returns>
        /// <remarks>
        /// The watchdog checks progress at <c>_responseProgressCheckInterval</c> and signals a fault when the elapsed time
        /// since the last definitive response exceeds <c>_responseProgressTimeout</c> while pending work exists.
        /// </remarks>
        private async Task ResponseProgressWatchdogLoopAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(_responseProgressCheckInterval, cancellationToken).ConfigureAwait(false);

                    if (Volatile.Read(ref _shutdownRequested) == 1)
                    {
                        continue;
                    }

                    if (_pendingByMessageId.IsEmpty)
                    {
                        continue;
                    }

                    long lastProgressTick = Volatile.Read(ref _lastDefinitiveResponseProgressTick);
                    if (lastProgressTick == 0)
                    {
                        continue;
                    }

                    TimeSpan elapsed = Stopwatch.GetElapsedTime(lastProgressTick);
                    if (elapsed <= _responseProgressTimeout)
                    {
                        continue;
                    }

                    TimeoutException timeout = new($"Transit response progress timeout exceeded for connection {ConnectionId} after {elapsed.TotalSeconds:F3}s with {_pendingByMessageId.Count} outstanding work items.");
                    TrySignalResponseLoopFault(timeout, cancelResponseLoop: true);
                    return;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                TrySignalResponseLoopFault(ex, cancelResponseLoop: true);
            }
        }

        /// <summary>
        /// Records and propagates the first terminal response-loop failure for this connection.
        /// </summary>
        /// <param name="ex">The failure that triggered fault signaling.</param>
        /// <param name="cancelResponseLoop"><see langword="true"/> to request cancellation of the response loop after fault signaling.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="ex"/> is <see langword="null"/>.</exception>
        /// <remarks>
        /// Fault signaling is single-shot. After the first signal, state transitions to <see cref="TransitConnectionState.Faulted"/>,
        /// direct-submit pending work is settled as ambiguous, and the completion channel is completed with the captured exception.
        /// </remarks>
        private void TrySignalResponseLoopFault(Exception ex, bool cancelResponseLoop)
        {
            ArgumentNullException.ThrowIfNull(ex);

            if (Interlocked.CompareExchange(ref _responseLoopFaulted, 1, 0) != 0)
            {
                Console.WriteLine($"[TRACE-RI-77] {TraceStamp()} ResponseLoopFault ALREADY-SIGNALED connectionId={ConnectionId} exType={ex.GetType().FullName} exMessage={ex.Message}");
                return;
            }

            Console.WriteLine($"[TRACE-RI-78] {TraceStamp()} ResponseLoopFault SIGNAL connectionId={ConnectionId} exType={ex.GetType().FullName} exMessage={ex.Message} cancelResponseLoop={cancelResponseLoop}");
            _responseLoopFault = ExceptionDispatchInfo.Capture(ex);
            TransitionState(TransitConnectionState.Faulted);
            LogTransitResponseLoopFaulted(_logger, ex, ConnectionId);
            SettleUnresolvedDirectSubmitWorkForFault(ex);
            _ = _completedQueue.Writer.TryComplete(ex);

            if (!cancelResponseLoop)
            {
                return;
            }

            try
            {
                _responseLoopCancellation?.Cancel();
            }
            catch
            {
            }
        }

        /// <summary>
        /// Parses one NNTP TAKETHIS response line into a publish result payload.
        /// </summary>
        /// <param name="responseLine">Raw server response line read from the transport.</param>
        /// <param name="responseAvailableTick">Timestamp captured when the response line became available to parse.</param>
        /// <returns>
        /// A mapped publish result when the response line is non-empty; otherwise <see langword="null"/>.
        /// </returns>
        /// <exception cref="InvalidOperationException">Thrown when Message-ID correlation cannot be resolved safely for the response.</exception>
        private TransitPublishResult? MapTakethisResponse(string responseLine, long responseAvailableTick)
        {
            if (string.IsNullOrWhiteSpace(responseLine))
            {
                return null;
            }

            (int code, string responseText) = TransitProtocolParser.ParseStatusCodeAndText(responseLine);
            string messageId = ResolveResponseMessageId(code, responseText, responseLine);

            TransitPublishStatus status = code switch
            {
                239 => TransitPublishStatus.Accepted,
                439 => TransitPublishStatus.Rejected,
                431 => TransitPublishStatus.Rejected,
                400 => TransitPublishStatus.Ambiguous,
                _ => TransitPublishStatus.Failed,
            };

            return new TransitPublishResult(
                MessageId: messageId,
                Status: status,
                ResponseCode: code,
                ResponseText: responseText,
                T4ResponseAvailableTick: responseAvailableTick,
                T5ResponseParsedTick: Stopwatch.GetTimestamp(),
                Provenance: code == 400 ? TransitPublishProvenance.Response400 : TransitPublishProvenance.OtherOrUnknown,
                ProvenanceConnectionId: ConnectionId,
                ProvenanceConnectionState: CurrentState,
                ProvenanceTick: responseAvailableTick);
        }

        /// <summary>
        /// Resolves the Message-ID used to correlate a response with pending submission state.
        /// </summary>
        /// <param name="code">Parsed NNTP status code.</param>
        /// <param name="responseText">Text component following the status code.</param>
        /// <param name="responseLine">Original response line for diagnostic exception context.</param>
        /// <returns>The correlated Message-ID token.</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown when a Message-ID token is missing and FIFO fallback correlation is unsafe or impossible.
        /// </exception>
        /// <remarks>
        /// For specific tokenless success responses (<c>239 Article transferred OK</c>), FIFO fallback correlation is permitted
        /// only when exactly one submission is outstanding.
        /// </remarks>
        private string ResolveResponseMessageId(int code, string responseText, string responseLine)
        {
            if (TryResolveLeadingMessageIdToken(responseText, out string? messageId)
                && messageId is not null)
            {
                return messageId;
            }

            if (code is 239 or 431 or 439)
            {
                bool tokenlessCorrelatable = code == 239
                    && string.Equals(responseText, "Article transferred OK", StringComparison.OrdinalIgnoreCase);
                if (!tokenlessCorrelatable)
                {
                    throw new InvalidOperationException($"TAKETHIS response omitted Message-ID token: '{responseLine}'.");
                }

                if (_pendingByMessageId.Count != 1)
                {
                    throw new InvalidOperationException($"TAKETHIS response omitted Message-ID token while {_pendingByMessageId.Count} submissions were outstanding; cannot safely correlate without Message-ID.");
                }

                while (_pendingBySendOrder.TryDequeue(out string? pendingId))
                {
                    if (string.IsNullOrWhiteSpace(pendingId))
                    {
                        continue;
                    }

                    if (_pendingByMessageId.ContainsKey(pendingId))
                    {
                        _ = Interlocked.Exchange(ref _tokenlessSuccessModeEnabled, 1);
                        return pendingId;
                    }
                }

                throw new InvalidOperationException($"TAKETHIS response omitted Message-ID token and no outstanding submission was available for FIFO correlation: '{responseLine}'.");
            }

            throw new InvalidOperationException($"Unable to resolve response Message-ID from line: {responseLine}");
        }

        /// <summary>
        /// Attempts to parse a leading Message-ID token from response text.
        /// </summary>
        /// <param name="responseText">Status text segment from a parsed NNTP response line.</param>
        /// <param name="messageId">When this method returns <see langword="true"/>, receives the parsed leading Message-ID token.</param>
        /// <returns><see langword="true"/> when a leading <c>&lt;...&gt;</c> token is present; otherwise <see langword="false"/>.</returns>
        private static bool TryResolveLeadingMessageIdToken(string responseText, [NotNullWhen(true)] out string? messageId)
        {
            messageId = null;

            if (string.IsNullOrWhiteSpace(responseText))
            {
                return false;
            }

            ReadOnlySpan<char> span = responseText.AsSpan().TrimStart();
            if (span.IsEmpty || span[0] != '<')
            {
                return false;
            }

            int terminatorIndex = span.IndexOf(' ');
            ReadOnlySpan<char> token = terminatorIndex < 0 ? span : span[..terminatorIndex];
            if (token.IsEmpty || token[0] != '<')
            {
                return false;
            }

            messageId = token.ToString();
            return true;
        }

        /// <summary>
        /// Advances the send-order queue past the specified Message-ID once that submission is settled.
        /// </summary>
        /// <param name="messageId">Message-ID that has been correlated and should be acknowledged in send order.</param>
        private void AcknowledgeSendOrder(string messageId)
        {
            while (_pendingBySendOrder.TryPeek(out string? queued))
            {
                if (string.Equals(queued, messageId, StringComparison.Ordinal))
                {
                    _ = _pendingBySendOrder.TryDequeue(out _);
                    return;
                }

                _ = _pendingBySendOrder.TryDequeue(out _);
            }
        }

        /// <summary>
        /// Writes one complete TAKETHIS frame into the pipe writer, including command line, dot-stuffed payload, and terminator.
        /// </summary>
        /// <param name="writer">Destination protocol writer for staged bytes.</param>
        /// <param name="messageId">Message-ID token appended to the TAKETHIS command.</param>
        /// <param name="articlePayload">Raw article bytes to dot-stuff before framing termination.</param>
        /// <returns>Total number of bytes staged for this frame.</returns>
        private int StageTakethisFrame(PipeWriter writer, string messageId, ReadOnlyMemory<byte> articlePayload)
        {
            int commandLength = WriteTakethisCommand(writer, messageId);
            WriteBytes(writer, CrLfBytes);

            long dotStuffStageStartTick = Stopwatch.GetTimestamp();
            DotStuffWriteMetrics dotStuffMetrics = WriteDotStuffedArticle(writer, articlePayload);
            _timingCollector?.RecordDotStuffStage(
                elapsedTicks: Stopwatch.GetTimestamp() - dotStuffStageStartTick,
                payloadBytes: articlePayload.Length,
                getSpanCalls: dotStuffMetrics.GetSpanCalls,
                advanceCalls: dotStuffMetrics.AdvanceCalls,
                stuffedDotEvents: dotStuffMetrics.StuffedDotEvents);

            WriteBytes(writer, DotTerminatorBytes);

            return commandLength + CrLfBytes.Length + dotStuffMetrics.BytesWritten + DotTerminatorBytes.Length;
        }

        /// <summary>
        /// Writes the ASCII TAKETHIS command prefix and Message-ID token into the destination writer.
        /// </summary>
        /// <param name="writer">Destination protocol writer.</param>
        /// <param name="messageId">Message-ID token to append after the command verb.</param>
        /// <returns>Number of bytes written for the command line excluding trailing CRLF.</returns>
        private static int WriteTakethisCommand(PipeWriter writer, string messageId)
        {
            int messageIdByteCount = Encoding.ASCII.GetByteCount(messageId);
            int totalBytes = TakethisPrefixBytes.Length + messageIdByteCount;
            Span<byte> destination = writer.GetSpan(totalBytes)[..totalBytes];

            TakethisPrefixBytes.CopyTo(destination);
            _ = Encoding.ASCII.GetBytes(messageId, destination[TakethisPrefixBytes.Length..]);

            writer.Advance(totalBytes);
            return totalBytes;
        }

        /// <summary>
        /// Dot-stuffs article payload bytes and stages the transformed payload into the destination writer.
        /// </summary>
        /// <param name="writer">Destination protocol writer.</param>
        /// <param name="payload">Original article bytes prior to NNTP dot-stuffing transformation.</param>
        /// <returns>Metrics describing bytes staged and transformation counters for timing instrumentation.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the destination span cannot accommodate the computed transform.</exception>
        private static DotStuffWriteMetrics WriteDotStuffedArticle(PipeWriter writer, ReadOnlyMemory<byte> payload)
        {
            ReadOnlySpan<byte> source = payload.Span;
            int requiredLength = TransitDotStuffing.GetRequiredDestinationLength(source, appendTrailingCrlfWhenMissingLf: true, out int stuffedDotCount);
            Span<byte> destination = writer.GetSpan(requiredLength)[..requiredLength];

            if (!TransitDotStuffing.TryDotStuff(
                source,
                destination,
                out TransitDotStuffTransformResult transform,
                algorithm: TransitDotStuffingAlgorithm.BulkLineOrientedSinglePass,
                appendTrailingCrlfWhenMissingLf: true))
            {
                throw new InvalidOperationException("Unable to stage dot-stuffed payload due to insufficient writer destination span.");
            }

            writer.Advance(transform.BytesWritten);

            return new DotStuffWriteMetrics(
                BytesWritten: transform.BytesWritten,
                GetSpanCalls: 1,
                AdvanceCalls: 1,
                StuffedDotEvents: stuffedDotCount);
        }

        /// <summary>
        /// Copies an already prepared byte span into the destination writer and advances the writer cursor.
        /// </summary>
        /// <param name="writer">Destination protocol writer.</param>
        /// <param name="bytes">Bytes to stage without additional transformation.</param>
        private static void WriteBytes(PipeWriter writer, ReadOnlySpan<byte> bytes)
        {
            Span<byte> destination = writer.GetSpan(bytes.Length);
            bytes.CopyTo(destination);
            writer.Advance(bytes.Length);
        }

        /// <summary>
        /// Reads one NNTP line from the current protocol reader and updates received-byte accounting.
        /// </summary>
        /// <param name="cancellationToken">Token used to cancel the read operation.</param>
        /// <returns>The decoded NNTP response line without trailing CRLF.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the protocol reader has not been initialized.</exception>
        private async ValueTask<string> ReadLineAsync(CancellationToken cancellationToken)
        {
            PipeReader reader = _reader ?? throw new InvalidOperationException("Transit protocol reader is not initialized.");
            (string line, int bytesRead) = await TransitProtocolParser.ReadNntpLineWithByteCountAsync(reader, cancellationToken).ConfigureAwait(false);
            _ = Interlocked.Add(ref _bytesReceived, bytesRead);
            return line;
        }

        /// <summary>
        /// Executes one initialization stage with linked cancellation and per-stage timeout enforcement.
        /// </summary>
        /// <param name="operation">Asynchronous stage operation to execute.</param>
        /// <param name="stageName">Stage label used for diagnostics and timeout error messages.</param>
        /// <param name="cancellationToken">External initialization cancellation token.</param>
        /// <returns>A task that completes when the stage operation completes within timeout.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="operation"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="stageName"/> is null, empty, or whitespace.</exception>
        /// <exception cref="TransitConnectionLifecycleException">Thrown when the stage exceeds the configured initialization timeout.</exception>
        private async Task AwaitInitializationStageAsync(
            Func<CancellationToken, Task> operation,
            string stageName,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(operation);
            ArgumentException.ThrowIfNullOrWhiteSpace(stageName);

            Console.WriteLine($"[TRACE-RI-69] {TraceStamp()} InitStage START connectionId={ConnectionId} stage='{stageName}' timeoutMs={_responseProgressTimeout.TotalMilliseconds:F0}");
            using CancellationTokenSource stageTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            stageTimeout.CancelAfter(_responseProgressTimeout);

            try
            {
                await operation(stageTimeout.Token).ConfigureAwait(false);
                Console.WriteLine($"[TRACE-RI-70] {TraceStamp()} InitStage COMPLETE connectionId={ConnectionId} stage='{stageName}'");
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && stageTimeout.IsCancellationRequested)
            {
                Console.WriteLine($"[TRACE-RI-71] {TraceStamp()} InitStage TIMEOUT connectionId={ConnectionId} stage='{stageName}'");
                throw new TransitConnectionLifecycleException(TransitConnectionLifecycleFailure.InitializationProgressTimeout, stageName);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[TRACE-RI-72] {TraceStamp()} InitStage EXCEPTION connectionId={ConnectionId} stage='{stageName}' exType={ex.GetType().FullName} exMessage={ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Executes one value-producing initialization stage with linked cancellation and per-stage timeout enforcement.
        /// </summary>
        /// <typeparam name="T">Result type produced by the stage operation.</typeparam>
        /// <param name="operation">Asynchronous stage operation to execute.</param>
        /// <param name="stageName">Stage label used for diagnostics and timeout error messages.</param>
        /// <param name="cancellationToken">External initialization cancellation token.</param>
        /// <returns>The value produced by <paramref name="operation"/> when it completes within timeout.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="operation"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="stageName"/> is null, empty, or whitespace.</exception>
        /// <exception cref="TransitConnectionLifecycleException">Thrown when the stage exceeds the configured initialization timeout.</exception>
        private async Task<T> AwaitInitializationStageAsync<T>(
            Func<CancellationToken, Task<T>> operation,
            string stageName,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(operation);
            ArgumentException.ThrowIfNullOrWhiteSpace(stageName);

            Console.WriteLine($"[TRACE-RI-73] {TraceStamp()} InitStageT START connectionId={ConnectionId} stage='{stageName}' timeoutMs={_responseProgressTimeout.TotalMilliseconds:F0}");
            using CancellationTokenSource stageTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            stageTimeout.CancelAfter(_responseProgressTimeout);

            try
            {
                T value = await operation(stageTimeout.Token).ConfigureAwait(false);
                Console.WriteLine($"[TRACE-RI-74] {TraceStamp()} InitStageT COMPLETE connectionId={ConnectionId} stage='{stageName}'");
                return value;
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && stageTimeout.IsCancellationRequested)
            {
                Console.WriteLine($"[TRACE-RI-75] {TraceStamp()} InitStageT TIMEOUT connectionId={ConnectionId} stage='{stageName}'");
                throw new TransitConnectionLifecycleException(TransitConnectionLifecycleFailure.InitializationProgressTimeout, stageName);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[TRACE-RI-76] {TraceStamp()} InitStageT EXCEPTION connectionId={ConnectionId} stage='{stageName}' exType={ex.GetType().FullName} exMessage={ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Reads CAPABILITIES response lines until the NNTP terminator line is encountered.
        /// </summary>
        /// <param name="cancellationToken">Token used to cancel response reading.</param>
        /// <returns>The ordered response lines including the terminal <c>.</c> line.</returns>
        private async Task<IReadOnlyList<string>> ReadCapabilitiesLinesAsync(CancellationToken cancellationToken)
        {
            List<string> responseLines = [];
            while (true)
            {
                string line = await ReadLineAsync(cancellationToken).ConfigureAwait(false);
                responseLines.Add(line);
                if (line == ".")
                {
                    break;
                }
            }

            return responseLines;
        }

        /// <summary>
        /// Issues CAPABILITIES and parses the server response into a capability snapshot.
        /// </summary>
        /// <param name="cancellationToken">Token used to cancel command write and response read operations.</param>
        /// <returns>The parsed capability snapshot for the current transport mode.</returns>
        private async Task<TransitCapabilitySnapshot> ReadCapabilitiesAsync(CancellationToken cancellationToken)
        {
            await WriteCommandAsync("CAPABILITIES", cancellationToken).ConfigureAwait(false);
            IReadOnlyList<string> lines = await ReadCapabilitiesLinesAsync(cancellationToken).ConfigureAwait(false);
            return TransitProtocolParser.ParseCapabilitiesResponse(lines);
        }

        /// <summary>
        /// Writes an ASCII NNTP command line to the active write stream and flushes it.
        /// </summary>
        /// <param name="command">Command verb and arguments without trailing CRLF.</param>
        /// <param name="cancellationToken">Token used to cancel stream write and flush operations.</param>
        /// <returns>A task that completes when the command has been flushed to the transport stream.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the transport write stream is not initialized.</exception>
        private async Task WriteCommandAsync(string command, CancellationToken cancellationToken)
        {
            Stream writeStream = _writeStream ?? throw new InvalidOperationException("Transit transport write stream is not initialized.");
            byte[] commandBytes = Encoding.ASCII.GetBytes(command + "\r\n");
            await writeStream.WriteAsync(commandBytes, cancellationToken).ConfigureAwait(false);
            await writeStream.FlushAsync(cancellationToken).ConfigureAwait(false);
            _ = Interlocked.Add(ref _bytesTransmitted, commandBytes.Length);
        }

        /// <summary>
        /// Negotiates STARTTLS on an established plaintext session, then upgrades transport streams to TLS.
        /// </summary>
        /// <param name="cancellationToken">Token used to cancel command/response exchange and TLS authentication.</param>
        /// <returns>A task that completes when the TLS upgrade succeeds.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the server does not return status code <c>382</c> for STARTTLS.</exception>
        private async Task StartTlsAsync(CancellationToken cancellationToken)
        {
            await WriteCommandAsync("STARTTLS", cancellationToken).ConfigureAwait(false);
            string startTlsResponse = await ReadLineAsync(cancellationToken).ConfigureAwait(false);
            (int code, _) = TransitProtocolParser.ParseStatusCodeAndText(startTlsResponse);

            if (code != 382)
            {
                throw new InvalidOperationException($"Unexpected STARTTLS response code: {code}.");
            }

            await UpgradeToTlsAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Wraps the current transport stream in an <see cref="SslStream"/> and authenticates as an NNTP TLS client.
        /// </summary>
        /// <param name="cancellationToken">Token used to cancel TLS client authentication.</param>
        /// <returns>A task that completes after stream replacement and TLS activation state updates.</returns>
        /// <exception cref="InvalidOperationException">Thrown when no transport stream is available to upgrade.</exception>
        /// <remarks>
        /// TLS protocol negotiation is restricted to TLS 1.2 and TLS 1.3. When no explicit
        /// <c>RemoteCertificateValidationCallback</c> is provided, platform-default certificate validation is used.
        /// </remarks>
        private async Task UpgradeToTlsAsync(CancellationToken cancellationToken)
        {
            if (_transportStream is null)
            {
                throw new InvalidOperationException("Transit transport stream is not initialized.");
            }

            SslStream sslStream = _serverCertificateValidationCallback is null
                ? new SslStream(_transportStream, leaveInnerStreamOpen: true)
                : new SslStream(_transportStream, leaveInnerStreamOpen: true, _serverCertificateValidationCallback);

            SslClientAuthenticationOptions options = new()
            {
                TargetHost = _host,
                EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
            };

            if (_serverCertificateValidationCallback is not null)
            {
                options.RemoteCertificateValidationCallback = _serverCertificateValidationCallback;
            }
            await sslStream.AuthenticateAsClientAsync(options, cancellationToken).ConfigureAwait(false);

            _readStream = sslStream;
            _writeStream = sslStream;
            _transportStream = sslStream;
            IsTlsActive = true;
        }

        /// <summary>
        /// Atomically raises the recorded peak concurrent-submission counter when a new maximum is observed.
        /// </summary>
        /// <param name="currentConcurrent">Current in-flight submission count observed by the caller.</param>
        private void ObserveMaxConcurrentSubmissions(int currentConcurrent)
        {
            while (true)
            {
                int observed = Volatile.Read(ref _maxConcurrentSubmissions);
                if (currentConcurrent <= observed)
                {
                    return;
                }

                if (Interlocked.CompareExchange(ref _maxConcurrentSubmissions, currentConcurrent, observed) == observed)
                {
                    return;
                }
            }
        }

        /// <summary>
        /// Atomically raises the recorded peak writer batch size when a larger batch is observed.
        /// </summary>
        /// <param name="batchSize">Number of work items staged in the current batch.</param>
        private void UpdateMaxBatchSize(int batchSize)
        {
            while (true)
            {
                int observed = Volatile.Read(ref _maxWriterBatchSize);
                if (batchSize <= observed)
                {
                    return;
                }

                if (Interlocked.CompareExchange(ref _maxWriterBatchSize, batchSize, observed) == observed)
                {
                    return;
                }
            }
        }

        /// <summary>
        /// Updates per-status submission counters for settled publish outcomes.
        /// </summary>
        /// <param name="status">Settled publish status to account for.</param>
        /// <remarks>
        /// <see cref="TransitPublishStatus.Queued"/> and <see cref="TransitPublishStatus.Canceled"/> are intentionally
        /// not counted in aggregate acceptance/rejection/failure/ambiguity totals.
        /// </remarks>
        private void RecordSubmissionResult(TransitPublishStatus status)
        {
            switch (status)
            {
                case TransitPublishStatus.Accepted:
                    _ = Interlocked.Increment(ref _submissionsAccepted);
                    break;
                case TransitPublishStatus.Rejected:
                    _ = Interlocked.Increment(ref _submissionsRejected);
                    break;
                case TransitPublishStatus.Failed:
                    _ = Interlocked.Increment(ref _submissionsFailed);
                    break;
                case TransitPublishStatus.Ambiguous:
                    _ = Interlocked.Increment(ref _submissionsAmbiguous);
                    break;
                case TransitPublishStatus.Unavailable:
                    _ = Interlocked.Increment(ref _submissionsUnavailable);
                    break;
                case TransitPublishStatus.Queued:
                case TransitPublishStatus.Canceled:
                    break;
                default:
                    break;
            }
        }

        /// <summary>
        /// Assigns a new lifecycle state and emits the corresponding state-transition log event.
        /// </summary>
        /// <param name="state">Next lifecycle state for this connection instance.</param>
        private void TransitionState(TransitConnectionState state)
        {
            CurrentState = state;
            LogTransitStateTransition(_logger, ConnectionId, state);
        }

        /// <summary>
        /// Shuts down the connection, settles unresolved owned work, and releases all owned resources.
        /// </summary>
        /// <returns>A value task representing asynchronous disposal and teardown completion.</returns>
        /// <remarks>
        /// Disposal is idempotent. The first call requests shutdown, cancels background loops, optionally sends QUIT,
        /// settles remaining pending work as ambiguous, completes channels/pipes, disposes transport artifacts, and
        /// transitions state to <see cref="TransitConnectionState.Disconnected"/>.
        /// </remarks>
        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _shutdownRequested, 1) != 0)
            {
                return;
            }

            TransitConnectionState stateBeforeShutdown = CurrentState;

            try
            {
                TransitionState(TransitConnectionState.Disconnecting);

                CancellationTokenSource? responseLoopCancellation = _responseLoopCancellation;
                responseLoopCancellation?.Cancel();

                CancellationTokenSource? responseProgressWatchdogCancellation = _responseProgressWatchdogCancellation;
                responseProgressWatchdogCancellation?.Cancel();

                Task? responseLoopTask = _responseLoopTask;
                Task? responseProgressWatchdogTask = _responseProgressWatchdogTask;
                if (responseLoopTask is not null || responseProgressWatchdogTask is not null)
                {
                    List<Task> lifecycleTasks = [];
                    if (responseLoopTask is not null)
                    {
                        lifecycleTasks.Add(responseLoopTask);
                    }

                    if (responseProgressWatchdogTask is not null)
                    {
                        lifecycleTasks.Add(responseProgressWatchdogTask);
                    }

                    try
                    {
                        Task allTasks = Task.WhenAll(lifecycleTasks);
                        using CancellationTokenSource stopWait = new(TimeSpan.FromSeconds(5));
                        await allTasks.WaitAsync(stopWait.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                }

                SettleUnresolvedOwnedWorkDuringDispose();

                if (stateBeforeShutdown is not TransitConnectionState.Faulted and not TransitConnectionState.Disconnected)
                {
                    try
                    {
                        await WriteCommandAsync("QUIT", CancellationToken.None).ConfigureAwait(false);
                    }
                    catch
                    {
                    }
                }
            }
            finally
            {
                try
                {
                    if (_reader is not null)
                    {
                        await _reader.CompleteAsync().ConfigureAwait(false);
                    }
                }
                catch
                {
                }

                try
                {
                    if (_writer is not null)
                    {
                        await _writer.CompleteAsync().ConfigureAwait(false);
                    }
                }
                catch
                {
                }

                _readStream?.Dispose();
                _writeStream?.Dispose();
                _transportStream?.Dispose();
                _tcpClient?.Dispose();

                _ = _completedQueue.Writer.TryComplete();
                _responseLoopCancellation?.Dispose();
                _responseProgressWatchdogCancellation?.Dispose();
                _writeGate.Dispose();
                _tokenlessCorrelationGate.Dispose();

                _readStream = null;
                _writeStream = null;
                _transportStream = null;
                _tcpClient = null;
                _reader = null;
                _writer = null;
                _responseLoopTask = null;
                _responseLoopCancellation = null;
                _responseProgressWatchdogTask = null;
                _responseProgressWatchdogCancellation = null;
                TransitionState(TransitConnectionState.Disconnected);
            }
        }

        /// <summary>
        /// Settles all remaining owned pending work as ambiguous during shutdown.
        /// </summary>
        /// <remarks>
        /// This path enqueues completion notifications because ownership remains with this connection during disposal.
        /// </remarks>
        private void SettleUnresolvedOwnedWorkDuringDispose()
        {
            IReadOnlyList<PendingOwnedWork> unresolved = DrainOwnedPendingWork(static _ => true);
            SettlePendingAsAmbiguous(unresolved, TransitPublishProvenance.Shutdown, "Transit connection shutdown before definitive TAKETHIS response.", enqueueCompletion: true);
        }

        /// <summary>
        /// Settles direct-submit pending work as ambiguous when a response-loop fault occurs.
        /// </summary>
        /// <param name="ex">Fault that determines ambiguity provenance classification.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="ex"/> is <see langword="null"/>.</exception>
        private void SettleUnresolvedDirectSubmitWorkForFault(Exception ex)
        {
            ArgumentNullException.ThrowIfNull(ex);

            IReadOnlyList<PendingOwnedWork> unresolved = DrainOutstandingDirectSubmitPendingWork();
            if (unresolved.Count == 0)
            {
                return;
            }

            TransitPublishProvenance provenance = ex is IOException or SocketException
                ? TransitPublishProvenance.ConnectionClose
                : TransitPublishProvenance.ResponseLoopFailure;

            SettlePendingAsAmbiguous(
                unresolved,
                provenance,
                "Transit connection closed before definitive TAKETHIS response.",
                enqueueCompletion: false);
        }

        /// <summary>
        /// Creates ambiguous publish results for unresolved pending work and settles associated completion paths.
        /// </summary>
        /// <param name="unresolved">Pending work entries to settle.</param>
        /// <param name="provenance">Provenance classification to stamp on each ambiguous result.</param>
        /// <param name="responseText">Response text used in synthesized ambiguous results.</param>
        /// <param name="enqueueCompletion">
        /// <see langword="true"/> to enqueue completion-channel entries; otherwise only direct-submit completions are signaled.
        /// </param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="unresolved"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="responseText"/> is null, empty, or whitespace.</exception>
        private void SettlePendingAsAmbiguous(
            IReadOnlyList<PendingOwnedWork> unresolved,
            TransitPublishProvenance provenance,
            string responseText,
            bool enqueueCompletion)
        {
            ArgumentNullException.ThrowIfNull(unresolved);
            ArgumentException.ThrowIfNullOrWhiteSpace(responseText);

            if (unresolved.Count == 0)
            {
                return;
            }

            long settledAtTick = Stopwatch.GetTimestamp();
            foreach (PendingOwnedWork pending in unresolved)
            {
                TransitPublishResult ambiguous = new(
                    MessageId: pending.WorkItem.MessageId,
                    Status: TransitPublishStatus.Ambiguous,
                    ResponseCode: null,
                    ResponseText: responseText,
                    T2SocketWriteBeginTick: pending.T2SocketWriteBeginTick,
                    T3SocketWriteEndTick: pending.T3SocketWriteEndTick,
                    T6ResponseCorrelatedTick: settledAtTick,
                    Provenance: provenance,
                    ProvenanceConnectionId: ConnectionId,
                    ProvenanceConnectionState: CurrentState,
                    ProvenanceTick: settledAtTick);

                RecordSubmissionResult(ambiguous.Status);
                if (_timingCollector is not null)
                {
                    _completionEnqueuedTicks[pending.WorkItem.WorkItemId] = Stopwatch.GetTimestamp();
                }

                if (enqueueCompletion)
                {
                    _ = _completedQueue.Writer.TryWrite(new CompletedWork(pending.WorkItem, ambiguous));
                }

                TryCompleteDirectSubmit(pending.WorkItem.WorkItemId, ambiguous);
            }
        }

        /// <summary>
        /// Completes a pending direct-submit completion source when one is registered for the specified work item.
        /// </summary>
        /// <param name="workItemId">Work-item identifier used as the direct-submit completion key.</param>
        /// <param name="result">Result value to publish to the waiting direct-submit caller.</param>
        private void TryCompleteDirectSubmit(long workItemId, TransitPublishResult result)
        {
            if (_directSubmitCompletions.TryRemove(workItemId, out TaskCompletionSource<TransitPublishResult>? directCompletion))
            {
                _ = directCompletion.TrySetResult(result);
            }
        }

        /// <summary>
        /// Immutable diagnostics payload describing connection state, counters, and sampled publish pipeline data.
        /// </summary>
        internal sealed record TransitConnectionDiagnosticsSnapshot(
            string ConnectionId,
            string Host,
            int Port,
            TransitConnectionState CurrentState,
            bool IsTlsActive,
            long SocketOpenCount,
            long ReadyTransitionCount,
            long SubmissionsStarted,
            long SubmissionsAccepted,
            long SubmissionsRejected,
            long SubmissionsAmbiguous,
            long SubmissionsUnavailable,
            long SubmissionsFailed,
            long BytesTransmitted,
            long BytesReceived,
            int MaxConcurrentSubmissions,
            int CurrentConcurrentSubmissions,
            long CurrentWriteIntentQueueDepth,
            string? LocalEndpoint,
            string? RemoteEndpoint,
            PipeliningDiagnosticSummary DiagnosticsSummary,
            DiagnosticOperationRecord[] DiagnosticSampleRecords,
            OutstandingPublishOperationSnapshot[] OutstandingOperations);

        /// <summary>
        /// Immutable provenance payload for analyzing first-greeting (P1) lifecycle timing and event ordering.
        /// </summary>
        internal sealed record P1GreetingProvenanceSnapshot(
            string ConnectionId,
            string Host,
            int Port,
            int InitializationAttemptId,
            string? LocalIp,
            int LocalPort,
            string? RemoteIp,
            int RemotePort,
            long CapturedAtTick,
            long? ConnectedAtTick,
            long? PipesCreatedAtTick,
            long? AwaitingGreetingAtTick,
            DateTimeOffset? ConnectedAtUtc,
            DateTimeOffset? P1AtUtc,
            bool LocalDisposeAsyncBeforeP1,
            bool LocalResetTransportStateBeforeP1,
            bool LocalDisposeTransportArtifactsBeforeP1,
            bool LocalRebuildPipesBeforeP1,
            bool LocalCleanupFailedInitializationBeforeP1,
            bool InitializationCancellationBeforeP1,
            P1GreetingLifecycleEventRecord[] LifecycleEvents);

        /// <summary>
        /// Enumerates lifecycle markers captured while collecting first-greeting provenance data.
        /// </summary>
        internal enum P1GreetingProvenanceLifecycleEvent
        {
            Connected = 1,
            RebuildPipes = 2,
            PipesCreated = 3,
            AwaitingGreeting = 4,
            ResetTransport = 5,
            CleanupFailedInitialization = 6,
            DisposeAsync = 7,
            DisposeTransportArtifacts = 8,
            Cancellation = 9,
            P1GreetingEof = 10,
        }

        /// <summary>
        /// One timestamped lifecycle event entry within a <see cref="P1GreetingProvenanceSnapshot"/>.
        /// </summary>
        internal readonly record struct P1GreetingLifecycleEventRecord(
            P1GreetingProvenanceLifecycleEvent Event,
            long Tick,
            int AttemptId);

        /// <summary>
        /// Detailed per-operation timing sample captured for pipeline diagnostics.
        /// </summary>
        internal readonly record struct DiagnosticOperationRecord(
            string MessageId,
            long T0SubmitEnterTick,
            long T0SubmitTakethisEnterTick,
            long T1PendingRegisteredTick,
            long T2WriteIntentEnqueueStartTick,
            long T2WriteIntentEnqueuedTick,
            long T2BeforeCompletionAwaitTick,
            long T3WriterDequeuedTick,
            long T4AssignedToBatchTick,
            long T5FrameStageBeginTick,
            long T6FrameStageEndTick,
            long T7BatchFlushBeginTick,
            long T8BatchFlushEndTick,
            long T9ResponseCorrelatedTick,
            long T10SubmitCompletionTick,
            long PendingDepthAtT1,
            long PendingDepthAtT2,
            long PendingDepthAtT3,
            long PendingDepthAtT4,
            long PendingDepthAtT9,
            long QueueDepthAtT2,
            long QueueDepthAtT3,
            long QueueDepthAtBatchStart,
            int BatchDequeuedCount,
            long QueueDepthAtT9,
            long BatchId,
            int BatchPosition,
            int BatchSize,
            long SendSequence,
            long LogicalOutstandingAheadAtResponse);

        /// <summary>
        /// Snapshot of one currently outstanding publish operation and its known pipeline milestones.
        /// </summary>
        internal readonly record struct OutstandingPublishOperationSnapshot(
            string MessageId,
            long T2WriteIntentEnqueuedTick,
            long T6FrameStageEndTick,
            long T8BatchFlushEndTick,
            long T9ResponseCorrelatedTick,
            bool WriteIntentEnqueued,
            bool TakethisStagedForWrite,
            bool FlushCompleted,
            bool WaitingFor239Response,
            bool CompletionTaskIsCompleted,
            string CompletionTaskStatus,
            TransitPublishStatus? CompletionStatus,
            string LikelyAwaitingPath);

        /// <summary>
        /// Aggregate pipeline statistics derived from connection-level publish activity.
        /// </summary>
        internal readonly record struct PipeliningDiagnosticSummary(
            long MaxPendingDepth,
            long MaxWriteQueueDepth,
            int MaxWriterBatchSize,
            double AverageWriterBatchSize,
            double P50WriterBatchSize,
            double P95WriterBatchSize,
            double P99WriterBatchSize,
            long NumberOfBatches,
            string BatchSizeHistogram,
            long[] BatchSizeCounts,
            double AverageCoalescingWaitMicroseconds,
            double P50CoalescingWaitMicroseconds,
            double P95CoalescingWaitMicroseconds,
            double P99CoalescingWaitMicroseconds,
            long MaxLogicalOutstandingAheadAtResponse,
            long CapturedOperationCount,
            int SampledOperationCount);

        /// <summary>
        /// Mutable correlation entry that pairs a pending work item with send/response timing milestones.
        /// </summary>
        private sealed class PendingOwnedWork
        {
            /// <summary>
            /// Initializes a new pending-work correlation entry.
            /// </summary>
            /// <param name="workItem">Work item currently owned by this connection and awaiting settlement.</param>
            internal PendingOwnedWork(TransitWorkItem workItem)
            {
                WorkItem = workItem;
            }

            /// <summary>
            /// Gets the pending work item associated with this correlation entry.
            /// </summary>
            internal TransitWorkItem WorkItem { get; }

            /// <summary>
            /// Timestamp marking the beginning of socket write staging for this work item.
            /// </summary>
            internal long T2SocketWriteBeginTick;

            /// <summary>
            /// Timestamp marking completion of socket write staging for this work item.
            /// </summary>
            internal long T3SocketWriteEndTick;

            /// <summary>
            /// Timestamp marking correlation of a server response for this work item.
            /// </summary>
            internal long T6ResponseCorrelatedTick;

            /// <summary>
            /// Monotonic send-sequence assigned during frame staging.
            /// </summary>
            internal long SendSequence;
        }

        /// <summary>
        /// Immutable completion tuple pairing a settled work item with its publish result.
        /// </summary>
        private sealed record CompletedWork(TransitWorkItem WorkItem, TransitPublishResult Result);

        /// <summary>
        /// Metrics emitted by payload dot-stuff staging for instrumentation.
        /// </summary>
        private readonly record struct DotStuffWriteMetrics(
            int BytesWritten,
            long GetSpanCalls,
            long AdvanceCalls,
            long StuffedDotEvents);

        /// <summary>
        /// Classifies lifecycle failures raised by transit-connection initialization and submission helpers.
        /// </summary>
        internal enum TransitConnectionLifecycleFailure
        {
            WriterNotInitialized,
            WriterCompletedDuringTakethisSubmission,
            InitializationProgressTimeout,
            WriterDisposedDuringTakethisSubmission,
        }

        /// <summary>
        /// Exception type used to surface classified transit-connection lifecycle failures.
        /// </summary>
        internal sealed class TransitConnectionLifecycleException : InvalidOperationException
        {
            /// <summary>
            /// Initializes a new lifecycle exception with a message derived from the supplied failure classification.
            /// </summary>
            /// <param name="failure">Lifecycle failure classification that determines the base exception message.</param>
            /// <param name="stageName">Optional stage label used by timeout failure messages.</param>
            internal TransitConnectionLifecycleException(TransitConnectionLifecycleFailure failure, string? stageName = null)
                : base(failure switch
                {
                    TransitConnectionLifecycleFailure.WriterNotInitialized => "Transit protocol writer is not initialized.",
                    TransitConnectionLifecycleFailure.WriterCompletedDuringTakethisSubmission => "Transit protocol writer completed during TAKETHIS submission.",
                    TransitConnectionLifecycleFailure.InitializationProgressTimeout => $"Transit connection initialization timed out while awaiting {stageName ?? "protocol progress"}.",
                    TransitConnectionLifecycleFailure.WriterDisposedDuringTakethisSubmission => "Transit protocol writer was disposed during TAKETHIS submission.",
                    _ => throw new ArgumentOutOfRangeException(nameof(failure), failure, "Unknown transit lifecycle failure."),
                })
            {
                Failure = failure;
            }

            /// <summary>
            /// Gets the lifecycle failure classification associated with this exception instance.
            /// </summary>
            internal TransitConnectionLifecycleFailure Failure { get; }
        }

        /// <summary>
        /// Emits the transit state transition log event for transit connection.
        /// </summary>
        [LoggerMessage(EventId = 2210, Level = LogLevel.Debug, Message = "Transit connection {ConnectionId} state changed to {State}")]
        private static partial void LogTransitStateTransition(ILogger logger, string connectionId, TransitConnectionState state);

        /// <summary>
        /// Emits the transit capabilities log event for transit connection.
        /// </summary>
        [LoggerMessage(EventId = 2211, Level = LogLevel.Information, Message = "Transit connection {ConnectionId} capabilities: STARTTLS={SupportsStartTls}, STREAMING={SupportsStreaming}")]
        private static partial void LogTransitCapabilities(ILogger logger, string connectionId, bool supportsStartTls, bool supportsStreaming);

        /// <summary>
        /// Emits the transit connection ready log event for transit connection.
        /// </summary>
        [LoggerMessage(EventId = 2212, Level = LogLevel.Information, Message = "Transit connection {ConnectionId} is ready (TLS={TlsActive})")]
        private static partial void LogTransitConnectionReady(ILogger logger, string connectionId, bool tlsActive);

        /// <summary>
        /// Emits the transit response loop faulted log event for transit connection.
        /// </summary>
        [LoggerMessage(EventId = 2213, Level = LogLevel.Warning, Message = "Transit connection {ConnectionId} response loop faulted")]
        private static partial void LogTransitResponseLoopFaulted(ILogger logger, Exception exception, string connectionId);
    }
}
