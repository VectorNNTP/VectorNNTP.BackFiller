// <copyright file="TransitConnection.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Transit
// Implements the transit connection behavior.

using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
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
    /// Manages one outbound NNTP transit connection lifecycle and per-connection response correlation.
    /// </summary>
    internal sealed partial class TransitConnection : IAsyncDisposable
    {
        /// <summary>
        /// Stores cr lf bytes for transit connection.
        /// </summary>
        private static readonly byte[] CrLfBytes = "\r\n"u8.ToArray();
        /// <summary>
        /// Stores dot terminator bytes for transit connection.
        /// </summary>
        private static readonly byte[] DotTerminatorBytes = ".\r\n"u8.ToArray();
        /// <summary>
        /// Stores takethis prefix bytes for transit connection.
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
        /// Stores host used by transit connection.
        /// </summary>
        private readonly string _host;
        /// <summary>
        /// Stores port used by transit connection.
        /// </summary>
        private readonly int _port;
        /// <summary>
        /// Stores use ssl used by transit connection.
        /// </summary>
        private readonly bool _useSsl;
        /// <summary>
        /// Supplies the logger used by transit connection.
        /// </summary>
        private readonly ILogger _logger;
        /// <summary>
        /// Stores server certificate validation callback used by transit connection.
        /// </summary>
        private readonly RemoteCertificateValidationCallback? _serverCertificateValidationCallback;
        /// <summary>
        /// Stores pipeline depth used by transit connection.
        /// </summary>
        private readonly int _pipelineDepth;
        /// <summary>
        /// Stores write batch coalesce microseconds used by transit connection.
        /// </summary>
        private readonly int _writeBatchCoalesceMicroseconds;
        /// <summary>
        /// Configures response progress timeout for transit connection.
        /// </summary>
        private readonly TimeSpan _responseProgressTimeout;
        /// <summary>
        /// Configures response progress check interval for transit connection.
        /// </summary>
        private readonly TimeSpan _responseProgressCheckInterval;
        /// <summary>
        /// Stores timing collector used by transit connection.
        /// </summary>
        private readonly TransitTimingCollector? _timingCollector;

        /// <summary>
        /// Stores write gate used by transit connection.
        /// </summary>
        private readonly SemaphoreSlim _writeGate = new(1, 1);
        /// <summary>
        /// Stores tokenless correlation gate used by transit connection.
        /// </summary>
        private readonly SemaphoreSlim _tokenlessCorrelationGate = new(1, 1);

        /// <summary>
        /// Stores tcp client used by transit connection.
        /// </summary>
        private TcpClient? _tcpClient;
        /// <summary>
        /// Stores transport stream used by transit connection.
        /// </summary>
        private Stream? _transportStream;
        /// <summary>
        /// Stores read stream used by transit connection.
        /// </summary>
        private Stream? _readStream;
        /// <summary>
        /// Stores write stream used by transit connection.
        /// </summary>
        private Stream? _writeStream;
        /// <summary>
        /// Stores reader used by transit connection.
        /// </summary>
        private PipeReader? _reader;
        /// <summary>
        /// Stores writer used by transit connection.
        /// </summary>
        private PipeWriter? _writer;

        /// <summary>
        /// Stores response loop cancellation used by transit connection.
        /// </summary>
        private CancellationTokenSource? _responseLoopCancellation;
        /// <summary>
        /// Stores response loop task used by transit connection.
        /// </summary>
        private Task? _responseLoopTask;
        /// <summary>
        /// Stores response progress watchdog cancellation used by transit connection.
        /// </summary>
        private CancellationTokenSource? _responseProgressWatchdogCancellation;
        /// <summary>
        /// Stores response progress watchdog task used by transit connection.
        /// </summary>
        private Task? _responseProgressWatchdogTask;
        /// <summary>
        /// Stores response loop fault used by transit connection.
        /// </summary>
        private ExceptionDispatchInfo? _responseLoopFault;
        /// <summary>
        /// Stores response loop faulted used by transit connection.
        /// </summary>
        private int _responseLoopFaulted;

        /// <summary>
        /// Stores pending by message id used by transit connection.
        /// </summary>
        private readonly ConcurrentDictionary<string, PendingOwnedWork> _pendingByMessageId = new(StringComparer.Ordinal);
        /// <summary>
        /// Stores pending by send order used by transit connection.
        /// </summary>
        private readonly ConcurrentQueue<string> _pendingBySendOrder = new();
        /// <summary>
        /// Stores completed queue used by transit connection.
        /// </summary>
        private readonly Channel<CompletedWork> _completedQueue = Channel.CreateUnbounded<CompletedWork>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = false,
        });
        /// <summary>
        /// Stores direct submit completions used by transit connection.
        /// </summary>
        private readonly ConcurrentDictionary<long, TaskCompletionSource<TransitPublishResult>> _directSubmitCompletions = new();
        /// <summary>
        /// Stores completion enqueued ticks used by transit connection.
        /// </summary>
        private readonly ConcurrentDictionary<long, long> _completionEnqueuedTicks = new();

        /// <summary>
        /// Stores capabilities used by transit connection.
        /// </summary>
        private TransitCapabilitySnapshot _capabilities = new(SupportsStartTls: false, SupportsStreaming: false);
        /// <summary>
        /// Stores tls active used by transit connection.
        /// </summary>
        private bool _tlsActive;
        /// <summary>
        /// Stores streaming mode negotiated used by transit connection.
        /// </summary>
        private bool _streamingModeNegotiated;
        /// <summary>
        /// Stores state used by transit connection.
        /// </summary>
        private TransitConnectionState _state = TransitConnectionState.Disconnected;

        /// <summary>
        /// Stores shutdown requested used by transit connection.
        /// </summary>
        private int _shutdownRequested;
        /// <summary>
        /// Stores tokenless success mode enabled used by transit connection.
        /// </summary>
        private int _tokenlessSuccessModeEnabled;
        /// <summary>
        /// Stores bytes transmitted for transit connection.
        /// </summary>
        private long _bytesTransmitted;
        /// <summary>
        /// Stores bytes received for transit connection.
        /// </summary>
        private long _bytesReceived;
        /// <summary>
        /// Limits socket open count for transit connection.
        /// </summary>
        private long _socketOpenCount;
        /// <summary>
        /// Limits ready transition count for transit connection.
        /// </summary>
        private long _readyTransitionCount;
        /// <summary>
        /// Stores submissions started used by transit connection.
        /// </summary>
        private long _submissionsStarted;
        /// <summary>
        /// Stores submissions accepted used by transit connection.
        /// </summary>
        private long _submissionsAccepted;
        /// <summary>
        /// Stores submissions rejected used by transit connection.
        /// </summary>
        private long _submissionsRejected;
        /// <summary>
        /// Stores submissions failed used by transit connection.
        /// </summary>
        private long _submissionsFailed;
        /// <summary>
        /// Stores submissions ambiguous used by transit connection.
        /// </summary>
        private long _submissionsAmbiguous;
        /// <summary>
        /// Stores submissions unavailable used by transit connection.
        /// </summary>
        private long _submissionsUnavailable;
        /// <summary>
        /// Limits max concurrent submissions for transit connection.
        /// </summary>
        private int _maxConcurrentSubmissions;
        /// <summary>
        /// Stores send sequence used by transit connection.
        /// </summary>
        private long _sendSequence;
        /// <summary>
        /// Limits batch count for transit connection.
        /// </summary>
        private long _batchCount;
        /// <summary>
        /// Limits batch size total for transit connection.
        /// </summary>
        private long _batchSizeTotal;
        /// <summary>
        /// Limits max writer batch size for transit connection.
        /// </summary>
        private int _maxWriterBatchSize;
        /// <summary>
        /// Stores last definitive response progress tick used by transit connection.
        /// </summary>
        private long _lastDefinitiveResponseProgressTick;

        /// <summary>
        /// Handles trace stamp for transit connection.
        /// </summary>
        private static string TraceStamp()
        {
            return $"{DateTimeOffset.UtcNow:O}|tid={Environment.CurrentManagedThreadId}|task={Task.CurrentId?.ToString() ?? "-"}";
        }

        /// <summary>
        /// Handles transit connection for transit connection.
        /// </summary>
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
        /// Handles transit connection for transit connection.
        /// </summary>
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
            _pipelineDepth = perConnectionPipelineDepth;
            _writeBatchCoalesceMicroseconds = writeBatchCoalesceMicroseconds;
            _responseProgressTimeout = effectiveResponseProgressTimeout;
            _responseProgressCheckInterval = effectiveResponseProgressCheckInterval;
            _timingCollector = timingCollector;
            _ = expectedBatchIntentCountProvider;
        }

        /// <summary>
        /// Stores connection id used by transit connection.
        /// </summary>
        internal string ConnectionId { get; } = Guid.NewGuid().ToString("N");

        /// <summary>
        /// Stores current state used by transit connection.
        /// </summary>
        internal TransitConnectionState CurrentState => _state;

        /// <summary>
        /// Stores is tls active used by transit connection.
        /// </summary>
        internal bool IsTlsActive => _tlsActive;

        /// <summary>
        /// Stores capabilities used by transit connection.
        /// </summary>
        internal TransitCapabilitySnapshot Capabilities => _capabilities;

        /// <summary>
        /// Limits outstanding submission count for transit connection.
        /// </summary>
        internal int OutstandingSubmissionCount => _pendingByMessageId.Count;

        /// <summary>
        /// Stores pipeline depth used by transit connection.
        /// </summary>
        internal int PipelineDepth => _pipelineDepth;

        /// <summary>
        /// Stores is response loop faulted used by transit connection.
        /// </summary>
        internal bool IsResponseLoopFaulted => Volatile.Read(ref _responseLoopFaulted) == 1;

        /// <summary>
        /// Handles throw if response loop faulted for transit connection.
        /// </summary>
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
        /// Handles is recorded response loop fault for transit connection.
        /// </summary>
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
        /// Handles notify materialization reservation changed for transit connection.
        /// </summary>
        internal static void NotifyMaterializationReservationChanged()
        {
            // Intentionally no-op in global queue architecture.
        }

        /// <summary>
        /// Handles record reconnect event for transit connection.
        /// </summary>
        internal static void RecordReconnectEvent()
        {
            // Intentionally no-op in global queue architecture.
        }

        /// <summary>
        /// Handles initialize async for transit connection.
        /// </summary>
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
                _tlsActive = false;
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
                _capabilities = await AwaitInitializationStageAsync(
                    ReadCapabilitiesAsync,
                    "CAPABILITIES exchange",
                    cancellationToken).ConfigureAwait(false);
                Console.WriteLine($"[TRACE-RI-65] {TraceStamp()} Connection.Initialize CAPABILITIES-COMPLETE connectionId={ConnectionId} supportsStreaming={_capabilities.SupportsStreaming}");
                LogTransitCapabilities(_logger, ConnectionId, _capabilities.SupportsStartTls, _capabilities.SupportsStreaming);

                if (!_useSsl && _capabilities.SupportsStartTls)
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
                    _capabilities = await AwaitInitializationStageAsync(
                        ReadCapabilitiesAsync,
                        "CAPABILITIES exchange (post-STARTTLS)",
                        cancellationToken).ConfigureAwait(false);
                    Console.WriteLine($"[TRACE-RI-65A] {TraceStamp()} Connection.Initialize CAPABILITIES-RENEGOTIATE-COMPLETE connectionId={ConnectionId} supportsStreaming={_capabilities.SupportsStreaming}");
                    LogTransitCapabilities(_logger, ConnectionId, _capabilities.SupportsStartTls, _capabilities.SupportsStreaming);
                }

                if (!_capabilities.SupportsStreaming)
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
                Console.WriteLine($"[TRACE-RI-68] {TraceStamp()} Connection.Initialize SUCCESS connectionId={ConnectionId} state={_state}");
                _ = Interlocked.Increment(ref _readyTransitionCount);
                LogTransitConnectionReady(_logger, ConnectionId, _tlsActive);

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
        /// Handles cleanup initialization failure async for transit connection.
        /// </summary>
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

            _tlsActive = false;
            _streamingModeNegotiated = false;
            _capabilities = new TransitCapabilitySnapshot(SupportsStartTls: false, SupportsStreaming: false);

            TransitionState(TransitConnectionState.Disconnected);
        }

        /// <summary>
        /// Handles process batch async for transit connection.
        /// </summary>
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
        /// Handles try take completed for transit connection.
        /// </summary>
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
        /// Handles wait for completed async for transit connection.
        /// </summary>
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
        /// Handles submit takethis async for transit connection.
        /// </summary>
        internal async ValueTask<TransitPublishResult> SubmitTakethisAsync(
            string messageId,
            ReadOnlyMemory<byte> articlePayload,
            CancellationToken cancellationToken,
            long publishAsyncEnterTick,
            long dispatcherAssignedTick,
            Action? onWriteIntentMaterialized = null)
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

            if (Volatile.Read(ref _shutdownRequested) == 1 || (_state != TransitConnectionState.Ready && _state != TransitConnectionState.Publishing) || !_streamingModeNegotiated)
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
                    ProvenanceConnectionState: _state,
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
                        ProvenanceConnectionState: _state,
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
                    ProvenanceConnectionState: _state,
                    ProvenanceTick: Stopwatch.GetTimestamp());
            }
            finally
            {
                _ = _directSubmitCompletions.TryRemove(item.WorkItemId, out _);
            }
        }

        /// <summary>
        /// Handles drain outstanding owned work for retry for transit connection.
        /// </summary>
        internal IReadOnlyList<TransitWorkItem> DrainOutstandingOwnedWorkForRetry()
        {
            IReadOnlyList<TransitWorkItem> drained = [.. DrainOwnedPendingWork(static _ => true).Select(static pending => pending.WorkItem)];
            // Console.WriteLine($"[TRACE-RI-79] {TraceStamp()} DrainOutstandingOwnedWorkForRetry connectionId={ConnectionId} count={drained.Count} items=[{string.Join(",", drained.Select(static x => $"{x.WorkItemId}:{x.State}:{x.AttemptCount}"))}]");
            return drained;
        }

        /// <summary>
        /// Handles drain outstanding direct submit pending work for transit connection.
        /// </summary>
        private IReadOnlyList<PendingOwnedWork> DrainOutstandingDirectSubmitPendingWork()
        {
            return DrainOwnedPendingWork(pending => _directSubmitCompletions.ContainsKey(pending.WorkItem.WorkItemId));
        }

        /// <summary>
        /// Handles drain owned pending work for transit connection.
        /// </summary>
        private IReadOnlyList<PendingOwnedWork> DrainOwnedPendingWork(Func<PendingOwnedWork, bool> shouldDrain)
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
        /// Handles capture diagnostics snapshot for transit connection.
        /// </summary>
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
                CurrentState: _state,
                IsTlsActive: _tlsActive,
                SocketOpenCount: Interlocked.Read(ref _socketOpenCount),
                ReadyTransitionCount: Interlocked.Read(ref _readyTransitionCount),
                SubmissionsStarted: Interlocked.Read(ref _submissionsStarted),
                SubmissionsAccepted: Interlocked.Read(ref _submissionsAccepted),
                SubmissionsRejected: Interlocked.Read(ref _submissionsRejected),
                SubmissionsAmbiguous: Interlocked.Read(ref _submissionsAmbiguous),
                SubmissionsUnavailable: Interlocked.Read(ref _submissionsUnavailable),
                SubmissionsFailed: Interlocked.Read(ref _submissionsFailed),
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
        /// Handles capture first p1 greeting provenance snapshot for transit connection.
        /// </summary>
        internal static P1GreetingProvenanceSnapshot? CaptureFirstP1GreetingProvenanceSnapshot()
        {
            return null;
        }

        /// <summary>
        /// Handles response loop async for transit connection.
        /// </summary>
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
        /// Handles response progress watchdog loop async for transit connection.
        /// </summary>
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
        /// Handles try signal response loop fault for transit connection.
        /// </summary>
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
        /// Handles map takethis response for transit connection.
        /// </summary>
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
                ProvenanceConnectionState: _state,
                ProvenanceTick: responseAvailableTick);
        }

        /// <summary>
        /// Handles resolve response message id for transit connection.
        /// </summary>
        private string ResolveResponseMessageId(int code, string responseText, string responseLine)
        {
            if (TryResolveLeadingMessageIdToken(responseText, out string? messageId))
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
        /// Handles try resolve leading message id token for transit connection.
        /// </summary>
        private static bool TryResolveLeadingMessageIdToken(string responseText, out string? messageId)
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
        /// Handles acknowledge send order for transit connection.
        /// </summary>
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
        /// Handles stage takethis frame for transit connection.
        /// </summary>
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
        /// Handles write takethis command for transit connection.
        /// </summary>
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
        /// Handles write dot stuffed article for transit connection.
        /// </summary>
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
        /// Handles write bytes for transit connection.
        /// </summary>
        private static void WriteBytes(PipeWriter writer, ReadOnlySpan<byte> bytes)
        {
            Span<byte> destination = writer.GetSpan(bytes.Length);
            bytes.CopyTo(destination);
            writer.Advance(bytes.Length);
        }

        /// <summary>
        /// Handles read line async for transit connection.
        /// </summary>
        private async ValueTask<string> ReadLineAsync(CancellationToken cancellationToken)
        {
            PipeReader reader = _reader ?? throw new InvalidOperationException("Transit protocol reader is not initialized.");
            (string line, int bytesRead) = await TransitProtocolParser.ReadNntpLineWithByteCountAsync(reader, cancellationToken).ConfigureAwait(false);
            _ = Interlocked.Add(ref _bytesReceived, bytesRead);
            return line;
        }

        /// <summary>
        /// Handles await initialization stage async for transit connection.
        /// </summary>
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
        /// Handles read capabilities lines async for transit connection.
        /// </summary>
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
        /// Handles read capabilities async for transit connection.
        /// </summary>
        private async Task<TransitCapabilitySnapshot> ReadCapabilitiesAsync(CancellationToken cancellationToken)
        {
            await WriteCommandAsync("CAPABILITIES", cancellationToken).ConfigureAwait(false);
            IReadOnlyList<string> lines = await ReadCapabilitiesLinesAsync(cancellationToken).ConfigureAwait(false);
            return TransitProtocolParser.ParseCapabilitiesResponse(lines);
        }

        /// <summary>
        /// Handles write command async for transit connection.
        /// </summary>
        private async Task WriteCommandAsync(string command, CancellationToken cancellationToken)
        {
            Stream writeStream = _writeStream ?? throw new InvalidOperationException("Transit transport write stream is not initialized.");
            byte[] commandBytes = Encoding.ASCII.GetBytes(command + "\r\n");
            await writeStream.WriteAsync(commandBytes, cancellationToken).ConfigureAwait(false);
            await writeStream.FlushAsync(cancellationToken).ConfigureAwait(false);
            _ = Interlocked.Add(ref _bytesTransmitted, commandBytes.Length);
        }

        /// <summary>
        /// Handles start tls async for transit connection.
        /// </summary>
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
        /// Handles upgrade to tls async for transit connection.
        /// </summary>
        private async Task UpgradeToTlsAsync(CancellationToken cancellationToken)
        {
            if (_transportStream is null)
            {
                throw new InvalidOperationException("Transit transport stream is not initialized.");
            }

            RemoteCertificateValidationCallback certificateValidationCallback = _serverCertificateValidationCallback
                ?? ((object _, X509Certificate? _, X509Chain? _, SslPolicyErrors _) => true);

            SslClientAuthenticationOptions options = new()
            {
                TargetHost = _host,
                EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
                CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
                RemoteCertificateValidationCallback = certificateValidationCallback,
            };

            SslStream sslStream = new(_transportStream, leaveInnerStreamOpen: true, certificateValidationCallback);
            await sslStream.AuthenticateAsClientAsync(options, cancellationToken).ConfigureAwait(false);

            _readStream = sslStream;
            _writeStream = sslStream;
            _transportStream = sslStream;
            _tlsActive = true;
        }

        /// <summary>
        /// Handles observe max concurrent submissions for transit connection.
        /// </summary>
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
        /// Handles update max batch size for transit connection.
        /// </summary>
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
        /// Handles record submission result for transit connection.
        /// </summary>
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
            }
        }

        /// <summary>
        /// Handles transition state for transit connection.
        /// </summary>
        private void TransitionState(TransitConnectionState state)
        {
            _state = state;
            LogTransitStateTransition(_logger, ConnectionId, state);
        }

        /// <summary>
        /// Handles dispose async for transit connection.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _shutdownRequested, 1) != 0)
            {
                return;
            }

            TransitConnectionState stateBeforeShutdown = _state;

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
        /// Handles settle unresolved owned work during dispose for transit connection.
        /// </summary>
        private void SettleUnresolvedOwnedWorkDuringDispose()
        {
            IReadOnlyList<PendingOwnedWork> unresolved = DrainOwnedPendingWork(static _ => true);
            SettlePendingAsAmbiguous(unresolved, TransitPublishProvenance.Shutdown, "Transit connection shutdown before definitive TAKETHIS response.", enqueueCompletion: true);
        }

        /// <summary>
        /// Handles settle unresolved direct submit work for fault for transit connection.
        /// </summary>
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
        /// Handles settle pending as ambiguous for transit connection.
        /// </summary>
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
                    ProvenanceConnectionState: _state,
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
        /// Handles try complete direct submit for transit connection.
        /// </summary>
        private void TryCompleteDirectSubmit(long workItemId, TransitPublishResult result)
        {
            if (_directSubmitCompletions.TryRemove(workItemId, out TaskCompletionSource<TransitPublishResult>? directCompletion))
            {
                _ = directCompletion.TrySetResult(result);
            }
        }

        /// <summary>
        /// Defines transit connection diagnostics snapshot and its transit connection contract.
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
            int MaxConcurrentSubmissions,
            int CurrentConcurrentSubmissions,
            long CurrentWriteIntentQueueDepth,
            string? LocalEndpoint,
            string? RemoteEndpoint,
            PipeliningDiagnosticSummary DiagnosticsSummary,
            DiagnosticOperationRecord[] DiagnosticSampleRecords,
            OutstandingPublishOperationSnapshot[] OutstandingOperations);

        /// <summary>
        /// Defines p1 greeting provenance snapshot and its transit connection contract.
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
        /// Defines p1 greeting provenance lifecycle event and its transit connection contract.
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
        /// Defines struct and its transit connection contract.
        /// </summary>
        internal readonly record struct P1GreetingLifecycleEventRecord(
            P1GreetingProvenanceLifecycleEvent Event,
            long Tick,
            int AttemptId);

        /// <summary>
        /// Defines struct and its transit connection contract.
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
        /// Defines struct and its transit connection contract.
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
        /// Defines struct and its transit connection contract.
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
        /// Defines pending owned work and its transit connection contract.
        /// </summary>
        private sealed class PendingOwnedWork
        {
            /// <summary>
            /// Handles pending owned work for transit connection.
            /// </summary>
            internal PendingOwnedWork(TransitWorkItem workItem)
            {
                WorkItem = workItem;
            }

            /// <summary>
            /// Stores work item used by transit connection.
            /// </summary>
            internal TransitWorkItem WorkItem { get; }

            /// <summary>
            /// Stores t2 socket write begin tick used by transit connection.
            /// </summary>
            internal long T2SocketWriteBeginTick;

            /// <summary>
            /// Stores t3 socket write end tick used by transit connection.
            /// </summary>
            internal long T3SocketWriteEndTick;

            /// <summary>
            /// Stores t6 response correlated tick used by transit connection.
            /// </summary>
            internal long T6ResponseCorrelatedTick;

            /// <summary>
            /// Stores send sequence used by transit connection.
            /// </summary>
            internal long SendSequence;
        }

        /// <summary>
        /// Defines completed work and its transit connection contract.
        /// </summary>
        private sealed record CompletedWork(TransitWorkItem WorkItem, TransitPublishResult Result);

        /// <summary>
        /// Defines struct and its transit connection contract.
        /// </summary>
        private readonly record struct DotStuffWriteMetrics(
            int BytesWritten,
            long GetSpanCalls,
            long AdvanceCalls,
            long StuffedDotEvents);

        /// <summary>
        /// Defines transit connection lifecycle failure and its transit connection contract.
        /// </summary>
        internal enum TransitConnectionLifecycleFailure
        {
            WriterNotInitialized,
            WriterCompletedDuringTakethisSubmission,
            InitializationProgressTimeout,
            WriterDisposedDuringTakethisSubmission,
        }

        /// <summary>
        /// Defines transit connection lifecycle exception and its transit connection contract.
        /// </summary>
        internal sealed class TransitConnectionLifecycleException : InvalidOperationException
        {
            /// <summary>
            /// Handles transit connection lifecycle exception for transit connection.
            /// </summary>
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
            /// Stores failure used by transit connection.
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
