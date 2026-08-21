using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net;
using System.Threading.Channels;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Runtime.ExceptionServices;

namespace VectorNNTP.Backfiller.Runtime.Transit
{
    /// <summary>
    /// Manages one outbound NNTP transit connection lifecycle and per-connection response correlation.
    /// </summary>
    internal sealed partial class TransitConnection : IAsyncDisposable
    {
        private static readonly byte[] CrLfBytes = "\r\n"u8.ToArray();
        private static readonly byte[] DotTerminatorBytes = ".\r\n"u8.ToArray();
        private static readonly byte[] TakethisPrefixBytes = "TAKETHIS "u8.ToArray();
        private static readonly TimeSpan DefaultResponseProgressTimeout = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan DefaultResponseProgressCheckInterval = TimeSpan.FromMilliseconds(250);

        private readonly string _host;
        private readonly int _port;
        private readonly bool _useSsl;
        private readonly ILogger _logger;
        private readonly RemoteCertificateValidationCallback? _serverCertificateValidationCallback;
        private readonly int _pipelineDepth;
        private readonly int _writeBatchCoalesceMicroseconds;
        private readonly TimeSpan _responseProgressTimeout;
        private readonly TimeSpan _responseProgressCheckInterval;
        private readonly TransitTimingCollector? _timingCollector;

        private readonly SemaphoreSlim _writeGate = new(1, 1);
        private readonly SemaphoreSlim _tokenlessCorrelationGate = new(1, 1);

        private TcpClient? _tcpClient;
        private Stream? _transportStream;
        private Stream? _readStream;
        private Stream? _writeStream;
        private PipeReader? _reader;
        private PipeWriter? _writer;

        private CancellationTokenSource? _responseLoopCancellation;
        private Task? _responseLoopTask;
        private CancellationTokenSource? _responseProgressWatchdogCancellation;
        private Task? _responseProgressWatchdogTask;
        private ExceptionDispatchInfo? _responseLoopFault;
        private int _responseLoopFaulted;

        private readonly ConcurrentDictionary<string, PendingOwnedWork> _pendingByMessageId = new(StringComparer.Ordinal);
        private readonly ConcurrentQueue<string> _pendingBySendOrder = new();
        private readonly Channel<CompletedWork> _completedQueue = Channel.CreateUnbounded<CompletedWork>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = false,
        });
        private readonly ConcurrentDictionary<long, TaskCompletionSource<TransitPublishResult>> _directSubmitCompletions = new();
        private readonly ConcurrentDictionary<long, long> _completionEnqueuedTicks = new();

        private TransitCapabilitySnapshot _capabilities = new(SupportsStartTls: false, SupportsStreaming: false);
        private bool _tlsActive;
        private bool _streamingModeNegotiated;
        private TransitConnectionState _state = TransitConnectionState.Disconnected;

        private int _shutdownRequested;
        private int _tokenlessSuccessModeEnabled;
        private long _bytesTransmitted;
        private long _bytesReceived;
        private long _socketOpenCount;
        private long _readyTransitionCount;
        private long _submissionsStarted;
        private long _submissionsAccepted;
        private long _submissionsRejected;
        private long _submissionsFailed;
        private long _submissionsAmbiguous;
        private long _submissionsUnavailable;
        private int _maxConcurrentSubmissions;
        private long _sendSequence;
        private long _batchCount;
        private long _batchSizeTotal;
        private int _maxWriterBatchSize;
        private long _lastDefinitiveResponseProgressTick;

        private static string TraceStamp()
        {
            return $"{DateTimeOffset.UtcNow:O}|tid={Environment.CurrentManagedThreadId}|task={Task.CurrentId?.ToString() ?? "-"}";
        }

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

        internal string ConnectionId { get; } = Guid.NewGuid().ToString("N");

        internal TransitConnectionState CurrentState => _state;

        internal bool IsTlsActive => _tlsActive;

        internal TransitCapabilitySnapshot Capabilities => _capabilities;

        internal int OutstandingSubmissionCount => _pendingByMessageId.Count;

        internal int PipelineDepth => _pipelineDepth;

        internal bool IsResponseLoopFaulted => Volatile.Read(ref _responseLoopFaulted) == 1;

        internal void ThrowIfResponseLoopFaulted()
        {
            if (!IsResponseLoopFaulted)
            {
                return;
            }

            Exception? inner = _responseLoopFault?.SourceException;
            throw new IOException("Transit response loop faulted before pending responses completed.", inner);
        }

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

        internal void NotifyMaterializationReservationChanged()
        {
            // Intentionally no-op in global queue architecture.
        }

        internal void RecordReconnectEvent()
        {
            // Intentionally no-op in global queue architecture.
        }

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
                Interlocked.Increment(ref _socketOpenCount);

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
                    token => ReadCapabilitiesAsync(token),
                    "CAPABILITIES exchange",
                    cancellationToken).ConfigureAwait(false);
                Console.WriteLine($"[TRACE-RI-65] {TraceStamp()} Connection.Initialize CAPABILITIES-COMPLETE connectionId={ConnectionId} supportsStreaming={_capabilities.SupportsStreaming}");
                LogTransitCapabilities(_logger, ConnectionId, _capabilities.SupportsStartTls, _capabilities.SupportsStreaming);

                if (!_useSsl && _capabilities.SupportsStartTls)
                {
                    TransitionState(TransitConnectionState.StartingTls);
                    await AwaitInitializationStageAsync(
                        token => StartTlsAsync(token),
                        "STARTTLS negotiation",
                        cancellationToken).ConfigureAwait(false);
                    TransitionState(TransitConnectionState.TlsEstablished);

                    _reader = PipeReader.Create(_readStream ?? throw new InvalidOperationException("Transit transport read stream is not initialized."), new StreamPipeReaderOptions(leaveOpen: true));
                    _writer = PipeWriter.Create(_writeStream ?? throw new InvalidOperationException("Transit transport write stream is not initialized."), new StreamPipeWriterOptions(leaveOpen: true));

                    TransitionState(TransitConnectionState.CapabilitiesNegotiation);
                    Console.WriteLine($"[TRACE-RI-64A] {TraceStamp()} Connection.Initialize CAPABILITIES-RENEGOTIATE-START connectionId={ConnectionId}");
                    _capabilities = await AwaitInitializationStageAsync(
                        token => ReadCapabilitiesAsync(token),
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
                Interlocked.Increment(ref _readyTransitionCount);
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
                    Interlocked.Increment(ref _submissionsStarted);
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

                    Interlocked.Add(ref _bytesTransmitted, batchBytesStaged);
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
                    _writeGate.Release();
                }

                Interlocked.Increment(ref _batchCount);
                Interlocked.Add(ref _batchSizeTotal, items.Count);
                UpdateMaxBatchSize(items.Count);
            }
            finally
            {
                if (tokenlessModeEnabled)
                {
                    _tokenlessCorrelationGate.Release();
                }
            }
        }

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
                Interlocked.Increment(ref _submissionsUnavailable);
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
                    Interlocked.Increment(ref _submissionsFailed);
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
                _directSubmitCompletions.TryRemove(item.WorkItemId, out _);
            }
        }

        internal IReadOnlyList<TransitWorkItem> DrainOutstandingOwnedWorkForRetry()
        {
            IReadOnlyList<TransitWorkItem> drained = DrainOwnedPendingWork(static _ => true)
                .Select(static pending => pending.WorkItem)
                .ToArray();
            Console.WriteLine($"[TRACE-RI-79] {TraceStamp()} DrainOutstandingOwnedWorkForRetry connectionId={ConnectionId} count={drained.Count} items=[{string.Join(",", drained.Select(static x => $"{x.WorkItemId}:{x.State}:{x.AttemptCount}"))}]");
            return drained;
        }

        private IReadOnlyList<PendingOwnedWork> DrainOutstandingDirectSubmitPendingWork()
        {
            return DrainOwnedPendingWork(pending => _directSubmitCompletions.ContainsKey(pending.WorkItem.WorkItemId));
        }

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

        internal TransitConnectionDiagnosticsSnapshot CaptureDiagnosticsSnapshot()
        {
            OutstandingPublishOperationSnapshot[] outstanding = _pendingByMessageId.Values
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
                    LikelyAwaitingPath: "ResponseLoop"))
                .ToArray();

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

        internal P1GreetingProvenanceSnapshot? CaptureFirstP1GreetingProvenanceSnapshot()
        {
            return null;
        }

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
            _completedQueue.Writer.TryComplete(ex);

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

        private string ResolveResponseMessageId(int code, string responseText, string responseLine)
        {
            if (TryResolveLeadingMessageIdToken(responseText, out string? messageId))
            {
                return messageId;
            }

            if (code == 239 || code == 431 || code == 439)
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
                        Interlocked.Exchange(ref _tokenlessSuccessModeEnabled, 1);
                        return pendingId;
                    }
                }

                throw new InvalidOperationException($"TAKETHIS response omitted Message-ID token and no outstanding submission was available for FIFO correlation: '{responseLine}'.");
            }

            throw new InvalidOperationException($"Unable to resolve response Message-ID from line: {responseLine}");
        }

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

        private void AcknowledgeSendOrder(string messageId)
        {
            while (_pendingBySendOrder.TryPeek(out string? queued))
            {
                if (string.Equals(queued, messageId, StringComparison.Ordinal))
                {
                    _pendingBySendOrder.TryDequeue(out _);
                    return;
                }

                _pendingBySendOrder.TryDequeue(out _);
            }
        }

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

        private static void WriteBytes(PipeWriter writer, ReadOnlySpan<byte> bytes)
        {
            Span<byte> destination = writer.GetSpan(bytes.Length);
            bytes.CopyTo(destination);
            writer.Advance(bytes.Length);
        }

        private async ValueTask<string> ReadLineAsync(CancellationToken cancellationToken)
        {
            PipeReader reader = _reader ?? throw new InvalidOperationException("Transit protocol reader is not initialized.");
            (string line, int bytesRead) = await TransitProtocolParser.ReadNntpLineWithByteCountAsync(reader, cancellationToken).ConfigureAwait(false);
            Interlocked.Add(ref _bytesReceived, bytesRead);
            return line;
        }

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

        private async Task<TransitCapabilitySnapshot> ReadCapabilitiesAsync(CancellationToken cancellationToken)
        {
            await WriteCommandAsync("CAPABILITIES", cancellationToken).ConfigureAwait(false);
            IReadOnlyList<string> lines = await ReadCapabilitiesLinesAsync(cancellationToken).ConfigureAwait(false);
            return TransitProtocolParser.ParseCapabilitiesResponse(lines);
        }

        private async Task WriteCommandAsync(string command, CancellationToken cancellationToken)
        {
            Stream writeStream = _writeStream ?? throw new InvalidOperationException("Transit transport write stream is not initialized.");
            byte[] commandBytes = Encoding.ASCII.GetBytes(command + "\r\n");
            await writeStream.WriteAsync(commandBytes, cancellationToken).ConfigureAwait(false);
            await writeStream.FlushAsync(cancellationToken).ConfigureAwait(false);
            Interlocked.Add(ref _bytesTransmitted, commandBytes.Length);
        }

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

        private void RecordSubmissionResult(TransitPublishStatus status)
        {
            switch (status)
            {
                case TransitPublishStatus.Accepted:
                    Interlocked.Increment(ref _submissionsAccepted);
                    break;
                case TransitPublishStatus.Rejected:
                    Interlocked.Increment(ref _submissionsRejected);
                    break;
                case TransitPublishStatus.Failed:
                    Interlocked.Increment(ref _submissionsFailed);
                    break;
                case TransitPublishStatus.Ambiguous:
                    Interlocked.Increment(ref _submissionsAmbiguous);
                    break;
                case TransitPublishStatus.Unavailable:
                    Interlocked.Increment(ref _submissionsUnavailable);
                    break;
            }
        }

        private void TransitionState(TransitConnectionState state)
        {
            _state = state;
            LogTransitStateTransition(_logger, ConnectionId, state);
        }

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
                if (responseLoopCancellation is not null)
                {
                    responseLoopCancellation.Cancel();
                }

                CancellationTokenSource? responseProgressWatchdogCancellation = _responseProgressWatchdogCancellation;
                if (responseProgressWatchdogCancellation is not null)
                {
                    responseProgressWatchdogCancellation.Cancel();
                }

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

                _completedQueue.Writer.TryComplete();
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

        private void SettleUnresolvedOwnedWorkDuringDispose()
        {
            IReadOnlyList<PendingOwnedWork> unresolved = DrainOwnedPendingWork(static _ => true);
            SettlePendingAsAmbiguous(unresolved, TransitPublishProvenance.Shutdown, "Transit connection shutdown before definitive TAKETHIS response.", enqueueCompletion: true);
        }

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

        private void TryCompleteDirectSubmit(long workItemId, TransitPublishResult result)
        {
            if (_directSubmitCompletions.TryRemove(workItemId, out TaskCompletionSource<TransitPublishResult>? directCompletion))
            {
                _ = directCompletion.TrySetResult(result);
            }
        }

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

        internal readonly record struct P1GreetingLifecycleEventRecord(
            P1GreetingProvenanceLifecycleEvent Event,
            long Tick,
            int AttemptId);

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

        private sealed class PendingOwnedWork
        {
            internal PendingOwnedWork(TransitWorkItem workItem)
            {
                WorkItem = workItem;
            }

            internal TransitWorkItem WorkItem { get; }

            internal long T2SocketWriteBeginTick;

            internal long T3SocketWriteEndTick;

            internal long T6ResponseCorrelatedTick;

            internal long SendSequence;
        }

        private sealed record CompletedWork(TransitWorkItem WorkItem, TransitPublishResult Result);

        private readonly record struct DotStuffWriteMetrics(
            int BytesWritten,
            long GetSpanCalls,
            long AdvanceCalls,
            long StuffedDotEvents);

        internal enum TransitConnectionLifecycleFailure
        {
            WriterNotInitialized,
            WriterCompletedDuringTakethisSubmission,
            InitializationProgressTimeout,
            WriterDisposedDuringTakethisSubmission,
        }

        internal sealed class TransitConnectionLifecycleException : InvalidOperationException
        {
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

            internal TransitConnectionLifecycleFailure Failure { get; }
        }

        [LoggerMessage(EventId = 2210, Level = LogLevel.Debug, Message = "Transit connection {ConnectionId} state changed to {State}")]
        private static partial void LogTransitStateTransition(ILogger logger, string connectionId, TransitConnectionState state);

        [LoggerMessage(EventId = 2211, Level = LogLevel.Information, Message = "Transit connection {ConnectionId} capabilities: STARTTLS={SupportsStartTls}, STREAMING={SupportsStreaming}")]
        private static partial void LogTransitCapabilities(ILogger logger, string connectionId, bool supportsStartTls, bool supportsStreaming);

        [LoggerMessage(EventId = 2212, Level = LogLevel.Information, Message = "Transit connection {ConnectionId} is ready (TLS={TlsActive})")]
        private static partial void LogTransitConnectionReady(ILogger logger, string connectionId, bool tlsActive);

        [LoggerMessage(EventId = 2213, Level = LogLevel.Warning, Message = "Transit connection {ConnectionId} response loop faulted")]
        private static partial void LogTransitResponseLoopFaulted(ILogger logger, Exception exception, string connectionId);
    }
}
