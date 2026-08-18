using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Text;
using System.Threading.Channels;

namespace VectorNNTP.Backfiller.Runtime.Transit
{
    /// <summary>
    /// Manages one outbound NNTP transit connection and protocol negotiation lifecycle.
    /// </summary>
    internal sealed partial class TransitConnection : IAsyncDisposable
    {
        private readonly string _host;
        private readonly int _port;
        private readonly bool _useSsl;
        private readonly ILogger _logger;
        private readonly RemoteCertificateValidationCallback? _serverCertificateValidationCallback;

        private const int DiagnosticMaxOperationRecords = 200_000;

        private static readonly byte[] CrLfBytes = "\r\n"u8.ToArray();
        private static readonly byte[] DotTerminatorBytes = ".\r\n"u8.ToArray();

        private readonly SemaphoreSlim _writeGate = new(1, 1);
        private readonly SemaphoreSlim _tokenlessCorrelationGate = new(1, 1);
        private readonly ConcurrentDictionary<string, PendingPublishOperation> _pendingByMessageId = new(StringComparer.Ordinal);
        private readonly ConcurrentQueue<string> _pendingBySendOrder = new();
        private TaskCompletionSource? _shutdownDrainCompletion;
        private int _shutdownRequested;
        private readonly int _maxWriteBatchSize;
        private readonly int _writeIntentQueueCapacity;
        private readonly int _writeBatchCoalesceMicroseconds;
        private readonly object _diagnosticGate = new();
        private readonly Dictionary<string, int> _diagnosticIndexByMessageId = new(StringComparer.Ordinal);
        private readonly List<DiagnosticOperationRecord> _diagnosticRecords = [];
        private readonly List<int> _diagnosticBatchSizes = [];
        private readonly long[] _diagnosticBatchSizeHistogram;
        private long _diagnosticBatchIdSequence;
        private long _diagnosticSendSequence;
        private long _diagnosticMaxPendingDepth;
        private long _diagnosticMaxWriteQueueDepth;
        private long _diagnosticWriteQueueDepth;
        private long _diagnosticLogicalOutstandingDepthMax;
        private readonly List<double> _diagnosticCoalescingWaitMicroseconds = [];
        private Channel<WriteIntent>? _writeIntentChannel;
        private CancellationTokenSource? _writeLoopCancellation;
        private Task? _writeLoopTask;
        private int _tokenlessSuccessModeEnabled;

        private TcpClient? _tcpClient;
        private Stream? _transportStream;
        private Stream? _readStream;
        private Stream? _writeStream;
        private PipeReader? _reader;
        private PipeWriter? _writer;

        private CancellationTokenSource? _responseLoopCancellation;
        private Task? _responseLoopTask;

        private TransitCapabilitySnapshot _capabilities = new(
            SupportsStartTls: false,
            SupportsCompressDeflate: false,
            SupportsStreaming: false);

        private bool _tlsActive;
        private bool _compressionActive;
        private bool _skipCompressionForCurrentInitialization;
        private bool _streamingModeNegotiated;
        private TransitConnectionState _state = TransitConnectionState.Disconnected;
        private long _bytesTransmitted;
        private long _bytesReceived;
        private long _socketOpenCount;
        private long _readyTransitionCount;
        private long _submissionsStarted;
        private long _submissionsAccepted;
        private long _submissionsRejected;
        private long _submissionsAmbiguous;
        private long _submissionsUnavailable;
        private long _submissionsFailed;
        private int _maxConcurrentSubmissions;

        internal TransitConnection(string host, int port, bool useSsl, ILogger logger, int perConnectionPipelineDepth = 8, int writeBatchCoalesceMicroseconds = 250)
            : this(host, port, useSsl, logger, serverCertificateValidationCallback: null, perConnectionPipelineDepth, writeBatchCoalesceMicroseconds)
        {
        }

        internal TransitConnection(
            string host,
            int port,
            bool useSsl,
            ILogger logger,
            RemoteCertificateValidationCallback? serverCertificateValidationCallback,
            int perConnectionPipelineDepth = 8,
            int writeBatchCoalesceMicroseconds = 250)
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

            _host = host.Trim();
            _port = port;
            _useSsl = useSsl;
            _logger = logger;
            _serverCertificateValidationCallback = serverCertificateValidationCallback;
            _maxWriteBatchSize = perConnectionPipelineDepth;
            _writeIntentQueueCapacity = perConnectionPipelineDepth;
            _writeBatchCoalesceMicroseconds = writeBatchCoalesceMicroseconds;
            _diagnosticBatchSizeHistogram = new long[Math.Max(1, perConnectionPipelineDepth) + 1];
        }

        internal string ConnectionId { get; } = Guid.NewGuid().ToString("N");

        internal TransitConnectionState CurrentState => _state;

        internal TransitCapabilitySnapshot Capabilities => _capabilities;

        internal bool IsTlsActive => _tlsActive;

        internal bool IsCompressionActive => _compressionActive;

        internal int OutstandingSubmissionCount => _pendingByMessageId.Count;

        internal long CurrentWriteIntentQueueDepth => Volatile.Read(ref _diagnosticWriteQueueDepth);

        internal long BytesTransmitted => Interlocked.Read(ref _bytesTransmitted);

        internal long BytesReceived => Interlocked.Read(ref _bytesReceived);

        internal TransitConnectionDiagnosticsSnapshot CaptureDiagnosticsSnapshot()
        {
            int currentInFlight = _pendingByMessageId.Count;
            OutstandingPublishOperationSnapshot[] outstandingOperations = CaptureOutstandingOperationSnapshots();
            PipeliningDiagnosticSummary summary;
            DiagnosticOperationRecord[] sample;

            lock (_diagnosticGate)
            {
                int[] batchSizes = _diagnosticBatchSizes.ToArray();
                Array.Sort(batchSizes);

                double[] coalescingWaitMicroseconds = _diagnosticCoalescingWaitMicroseconds.ToArray();
                Array.Sort(coalescingWaitMicroseconds);

                long numberOfBatches = batchSizes.LongLength;
                int maxBatchSize = batchSizes.Length == 0 ? 0 : batchSizes[^1];
                double averageBatchSize = batchSizes.Length == 0 ? 0 : batchSizes.Average();
                double p50 = PercentileFromSorted(batchSizes, 0.50);
                double p95 = PercentileFromSorted(batchSizes, 0.95);
                double p99 = PercentileFromSorted(batchSizes, 0.99);
                double averageCoalescingWaitMicroseconds = coalescingWaitMicroseconds.Length == 0 ? 0 : coalescingWaitMicroseconds.Average();
                double p50CoalescingWaitMicroseconds = PercentileFromSorted(coalescingWaitMicroseconds, 0.50);
                double p95CoalescingWaitMicroseconds = PercentileFromSorted(coalescingWaitMicroseconds, 0.95);
                double p99CoalescingWaitMicroseconds = PercentileFromSorted(coalescingWaitMicroseconds, 0.99);

                StringBuilder histogramBuilder = new();
                int histogramLimit = Math.Min(_diagnosticBatchSizeHistogram.Length - 1, _maxWriteBatchSize);
                long[] batchSizeCounts = new long[histogramLimit + 1];
                for (int size = 1; size <= histogramLimit; size++)
                {
                    long count = _diagnosticBatchSizeHistogram[size];
                    batchSizeCounts[size] = count;

                    if (histogramBuilder.Length > 0)
                    {
                        histogramBuilder.Append("; ");
                    }

                    histogramBuilder.Append("size");
                    histogramBuilder.Append(size);
                    histogramBuilder.Append('=');
                    histogramBuilder.Append(count);
                }

                sample = _diagnosticRecords.ToArray();

                summary = new PipeliningDiagnosticSummary(
                    MaxPendingDepth: _diagnosticMaxPendingDepth,
                    MaxWriteQueueDepth: _diagnosticMaxWriteQueueDepth,
                    MaxWriterBatchSize: maxBatchSize,
                    AverageWriterBatchSize: averageBatchSize,
                    P50WriterBatchSize: p50,
                    P95WriterBatchSize: p95,
                    P99WriterBatchSize: p99,
                    NumberOfBatches: numberOfBatches,
                    BatchSizeHistogram: histogramBuilder.ToString(),
                    BatchSizeCounts: batchSizeCounts,
                    AverageCoalescingWaitMicroseconds: averageCoalescingWaitMicroseconds,
                    P50CoalescingWaitMicroseconds: p50CoalescingWaitMicroseconds,
                    P95CoalescingWaitMicroseconds: p95CoalescingWaitMicroseconds,
                    P99CoalescingWaitMicroseconds: p99CoalescingWaitMicroseconds,
                    MaxLogicalOutstandingAheadAtResponse: _diagnosticLogicalOutstandingDepthMax,
                    CapturedOperationCount: _diagnosticIndexByMessageId.Count,
                    SampledOperationCount: sample.Length);
            }

            return new TransitConnectionDiagnosticsSnapshot(
                ConnectionId: ConnectionId,
                Host: _host,
                Port: _port,
                CurrentState: _state,
                IsTlsActive: _tlsActive,
                IsCompressionActive: _compressionActive,
                SocketOpenCount: Interlocked.Read(ref _socketOpenCount),
                ReadyTransitionCount: Interlocked.Read(ref _readyTransitionCount),
                SubmissionsStarted: Interlocked.Read(ref _submissionsStarted),
                SubmissionsAccepted: Interlocked.Read(ref _submissionsAccepted),
                SubmissionsRejected: Interlocked.Read(ref _submissionsRejected),
                SubmissionsAmbiguous: Interlocked.Read(ref _submissionsAmbiguous),
                SubmissionsUnavailable: Interlocked.Read(ref _submissionsUnavailable),
                SubmissionsFailed: Interlocked.Read(ref _submissionsFailed),
                MaxConcurrentSubmissions: Volatile.Read(ref _maxConcurrentSubmissions),
                CurrentConcurrentSubmissions: currentInFlight,
                CurrentWriteIntentQueueDepth: Volatile.Read(ref _diagnosticWriteQueueDepth),
                LocalEndpoint: TryGetEndpointString(static tcpClient => tcpClient.Client.LocalEndPoint),
                RemoteEndpoint: TryGetEndpointString(static tcpClient => tcpClient.Client.RemoteEndPoint),
                DiagnosticsSummary: summary,
                DiagnosticSampleRecords: sample,
                OutstandingOperations: outstandingOperations);
        }

        internal void RecordReconnectEvent()
        {
            Interlocked.Increment(ref _socketOpenCount);
        }

        private string? TryGetEndpointString(Func<TcpClient, EndPoint?> endpointAccessor)
        {
            TcpClient? tcpClient = _tcpClient;
            if (tcpClient is null)
            {
                return null;
            }

            try
            {
                EndPoint? endpoint = endpointAccessor(tcpClient);
                return endpoint?.ToString();
            }
            catch (ObjectDisposedException)
            {
                return null;
            }
            catch (SocketException)
            {
                return null;
            }
            catch (InvalidOperationException)
            {
                return null;
            }
        }

        private static double PercentileFromSorted(int[] sortedValues, double percentile)
        {
            if (sortedValues.Length == 0)
            {
                return 0;
            }

            percentile = Math.Clamp(percentile, 0, 1);
            int index = (int)Math.Ceiling(percentile * sortedValues.Length) - 1;
            index = Math.Clamp(index, 0, sortedValues.Length - 1);
            return sortedValues[index];
        }

        private static double PercentileFromSorted(double[] sortedValues, double percentile)
        {
            if (sortedValues.Length == 0)
            {
                return 0;
            }

            percentile = Math.Clamp(percentile, 0, 1);
            int index = (int)Math.Ceiling(percentile * sortedValues.Length) - 1;
            index = Math.Clamp(index, 0, sortedValues.Length - 1);
            return sortedValues[index];
        }

        private static DiagnosticOperationRecord ToDiagnosticRecord(PendingPublishOperation operation)
        {
            return new DiagnosticOperationRecord(
                MessageId: operation.MessageId,
                T0SubmitEnterTick: operation.T0PublishAsyncEnterTick,
                T0SubmitTakethisEnterTick: operation.T0SubmitTakethisEnterTick,
                T1PendingRegisteredTick: operation.T1PendingRegisteredTick,
                T2WriteIntentEnqueueStartTick: operation.T2WriteIntentEnqueueStartTick,
                T2WriteIntentEnqueuedTick: operation.T2WriteIntentEnqueuedTick,
                T2BeforeCompletionAwaitTick: operation.T2BeforeCompletionAwaitTick,
                T3WriterDequeuedTick: operation.T3WriterDequeuedTick,
                T4AssignedToBatchTick: operation.T4AssignedToBatchTick,
                T5FrameStageBeginTick: operation.T5FrameStageBeginTick,
                T6FrameStageEndTick: operation.T6FrameStageEndTick,
                T7BatchFlushBeginTick: operation.T7BatchFlushBeginTick,
                T8BatchFlushEndTick: operation.T8BatchFlushEndTick,
                T9ResponseCorrelatedTick: operation.T9ResponseCorrelatedTick,
                T10SubmitCompletionTick: operation.T10SubmitCompletionTick,
                PendingDepthAtT1: operation.PendingDepthAtT1,
                PendingDepthAtT2: operation.PendingDepthAtT2,
                PendingDepthAtT3: operation.PendingDepthAtT3,
                PendingDepthAtT4: operation.PendingDepthAtT4,
                PendingDepthAtT9: operation.PendingDepthAtT9,
                QueueDepthAtT2: operation.QueueDepthAtT2,
                QueueDepthAtT3: operation.QueueDepthAtT3,
                QueueDepthAtBatchStart: operation.QueueDepthAtBatchStart,
                BatchDequeuedCount: operation.BatchDequeuedCount,
                QueueDepthAtT9: operation.QueueDepthAtT9,
                BatchId: operation.BatchId,
                BatchPosition: operation.BatchPosition,
                BatchSize: operation.BatchSize,
                SendSequence: operation.SendSequence,
                LogicalOutstandingAheadAtResponse: operation.LogicalOutstandingAheadAtResponse);
        }

        private static OutstandingPublishOperationSnapshot ToOutstandingOperationSnapshot(PendingPublishOperation operation)
        {
            Task<TransitPublishResult> completionTask = operation.Completion.Task;
            TransitPublishStatus? completionStatus = null;
            if (completionTask.IsCompletedSuccessfully)
            {
                completionStatus = completionTask.Result.Status;
            }

            bool writeIntentEnqueued = operation.T2WriteIntentEnqueuedTick > 0;
            bool takethisStagedForWrite = operation.T6FrameStageEndTick > 0;
            bool flushCompleted = operation.T8BatchFlushEndTick > 0;
            bool waitingFor239Response = writeIntentEnqueued && takethisStagedForWrite && operation.T9ResponseCorrelatedTick == 0;

            string likelyAwaitingPath;
            if (completionTask.IsCompleted)
            {
                likelyAwaitingPath = "Completed";
            }
            else if (!writeIntentEnqueued)
            {
                likelyAwaitingPath = "PendingWriteIntentEnqueue";
            }
            else if (!takethisStagedForWrite)
            {
                likelyAwaitingPath = "PendingWriterStage";
            }
            else if (!flushCompleted)
            {
                likelyAwaitingPath = "PendingWriterFlush";
            }
            else
            {
                likelyAwaitingPath = "WaitingFor239Response";
            }

            return new OutstandingPublishOperationSnapshot(
                MessageId: operation.MessageId,
                T2WriteIntentEnqueuedTick: operation.T2WriteIntentEnqueuedTick,
                T6FrameStageEndTick: operation.T6FrameStageEndTick,
                T8BatchFlushEndTick: operation.T8BatchFlushEndTick,
                T9ResponseCorrelatedTick: operation.T9ResponseCorrelatedTick,
                WriteIntentEnqueued: writeIntentEnqueued,
                TakethisStagedForWrite: takethisStagedForWrite,
                FlushCompleted: flushCompleted,
                WaitingFor239Response: waitingFor239Response,
                CompletionTaskIsCompleted: completionTask.IsCompleted,
                CompletionTaskStatus: completionTask.Status.ToString(),
                CompletionStatus: completionStatus,
                LikelyAwaitingPath: likelyAwaitingPath);
        }

        private OutstandingPublishOperationSnapshot[] CaptureOutstandingOperationSnapshots()
        {
            KeyValuePair<string, PendingPublishOperation>[] pending = _pendingByMessageId.ToArray();
            OutstandingPublishOperationSnapshot[] snapshots = new OutstandingPublishOperationSnapshot[pending.Length];
            for (int i = 0; i < pending.Length; i++)
            {
                snapshots[i] = ToOutstandingOperationSnapshot(pending[i].Value);
            }

            Array.Sort(snapshots, static (left, right) => StringComparer.Ordinal.Compare(left.MessageId, right.MessageId));
            return snapshots;
        }

        private void TrackMax(ref long target, long observed)
        {
            while (true)
            {
                long current = Volatile.Read(ref target);
                if (observed <= current)
                {
                    return;
                }

                if (Interlocked.CompareExchange(ref target, observed, current) == current)
                {
                    return;
                }
            }
        }

        private void EnsureDiagnosticOperationTracked(PendingPublishOperation operation)
        {
            lock (_diagnosticGate)
            {
                if (_diagnosticIndexByMessageId.ContainsKey(operation.MessageId))
                {
                    return;
                }

                if (_diagnosticRecords.Count >= DiagnosticMaxOperationRecords)
                {
                    return;
                }

                int index = _diagnosticRecords.Count;
                _diagnosticRecords.Add(ToDiagnosticRecord(operation));
                _diagnosticIndexByMessageId[operation.MessageId] = index;
            }
        }

        private void UpdateDiagnosticOperation(PendingPublishOperation operation)
        {
            lock (_diagnosticGate)
            {
                if (!_diagnosticIndexByMessageId.TryGetValue(operation.MessageId, out int index))
                {
                    if (_diagnosticRecords.Count >= DiagnosticMaxOperationRecords)
                    {
                        return;
                    }

                    index = _diagnosticRecords.Count;
                    _diagnosticIndexByMessageId[operation.MessageId] = index;
                    _diagnosticRecords.Add(ToDiagnosticRecord(operation));
                    return;
                }

                _diagnosticRecords[index] = ToDiagnosticRecord(operation);
            }
        }

        private void IncrementBatchHistogram(int batchSize)
        {
            if (batchSize <= 0)
            {
                return;
            }

            int histogramIndex = Math.Min(batchSize, _diagnosticBatchSizeHistogram.Length - 1);
            lock (_diagnosticGate)
            {
                _diagnosticBatchSizes.Add(batchSize);
                _diagnosticBatchSizeHistogram[histogramIndex]++;
            }
        }

        private void RecordCoalescingWait(long coalescingWaitTicks)
        {
            if (coalescingWaitTicks < 0)
            {
                coalescingWaitTicks = 0;
            }

            double coalescingWaitMicroseconds = coalescingWaitTicks * 1_000_000d / Stopwatch.Frequency;
            lock (_diagnosticGate)
            {
                _diagnosticCoalescingWaitMicroseconds.Add(coalescingWaitMicroseconds);
            }
        }

        private long IncrementDiagnosticQueueDepthOnEnqueue()
        {
            long depth = Interlocked.Increment(ref _diagnosticWriteQueueDepth);
            TrackMax(ref _diagnosticMaxWriteQueueDepth, depth);
            return depth;
        }

        private long DecrementDiagnosticQueueDepthOnDequeue()
        {
            while (true)
            {
                long current = Volatile.Read(ref _diagnosticWriteQueueDepth);
                if (current <= 0)
                {
                    return 0;
                }

                long updated = current - 1;
                if (Interlocked.CompareExchange(ref _diagnosticWriteQueueDepth, updated, current) == current)
                {
                    return updated;
                }
            }
        }

        private int CapturePendingDepthAndTrackMax()
        {
            int pendingDepth = _pendingByMessageId.Count;
            TrackMax(ref _diagnosticMaxPendingDepth, pendingDepth);
            return pendingDepth;
        }

        private void RecordSubmissionCompletion(PendingPublishOperation operation)
        {
            operation.T10SubmitCompletionTick = Stopwatch.GetTimestamp();
            UpdateDiagnosticOperation(operation);
            SignalDrainIfCompleted();
        }

        /// <summary>
        /// Waits for all outstanding TAKETHIS operations to correlate and complete.
        /// </summary>
        private async Task DrainPendingTakethisAsync()
        {
            if (_pendingByMessageId.IsEmpty)
            {
                return;
            }

            TaskCompletionSource drainCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _shutdownDrainCompletion = drainCompletion;
            SignalDrainIfCompleted();
            await drainCompletion.Task.ConfigureAwait(false);
        }

        /// <summary>
        /// Signals graceful-shutdown drain completion when no pending TAKETHIS operations remain.
        /// </summary>
        private void SignalDrainIfCompleted()
        {
            TaskCompletionSource? drainCompletion = _shutdownDrainCompletion;
            if (drainCompletion is null)
            {
                return;
            }

            if (_pendingByMessageId.IsEmpty)
            {
                drainCompletion.TrySetResult();
            }
        }

        private void LogSkippedWriteIntent(WriteIntent intent, long batchId, int batchPosition, bool pendingContains, string skipReason)
        {
            Task<TransitPublishResult> completionTask = intent.Operation.Completion.Task;
            string completionTaskState = completionTask.Status.ToString();
            bool responseLoopPreviouslyCompleted = intent.Operation.T9ResponseCorrelatedTick > 0;
            string completionStatus = "(unavailable)";
            string completionReason = "(unavailable)";

            if (completionTask.IsCompletedSuccessfully)
            {
                TransitPublishResult completionResult = completionTask.Result;
                completionStatus = completionResult.Status.ToString();
                completionReason = completionResult.ResponseText ?? "(none)";
            }

            _logger.LogInformation(
                "[WRITE-SKIP-DIAG] connectionId={ConnectionId} messageId={MessageId} batchId={BatchId} batchPosition={BatchPosition} sendSequence={SendSequence} t1PendingRegisteredTick={T1PendingRegisteredTick} t2WriteIntentEnqueuedTick={T2WriteIntentEnqueuedTick} t3WriterDequeuedTick={T3WriterDequeuedTick} pendingContains={PendingContains} completionTaskState={CompletionTaskState} responseLoopPreviouslyCompleted={ResponseLoopPreviouslyCompleted} completionStatus={CompletionStatus} completionReason={CompletionReason} skipReason={SkipReason}",
                ConnectionId,
                intent.MessageId,
                batchId,
                batchPosition,
                intent.Operation.SendSequence,
                intent.Operation.T1PendingRegisteredTick,
                intent.Operation.T2WriteIntentEnqueuedTick,
                intent.Operation.T3WriterDequeuedTick,
                pendingContains,
                completionTaskState,
                responseLoopPreviouslyCompleted,
                completionStatus,
                completionReason,
                skipReason);
        }

        internal async Task InitializeAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("[INIT-TRACE] TransitConnection.InitializeAsync START connectionId={ConnectionId}", ConnectionId);
            cancellationToken.ThrowIfCancellationRequested();

            bool fallbackAttempted = false;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    _logger.LogInformation("[INIT-TRACE] TransitConnection.InitializeAsync BEFORE InitializeCoreAsync connectionId={ConnectionId}", ConnectionId);
                    await InitializeCoreAsync(cancellationToken).ConfigureAwait(false);
                    _logger.LogInformation("[INIT-TRACE] TransitConnection.InitializeAsync COMPLETE connectionId={ConnectionId}", ConnectionId);
                    return;
                }
                catch (OperationCanceledException ex) when (cancellationToken.IsCancellationRequested)
                {
                    _logger.LogError(ex, "[INIT-TRACE] TransitConnection.InitializeAsync CANCELED connectionId={ConnectionId}: {ExceptionType}: {ExceptionMessage}", ConnectionId, ex.GetType().FullName, ex.Message);
                    await CleanupFailedInitializationAttemptAsync().ConfigureAwait(false);
                    throw;
                }
                catch (Exception ex) when (!fallbackAttempted && IsCompressionInteroperabilityFailure(ex))
                {
                    _logger.LogWarning(ex, "[INIT-TRACE] TransitConnection.InitializeAsync COMPRESS fallback triggered connectionId={ConnectionId}: {ExceptionType}: {ExceptionMessage}", ConnectionId, ex.GetType().FullName, ex.Message);
                    fallbackAttempted = true;
                    _skipCompressionForCurrentInitialization = true;
                    LogTransitCompressionInteroperabilityFallback(
                        _logger,
                        ConnectionId,
                        _host,
                        _port);

                    await CleanupFailedInitializationAttemptAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[INIT-TRACE] TransitConnection.InitializeAsync FAILED connectionId={ConnectionId}: {ExceptionType}: {ExceptionMessage}", ConnectionId, ex.GetType().FullName, ex.Message);
                    await CleanupFailedInitializationAttemptAsync().ConfigureAwait(false);
                    throw;
                }
            }
        }

        private async Task InitializeCoreAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("[INIT-TRACE] TransitConnection.InitializeCoreAsync START connectionId={ConnectionId}", ConnectionId);
            TransitionState(TransitConnectionState.Connecting);
            _tcpClient = new TcpClient();
            _logger.LogInformation("[INIT-TRACE] TransitConnection BEFORE ConnectAsync connectionId={ConnectionId}, host={Host}, port={Port}", ConnectionId, _host, _port);
            await _tcpClient.ConnectAsync(_host, _port, cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("[INIT-TRACE] TransitConnection AFTER ConnectAsync connectionId={ConnectionId}", ConnectionId);
            Interlocked.Increment(ref _socketOpenCount);

            _transportStream = _tcpClient.GetStream();
            _readStream = _transportStream;
            _writeStream = _transportStream;
            _tlsActive = false;
            _compressionActive = false;
            _streamingModeNegotiated = false;

            if (_useSsl)
            {
                TransitionState(TransitConnectionState.StartingTls);
                _logger.LogInformation("[INIT-TRACE] TransitConnection BEFORE TLS handshake (useSsl=true) connectionId={ConnectionId}", ConnectionId);
                await UpgradeToTlsAsync(cancellationToken).ConfigureAwait(false);
                _logger.LogInformation("[INIT-TRACE] TransitConnection AFTER TLS handshake (useSsl=true) connectionId={ConnectionId}", ConnectionId);
                TransitionState(TransitConnectionState.TlsEstablished);
            }

            await RebuildPipesAsync(cancellationToken).ConfigureAwait(false);

            TransitionState(TransitConnectionState.AwaitingGreeting);
            _logger.LogInformation("[INIT-TRACE] TransitConnection BEFORE greeting read connectionId={ConnectionId}", ConnectionId);
            string greetingLine = await ReadLineAsync(cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("[INIT-TRACE] TransitConnection AFTER greeting read connectionId={ConnectionId}, greeting={Greeting}", ConnectionId, greetingLine);
            TransitProtocolParser.ValidateGreeting(greetingLine);

            TransitionState(TransitConnectionState.CapabilitiesNegotiation);
            _logger.LogInformation("[INIT-TRACE] TransitConnection BEFORE capabilities connectionId={ConnectionId}", ConnectionId);
            _capabilities = await ReadCapabilitiesAsync(cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("[INIT-TRACE] TransitConnection AFTER capabilities connectionId={ConnectionId}, startTls={SupportsStartTls}, compress={SupportsCompressDeflate}, streaming={SupportsStreaming}", ConnectionId, _capabilities.SupportsStartTls, _capabilities.SupportsCompressDeflate, _capabilities.SupportsStreaming);

            if (!_useSsl && _capabilities.SupportsStartTls)
            {
                TransitionState(TransitConnectionState.StartingTls);
                _logger.LogInformation("[INIT-TRACE] TransitConnection BEFORE STARTTLS command/handshake connectionId={ConnectionId}", ConnectionId);
                await StartTlsAsync(cancellationToken).ConfigureAwait(false);
                _logger.LogInformation("[INIT-TRACE] TransitConnection AFTER STARTTLS command/handshake connectionId={ConnectionId}", ConnectionId);
                TransitionState(TransitConnectionState.TlsEstablished);

                TransitionState(TransitConnectionState.CapabilitiesNegotiation);
                _logger.LogInformation("[INIT-TRACE] TransitConnection BEFORE capabilities (post-STARTTLS) connectionId={ConnectionId}", ConnectionId);
                _capabilities = await ReadCapabilitiesAsync(cancellationToken).ConfigureAwait(false);
                _logger.LogInformation("[INIT-TRACE] TransitConnection AFTER capabilities (post-STARTTLS) connectionId={ConnectionId}, startTls={SupportsStartTls}, compress={SupportsCompressDeflate}, streaming={SupportsStreaming}", ConnectionId, _capabilities.SupportsStartTls, _capabilities.SupportsCompressDeflate, _capabilities.SupportsStreaming);
            }

            if (!_skipCompressionForCurrentInitialization && _capabilities.SupportsCompressDeflate)
            {
                TransitionState(TransitConnectionState.StartingCompression);
                _logger.LogInformation("[INIT-TRACE] TransitConnection BEFORE COMPRESS DEFLATE connectionId={ConnectionId}", ConnectionId);
                await StartCompressionAsync(cancellationToken).ConfigureAwait(false);
                _logger.LogInformation("[INIT-TRACE] TransitConnection AFTER COMPRESS DEFLATE connectionId={ConnectionId}", ConnectionId);
                TransitionState(TransitConnectionState.CompressionEstablished);
            }
            else
            {
                _logger.LogInformation("[INIT-TRACE] TransitConnection COMPRESS skipped connectionId={ConnectionId}, skipFlag={SkipFlag}, supportsCompress={SupportsCompress}", ConnectionId, _skipCompressionForCurrentInitialization, _capabilities.SupportsCompressDeflate);
            }

            if (!_capabilities.SupportsStreaming)
            {
                throw new InvalidOperationException("Transit server does not advertise STREAMING capability required for TAKETHIS publishing.");
            }

            TransitionState(TransitConnectionState.StartingStreaming);
            _logger.LogInformation("[INIT-TRACE] TransitConnection BEFORE MODE STREAM connectionId={ConnectionId}", ConnectionId);
            await EnterStreamingModeAsync(cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("[INIT-TRACE] TransitConnection AFTER MODE STREAM connectionId={ConnectionId}", ConnectionId);

            if (!_streamingModeNegotiated)
            {
                throw new InvalidOperationException("Transit connection cannot enter Ready state before MODE STREAM is successfully negotiated.");
            }

            TransitionState(TransitConnectionState.Ready);
            Interlocked.Increment(ref _readyTransitionCount);
            LogTransitConnectionReady(_logger, ConnectionId, _tlsActive, _compressionActive);

            _responseLoopCancellation = new CancellationTokenSource();
            _responseLoopTask = Task.Run(() => ResponseLoopAsync(_responseLoopCancellation.Token), CancellationToken.None);

            _writeIntentChannel = Channel.CreateBounded<WriteIntent>(new BoundedChannelOptions(_writeIntentQueueCapacity)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait,
            });

            CancellationTokenSource writeLoopCancellation = new();
            _writeLoopCancellation = writeLoopCancellation;
            _writeLoopTask = Task.Run(() => WriteLoopAsync(writeLoopCancellation.Token), CancellationToken.None);
            _logger.LogInformation("[INIT-TRACE] TransitConnection.InitializeCoreAsync COMPLETE connectionId={ConnectionId}", ConnectionId);
        }

        private bool IsCompressionInteroperabilityFailure(Exception exception)
        {
            if (!_compressionActive || _state != TransitConnectionState.StartingStreaming)
            {
                return false;
            }

            Exception candidate = exception;
            if (candidate is IOException ioException && ioException.InnerException is not null)
            {
                candidate = ioException.InnerException;
            }

            if (candidate is InvalidDataException invalidDataException)
            {
                return invalidDataException.Message.Contains(
                    "unsupported compression method",
                    StringComparison.OrdinalIgnoreCase);
            }

            if (candidate is InvalidOperationException invalidOperationException)
            {
                return invalidOperationException.Message.Contains(
                    "unsupported compression method",
                    StringComparison.OrdinalIgnoreCase);
            }

            return false;
        }

        private async Task ResetTransportStateAsync()
        {
            await StopWriteLoopAsync(requestCancellation: true, drainQueuedWriteIntentsAsAmbiguous: true).ConfigureAwait(false);

            PipeWriter? writer = _writer;
            _writer = null;

            if (writer is not null)
            {
                try
                {
                    await writer.CompleteAsync().ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException or InvalidOperationException)
                {
                }
            }

            PipeReader? reader = _reader;
            _reader = null;

            if (reader is not null)
            {
                try
                {
                    await reader.CompleteAsync().ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException or InvalidOperationException)
                {
                }
            }

            DisposeTransportArtifacts();

            _tlsActive = false;
            _compressionActive = false;
            _streamingModeNegotiated = false;
            _capabilities = new TransitCapabilitySnapshot(
                SupportsStartTls: false,
                SupportsCompressDeflate: false,
                SupportsStreaming: false);
        }

        private async Task CleanupFailedInitializationAttemptAsync()
        {
            try
            {
                await ResetTransportStateAsync().ConfigureAwait(false);
            }
            catch
            {
            }

            if (_responseLoopCancellation is not null)
            {
                try
                {
                    _responseLoopCancellation.Cancel();
                }
                catch (ObjectDisposedException)
                {
                }

                _responseLoopCancellation.Dispose();
                _responseLoopCancellation = null;
            }

            _responseLoopTask = null;
            TransitionState(TransitConnectionState.Disconnected);
        }

        private async Task EnterStreamingModeAsync(CancellationToken cancellationToken)
        {
            await WriteCommandAsync("MODE STREAM", cancellationToken).ConfigureAwait(false);
            string modeStreamResponse = await ReadLineAsync(cancellationToken).ConfigureAwait(false);
            (int code, string text, _) = TransitProtocolParser.ParseStatusLine(modeStreamResponse);

            if (code != 203)
            {
                throw new InvalidOperationException($"MODE STREAM rejected by transit server ({code}): {text}");
            }

            _streamingModeNegotiated = true;
        }

        private async Task StartCompressionAsync(CancellationToken cancellationToken)
        {
            await WriteCommandAsync("COMPRESS DEFLATE", cancellationToken).ConfigureAwait(false);
            string compressionResponse = await ReadLineAsync(cancellationToken).ConfigureAwait(false);
            (int code, string text, _) = TransitProtocolParser.ParseStatusLine(compressionResponse);

            if (code != 206)
            {
                throw new InvalidOperationException($"COMPRESS DEFLATE negotiation failed ({code}): {text}");
            }

            if (_transportStream is null)
            {
                throw new InvalidOperationException("Transit connection transport stream is not initialized for compression.");
            }

            _readStream = new DeflateStream(_transportStream, CompressionMode.Decompress, leaveOpen: true);
            _writeStream = new DeflateStream(_transportStream, CompressionMode.Compress, leaveOpen: true);
            _compressionActive = true;

            await RebuildPipesAsync(cancellationToken).ConfigureAwait(false);
        }

        private async Task StartTlsAsync(CancellationToken cancellationToken)
        {
            await WriteCommandAsync("STARTTLS", cancellationToken).ConfigureAwait(false);
            string startTlsResponse = await ReadLineAsync(cancellationToken).ConfigureAwait(false);
            (int code, string text, _) = TransitProtocolParser.ParseStatusLine(startTlsResponse);

            if (code != 382)
            {
                throw new InvalidOperationException($"STARTTLS negotiation rejected ({code}): {text}");
            }

            await UpgradeToTlsAsync(cancellationToken).ConfigureAwait(false);
            await RebuildPipesAsync(cancellationToken).ConfigureAwait(false);
        }

        private async Task UpgradeToTlsAsync(CancellationToken cancellationToken)
        {
            if (_transportStream is null)
            {
                throw new InvalidOperationException("Transit connection transport stream is not initialized for TLS negotiation.");
            }

            SslStream sslStream = new(_transportStream, leaveInnerStreamOpen: false);
            SslClientAuthenticationOptions options = new()
            {
                TargetHost = _host,
                EnabledSslProtocols = SslProtocols.None,
                RemoteCertificateValidationCallback = _serverCertificateValidationCallback,
            };

            await sslStream.AuthenticateAsClientAsync(options, cancellationToken).ConfigureAwait(false);

            _transportStream = sslStream;
            _readStream = sslStream;
            _writeStream = sslStream;
            _tlsActive = true;
        }

        private async Task<TransitCapabilitySnapshot> ReadCapabilitiesAsync(CancellationToken cancellationToken)
        {
            await WriteCommandAsync("CAPABILITIES", cancellationToken).ConfigureAwait(false);

            List<string> lines = [];

            while (true)
            {
                string line = await ReadLineAsync(cancellationToken).ConfigureAwait(false);
                lines.Add(line);

                if (line == ".")
                {
                    break;
                }
            }

            TransitCapabilitySnapshot snapshot = TransitProtocolParser.ParseCapabilitiesResponse(lines);
            LogTransitCapabilities(_logger, ConnectionId, snapshot.SupportsStartTls, snapshot.SupportsCompressDeflate, snapshot.SupportsStreaming);
            return snapshot;
        }

        private async Task<string> ReadLineAsync(CancellationToken cancellationToken)
        {
            if (_reader is null)
            {
                throw new InvalidOperationException("Transit protocol reader is not initialized.");
            }

            (string line, int bytesRead) = await TransitProtocolParser.ReadNntpLineWithByteCountAsync(_reader, cancellationToken).ConfigureAwait(false);
            Interlocked.Add(ref _bytesReceived, bytesRead);
            return line;
        }

        private async Task WriteCommandAsync(string command, CancellationToken cancellationToken)
        {
            if (_writer is null)
            {
                throw new InvalidOperationException("Transit protocol writer is not initialized.");
            }

            ArgumentException.ThrowIfNullOrWhiteSpace(command);

            byte[] bytes = System.Text.Encoding.ASCII.GetBytes(command + "\r\n");
            await _writer.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
            Interlocked.Add(ref _bytesTransmitted, bytes.Length);
            FlushResult flush = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);

            if (flush.IsCompleted)
            {
                throw new InvalidOperationException("Transit protocol writer was completed while issuing NNTP command.");
            }
        }

        private async Task RebuildPipesAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_readStream is null || _writeStream is null)
            {
                throw new InvalidOperationException("Transit transport streams are not initialized.");
            }

            if (_reader is not null)
            {
                EnsureNoBufferedReadData();
                await _reader.CompleteAsync().ConfigureAwait(false);
            }

            if (_writer is not null)
            {
                await _writer.CompleteAsync().ConfigureAwait(false);
            }

            _reader = PipeReader.Create(_readStream, new StreamPipeReaderOptions(leaveOpen: true));
            _writer = PipeWriter.Create(_writeStream, new StreamPipeWriterOptions(leaveOpen: true));
        }

        private void EnsureNoBufferedReadData()
        {
            if (_reader is null)
            {
                return;
            }

            if (!_reader.TryRead(out ReadResult peek))
            {
                return;
            }

            ReadOnlySequence<byte> buffer = peek.Buffer;
            _reader.AdvanceTo(buffer.Start, buffer.Start);

            if (!buffer.IsEmpty)
            {
                throw new InvalidOperationException("Buffered NNTP data remained in PipeReader during transport-layer transition.");
            }
        }

        private async Task ResponseLoopAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    string responseLine = await ReadLineAsync(cancellationToken).ConfigureAwait(false);
                    long responseAvailableTick = Stopwatch.GetTimestamp();
                    TransitPublishResult? mapped = MapTakethisResponse(responseLine, responseAvailableTick);
                    if (mapped is null)
                    {
                        continue;
                    }

                    if (_pendingByMessageId.TryRemove(mapped.MessageId, out PendingPublishOperation pending))
                    {
                        pending.T9ResponseCorrelatedTick = Stopwatch.GetTimestamp();
                        pending.PendingDepthAtT9 = CapturePendingDepthAndTrackMax();
                        pending.QueueDepthAtT9 = Volatile.Read(ref _diagnosticWriteQueueDepth);
                        long currentSendSequence = Volatile.Read(ref _diagnosticSendSequence);
                        if (pending.SendSequence > 0)
                        {
                            long laterTransmittedCount = currentSendSequence - pending.SendSequence;
                            if (laterTransmittedCount > 0)
                            {
                                pending.LogicalOutstandingAheadAtResponse = laterTransmittedCount;
                                TrackMax(ref _diagnosticLogicalOutstandingDepthMax, laterTransmittedCount);
                            }
                        }

                        UpdateDiagnosticOperation(pending);

                        AcknowledgeSendOrder(mapped.MessageId);
                        TransitPublishResult correlatedResult = mapped with
                        {
                            T0PublishAsyncEnterTick = pending.T0PublishAsyncEnterTick,
                            T1DispatcherAssignedTick = pending.T1DispatcherAssignedTick,
                            T2SocketWriteBeginTick = pending.T2SocketWriteBeginTick,
                            T3SocketWriteEndTick = pending.T3SocketWriteEndTick,
                            T4ResponseAvailableTick = mapped.T4ResponseAvailableTick == 0 ? responseAvailableTick : mapped.T4ResponseAvailableTick,
                            T6ResponseCorrelatedTick = pending.T9ResponseCorrelatedTick,
                        };
                        pending.Completion.TrySetResult(correlatedResult);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                TransitionState(TransitConnectionState.Faulted);
                LogTransitResponseLoopFaulted(_logger, ex, ConnectionId);
                FailOutstandingAsAmbiguous("Connection failed before definitive TAKETHIS responses were received.");
            }
        }

        private async Task WriteLoopAsync(CancellationToken cancellationToken)
        {
            Channel<WriteIntent>? writeIntentChannel = _writeIntentChannel;
            if (writeIntentChannel is null)
            {
                return;
            }

            ChannelReader<WriteIntent> reader = writeIntentChannel.Reader;
            List<WriteIntent> batch = new(_maxWriteBatchSize);

            try
            {
                while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    batch.Clear();

                    int writerThreadId = Environment.CurrentManagedThreadId;
                    int writerTaskId = Task.CurrentId ?? -1;
                    long batchId = Interlocked.Increment(ref _diagnosticBatchIdSequence);
                    long queueDepthBeforeDrain = Volatile.Read(ref _diagnosticWriteQueueDepth);
                    int dequeueAttempt = 1;
                    string drainExitReason = "Other";
                    StringBuilder drainAttemptLog = new();

                    long queueDepthBeforeFirstTryDequeue = Volatile.Read(ref _diagnosticWriteQueueDepth);
                    bool firstDequeueSucceeded = reader.TryRead(out WriteIntent firstIntent);
                    drainAttemptLog.Append("attempt=")
                        .Append(dequeueAttempt)
                        .Append(" queueBeforeTryDequeue=")
                        .Append(queueDepthBeforeFirstTryDequeue)
                        .Append(" success=")
                        .Append(firstDequeueSucceeded ? "true" : "false");

                    if (!firstDequeueSucceeded)
                    {
                        drainAttemptLog.Append(" batchCount=0");
                        _logger.LogInformation("[BATCH-DRAIN-DIAG] connectionId={ConnectionId} batchId={BatchId} writerThreadId={WriterThreadId} writerTaskId={WriterTaskId} queueBeforeDrain={QueueBeforeDrain} {DrainAttempts} finalDequeued=0 finalStaged=0 exitReason={ExitReason}", ConnectionId, batchId, writerThreadId, writerTaskId, queueDepthBeforeDrain, drainAttemptLog.ToString(), "QueueEmpty");
                        continue;
                    }

                    batch.Add(firstIntent);
                    long queueDepthAfterFirstDequeue = DecrementDiagnosticQueueDepthOnDequeue();
                    firstIntent.Operation.T3WriterDequeuedTick = Stopwatch.GetTimestamp();
                    firstIntent.Operation.QueueDepthAtT3 = queueDepthAfterFirstDequeue;
                    firstIntent.Operation.PendingDepthAtT3 = CapturePendingDepthAndTrackMax();
                    drainAttemptLog.Append(" batchCount=").Append(batch.Count);

                    long coalesceStartTick = Stopwatch.GetTimestamp();
                    long coalesceEndTick = coalesceStartTick;
                    long coalesceBudgetTicks = Math.Max(1L, (long)Math.Round(_writeBatchCoalesceMicroseconds * Stopwatch.Frequency / 1_000_000d));
                    long coalesceDeadlineTick = coalesceStartTick + coalesceBudgetTicks;
                    int additionalTryReadAttempts = 0;
                    int successfulAdditionalDequeues = 0;

                    while (batch.Count < _maxWriteBatchSize)
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            drainExitReason = "Cancellation";
                            break;
                        }

                        long nowTick = Stopwatch.GetTimestamp();
                        if (nowTick >= coalesceDeadlineTick)
                        {
                            drainExitReason = "CoalesceWindowExpired";
                            break;
                        }

                        dequeueAttempt++;
                        additionalTryReadAttempts++;
                        long queueDepthBeforeTryDequeue = Volatile.Read(ref _diagnosticWriteQueueDepth);
                        bool nextDequeueSucceeded = reader.TryRead(out WriteIntent nextIntent);

                        drainAttemptLog.Append(" | attempt=")
                            .Append(dequeueAttempt)
                            .Append(" queueBeforeTryDequeue=")
                            .Append(queueDepthBeforeTryDequeue)
                            .Append(" success=")
                            .Append(nextDequeueSucceeded ? "true" : "false");

                        if (!nextDequeueSucceeded)
                        {
                            long remainingTicks = coalesceDeadlineTick - nowTick;
                            if (remainingTicks > 0)
                            {
                                long remainingMilliseconds = Math.Max(1L, (long)Math.Ceiling(remainingTicks * 1000d / Stopwatch.Frequency));
                                await Task.Delay(TimeSpan.FromMilliseconds(remainingMilliseconds), cancellationToken).ConfigureAwait(false);
                                continue;
                            }

                            drainAttemptLog.Append(" batchCount=").Append(batch.Count);
                            drainExitReason = "QueueEmpty";
                            break;
                        }

                        batch.Add(nextIntent);
                        successfulAdditionalDequeues++;
                        long queueDepthAfterDequeue = DecrementDiagnosticQueueDepthOnDequeue();
                        nextIntent.Operation.T3WriterDequeuedTick = Stopwatch.GetTimestamp();
                        nextIntent.Operation.QueueDepthAtT3 = queueDepthAfterDequeue;
                        nextIntent.Operation.PendingDepthAtT3 = CapturePendingDepthAndTrackMax();
                        drainAttemptLog.Append(" batchCount=").Append(batch.Count);
                    }

                    coalesceEndTick = Stopwatch.GetTimestamp();
                    RecordCoalescingWait(coalesceEndTick - coalesceStartTick);
                    _logger.LogInformation("[COALESCE-DIAG] connectionId={ConnectionId} batchId={BatchId} coalesceStartTick={CoalesceStartTick} coalesceEndTick={CoalesceEndTick} configuredWindowUs={ConfiguredWindowUs} additionalTryReadAttempts={AdditionalTryReadAttempts} successfulAdditionalDequeues={SuccessfulAdditionalDequeues} finalBatchSize={FinalBatchSize}", ConnectionId, batchId, coalesceStartTick, coalesceEndTick, _writeBatchCoalesceMicroseconds, additionalTryReadAttempts, successfulAdditionalDequeues, batch.Count);

                    if (drainExitReason == "Other")
                    {
                        drainExitReason = batch.Count >= _maxWriteBatchSize ? "ReachedMaxBatchSize" : "Other";
                    }

                    await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
                    try
                    {
                        PipeWriter? writer = _writer;
                        if (writer is null)
                        {
                            throw new InvalidOperationException("Transit protocol writer is not initialized.");
                        }

                        TransitionState(TransitConnectionState.Publishing);
                        int batchBytesStaged = 0;

                        int batchDequeuedCount = batch.Count;
                        int batchSize = 0;
                        int skippedNotPendingCount = 0;
                        int skippedCompletedCount = 0;
                        long flushBeginTick = 0;
                        long flushEndTick = 0;

                        for (int i = 0; i < batch.Count; i++)
                        {
                            WriteIntent intent = batch[i];
                            int batchPosition = i + 1;

                            if (!_pendingByMessageId.ContainsKey(intent.MessageId))
                            {
                                skippedNotPendingCount++;
                                LogSkippedWriteIntent(intent, batchId, batchPosition, pendingContains: false, skipReason: "NotPendingAtStage");
                                continue;
                            }

                            if (intent.Operation.Completion.Task.IsCompleted)
                            {
                                skippedCompletedCount++;
                                bool pendingContains = _pendingByMessageId.ContainsKey(intent.MessageId);
                                LogSkippedWriteIntent(intent, batchId, batchPosition, pendingContains, skipReason: "CompletionAlreadySetBeforeStage");
                                _pendingByMessageId.TryRemove(intent.MessageId, out _);
                                continue;
                            }

                            intent.Operation.T4AssignedToBatchTick = Stopwatch.GetTimestamp();
                            intent.Operation.PendingDepthAtT4 = CapturePendingDepthAndTrackMax();
                            intent.Operation.QueueDepthAtBatchStart = queueDepthBeforeDrain;
                            intent.Operation.BatchDequeuedCount = batchDequeuedCount;
                            intent.Operation.BatchId = batchId;
                            intent.Operation.BatchPosition = batchSize + 1;
                            intent.Operation.SendSequence = Interlocked.Increment(ref _diagnosticSendSequence);
                            intent.Operation.T5FrameStageBeginTick = Stopwatch.GetTimestamp();
                            intent.Operation.T2SocketWriteBeginTick = intent.Operation.T5FrameStageBeginTick;
                            batchBytesStaged += StageTakethisFrame(writer, intent.MessageId, intent.ArticlePayload);
                            intent.Operation.T6FrameStageEndTick = Stopwatch.GetTimestamp();
                            intent.Operation.T3SocketWriteEndTick = intent.Operation.T6FrameStageEndTick;

                            batchSize++;
                            batch[batchSize - 1] = intent;
                        }

                        if (batchBytesStaged <= 0)
                        {
                            if (batchDequeuedCount > 0)
                            {
                                _logger.LogInformation("[BATCH-DIAG] connectionId={ConnectionId} batchId={BatchId} queueBeforeDrain={QueueBeforeDrain} dequeued={Dequeued} staged={Staged} skippedNotPending={SkippedNotPending} skippedCompleted={SkippedCompleted} flushTick=0", ConnectionId, batchId, queueDepthBeforeDrain, batchDequeuedCount, batchSize, skippedNotPendingCount, skippedCompletedCount);
                            }

                            _logger.LogInformation("[BATCH-DRAIN-DIAG] connectionId={ConnectionId} batchId={BatchId} writerThreadId={WriterThreadId} writerTaskId={WriterTaskId} queueBeforeDrain={QueueBeforeDrain} {DrainAttempts} finalDequeued={FinalDequeued} finalStaged={FinalStaged} exitReason={ExitReason}", ConnectionId, batchId, writerThreadId, writerTaskId, queueDepthBeforeDrain, drainAttemptLog.ToString(), batchDequeuedCount, batchSize, drainExitReason);
                            continue;
                        }

                        for (int i = 0; i < batchSize; i++)
                        {
                            WriteIntent intent = batch[i];
                            intent.Operation.BatchSize = batchSize;
                            UpdateDiagnosticOperation(intent.Operation);
                        }

                        IncrementBatchHistogram(batchSize);
                        TrackMax(ref _diagnosticMaxPendingDepth, _pendingByMessageId.Count);

                        Interlocked.Add(ref _bytesTransmitted, batchBytesStaged);

                        flushBeginTick = Stopwatch.GetTimestamp();
                        for (int i = 0; i < batchSize; i++)
                        {
                            batch[i].Operation.T7BatchFlushBeginTick = flushBeginTick;
                        }

                        FlushResult flush = await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                        flushEndTick = Stopwatch.GetTimestamp();

                        _logger.LogInformation("[BATCH-DIAG] connectionId={ConnectionId} batchId={BatchId} queueBeforeDrain={QueueBeforeDrain} dequeued={Dequeued} staged={Staged} skippedNotPending={SkippedNotPending} skippedCompleted={SkippedCompleted} flushTick={FlushTick}", ConnectionId, batchId, queueDepthBeforeDrain, batchDequeuedCount, batchSize, skippedNotPendingCount, skippedCompletedCount, flushEndTick);
                        _logger.LogInformation("[BATCH-DRAIN-DIAG] connectionId={ConnectionId} batchId={BatchId} writerThreadId={WriterThreadId} writerTaskId={WriterTaskId} queueBeforeDrain={QueueBeforeDrain} {DrainAttempts} finalDequeued={FinalDequeued} finalStaged={FinalStaged} exitReason={ExitReason}", ConnectionId, batchId, writerThreadId, writerTaskId, queueDepthBeforeDrain, drainAttemptLog.ToString(), batchDequeuedCount, batchSize, drainExitReason);

                        for (int i = 0; i < batchSize; i++)
                        {
                            batch[i].Operation.T8BatchFlushEndTick = flushEndTick;
                            UpdateDiagnosticOperation(batch[i].Operation);
                        }

                        if (flush.IsCompleted)
                        {
                            throw new InvalidOperationException("Transit protocol writer completed during TAKETHIS submission.");
                        }
                    }
                    finally
                    {
                        _writeGate.Release();
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (ChannelClosedException)
            {
            }
            catch (Exception ex)
            {
                TransitionState(TransitConnectionState.Faulted);
                _logger.LogWarning(ex, "Transit connection {ConnectionId} write loop faulted", ConnectionId);
                writeIntentChannel.Writer.TryComplete(ex);
                FailOutstandingAsAmbiguous("Connection failed before definitive TAKETHIS responses were received.");
            }
            finally
            {
                DrainQueuedWriteIntentsAsAmbiguous(reader, "Connection closed before definitive TAKETHIS responses were received.");
            }
        }

        private async Task StopWriteLoopAsync(bool requestCancellation, bool drainQueuedWriteIntentsAsAmbiguous)
        {
            Channel<WriteIntent>? writeIntentChannel = _writeIntentChannel;
            long pendingBeforeStop = _pendingByMessageId.Count;
            long queueDepthBeforeStop = Volatile.Read(ref _diagnosticWriteQueueDepth);
            _logger.LogInformation("[SHUTDOWN-DIAG] TransitConnection.StopWriteLoopAsync start connectionId={ConnectionId} pendingMessageIds={PendingMessageIds} writeQueueDepth={WriteQueueDepth}", ConnectionId, pendingBeforeStop, queueDepthBeforeStop);
            _writeIntentChannel = null;

            if (writeIntentChannel is not null)
            {
                writeIntentChannel.Writer.TryComplete();
                _logger.LogInformation("[SHUTDOWN-DIAG] TransitConnection write-intent channel completed connectionId={ConnectionId}", ConnectionId);
            }

            CancellationTokenSource? writeLoopCancellation = _writeLoopCancellation;
            _writeLoopCancellation = null;

            if (requestCancellation && writeLoopCancellation is not null)
            {
                try
                {
                    writeLoopCancellation.Cancel();
                    _logger.LogInformation("[SHUTDOWN-DIAG] TransitConnection write loop cancellation requested connectionId={ConnectionId}", ConnectionId);
                }
                catch (ObjectDisposedException)
                {
                }
            }

            Task? writeLoopTask = _writeLoopTask;
            _writeLoopTask = null;

            if (writeLoopTask is not null)
            {
                try
                {
                    await writeLoopTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }

                _logger.LogInformation("[SHUTDOWN-DIAG] TransitConnection write loop terminated connectionId={ConnectionId}", ConnectionId);
            }

            if (drainQueuedWriteIntentsAsAmbiguous && writeIntentChannel is not null)
            {
                DrainQueuedWriteIntentsAsAmbiguous(writeIntentChannel.Reader, "Connection closed before definitive TAKETHIS responses were received.");
                _logger.LogInformation("[SHUTDOWN-DIAG] TransitConnection drained queued write intents as ambiguous connectionId={ConnectionId}", ConnectionId);
            }

            if (writeLoopCancellation is not null)
            {
                writeLoopCancellation.Dispose();
            }

            long pendingAfterStop = _pendingByMessageId.Count;
            long queueDepthAfterStop = Volatile.Read(ref _diagnosticWriteQueueDepth);
            _logger.LogInformation("[SHUTDOWN-DIAG] TransitConnection.StopWriteLoopAsync complete connectionId={ConnectionId} pendingMessageIds={PendingMessageIds} writeQueueDepth={WriteQueueDepth}", ConnectionId, pendingAfterStop, queueDepthAfterStop);
        }

        private void DrainQueuedWriteIntentsAsAmbiguous(ChannelReader<WriteIntent> reader, string reason)
        {
            int drainedCount = 0;
            while (reader.TryRead(out WriteIntent intent))
            {
                if (_pendingByMessageId.TryRemove(intent.MessageId, out PendingPublishOperation? pending))
                {
                    pending.Completion.TrySetResult(new TransitPublishResult(
                        MessageId: intent.MessageId,
                        Status: TransitPublishStatus.Ambiguous,
                        ResponseCode: null,
                        ResponseText: reason));
                    Interlocked.Increment(ref _submissionsAmbiguous);
                    drainedCount++;
                }
            }

            if (drainedCount > 0)
            {
                _logger.LogInformation("[SHUTDOWN-DIAG] TransitConnection drained queued write intents count={DrainedCount} connectionId={ConnectionId}", drainedCount, ConnectionId);
            }
        }

        /// <summary>
        /// Sends QUIT over a healthy connection and ensures command bytes are flushed to transport.
        /// </summary>
        private async Task SendQuitAsync()
        {
            if (_state == TransitConnectionState.Faulted)
            {
                _logger.LogInformation("[SHUTDOWN-DIAG] TransitConnection skipping QUIT because connection is faulted connectionId={ConnectionId}", ConnectionId);
                return;
            }

            if (_writer is null)
            {
                _logger.LogInformation("[SHUTDOWN-DIAG] TransitConnection skipping QUIT because protocol writer is not initialized connectionId={ConnectionId}", ConnectionId);
                return;
            }

            try
            {
                await _writeGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
                try
                {
                    _logger.LogInformation("[SHUTDOWN-DIAG] TransitConnection sending QUIT connectionId={ConnectionId}", ConnectionId);
                    await WriteCommandAsync("QUIT", CancellationToken.None).ConfigureAwait(false);
                }
                finally
                {
                    _writeGate.Release();
                }
            }
            catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException or InvalidOperationException)
            {
                _logger.LogWarning(ex, "Transit connection {ConnectionId} failed while sending QUIT.", ConnectionId);
            }
        }

        private TransitPublishResult? MapTakethisResponse(string responseLine, long responseAvailableTick)
        {
            (int code, string responseText, string[] tokens) = TransitProtocolParser.ParseStatusLine(responseLine);
            long responseParsedTick = Stopwatch.GetTimestamp();

            if (code is 239 or 439 or 431 or 400)
            {
                string messageId = ResolveResponseMessageId(code, responseText, responseLine, tokens);

                return code switch
                {
                    239 => new TransitPublishResult(messageId, TransitPublishStatus.Accepted, code, responseText, T4ResponseAvailableTick: responseAvailableTick, T5ResponseParsedTick: responseParsedTick),
                    439 => new TransitPublishResult(messageId, TransitPublishStatus.Rejected, code, responseText, T4ResponseAvailableTick: responseAvailableTick, T5ResponseParsedTick: responseParsedTick),
                    431 => new TransitPublishResult(messageId, TransitPublishStatus.Rejected, code, responseText, T4ResponseAvailableTick: responseAvailableTick, T5ResponseParsedTick: responseParsedTick),
                    400 => new TransitPublishResult(messageId, TransitPublishStatus.Ambiguous, code, responseText, T4ResponseAvailableTick: responseAvailableTick, T5ResponseParsedTick: responseParsedTick),
                    _ => null,
                };
            }

            return null;
        }

        private string ResolveResponseMessageId(int code, string responseText, string responseLine, string[] tokens)
        {
            if (tokens.Length > 0)
            {
                string firstToken = tokens[0];
                if (firstToken.Length >= 3 && firstToken[0] == '<' && firstToken[^1] == '>')
                {
                    return firstToken;
                }

                if (!CanCorrelateBySendOrder(code, responseText, tokens))
                {
                    throw new InvalidOperationException($"TAKETHIS response contains malformed Message-ID token: '{responseLine}'.");
                }
            }

            if (!CanCorrelateBySendOrder(code, responseText, tokens))
            {
                throw new InvalidOperationException($"TAKETHIS response omitted Message-ID token: '{responseLine}'.");
            }

            if (_pendingByMessageId.Count != 1)
            {
                throw new InvalidOperationException($"TAKETHIS response omitted Message-ID token while {_pendingByMessageId.Count} submissions were outstanding; cannot safely correlate without Message-ID.");
            }

            while (_pendingBySendOrder.TryDequeue(out string nextMessageId))
            {
                if (_pendingByMessageId.ContainsKey(nextMessageId))
                {
                    Interlocked.Exchange(ref _tokenlessSuccessModeEnabled, 1);
                    return nextMessageId;
                }
            }

            throw new InvalidOperationException($"TAKETHIS response omitted Message-ID token and no outstanding submission was available for FIFO correlation: '{responseLine}'.");
        }

        private static bool CanCorrelateBySendOrder(int code, string responseText, string[] tokens)
        {
            if (code != 239)
            {
                return false;
            }

            _ = tokens;
            return string.Equals(responseText, "Article transferred OK", StringComparison.OrdinalIgnoreCase);
        }

        private void AcknowledgeSendOrder(string messageId)
        {
            while (_pendingBySendOrder.TryPeek(out string queuedMessageId))
            {
                if (string.Equals(queuedMessageId, messageId, StringComparison.Ordinal))
                {
                    _pendingBySendOrder.TryDequeue(out _);
                    return;
                }

                if (!_pendingByMessageId.ContainsKey(queuedMessageId))
                {
                    _pendingBySendOrder.TryDequeue(out _);
                    continue;
                }

                return;
            }
        }

        private int StageTakethisFrame(PipeWriter writer, string messageId, ReadOnlyMemory<byte> articlePayload)
        {
            byte[] commandPrefix = Encoding.ASCII.GetBytes("TAKETHIS " + messageId);

            WriteBytes(writer, commandPrefix);
            WriteBytes(writer, CrLfBytes);

            int payloadBytesWritten = WriteDotStuffedArticle(writer, articlePayload);
            WriteBytes(writer, DotTerminatorBytes);

            return commandPrefix.Length + CrLfBytes.Length + payloadBytesWritten + DotTerminatorBytes.Length;
        }

        private static int WriteDotStuffedArticle(PipeWriter writer, ReadOnlyMemory<byte> payload)
        {
            ReadOnlySpan<byte> span = payload.Span;
            bool atLineStart = true;
            int bytesWritten = 0;

            for (int i = 0; i < span.Length; i++)
            {
                byte current = span[i];

                if (atLineStart && current == (byte)'.')
                {
                    Span<byte> stuffedDotDestination = writer.GetSpan(1);
                    stuffedDotDestination[0] = (byte)'.';
                    writer.Advance(1);
                    bytesWritten++;
                }

                Span<byte> destination = writer.GetSpan(1);
                destination[0] = current;
                writer.Advance(1);
                bytesWritten++;

                atLineStart = current == (byte)'\n';
            }

            if (span.Length > 0 && span[^1] != (byte)'\n')
            {
                WriteBytes(writer, CrLfBytes);
                bytesWritten += CrLfBytes.Length;
            }

            return bytesWritten;
        }

        private static void WriteBytes(PipeWriter writer, ReadOnlySpan<byte> bytes)
        {
            Span<byte> destination = writer.GetSpan(bytes.Length);
            bytes.CopyTo(destination);
            writer.Advance(bytes.Length);
        }

        private void FailOutstandingAsAmbiguous(string reason)
        {
            int completedAsAmbiguous = 0;
            foreach ((string messageId, PendingPublishOperation pending) in _pendingByMessageId)
            {
                _ = _pendingByMessageId.TryRemove(messageId, out _);
                pending.Completion.TrySetResult(new TransitPublishResult(
                    MessageId: messageId,
                    Status: TransitPublishStatus.Ambiguous,
                    ResponseCode: null,
                    ResponseText: reason));
                Interlocked.Increment(ref _submissionsAmbiguous);
                completedAsAmbiguous++;
            }

            if (completedAsAmbiguous > 0)
            {
                _logger.LogInformation("[SHUTDOWN-DIAG] TransitConnection completed outstanding operations as ambiguous count={OutstandingCount} connectionId={ConnectionId}", completedAsAmbiguous, ConnectionId);
            }

            SignalDrainIfCompleted();
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
                case TransitPublishStatus.Ambiguous:
                    Interlocked.Increment(ref _submissionsAmbiguous);
                    break;
                case TransitPublishStatus.Unavailable:
                    Interlocked.Increment(ref _submissionsUnavailable);
                    break;
                case TransitPublishStatus.Failed:
                    Interlocked.Increment(ref _submissionsFailed);
                    break;
            }
        }

        private void TransitionState(TransitConnectionState state)
        {
            _state = state;
            LogTransitStateTransition(_logger, ConnectionId, state);
        }

        internal async ValueTask<TransitPublishResult> SubmitTakethisAsync(
            string messageId,
            ReadOnlyMemory<byte> articlePayload,
            CancellationToken cancellationToken,
            long publishAsyncEnterTick,
            long dispatcherAssignedTick)
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

            if (Volatile.Read(ref _shutdownRequested) == 1
                || (_state != TransitConnectionState.Ready && _state != TransitConnectionState.Publishing)
                || !_streamingModeNegotiated)
            {
                Interlocked.Increment(ref _submissionsUnavailable);
                return new TransitPublishResult(
                    MessageId: messageId,
                    Status: TransitPublishStatus.Unavailable,
                    ResponseCode: null,
                    ResponseText: "Transit connection is not ready for publishing.",
                    T0PublishAsyncEnterTick: publishAsyncEnterTick,
                    T1DispatcherAssignedTick: dispatcherAssignedTick);
            }

            bool tokenlessModeEnabled = Volatile.Read(ref _tokenlessSuccessModeEnabled) == 1;
            if (tokenlessModeEnabled)
            {
                await _tokenlessCorrelationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            try
            {
                PendingPublishOperation operation = new(messageId, publishAsyncEnterTick, dispatcherAssignedTick);
                operation.T0SubmitTakethisEnterTick = Stopwatch.GetTimestamp();
                _logger.LogInformation("[SUBMIT-PATH] stage=submit-takethis-entry messageId={MessageId} tick={Tick}", messageId, operation.T0SubmitTakethisEnterTick);

                if (!_pendingByMessageId.TryAdd(messageId, operation))
                {
                    Interlocked.Increment(ref _submissionsFailed);
                    return new TransitPublishResult(
                        MessageId: messageId,
                        Status: TransitPublishStatus.Failed,
                        ResponseCode: null,
                        ResponseText: "Duplicate in-flight Message-ID on same connection.",
                        T0PublishAsyncEnterTick: publishAsyncEnterTick,
                        T1DispatcherAssignedTick: dispatcherAssignedTick);
                }

                operation.T1PendingRegisteredTick = Stopwatch.GetTimestamp();
                _logger.LogInformation("[SUBMIT-PATH] stage=pending-registered messageId={MessageId} tick={Tick}", messageId, operation.T1PendingRegisteredTick);
                operation.PendingDepthAtT1 = CapturePendingDepthAndTrackMax();
                EnsureDiagnosticOperationTracked(operation);

                Interlocked.Increment(ref _submissionsStarted);
                ObserveMaxConcurrentSubmissions(_pendingByMessageId.Count);
                _pendingBySendOrder.Enqueue(messageId);

                try
                {
                    Channel<WriteIntent>? writeIntentChannel = _writeIntentChannel;
                    if (writeIntentChannel is null)
                    {
                        _pendingByMessageId.TryRemove(messageId, out _);
                        Interlocked.Increment(ref _submissionsUnavailable);
                        return new TransitPublishResult(
                            MessageId: messageId,
                            Status: TransitPublishStatus.Unavailable,
                            ResponseCode: null,
                            ResponseText: "Transit write channel is not available.",
                            T0PublishAsyncEnterTick: publishAsyncEnterTick,
                            T1DispatcherAssignedTick: dispatcherAssignedTick);
                    }

                    byte[] retainedPayload = articlePayload.ToArray();
                    WriteIntent intent = new(messageId, retainedPayload, operation);
                    operation.T2WriteIntentEnqueueStartTick = Stopwatch.GetTimestamp();
                    await writeIntentChannel.Writer.WriteAsync(intent, cancellationToken).ConfigureAwait(false);

                    operation.T2WriteIntentEnqueuedTick = Stopwatch.GetTimestamp();
                    _logger.LogInformation("[SUBMIT-PATH] stage=write-intent-enqueued messageId={MessageId} tick={Tick}", messageId, operation.T2WriteIntentEnqueuedTick);
                    operation.QueueDepthAtT2 = IncrementDiagnosticQueueDepthOnEnqueue();
                    operation.PendingDepthAtT2 = CapturePendingDepthAndTrackMax();
                    operation.T2BeforeCompletionAwaitTick = Stopwatch.GetTimestamp();
                    UpdateDiagnosticOperation(operation);

                    TransitPublishResult result = await operation.Completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                    RecordSubmissionCompletion(operation);
                    if (result.T2SocketWriteBeginTick == 0 || result.T3SocketWriteEndTick == 0)
                    {
                        result = result with
                        {
                            T2SocketWriteBeginTick = result.T2SocketWriteBeginTick == 0 ? operation.T2SocketWriteBeginTick : result.T2SocketWriteBeginTick,
                            T3SocketWriteEndTick = result.T3SocketWriteEndTick == 0 ? operation.T3SocketWriteEndTick : result.T3SocketWriteEndTick,
                        };
                    }

                    RecordSubmissionResult(result.Status);
                    return result;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    if (operation.Completion.Task.IsCompletedSuccessfully)
                    {
                        TransitPublishResult completedResult = operation.Completion.Task.Result;
                        RecordSubmissionCompletion(operation);
                        RecordSubmissionResult(completedResult.Status);
                        return completedResult;
                    }

                    if (operation.T2SocketWriteBeginTick == 0 && _pendingByMessageId.TryRemove(messageId, out PendingPublishOperation? pending))
                    {
                        pending.Completion.TrySetResult(new TransitPublishResult(
                            MessageId: messageId,
                            Status: TransitPublishStatus.Canceled,
                            ResponseCode: null,
                            ResponseText: "Transit publisher canceled."));
                        _logger.LogInformation("[SHUTDOWN-DIAG] TransitConnection canceled pending operation before write begin connectionId={ConnectionId} messageId={MessageId}", ConnectionId, messageId);
                    }

                    RecordSubmissionCompletion(operation);
                    _logger.LogInformation("[SHUTDOWN-DIAG] TransitConnection SubmitTakethisAsync cancellation path connectionId={ConnectionId} messageId={MessageId} writeIntentEnqueued={WriteIntentEnqueued} writeBeginTick={WriteBeginTick} responseCorrelatedTick={ResponseCorrelatedTick}", ConnectionId, messageId, operation.T2WriteIntentEnqueuedTick > 0, operation.T2SocketWriteBeginTick, operation.T9ResponseCorrelatedTick);
                    return new TransitPublishResult(
                        MessageId: messageId,
                        Status: TransitPublishStatus.Canceled,
                        ResponseCode: null,
                        ResponseText: "Transit publisher canceled.");
                }
                catch
                {
                    if (_pendingByMessageId.TryRemove(messageId, out PendingPublishOperation? pending))
                    {
                        pending.Completion.TrySetResult(new TransitPublishResult(
                            MessageId: messageId,
                            Status: TransitPublishStatus.Ambiguous,
                            ResponseCode: null,
                            ResponseText: "Connection failed before definitive TAKETHIS responses were received.",
                            T0PublishAsyncEnterTick: pending.T0PublishAsyncEnterTick,
                            T1DispatcherAssignedTick: pending.T1DispatcherAssignedTick,
                            T2SocketWriteBeginTick: pending.T2SocketWriteBeginTick,
                            T3SocketWriteEndTick: pending.T3SocketWriteEndTick));
                        Interlocked.Increment(ref _submissionsAmbiguous);
                    }

                    RecordSubmissionCompletion(operation);
                    throw;
                }
            }
            finally
            {
                if (tokenlessModeEnabled)
                {
                    _tokenlessCorrelationGate.Release();
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _shutdownRequested, 1) == 1)
            {
                return;
            }

            TransitConnectionState stateBeforeShutdown = _state;
            TransitionState(TransitConnectionState.Disconnecting);

            TransitConnectionDiagnosticsSnapshot preDisposeSnapshot = CaptureDiagnosticsSnapshot();
            _logger.LogInformation("[SHUTDOWN-DIAG] TransitConnection.DisposeAsync start connectionId={ConnectionId} pendingMessageIds={PendingMessageIds} writeQueueDepth={WriteQueueDepth} outstandingOps={OutstandingOps}", ConnectionId, preDisposeSnapshot.CurrentConcurrentSubmissions, preDisposeSnapshot.CurrentWriteIntentQueueDepth, preDisposeSnapshot.OutstandingOperations.Length);

            try
            {
                await StopWriteLoopAsync(requestCancellation: false, drainQueuedWriteIntentsAsAmbiguous: false).ConfigureAwait(false);
                await DrainPendingTakethisAsync().ConfigureAwait(false);

                if (stateBeforeShutdown is not TransitConnectionState.Faulted and not TransitConnectionState.Disconnected)
                {
                    await SendQuitAsync().ConfigureAwait(false);
                }
                else
                {
                    _logger.LogInformation("[SHUTDOWN-DIAG] TransitConnection skipping QUIT because pre-shutdown state was {State} connectionId={ConnectionId}", stateBeforeShutdown, ConnectionId);
                }

                if (_responseLoopCancellation is not null)
                {
                    try
                    {
                        _responseLoopCancellation.Cancel();
                        _logger.LogInformation("[SHUTDOWN-DIAG] TransitConnection response loop cancellation requested connectionId={ConnectionId}", ConnectionId);
                    }
                    catch (ObjectDisposedException)
                    {
                    }
                }

                if (_responseLoopTask is not null)
                {
                    try
                    {
                        await _responseLoopTask.ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                    }

                    _logger.LogInformation("[SHUTDOWN-DIAG] TransitConnection response loop terminated connectionId={ConnectionId}", ConnectionId);
                    _responseLoopTask = null;
                }
            }
            finally
            {
                _shutdownDrainCompletion = null;
            }

            if (_writer is not null)
            {
                await _writer.CompleteAsync().ConfigureAwait(false);
                _writer = null;
            }

            if (_reader is not null)
            {
                await _reader.CompleteAsync().ConfigureAwait(false);
                _reader = null;
            }

            DisposeTransportArtifacts();
            _logger.LogInformation("[SHUTDOWN-DIAG] TransitConnection transport disposed connectionId={ConnectionId}", ConnectionId);
            FailOutstandingAsAmbiguous("Connection closed before definitive TAKETHIS responses were received.");
            _logger.LogInformation("[SHUTDOWN-DIAG] TransitConnection outstanding operations completed as ambiguous connectionId={ConnectionId}", ConnectionId);

            if (_responseLoopCancellation is not null)
            {
                _responseLoopCancellation.Dispose();
                _responseLoopCancellation = null;
            }

            TransitConnectionDiagnosticsSnapshot postDisposeSnapshot = CaptureDiagnosticsSnapshot();
            _logger.LogInformation("[SHUTDOWN-DIAG] TransitConnection.DisposeAsync complete connectionId={ConnectionId} pendingMessageIds={PendingMessageIds} writeQueueDepth={WriteQueueDepth} outstandingOps={OutstandingOps}", ConnectionId, postDisposeSnapshot.CurrentConcurrentSubmissions, postDisposeSnapshot.CurrentWriteIntentQueueDepth, postDisposeSnapshot.OutstandingOperations.Length);
            TransitionState(TransitConnectionState.Disconnected);
        }

        private void DisposeTransportArtifacts()
        {
            try
            {
                _readStream?.Dispose();
            }
            catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
            {
            }

            try
            {
                _writeStream?.Dispose();
            }
            catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
            {
            }

            try
            {
                _transportStream?.Dispose();
            }
            catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
            {
            }

            _tcpClient?.Dispose();

            _readStream = null;
            _writeStream = null;
            _transportStream = null;
            _tcpClient = null;
        }

        internal sealed record TransitConnectionDiagnosticsSnapshot(
            string ConnectionId,
            string Host,
            int Port,
            TransitConnectionState CurrentState,
            bool IsTlsActive,
            bool IsCompressionActive,
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

        [LoggerMessage(EventId = 2210, Level = LogLevel.Debug, Message = "Transit connection {ConnectionId} state changed to {State}")]
        private static partial void LogTransitStateTransition(ILogger logger, string connectionId, TransitConnectionState state);

        [LoggerMessage(EventId = 2211, Level = LogLevel.Information, Message = "Transit connection {ConnectionId} capabilities: STARTTLS={SupportsStartTls}, COMPRESS DEFLATE={SupportsCompressDeflate}, STREAMING={SupportsStreaming}")]
        private static partial void LogTransitCapabilities(ILogger logger, string connectionId, bool supportsStartTls, bool supportsCompressDeflate, bool supportsStreaming);

        [LoggerMessage(EventId = 2212, Level = LogLevel.Information, Message = "Transit connection {ConnectionId} is ready (TLS={TlsActive}, Compression={CompressionActive})")]
        private static partial void LogTransitConnectionReady(ILogger logger, string connectionId, bool tlsActive, bool compressionActive);

        [LoggerMessage(EventId = 2213, Level = LogLevel.Warning, Message = "Transit connection {ConnectionId} response loop faulted")]
        private static partial void LogTransitResponseLoopFaulted(ILogger logger, Exception exception, string connectionId);

        [LoggerMessage(EventId = 2214, Level = LogLevel.Warning, Message = "Transit server COMPRESS DEFLATE interoperability failure detected for {Host}:{Port} on connection {ConnectionId}; RFC-compliant compression will be disabled for this connection and the client will reconnect without compression.")]
        private static partial void LogTransitCompressionInteroperabilityFallback(ILogger logger, string connectionId, string host, int port);

        private readonly record struct WriteIntent(
            string MessageId,
            ReadOnlyMemory<byte> ArticlePayload,
            PendingPublishOperation Operation);

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

        private sealed class PendingPublishOperation
        {
            internal PendingPublishOperation(string messageId, long publishAsyncEnterTick, long dispatcherAssignedTick)
            {
                MessageId = messageId;
                T0PublishAsyncEnterTick = publishAsyncEnterTick;
                T1DispatcherAssignedTick = dispatcherAssignedTick;
            }

            internal string MessageId { get; }

            internal long T0PublishAsyncEnterTick { get; }

            internal long T1DispatcherAssignedTick { get; }

            internal long T0SubmitTakethisEnterTick;

            internal long T1PendingRegisteredTick;

            internal long T2WriteIntentEnqueueStartTick;

            internal long T2WriteIntentEnqueuedTick;

            internal long T2BeforeCompletionAwaitTick;

            internal long T3WriterDequeuedTick;

            internal long T4AssignedToBatchTick;

            internal long T5FrameStageBeginTick;

            internal long T6FrameStageEndTick;

            internal long T7BatchFlushBeginTick;

            internal long T8BatchFlushEndTick;

            internal long T9ResponseCorrelatedTick;

            internal long T10SubmitCompletionTick;

            internal long PendingDepthAtT1;

            internal long PendingDepthAtT2;

            internal long PendingDepthAtT3;

            internal long PendingDepthAtT4;

            internal long PendingDepthAtT9;

            internal long QueueDepthAtT2;

            internal long QueueDepthAtT3;

            internal long QueueDepthAtBatchStart;

            internal int BatchDequeuedCount;

            internal long QueueDepthAtT9;

            internal long BatchId;

            internal int BatchPosition;

            internal int BatchSize;

            internal long SendSequence;

            internal long LogicalOutstandingAheadAtResponse;

            internal long T2SocketWriteBeginTick;

            internal long T3SocketWriteEndTick;

            internal TaskCompletionSource<TransitPublishResult> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }
}
