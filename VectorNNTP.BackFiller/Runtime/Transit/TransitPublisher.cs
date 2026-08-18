using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Sockets;
using System.Reflection;
using System.Threading.Channels;
using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Runtime.Transit
{
    /// <summary>
    /// Outbound NNTP Transit publisher responsible for connection initialization and publish submission intake.
    /// </summary>
    internal sealed partial class TransitPublisher : IAsyncDisposable
    {
        private const int DefaultPerConnectionPipelineDepth = 8;

        private static readonly TimeSpan StatsInterval = TimeSpan.FromSeconds(60);
        private static readonly Action<ILogger, Exception?> LogPublishCancellationContinuationFailure =
            LoggerMessage.Define(
                LogLevel.Warning,
                new EventId(2209, "PublishCancellationContinuationFailed"),
                "Transit publisher cancellation continuation failed while logging delayed publish outcome");

        private readonly BackFillerRuntimeOptions _runtimeOptions;
        private readonly TimeProvider _timeProvider;
        private readonly ILogger<TransitPublisher> _logger;

        private readonly Channel<PendingSubmission> _submissionChannel;
        private readonly ConnectionSlot[] _connectionSlots;
        private readonly ConcurrentBag<ConnectionRecord> _connectionHistory = [];
        private readonly int _connectionPoolSize;
        private readonly int _perConnectionPipelineDepth;
        private readonly object _submissionTraceGate = new();
        private readonly List<SubmissionTraceRecord> _submissionTraceRecords = [];
        private readonly List<PublishToConnectionTraceRecord> _publishToConnectionTraceRecords = [];
        private const int SubmissionTraceRecordLimit = 500_000;
        private CancellationTokenSource? _submissionWorkersCancellation;
        private Task[]? _submissionWorkers;
        private CancellationTokenSource? _statsLoopCancellation;
        private Task? _statsLoop;

        private long _totalBytesTransmitted;
        private long _totalBytesReceived;
        private long _totalArticlesSubmitted;
        private long _totalArticlesAccepted;
        private long _totalArticlesRejected;
        private long _totalArticlesAmbiguous;
        private long _totalReconnects;
        private long _queuedSubmissionCount;
        private long _nextSubmissionId;
        private long _submissionPumpFaultCount;
        private long _submissionPumpInitiatingFaultCount;
        private long _submissionPumpCascadeFaultCount;
        private long _submissionPumpFaultSequence;
        private long _measurementStartStopwatchTick;
        private long _measurementEndStopwatchTick;
        private long _measurementBoundaryObserved;

        private readonly ConcurrentDictionary<long, PendingSubmission> _activeSubmissions = new();

        private volatile bool _disposeRequested;
        private volatile TransitConnectionState _state = TransitConnectionState.Disconnected;
        private volatile ProducerCompletionState _producerCompletionState;
        private volatile DispatchersCompletedState _dispatchersCompletedState;

        private PumpFaultTelemetrySnapshot? _firstSubmissionPumpFault;

        public TransitPublisher(
            BackFillerRuntimeOptions runtimeOptions,
            TimeProvider timeProvider,
            ILogger<TransitPublisher> logger,
            int connectionPoolSize = 1,
            int perConnectionPipelineDepth = DefaultPerConnectionPipelineDepth)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);
            ArgumentNullException.ThrowIfNull(timeProvider);
            ArgumentNullException.ThrowIfNull(logger);

            if (connectionPoolSize <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(connectionPoolSize), connectionPoolSize, "Connection pool size must be greater than zero.");
            }

            if (perConnectionPipelineDepth <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(perConnectionPipelineDepth), perConnectionPipelineDepth, "Per-connection pipeline depth must be greater than zero.");
            }

            _runtimeOptions = runtimeOptions;
            _timeProvider = timeProvider;
            _logger = logger;
            _connectionPoolSize = connectionPoolSize;
            _perConnectionPipelineDepth = perConnectionPipelineDepth;

            _connectionSlots = new ConnectionSlot[_connectionPoolSize];
            for (int i = 0; i < _connectionSlots.Length; i++)
            {
                _connectionSlots[i] = new ConnectionSlot(i);
            }

            _submissionChannel = Channel.CreateBounded<PendingSubmission>(new BoundedChannelOptions(2048)
            {
                SingleReader = false,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait,
            });
        }

        internal TransitConnectionState CurrentState => _state;

        internal TransitPublisherConnectionDiagnosticsSnapshot CaptureConnectionDiagnosticsSnapshot()
        {
            SubmissionTraceRecord[] submissionTraceRecords;
            PublishToConnectionTraceRecord[] publishToConnectionTraceRecords;
            lock (_submissionTraceGate)
            {
                submissionTraceRecords = _submissionTraceRecords.ToArray();
                publishToConnectionTraceRecords = _publishToConnectionTraceRecords.ToArray();
            }

            ConnectionSlotSnapshot[] slotSnapshots = new ConnectionSlotSnapshot[_connectionSlots.Length];
            for (int i = 0; i < _connectionSlots.Length; i++)
            {
                ConnectionSlot slot = _connectionSlots[i];
                TransitConnection? currentConnection = slot.Connection;
                slotSnapshots[i] = new ConnectionSlotSnapshot(
                    SlotIndex: slot.SlotIndex,
                    HasCurrentConnection: currentConnection is not null,
                    CurrentConnectionId: currentConnection?.ConnectionId,
                    TotalSubmissionsRouted: Interlocked.Read(ref slot.TotalSubmissionsRouted),
                    Reconnects: Interlocked.Read(ref slot.Reconnects),
                    CreatedConnections: Interlocked.Read(ref slot.CreatedConnections),
                    MaxObservedInFlightDepth: (int)Interlocked.Read(ref slot.MaxObservedInFlightDepth),
                    WaitedForChannelReadabilityCount: Interlocked.Read(ref slot.WaitedForChannelReadabilityCount),
                    WaitedForCompletionWhilePipelineFullCount: Interlocked.Read(ref slot.WaitedForCompletionWhilePipelineFullCount),
                    FirstReachedConfiguredDepthTick: Interlocked.Read(ref slot.FirstReachedConfiguredDepthTick));
            }

            List<ConnectionDiagnosticsEntry> connections = [];
            foreach (ConnectionRecord record in _connectionHistory)
            {
                connections.Add(new ConnectionDiagnosticsEntry(
                    SlotIndex: record.SlotIndex,
                    Snapshot: record.Connection.CaptureDiagnosticsSnapshot()));
            }

            return new TransitPublisherConnectionDiagnosticsSnapshot(
                ConfiguredConnectionPoolSize: _connectionPoolSize,
                ConfiguredPerConnectionPipelineDepth: _perConnectionPipelineDepth,
                TotalReconnects: Interlocked.Read(ref _totalReconnects),
                QueuedSubmissionCount: Interlocked.Read(ref _queuedSubmissionCount),
                SubmissionTraceRecords: submissionTraceRecords,
                PublishToConnectionTraceRecords: publishToConnectionTraceRecords,
                Slots: slotSnapshots,
                Connections: connections.ToArray());
        }

        internal PumpFaultTelemetrySnapshot? CaptureSubmissionPumpFaultTelemetrySnapshot()
        {
            return Volatile.Read(ref _firstSubmissionPumpFault);
        }

        internal SubmissionPumpFaultCounts CaptureSubmissionPumpFaultCounts()
        {
            return new SubmissionPumpFaultCounts(
                TotalFaultCount: Interlocked.Read(ref _submissionPumpFaultCount),
                InitiatingFaultCount: Interlocked.Read(ref _submissionPumpInitiatingFaultCount),
                CascadeFaultCount: Interlocked.Read(ref _submissionPumpCascadeFaultCount));
        }

        internal TransitConnection.P1GreetingProvenanceSnapshot? CaptureFirstP1GreetingProvenanceSnapshot()
        {
            foreach (ConnectionRecord record in _connectionHistory)
            {
                TransitConnection.P1GreetingProvenanceSnapshot? snapshot = record.Connection.CaptureFirstP1GreetingProvenanceSnapshot();
                if (snapshot is not null)
                {
                    return snapshot;
                }
            }

            return null;
        }

        /// <summary>
        /// Marks submission-pump telemetry measurement window boundaries.
        /// </summary>
        /// <param name="measurementStartStopwatchTick">Measurement start stopwatch tick.</param>
        /// <param name="measurementEndStopwatchTick">Measurement end stopwatch tick when known; otherwise zero.</param>
        /// <param name="measurementBoundaryObserved">True when measurement end boundary has been observed.</param>
        internal void MarkSubmissionPumpFaultMeasurementWindow(long measurementStartStopwatchTick, long measurementEndStopwatchTick, bool measurementBoundaryObserved)
        {
            if (measurementStartStopwatchTick > 0)
            {
                Interlocked.Exchange(ref _measurementStartStopwatchTick, measurementStartStopwatchTick);
            }

            if (measurementEndStopwatchTick > 0)
            {
                Interlocked.Exchange(ref _measurementEndStopwatchTick, measurementEndStopwatchTick);
            }

            Interlocked.Exchange(ref _measurementBoundaryObserved, measurementBoundaryObserved ? 1L : 0L);
        }

        /// <summary>
        /// Marks whether all producers have completed for submission-pump fault telemetry.
        /// </summary>
        /// <param name="allProducersCompleted">True when all producers have completed.</param>
        internal void MarkSubmissionPumpFaultProducerCompletion(bool allProducersCompleted)
        {
            _producerCompletionState = allProducersCompleted
                ? ProducerCompletionState.Complete
                : ProducerCompletionState.Incomplete;
        }

        /// <summary>
        /// Marks whether all dispatch workers have completed for submission-pump fault telemetry.
        /// </summary>
        /// <param name="dispatchersCompleted">True when all dispatch workers have completed.</param>
        internal void MarkSubmissionPumpFaultDispatchersCompleted(bool dispatchersCompleted)
        {
            _dispatchersCompletedState = dispatchersCompleted
                ? DispatchersCompletedState.Complete
                : DispatchersCompletedState.Incomplete;
        }

        internal async Task InitializeAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("[INIT-TRACE] TransitPublisher.InitializeAsync START");
            cancellationToken.ThrowIfCancellationRequested();

            if (_disposeRequested)
            {
                throw new OperationCanceledException("Transit publisher initialization canceled because shutdown has already begun.", cancellationToken);
            }

            if (_connectionSlots[0].Connection is not null)
            {
                throw new InvalidOperationException("Transit publisher was already initialized.");
            }

            try
            {
                _logger.LogInformation("[INIT-TRACE] TransitPublisher.InitializeAsync BEFORE EstablishConnectionAsync(slot=0)");
                await EstablishConnectionAsync(slotIndex: 0, incrementReconnectCounter: false, cancellationToken).ConfigureAwait(false);
                _logger.LogInformation("[INIT-TRACE] TransitPublisher.InitializeAsync AFTER EstablishConnectionAsync(slot=0)");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[INIT-TRACE] TransitPublisher.InitializeAsync EstablishConnectionAsync(slot=0) FAILED: {ExceptionType}: {ExceptionMessage}", ex.GetType().FullName, ex.Message);
                throw;
            }

            if (_disposeRequested)
            {
                await DisposeConnectionsAsync().ConfigureAwait(false);
                TransitionState(TransitConnectionState.Disconnected);
                throw new OperationCanceledException("Transit publisher initialization canceled because shutdown began during connection setup.", cancellationToken);
            }

            _submissionWorkersCancellation = new CancellationTokenSource();
            _submissionWorkers = new Task[_connectionPoolSize];

            for (int workerIndex = 0; workerIndex < _connectionPoolSize; workerIndex++)
            {
                int slotIndex = workerIndex;
                _submissionWorkers[workerIndex] = Task.Run(
                    () => RunSubmissionPumpAsync(slotIndex, _submissionWorkersCancellation.Token),
                    CancellationToken.None);
            }

            _statsLoopCancellation = new CancellationTokenSource();
            _statsLoop = Task.Run(
                () => RunStatsLoopAsync(_statsLoopCancellation.Token),
                CancellationToken.None);
            _logger.LogInformation("[INIT-TRACE] TransitPublisher.InitializeAsync COMPLETE");
        }

        internal async ValueTask<TransitPublishResult> PublishAsync(
            string messageId,
            ReadOnlyMemory<byte> articlePayload,
            CancellationToken cancellationToken)
        {
            long publishAsyncEnterTick = Stopwatch.GetTimestamp();

            if (string.IsNullOrWhiteSpace(messageId))
            {
                throw new ArgumentException("Message-ID is required.", nameof(messageId));
            }

            if (articlePayload.IsEmpty)
            {
                throw new ArgumentException("Article payload must not be empty.", nameof(articlePayload));
            }

            if (articlePayload.Span[^1] != (byte)'\n')
            {
                throw new ArgumentException("Article payload must end with LF to preserve byte integrity during TAKETHIS framing.", nameof(articlePayload));
            }

            if (_disposeRequested || _submissionWorkers is null || _submissionWorkersCancellation is null)
            {
                LogArticleSubmissionUnavailable(_logger, messageId, _state);
                return new TransitPublishResult(
                    MessageId: messageId,
                    Status: TransitPublishStatus.Unavailable,
                    ResponseCode: null,
                    ResponseText: "Transit connection unavailable.",
                    T0PublishAsyncEnterTick: publishAsyncEnterTick,
                    T7PublishAsyncCompleteTick: Stopwatch.GetTimestamp(),
                    Provenance: TransitPublishProvenance.Unavailable,
                    ProvenanceConnectionState: _state,
                    ProvenanceTick: Stopwatch.GetTimestamp());
            }

            TaskCompletionSource<TransitPublishResult> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            PendingSubmission submission = new(
                submissionId: Interlocked.Increment(ref _nextSubmissionId),
                messageId: messageId,
                articlePayload: articlePayload,
                completion: completion,
                publishAsyncEnterTick: publishAsyncEnterTick);

            _activeSubmissions[submission.SubmissionId] = submission;
            Interlocked.Increment(ref _queuedSubmissionCount);

            long submissionChannelWriteStartTick = Stopwatch.GetTimestamp();
            long submissionChannelWriteEndTick;

            try
            {
                await _submissionChannel.Writer.WriteAsync(submission, cancellationToken).ConfigureAwait(false);
                submissionChannelWriteEndTick = Stopwatch.GetTimestamp();
                _logger.LogInformation("[SUBMIT-PATH] stage=submission-channel-write messageId={MessageId} writeStartTick={WriteStartTick} writeEndTick={WriteEndTick}", messageId, submissionChannelWriteStartTick, submissionChannelWriteEndTick);
                Interlocked.Increment(ref _totalArticlesSubmitted);
            }
            catch (ChannelClosedException)
            {
                _ = CompleteSubmissionIfPending(
                    submission,
                    new TransitPublishResult(
                        MessageId: messageId,
                        Status: TransitPublishStatus.Unavailable,
                        ResponseCode: null,
                        ResponseText: "Transit publisher is shutting down.",
                        Provenance: TransitPublishProvenance.Unavailable,
                        ProvenanceConnectionState: _state,
                        ProvenanceTick: Stopwatch.GetTimestamp()),
                    countAsAmbiguous: false,
                    allowConnectionOwned: false);

                return new TransitPublishResult(
                    MessageId: messageId,
                    Status: TransitPublishStatus.Unavailable,
                    ResponseCode: null,
                    ResponseText: "Transit publisher is shutting down.",
                    T0PublishAsyncEnterTick: publishAsyncEnterTick,
                    T7PublishAsyncCompleteTick: Stopwatch.GetTimestamp(),
                    Provenance: TransitPublishProvenance.Unavailable,
                    ProvenanceConnectionState: _state,
                    ProvenanceTick: Stopwatch.GetTimestamp());
            }
            catch (OperationCanceledException)
            {
                _ = CompleteSubmissionIfPending(
                    submission,
                    new TransitPublishResult(
                        MessageId: messageId,
                        Status: TransitPublishStatus.Canceled,
                        ResponseCode: null,
                        ResponseText: "Transit publisher admission canceled.",
                        Provenance: TransitPublishProvenance.Cancellation,
                        ProvenanceConnectionState: _state,
                        ProvenanceTick: Stopwatch.GetTimestamp()),
                    countAsAmbiguous: false,
                    allowConnectionOwned: false);

                throw;
            }

            LogArticleSubmissionQueued(_logger, messageId);

            Task<TransitPublishResult> completionTask = completion.Task;

            if (!cancellationToken.CanBeCanceled)
            {
                TransitPublishResult result = await completionTask.ConfigureAwait(false);
                result = result with { T7PublishAsyncCompleteTick = Stopwatch.GetTimestamp() };
                LogArticleSubmissionOutcome(_logger, result.MessageId, result.Status, result.ResponseCode, result.ResponseText);
                return result;
            }

            try
            {
                TransitPublishResult result = await completionTask.WaitAsync(cancellationToken).ConfigureAwait(false);
                result = result with { T7PublishAsyncCompleteTick = Stopwatch.GetTimestamp() };
                LogArticleSubmissionOutcome(_logger, result.MessageId, result.Status, result.ResponseCode, result.ResponseText);
                return result;
            }
            catch (OperationCanceledException)
            {
                _ = completionTask.ContinueWith(
                    static (task, state) =>
                    {
                        ILogger<TransitPublisher> logger = (ILogger<TransitPublisher>)state!;

                        try
                        {
                            if (task.IsCompletedSuccessfully)
                            {
                                TransitPublishResult result = task.Result;
                                LogArticleSubmissionOutcome(logger, result.MessageId, result.Status, result.ResponseCode, result.ResponseText);
                            }
                        }
                        catch (Exception ex)
                        {
                            LogPublishCancellationContinuationFailure(logger, ex);
                        }
                    },
                    _logger,
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);

                throw;
            }
        }

        internal TransitTransportSnapshot CaptureTransportSnapshot(int activeConnections, int outstandingSubmissions)
        {
            long transmitted = 0;
            long received = 0;

            for (int i = 0; i < _connectionSlots.Length; i++)
            {
                TransitConnection? connection = _connectionSlots[i].Connection;
                if (connection is null)
                {
                    continue;
                }

                transmitted += connection.BytesTransmitted;
                received += connection.BytesReceived;
            }

            Interlocked.Exchange(ref _totalBytesTransmitted, transmitted);
            Interlocked.Exchange(ref _totalBytesReceived, received);

            return new TransitTransportSnapshot(
                TotalBytesTransmitted: transmitted,
                TotalBytesReceived: received,
                TotalArticlesSubmitted: Interlocked.Read(ref _totalArticlesSubmitted),
                TotalArticlesAccepted: Interlocked.Read(ref _totalArticlesAccepted),
                TotalArticlesRejected: Interlocked.Read(ref _totalArticlesRejected),
                TotalArticlesAmbiguous: Interlocked.Read(ref _totalArticlesAmbiguous),
                TotalReconnects: Interlocked.Read(ref _totalReconnects),
                ActiveConnections: activeConnections,
                OutstandingSubmissions: outstandingSubmissions);
        }

        private async Task RunSubmissionPumpAsync(int slotIndex, CancellationToken cancellationToken)
        {
            List<InFlightSubmission> inFlight = new(_perConnectionPipelineDepth);

            try
            {
                ChannelReader<PendingSubmission> reader = _submissionChannel.Reader;

                while (true)
                {
                    while (inFlight.Count < _perConnectionPipelineDepth && reader.TryRead(out PendingSubmission? submission))
                    {
                        if (!submission.TryMarkInFlight())
                        {
                            continue;
                        }

                        long removedFromSubmissionChannelTick = Stopwatch.GetTimestamp();
                        _logger.LogInformation("[SUBMIT-PATH] stage=pump-read messageId={MessageId} tick={Tick}", submission.MessageId, removedFromSubmissionChannelTick);
                        int currentInFlightBeforeAdd = inFlight.Count;
                        int writeIntentQueueDepthAtPumpRead = _connectionSlots[slotIndex].Connection?.CurrentWriteIntentQueueDepth is long depth ? (int)depth : -1;
                        long publishInvocationTick = Stopwatch.GetTimestamp();
                        long dispatcherAssignedTick = publishInvocationTick;
                        Task<TransitPublishResult> publishTask = PublishToConnectionWithReconnectAsync(slotIndex, submission, dispatcherAssignedTick, cancellationToken).AsTask();
                        InFlightSubmission inFlightSubmission = new(submission, publishTask);
                        inFlight.Add(inFlightSubmission);
                        int currentInFlightAfterAdd = inFlight.Count;
                        ObserveSubmissionPumpDepth(slotIndex, currentInFlightAfterAdd);
                        RecordSubmissionTrace(new SubmissionTraceRecord(
                            MessageId: submission.MessageId,
                            RemovedFromSubmissionChannelTick: removedFromSubmissionChannelTick,
                            PublishToConnectionInvokedTick: publishInvocationTick,
                            InFlightCountBeforeAdd: currentInFlightBeforeAdd,
                            InFlightCountAfterAdd: currentInFlightAfterAdd,
                            WriteIntentQueueDepthAtPumpRead: writeIntentQueueDepthAtPumpRead));
                    }

                    if (inFlight.Count == 0)
                    {
                        bool hasMore = await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false);
                        if (!hasMore)
                        {
                            break;
                        }

                        continue;
                    }

                    bool belowPipelineDepth = inFlight.Count < _perConnectionPipelineDepth;
                    bool submissionReadable = false;
                    PendingSubmission? pendingReadableSubmission = null;
                    if (belowPipelineDepth)
                    {
                        submissionReadable = reader.TryRead(out pendingReadableSubmission);
                    }

                    if (belowPipelineDepth && !submissionReadable)
                    {
                        Interlocked.Increment(ref _connectionSlots[slotIndex].WaitedForChannelReadabilityCount);
                        Task<bool> readabilityTask = reader.WaitToReadAsync(cancellationToken).AsTask();
                        Task[] pendingTasks = new Task[inFlight.Count + 1];
                        pendingTasks[0] = readabilityTask;
                        for (int i = 0; i < inFlight.Count; i++)
                        {
                            pendingTasks[i + 1] = inFlight[i].PublishTask;
                        }

                        Task completedTask = await Task.WhenAny(pendingTasks).ConfigureAwait(false);
                        if (ReferenceEquals(completedTask, readabilityTask))
                        {
                            bool hasMore = await readabilityTask.ConfigureAwait(false);
                            if (!hasMore && inFlight.Count == 0)
                            {
                                break;
                            }

                            continue;
                        }
                    }
                    else if (belowPipelineDepth && submissionReadable)
                    {
                        PendingSubmission submission = pendingReadableSubmission!;
                        if (!submission.TryMarkInFlight())
                        {
                            continue;
                        }

                        long removedFromSubmissionChannelTick = Stopwatch.GetTimestamp();
                        _logger.LogInformation("[SUBMIT-PATH] stage=pump-read messageId={MessageId} tick={Tick}", submission.MessageId, removedFromSubmissionChannelTick);
                        int currentInFlightBeforeAdd = inFlight.Count;
                        int writeIntentQueueDepthAtPumpRead = _connectionSlots[slotIndex].Connection?.CurrentWriteIntentQueueDepth is long depth ? (int)depth : -1;
                        long publishInvocationTick = Stopwatch.GetTimestamp();
                        long dispatcherAssignedTick = publishInvocationTick;
                        Task<TransitPublishResult> publishTask = PublishToConnectionWithReconnectAsync(slotIndex, submission, dispatcherAssignedTick, cancellationToken).AsTask();
                        InFlightSubmission inFlightSubmission = new(submission, publishTask);
                        inFlight.Add(inFlightSubmission);
                        int currentInFlightAfterAdd = inFlight.Count;
                        ObserveSubmissionPumpDepth(slotIndex, currentInFlightAfterAdd);
                        RecordSubmissionTrace(new SubmissionTraceRecord(
                            MessageId: submission.MessageId,
                            RemovedFromSubmissionChannelTick: removedFromSubmissionChannelTick,
                            PublishToConnectionInvokedTick: publishInvocationTick,
                            InFlightCountBeforeAdd: currentInFlightBeforeAdd,
                            InFlightCountAfterAdd: currentInFlightAfterAdd,
                            WriteIntentQueueDepthAtPumpRead: writeIntentQueueDepthAtPumpRead));
                        continue;
                    }

                    int completedIndex = GetCompletedInFlightIndex(inFlight);
                    if (!belowPipelineDepth)
                    {
                        Interlocked.Increment(ref _connectionSlots[slotIndex].WaitedForCompletionWhilePipelineFullCount);
                    }

                    if (completedIndex < 0)
                    {
                        Task cancellationTask = Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                        Task[] pendingTasks = new Task[inFlight.Count + 1];
                        pendingTasks[0] = cancellationTask;
                        for (int i = 0; i < inFlight.Count; i++)
                        {
                            pendingTasks[i + 1] = inFlight[i].PublishTask;
                        }

                        Task completedTask = await Task.WhenAny(pendingTasks).ConfigureAwait(false);
                        if (ReferenceEquals(completedTask, cancellationTask))
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                        }

                        completedIndex = -1;
                        for (int i = 0; i < inFlight.Count; i++)
                        {
                            if (ReferenceEquals(inFlight[i].PublishTask, completedTask))
                            {
                                completedIndex = i;
                                break;
                            }
                        }

                        if (completedIndex < 0)
                        {
                            throw new InvalidOperationException("Unable to resolve completed transit publish task.");
                        }
                    }

                    InFlightSubmission completed = inFlight[completedIndex];
                    RemoveInFlightAt(inFlight, completedIndex);
                    await CompleteInFlightSubmissionAsync(completed, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation("[SHUTDOWN-DIAG] Submission pump cancellation observed slot={SlotIndex} inFlight={InFlightCount} queuedSubmissions={QueuedSubmissions}", slotIndex, inFlight.Count, Interlocked.Read(ref _queuedSubmissionCount));
                CompleteInFlightSubmissionsForPreemption(inFlight);
                CompletePendingSubmissionsAsCanceled();
                _logger.LogInformation("[SHUTDOWN-DIAG] Submission pump cancellation handling complete slot={SlotIndex} queuedSubmissions={QueuedSubmissions}", slotIndex, Interlocked.Read(ref _queuedSubmissionCount));
            }
            catch (Exception ex)
            {
                int activeConnections = GetActiveConnectionCount();
                int readyConnections = GetReadyConnectionCount();
                int faultedConnections = GetFaultedConnectionCount();
                int reconnectingConnections = GetReconnectingConnectionCount();
                long outstandingConnectionOperations = GetOutstandingConnectionOperationsCount();
                long queuedSubmissionCount = Interlocked.Read(ref _queuedSubmissionCount);
                long activeSubmissionCount = _activeSubmissions.Count;
                int inFlightCount = inFlight.Count;
                bool initiatingFault = _state != TransitConnectionState.Faulted;

                long totalFaultCount = Interlocked.Increment(ref _submissionPumpFaultCount);
                if (initiatingFault)
                {
                    Interlocked.Increment(ref _submissionPumpInitiatingFaultCount);
                }
                else
                {
                    Interlocked.Increment(ref _submissionPumpCascadeFaultCount);
                }

                long faultSequence = Interlocked.Increment(ref _submissionPumpFaultSequence);
                TransitPublisherPumpFaultOrigin originBucket = ClassifySubmissionPumpFaultOrigin(ex);
                Exception baseException = ex.GetBaseException();
                string baseExceptionType = string.Equals(baseException.GetType().FullName, ex.GetType().FullName, StringComparison.Ordinal)
                    ? ex.GetType().FullName ?? ex.GetType().Name
                    : baseException.GetType().FullName ?? baseException.GetType().Name;

                InvalidOperationFingerprintMessageClass invalidOperationMessageClass = ClassifyInvalidOperationFingerprintMessageClass(ex);
                SanitizedFirstFaultMessageClass sanitizedFirstFaultMessageClass = ClassifySanitizedFirstFaultMessage(ex);
                string sanitizedFirstFaultMessage = sanitizedFirstFaultMessageClass.ToString();
                bool captureFullStackForThisFault = initiatingFault && Volatile.Read(ref _firstSubmissionPumpFault) is null;
                string? fullFirstFaultStackTrace = captureFullStackForThisFault
                    ? CaptureExceptionStackTrace(ex)
                    : null;

                string? topStackFrameDeclaringType = null;
                string? topStackFrameMethodName = null;
                TryCaptureTopStackFrameFingerprint(ex, out topStackFrameDeclaringType, out topStackFrameMethodName);

                long capturedAtTick = Stopwatch.GetTimestamp();
                long measurementStartTick = Interlocked.Read(ref _measurementStartStopwatchTick);
                bool measurementBoundaryObserved = Interlocked.Read(ref _measurementBoundaryObserved) == 1;
                long measurementEndTick = measurementBoundaryObserved ? Interlocked.Read(ref _measurementEndStopwatchTick) : 0;
                double? millisecondsFromMeasurementStart = measurementStartTick > 0
                    ? (capturedAtTick - measurementStartTick) * 1000d / Stopwatch.Frequency
                    : null;
                double? millisecondsFromMeasurementEnd = measurementBoundaryObserved && measurementEndTick > 0
                    ? (capturedAtTick - measurementEndTick) * 1000d / Stopwatch.Frequency
                    : null;

                PumpFaultMeasurementState measurementStateAtFault = PumpFaultMeasurementState.Unknown;
                if (measurementBoundaryObserved && measurementEndTick > 0)
                {
                    measurementStateAtFault = capturedAtTick < measurementEndTick
                        ? PumpFaultMeasurementState.BeforeMeasurementEnd
                        : PumpFaultMeasurementState.AfterMeasurementEnd;
                }

                int? channelImmediateAvailableCount = _submissionChannel.Reader.TryPeek(out _) ? 1 : 0;

                PumpFaultTelemetrySnapshot snapshot = new(
                    FaultSequence: faultSequence,
                    IsInitiatingFault: initiatingFault,
                    SlotIndex: slotIndex,
                    CapturedAtTick: capturedAtTick,
                    ExceptionType: ex.GetType().FullName ?? ex.GetType().Name,
                    BaseExceptionType: baseExceptionType,
                    HResult: ex.HResult,
                    InvalidOperationMessageClass: invalidOperationMessageClass,
                    SanitizedFirstFaultMessageClass: sanitizedFirstFaultMessageClass,
                    SanitizedFirstFaultMessage: sanitizedFirstFaultMessage,
                    FullFirstFaultStackTrace: fullFirstFaultStackTrace,
                    TopStackFrameDeclaringType: topStackFrameDeclaringType,
                    TopStackFrameMethodName: topStackFrameMethodName,
                    Origin: originBucket,
                    QueuedSubmissionCount: queuedSubmissionCount,
                    InFlightCount: inFlightCount,
                    ActiveSubmissionCount: activeSubmissionCount,
                    ActiveConnectionCount: activeConnections,
                    ReadyConnectionCount: readyConnections,
                    FaultedConnectionCount: faultedConnections,
                    ReconnectingConnectionCount: reconnectingConnections,
                    OutstandingConnectionOperations: outstandingConnectionOperations,
                    ProducerCompletionState: _producerCompletionState,
                    MeasurementBoundaryObserved: measurementBoundaryObserved,
                    MeasurementStartTick: measurementStartTick,
                    MeasurementEndTick: measurementEndTick,
                    MillisecondsFromMeasurementStart: millisecondsFromMeasurementStart,
                    MillisecondsFromMeasurementEnd: millisecondsFromMeasurementEnd,
                    MeasurementStateAtFault: measurementStateAtFault,
                    DispatchersCompletedState: _dispatchersCompletedState,
                    ChannelImmediateAvailableCount: channelImmediateAvailableCount);

                if (initiatingFault)
                {
                    Interlocked.CompareExchange(ref _firstSubmissionPumpFault, snapshot, null);
                }

                _logger.LogWarning(ex, "[SHUTDOWN-DIAG] Submission pump faulted slot={SlotIndex} inFlight={InFlightCount} queuedSubmissions={QueuedSubmissions}", slotIndex, inFlightCount, queuedSubmissionCount);
                CompleteInFlightSubmissionsAsAmbiguous(inFlight, "Transit submission pump faulted before definitive TAKETHIS responses were received.");
                TransitionState(TransitConnectionState.Faulted);
                LogTransitSubmissionPumpFaulted(_logger, ex);
                CompletePendingSubmissionsAsAmbiguous("Transit submission pump faulted before definitive TAKETHIS responses were received.");
                _logger.LogInformation("[SHUTDOWN-DIAG] Submission pump fault handling complete slot={SlotIndex} queuedSubmissions={QueuedSubmissions} totalFaults={TotalFaults} initiatingFaults={InitiatingFaults} cascadeFaults={CascadeFaults}", slotIndex, Interlocked.Read(ref _queuedSubmissionCount), totalFaultCount, Interlocked.Read(ref _submissionPumpInitiatingFaultCount), Interlocked.Read(ref _submissionPumpCascadeFaultCount));
            }
        }

        private async ValueTask<TransitPublishResult> PublishToConnectionWithReconnectAsync(
            int slotIndex,
            PendingSubmission submission,
            long dispatcherAssignedTick,
            CancellationToken cancellationToken)
        {
            long methodEntryTick = Stopwatch.GetTimestamp();
            _logger.LogInformation("[SUBMIT-PATH] stage=publish-entry messageId={MessageId} tick={Tick}", submission.MessageId, methodEntryTick);
            await EnsureConnectedForPublishAsync(slotIndex, cancellationToken).ConfigureAwait(false);

            TransitConnection? selectedConnection = _connectionSlots[slotIndex].Connection;
            string? selectedConnectionId = selectedConnection?.ConnectionId;

            long beforeSubmitTakethisTick = Stopwatch.GetTimestamp();
            TransitPublishResult result = await PublishToConnectionAsync(slotIndex, submission, dispatcherAssignedTick, cancellationToken).ConfigureAwait(false);
            long afterSubmitTakethisTick = Stopwatch.GetTimestamp();

            if (result.Status == TransitPublishStatus.Unavailable)
            {
                await ReconnectAsync(slotIndex, cancellationToken).ConfigureAwait(false);
                selectedConnection = _connectionSlots[slotIndex].Connection;
                selectedConnectionId = selectedConnection?.ConnectionId;
                beforeSubmitTakethisTick = Stopwatch.GetTimestamp();
                result = await PublishToConnectionAsync(slotIndex, submission, dispatcherAssignedTick, cancellationToken).ConfigureAwait(false);
                afterSubmitTakethisTick = Stopwatch.GetTimestamp();
            }

            RecordPublishToConnectionTrace(new PublishToConnectionTraceRecord(
                MessageId: submission.MessageId,
                SlotIndex: slotIndex,
                MethodEntryTick: methodEntryTick,
                SelectedConnectionId: selectedConnectionId,
                BeforeSubmitTakethisTick: beforeSubmitTakethisTick,
                AfterSubmitTakethisTick: afterSubmitTakethisTick));

            return result;
        }

        private static int GetCompletedInFlightIndex(List<InFlightSubmission> inFlight)
        {
            for (int i = 0; i < inFlight.Count; i++)
            {
                if (inFlight[i].PublishTask.IsCompleted)
                {
                    return i;
                }
            }

            return -1;
        }

        private static void RemoveInFlightAt(List<InFlightSubmission> inFlight, int index)
        {
            int lastIndex = inFlight.Count - 1;
            if (index != lastIndex)
            {
                inFlight[index] = inFlight[lastIndex];
            }

            inFlight.RemoveAt(lastIndex);
        }

        private async Task CompleteInFlightSubmissionAsync(InFlightSubmission inFlightSubmission, CancellationToken cancellationToken)
        {
            try
            {
                TransitPublishResult result = await inFlightSubmission.PublishTask.ConfigureAwait(false);
                _ = CompleteSubmissionIfPending(inFlightSubmission.Submission, result, countAsAmbiguous: false, allowConnectionOwned: true);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _ = CompleteSubmissionIfPending(
                    inFlightSubmission.Submission,
                    new TransitPublishResult(
                        MessageId: inFlightSubmission.Submission.MessageId,
                        Status: TransitPublishStatus.Canceled,
                        ResponseCode: null,
                        ResponseText: "Transit publisher canceled.",
                        Provenance: TransitPublishProvenance.Preemption,
                        ProvenanceConnectionState: _state,
                        ProvenanceTick: Stopwatch.GetTimestamp()),
                    countAsAmbiguous: false,
                    allowConnectionOwned: true);
            }
            catch
            {
                _ = CompleteSubmissionIfPending(
                    inFlightSubmission.Submission,
                    new TransitPublishResult(
                        MessageId: inFlightSubmission.Submission.MessageId,
                        Status: TransitPublishStatus.Ambiguous,
                        ResponseCode: null,
                        ResponseText: "Transit submission pump faulted before definitive TAKETHIS responses were received.",
                        Provenance: TransitPublishProvenance.Shutdown,
                        ProvenanceConnectionState: _state,
                        ProvenanceTick: Stopwatch.GetTimestamp()),
                    countAsAmbiguous: true,
                    allowConnectionOwned: true);
                throw;
            }
        }

        private void CompleteInFlightSubmissionsAsCanceled(List<InFlightSubmission> inFlight)
        {
            for (int i = 0; i < inFlight.Count; i++)
            {
                PendingSubmission submission = inFlight[i].Submission;
                _ = CompleteSubmissionIfPending(
                    submission,
                    new TransitPublishResult(
                        MessageId: submission.MessageId,
                        Status: TransitPublishStatus.Canceled,
                        ResponseCode: null,
                        ResponseText: "Transit publisher canceled."),
                    countAsAmbiguous: false,
                    allowConnectionOwned: true);
            }

            inFlight.Clear();
        }

        /// <summary>
        /// Completes in-flight submissions during publisher preemption using ownership-aware terminal statuses.
        /// </summary>
        /// <param name="inFlight">The in-flight submissions owned by the submission pump.</param>
        private void CompleteInFlightSubmissionsForPreemption(List<InFlightSubmission> inFlight)
        {
            for (int i = 0; i < inFlight.Count; i++)
            {
                CompleteSubmissionForPreemption(inFlight[i].Submission);
            }

            inFlight.Clear();
        }

        private void CompleteInFlightSubmissionsAsAmbiguous(List<InFlightSubmission> inFlight, string reason)
        {
            for (int i = 0; i < inFlight.Count; i++)
            {
                PendingSubmission submission = inFlight[i].Submission;
                _ = CompleteSubmissionIfPending(
                    submission,
                    new TransitPublishResult(
                        MessageId: submission.MessageId,
                        Status: TransitPublishStatus.Ambiguous,
                        ResponseCode: null,
                        ResponseText: reason,
                        Provenance: TransitPublishProvenance.Shutdown,
                        ProvenanceConnectionState: _state,
                        ProvenanceTick: Stopwatch.GetTimestamp()),
                    countAsAmbiguous: true,
                    allowConnectionOwned: true);
            }

            inFlight.Clear();
        }

        private async Task EnsureConnectedForPublishAsync(int slotIndex, CancellationToken cancellationToken)
        {
            ThrowIfShutdownRequested(cancellationToken);

            ConnectionSlot slot = _connectionSlots[slotIndex];
            await slot.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                ThrowIfShutdownRequested(cancellationToken);

                if (slot.Connection is null)
                {
                    await EstablishConnectionAsync(slotIndex, incrementReconnectCounter: false, cancellationToken).ConfigureAwait(false);
                    return;
                }

                if (slot.Connection.CurrentState is TransitConnectionState.Disconnected or TransitConnectionState.Faulted)
                {
                    await ReconnectCoreAsync(slotIndex, incrementReconnectCounter: true, cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                slot.Gate.Release();
            }
        }

        private async Task ReconnectAsync(int slotIndex, CancellationToken cancellationToken)
        {
            ThrowIfShutdownRequested(cancellationToken);

            ConnectionSlot slot = _connectionSlots[slotIndex];
            await slot.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                ThrowIfShutdownRequested(cancellationToken);

                if (slot.Connection is not null && slot.Connection.CurrentState is TransitConnectionState.Ready or TransitConnectionState.Publishing)
                {
                    return;
                }

                await ReconnectCoreAsync(slotIndex, incrementReconnectCounter: true, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                slot.Gate.Release();
            }
        }

        private async Task ReconnectCoreAsync(int slotIndex, bool incrementReconnectCounter, CancellationToken cancellationToken)
        {
            ThrowIfShutdownRequested(cancellationToken);

            ConnectionSlot slot = _connectionSlots[slotIndex];

            if (slot.Connection is not null)
            {
                await slot.Connection.DisposeAsync().ConfigureAwait(false);
                slot.Connection = null;
            }

            ThrowIfShutdownRequested(cancellationToken);
            await EstablishConnectionAsync(slotIndex, incrementReconnectCounter, cancellationToken).ConfigureAwait(false);
        }

        private async Task EstablishConnectionAsync(int slotIndex, bool incrementReconnectCounter, CancellationToken cancellationToken)
        {
            _logger.LogInformation("[INIT-TRACE] TransitPublisher.EstablishConnectionAsync START slot={SlotIndex}, reconnect={Reconnect}", slotIndex, incrementReconnectCounter);
            ThrowIfShutdownRequested(cancellationToken);

            if (slotIndex == 0)
            {
                TransitionState(TransitConnectionState.Connecting);
            }

            LogTransitConnectionAttempt(_logger, _runtimeOptions.TransitServerHost, _runtimeOptions.TransitServerPort, _runtimeOptions.TransitServerUseSsl);

            TransitConnection connection = new(
                _runtimeOptions.TransitServerHost,
                _runtimeOptions.TransitServerPort,
                _runtimeOptions.TransitServerUseSsl,
                _logger,
                _perConnectionPipelineDepth,
                _runtimeOptions.WriteBatchCoalesceMicroseconds);

            _connectionHistory.Add(new ConnectionRecord(slotIndex, connection));
            Interlocked.Increment(ref _connectionSlots[slotIndex].CreatedConnections);

            try
            {
                _logger.LogInformation("[INIT-TRACE] TransitPublisher.EstablishConnectionAsync BEFORE TransitConnection.InitializeAsync slot={SlotIndex}, connectionId={ConnectionId}", slotIndex, connection.ConnectionId);
                await connection.InitializeAsync(cancellationToken).ConfigureAwait(false);
                _logger.LogInformation("[INIT-TRACE] TransitPublisher.EstablishConnectionAsync AFTER TransitConnection.InitializeAsync slot={SlotIndex}, connectionId={ConnectionId}", slotIndex, connection.ConnectionId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[INIT-TRACE] TransitPublisher.EstablishConnectionAsync FAILED slot={SlotIndex}, connectionId={ConnectionId}: {ExceptionType}: {ExceptionMessage}", slotIndex, connection.ConnectionId, ex.GetType().FullName, ex.Message);
                throw;
            }

            if (_disposeRequested)
            {
                await connection.DisposeAsync().ConfigureAwait(false);
                throw new OperationCanceledException("Transit publisher reconnect canceled because shutdown has begun.", cancellationToken);
            }

            _connectionSlots[slotIndex].Connection = connection;

            if (incrementReconnectCounter)
            {
                Interlocked.Increment(ref _totalReconnects);
                Interlocked.Increment(ref _connectionSlots[slotIndex].Reconnects);
            }

            if (slotIndex == 0)
            {
                TransitionState(connection.CurrentState);
            }
        }

        private async ValueTask<TransitPublishResult> PublishToConnectionAsync(
            int slotIndex,
            PendingSubmission submission,
            long dispatcherAssignedTick,
            CancellationToken cancellationToken)
        {
            ConnectionSlot slot = _connectionSlots[slotIndex];
            TransitConnection? connection = slot.Connection;
            if (connection is null)
            {
                return new TransitPublishResult(
                    MessageId: submission.MessageId,
                    Status: TransitPublishStatus.Unavailable,
                    ResponseCode: null,
                    ResponseText: "Transit connection unavailable.",
                    T0PublishAsyncEnterTick: submission.PublishAsyncEnterTick,
                    T1DispatcherAssignedTick: dispatcherAssignedTick,
                    Provenance: TransitPublishProvenance.Unavailable,
                    ProvenanceConnectionState: _state,
                    ProvenanceSlotIndex: slotIndex,
                    ProvenanceTick: Stopwatch.GetTimestamp());
            }

            Interlocked.Increment(ref slot.TotalSubmissionsRouted);

            TransitPublishResult result;
            try
            {
                if (!submission.TryMarkConnectionOwned())
                {
                    throw new InvalidOperationException("Submission lifecycle invariant violated: connection ownership was not established before transit submit.");
                }

                result = await connection.SubmitTakethisAsync(
                    submission.MessageId,
                    submission.ArticlePayload,
                    cancellationToken,
                    submission.PublishAsyncEnterTick,
                    dispatcherAssignedTick).ConfigureAwait(false);
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested && IsConnectionLifecycleSubmitFailure(connection, ex))
            {
                result = new TransitPublishResult(
                    MessageId: submission.MessageId,
                    Status: TransitPublishStatus.Ambiguous,
                    ResponseCode: null,
                    ResponseText: "Connection failed before definitive TAKETHIS responses were received.",
                    T0PublishAsyncEnterTick: submission.PublishAsyncEnterTick,
                    T1DispatcherAssignedTick: dispatcherAssignedTick,
                    Provenance: TransitPublishProvenance.ConnectionClose,
                    ProvenanceConnectionId: connection.ConnectionId,
                    ProvenanceConnectionState: connection.CurrentState,
                    ProvenanceSlotIndex: slotIndex,
                    ProvenanceTick: Stopwatch.GetTimestamp());
            }

            if (result.T0PublishAsyncEnterTick == 0 || result.T1DispatcherAssignedTick == 0)
            {
                result = result with
                {
                    T0PublishAsyncEnterTick = result.T0PublishAsyncEnterTick == 0 ? submission.PublishAsyncEnterTick : result.T0PublishAsyncEnterTick,
                    T1DispatcherAssignedTick = result.T1DispatcherAssignedTick == 0 ? dispatcherAssignedTick : result.T1DispatcherAssignedTick,
                };
            }

            if (result.Status == TransitPublishStatus.Accepted)
            {
                Interlocked.Increment(ref _totalArticlesAccepted);
            }
            else if (result.Status == TransitPublishStatus.Rejected)
            {
                Interlocked.Increment(ref _totalArticlesRejected);
            }
            else if (result.Status == TransitPublishStatus.Ambiguous)
            {
                Interlocked.Increment(ref _totalArticlesAmbiguous);
            }

            return result;
        }

        private static bool IsConnectionLifecycleSubmitFailure(TransitConnection connection, Exception exception)
        {
            if (exception is ObjectDisposedException or IOException or SocketException)
            {
                return true;
            }

            if (exception is TransitConnection.TransitConnectionLifecycleException lifecycleException)
            {
                return lifecycleException.Failure is TransitConnection.TransitConnectionLifecycleFailure.WriterNotInitialized
                    or TransitConnection.TransitConnectionLifecycleFailure.WriterCompletedDuringTakethisSubmission;
            }

            if (exception is InvalidOperationException)
            {
                return connection.CurrentState is TransitConnectionState.Disconnecting or TransitConnectionState.Disconnected or TransitConnectionState.Faulted;
            }

            return false;
        }

        private async Task RunStatsLoopAsync(CancellationToken cancellationToken)
        {
            using PeriodicTimer timer = new(StatsInterval, _timeProvider);
            long previousTxBytes = 0;
            long previousRxBytes = 0;

            DateTimeOffset previousTimestamp = _timeProvider.GetUtcNow();

            try
            {
                while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
                {
                    DateTimeOffset currentTimestamp = _timeProvider.GetUtcNow();
                    TimeSpan elapsed = currentTimestamp - previousTimestamp;
                    if (elapsed <= TimeSpan.Zero)
                    {
                        elapsed = StatsInterval;
                    }

                    int activeConnections = GetActiveConnectionCount();
                    int outstandingSubmissions = checked((int)Interlocked.Read(ref _queuedSubmissionCount));
                    TransitTransportSnapshot snapshot = CaptureTransportSnapshot(activeConnections, outstandingSubmissions);

                    long txDelta = Math.Max(0, snapshot.TotalBytesTransmitted - previousTxBytes);
                    long rxDelta = Math.Max(0, snapshot.TotalBytesReceived - previousRxBytes);

                    previousTxBytes = snapshot.TotalBytesTransmitted;
                    previousRxBytes = snapshot.TotalBytesReceived;
                    previousTimestamp = currentTimestamp;

                    double txBitsPerSecond = txDelta <= 0 ? 0 : (txDelta * 8d) / elapsed.TotalSeconds;
                    double rxBitsPerSecond = rxDelta <= 0 ? 0 : (rxDelta * 8d) / elapsed.TotalSeconds;

                    LogTransitSnapshot(
                        _logger,
                        snapshot.TotalArticlesSubmitted,
                        snapshot.TotalArticlesAccepted,
                        snapshot.TotalArticlesRejected,
                        snapshot.TotalArticlesAmbiguous,
                        snapshot.TotalReconnects,
                        snapshot.OutstandingSubmissions,
                        snapshot.TotalBytesTransmitted,
                        snapshot.TotalBytesReceived,
                        FormatBitRate(txBitsPerSecond),
                        FormatBitRate(rxBitsPerSecond));
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
        }

        private void CompletePendingSubmissionsAsAmbiguous(string reason)
        {
            while (_submissionChannel.Reader.TryRead(out PendingSubmission? pending))
            {
                _ = CompleteSubmissionIfPending(
                    pending,
                    new TransitPublishResult(
                        MessageId: pending.MessageId,
                        Status: TransitPublishStatus.Ambiguous,
                        ResponseCode: null,
                        ResponseText: reason,
                        Provenance: TransitPublishProvenance.Shutdown,
                        ProvenanceConnectionState: _state,
                        ProvenanceTick: Stopwatch.GetTimestamp()),
                    countAsAmbiguous: true,
                    allowConnectionOwned: false);
            }
        }

        private void CompletePendingSubmissionsAsCanceled()
        {
            int completedCount = 0;
            while (_submissionChannel.Reader.TryRead(out PendingSubmission? pending))
            {
                if (CompleteSubmissionIfPending(
                        pending,
                        new TransitPublishResult(
                            MessageId: pending.MessageId,
                            Status: TransitPublishStatus.Canceled,
                            ResponseCode: null,
                            ResponseText: "Transit publisher canceled.",
                            Provenance: TransitPublishProvenance.Cancellation,
                            ProvenanceConnectionState: _state,
                            ProvenanceTick: Stopwatch.GetTimestamp()),
                        countAsAmbiguous: false,
                        allowConnectionOwned: false))
                {
                    completedCount++;
                }
            }

            if (completedCount > 0)
            {
                _logger.LogInformation("[SHUTDOWN-DIAG] Pending submissions completed as canceled count={Count}", completedCount);
            }
        }

        private int GetActiveConnectionCount()
        {
            int active = 0;

            for (int i = 0; i < _connectionSlots.Length; i++)
            {
                TransitConnection? connection = _connectionSlots[i].Connection;
                if (connection is not null && connection.CurrentState is TransitConnectionState.Ready or TransitConnectionState.Publishing)
                {
                    active++;
                }
            }

            return active;
        }

        private int GetReadyConnectionCount()
        {
            int ready = 0;
            for (int i = 0; i < _connectionSlots.Length; i++)
            {
                TransitConnection? connection = _connectionSlots[i].Connection;
                if (connection is not null && connection.CurrentState == TransitConnectionState.Ready)
                {
                    ready++;
                }
            }

            return ready;
        }

        private int GetFaultedConnectionCount()
        {
            int faulted = 0;
            for (int i = 0; i < _connectionSlots.Length; i++)
            {
                TransitConnection? connection = _connectionSlots[i].Connection;
                if (connection is not null && connection.CurrentState == TransitConnectionState.Faulted)
                {
                    faulted++;
                }
            }

            return faulted;
        }

        private int GetReconnectingConnectionCount()
        {
            int reconnecting = 0;
            for (int i = 0; i < _connectionSlots.Length; i++)
            {
                TransitConnection? connection = _connectionSlots[i].Connection;
                if (connection is not null && connection.CurrentState == TransitConnectionState.Connecting)
                {
                    reconnecting++;
                }
            }

            return reconnecting;
        }

        private long GetOutstandingConnectionOperationsCount()
        {
            long outstanding = 0;
            foreach (ConnectionRecord record in _connectionHistory)
            {
                TransitConnection.TransitConnectionDiagnosticsSnapshot snapshot = record.Connection.CaptureDiagnosticsSnapshot();
                outstanding += snapshot.OutstandingOperations.Length;
            }

            return outstanding;
        }

        private static TransitPublisherPumpFaultOrigin ClassifySubmissionPumpFaultOrigin(Exception ex)
        {
            if (ex is InvalidOperationException invalidOperation)
            {
                string message = invalidOperation.Message;
                if (message.Contains("Unable to resolve completed transit publish task.", StringComparison.Ordinal))
                {
                    return TransitPublisherPumpFaultOrigin.PumpCoordination;
                }

                if (message.Contains("connection ownership was not established", StringComparison.OrdinalIgnoreCase))
                {
                    return TransitPublisherPumpFaultOrigin.PublishToConnectionAsync;
                }
            }

            return TransitPublisherPumpFaultOrigin.Unknown;
        }

        private static InvalidOperationFingerprintMessageClass ClassifyInvalidOperationFingerprintMessageClass(Exception ex)
        {
            if (ex is not InvalidOperationException invalidOperation)
            {
                return InvalidOperationFingerprintMessageClass.NotInvalidOperationException;
            }

            string message = invalidOperation.Message;
            if (string.Equals(message, "Unable to resolve completed transit publish task.", StringComparison.Ordinal))
            {
                return InvalidOperationFingerprintMessageClass.PumpTaskResolution;
            }

            if (string.Equals(message, "Submission lifecycle invariant violated: connection ownership was not established before transit submit.", StringComparison.Ordinal))
            {
                return InvalidOperationFingerprintMessageClass.ConnectionOwnershipInvariant;
            }

            if (string.Equals(message, "Submission terminalization invariant violated: terminal state reached without task completion.", StringComparison.Ordinal))
            {
                return InvalidOperationFingerprintMessageClass.TerminalizationMissingTask;
            }

            if (string.Equals(message, "Submission terminalization invariant violated: active tracking entry missing during terminalization.", StringComparison.Ordinal))
            {
                return InvalidOperationFingerprintMessageClass.TerminalizationMissingTrackingEntry;
            }

            if (string.Equals(message, "Submission accounting invariant violated: queued submission count became negative.", StringComparison.Ordinal))
            {
                return InvalidOperationFingerprintMessageClass.QueueAccountingInvariant;
            }

            return InvalidOperationFingerprintMessageClass.OtherInvalidOperationException;
        }

        private static SanitizedFirstFaultMessageClass ClassifySanitizedFirstFaultMessage(Exception ex)
        {
            if (ex is not InvalidOperationException invalidOperation)
            {
                return SanitizedFirstFaultMessageClass.NotInvalidOperation;
            }

            string message = invalidOperation.Message;
            if (string.Equals(message, "NNTP connection closed while awaiting line response.", StringComparison.Ordinal))
            {
                return SanitizedFirstFaultMessageClass.P1_EOF;
            }

            if (string.Equals(message, "NNTP response line exceeded maximum length of 16384 bytes.", StringComparison.Ordinal))
            {
                return SanitizedFirstFaultMessageClass.P2_LINE_LENGTH;
            }

            string? stackTrace = ex.StackTrace;
            if (!string.IsNullOrEmpty(stackTrace) && stackTrace.Contains("System.IO.Pipelines", StringComparison.Ordinal))
            {
                return SanitizedFirstFaultMessageClass.F1_PIPE_READER_INVALID_OPERATION;
            }

            return SanitizedFirstFaultMessageClass.OTHER_REDACTED;
        }

        private static string CaptureExceptionStackTrace(Exception ex)
        {
            return string.IsNullOrWhiteSpace(ex.StackTrace)
                ? "(stack-trace-unavailable)"
                : ex.StackTrace;
        }

        private static void TryCaptureTopStackFrameFingerprint(Exception ex, out string? declaringType, out string? methodName)
        {
            declaringType = null;
            methodName = null;

            try
            {
                StackTrace trace = new(ex, fNeedFileInfo: false);
                StackFrame? frame = trace.GetFrame(0);
                MethodBase? method = frame?.GetMethod();
                declaringType = method?.DeclaringType?.FullName;
                methodName = method?.Name;
            }
            catch
            {
                declaringType = null;
                methodName = null;
            }
        }

        private static string FormatBitRate(double bitsPerSecond)
        {
            if (bitsPerSecond < 1000)
            {
                return $"{bitsPerSecond:F1} bps";
            }

            string[] units = ["Kbps", "Mbps", "Gbps", "Tbps"];
            double value = bitsPerSecond / 1000d;
            int unitIndex = 0;

            while (value >= 1000d && unitIndex < units.Length - 1)
            {
                value /= 1000d;
                unitIndex++;
            }

            return $"{value:F1} {units[unitIndex]}";
        }

        private void TransitionState(TransitConnectionState next)
        {
            _state = next;
            LogTransitStateTransition(_logger, next);
        }

        /// <summary>
        /// Preempts publisher submission processing by canceling submission pumps and terminalizing publisher-owned pending submissions.
        /// </summary>
        internal async Task PreemptSubmissionProcessingAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            _disposeRequested = true;
            _submissionChannel.Writer.TryComplete();
            _logger.LogInformation("[SHUTDOWN-DIAG] TransitPublisher submission preemption requested; submission channel completed");

            CompletePendingSubmissionsAsCanceled();
            CompleteTrackedPublisherOwnedSubmissionsForPreemption();

            await StopSubmissionWorkersAsync(cancellationToken).ConfigureAwait(false);

            CompletePendingSubmissionsAsCanceled();
            CompleteTrackedPublisherOwnedSubmissionsForPreemption();
            _logger.LogInformation("[SHUTDOWN-DIAG] TransitPublisher submission preemption completed queuedSubmissionCount={QueuedSubmissionCount}", Interlocked.Read(ref _queuedSubmissionCount));
        }

        public async ValueTask DisposeAsync()
        {
            _disposeRequested = true;
            TransitionState(TransitConnectionState.Disconnecting);

            TransitPublisherConnectionDiagnosticsSnapshot preDisposeSnapshot = CaptureConnectionDiagnosticsSnapshot();
            int preDisposePendingAcrossConnections = preDisposeSnapshot.Connections.Sum(static entry => entry.Snapshot.CurrentConcurrentSubmissions);
            _logger.LogInformation("[SHUTDOWN-DIAG] TransitPublisher.DisposeAsync start queuedSubmissionCount={QueuedSubmissionCount} pendingAcrossConnections={PendingAcrossConnections} activeSlots={ActiveSlots}", preDisposeSnapshot.QueuedSubmissionCount, preDisposePendingAcrossConnections, preDisposeSnapshot.Slots.Count(static slot => slot.TotalSubmissionsRouted > 0));

            await PreemptSubmissionProcessingAsync(CancellationToken.None).ConfigureAwait(false);

            if (_statsLoopCancellation is not null)
            {
                _statsLoopCancellation.Cancel();
                _logger.LogInformation("[SHUTDOWN-DIAG] TransitPublisher stats loop cancellation requested");
                _statsLoopCancellation.Dispose();
                _statsLoopCancellation = null;
            }

            if (_submissionWorkersCancellation is not null)
            {
                _submissionWorkersCancellation.Dispose();
                _submissionWorkersCancellation = null;
            }

            if (_statsLoop is not null)
            {
                try
                {
                    await _statsLoop.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }

                _statsLoop = null;
            }

            await DisposeConnectionsAsync().ConfigureAwait(false);
            _logger.LogInformation("[SHUTDOWN-DIAG] TransitPublisher connections disposed");

            CompletePendingSubmissionsAsCanceled();
            _logger.LogInformation("[SHUTDOWN-DIAG] TransitPublisher pending submissions completed as canceled");

            TransitPublisherConnectionDiagnosticsSnapshot postDisposeSnapshot = CaptureConnectionDiagnosticsSnapshot();
            int postDisposePendingAcrossConnections = postDisposeSnapshot.Connections.Sum(static entry => entry.Snapshot.CurrentConcurrentSubmissions);
            _logger.LogInformation("[SHUTDOWN-DIAG] TransitPublisher.DisposeAsync complete queuedSubmissionCount={QueuedSubmissionCount} pendingAcrossConnections={PendingAcrossConnections}", postDisposeSnapshot.QueuedSubmissionCount, postDisposePendingAcrossConnections);
            TransitionState(TransitConnectionState.Disconnected);
        }

        /// <summary>
        /// Cancels and awaits submission pump workers.
        /// </summary>
        private async Task StopSubmissionWorkersAsync(CancellationToken cancellationToken)
        {
            CancellationTokenSource? submissionWorkersCancellation = _submissionWorkersCancellation;
            if (submissionWorkersCancellation is not null)
            {
                try
                {
                    submissionWorkersCancellation.Cancel();
                    _logger.LogInformation("[SHUTDOWN-DIAG] TransitPublisher submission workers cancellation requested");
                }
                catch (ObjectDisposedException)
                {
                }
            }

            Task[]? submissionWorkers = _submissionWorkers;
            if (submissionWorkers is null)
            {
                return;
            }

            try
            {
                Task workersCompletion = Task.WhenAll(submissionWorkers);
                if (cancellationToken.CanBeCanceled)
                {
                    await workersCompletion.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await workersCompletion.ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
            }

            _submissionWorkers = null;
        }

        private void ObserveSubmissionPumpDepth(int slotIndex, int currentInFlightDepth)
        {
            ConnectionSlot slot = _connectionSlots[slotIndex];

            while (true)
            {
                long observedMax = Volatile.Read(ref slot.MaxObservedInFlightDepth);
                if (currentInFlightDepth <= observedMax)
                {
                    break;
                }

                if (Interlocked.CompareExchange(ref slot.MaxObservedInFlightDepth, currentInFlightDepth, observedMax) == observedMax)
                {
                    break;
                }
            }

            if (currentInFlightDepth >= _perConnectionPipelineDepth)
            {
                long reachedTick = Stopwatch.GetTimestamp();
                _ = Interlocked.CompareExchange(ref slot.FirstReachedConfiguredDepthTick, reachedTick, 0);
            }
        }

        private void RecordSubmissionTrace(SubmissionTraceRecord record)
        {
            lock (_submissionTraceGate)
            {
                if (_submissionTraceRecords.Count >= SubmissionTraceRecordLimit)
                {
                    return;
                }

                _submissionTraceRecords.Add(record);
            }
        }

        private void RecordPublishToConnectionTrace(PublishToConnectionTraceRecord record)
        {
            lock (_submissionTraceGate)
            {
                if (_publishToConnectionTraceRecords.Count >= SubmissionTraceRecordLimit)
                {
                    return;
                }

                _publishToConnectionTraceRecords.Add(record);
            }
        }

        private async Task DisposeConnectionsAsync()
        {
            for (int i = 0; i < _connectionSlots.Length; i++)
            {
                ConnectionSlot slot = _connectionSlots[i];
                await slot.Gate.WaitAsync(CancellationToken.None).ConfigureAwait(false);

                try
                {
                    if (slot.Connection is not null)
                    {
                        await slot.Connection.DisposeAsync().ConfigureAwait(false);
                        slot.Connection = null;
                    }
                }
                finally
                {
                    slot.Gate.Release();
                }
            }
        }

        /// <summary>
        /// Completes and accounts for a submission exactly once.
        /// </summary>
        private bool CompleteSubmissionIfPending(PendingSubmission submission, TransitPublishResult result, bool countAsAmbiguous, bool allowConnectionOwned)
        {
            if (!submission.TryMarkTerminal(allowConnectionOwned))
            {
                return false;
            }

            bool completionApplied = submission.Completion.TrySetResult(result);
            if (!completionApplied)
            {
                throw new InvalidOperationException("Submission terminalization invariant violated: terminal state reached without task completion.");
            }

            if (countAsAmbiguous)
            {
                Interlocked.Increment(ref _totalArticlesAmbiguous);
            }

            if (!_activeSubmissions.TryRemove(submission.SubmissionId, out _))
            {
                throw new InvalidOperationException("Submission terminalization invariant violated: active tracking entry missing during terminalization.");
            }

            long queuedAfterDecrement = Interlocked.Decrement(ref _queuedSubmissionCount);
            if (queuedAfterDecrement < 0)
            {
                throw new InvalidOperationException("Submission accounting invariant violated: queued submission count became negative.");
            }

            return true;
        }

        /// <summary>
        /// Completes all tracked submissions during publisher preemption using ownership-aware terminal statuses.
        /// </summary>
        private void CompleteTrackedPublisherOwnedSubmissionsForPreemption()
        {
            foreach ((long _, PendingSubmission submission) in _activeSubmissions)
            {
                CompleteSubmissionForPreemption(submission);
            }
        }

        /// <summary>
        /// Completes a submission during preemption as canceled if still publisher-local, or ambiguous once connection-owned.
        /// </summary>
        /// <param name="submission">The submission to terminalize.</param>
        private void CompleteSubmissionForPreemption(PendingSubmission submission)
        {
            if (CompleteSubmissionIfPending(
                    submission,
                    new TransitPublishResult(
                        MessageId: submission.MessageId,
                        Status: TransitPublishStatus.Canceled,
                        ResponseCode: null,
                        ResponseText: "Transit publisher canceled.",
                        Provenance: TransitPublishProvenance.Preemption,
                        ProvenanceConnectionState: _state,
                        ProvenanceTick: Stopwatch.GetTimestamp()),
                    countAsAmbiguous: false,
                    allowConnectionOwned: false))
            {
                return;
            }

            _ = CompleteSubmissionIfPending(
                submission,
                new TransitPublishResult(
                    MessageId: submission.MessageId,
                    Status: TransitPublishStatus.Ambiguous,
                    ResponseCode: null,
                    ResponseText: "Transit publisher shutdown occurred after connection ownership was established before a definitive TAKETHIS response was available.",
                    Provenance: TransitPublishProvenance.Preemption,
                    ProvenanceConnectionState: _state,
                    ProvenanceTick: Stopwatch.GetTimestamp()),
                countAsAmbiguous: true,
                allowConnectionOwned: true);
        }

        private void ThrowIfShutdownRequested(CancellationToken cancellationToken)
        {
            if (_disposeRequested)
            {
                throw new OperationCanceledException("Transit publisher operation canceled because shutdown has begun.", cancellationToken);
            }
        }

        private sealed class PendingSubmission
        {
            private const int LifecycleQueued = 0;
            private const int LifecycleInFlight = 1;
            private const int LifecycleConnectionOwned = 2;
            private const int LifecycleTerminal = 3;

            private int _lifecycleState = LifecycleQueued;

            /// <summary>
            /// Initializes a publisher submission container with lifecycle state tracking.
            /// </summary>
            internal PendingSubmission(
                long submissionId,
                string messageId,
                ReadOnlyMemory<byte> articlePayload,
                TaskCompletionSource<TransitPublishResult> completion,
                long publishAsyncEnterTick)
            {
                SubmissionId = submissionId;
                MessageId = messageId;
                ArticlePayload = articlePayload;
                Completion = completion;
                PublishAsyncEnterTick = publishAsyncEnterTick;
            }

            internal long SubmissionId { get; }

            internal string MessageId { get; }

            internal ReadOnlyMemory<byte> ArticlePayload { get; }

            internal TaskCompletionSource<TransitPublishResult> Completion { get; }

            internal long PublishAsyncEnterTick { get; }

            internal bool TryMarkInFlight()
            {
                return Interlocked.CompareExchange(ref _lifecycleState, LifecycleInFlight, LifecycleQueued) == LifecycleQueued;
            }

            internal bool TryMarkConnectionOwned()
            {
                return Interlocked.CompareExchange(ref _lifecycleState, LifecycleConnectionOwned, LifecycleInFlight) == LifecycleInFlight;
            }

            internal bool TryMarkTerminal(bool allowConnectionOwned)
            {
                while (true)
                {
                    int current = Volatile.Read(ref _lifecycleState);
                    if (current == LifecycleTerminal)
                    {
                        return false;
                    }

                    if (current == LifecycleQueued || current == LifecycleInFlight || (allowConnectionOwned && current == LifecycleConnectionOwned))
                    {
                        if (Interlocked.CompareExchange(ref _lifecycleState, LifecycleTerminal, current) == current)
                        {
                            return true;
                        }

                        continue;
                    }

                    return false;
                }
            }
        }

        private sealed record InFlightSubmission(
            PendingSubmission Submission,
            Task<TransitPublishResult> PublishTask);

        internal readonly record struct SubmissionTraceRecord(
            string MessageId,
            long RemovedFromSubmissionChannelTick,
            long PublishToConnectionInvokedTick,
            int InFlightCountBeforeAdd,
            int InFlightCountAfterAdd,
            int WriteIntentQueueDepthAtPumpRead);

        internal readonly record struct PublishToConnectionTraceRecord(
            string MessageId,
            int SlotIndex,
            long MethodEntryTick,
            string? SelectedConnectionId,
            long BeforeSubmitTakethisTick,
            long AfterSubmitTakethisTick);

        internal enum TransitPublisherPumpFaultOrigin
        {
            CompleteInFlightSubmissionAsync = 0,
            PublishToConnectionWithReconnectAsync = 1,
            EnsureConnectedForPublishAsync = 2,
            ReconnectAsync = 3,
            ReconnectCoreAsync = 4,
            EstablishConnectionAsync = 5,
            PublishToConnectionAsync = 6,
            PumpCoordination = 7,
            Unknown = 8,
        }

        internal enum PumpFaultMeasurementState
        {
            Unknown = 0,
            BeforeMeasurementEnd = 1,
            AfterMeasurementEnd = 2,
        }

        internal enum ProducerCompletionState
        {
            Unknown = 0,
            Incomplete = 1,
            Complete = 2,
        }

        internal enum DispatchersCompletedState
        {
            Unknown = 0,
            Incomplete = 1,
            Complete = 2,
        }

        internal enum InvalidOperationFingerprintMessageClass
        {
            None = 0,
            PumpTaskResolution = 1,
            ConnectionOwnershipInvariant = 2,
            TerminalizationMissingTask = 3,
            TerminalizationMissingTrackingEntry = 4,
            QueueAccountingInvariant = 5,
            OtherInvalidOperationException = 6,
            NotInvalidOperationException = 7,
        }

        internal enum SanitizedFirstFaultMessageClass
        {
            None = 0,
            P1_EOF = 1,
            P2_LINE_LENGTH = 2,
            F1_PIPE_READER_INVALID_OPERATION = 3,
            OTHER_REDACTED = 4,
            NotInvalidOperation = 5,
        }

        internal readonly record struct SubmissionPumpFaultCounts(
            long TotalFaultCount,
            long InitiatingFaultCount,
            long CascadeFaultCount);

        internal sealed record PumpFaultTelemetrySnapshot(
            long FaultSequence,
            bool IsInitiatingFault,
            int SlotIndex,
            long CapturedAtTick,
            string ExceptionType,
            string BaseExceptionType,
            int HResult,
            InvalidOperationFingerprintMessageClass InvalidOperationMessageClass,
            SanitizedFirstFaultMessageClass SanitizedFirstFaultMessageClass,
            string SanitizedFirstFaultMessage,
            string? FullFirstFaultStackTrace,
            string? TopStackFrameDeclaringType,
            string? TopStackFrameMethodName,
            TransitPublisherPumpFaultOrigin Origin,
            long QueuedSubmissionCount,
            int InFlightCount,
            long ActiveSubmissionCount,
            int ActiveConnectionCount,
            int ReadyConnectionCount,
            int FaultedConnectionCount,
            int ReconnectingConnectionCount,
            long OutstandingConnectionOperations,
            ProducerCompletionState ProducerCompletionState,
            bool MeasurementBoundaryObserved,
            long MeasurementStartTick,
            long MeasurementEndTick,
            double? MillisecondsFromMeasurementStart,
            double? MillisecondsFromMeasurementEnd,
            PumpFaultMeasurementState MeasurementStateAtFault,
            DispatchersCompletedState DispatchersCompletedState,
            int? ChannelImmediateAvailableCount);

        internal readonly record struct P1GreetingLifecycleEventSummary(
            string Event,
            long Tick,
            int InitializationAttemptId);

        internal readonly record struct P1GreetingProvenanceSummary(
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
            P1GreetingLifecycleEventSummary[] LifecycleEvents);

        internal sealed record TransitPublisherConnectionDiagnosticsSnapshot(
            int ConfiguredConnectionPoolSize,
            int ConfiguredPerConnectionPipelineDepth,
            long TotalReconnects,
            long QueuedSubmissionCount,
            SubmissionTraceRecord[] SubmissionTraceRecords,
            PublishToConnectionTraceRecord[] PublishToConnectionTraceRecords,
            ConnectionSlotSnapshot[] Slots,
            ConnectionDiagnosticsEntry[] Connections);

        internal sealed record ConnectionSlotSnapshot(
            int SlotIndex,
            bool HasCurrentConnection,
            string? CurrentConnectionId,
            long TotalSubmissionsRouted,
            long Reconnects,
            long CreatedConnections,
            int MaxObservedInFlightDepth,
            long WaitedForChannelReadabilityCount,
            long WaitedForCompletionWhilePipelineFullCount,
            long FirstReachedConfiguredDepthTick);

        internal sealed record ConnectionDiagnosticsEntry(
            int SlotIndex,
            TransitConnection.TransitConnectionDiagnosticsSnapshot Snapshot);

        private sealed record ConnectionRecord(int SlotIndex, TransitConnection Connection);

        private sealed class ConnectionSlot
        {
            internal ConnectionSlot(int slotIndex)
            {
                SlotIndex = slotIndex;
            }

            internal int SlotIndex { get; }

            internal SemaphoreSlim Gate { get; } = new(1, 1);

            internal TransitConnection? Connection { get; set; }

            internal long TotalSubmissionsRouted;

            internal long Reconnects;

            internal long CreatedConnections;

            internal long MaxObservedInFlightDepth;

            internal long WaitedForChannelReadabilityCount;

            internal long WaitedForCompletionWhilePipelineFullCount;

            internal long FirstReachedConfiguredDepthTick;
        }

        [LoggerMessage(EventId = 2200, Level = LogLevel.Information, Message = "Transit connection attempt to {Host}:{Port} (UseSsl={UseSsl})")]
        private static partial void LogTransitConnectionAttempt(ILogger logger, string host, int port, bool useSsl);

        [LoggerMessage(EventId = 2201, Level = LogLevel.Debug, Message = "Transit connection state changed to {State}")]
        private static partial void LogTransitStateTransition(ILogger logger, TransitConnectionState state);

        [LoggerMessage(EventId = 2202, Level = LogLevel.Warning, Message = "Article submission unavailable for MessageId={MessageId}; state={State}")]
        private static partial void LogArticleSubmissionUnavailable(ILogger logger, string messageId, TransitConnectionState state);

        [LoggerMessage(EventId = 2203, Level = LogLevel.Information, Message = "Article submission queued for MessageId={MessageId}")]
        private static partial void LogArticleSubmissionQueued(ILogger logger, string messageId);

        [LoggerMessage(EventId = 2204, Level = LogLevel.Information, Message = "Article submission outcome for MessageId={MessageId}; Status={Status}; ResponseCode={ResponseCode}; ResponseText={ResponseText}")]
        private static partial void LogArticleSubmissionOutcome(ILogger logger, string messageId, TransitPublishStatus status, int? responseCode, string? responseText);

        [LoggerMessage(EventId = 2205, Level = LogLevel.Warning, Message = "Transit submission pump faulted")]
        private static partial void LogTransitSubmissionPumpFaulted(ILogger logger, Exception exception);

        [LoggerMessage(EventId = 2208, Level = LogLevel.Information, Message = "Transit snapshot: Submitted={Submitted}, Accepted={Accepted}, Rejected={Rejected}, Ambiguous={Ambiguous}, Reconnects={Reconnects}, Outstanding={Outstanding}, BytesTx={BytesTransmitted}, BytesRx={BytesReceived}, TxRate={TxRate}, RxRate={RxRate}")]
        private static partial void LogTransitSnapshot(ILogger logger, long submitted, long accepted, long rejected, long ambiguous, long reconnects, int outstanding, long bytesTransmitted, long bytesReceived, string txRate, string rxRate);

    }
}
