using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Sockets;
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

        private volatile bool _disposeRequested;
        private volatile TransitConnectionState _state = TransitConnectionState.Disconnected;

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
                    T7PublishAsyncCompleteTick: Stopwatch.GetTimestamp());
            }

            TaskCompletionSource<TransitPublishResult> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            PendingSubmission submission = new(messageId, articlePayload, completion, publishAsyncEnterTick);

            long submissionChannelWriteStartTick = Stopwatch.GetTimestamp();
            long submissionChannelWriteEndTick;

            try
            {
                await _submissionChannel.Writer.WriteAsync(submission, cancellationToken).ConfigureAwait(false);
                submissionChannelWriteEndTick = Stopwatch.GetTimestamp();
                _logger.LogInformation("[SUBMIT-PATH] stage=submission-channel-write messageId={MessageId} writeStartTick={WriteStartTick} writeEndTick={WriteEndTick}", messageId, submissionChannelWriteStartTick, submissionChannelWriteEndTick);
                Interlocked.Increment(ref _totalArticlesSubmitted);
                Interlocked.Increment(ref _queuedSubmissionCount);
            }
            catch (ChannelClosedException)
            {
                return new TransitPublishResult(
                    MessageId: messageId,
                    Status: TransitPublishStatus.Unavailable,
                    ResponseCode: null,
                    ResponseText: "Transit publisher is shutting down.",
                    T0PublishAsyncEnterTick: publishAsyncEnterTick,
                    T7PublishAsyncCompleteTick: Stopwatch.GetTimestamp());
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

                        if (task.IsCompletedSuccessfully)
                        {
                            TransitPublishResult result = task.Result;
                            LogArticleSubmissionOutcome(logger, result.MessageId, result.Status, result.ResponseCode, result.ResponseText);
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
                        long removedFromSubmissionChannelTick = Stopwatch.GetTimestamp();
                        _logger.LogInformation("[SUBMIT-PATH] stage=pump-read messageId={MessageId} tick={Tick}", submission.MessageId, removedFromSubmissionChannelTick);
                        int currentInFlightBeforeAdd = inFlight.Count;
                        int writeIntentQueueDepthAtPumpRead = _connectionSlots[slotIndex].Connection?.CurrentWriteIntentQueueDepth is long depth ? (int)depth : -1;
                        long publishInvocationTick = Stopwatch.GetTimestamp();
                        long dispatcherAssignedTick = publishInvocationTick;
                        Task<TransitPublishResult> publishTask = PublishToConnectionWithReconnectAsync(slotIndex, submission, dispatcherAssignedTick, cancellationToken).AsTask();
                        inFlight.Add(new InFlightSubmission(submission, publishTask));
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
                    bool submissionReadable = reader.TryRead(out PendingSubmission? pendingReadableSubmission);
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
                        long removedFromSubmissionChannelTick = Stopwatch.GetTimestamp();
                        PendingSubmission submission = pendingReadableSubmission;
                        _logger.LogInformation("[SUBMIT-PATH] stage=pump-read messageId={MessageId} tick={Tick}", submission.MessageId, removedFromSubmissionChannelTick);
                        int currentInFlightBeforeAdd = inFlight.Count;
                        int writeIntentQueueDepthAtPumpRead = _connectionSlots[slotIndex].Connection?.CurrentWriteIntentQueueDepth is long depth ? (int)depth : -1;
                        long publishInvocationTick = Stopwatch.GetTimestamp();
                        long dispatcherAssignedTick = publishInvocationTick;
                        Task<TransitPublishResult> publishTask = PublishToConnectionWithReconnectAsync(slotIndex, submission, dispatcherAssignedTick, cancellationToken).AsTask();
                        inFlight.Add(new InFlightSubmission(submission, publishTask));
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
                        Task[] pendingTasks = new Task[inFlight.Count];
                        for (int i = 0; i < inFlight.Count; i++)
                        {
                            pendingTasks[i] = inFlight[i].PublishTask;
                        }

                        Task completedTask = await Task.WhenAny(pendingTasks).ConfigureAwait(false);

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
                CompleteInFlightSubmissionsAsCanceled(inFlight);
                CompletePendingSubmissionsAsCanceled();
                _logger.LogInformation("[SHUTDOWN-DIAG] Submission pump cancellation handling complete slot={SlotIndex} queuedSubmissions={QueuedSubmissions}", slotIndex, Interlocked.Read(ref _queuedSubmissionCount));
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[SHUTDOWN-DIAG] Submission pump faulted slot={SlotIndex} inFlight={InFlightCount} queuedSubmissions={QueuedSubmissions}", slotIndex, inFlight.Count, Interlocked.Read(ref _queuedSubmissionCount));
                CompleteInFlightSubmissionsAsAmbiguous(inFlight, "Transit submission pump faulted before definitive TAKETHIS responses were received.");
                TransitionState(TransitConnectionState.Faulted);
                LogTransitSubmissionPumpFaulted(_logger, ex);
                CompletePendingSubmissionsAsAmbiguous("Transit submission pump faulted before definitive TAKETHIS responses were received.");
                _logger.LogInformation("[SHUTDOWN-DIAG] Submission pump fault handling complete slot={SlotIndex} queuedSubmissions={QueuedSubmissions}", slotIndex, Interlocked.Read(ref _queuedSubmissionCount));
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
                inFlightSubmission.Submission.Completion.TrySetResult(result);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                inFlightSubmission.Submission.Completion.TrySetResult(new TransitPublishResult(
                    MessageId: inFlightSubmission.Submission.MessageId,
                    Status: TransitPublishStatus.Canceled,
                    ResponseCode: null,
                    ResponseText: "Transit publisher canceled."));
            }
            catch
            {
                inFlightSubmission.Submission.Completion.TrySetResult(new TransitPublishResult(
                    MessageId: inFlightSubmission.Submission.MessageId,
                    Status: TransitPublishStatus.Ambiguous,
                    ResponseCode: null,
                    ResponseText: "Transit submission pump faulted before definitive TAKETHIS responses were received."));
                Interlocked.Increment(ref _totalArticlesAmbiguous);
                throw;
            }
            finally
            {
                Interlocked.Decrement(ref _queuedSubmissionCount);
            }
        }

        private void CompleteInFlightSubmissionsAsCanceled(List<InFlightSubmission> inFlight)
        {
            for (int i = 0; i < inFlight.Count; i++)
            {
                InFlightSubmission submission = inFlight[i];
                submission.Submission.Completion.TrySetResult(new TransitPublishResult(
                    MessageId: submission.Submission.MessageId,
                    Status: TransitPublishStatus.Canceled,
                    ResponseCode: null,
                    ResponseText: "Transit publisher canceled."));
                Interlocked.Decrement(ref _queuedSubmissionCount);
            }

            inFlight.Clear();
        }

        private void CompleteInFlightSubmissionsAsAmbiguous(List<InFlightSubmission> inFlight, string reason)
        {
            for (int i = 0; i < inFlight.Count; i++)
            {
                InFlightSubmission submission = inFlight[i];
                submission.Submission.Completion.TrySetResult(new TransitPublishResult(
                    MessageId: submission.Submission.MessageId,
                    Status: TransitPublishStatus.Ambiguous,
                    ResponseCode: null,
                    ResponseText: reason));
                Interlocked.Increment(ref _totalArticlesAmbiguous);
                Interlocked.Decrement(ref _queuedSubmissionCount);
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
                    T1DispatcherAssignedTick: dispatcherAssignedTick);
            }

            Interlocked.Increment(ref slot.TotalSubmissionsRouted);

            TransitPublishResult result;
            try
            {
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
                    T1DispatcherAssignedTick: dispatcherAssignedTick);
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

            if (exception is InvalidOperationException invalidOperationException)
            {
                return invalidOperationException.Message.Contains("Transit protocol writer is not initialized.", StringComparison.Ordinal)
                    || invalidOperationException.Message.Contains("Transit protocol writer completed during TAKETHIS submission.", StringComparison.Ordinal)
                    || connection.CurrentState is TransitConnectionState.Disconnecting or TransitConnectionState.Disconnected or TransitConnectionState.Faulted;
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
                pending.Completion.TrySetResult(new TransitPublishResult(
                    MessageId: pending.MessageId,
                    Status: TransitPublishStatus.Ambiguous,
                    ResponseCode: null,
                    ResponseText: reason));

                Interlocked.Increment(ref _totalArticlesAmbiguous);
                Interlocked.Decrement(ref _queuedSubmissionCount);
            }
        }

        private void CompletePendingSubmissionsAsCanceled()
        {
            int completedCount = 0;
            while (_submissionChannel.Reader.TryRead(out PendingSubmission? pending))
            {
                pending.Completion.TrySetResult(new TransitPublishResult(
                    MessageId: pending.MessageId,
                    Status: TransitPublishStatus.Canceled,
                    ResponseCode: null,
                    ResponseText: "Transit publisher canceled."));

                Interlocked.Decrement(ref _queuedSubmissionCount);
                completedCount++;
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

        public async ValueTask DisposeAsync()
        {
            _disposeRequested = true;
            TransitionState(TransitConnectionState.Disconnecting);

            TransitPublisherConnectionDiagnosticsSnapshot preDisposeSnapshot = CaptureConnectionDiagnosticsSnapshot();
            int preDisposePendingAcrossConnections = preDisposeSnapshot.Connections.Sum(static entry => entry.Snapshot.CurrentConcurrentSubmissions);
            _logger.LogInformation("[SHUTDOWN-DIAG] TransitPublisher.DisposeAsync start queuedSubmissionCount={QueuedSubmissionCount} pendingAcrossConnections={PendingAcrossConnections} activeSlots={ActiveSlots}", preDisposeSnapshot.QueuedSubmissionCount, preDisposePendingAcrossConnections, preDisposeSnapshot.Slots.Count(static slot => slot.TotalSubmissionsRouted > 0));

            _submissionChannel.Writer.TryComplete();
            _logger.LogInformation("[SHUTDOWN-DIAG] TransitPublisher submission channel completed");

            if (_statsLoopCancellation is not null)
            {
                _statsLoopCancellation.Cancel();
                _logger.LogInformation("[SHUTDOWN-DIAG] TransitPublisher stats loop cancellation requested");
                _statsLoopCancellation.Dispose();
                _statsLoopCancellation = null;
            }

            if (_submissionWorkersCancellation is not null)
            {
                _submissionWorkersCancellation.Cancel();
                _logger.LogInformation("[SHUTDOWN-DIAG] TransitPublisher submission workers cancellation requested");
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

            if (_submissionWorkers is not null)
            {
                try
                {
                    await Task.WhenAll(_submissionWorkers).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }

                _submissionWorkers = null;
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

        private void ThrowIfShutdownRequested(CancellationToken cancellationToken)
        {
            if (_disposeRequested)
            {
                throw new OperationCanceledException("Transit publisher operation canceled because shutdown has begun.", cancellationToken);
            }
        }

        private sealed record PendingSubmission(
            string MessageId,
            ReadOnlyMemory<byte> ArticlePayload,
            TaskCompletionSource<TransitPublishResult> Completion,
            long PublishAsyncEnterTick);

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
