using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Threading;
using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Runtime.Transit
{
    /// <summary>
    /// BackFiller transit front-door and orchestration owner for the global bounded queue model.
    ///
    /// WHY THIS LOOKS DIFFERENT:
    /// The design intentionally makes a single global queue the ownership boundary.
    /// Publisher validates and admits work; connections claim and transmit work.
    /// We intentionally avoid slot queues, materialization reservations, and write-intent ownership paths.
    /// </summary>
    internal sealed partial class TransitPublisher : IAsyncDisposable
    {
        private const int DefaultPerConnectionPipelineDepth = 8;

        private readonly BackFillerRuntimeOptions _runtimeOptions;
        private readonly TimeProvider _timeProvider;
        private readonly ILogger<TransitPublisher> _logger;
        private readonly int _connectionPoolSize;
        private readonly int _perConnectionPipelineDepth;
        private readonly TimeSpan? _connectionResponseProgressTimeout;
        private readonly TimeSpan? _connectionResponseProgressCheckInterval;

        private readonly GlobalTransitWorkQueue _globalQueue;
        private readonly TransitConnection[] _connections;
        private readonly Task[] _connectionWorkers;
        private readonly SemaphoreSlim[] _reconnectGates;
        private readonly CancellationTokenSource _connectionWorkersCancellation = new();
        private readonly ConcurrentDictionary<long, TransitWorkItem> _activeWorkItems = new();
        private readonly ConcurrentDictionary<string, Task> _deferredConnectionDisposals = new(StringComparer.Ordinal);
        private readonly TransitTimingCollector? _timingCollector;

        private long _nextWorkItemId;
        private long _totalBytesTransmitted;
        private long _totalBytesReceived;
        private long _totalArticlesSubmitted;
        private long _totalArticlesAccepted;
        private long _totalArticlesRejected;
        private long _totalArticlesAmbiguous;
        private long _totalArticlesFailed;
        private long _totalArticlesCanceled;
        private long _totalReconnects;

        private int _initialized;
        private volatile bool _disposeRequested;
        private volatile TransitConnectionState _state = TransitConnectionState.Disconnected;

        private static string TraceStamp()
        {
            return $"{DateTimeOffset.UtcNow:O}|tid={Environment.CurrentManagedThreadId}|task={Task.CurrentId?.ToString() ?? "-"}";
        }

        public TransitPublisher(
            BackFillerRuntimeOptions runtimeOptions,
            TimeProvider timeProvider,
            ILogger<TransitPublisher> logger,
            int connectionPoolSize = 1,
            int perConnectionPipelineDepth = DefaultPerConnectionPipelineDepth,
            TimeSpan? connectionResponseProgressTimeout = null,
            TimeSpan? connectionResponseProgressCheckInterval = null,
            TransitTimingCollector? timingCollector = null)
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
            _timingCollector = timingCollector;
            _connectionPoolSize = connectionPoolSize;
            _perConnectionPipelineDepth = perConnectionPipelineDepth;
            _connectionResponseProgressTimeout = connectionResponseProgressTimeout;
            _connectionResponseProgressCheckInterval = connectionResponseProgressCheckInterval;

            _globalQueue = new GlobalTransitWorkQueue(
                maxQueuedItemCount: runtimeOptions.TransitQueueMaxItemCount,
                maxQueuedPayloadBytes: runtimeOptions.TransitQueueMaxPayloadBytes);

            _connections = new TransitConnection[_connectionPoolSize];
            _connectionWorkers = new Task[_connectionPoolSize];
            _reconnectGates = new SemaphoreSlim[_connectionPoolSize];
            for (int i = 0; i < _connectionPoolSize; i++)
            {
                _reconnectGates[i] = new SemaphoreSlim(1, 1);
            }
        }

        internal TransitConnectionState CurrentState => _state;

        internal TransitTimingSnapshot? CaptureTimingSnapshot()
        {
            return _timingCollector?.CaptureSnapshot();
        }

        internal TransitPublisherConnectionDiagnosticsSnapshot CaptureConnectionDiagnosticsSnapshot()
        {
            ConnectionSlotSnapshot[] slots = new ConnectionSlotSnapshot[_connections.Length];
            List<ConnectionDiagnosticsEntry> connectionEntries = new();

            for (int i = 0; i < _connections.Length; i++)
            {
                TransitConnection? connection = _connections[i];
                slots[i] = new ConnectionSlotSnapshot(
                    SlotIndex: i,
                    HasCurrentConnection: connection is not null,
                    CurrentConnectionId: connection?.ConnectionId,
                    TotalSubmissionsRouted: 0,
                    Reconnects: 0,
                    CreatedConnections: connection is null ? 0 : 1,
                    MaxObservedInFlightDepth: 0,
                    WaitedForChannelReadabilityCount: 0,
                    WaitedForCompletionWhilePipelineFullCount: 0,
                    FirstReachedConfiguredDepthTick: 0);

                if (connection is not null)
                {
                    connectionEntries.Add(new ConnectionDiagnosticsEntry(
                        SlotIndex: i,
                        ConnectionId: connection.ConnectionId,
                        Snapshot: connection.CaptureDiagnosticsSnapshot()));
                }
            }

            return new TransitPublisherConnectionDiagnosticsSnapshot(
                ConfiguredConnectionPoolSize: _connectionPoolSize,
                ConfiguredPerConnectionPipelineDepth: _perConnectionPipelineDepth,
                TotalReconnects: Interlocked.Read(ref _totalReconnects),
                QueuedSubmissionCount: _globalQueue.QueuedItemCount,
                Slots: slots,
                Connections: [.. connectionEntries],
                SubmissionTraceRecords: [],
                PublishToConnectionTraceRecords: [],
                PumpFaultTelemetry: null,
                QueueSnapshot: _globalQueue.CaptureSnapshot());
        }

        internal TransitTransportSnapshot CaptureTransportSnapshot(int activeConnections, int outstandingSubmissions)
        {
            return new TransitTransportSnapshot(
                TotalBytesTransmitted: Interlocked.Read(ref _totalBytesTransmitted),
                TotalBytesReceived: Interlocked.Read(ref _totalBytesReceived),
                TotalArticlesSubmitted: Interlocked.Read(ref _totalArticlesSubmitted),
                TotalArticlesAccepted: Interlocked.Read(ref _totalArticlesAccepted),
                TotalArticlesRejected: Interlocked.Read(ref _totalArticlesRejected),
                TotalArticlesAmbiguous: Interlocked.Read(ref _totalArticlesAmbiguous),
                TotalReconnects: Interlocked.Read(ref _totalReconnects),
                ActiveConnections: activeConnections,
                OutstandingSubmissions: outstandingSubmissions);
        }

        internal async Task InitializeAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (_disposeRequested)
            {
                throw new OperationCanceledException("Transit publisher initialization canceled because shutdown has already begun.", cancellationToken);
            }

            if (Interlocked.CompareExchange(ref _initialized, 1, 0) != 0)
            {
                throw new InvalidOperationException("Transit publisher was already initialized.");
            }

            TransitionState(TransitConnectionState.Connecting);

            for (int i = 0; i < _connections.Length; i++)
            {
                int slotIndex = i;
                _connectionWorkers[i] = Task.Run(() => RunConnectionWorkerAsync(slotIndex, _connectionWorkersCancellation.Token), CancellationToken.None);
            }

            foreach (Task worker in _connectionWorkers)
            {
                if (worker is not null && worker.IsFaulted)
                {
                    await worker.ConfigureAwait(false);
                }
            }

            cancellationToken.ThrowIfCancellationRequested();
            if (_disposeRequested)
            {
                throw new OperationCanceledException("Transit publisher initialization canceled because shutdown has already begun.", cancellationToken);
            }

            TransitionState(TransitConnectionState.Ready);
        }

        internal async ValueTask<TransitPublishResult> PublishAsync(
            string messageId,
            ReadOnlyMemory<byte> articlePayload,
            CancellationToken cancellationToken)
        {
            long publishAsyncEnterTick = Stopwatch.GetTimestamp();
            Console.WriteLine($"[TRACE-RI-01] {TraceStamp()} PublishAsync ENTER messageId={messageId}");

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

            if (_disposeRequested || Volatile.Read(ref _initialized) == 0)
            {
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

            long payloadCopyStartTick = Stopwatch.GetTimestamp();
            byte[] payloadCopy = articlePayload.ToArray();
            _timingCollector?.RecordPublishPayloadCopy(Stopwatch.GetTimestamp() - payloadCopyStartTick);

            TransitWorkItem workItem = new(
                workItemId: Interlocked.Increment(ref _nextWorkItemId),
                messageId: messageId,
                payload: payloadCopy,
                maxAttempts: _runtimeOptions.TransitRetryMaxAttempts);
            Console.WriteLine($"[TRACE-RI-02] {TraceStamp()} PublishAsync CREATED workItemId={workItem.WorkItemId} state={workItem.State} maxAttempts={workItem.MaxAttempts}");

            _activeWorkItems[workItem.WorkItemId] = workItem;
            Console.WriteLine($"[TRACE-RI-03] {TraceStamp()} PublishAsync ACTIVE-ADD workItemId={workItem.WorkItemId} activeCount={_activeWorkItems.Count} state={workItem.State}");

            try
            {
                await _globalQueue.EnqueueAsync(workItem, cancellationToken).ConfigureAwait(false);
                Interlocked.Increment(ref _totalArticlesSubmitted);
                Console.WriteLine($"[TRACE-RI-04] {TraceStamp()} PublishAsync ENQUEUED workItemId={workItem.WorkItemId} state={workItem.State} totalSubmitted={Interlocked.Read(ref _totalArticlesSubmitted)}");
            }
            catch (OperationCanceledException)
            {
                _activeWorkItems.TryRemove(workItem.WorkItemId, out _);
                throw;
            }
            catch (Exception)
            {
                _activeWorkItems.TryRemove(workItem.WorkItemId, out _);
                throw;
            }

            if (!cancellationToken.CanBeCanceled)
            {
                Console.WriteLine($"[TRACE-RI-05] {TraceStamp()} PublishAsync COMPLETIONTASK-OBTAINED workItemId={workItem.WorkItemId} taskId={workItem.CompletionTask.Id} canCancel=false isCompleted={workItem.CompletionTask.IsCompleted}");
                TransitPublishResult completed = await workItem.CompletionTask.ConfigureAwait(false);
                Console.WriteLine($"[TRACE-RI-06] {TraceStamp()} PublishAsync COMPLETIONTASK-COMPLETED workItemId={workItem.WorkItemId} taskId={workItem.CompletionTask.Id} status={completed.Status}");
                TransitPublishResult tracedResult = completed with
                {
                    T0PublishAsyncEnterTick = publishAsyncEnterTick,
                    T7PublishAsyncCompleteTick = Stopwatch.GetTimestamp(),
                };
                Console.WriteLine($"[TRACE-RI-07] {TraceStamp()} PublishAsync RETURN workItemId={workItem.WorkItemId} status={tracedResult.Status}");
                return tracedResult;
            }

            Task<TransitPublishResult> completionTask = workItem.CompletionTask;
            Console.WriteLine($"[TRACE-RI-05] {TraceStamp()} PublishAsync COMPLETIONTASK-OBTAINED workItemId={workItem.WorkItemId} taskId={completionTask.Id} canCancel=true isCompleted={completionTask.IsCompleted}");
            Task cancellationTask = Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            Task completedTask = await Task.WhenAny(completionTask, cancellationTask).ConfigureAwait(false);

            if (ReferenceEquals(completedTask, completionTask))
            {
                TransitPublishResult result = await completionTask.ConfigureAwait(false);
                Console.WriteLine($"[TRACE-RI-06] {TraceStamp()} PublishAsync COMPLETIONTASK-COMPLETED workItemId={workItem.WorkItemId} taskId={completionTask.Id} status={result.Status}");
                TransitPublishResult tracedResult = result with
                {
                    T0PublishAsyncEnterTick = publishAsyncEnterTick,
                    T7PublishAsyncCompleteTick = Stopwatch.GetTimestamp(),
                };
                Console.WriteLine($"[TRACE-RI-07] {TraceStamp()} PublishAsync RETURN workItemId={workItem.WorkItemId} status={tracedResult.Status}");
                return tracedResult;
            }

            workItem.MarkCancelRequested();
            Console.WriteLine($"[TRACE-RI-08] {TraceStamp()} PublishAsync CANCELED-BEFORE-COMPLETION workItemId={workItem.WorkItemId} taskId={completionTask.Id} completionIsCompleted={completionTask.IsCompleted} itemTerminal={workItem.IsTerminal}");
            throw new OperationCanceledException("Transit publish canceled.", cancellationToken);
        }

        internal void MarkSubmissionPumpFaultMeasurementWindow(long measurementStartStopwatchTick, long measurementEndStopwatchTick, bool measurementBoundaryObserved)
        {
        }

        internal void MarkSubmissionPumpFaultProducerCompletion(bool allProducersCompleted)
        {
        }

        internal void MarkSubmissionPumpFaultDispatchersCompleted(bool dispatchersCompleted)
        {
        }

        internal PumpFaultTelemetrySnapshot? CaptureSubmissionPumpFaultTelemetrySnapshot()
        {
            return null;
        }

        internal SubmissionPumpFaultCounts CaptureSubmissionPumpFaultCounts()
        {
            return new SubmissionPumpFaultCounts(
                TotalFaultCount: 0,
                InitiatingFaultCount: 0,
                CascadeFaultCount: 0);
        }

        internal TransitConnection.P1GreetingProvenanceSnapshot? CaptureFirstP1GreetingProvenanceSnapshot()
        {
            foreach (TransitConnection connection in _connections)
            {
                TransitConnection.P1GreetingProvenanceSnapshot? snapshot = connection?.CaptureFirstP1GreetingProvenanceSnapshot();
                if (snapshot is not null)
                {
                    return snapshot;
                }
            }

            return null;
        }

        internal async Task PreemptSubmissionProcessingAsync(CancellationToken cancellationToken)
        {
            _globalQueue.FreezeAdmission();
            _connectionWorkersCancellation.Cancel();

            await ForceTerminalizeRemainingWorkAsync().ConfigureAwait(false);

            using CancellationTokenSource workerWaitCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            workerWaitCts.CancelAfter(_runtimeOptions.EffectiveTransitShutdownDrainInactivityWatchdog);

            bool workerWaitTimedOut = false;
            foreach (Task worker in _connectionWorkers)
            {
                try
                {
                    await worker.WaitAsync(workerWaitCts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (OperationCanceledException) when (workerWaitCts.IsCancellationRequested)
                {
                    workerWaitTimedOut = true;
                    break;
                }
                catch
                {
                }
            }

            if (workerWaitTimedOut)
            {
                Console.WriteLine("[SHUTDOWN-DIAG] Worker preemption wait timed out; continuing with forced terminalization.");
            }

            await ForceTerminalizeRemainingWorkAsync().ConfigureAwait(false);
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposeRequested)
            {
                return;
            }

            _disposeRequested = true;
            _globalQueue.FreezeAdmission();

            _connectionWorkersCancellation.Cancel();
            await ForceTerminalizeRemainingWorkAsync().ConfigureAwait(false);

            DateTimeOffset shutdownStart = _timeProvider.GetUtcNow();
            DateTimeOffset graceEnd = shutdownStart.Add(_runtimeOptions.EffectiveTransitShutdownDrainGracePeriod);
            DateTimeOffset inactivityEnd = shutdownStart.Add(_runtimeOptions.EffectiveTransitShutdownDrainInactivityWatchdog);
            DateTimeOffset absoluteEnd = shutdownStart.Add(_runtimeOptions.EffectiveTransitShutdownAbsoluteMaximum);

            GlobalTransitWorkQueueSnapshot previous = _globalQueue.CaptureSnapshot();
            long previousCompleted = Interlocked.Read(ref _totalArticlesAccepted)
                + Interlocked.Read(ref _totalArticlesRejected)
                + Interlocked.Read(ref _totalArticlesAmbiguous)
                + Interlocked.Read(ref _totalArticlesFailed)
                + Interlocked.Read(ref _totalArticlesCanceled);

            while (true)
            {
                GlobalTransitWorkQueueSnapshot current = _globalQueue.CaptureSnapshot();
                bool drained = current.QueuedItemCount == 0 && current.RetryPendingCount == 0 && current.InFlightCount == 0;
                bool allActiveTerminal = _activeWorkItems.Values.All(static item => item.IsTerminal);
                if (drained || allActiveTerminal)
                {
                    break;
                }

                DateTimeOffset now = _timeProvider.GetUtcNow();
                if (now >= absoluteEnd)
                {
                    break;
                }

                long completedNow = Interlocked.Read(ref _totalArticlesAccepted)
                    + Interlocked.Read(ref _totalArticlesRejected)
                    + Interlocked.Read(ref _totalArticlesAmbiguous)
                    + Interlocked.Read(ref _totalArticlesFailed)
                    + Interlocked.Read(ref _totalArticlesCanceled);

                bool forwardProgress = completedNow > previousCompleted
                    || current.QueuedItemCount < previous.QueuedItemCount
                    || current.RetryPendingCount < previous.RetryPendingCount
                    || current.InFlightCount < previous.InFlightCount;

                if (forwardProgress)
                {
                    inactivityEnd = now.Add(_runtimeOptions.EffectiveTransitShutdownDrainInactivityWatchdog);
                }

                if (now >= graceEnd && now >= inactivityEnd)
                {
                    break;
                }

                previous = current;
                previousCompleted = completedNow;
                await Task.Delay(TimeSpan.FromMilliseconds(100), CancellationToken.None).ConfigureAwait(false);
            }
            foreach (Task worker in _connectionWorkers)
            {
                TimeSpan remaining = absoluteEnd - _timeProvider.GetUtcNow();
                if (remaining <= TimeSpan.Zero)
                {
                    Console.WriteLine("[SHUTDOWN-DIAG] Publisher dispose reached absolute shutdown deadline while awaiting connection workers.");
                    break;
                }

                try
                {
                    await worker.WaitAsync(remaining).ConfigureAwait(false);
                }
                catch (TimeoutException)
                {
                    Console.WriteLine("[SHUTDOWN-DIAG] Publisher dispose timed out while awaiting connection workers; continuing teardown.");
                    break;
                }
                catch (OperationCanceledException)
                {
                }
                catch
                {
                }
            }

            foreach (TransitConnection? connection in _connections)
            {
                if (connection is null)
                {
                    continue;
                }

                try
                {
                    await TrackDeferredConnectionDisposal(connection).ConfigureAwait(false);
                }
                catch
                {
                }
            }

            await AwaitDeferredConnectionDisposalsAsync().ConfigureAwait(false);

            _connectionWorkersCancellation.Dispose();
            TransitionState(TransitConnectionState.Disconnected);
        }

        private async Task RunConnectionWorkerAsync(int slotIndex, CancellationToken cancellationToken)
        {
            Console.WriteLine($"[TRACE-RI-10] {TraceStamp()} Worker START slot={slotIndex}");
            TransitConnection? connection = null;

            while (!cancellationToken.IsCancellationRequested)
            {
                List<TransitWorkItem>? claimed = null;

                try
                {
                    await _globalQueue.DrainEligibleRetriesAsync(cancellationToken).ConfigureAwait(false);

                    bool hasWork = await _globalQueue.WaitForWorkAsync(cancellationToken).ConfigureAwait(false);
                    Console.WriteLine($"[TRACE-RI-12] {TraceStamp()} Worker WAIT-FOR-WORK slot={slotIndex} connectionId={(connection?.ConnectionId ?? "none")} hasWork={hasWork}");
                    if (!hasWork)
                    {
                        continue;
                    }

                    if (connection is null)
                    {
                        connection = await CreateAndInitializeConnectionAsync(slotIndex, reconnecting: false, cancellationToken).ConfigureAwait(false);
                        Console.WriteLine($"[TRACE-RI-11] {TraceStamp()} Worker INITIAL-CONNECTION-READY slot={slotIndex} connectionId={connection.ConnectionId} state={connection.CurrentState}");
                        _connections[slotIndex] = connection;
                    }

                    if (connection.CurrentState == TransitConnectionState.Faulted)
                    {
                        throw new IOException("Transit connection faulted before claiming new work.");
                    }

                    connection.ThrowIfResponseLoopFaulted();

                    claimed = new List<TransitWorkItem>(connection.PipelineDepth);
                    for (int i = 0; i < connection.PipelineDepth; i++)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        if (connection.CurrentState == TransitConnectionState.Faulted)
                        {
                            throw new IOException("Transit connection faulted before claiming new work.");
                        }

                        connection.ThrowIfResponseLoopFaulted();

                        if (!_globalQueue.TryClaim(connection.ConnectionId, out TransitWorkItem? item) || item is null)
                        {
                            break;
                        }

                        claimed.Add(item);
                        cancellationToken.ThrowIfCancellationRequested();
                    }

                    if (claimed.Count == 0)
                    {
                        continue;
                    }

                    Console.WriteLine($"[TRACE-RI-13] {TraceStamp()} Worker CLAIMED slot={slotIndex} connectionId={connection.ConnectionId} claimedCount={claimed.Count} items=[{string.Join(",", claimed.Select(static x => $"{x.WorkItemId}:{x.State}"))}]");
                    await connection.ProcessBatchAsync(claimed, cancellationToken).ConfigureAwait(false);

                    int remainingCompletions = claimed.Count;
                    while (remainingCompletions > 0)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        while (connection.TryTakeCompleted(out TransitWorkItem completedItem, out TransitPublishResult result))
                        {
                            CompleteTerminal(completedItem, result);
                            remainingCompletions--;
                            if (remainingCompletions == 0)
                            {
                                break;
                            }
                        }

                        if (remainingCompletions == 0)
                        {
                            break;
                        }

                        try
                        {
                            connection.ThrowIfResponseLoopFaulted();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[TRACE-RI-14] {TraceStamp()} Worker THROWIFRESPONSELOOPFAULTED-THREW slot={slotIndex} connectionId={connection.ConnectionId} exType={ex.GetType().FullName} exMessage={ex.Message}");
                            throw;
                        }

                        _ = await connection.WaitForCompletedAsync(cancellationToken).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    await RequeueClaimedAndOutstandingAfterFaultAsync(connection, claimed, CancellationToken.None).ConfigureAwait(false);
                    _ = TrackDeferredConnectionDisposal(connection);
                    break;
                }
                catch (Exception ex) when (connection is not null && IsConnectionLifecycleSubmitFailure(connection, ex))
                {
                    Console.WriteLine($"[TRACE-RI-15] {TraceStamp()} Worker LIFECYCLE-CATCH-ENTER slot={slotIndex} connectionId={connection.ConnectionId} exType={ex.GetType().FullName} exMessage={ex.Message}");
                    await RequeueClaimedAndOutstandingAfterFaultAsync(connection, claimed, cancellationToken).ConfigureAwait(false);
                    Console.WriteLine($"[TRACE-RI-16] {TraceStamp()} Worker REQUEUE-COMPLETE slot={slotIndex} connectionId={connection.ConnectionId}");

                    bool shutdownActive = _disposeRequested || cancellationToken.IsCancellationRequested;
                    _ = TrackDeferredConnectionDisposal(connection);
                    Console.WriteLine($"[TRACE-RI-17] {TraceStamp()} Worker DEFER-DISPOSE-SCHEDULED slot={slotIndex} connectionId={connection.ConnectionId} shutdownActive={shutdownActive}");
                    if (shutdownActive)
                    {
                        break;
                    }

                    if (_disposeRequested || cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }

                    if (!HasConnectionDemand())
                    {
                        connection = null;
                        _connections[slotIndex] = null;
                        continue;
                    }

                    TransitConnection reconnectTarget = connection;
                    SemaphoreSlim reconnectGate = _reconnectGates[slotIndex];
                    await reconnectGate.WaitAsync(cancellationToken).ConfigureAwait(false);
                    try
                    {
                        TransitConnection? replacement = _connections[slotIndex];
                        if (replacement is not null && !ReferenceEquals(replacement, reconnectTarget))
                        {
                            Console.WriteLine($"[TRACE-RI-18] {TraceStamp()} Worker RECONNECT-SKIP-EXTERNAL slot={slotIndex} priorConnectionId={reconnectTarget.ConnectionId} replacementConnectionId={replacement.ConnectionId}");
                            connection = replacement;
                            continue;
                        }

                        long reconnects = Interlocked.Increment(ref _totalReconnects);
                        Console.WriteLine($"[TRACE-RI-18] {TraceStamp()} Worker RECONNECT-START slot={slotIndex} priorConnectionId={reconnectTarget.ConnectionId} totalReconnects={reconnects}");
                        try
                        {
                            connection = await CreateAndInitializeConnectionAsync(slotIndex, reconnecting: true, cancellationToken).ConfigureAwait(false);
                        }
                        catch (NoConnectionDemandException)
                        {
                            connection = null;
                            _connections[slotIndex] = null;
                            continue;
                        }
                        Console.WriteLine($"[TRACE-RI-19] {TraceStamp()} Worker RECONNECT-READY slot={slotIndex} connectionId={connection.ConnectionId} state={connection.CurrentState}");
                        _connections[slotIndex] = connection;
                    }
                    finally
                    {
                        reconnectGate.Release();
                    }
                }
            }
        }

        private async Task ReconnectAsync(int slotIndex, CancellationToken cancellationToken)
        {
            if ((uint)slotIndex >= (uint)_connections.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(slotIndex), slotIndex, "Connection slot index is out of range.");
            }

            SemaphoreSlim reconnectGate = _reconnectGates[slotIndex];
            await reconnectGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                TransitConnection? current = _connections[slotIndex];
                if (current is null)
                {
                    throw new InvalidOperationException("Cannot reconnect because no active connection exists for the slot.");
                }

                await RequeueClaimedAndOutstandingAfterFaultAsync(current, claimed: null, cancellationToken).ConfigureAwait(false);
                _ = TrackDeferredConnectionDisposal(current);

                if (_disposeRequested || cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                Interlocked.Increment(ref _totalReconnects);
                TransitConnection replacement = await CreateAndInitializeConnectionAsync(slotIndex, reconnecting: true, cancellationToken).ConfigureAwait(false);
                _connections[slotIndex] = replacement;
            }
            finally
            {
                reconnectGate.Release();
            }
        }

        private async Task RequeueClaimedAndOutstandingAfterFaultAsync(
            TransitConnection connection,
            IReadOnlyList<TransitWorkItem>? claimed,
            CancellationToken cancellationToken)
        {
            Console.WriteLine($"[TRACE-RI-20] {TraceStamp()} Requeue BEGIN connectionId={connection.ConnectionId} claimedCount={(claimed is null ? 0 : claimed.Count)}");
            while (connection.TryTakeCompleted(out TransitWorkItem completedItem, out TransitPublishResult completedResult))
            {
                Console.WriteLine($"[TRACE-RI-21] {TraceStamp()} Requeue DRAIN-COMPLETED connectionId={connection.ConnectionId} workItemId={completedItem.WorkItemId} status={completedResult.Status}");
                CompleteTerminal(completedItem, completedResult);
            }

            Dictionary<long, TransitWorkItem> unresolvedById = new();

            IReadOnlyList<TransitWorkItem> unresolvedOwned = connection.DrainOutstandingOwnedWorkForRetry();
            Console.WriteLine($"[TRACE-RI-22] {TraceStamp()} Requeue DRAIN-OWNED connectionId={connection.ConnectionId} unresolvedOwnedCount={unresolvedOwned.Count}");
            foreach (TransitWorkItem item in unresolvedOwned)
            {
                unresolvedById[item.WorkItemId] = item;
            }

            if (claimed is not null)
            {
                foreach (TransitWorkItem item in claimed)
                {
                    if (!item.IsTerminal)
                    {
                        unresolvedById[item.WorkItemId] = item;
                    }
                }
            }

            foreach (TransitWorkItem item in unresolvedById.Values)
            {
                Console.WriteLine($"[TRACE-RI-23] {TraceStamp()} Requeue ITEM connectionId={connection.ConnectionId} workItemId={item.WorkItemId} stateBefore={item.State} attempts={item.AttemptCount}");
                await RequeueOrTerminalizeFailureAsync(
                    item,
                    TransitWorkFailureClass.ConnectionDisposed,
                    TransitTransmissionUncertainty.ConnectionFailedDuringSend,
                    cancellationToken).ConfigureAwait(false);
                Console.WriteLine($"[TRACE-RI-24] {TraceStamp()} Requeue ITEM-DONE connectionId={connection.ConnectionId} workItemId={item.WorkItemId} stateAfter={item.State} terminal={item.IsTerminal}");
            }

            Console.WriteLine($"[TRACE-RI-25] {TraceStamp()} Requeue END connectionId={connection.ConnectionId} unresolvedCount={unresolvedById.Count}");
        }

        private async ValueTask RequeueOrTerminalizeFailureAsync(
            TransitWorkItem item,
            TransitWorkFailureClass failureClass,
            TransitTransmissionUncertainty uncertainty,
            CancellationToken cancellationToken)
        {
            if (uncertainty == TransitTransmissionUncertainty.ConnectionFailedDuringSend
                && item.State is TransitWorkItemState.Claimed
                    or TransitWorkItemState.Staged
                    or TransitWorkItemState.Flushed
                    or TransitWorkItemState.AwaitingResponse)
            {
                TransitPublishResult ambiguous = new(
                    MessageId: item.MessageId,
                    Status: TransitPublishStatus.Ambiguous,
                    ResponseCode: null,
                    ResponseText: "Transit connection closed before definitive TAKETHIS response.",
                    Provenance: TransitPublishProvenance.ConnectionClose,
                    ProvenanceConnectionState: _state,
                    ProvenanceTick: Stopwatch.GetTimestamp());

                CompleteTerminal(item, ambiguous);
                Console.WriteLine($"[TRACE-RI-28] {TraceStamp()} RequeueOrTerminalize TERMINAL-AMBIGUOUS workItemId={item.WorkItemId} state={item.State}");
                return;
            }

            if (item.State == TransitWorkItemState.RetryPending)
            {
                Console.WriteLine($"[TRACE-RI-28] {TraceStamp()} RequeueOrTerminalize ALREADY-RETRYPENDING workItemId={item.WorkItemId}");
                return;
            }

            TimeSpan delay = ComputeRetryDelay(item.AttemptCount);
            Console.WriteLine($"[TRACE-RI-26] {TraceStamp()} RequeueOrTerminalize START workItemId={item.WorkItemId} state={item.State} attempts={item.AttemptCount} delayMs={delay.TotalMilliseconds:F0}");
            bool transferOwnershipFromInFlight = item.State is TransitWorkItemState.Claimed
                or TransitWorkItemState.Staged
                or TransitWorkItemState.Flushed
                or TransitWorkItemState.AwaitingResponse;

            bool scheduled = await _globalQueue.ScheduleRetryAsync(
                item,
                failureClass,
                uncertainty,
                delay,
                transferOwnershipFromInFlight,
                cancellationToken).ConfigureAwait(false);
            Console.WriteLine($"[TRACE-RI-27] {TraceStamp()} RequeueOrTerminalize SCHEDULE-RESULT workItemId={item.WorkItemId} scheduled={scheduled} state={item.State} terminal={item.IsTerminal}");

            if (scheduled)
            {
                return;
            }

            TransitPublishResult failed = new(
                MessageId: item.MessageId,
                Status: TransitPublishStatus.Failed,
                ResponseCode: null,
                ResponseText: "Transmission retry budget exhausted.",
                Provenance: TransitPublishProvenance.Failed,
                ProvenanceConnectionState: _state,
                ProvenanceTick: Stopwatch.GetTimestamp());

            CompleteTerminal(item, failed, inFlightOwnershipAlreadyTransferred: true);
            Console.WriteLine($"[TRACE-RI-28] {TraceStamp()} RequeueOrTerminalize TERMINAL-FAILED workItemId={item.WorkItemId} state={item.State}");
        }

        private void CompleteTerminal(TransitWorkItem item, TransitPublishResult result, bool inFlightOwnershipAlreadyTransferred = false)
        {
            bool transitioned = item.TryTransitionToTerminal(result.Status, result.Provenance, out TransitWorkItemState priorState);
            Console.WriteLine($"[TRACE-RI-29] {TraceStamp()} CompleteTerminal TRY-COMPLETE workItemId={item.WorkItemId} status={result.Status} priorState={priorState} completed={transitioned} completionTaskCompleted={item.CompletionTask.IsCompleted}");
            if (!transitioned)
            {
                return;
            }

            if (!inFlightOwnershipAlreadyTransferred && priorState is TransitWorkItemState.Claimed
                or TransitWorkItemState.Staged
                or TransitWorkItemState.Flushed
                or TransitWorkItemState.AwaitingResponse)
            {
                _globalQueue.MarkInFlightTerminal();
            }

            bool removed = _activeWorkItems.TryRemove(item.WorkItemId, out _);
            Console.WriteLine($"[TRACE-RI-30] {TraceStamp()} CompleteTerminal ACTIVE-REMOVE workItemId={item.WorkItemId} removed={removed} activeCount={_activeWorkItems.Count}");

            switch (result.Status)
            {
                case TransitPublishStatus.Accepted:
                    Interlocked.Increment(ref _totalArticlesAccepted);
                    break;
                case TransitPublishStatus.Rejected:
                    Interlocked.Increment(ref _totalArticlesRejected);
                    break;
                case TransitPublishStatus.Canceled:
                    Interlocked.Increment(ref _totalArticlesCanceled);
                    break;
                case TransitPublishStatus.Ambiguous:
                    Interlocked.Increment(ref _totalArticlesAmbiguous);
                    break;
                default:
                    Interlocked.Increment(ref _totalArticlesFailed);
                    break;
            }

            _ = item.TrySetCompletionResult(result);
        }

        private async Task ForceTerminalizeRemainingWorkAsync()
        {
            TransitWorkItem[] remaining = _activeWorkItems.Values.ToArray();
            Console.WriteLine($"[TRACE-RI-31] {TraceStamp()} ForceTerminalize ENTER remainingCount={remaining.Length}");
            foreach (TransitWorkItem item in remaining)
            {
                TransitPublishResult forced = new(
                    MessageId: item.MessageId,
                    Status: item.CancelRequested ? TransitPublishStatus.Canceled : TransitPublishStatus.Ambiguous,
                    ResponseCode: null,
                    ResponseText: "Shutdown deadline reached before terminal completion.",
                    Provenance: TransitPublishProvenance.Shutdown,
                    ProvenanceConnectionState: _state,
                    ProvenanceTick: Stopwatch.GetTimestamp());

                TransitWorkItemState stateBefore = item.State;
                bool transitioned = item.TryTransitionToTerminal(forced.Status, forced.Provenance, out TransitWorkItemState priorState);
                Console.WriteLine($"[TRACE-RI-32] {TraceStamp()} ForceTerminalize TRY-COMPLETE workItemId={item.WorkItemId} stateBefore={stateBefore} priorState={priorState} forcedStatus={forced.Status} completed={transitioned} completionTaskCompleted={item.CompletionTask.IsCompleted}");
                if (!transitioned)
                {
                    continue;
                }

                switch (priorState)
                {
                    case TransitWorkItemState.Queued:
                        _globalQueue.MarkQueuedTerminal(item.PayloadBytes);
                        break;
                    case TransitWorkItemState.RetryPending:
                        _globalQueue.MarkRetryPendingTerminal();
                        break;
                    case TransitWorkItemState.Claimed:
                    case TransitWorkItemState.Staged:
                    case TransitWorkItemState.Flushed:
                    case TransitWorkItemState.AwaitingResponse:
                        _globalQueue.MarkInFlightTerminal();
                        break;
                }

                bool removed = _activeWorkItems.TryRemove(item.WorkItemId, out _);
                Console.WriteLine($"[TRACE-RI-33] {TraceStamp()} ForceTerminalize ACTIVE-REMOVE workItemId={item.WorkItemId} removed={removed} activeCount={_activeWorkItems.Count}");

                switch (forced.Status)
                {
                    case TransitPublishStatus.Accepted:
                        Interlocked.Increment(ref _totalArticlesAccepted);
                        break;
                    case TransitPublishStatus.Rejected:
                        Interlocked.Increment(ref _totalArticlesRejected);
                        break;
                    case TransitPublishStatus.Canceled:
                        Interlocked.Increment(ref _totalArticlesCanceled);
                        break;
                    case TransitPublishStatus.Ambiguous:
                        Interlocked.Increment(ref _totalArticlesAmbiguous);
                        break;
                    default:
                        Interlocked.Increment(ref _totalArticlesFailed);
                        break;
                }

                _ = item.TrySetCompletionResult(forced);
            }

            Console.WriteLine($"[TRACE-RI-34] {TraceStamp()} ForceTerminalize EXIT");
            await Task.CompletedTask.ConfigureAwait(false);
        }

        private async Task<TransitConnection> CreateAndInitializeConnectionAsync(int slotIndex, bool reconnecting, CancellationToken cancellationToken)
        {
            int consecutiveLifecycleInitializationFailures = 0;
            int attempt = 0;

            while (true)
            {
                if (reconnecting && !HasConnectionDemand())
                {
                    throw new NoConnectionDemandException();
                }

                attempt++;
                Console.WriteLine($"[TRACE-RI-35] {TraceStamp()} InitLoop ATTEMPT-START slot={slotIndex} reconnecting={reconnecting} attempt={attempt} failureCount={consecutiveLifecycleInitializationFailures}");
                cancellationToken.ThrowIfCancellationRequested();
                if (_disposeRequested)
                {
                    throw new OperationCanceledException("Transit publisher shutdown in progress.", cancellationToken);
                }

                bool hasOutstandingAdmittedWork = HasOutstandingAdmittedWork();
                TimeSpan? initializationResponseProgressTimeout = ResolveInitializationResponseProgressTimeout(reconnecting, hasOutstandingAdmittedWork);

                TransitConnection connection = new(
                    host: _runtimeOptions.TransitServerHost,
                    port: _runtimeOptions.TransitServerPort,
                    useSsl: _runtimeOptions.TransitServerUseSsl,
                    logger: _logger,
                    perConnectionPipelineDepth: _perConnectionPipelineDepth,
                    responseProgressTimeout: initializationResponseProgressTimeout,
                    responseProgressCheckInterval: _connectionResponseProgressCheckInterval,
                    timingCollector: _timingCollector);
                Console.WriteLine($"[TRACE-RI-36] {TraceStamp()} InitLoop CONNECTION-CREATED slot={slotIndex} reconnecting={reconnecting} attempt={attempt} connectionId={connection.ConnectionId} timeoutMs={(initializationResponseProgressTimeout?.TotalMilliseconds.ToString("F0") ?? "null")} hasOutstanding={hasOutstandingAdmittedWork}");

                try
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (_disposeRequested)
                    {
                        throw new OperationCanceledException("Transit publisher shutdown in progress.", cancellationToken);
                    }

                    Console.WriteLine($"[TRACE-RI-37] {TraceStamp()} InitLoop INITIALIZE-START slot={slotIndex} reconnecting={reconnecting} attempt={attempt} connectionId={connection.ConnectionId}");
                    await connection.InitializeAsync(cancellationToken).ConfigureAwait(false);
                    Console.WriteLine($"[TRACE-RI-38] {TraceStamp()} InitLoop INITIALIZE-SUCCESS slot={slotIndex} reconnecting={reconnecting} attempt={attempt} connectionId={connection.ConnectionId} state={connection.CurrentState}");
                    return connection;
                }
                catch (Exception ex) when (IsConnectionLifecycleSubmitFailure(connection, ex))
                {
                    Console.WriteLine($"[TRACE-RI-39] {TraceStamp()} InitLoop INITIALIZE-EXCEPTION slot={slotIndex} reconnecting={reconnecting} attempt={attempt} connectionId={connection.ConnectionId} exType={ex.GetType().FullName} exMessage={ex.Message}");
                    try
                    {
                        Console.WriteLine($"[TRACE-RI-40] {TraceStamp()} InitLoop DISPOSE-FAILED-CONNECTION-START slot={slotIndex} attempt={attempt} connectionId={connection.ConnectionId}");
                        await connection.DisposeAsync().ConfigureAwait(false);
                        Console.WriteLine($"[TRACE-RI-41] {TraceStamp()} InitLoop DISPOSE-FAILED-CONNECTION-END slot={slotIndex} attempt={attempt} connectionId={connection.ConnectionId}");
                    }
                    catch (Exception disposeEx)
                    {
                        Console.WriteLine($"[TRACE-RI-42] {TraceStamp()} InitLoop DISPOSE-FAILED-CONNECTION-EXCEPTION slot={slotIndex} attempt={attempt} connectionId={connection.ConnectionId} exType={disposeEx.GetType().FullName} exMessage={disposeEx.Message}");
                    }

                    cancellationToken.ThrowIfCancellationRequested();
                    if (_disposeRequested)
                    {
                        throw new OperationCanceledException("Transit publisher shutdown in progress.", cancellationToken);
                    }

                    bool hasOutstandingNow = HasOutstandingAdmittedWork();
                    Console.WriteLine($"[TRACE-RI-43] {TraceStamp()} InitLoop FAILURE-COUNT-BEFORE slot={slotIndex} reconnecting={reconnecting} attempt={attempt} failureCount={consecutiveLifecycleInitializationFailures} hasOutstanding={hasOutstandingNow} threshold={_runtimeOptions.TransitRetryMaxAttempts}");
                    Interlocked.Increment(ref _totalReconnects);
                    consecutiveLifecycleInitializationFailures++;
                    bool thresholdReached = hasOutstandingNow && consecutiveLifecycleInitializationFailures >= _runtimeOptions.TransitRetryMaxAttempts;
                    Console.WriteLine($"[TRACE-RI-44] {TraceStamp()} InitLoop FAILURE-COUNT-AFTER slot={slotIndex} reconnecting={reconnecting} attempt={attempt} failureCount={consecutiveLifecycleInitializationFailures} thresholdReached={thresholdReached}");

                    if (thresholdReached)
                    {
                        Console.WriteLine($"[TRACE-RI-45] {TraceStamp()} InitLoop FORCE-TERMINALIZE-ENTER slot={slotIndex} reconnecting={reconnecting} attempt={attempt}");
                        await ForceTerminalizeRemainingWorkAsync().ConfigureAwait(false);
                        Console.WriteLine($"[TRACE-RI-46] {TraceStamp()} InitLoop FORCE-TERMINALIZE-EXIT slot={slotIndex} reconnecting={reconnecting} attempt={attempt}");
                        consecutiveLifecycleInitializationFailures = 0;
                        Console.WriteLine($"[TRACE-RI-47] {TraceStamp()} InitLoop FAILURE-COUNT-RESET slot={slotIndex} reconnecting={reconnecting} attempt={attempt} failureCount={consecutiveLifecycleInitializationFailures}");
                    }

                    Console.WriteLine($"[TRACE-RI-48] {TraceStamp()} InitLoop RETRY-DELAY-START slot={slotIndex} reconnecting={reconnecting} attempt={attempt} delayMs=250");
                    await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken).ConfigureAwait(false);
                    Console.WriteLine($"[TRACE-RI-49] {TraceStamp()} InitLoop RETRY-DELAY-END slot={slotIndex} reconnecting={reconnecting} attempt={attempt}");
                }
            }
        }

        private bool HasOutstandingAdmittedWork()
        {
            return _activeWorkItems.Values.Any(static item => !item.IsTerminal);
        }

        private bool HasConnectionDemand()
        {
            GlobalTransitWorkQueueSnapshot snapshot = _globalQueue.CaptureSnapshot();
            return snapshot.QueuedItemCount > 0
                || snapshot.RetryPendingCount > 0
                || snapshot.InFlightCount > 0
                || HasOutstandingAdmittedWork();
        }

        private TimeSpan? ResolveInitializationResponseProgressTimeout(bool reconnecting, bool hasOutstandingAdmittedWork)
        {
            if (reconnecting || hasOutstandingAdmittedWork)
            {
                return _runtimeOptions.EffectiveTransitReconnectInitializationTimeout;
            }

            return _connectionResponseProgressTimeout;
        }

        private static TimeSpan ComputeRetryDelay(int attempt)
        {
            int boundedAttempt = Math.Clamp(attempt, 1, 3);
            int exponentialMs = boundedAttempt switch
            {
                1 => 100,
                2 => 250,
                _ => 500,
            };

            int jitterMs = RandomNumberGenerator.GetInt32(0, 100);
            int delayMs = Math.Min(2_000, exponentialMs + jitterMs);
            return TimeSpan.FromMilliseconds(delayMs);
        }

        private void TransitionState(TransitConnectionState state)
        {
            _state = state;
        }

        private sealed class NoConnectionDemandException : OperationCanceledException
        {
        }

        private static bool IsConnectionLifecycleSubmitFailure(TransitConnection connection, Exception exception)
        {
            ArgumentNullException.ThrowIfNull(connection);
            ArgumentNullException.ThrowIfNull(exception);

            bool result;
            if (exception is TransitConnection.TransitConnectionLifecycleException)
            {
                result = true;
            }
            else if (connection.CurrentState == TransitConnectionState.Faulted || connection.IsResponseLoopFaulted)
            {
                result = exception is IOException
                    or ObjectDisposedException
                    or SocketException
                    or TimeoutException
                    or System.Threading.Channels.ChannelClosedException;
            }
            else
            {
                result = exception is IOException
                    or ObjectDisposedException
                    or SocketException
                    || (exception is InvalidOperationException invalid
                        && (IsInitializationProtocolFailure(connection, invalid)
                            || invalid.Message.Contains("connection", StringComparison.OrdinalIgnoreCase)
                            || invalid.Message.Contains("Duplicate in-flight Message-ID on same connection.", StringComparison.Ordinal)));
            }

            Console.WriteLine($"[TRACE-RI-50] {TraceStamp()} LifecycleFilter connectionId={connection.ConnectionId} state={connection.CurrentState} responseLoopFaulted={connection.IsResponseLoopFaulted} exType={exception.GetType().FullName} exMessage={exception.Message} result={result}");
            return result;
        }

        private static bool IsInitializationProtocolFailure(TransitConnection connection, InvalidOperationException exception)
        {
            ArgumentNullException.ThrowIfNull(connection);
            ArgumentNullException.ThrowIfNull(exception);

            if (connection.CurrentState is not (TransitConnectionState.AwaitingGreeting
                or TransitConnectionState.CapabilitiesNegotiation
                or TransitConnectionState.StartingStreaming))
            {
                return false;
            }

            string message = exception.Message;
            return message.Contains("greeting response code", StringComparison.OrdinalIgnoreCase)
                || message.Contains("CAPABILITIES", StringComparison.Ordinal)
                || message.Contains("MODE STREAM", StringComparison.Ordinal)
                || message.Contains("NNTP response", StringComparison.OrdinalIgnoreCase)
                || message.Contains("STREAMING capability", StringComparison.OrdinalIgnoreCase);
        }

        private Task TrackDeferredConnectionDisposal(TransitConnection connection)
        {
            ArgumentNullException.ThrowIfNull(connection);

            Task disposeTask = connection.DisposeAsync().AsTask();
            _deferredConnectionDisposals[connection.ConnectionId] = disposeTask;
            return disposeTask;
        }

        private async Task AwaitDeferredConnectionDisposalsAsync()
        {
            Task[] pending = _deferredConnectionDisposals.Values.ToArray();
            if (pending.Length == 0)
            {
                return;
            }

            foreach (Task disposeTask in pending)
            {
                try
                {
                    await disposeTask.ConfigureAwait(false);
                }
                catch
                {
                }
            }

            _deferredConnectionDisposals.Clear();
        }

        internal sealed record TransitPublisherConnectionDiagnosticsSnapshot(
            int ConfiguredConnectionPoolSize,
            int ConfiguredPerConnectionPipelineDepth,
            long TotalReconnects,
            long QueuedSubmissionCount,
            ConnectionSlotSnapshot[] Slots,
            ConnectionDiagnosticsEntry[] Connections,
            SubmissionTraceRecord[] SubmissionTraceRecords,
            PublishToConnectionTraceRecord[] PublishToConnectionTraceRecords,
            PumpFaultTelemetrySnapshot? PumpFaultTelemetry,
            GlobalTransitWorkQueueSnapshot QueueSnapshot);

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
            string ConnectionId,
            TransitConnection.TransitConnectionDiagnosticsSnapshot Snapshot);

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

        internal sealed record PumpFaultTelemetrySnapshot(
            ExceptionDispatchInfo? FirstFault,
            long FaultCount,
            long FaultSequence,
            long MeasurementStartStopwatchTick,
            long MeasurementEndStopwatchTick,
            bool MeasurementBoundaryObserved,
            bool ProducerCompleted,
            bool DispatchersCompleted,
            string? FaultMessage,
            string? FaultType,
            TransitConnectionState PublisherState,
            string? ConnectionId,
            int SlotIndex,
            long CapturedAtTick,
            PumpFaultMeasurementState MeasurementState,
            TransitPublisherPumpFaultOrigin Origin,
            InvalidOperationFingerprintMessageClass InvalidOperationClass,
            SanitizedFirstFaultMessageClass SanitizedMessageClass,
            bool LocalDisposeAsyncBeforeP1,
            bool LocalResetTransportStateBeforeP1,
            bool LocalDisposeTransportArtifactsBeforeP1,
            bool LocalRebuildPipesBeforeP1,
            bool LocalCleanupFailedInitializationBeforeP1,
            bool InitializationCancellationBeforeP1,
            bool IsSocketEof,
            bool IsConnectionLifecycleSubmitFailure,
            bool IsPumpCoordinationFailure,
            bool IsChannelQueueUnderflowInvariant,
            bool IsChannelQueueOverflowInvariant,
            bool IsConnectionOwnershipInvariant,
            bool IsTerminalizationMissingTaskInvariant,
            bool IsTerminalizationMissingTrackingInvariant)
        {
            internal string ExceptionType => FaultType ?? string.Empty;

            internal string BaseExceptionType => FaultType ?? string.Empty;

            internal int HResult => FirstFault?.SourceException.HResult ?? 0;

            internal InvalidOperationFingerprintMessageClass InvalidOperationMessageClass => InvalidOperationClass;

            internal SanitizedFirstFaultMessageClass SanitizedFirstFaultMessageClass => SanitizedMessageClass;

            internal string SanitizedFirstFaultMessage => FaultMessage ?? string.Empty;

            internal string FullFirstFaultStackTrace => FirstFault?.SourceException.ToString() ?? string.Empty;

            internal string TopStackFrameDeclaringType => string.Empty;

            internal string TopStackFrameMethodName => string.Empty;

            internal long MillisecondsFromMeasurementStart => 0;

            internal long MillisecondsFromMeasurementEnd => 0;

            internal PumpFaultMeasurementState MeasurementStateAtFault => MeasurementState;

            internal long QueuedSubmissionCount => 0;

            internal int InFlightCount => 0;

            internal int ActiveSubmissionCount => 0;

            internal int? ChannelImmediateAvailableCount => 0;

            internal int ActiveConnectionCount => 0;

            internal int ReadyConnectionCount => 0;

            internal int FaultedConnectionCount => 0;

            internal int ReconnectingConnectionCount => 0;

            internal long OutstandingConnectionOperations => 0;

            internal ProducerCompletionState ProducerCompletionState => ProducerCompletionState.Unknown;

            internal DispatchersCompletedState DispatchersCompletedState => DispatchersCompletedState.Unknown;
        }

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

        internal readonly record struct SubmissionPumpFaultCounts(
            long TotalFaultCount,
            long InitiatingFaultCount,
            long CascadeFaultCount);

        internal enum SanitizedFirstFaultMessageClass
        {
            None = 0,
            NntpConnectionClosedAwaitingLine = 1,
            TransitConnectionClosedBeforeResponse = 2,
            ConnectionOwnershipInvariant = 3,
            TerminalizationInvariant = 4,
            QueueAccountingInvariant = 5,
            OtherInvalidOperation = 6,
            NotInvalidOperation = 7,
        }
    }
}
