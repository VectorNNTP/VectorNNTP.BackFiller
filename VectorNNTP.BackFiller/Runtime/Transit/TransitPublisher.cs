// <copyright file="TransitPublisher.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Transit
// Implements the transit publisher behavior.

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
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
        /// <summary>
        /// Default per-connection pipeline depth used when no override is supplied.
        /// </summary>
        private const int DefaultPerConnectionPipelineDepth = 8;

        /// <summary>
        /// Validated runtime options that define queue bounds and transit endpoint settings.
        /// </summary>
        private readonly BackFillerRuntimeOptions _runtimeOptions;
        /// <summary>
        /// Unified time provider used for transport snapshots and diagnostics.
        /// </summary>
        private readonly TimeProvider _timeProvider;
        /// <summary>
        /// Supplies the logger used by transit publisher.
        /// </summary>
        private readonly ILogger<TransitPublisher> _logger;
        /// <summary>
        /// Configured number of connection slots maintained by the publisher.
        /// </summary>
        private readonly int _connectionPoolSize;
        /// <summary>
        /// Configured maximum batch depth targeted per active connection.
        /// </summary>
        private readonly int _perConnectionPipelineDepth;
        /// <summary>
        /// Configures connection response progress timeout for transit publisher.
        /// </summary>
        private readonly TimeSpan? _connectionResponseProgressTimeout;
        /// <summary>
        /// Configures connection response progress check interval for transit publisher.
        /// </summary>
        private readonly TimeSpan? _connectionResponseProgressCheckInterval;

        /// <summary>
        /// Single bounded global queue that owns admitted work until a connection claims it.
        /// </summary>
        private readonly GlobalTransitWorkQueue _globalQueue;
        /// <summary>
        /// Current connection instance per pool slot.
        /// </summary>
        private readonly TransitConnection[] _connections;
        /// <summary>
        /// Long-running worker task per connection slot.
        /// </summary>
        private readonly Task[] _connectionWorkers;
        /// <summary>
        /// Per-slot gates that prevent concurrent reconnect attempts for the same slot.
        /// </summary>
        private readonly SemaphoreSlim[] _reconnectGates;
        /// <summary>
        /// Cancellation source used to stop all connection workers during preemption or disposal.
        /// </summary>
        private readonly CancellationTokenSource _connectionWorkersCancellation = new();
        /// <summary>
        /// Active work items tracked by identifier until terminal completion is observed.
        /// </summary>
        private readonly ConcurrentDictionary<long, TransitWorkItem> _activeWorkItems = new();
        /// <summary>
        /// Connection-disposal tasks allowed to finish after ownership moves to a replacement connection.
        /// </summary>
        private readonly ConcurrentDictionary<string, Task> _deferredConnectionDisposals = new(StringComparer.Ordinal);
        /// <summary>
        /// Optional collector for timing data emitted by connection staging and completion observation.
        /// </summary>
        private readonly TransitTimingCollector? _timingCollector;

        /// <summary>
        /// Monotonic identifier source for newly admitted work items.
        /// </summary>
        private long _nextWorkItemId;
        /// <summary>
        /// Aggregate bytes transmitted across all connections.
        /// </summary>
        private long _totalBytesTransmitted;
        /// <summary>
        /// Aggregate bytes received across all connections.
        /// </summary>
        private long _totalBytesReceived;
        /// <summary>
        /// Aggregate count of articles admitted for submission.
        /// </summary>
        private long _totalArticlesSubmitted;
        /// <summary>
        /// Aggregate count of articles definitively accepted by remote transit servers.
        /// </summary>
        private long _totalArticlesAccepted;
        /// <summary>
        /// Aggregate count of articles definitively rejected by remote transit servers.
        /// </summary>
        private long _totalArticlesRejected;
        /// <summary>
        /// Aggregate count of articles terminalized as ambiguous.
        /// </summary>
        private long _totalArticlesAmbiguous;
        /// <summary>
        /// Aggregate count of articles terminalized as local failures.
        /// </summary>
        private long _totalArticlesFailed;
        /// <summary>
        /// Aggregate count of articles canceled before successful completion.
        /// </summary>
        private long _totalArticlesCanceled;
        /// <summary>
        /// Aggregate reconnect count across all pool slots.
        /// </summary>
        private long _totalReconnects;

        /// <summary>
        /// Single-bit guard ensuring initialization runs only once.
        /// </summary>
        private int _initialized;
        /// <summary>
        /// Indicates that preemption or disposal has started and new publishes must stop.
        /// </summary>
        private volatile bool _disposeRequested;
        /// <summary>
        /// Tracks the number of connection workers that have been started and not yet exited.
        /// </summary>
        private int _remainingConnectionWorkers;
        /// <summary>
        /// Observable aggregate publisher lifecycle state.
        /// </summary>
        private volatile TransitConnectionState _state = TransitConnectionState.Disconnected;

        /// <summary>
        /// Formats a low-level trace stamp for console diagnostics in this file.
        /// </summary>
        private static string TraceStamp()
        {
            return $"{DateTimeOffset.UtcNow:O}|tid={Environment.CurrentManagedThreadId}|task={Task.CurrentId?.ToString() ?? "-"}";
        }

        /// <summary>
        /// Initializes the transit publisher, global queue, and connection-slot bookkeeping.
        /// </summary>
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

        /// <summary>
        /// Gets the current aggregate publisher lifecycle state.
        /// </summary>
        internal TransitConnectionState CurrentState => _state;

        /// <summary>
        /// Captures the current timing snapshot when timing collection is enabled.
        /// </summary>
        /// <returns>The current timing snapshot, or <see langword="null"/> when timing collection is disabled.</returns>
        internal TransitTimingSnapshot? CaptureTimingSnapshot()
        {
            return _timingCollector?.CaptureSnapshot();
        }

        /// <summary>
        /// Captures a point-in-time diagnostic view of connection slots and active connections.
        /// </summary>
        /// <returns>The current connection diagnostics snapshot.</returns>
        internal TransitPublisherConnectionDiagnosticsSnapshot CaptureConnectionDiagnosticsSnapshot()
        {
            ConnectionSlotSnapshot[] slots = new ConnectionSlotSnapshot[_connections.Length];
            List<ConnectionDiagnosticsEntry> connectionEntries = [];

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

        /// <summary>
        /// Captures aggregate transport counters together with externally supplied connection counts.
        /// </summary>
        /// <param name="activeConnections">Current number of active connections.</param>
        /// <param name="outstandingSubmissions">Current number of outstanding submissions.</param>
        /// <returns>The current transport snapshot.</returns>
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

        /// <summary>
        /// Starts the per-slot connection workers and transitions the publisher into ready state.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token for initialization.</param>
        /// <returns>A task that completes after worker startup and readiness checks finish.</returns>
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
                _ = Interlocked.Increment(ref _remainingConnectionWorkers);
                try
                {
                    _connectionWorkers[i] = Task.Run(() => RunConnectionWorkerAsync(slotIndex, _connectionWorkersCancellation.Token), CancellationToken.None);
                }
                catch
                {
                    _ = Interlocked.Decrement(ref _remainingConnectionWorkers);
                    throw;
                }
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

        /// <summary>
        /// Validates, copies, admits, and waits for terminal completion of one article publish request.
        /// </summary>
        /// <param name="messageId">Article Message-ID used for protocol framing and response correlation.</param>
        /// <param name="articlePayload">Full article payload ending in LF so TAKETHIS framing preserves byte integrity.</param>
        /// <param name="cancellationToken">Cancellation token for admission and caller wait.</param>
        /// <returns>The terminal publish result for the admitted work item.</returns>
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
                _ = Interlocked.Increment(ref _totalArticlesSubmitted);
                Console.WriteLine($"[TRACE-RI-04] {TraceStamp()} PublishAsync ENQUEUED workItemId={workItem.WorkItemId} state={workItem.State} totalSubmitted={Interlocked.Read(ref _totalArticlesSubmitted)}");
            }
            catch (OperationCanceledException)
            {
                _ = _activeWorkItems.TryRemove(workItem.WorkItemId, out _);
                throw;
            }
            catch (Exception)
            {
                _ = _activeWorkItems.TryRemove(workItem.WorkItemId, out _);
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

        /// <summary>
        /// Placeholder hook for recording a submission-pump fault measurement window.
        /// </summary>
        /// <param name="measurementStartStopwatchTick">Stopwatch tick marking the start of the measurement window.</param>
        /// <param name="measurementEndStopwatchTick">Stopwatch tick marking the end of the measurement window.</param>
        /// <param name="measurementBoundaryObserved"><see langword="true"/> when the measurement end boundary had already been observed.</param>
        internal static void MarkSubmissionPumpFaultMeasurementWindow(long measurementStartStopwatchTick, long measurementEndStopwatchTick, bool measurementBoundaryObserved)
        {
        }

        /// <summary>
        /// Placeholder hook for recording whether all producers had completed when a pump fault was observed.
        /// </summary>
        /// <param name="allProducersCompleted"><see langword="true"/> when all producers had completed at fault time.</param>
        internal static void MarkSubmissionPumpFaultProducerCompletion(bool allProducersCompleted)
        {
        }

        /// <summary>
        /// Placeholder hook for recording whether dispatcher completion had been observed when a pump fault occurred.
        /// </summary>
        /// <param name="dispatchersCompleted"><see langword="true"/> when dispatcher completion had been observed at fault time.</param>
        internal static void MarkSubmissionPumpFaultDispatchersCompleted(bool dispatchersCompleted)
        {
        }

        /// <summary>
        /// Captures submission-pump fault telemetry when such tracking is enabled.
        /// </summary>
        /// <returns>The captured telemetry snapshot, or <see langword="null"/> when none is available.</returns>
        internal static PumpFaultTelemetrySnapshot? CaptureSubmissionPumpFaultTelemetrySnapshot()
        {
            return null;
        }

        /// <summary>
        /// Captures aggregate submission-pump fault counters.
        /// </summary>
        /// <returns>The current fault counters.</returns>
        internal static SubmissionPumpFaultCounts CaptureSubmissionPumpFaultCounts()
        {
            return new SubmissionPumpFaultCounts(
                TotalFaultCount: 0,
                InitiatingFaultCount: 0,
                CascadeFaultCount: 0);
        }

        /// <summary>
        /// Returns the first captured greeting provenance snapshot from any live connection, when available.
        /// </summary>
        /// <returns>The first greeting provenance snapshot found, or <see langword="null"/> when none is available.</returns>
        internal TransitConnection.P1GreetingProvenanceSnapshot? CaptureFirstP1GreetingProvenanceSnapshot()
        {
            foreach (TransitConnection connection in _connections)
            {
                // TODO: CK Code Commented out for now because it is not used and causes a warning. Uncomment if needed in the future.
                //TransitConnection.P1GreetingProvenanceSnapshot? snapshot = connection?.CaptureFirstP1GreetingProvenanceSnapshot();
                //if (snapshot is not null)
                //{
                //    return snapshot;
                //}
            }

            return null;
        }

        /// <summary>
        /// Freezes queue admission, stops connection workers, and terminalizes any remaining owned work.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token for worker shutdown.</param>
        /// <returns>A task that completes after preemption cleanup finishes.</returns>
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

        /// <summary>
        /// Disposes the publisher by preempting work, awaiting workers, and releasing connection resources.
        /// </summary>
        /// <returns>A value task that completes after worker shutdown and connection disposal finish.</returns>
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
            foreach (Task? worker in _connectionWorkers)
            {
                if (worker is null)
                {
                    continue;
                }

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
            TryFinalizeQueueDisposal();
            TransitionState(TransitConnectionState.Disconnected);
        }

        /// <summary>
        /// Main worker loop for one connection slot that ensures connectivity, claims work, and processes completions.
        /// </summary>
        private async Task RunConnectionWorkerAsync(int slotIndex, CancellationToken cancellationToken)
        {
            try
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
                        //Console.WriteLine($"[TRACE-RI-12] {TraceStamp()} Worker WAIT-FOR-WORK slot={slotIndex} connectionId={(connection?.ConnectionId ?? "none")} hasWork={hasWork}");
                        if (!hasWork)
                        {
                            continue;
                        }

                        if (connection is null)
                        {
                            connection = await CreateAndInitializeConnectionAsync(slotIndex, reconnecting: false, cancellationToken).ConfigureAwait(false);
                            //Console.WriteLine($"[TRACE-RI-11] {TraceStamp()} Worker INITIAL-CONNECTION-READY slot={slotIndex} connectionId={connection.ConnectionId} state={connection.CurrentState}");
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
                        if (connection is not null)
                        {
                            await RequeueClaimedAndOutstandingAfterFaultAsync(connection, claimed, CancellationToken.None).ConfigureAwait(false);
                            _ = TrackDeferredConnectionDisposal(connection);
                        }
                        else if (claimed is not null)
                        {
                            foreach (TransitWorkItem claimedItem in claimed)
                            {
                                await RequeueOrTerminalizeFailureAsync(
                                    claimedItem,
                                    TransitWorkFailureClass.ConnectionDisposed,
                                    TransitTransmissionUncertainty.ConnectionFailedDuringSend,
                                    CancellationToken.None).ConfigureAwait(false);
                            }
                        }

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
                            _ = reconnectGate.Release();
                        }
                    }
                }
            }
            finally
            {
                OnConnectionWorkerExited();
            }
        }

        /// <summary>
        /// Records one worker exit and finalizes queue disposal when shutdown has been requested and no workers remain.
        /// </summary>
        private void OnConnectionWorkerExited()
        {
            int remaining = Interlocked.Decrement(ref _remainingConnectionWorkers);
            if (_disposeRequested && remaining == 0)
            {
                _globalQueue.Dispose();
            }
        }

        /// <summary>
        /// Finalizes queue disposal immediately when shutdown has been requested and no workers remain.
        /// </summary>
        private void TryFinalizeQueueDisposal()
        {
            if (_disposeRequested && Volatile.Read(ref _remainingConnectionWorkers) == 0)
            {
                _globalQueue.Dispose();
            }
        }

        /// <summary>
        /// Requeues or terminalizes work still associated with a faulted connection after worker failure.
        /// </summary>
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

            Dictionary<long, TransitWorkItem> unresolvedById = [];

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

        /// <summary>
        /// Requeues a failed work item when retry budget remains, otherwise terminalizes it.
        /// </summary>
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

        /// <summary>
        /// Applies a terminal result, updates aggregate counters, and removes tracking for one work item.
        /// </summary>
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

            _ = result.Status switch
            {
                TransitPublishStatus.Accepted => Interlocked.Increment(ref _totalArticlesAccepted),
                TransitPublishStatus.Rejected => Interlocked.Increment(ref _totalArticlesRejected),
                TransitPublishStatus.Canceled => Interlocked.Increment(ref _totalArticlesCanceled),
                TransitPublishStatus.Ambiguous => Interlocked.Increment(ref _totalArticlesAmbiguous),
                _ => Interlocked.Increment(ref _totalArticlesFailed),
            };
            _ = item.TrySetCompletionResult(result);
        }

        /// <summary>
        /// Forces all still-tracked work items to terminal completion during preemption or shutdown.
        /// </summary>
        private async Task ForceTerminalizeRemainingWorkAsync()
        {
            TransitWorkItem[] remaining = [.. _activeWorkItems.Values];
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

                _ = forced.Status switch
                {
                    TransitPublishStatus.Accepted => Interlocked.Increment(ref _totalArticlesAccepted),
                    TransitPublishStatus.Rejected => Interlocked.Increment(ref _totalArticlesRejected),
                    TransitPublishStatus.Canceled => Interlocked.Increment(ref _totalArticlesCanceled),
                    TransitPublishStatus.Ambiguous => Interlocked.Increment(ref _totalArticlesAmbiguous),
                    _ => Interlocked.Increment(ref _totalArticlesFailed),
                };
                _ = item.TrySetCompletionResult(forced);
            }

            Console.WriteLine($"[TRACE-RI-34] {TraceStamp()} ForceTerminalize EXIT");
            await Task.CompletedTask.ConfigureAwait(false);
        }

        /// <summary>
        /// Reconnects a faulted slot without overwriting a newer healthy connection installed concurrently.
        /// </summary>
        /// <param name="slotIndex">The connection slot whose current connection is being replaced.</param>
        /// <param name="cancellationToken">Cancels the reconnect operation.</param>
        /// <returns>A task that completes when the reconnect attempt finishes.</returns>
        private async Task ReconnectAsync(int slotIndex, CancellationToken cancellationToken)
        {
            if (slotIndex < 0 || slotIndex >= _connections.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(slotIndex), slotIndex, "Reconnect slot index must reference a valid connection slot.");
            }

            TransitConnection? reconnectTarget = _connections[slotIndex];
            SemaphoreSlim reconnectGate = _reconnectGates[slotIndex];
            await reconnectGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                TransitConnection? replacement = _connections[slotIndex];
                if (replacement is not null && !ReferenceEquals(replacement, reconnectTarget))
                {
                    Console.WriteLine($"[TRACE-RI-18] {TraceStamp()} ReconnectAsync SKIP-EXTERNAL slot={slotIndex} priorConnectionId={reconnectTarget?.ConnectionId ?? "none"} replacementConnectionId={replacement.ConnectionId}");
                    return;
                }

                long reconnects = Interlocked.Increment(ref _totalReconnects);
                Console.WriteLine($"[TRACE-RI-18] {TraceStamp()} ReconnectAsync START slot={slotIndex} priorConnectionId={reconnectTarget?.ConnectionId ?? "none"} totalReconnects={reconnects}");
                TransitConnection connection = await CreateAndInitializeConnectionAsync(slotIndex, reconnecting: true, cancellationToken).ConfigureAwait(false);
                Console.WriteLine($"[TRACE-RI-19] {TraceStamp()} ReconnectAsync READY slot={slotIndex} connectionId={connection.ConnectionId} state={connection.CurrentState}");
                _connections[slotIndex] = connection;
            }
            finally
            {
                _ = reconnectGate.Release();
            }
        }

        /// <summary>
        /// Creates and initializes a new transit connection for one slot.
        /// </summary>
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
                Console.WriteLine($"[TRACE-RI-36] {TraceStamp()} InitLoop CONNECTION-CREATED slot={slotIndex} reconnecting={reconnecting} attempt={attempt} connectionId={connection.ConnectionId} timeoutMs={initializationResponseProgressTimeout?.TotalMilliseconds.ToString("F0") ?? "null"} hasOutstanding={hasOutstandingAdmittedWork}");

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
                    _ = Interlocked.Increment(ref _totalReconnects);
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

        /// <summary>
        /// Determines whether the publisher still owns any non-terminal admitted work.
        /// </summary>
        private bool HasOutstandingAdmittedWork()
        {
            return _activeWorkItems.Values.Any(static item => !item.IsTerminal);
        }

        /// <summary>
        /// Determines whether work demand still justifies keeping connection workers active.
        /// </summary>
        private bool HasConnectionDemand()
        {
            GlobalTransitWorkQueueSnapshot snapshot = _globalQueue.CaptureSnapshot();
            return snapshot.QueuedItemCount > 0
                || snapshot.RetryPendingCount > 0
                || snapshot.InFlightCount > 0
                || HasOutstandingAdmittedWork();
        }

        /// <summary>
        /// Resolves the initialization watchdog timeout to apply to newly created connections.
        /// </summary>
        private TimeSpan? ResolveInitializationResponseProgressTimeout(bool reconnecting, bool hasOutstandingAdmittedWork)
        {
            return reconnecting || hasOutstandingAdmittedWork
                ? _runtimeOptions.EffectiveTransitReconnectInitializationTimeout
                : _connectionResponseProgressTimeout;
        }

        /// <summary>
        /// Computes the bounded retry delay with per-attempt exponential backoff and jitter.
        /// </summary>
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

        /// <summary>
        /// Updates the aggregate publisher lifecycle state.
        /// </summary>
        private void TransitionState(TransitConnectionState state)
        {
            _state = state;
        }

        /// <summary>
        /// Sentinel cancellation used when a worker wakes but no connection demand remains.
        /// </summary>
        private sealed class NoConnectionDemandException : OperationCanceledException
        {
        }

        /// <summary>
        /// Classifies submit failures that should be treated as connection-lifecycle faults.
        /// </summary>
        private static bool IsConnectionLifecycleSubmitFailure(TransitConnection connection, Exception exception)
        {
            ArgumentNullException.ThrowIfNull(connection);
            ArgumentNullException.ThrowIfNull(exception);

            bool result = exception is TransitConnection.TransitConnectionLifecycleException || (connection.CurrentState == TransitConnectionState.Faulted || connection.IsResponseLoopFaulted
                    ? exception is IOException
                    or ObjectDisposedException
                    or SocketException
                    or TimeoutException
                    or System.Threading.Channels.ChannelClosedException
                    : exception is IOException
                    or ObjectDisposedException
                    or SocketException
                    || (exception is InvalidOperationException invalid
                        && (IsInitializationProtocolFailure(connection, invalid)
                            || invalid.Message.Contains("connection", StringComparison.OrdinalIgnoreCase)
                            || invalid.Message.Contains("Duplicate in-flight Message-ID on same connection.", StringComparison.Ordinal))));
            Console.WriteLine($"[TRACE-RI-50] {TraceStamp()} LifecycleFilter connectionId={connection.ConnectionId} state={connection.CurrentState} responseLoopFaulted={connection.IsResponseLoopFaulted} exType={exception.GetType().FullName} exMessage={exception.Message} result={result}");
            return result;
        }

        /// <summary>
        /// Detects initialization-phase protocol failures from invalid-operation diagnostics.
        /// </summary>
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

        /// <summary>
        /// Tracks a connection disposal task that may outlive the slot handoff to a replacement connection.
        /// </summary>
        private Task TrackDeferredConnectionDisposal(TransitConnection connection)
        {
            ArgumentNullException.ThrowIfNull(connection);

            Task disposeTask = connection.DisposeAsync().AsTask();
            _deferredConnectionDisposals[connection.ConnectionId] = disposeTask;
            return disposeTask;
        }

        /// <summary>
        /// Awaits and then clears all deferred connection disposal tasks.
        /// </summary>
        private async Task AwaitDeferredConnectionDisposalsAsync()
        {
            Task[] pending = [.. _deferredConnectionDisposals.Values];
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

        /// <summary>
        /// Point-in-time diagnostic snapshot of publisher slot state, active connections, and queue accounting.
        /// </summary>
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

        /// <summary>
        /// Diagnostic snapshot for one connection slot in the publisher pool.
        /// </summary>
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

        /// <summary>
        /// Pairs a slot index with the diagnostics snapshot captured from its current connection.
        /// </summary>
        internal sealed record ConnectionDiagnosticsEntry(
            int SlotIndex,
            string ConnectionId,
            TransitConnection.TransitConnectionDiagnosticsSnapshot Snapshot);

        /// <summary>
        /// Trace record describing when a submission left the publisher front door and entered connection routing.
        /// </summary>
        internal readonly record struct SubmissionTraceRecord(
            string MessageId,
            long RemovedFromSubmissionChannelTick,
            long PublishToConnectionInvokedTick,
            int InFlightCountBeforeAdd,
            int InFlightCountAfterAdd,
            int WriteIntentQueueDepthAtPumpRead);

        /// <summary>
        /// Trace record describing one publish-to-connection handoff attempt.
        /// </summary>
        internal readonly record struct PublishToConnectionTraceRecord(
            string MessageId,
            int SlotIndex,
            long MethodEntryTick,
            string? SelectedConnectionId,
            long BeforeSubmitTakethisTick,
            long AfterSubmitTakethisTick);

        /// <summary>
        /// Diagnostic snapshot for a captured submission-pump fault.
        /// </summary>
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
            /// <summary>
            /// Gets the captured fault type, or an empty string when none was recorded.
            /// </summary>
            internal string ExceptionType => FaultType ?? string.Empty;

            /// <summary>
            /// Gets the captured base exception type, or an empty string when none was recorded.
            /// </summary>
            internal string BaseExceptionType => FaultType ?? string.Empty;

            /// <summary>
            /// Gets the HRESULT of the captured fault, or zero when unavailable.
            /// </summary>
            internal int HResult => FirstFault?.SourceException.HResult ?? 0;

            /// <summary>
            /// Gets the invalid-operation fingerprint classification derived from the captured fault.
            /// </summary>
            internal InvalidOperationFingerprintMessageClass InvalidOperationMessageClass => InvalidOperationClass;

            /// <summary>
            /// Gets the sanitized fault-message classification derived from the first captured fault.
            /// </summary>
            internal SanitizedFirstFaultMessageClass SanitizedFirstFaultMessageClass => SanitizedMessageClass;

            /// <summary>
            /// Gets the sanitized first fault message, or an empty string when none was recorded.
            /// </summary>
            internal string SanitizedFirstFaultMessage => FaultMessage ?? string.Empty;

            /// <summary>
            /// Gets the full exception text for the first captured fault, or an empty string when unavailable.
            /// </summary>
            internal string FullFirstFaultStackTrace => FirstFault?.SourceException.ToString() ?? string.Empty;

            /// <summary>
            /// Gets the top stack-frame declaring type when populated by future telemetry, or an empty string placeholder.
            /// </summary>
            internal static string TopStackFrameDeclaringType => string.Empty;

            /// <summary>
            /// Gets the top stack-frame method name when populated by future telemetry, or an empty string placeholder.
            /// </summary>
            internal static string TopStackFrameMethodName => string.Empty;

            /// <summary>
            /// Gets the milliseconds from measurement start when populated by future telemetry, or zero placeholder.
            /// </summary>
            internal static long MillisecondsFromMeasurementStart => 0;

            /// <summary>
            /// Gets the milliseconds from measurement end when populated by future telemetry, or zero placeholder.
            /// </summary>
            internal static long MillisecondsFromMeasurementEnd => 0;

            /// <summary>
            /// Gets the measurement-boundary state associated with the captured fault.
            /// </summary>
            internal PumpFaultMeasurementState MeasurementStateAtFault => MeasurementState;

            /// <summary>
            /// Gets the queued submission count when populated by future telemetry, or zero placeholder.
            /// </summary>
            internal static long QueuedSubmissionCount => 0;

            /// <summary>
            /// Gets the in-flight submission count when populated by future telemetry, or zero placeholder.
            /// </summary>
            internal static int InFlightCount => 0;

            /// <summary>
            /// Gets the active submission count when populated by future telemetry, or zero placeholder.
            /// </summary>
            internal static int ActiveSubmissionCount => 0;

            /// <summary>
            /// Gets the immediate-availability count when populated by future telemetry, or zero placeholder.
            /// </summary>
            internal static int? ChannelImmediateAvailableCount => 0;

            /// <summary>
            /// Gets the active connection count when populated by future telemetry, or zero placeholder.
            /// </summary>
            internal static int ActiveConnectionCount => 0;

            /// <summary>
            /// Gets the ready connection count when populated by future telemetry, or zero placeholder.
            /// </summary>
            internal static int ReadyConnectionCount => 0;

            /// <summary>
            /// Gets the faulted connection count when populated by future telemetry, or zero placeholder.
            /// </summary>
            internal static int FaultedConnectionCount => 0;

            /// <summary>
            /// Gets the reconnecting connection count when populated by future telemetry, or zero placeholder.
            /// </summary>
            internal static int ReconnectingConnectionCount => 0;

            /// <summary>
            /// Gets the outstanding connection-operation count when populated by future telemetry, or zero placeholder.
            /// </summary>
            internal static long OutstandingConnectionOperations => 0;

            /// <summary>
            /// Gets the producer-completion state when populated by future telemetry, or <see cref="ProducerCompletionState.Unknown"/>.
            /// </summary>
            internal static ProducerCompletionState ProducerCompletionState => ProducerCompletionState.Unknown;

            /// <summary>
            /// Gets the dispatcher-completion state when populated by future telemetry, or <see cref="DispatchersCompletedState.Unknown"/>.
            /// </summary>
            internal static DispatchersCompletedState DispatchersCompletedState => DispatchersCompletedState.Unknown;
        }

        /// <summary>
        /// Identifies the publisher component that originated a captured pump fault.
        /// </summary>
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

        /// <summary>
        /// Indicates whether a fault was observed before or after the measurement boundary was closed.
        /// </summary>
        internal enum PumpFaultMeasurementState
        {
            Unknown = 0,
            BeforeMeasurementEnd = 1,
            AfterMeasurementEnd = 2,
        }

        /// <summary>
        /// Buckets invalid-operation failures by the invariant or subsystem they appear to represent.
        /// </summary>
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

        /// <summary>
        /// Placeholder classification for whether producers had completed at fault time.
        /// </summary>
        internal enum ProducerCompletionState
        {
            Unknown = 0,
            Incomplete = 1,
            Complete = 2,
        }

        /// <summary>
        /// Placeholder classification for whether dispatcher completion had been observed at fault time.
        /// </summary>
        internal enum DispatchersCompletedState
        {
            Unknown = 0,
            Incomplete = 1,
            Complete = 2,
        }

        /// <summary>
        /// Aggregate submission-pump fault counters.
        /// </summary>
        internal readonly record struct SubmissionPumpFaultCounts(
            long TotalFaultCount,
            long InitiatingFaultCount,
            long CascadeFaultCount);

        /// <summary>
        /// Buckets sanitized first-fault messages by recognized invariant or lifecycle pattern.
        /// </summary>
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
