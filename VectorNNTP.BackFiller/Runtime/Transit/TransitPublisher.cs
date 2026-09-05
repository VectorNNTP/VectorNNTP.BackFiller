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
    /// Owns transit admission, connection-slot orchestration, and the diagnostic snapshots that describe publisher-owned queue and lifetime state.
    /// </summary>
    /// <remarks>
    /// The publisher is the ownership boundary for outbound work. It validates and admits articles into the global queue, manages one worker slot
    /// per connection, and exposes the authoritative queue and transport counters used to understand reconnects, retirement, and shutdown.
    /// Connection objects own protocol I/O; the publisher owns slot visibility, lifetime aggregates, and the coordination needed to keep snapshots coherent.
    /// </remarks>
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
        private readonly TransitConnection?[] _connections;
        /// <summary>
        /// Monotonic per-slot version used to make slot handoffs and snapshots coherent.
        /// </summary>
        private readonly long[] _connectionSlotSnapshotVersions;
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
        /// Aggregate bytes transmitted across all connections that have participated in the publisher lifetime.
        /// </summary>
        private long _totalBytesTransmitted;
        /// <summary>
        /// Aggregate bytes received across all connections that have participated in the publisher lifetime.
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
        /// Initializes the publisher's queue ownership, worker slots, lifetime counters, and reconnect coordination state from validated runtime options.
        /// </summary>
        /// <param name="runtimeOptions">Validated runtime settings that define queue bounds, retry behavior, and transit endpoint configuration.</param>
        /// <param name="timeProvider">The time provider used to stamp runtime and diagnostic snapshots.</param>
        /// <param name="logger">Structured logger used by the publisher and the connections it creates for lifecycle and diagnostic reporting.</param>
        /// <param name="connectionPoolSize">The number of worker slots and slot records to maintain.</param>
        /// <param name="perConnectionPipelineDepth">The maximum number of admitted items each active connection may pipeline.</param>
        /// <param name="connectionResponseProgressTimeout">Optional watchdog timeout for detecting stalled connection responses during steady-state work.</param>
        /// <param name="connectionResponseProgressCheckInterval">Optional interval used when polling connection response progress.</param>
        /// <param name="timingCollector">Optional collector for timing measurements emitted by admission and completion observation.</param>
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

            _connections = new TransitConnection?[_connectionPoolSize];
            _connectionSlotSnapshotVersions = new long[_connectionPoolSize];
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
        /// Marks one connection slot as mutating so snapshots retry instead of observing a half-retired or half-published slot.
        /// </summary>
        /// <param name="slotIndex">The slot whose shared visibility is about to change.</param>
        private void BeginConnectionSlotTransition(int slotIndex)
        {
            _ = Interlocked.Increment(ref _connectionSlotSnapshotVersions[slotIndex]);
        }

        /// <summary>
        /// Marks one connection slot as settled after a shared visibility change completes.
        /// </summary>
        /// <param name="slotIndex">The slot whose shared visibility just settled.</param>
        private void EndConnectionSlotTransition(int slotIndex)
        {
            _ = Interlocked.Increment(ref _connectionSlotSnapshotVersions[slotIndex]);
        }

        /// <summary>
        /// Captures a coherent diagnostic view of slot ownership, queue state, and the connections currently visible through the publisher.
        /// </summary>
        /// <remarks>
        /// The version fence forces a retry while a slot is mutating so the returned snapshot never mixes retired bytes, retired ownership, and replacement visibility
        /// from different stages of the same handoff. A slot may legitimately be empty while a replacement connection is still initializing outside the fence.
        /// </remarks>
        /// <returns>A point-in-time publisher diagnostics snapshot suitable for operator inspection and troubleshooting.</returns>
        internal TransitPublisherConnectionDiagnosticsSnapshot CaptureConnectionDiagnosticsSnapshot()
        {
            while (true)
            {
                ConnectionSlotSnapshot[] slots = new ConnectionSlotSnapshot[_connections.Length];
                List<ConnectionDiagnosticsEntry> connectionEntries = [];
                bool retry = false;

                for (int i = 0; i < _connections.Length; i++)
                {
                    long slotVersion = Interlocked.Read(ref _connectionSlotSnapshotVersions[i]);
                    if ((slotVersion & 1L) != 0)
                    {
                        retry = true;
                        break;
                    }

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

                    if (Interlocked.Read(ref _connectionSlotSnapshotVersions[i]) != slotVersion)
                    {
                        retry = true;
                        break;
                    }
                }

                if (!retry)
                {
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
            }
        }

        /// <summary>
        /// Captures the publisher's lifetime transport counters together with caller-supplied operational counts.
        /// </summary>
        /// <remarks>
        /// The byte totals include retired connections that have already been rolled into the publisher lifetime aggregates. The same slot-version fence used by
        /// the connection diagnostics snapshot prevents the method from combining retired visibility with pre-rollover per-connection counters.
        /// </remarks>
        /// <param name="activeConnections">The caller's current count of active connections.</param>
        /// <param name="outstandingSubmissions">The caller's current count of outstanding submissions.</param>
        /// <returns>A transport snapshot containing lifetime byte and article totals plus the supplied operational counts.</returns>
        internal TransitTransportSnapshot CaptureTransportSnapshot(int activeConnections, int outstandingSubmissions)
        {
            while (true)
            {
                bool retry = false;

                for (int i = 0; i < _connections.Length; i++)
                {
                    long slotVersion = Interlocked.Read(ref _connectionSlotSnapshotVersions[i]);
                    if ((slotVersion & 1L) != 0)
                    {
                        retry = true;
                        break;
                    }
                }

                long totalBytesTransmitted = Interlocked.Read(ref _totalBytesTransmitted);
                long totalBytesReceived = Interlocked.Read(ref _totalBytesReceived);

                for (int i = 0; i < _connections.Length; i++)
                {
                    long slotVersion = Interlocked.Read(ref _connectionSlotSnapshotVersions[i]);
                    if ((slotVersion & 1L) != 0)
                    {
                        retry = true;
                        break;
                    }

                    TransitConnection? connection = _connections[i];
                    if (connection is not null)
                    {
                        TransitConnection.TransitConnectionDiagnosticsSnapshot diagnostics = connection.CaptureDiagnosticsSnapshot();
                        totalBytesTransmitted += diagnostics.BytesTransmitted;
                        totalBytesReceived += diagnostics.BytesReceived;
                    }

                    if (Interlocked.Read(ref _connectionSlotSnapshotVersions[i]) != slotVersion)
                    {
                        retry = true;
                        break;
                    }
                }

                if (!retry)
                {
                    return new TransitTransportSnapshot(
                        TotalBytesTransmitted: totalBytesTransmitted,
                        TotalBytesReceived: totalBytesReceived,
                        TotalArticlesSubmitted: Interlocked.Read(ref _totalArticlesSubmitted),
                        TotalArticlesAccepted: Interlocked.Read(ref _totalArticlesAccepted),
                        TotalArticlesRejected: Interlocked.Read(ref _totalArticlesRejected),
                        TotalArticlesAmbiguous: Interlocked.Read(ref _totalArticlesAmbiguous),
                        TotalReconnects: Interlocked.Read(ref _totalReconnects),
                        ActiveConnections: activeConnections,
                        OutstandingSubmissions: outstandingSubmissions);
                }
            }
        }

        /// <summary>
        /// Starts one worker per slot, waits for any immediate startup faults, and transitions the publisher to ready only after the worker set is established.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token used to abort initialization before the publisher becomes ready.</param>
        /// <returns>A task that completes after worker startup, fault observation, and the ready-state transition finish.</returns>
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
        /// Validates one article submission, copies it into publisher-owned storage, admits it to the global queue, and waits for terminal completion.
        /// </summary>
        /// <param name="messageId">The article Message-ID used for protocol framing and response correlation.</param>
        /// <param name="articlePayload">The full article payload; it must end in LF so TAKETHIS framing preserves byte integrity.</param>
        /// <param name="cancellationToken">Cancellation token applied to admission and to the caller's wait for the terminal result.</param>
        /// <returns>The terminal publish result for the admitted work item, or an unavailable result if the publisher has not been initialized or is shutting down.</returns>
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

            _activeWorkItems[workItem.WorkItemId] = workItem;

            try
            {
                await _globalQueue.EnqueueAsync(workItem, cancellationToken).ConfigureAwait(false);
                _ = Interlocked.Increment(ref _totalArticlesSubmitted);
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
                TransitPublishResult completed = await workItem.CompletionTask.ConfigureAwait(false);
                TransitPublishResult tracedResult = completed with
                {
                    T0PublishAsyncEnterTick = publishAsyncEnterTick,
                    T7PublishAsyncCompleteTick = Stopwatch.GetTimestamp(),
                };
                return tracedResult;
            }

            Task<TransitPublishResult> completionTask = workItem.CompletionTask;
            Task cancellationTask = Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            Task completedTask = await Task.WhenAny(completionTask, cancellationTask).ConfigureAwait(false);

            if (ReferenceEquals(completedTask, completionTask))
            {
                TransitPublishResult result = await completionTask.ConfigureAwait(false);
                TransitPublishResult tracedResult = result with
                {
                    T0PublishAsyncEnterTick = publishAsyncEnterTick,
                    T7PublishAsyncCompleteTick = Stopwatch.GetTimestamp(),
                };
                return tracedResult;
            }

            workItem.MarkCancelRequested();
            throw new OperationCanceledException("Transit publish canceled.", cancellationToken);
        }

        /// <summary>
        /// Records the measurement window associated with a submission-pump fault.
        /// </summary>
        /// <remarks>
        /// The current implementation is a no-op placeholder; the parameters define the diagnostic contract for future telemetry without changing
        /// current runtime behavior.
        /// </remarks>
        /// <param name="measurementStartStopwatchTick">Stopwatch tick marking the start of the measurement window.</param>
        /// <param name="measurementEndStopwatchTick">Stopwatch tick marking the end of the measurement window.</param>
        /// <param name="measurementBoundaryObserved"><see langword="true"/> when the end boundary was already observed at the time of the fault.</param>
        internal static void MarkSubmissionPumpFaultMeasurementWindow(long measurementStartStopwatchTick, long measurementEndStopwatchTick, bool measurementBoundaryObserved)
        {
            _ = measurementStartStopwatchTick;
            _ = measurementEndStopwatchTick;
            _ = measurementBoundaryObserved;
        }

        /// <summary>
        /// Records whether all producers had completed when a submission-pump fault was observed.
        /// </summary>
        /// <remarks>
        /// This is currently a no-op placeholder that preserves the future telemetry shape for fault analysis.
        /// </remarks>
        /// <param name="allProducersCompleted"><see langword="true"/> when all producers had completed at fault time.</param>
        internal static void MarkSubmissionPumpFaultProducerCompletion(bool allProducersCompleted)
        {
            _ = allProducersCompleted;
        }

        /// <summary>
        /// Records whether dispatcher completion had already been observed when a submission-pump fault occurred.
        /// </summary>
        /// <remarks>
        /// This is currently a no-op placeholder that preserves the future telemetry shape for fault analysis.
        /// </remarks>
        /// <param name="dispatchersCompleted"><see langword="true"/> when dispatcher completion had been observed at fault time.</param>
        internal static void MarkSubmissionPumpFaultDispatchersCompleted(bool dispatchersCompleted)
        {
            _ = dispatchersCompleted;
        }

        /// <summary>
        /// Captures the current submission-pump fault telemetry snapshot when future instrumentation has populated one.
        /// </summary>
        /// <returns>The captured telemetry snapshot, or <see langword="null"/> when no telemetry has been recorded.</returns>
        internal static PumpFaultTelemetrySnapshot? CaptureSubmissionPumpFaultTelemetrySnapshot()
        {
            return null;
        }

        /// <summary>
        /// Captures the current aggregate submission-pump fault counters.
        /// </summary>
        /// <returns>The current fault counters; the present implementation returns zeroed placeholder values.</returns>
        internal static SubmissionPumpFaultCounts CaptureSubmissionPumpFaultCounts()
        {
            return new SubmissionPumpFaultCounts(
                TotalFaultCount: 0,
                InitiatingFaultCount: 0,
                CascadeFaultCount: 0);
        }

        /// <summary>
        /// Returns a greeting-provenance snapshot when the publisher exposes one through the connection capture path.
        /// </summary>
        /// <remarks>
        /// The current implementation does not yet surface a live greeting-provenance capture and therefore returns <see langword="null"/>.
        /// </remarks>
        /// <returns>The first available greeting-provenance snapshot, or <see langword="null"/> when none is currently exposed.</returns>
        internal TransitConnection.P1GreetingProvenanceSnapshot? CaptureFirstP1GreetingProvenanceSnapshot()
        {
            foreach (TransitConnection? _ in _connections)
            {
                TransitConnection.P1GreetingProvenanceSnapshot? snapshot = TransitConnection.CaptureFirstP1GreetingProvenanceSnapshot();
                if (snapshot is not null)
                {
                    return snapshot;
                }
            }

            return null;
        }

        /// <summary>
        /// Freezes admission, cancels connection workers, and terminalizes any work still owned by the publisher during preemption.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token used while waiting for worker shutdown.</param>
        /// <returns>A task that completes after preemption cleanup and final terminalization finish.</returns>
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
            }

            await ForceTerminalizeRemainingWorkAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Disposes the publisher by stopping admission, canceling workers, draining remaining work, and releasing connection resources.
        /// </summary>
        /// <remarks>
        /// Disposal preserves ownership boundaries: the publisher cancels worker activity, terminalizes tracked work, and then waits for deferred connection
        /// disposals so slot handoffs do not leak resources or re-attach retired connections.
        /// </remarks>
        /// <returns>A value task that completes after worker shutdown, deferred connection disposal, and terminalization finish.</returns>
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
                    break;
                }

                try
                {
                    await worker.WaitAsync(remaining).ConfigureAwait(false);
                }
                catch (TimeoutException)
                {
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
        /// <param name="slotIndex">The index of the slot for which the connection worker is running.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        private async Task RunConnectionWorkerAsync(int slotIndex, CancellationToken cancellationToken)
        {
            try
            {
                TransitConnection? connection = null;

                while (!cancellationToken.IsCancellationRequested)
                {
                    List<TransitWorkItem>? claimed = null;

                    try
                    {
                        await _globalQueue.DrainEligibleRetriesAsync(cancellationToken).ConfigureAwait(false);

                        bool hasWork = await _globalQueue.WaitForWorkAsync(cancellationToken).ConfigureAwait(false);
                        if (!hasWork)
                        {
                            continue;
                        }

                        if (connection is null)
                        {
                            connection = await CreateAndInitializeConnectionAsync(slotIndex, reconnecting: false, cancellationToken).ConfigureAwait(false);
                            BeginConnectionSlotTransition(slotIndex);
                            try
                            {
                                _connections[slotIndex] = connection;
                            }
                            finally
                            {
                                EndConnectionSlotTransition(slotIndex);
                            }
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
                            catch
                            {
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
                            Task disposalTask = TrackDeferredConnectionDisposal(connection);
                            await AwaitDeferredConnectionDisposalAsync(disposalTask).ConfigureAwait(false);
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
                        await RequeueClaimedAndOutstandingAfterFaultAsync(connection, claimed, cancellationToken).ConfigureAwait(false);

                        bool shutdownActive = _disposeRequested || cancellationToken.IsCancellationRequested;
                        Task disposalTask = TrackDeferredConnectionDisposal(connection);
                        if (shutdownActive)
                        {
                            await AwaitDeferredConnectionDisposalAsync(disposalTask).ConfigureAwait(false);
                            break;
                        }

                        if (_disposeRequested || cancellationToken.IsCancellationRequested)
                        {
                            await AwaitDeferredConnectionDisposalAsync(disposalTask).ConfigureAwait(false);
                            break;
                        }

                        if (!HasConnectionDemand())
                        {
                            await AwaitDeferredConnectionDisposalAsync(disposalTask).ConfigureAwait(false);

                            BeginConnectionSlotTransition(slotIndex);
                            try
                            {
                                TransitConnection.TransitConnectionDiagnosticsSnapshot retiredDiagnostics = connection.CaptureDiagnosticsSnapshot();
                                connection = null;
                                _connections[slotIndex] = null;
                                _ = Interlocked.Add(ref _totalBytesTransmitted, retiredDiagnostics.BytesTransmitted);
                                _ = Interlocked.Add(ref _totalBytesReceived, retiredDiagnostics.BytesReceived);
                            }
                            finally
                            {
                                EndConnectionSlotTransition(slotIndex);
                            }

                            continue;
                        }

                        TransitConnection reconnectTarget = connection;
                        SemaphoreSlim reconnectGate = _reconnectGates[slotIndex];
                        await reconnectGate.WaitAsync(cancellationToken).ConfigureAwait(false);
                        try
                        {
                            await AwaitDeferredConnectionDisposalAsync(disposalTask).ConfigureAwait(false);

                            TransitConnection? replacement = _connections[slotIndex];
                            if (replacement is not null && !ReferenceEquals(replacement, reconnectTarget))
                            {
                                connection = replacement;
                                continue;
                            }

                            BeginConnectionSlotTransition(slotIndex);
                            try
                            {
                                TransitConnection.TransitConnectionDiagnosticsSnapshot retiredDiagnostics = reconnectTarget.CaptureDiagnosticsSnapshot();
                                connection = null;
                                _connections[slotIndex] = null;
                                _ = Interlocked.Add(ref _totalBytesTransmitted, retiredDiagnostics.BytesTransmitted);
                                _ = Interlocked.Add(ref _totalBytesReceived, retiredDiagnostics.BytesReceived);
                            }
                            finally
                            {
                                EndConnectionSlotTransition(slotIndex);
                            }

                            _ = Interlocked.Increment(ref _totalReconnects);
                            TransitConnection initializedReplacement;
                            try
                            {
                                initializedReplacement = await CreateAndInitializeConnectionAsync(slotIndex, reconnecting: true, cancellationToken).ConfigureAwait(false);
                            }
                            catch (NoConnectionDemandException)
                            {
                                connection = null;
                                continue;
                            }

                            BeginConnectionSlotTransition(slotIndex);
                            try
                            {
                                _connections[slotIndex] = initializedReplacement;
                                connection = initializedReplacement;
                            }
                            finally
                            {
                                EndConnectionSlotTransition(slotIndex);
                            }
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
        /// Records one worker exit and finalizes queue disposal when shutdown has been requested and the last worker leaves.
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
        /// Collects the work still owned by a faulted connection and routes it back through retry or terminalization paths.
        /// </summary>
        private async Task RequeueClaimedAndOutstandingAfterFaultAsync(
            TransitConnection connection,
            List<TransitWorkItem>? claimed,
            CancellationToken cancellationToken)
        {
            while (connection.TryTakeCompleted(out TransitWorkItem completedItem, out TransitPublishResult completedResult))
            {
                CompleteTerminal(completedItem, completedResult);
            }

            Dictionary<long, TransitWorkItem> unresolvedById = [];

            IReadOnlyList<TransitWorkItem> unresolvedOwned = connection.DrainOutstandingOwnedWorkForRetry();
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
                await RequeueOrTerminalizeFailureAsync(
                    item,
                    TransitWorkFailureClass.ConnectionDisposed,
                    TransitTransmissionUncertainty.ConnectionFailedDuringSend,
                    cancellationToken).ConfigureAwait(false);
            }

        }

        /// <summary>
        /// Requeues a failed work item when retry budget remains, otherwise terminalizes it as failed or ambiguous according to the observed uncertainty.
        /// </summary>
        /// <param name="item">The work item to requeue or terminalize.</param>
        /// <param name="failureClass">The class of the failure that occurred.</param>
        /// <param name="uncertainty">The level of uncertainty associated with the failure.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
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
                return;
            }

            if (item.State == TransitWorkItemState.RetryPending)
            {
                return;
            }

            TimeSpan delay = ComputeRetryDelay(item.AttemptCount);
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
        }

        /// <summary>
        /// Applies one terminal publish result exactly once, updates lifetime counters, and releases the publisher's tracking of the work item.
        /// </summary>
        /// <param name="item">The work item to complete.</param>
        /// <param name="result">The terminal result to apply.</param>
        /// <param name="inFlightOwnershipAlreadyTransferred">Indicates whether in-flight ownership was already transferred before this terminalization path ran.</param>
        private void CompleteTerminal(TransitWorkItem item, TransitPublishResult result, bool inFlightOwnershipAlreadyTransferred = false)
        {
            bool transitioned = item.TryTransitionToTerminal(result.Status, result.Provenance, out TransitWorkItemState priorState);
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

            _ = _activeWorkItems.TryRemove(item.WorkItemId, out _);

            _ = result.Status switch
            {
                TransitPublishStatus.Accepted => Interlocked.Increment(ref _totalArticlesAccepted),
                TransitPublishStatus.Rejected => Interlocked.Increment(ref _totalArticlesRejected),
                TransitPublishStatus.Canceled => Interlocked.Increment(ref _totalArticlesCanceled),
                TransitPublishStatus.Queued
                or TransitPublishStatus.Unavailable
                or TransitPublishStatus.Failed => Interlocked.Increment(ref _totalArticlesFailed),
                TransitPublishStatus.Ambiguous => Interlocked.Increment(ref _totalArticlesAmbiguous),
                _ => Interlocked.Increment(ref _totalArticlesFailed),
            };
            _ = item.TrySetCompletionResult(result);
        }

        /// <summary>
        /// Forces every still-tracked admitted item to a terminal result during preemption or shutdown.
        /// </summary>
        /// <returns>A task that completes after all remaining owned work has been terminalized.</returns>
        private async Task ForceTerminalizeRemainingWorkAsync()
        {
            TransitWorkItem[] remaining = [.. _activeWorkItems.Values];
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

                bool transitioned = item.TryTransitionToTerminal(forced.Status, forced.Provenance, out TransitWorkItemState priorState);
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
                    case TransitWorkItemState.CompletedAccepted:
                    case TransitWorkItemState.CompletedRejected:
                    case TransitWorkItemState.CompletedFailed:
                    case TransitWorkItemState.CompletedCanceled:
                        break;
                    default:
                        break;
                }

                _ = _activeWorkItems.TryRemove(item.WorkItemId, out _);

                _ = forced.Status switch
                {
                    TransitPublishStatus.Accepted => Interlocked.Increment(ref _totalArticlesAccepted),
                    TransitPublishStatus.Rejected => Interlocked.Increment(ref _totalArticlesRejected),
                    TransitPublishStatus.Canceled => Interlocked.Increment(ref _totalArticlesCanceled),
                    TransitPublishStatus.Queued
                    or TransitPublishStatus.Unavailable
                    or TransitPublishStatus.Failed => Interlocked.Increment(ref _totalArticlesFailed),
                    TransitPublishStatus.Ambiguous => Interlocked.Increment(ref _totalArticlesAmbiguous),
                    _ => Interlocked.Increment(ref _totalArticlesFailed),
                };
                _ = item.TrySetCompletionResult(forced);
            }

            await Task.CompletedTask.ConfigureAwait(false);
        }


        /// <summary>
        /// Creates a connection for one slot, initializes it, and retries lifecycle failures while the publisher still has demand.
        /// </summary>
        /// <param name="slotIndex">The slot index being serviced.</param>
        /// <param name="reconnecting">Indicates whether the call is part of a reconnect path.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
        /// <returns>The initialized <see cref="TransitConnection"/>.</returns>
        private async Task<TransitConnection> CreateAndInitializeConnectionAsync(int slotIndex, bool reconnecting, CancellationToken cancellationToken)
        {
            _ = slotIndex;
            int consecutiveLifecycleInitializationFailures = 0;
            int attempt = 0;

            while (true)
            {
                if (reconnecting && !HasConnectionDemand())
                {
                    throw new NoConnectionDemandException();
                }

                attempt++;
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

                try
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (_disposeRequested)
                    {
                        throw new OperationCanceledException("Transit publisher shutdown in progress.", cancellationToken);
                    }

                    await connection.InitializeAsync(cancellationToken).ConfigureAwait(false);
                    return connection;
                }
                catch (Exception ex) when (IsConnectionLifecycleSubmitFailure(connection, ex))
                {
                    try
                    {
                        await connection.DisposeAsync().ConfigureAwait(false);
                    }
                    catch
                    {
                    }

                    cancellationToken.ThrowIfCancellationRequested();
                    if (_disposeRequested)
                    {
                        throw new OperationCanceledException("Transit publisher shutdown in progress.", cancellationToken);
                    }

                    bool hasOutstandingNow = HasOutstandingAdmittedWork();
                    _ = Interlocked.Increment(ref _totalReconnects);
                    consecutiveLifecycleInitializationFailures++;
                    bool thresholdReached = hasOutstandingNow && consecutiveLifecycleInitializationFailures >= _runtimeOptions.TransitRetryMaxAttempts;

                    if (thresholdReached)
                    {
                        await ForceTerminalizeRemainingWorkAsync().ConfigureAwait(false);
                        consecutiveLifecycleInitializationFailures = 0;
                    }

                    await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Determines whether the publisher still owns any admitted work that has not reached a terminal result.
        /// </summary>
        /// <returns><c>true</c> if at least one admitted item is still non-terminal; otherwise, <c>false</c>.</returns>
        private bool HasOutstandingAdmittedWork()
        {
            return _activeWorkItems.Values.Any(static item => !item.IsTerminal);
        }

        /// <summary>
        /// Determines whether queued, retry-pending, in-flight, or still-owned work justifies keeping connection workers active.
        /// </summary>
        /// <returns><c>true</c> if the publisher still has connection demand; otherwise, <c>false</c>.</returns>
        private bool HasConnectionDemand()
        {
            GlobalTransitWorkQueueSnapshot snapshot = _globalQueue.CaptureSnapshot();
            return snapshot.QueuedItemCount > 0
                || snapshot.RetryPendingCount > 0
                || snapshot.InFlightCount > 0
                || HasOutstandingAdmittedWork();
        }

        /// <summary>
        /// Resolves the initialization watchdog timeout that should govern a newly created connection.
        /// </summary>
        /// <param name="reconnecting">Indicates whether the connection is being reestablished rather than created for fresh demand.</param>
        /// <param name="hasOutstandingAdmittedWork">Indicates whether the publisher still owns admitted work while the connection starts.</param>
        /// <returns>The initialization response progress timeout that applies to the connection being created.</returns>
        private TimeSpan? ResolveInitializationResponseProgressTimeout(bool reconnecting, bool hasOutstandingAdmittedWork)
        {
            return reconnecting || hasOutstandingAdmittedWork
                ? _runtimeOptions.EffectiveTransitReconnectInitializationTimeout
                : _connectionResponseProgressTimeout;
        }

        /// <summary>
        /// Computes the bounded retry delay with per-attempt exponential backoff and jitter.
        /// </summary>
        /// <param name="attempt">The current retry attempt.</param>
        /// <returns>The computed retry delay.</returns>
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
        /// <param name="state">The new state to transition to.</param>
        private void TransitionState(TransitConnectionState state)
        {
            _state = state;
        }

        /// <summary>
        /// Sentinel cancellation used when a worker wakes but no connection demand remains.
        /// </summary>
        /// <remarks>
        /// The exception is used as a local control-flow signal so a reconnect attempt can stop without converting demand loss into a hard failure.
        /// </remarks>
        private sealed class NoConnectionDemandException : OperationCanceledException
        {
        }

        /// <summary>
        /// Classifies submit failures that should be treated as connection-lifecycle faults instead of application-level publish failures.
        /// </summary>
        /// <remarks>
        /// This predicate lets the worker distinguish connection ownership, protocol, and transport failures from article-level settlement failures so the
        /// reconnect path can retire or replace a connection without misclassifying the publish result.
        /// </remarks>
        /// <param name="connection">The connection on which the failure occurred.</param>
        /// <param name="exception">The exception representing the failure being classified.</param>
        /// <returns><c>true</c> if the failure should trigger connection-lifecycle handling; otherwise, <c>false</c>.</returns>
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
            return result;
        }

        /// <summary>
        /// Detects initialization-phase protocol failures from invalid-operation diagnostics so they can be routed through lifecycle handling.
        /// </summary>
        /// <remarks>
        /// Only handshake-state failures are considered here; once the connection has moved past initialization, the caller treats the exception according to
        /// the broader lifecycle classification rules.
        /// </remarks>
        /// <param name="connection">The connection on which the failure occurred.</param>
        /// <param name="exception">The exception representing the failure being classified.</param>
        /// <returns><c>true</c> if the failure is an initialization-phase protocol failure; otherwise, <c>false</c>.</returns>
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
        /// Starts disposing a connection after ownership has moved away from the slot and keeps the disposal task for later observation.
        /// </summary>
        /// <param name="connection">The connection to dispose asynchronously after handoff.</param>
        /// <returns>The disposal task that was scheduled for deferred observation.</returns>
        private Task TrackDeferredConnectionDisposal(TransitConnection connection)
        {
            ArgumentNullException.ThrowIfNull(connection);

            Task disposeTask = connection.DisposeAsync().AsTask();
            _deferredConnectionDisposals[connection.ConnectionId] = disposeTask;
            return disposeTask;
        }

        /// <summary>
        /// Observes one deferred connection-disposal task while preserving the publisher's best-effort teardown policy.
        /// </summary>
        /// <param name="disposalTask">The deferred disposal task to observe.</param>
        /// <returns>A task that completes after the disposal task has been observed.</returns>
        private static async Task AwaitDeferredConnectionDisposalAsync(Task disposalTask)
        {
            ArgumentNullException.ThrowIfNull(disposalTask);

            try
            {
                await disposalTask.ConfigureAwait(false);
            }
            catch
            {
            }
        }

        /// <summary>
        /// Awaits and then clears all deferred connection disposal tasks so slot handoffs do not leak unmanaged resources.
        /// </summary>
        /// <returns>A task that completes after every deferred connection disposal has been observed.</returns>
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
        /// Structured publisher diagnostics combining slot state, active connections, queue accounting, and pump-fault telemetry.
        /// </summary>
        /// <remarks>
        /// The snapshot is a publisher-owned diagnostic contract: it reports what is currently visible through the slot fence, the queue totals seen at the same
        /// moment, and any captured pump-fault state needed to explain why a reconnect or shutdown path behaved the way it did.
        /// </remarks>
        /// <param name="ConfiguredConnectionPoolSize">The configured size of the connection pool.</param>
        /// <param name="ConfiguredPerConnectionPipelineDepth">The configured pipeline depth per connection.</param>
        /// <param name="TotalReconnects">The total number of reconnects observed by the publisher.</param>
        /// <param name="QueuedSubmissionCount">The number of submissions currently queued for routing.</param>
        /// <param name="Slots">The per-slot snapshots captured under the slot-version fence.</param>
        /// <param name="Connections">The active connection snapshots paired with their slot indexes.</param>
        /// <param name="SubmissionTraceRecords">The submission-routing trace records collected for diagnostics.</param>
        /// <param name="PublishToConnectionTraceRecords">The publish-to-connection handoff trace records collected for diagnostics.</param>
        /// <param name="PumpFaultTelemetry">The optional pump-fault telemetry snapshot when a submission-pump fault was captured.</param>
        /// <param name="QueueSnapshot">The snapshot of the global transit work queue at the moment the diagnostics were captured.</param>
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
        /// Structured diagnostic snapshot for one publisher slot, including the connection currently owning the slot when one exists.
        /// </summary>
        /// <remarks>
        /// A slot may be empty while a replacement connection is still initializing outside the slot fence. The snapshot reflects visibility, not lifetime
        /// ownership of the retired connection object.
        /// </remarks>
        /// <param name="SlotIndex">The index of the slot.</param>
        /// <param name="HasCurrentConnection">Indicates whether the slot currently has an owning connection.</param>
        /// <param name="CurrentConnectionId">The identifier of the current connection, if any.</param>
        /// <param name="TotalSubmissionsRouted">The total number of submissions routed through the slot.</param>
        /// <param name="Reconnects">The total number of reconnects for the slot.</param>
        /// <param name="CreatedConnections">The total number of connections created for the slot.</param>
        /// <param name="MaxObservedInFlightDepth">The maximum observed in-flight depth for the slot.</param>
        /// <param name="WaitedForChannelReadabilityCount">The number of times the slot waited for channel readability.</param>
        /// <param name="WaitedForCompletionWhilePipelineFullCount">The number of times the slot waited for completion while the pipeline was full.</param>
        /// <param name="FirstReachedConfiguredDepthTick">The tick at which the slot first reached the configured pipeline depth.</param>
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
        /// Associates one slot with the diagnostics snapshot captured from the connection that owned it when the snapshot was taken.
        /// </summary>
        /// <remarks>
        /// The entry ties a visible slot index to the corresponding connection diagnostics so operator snapshots can distinguish slot ownership from retired
        /// connection history.
        /// </remarks>
        /// <param name="SlotIndex">The index of the slot.</param>
        /// <param name="ConnectionId">The identifier of the connection that owned the slot.</param>
        /// <param name="Snapshot">The diagnostics snapshot captured from that connection.</param>
        internal sealed record ConnectionDiagnosticsEntry(
            int SlotIndex,
            string ConnectionId,
            TransitConnection.TransitConnectionDiagnosticsSnapshot Snapshot);

        /// <summary>
        /// Trace record describing when one submission left the publisher front door and entered connection routing.
        /// </summary>
        /// <remarks>
        /// The record captures a stable handoff timeline and queue depth correlation point for diagnostics only; it does not participate in settlement or retry
        /// decisions.
        /// </remarks>
        /// <param name="MessageId">The article Message-ID associated with the submission.</param>
        /// <param name="RemovedFromSubmissionChannelTick">The tick at which the submission was removed from the submission channel.</param>
        /// <param name="PublishToConnectionInvokedTick">The tick at which publish-to-connection routing was invoked.</param>
        /// <param name="InFlightCountBeforeAdd">The in-flight count before adding the submission.</param>
        /// <param name="InFlightCountAfterAdd">The in-flight count after adding the submission.</param>
        /// <param name="WriteIntentQueueDepthAtPumpRead">The write-intent queue depth when the pump observed the submission.</param>
        internal readonly record struct SubmissionTraceRecord(
            string MessageId,
            long RemovedFromSubmissionChannelTick,
            long PublishToConnectionInvokedTick,
            int InFlightCountBeforeAdd,
            int InFlightCountAfterAdd,
            int WriteIntentQueueDepthAtPumpRead);

        /// <summary>
        /// Trace record describing one publish-to-connection handoff attempt and the timing around the TAKETHIS submit.
        /// </summary>
        /// <remarks>
        /// The timing fields are diagnostic correlation points used to understand slot routing latency and do not alter publish behavior.
        /// </remarks>
        /// <param name="MessageId">The article Message-ID associated with the handoff.</param>
        /// <param name="SlotIndex">The index of the slot handling the attempt.</param>
        /// <param name="MethodEntryTick">The tick at which the method was entered.</param>
        /// <param name="SelectedConnectionId">The identifier of the selected connection, if any.</param>
        /// <param name="BeforeSubmitTakethisTick">The tick captured immediately before the TAKETHIS submit.</param>
        /// <param name="AfterSubmitTakethisTick">The tick captured immediately after the TAKETHIS submit.</param>
        internal readonly record struct PublishToConnectionTraceRecord(
            string MessageId,
            int SlotIndex,
            long MethodEntryTick,
            string? SelectedConnectionId,
            long BeforeSubmitTakethisTick,
            long AfterSubmitTakethisTick);

        /// <summary>
        /// Structured telemetry describing one captured submission-pump fault and the derived classifications that explain it.
        /// </summary>
        /// <remarks>
        /// The snapshot is a diagnostic summary of a faulted pump state. It collects the first captured fault, the fault-time classification flags, and the
        /// measurement/context fields needed to explain whether a reconnect, shutdown, or queue-invariant path produced the fault.
        /// </remarks>
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
            /// Gets the captured fault type, or an empty string when none was recorded.
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
        /// Identifies the publisher component or lifecycle stage that originated a captured pump fault.
        /// </summary>
        /// <remarks>
        /// The value is a diagnostic origin classification used to correlate the fault with the reconnect or shutdown stage that observed it.
        /// </remarks>
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
        /// Indicates whether a fault was captured before or after the measurement window was closed.
        /// </summary>
        /// <remarks>
        /// This classification is used to distinguish faults that occurred while the pump was still under observation from faults captured after the observation
        /// boundary had settled.
        /// </remarks>
        internal enum PumpFaultMeasurementState
        {
            Unknown = 0,
            BeforeMeasurementEnd = 1,
            AfterMeasurementEnd = 2,
        }

        /// <summary>
        /// Buckets invalid-operation failures by the invariant or subsystem they most closely represent in pump-fault diagnostics.
        /// </summary>
        /// <remarks>
        /// The buckets are diagnostic classifications, not control-flow outcomes. They help explain whether a fault was closer to queue accounting, connection
        /// ownership, terminalization, or a different invalid-operation path.
        /// </remarks>
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
        /// Placeholder classification for whether producers had completed when the fault was captured.
        /// </summary>
        /// <remarks>
        /// These states currently exist to preserve the diagnostic shape for future fault analysis.
        /// </remarks>
        internal enum ProducerCompletionState
        {
            Unknown = 0,
            Incomplete = 1,
            Complete = 2,
        }

        /// <summary>
        /// Placeholder classification for whether dispatcher completion had been observed when the fault was captured.
        /// </summary>
        /// <remarks>
        /// These states currently exist to preserve the diagnostic shape for future fault analysis.
        /// </remarks>
        internal enum DispatchersCompletedState
        {
            Unknown = 0,
            Incomplete = 1,
            Complete = 2,
        }

        /// <summary>
        /// Aggregate counts for submission-pump faults, including initiating and cascade totals.
        /// </summary>
        /// <remarks>
        /// These counters are diagnostic aggregates only; they do not participate in publish settlement or retry decisions.
        /// </remarks>
        internal readonly record struct SubmissionPumpFaultCounts(
            long TotalFaultCount,
            long InitiatingFaultCount,
            long CascadeFaultCount);

        /// <summary>
        /// Buckets sanitized first-fault messages by the invariant or lifecycle pattern they match for diagnostics.
        /// </summary>
        /// <remarks>
        /// The sanitized class preserves enough detail to correlate fault messages without exposing the full raw exception text as the primary diagnostic key.
        /// </remarks>
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
