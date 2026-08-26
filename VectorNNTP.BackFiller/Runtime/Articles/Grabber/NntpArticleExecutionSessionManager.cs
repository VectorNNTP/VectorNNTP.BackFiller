// <copyright file="NntpArticleExecutionSessionManager.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Grabber
// Session-management foundation that owns reusable authenticated acquisition-session lifetimes,
// single-work-item leasing, and deterministic session-health based recycle/reconnect behavior.

using System.Net.Security;
using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Accounts;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;

namespace VectorNNTP.Backfiller.Runtime.Articles.Grabber
{
    /// <summary>
    /// Classifies whether an acquisition outcome should keep the underlying session reusable.
    /// </summary>
    internal static class NntpArticleSessionHealthClassifier
    {
        /// <summary>
        /// Returns a value indicating whether the session is still safe to reuse after the specified acquisition outcome.
        /// </summary>
        /// <param name="failureCode">Terminal acquisition outcome for the lease operation.</param>
        /// <returns><see langword="true"/> when the session remains reusable; otherwise <see langword="false"/>.</returns>
        /// <remarks>
        /// Unknown enum values are treated conservatively as non-reusable to avoid returning potentially unhealthy sessions to the pool.
        /// </remarks>
        internal static bool IsSessionReusable(NntpArticleAcquisitionFailureCode failureCode)
        {
            return failureCode switch
            {
                NntpArticleAcquisitionFailureCode.None => true,
                NntpArticleAcquisitionFailureCode.InvalidMessageId => true,
                NntpArticleAcquisitionFailureCode.ArticleNotFound => true,
                NntpArticleAcquisitionFailureCode.RemoteRejected => true,
                NntpArticleAcquisitionFailureCode.ConnectionFailure => false,
                NntpArticleAcquisitionFailureCode.Timeout => false,
                NntpArticleAcquisitionFailureCode.MalformedResponse => false,
                NntpArticleAcquisitionFailureCode.TruncatedArticle => false,
                NntpArticleAcquisitionFailureCode.ArticleTooLarge => false,
                NntpArticleAcquisitionFailureCode.Cancelled => false,
                NntpArticleAcquisitionFailureCode.ProtocolFailure => false,
                NntpArticleAcquisitionFailureCode.AuthenticationFailure => false,
                _ => false,
            };
        }
    }

    /// <summary>
    /// Owns reusable authenticated NNTP acquisition sessions and enforces one active ARTICLE operation per session lease.
    /// </summary>
    /// <remarks>
    /// <para>This manager is the ownership boundary for session lifetime, authentication state retention,
    /// reconnect decisions, and lease-based concurrency.</para>
    /// <para>Work dispatchers acquire a lease, execute one workflow operation, report acquisition outcome,
    /// and release the lease. The manager then decides whether to reuse or recycle/reconnect the session.</para>
    /// </remarks>
    internal sealed partial class NntpArticleExecutionSessionManager : IAsyncDisposable
    {
        /// <summary>
        /// Logger used for session pool lifecycle and lease diagnostics.
        /// </summary>
        private readonly ILogger<NntpArticleExecutionSessionManager> _logger;

        /// <summary>
        /// Immutable acquisition guardrails reused by all owned sessions.
        /// </summary>
        private readonly NntpArticleAcquisitionOptions _options;

        /// <summary>
        /// Queue of ready slot indexes available for leasing.
        /// </summary>
        private readonly Channel<int> _availableSlots;

        /// <summary>
        /// Time provider used for deterministic UTC idle tracking and keepalive scheduling.
        /// </summary>
        private readonly TimeProvider _timeProvider;

        /// <summary>
        /// Logger factory used to create acquisition-session category loggers for protocol-level diagnostics.
        /// </summary>
        private readonly ILoggerFactory _loggerFactory;

        /// <summary>
        /// Optional per-session TLS server-certificate validation callback used for acquisition session connects.
        /// </summary>
        private readonly RemoteCertificateValidationCallback? _serverCertificateValidationCallback;

        /// <summary>
        /// Cancellation source used to stop keepalive maintenance during disposal.
        /// </summary>
        private readonly CancellationTokenSource _maintenanceCancellationSource = new();

        /// <summary>
        /// Background maintenance task that services idle authenticated sessions.
        /// </summary>
        private Task? _keepAliveMaintenanceTask;

        /// <summary>
        /// Mutable session slots keyed by slot index.
        /// </summary>
        private readonly List<SessionSlot> _slots = [];

        /// <summary>
        /// Synchronizes slot and lifecycle state.
        /// </summary>
        private readonly object _gate = new();

        /// <summary>
        /// Completion source tracking whether all active leases have returned.
        /// </summary>
        private TaskCompletionSource<bool> _allLeasesReturned = CreateCompletedLeaseCompletionSource();

        /// <summary>
        /// Indicates whether initialization completed successfully.
        /// </summary>
        private bool _initialized;

        /// <summary>
        /// Indicates disposal has started and no new leases should be issued.
        /// </summary>
        private bool _disposeRequested;

        /// <summary>
        /// Number of currently active leases.
        /// </summary>
        private int _activeLeases;

        /// <summary>
        /// Number of acquisition callers currently blocked waiting for an available slot.
        /// </summary>
        private int _pendingAcquireWaiters;

        /// <summary>
        /// Fixed maintenance cadence used while scanning for idle keepalive work.
        /// </summary>
        private static readonly TimeSpan KeepAliveMaintenanceInterval = TimeSpan.FromSeconds(1);

        /// <summary>
        /// Initializes a new execution session manager.
        /// </summary>
        /// <param name="logger">Logger used for lifecycle diagnostics.</param>
        /// <param name="options">Optional acquisition guardrails; defaults when null.</param>
        /// <param name="timeProvider">Optional time provider for UTC idle tracking and keepalive scheduling.</param>
        /// <param name="loggerFactory">Optional logger factory used for acquisition-session protocol logger creation.</param>
        /// <param name="serverCertificateValidationCallback">Optional per-session TLS server-certificate validation callback. When <see langword="null"/>, acquisition sessions use platform default certificate validation semantics.</param>
        internal NntpArticleExecutionSessionManager(
            ILogger<NntpArticleExecutionSessionManager> logger,
            NntpArticleAcquisitionOptions? options = null,
            TimeProvider? timeProvider = null,
            ILoggerFactory? loggerFactory = null,
            RemoteCertificateValidationCallback? serverCertificateValidationCallback = null)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _options = options ?? NntpArticleAcquisitionOptions.Default;
            _timeProvider = timeProvider ?? TimeProvider.System;
            _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
            _serverCertificateValidationCallback = serverCertificateValidationCallback;
            _availableSlots = Channel.CreateUnbounded<int>(new UnboundedChannelOptions
            {
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
            });
        }

        /// <summary>
        /// Gets the total configured session-slot count.
        /// </summary>
        internal int TotalSessionCount
        {
            get
            {
                lock (_gate)
                {
                    return _slots.Count;
                }
            }
        }

        /// <summary>
        /// Connects and authenticates acquisition sessions for all configured account slots.
        /// </summary>
        /// <param name="accounts">Runtime account snapshot entries defining endpoints, credentials, and connection counts.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A task that completes when at least one ready session is available.</returns>
        /// <exception cref="InvalidOperationException">Thrown when initialization is attempted more than once or no sessions become ready.</exception>
        internal async Task InitializeAsync(IReadOnlyList<NntpAccountSnapshot> accounts, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(accounts);

            lock (_gate)
            {
                if (_initialized)
                {
                    throw new InvalidOperationException("Session manager is already initialized.");
                }

                ObjectDisposedException.ThrowIf(_disposeRequested, this);
            }

            List<Task> allConnectionTasks = [];
            foreach (NntpAccountSnapshot account in accounts)
            {
                int desiredConnections = Math.Max(0, (int)account.MaxConnections);
                for (int connectionIndex = 0; connectionIndex < desiredConnections; connectionIndex++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    allConnectionTasks.Add(CreateAndRegisterSlotAsync(account, connectionIndex, cancellationToken));
                }
            }

            if (allConnectionTasks.Count > 0)
            {
                await Task.WhenAll(allConnectionTasks).ConfigureAwait(false);
            }

            lock (_gate)
            {
                _initialized = true;
            }

            if (TotalSessionCount == 0)
            {
                throw new InvalidOperationException("No acquisition sessions could be initialized from the current account snapshot.");
            }

            _keepAliveMaintenanceTask = RunKeepAliveMaintenanceAsync(_maintenanceCancellationSource.Token);
        }

        /// <summary>
        /// Gets the number of currently connected sessions owned by this manager.
        /// </summary>
        internal int ActiveSessionCount
        {
            get
            {
                lock (_gate)
                {
                    return _slots.Count(static slot => slot.Session is not null);
                }
            }
        }

        /// <summary>
        /// Reconciles the owned persistent session state for this account to the latest desired snapshot.
        /// </summary>
        /// <param name="desiredAccount">Authoritative desired account configuration.</param>
        /// <param name="cancellationToken">Cancellation token for shutdown-aware reconciliation work.</param>
        /// <returns>Deterministic reconciliation summary for control-plane diagnostics.</returns>
        /// <exception cref="InvalidOperationException">Thrown when called before manager initialization.</exception>
        internal async Task<NntpAccountSessionReconcileResult> ReconcileAccountAsync(
            NntpAccountSnapshot desiredAccount,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(desiredAccount);

            lock (_gate)
            {
                if (!_initialized)
                {
                    throw new InvalidOperationException("Session manager must be initialized before reconciliation.");
                }

                ObjectDisposedException.ThrowIf(_disposeRequested, this);
            }

            bool keepAliveUpdated = false;
            bool connectionSettingsChanged = false;
            int activeBefore;
            int desiredSessionCount = Math.Max(0, (int)desiredAccount.MaxConnections);

            lock (_gate)
            {
                activeBefore = _slots.Count(static slot => slot.Session is not null);
                foreach (SessionSlot slot in _slots)
                {
                    if (slot.Account.EntryId != desiredAccount.EntryId)
                    {
                        continue;
                    }

                    if (!connectionSettingsChanged && HasConnectionSettingsChanged(slot.Account, desiredAccount))
                    {
                        connectionSettingsChanged = true;
                    }

                    if (!keepAliveUpdated && slot.Account.KeepAliveSeconds != desiredAccount.KeepAliveSeconds)
                    {
                        keepAliveUpdated = true;
                    }
                }

                foreach (SessionSlot slot in _slots)
                {
                    if (slot.Account.EntryId != desiredAccount.EntryId)
                    {
                        continue;
                    }

                    slot.Account = desiredAccount;
                    if (connectionSettingsChanged)
                    {
                        slot.Endpoint = BuildEndpoint(desiredAccount);
                    }
                }
            }

            (int Requested, List<SessionSlot> ImmediateRetirements) retirement = connectionSettingsChanged
                ? RequestRetirement(desiredAccount.EntryId, int.MaxValue, reconnectOnRetire: true)
                : (0, []);

            int retiredSessions = retirement.Requested;
            await ProcessImmediateRetirementsAsync(retirement.ImmediateRetirements, cancellationToken).ConfigureAwait(false);

            int activeAfterRetire;
            int pendingReconnectRetire;
            lock (_gate)
            {
                activeAfterRetire = _slots.Count(static slot => slot.Session is not null);
                pendingReconnectRetire = _slots.Count(static slot => slot.RetireRequested && slot.ReconnectOnRelease);
            }

            if (!connectionSettingsChanged && activeAfterRetire > desiredSessionCount)
            {
                retirement = RequestRetirement(
                    desiredAccount.EntryId,
                    activeAfterRetire - desiredSessionCount,
                    reconnectOnRetire: false);

                retiredSessions += retirement.Requested;
                await ProcessImmediateRetirementsAsync(retirement.ImmediateRetirements, cancellationToken).ConfigureAwait(false);

                lock (_gate)
                {
                    activeAfterRetire = _slots.Count(static slot => slot.Session is not null);
                    pendingReconnectRetire = _slots.Count(static slot => slot.RetireRequested && slot.ReconnectOnRelease);
                }
            }

            int targetWithPendingReconnect = activeAfterRetire + pendingReconnectRetire;
            int addCount = Math.Max(0, desiredSessionCount - targetWithPendingReconnect);

            List<Task<bool>> addConnectionTasks = [];
            for (int connectionIndex = 0; connectionIndex < addCount; connectionIndex++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                addConnectionTasks.Add(CreateAndRegisterSlotAsync(desiredAccount, TotalSessionCount + connectionIndex, cancellationToken));
            }

            int addedSessions = 0;
            if (addConnectionTasks.Count > 0)
            {
                bool[] results = await Task.WhenAll(addConnectionTasks).ConfigureAwait(false);
                addedSessions = results.Count(static r => r);
            }

            int activeAfter;
            lock (_gate)
            {
                activeAfter = _slots.Count(static slot => slot.Session is not null);
            }

            return new NntpAccountSessionReconcileResult(
                desiredAccount.EntryId,
                desiredSessionCount,
                activeBefore,
                activeAfter,
                addedSessions,
                retiredSessions,
                keepAliveUpdated,
                connectionSettingsChanged);
        }

        /// <summary>
        /// Acquires one available session lease for processing a single work item.
        /// </summary>
        /// <param name="messageId">Canonical Message-ID used for correlation logging.</param>
        /// <param name="cancellationToken">Cancellation token for backpressure waiting.</param>
        /// <returns>A lease that owns one active session assignment until disposed.</returns>
        internal async ValueTask<NntpArticleSessionLease> AcquireAsync(string messageId, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(messageId);

            lock (_gate)
            {
                if (!_initialized)
                {
                    throw new InvalidOperationException("Session manager must be initialized before acquiring sessions.");
                }

                ObjectDisposedException.ThrowIf(_disposeRequested, this);
            }

            while (true)
            {
                lock (_gate)
                {
                    _pendingAcquireWaiters++;
                }

                int slotIndex;
                try
                {
                    slotIndex = await _availableSlots.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    lock (_gate)
                    {
                        if (_pendingAcquireWaiters > 0)
                        {
                            _pendingAcquireWaiters--;
                        }
                    }
                }

                SessionSlot slot;

                lock (_gate)
                {
                    ObjectDisposedException.ThrowIf(_disposeRequested, this);

                    slot = _slots[slotIndex];
                    slot.Enqueued = false;
                    if (slot.Session is null || slot.Busy || slot.RetireRequested)
                    {
                        continue;
                    }

                    slot.Busy = true;
                    if (_activeLeases == 0)
                    {
                        _allLeasesReturned = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    }

                    _activeLeases++;
                }

                LogSessionLeaseAcquired(_logger, messageId, slot.SlotId, slot.Account.EntryId, slot.Endpoint.Host, slot.Endpoint.Port);
                return new NntpArticleSessionLease(this, slotIndex, slot.Session);
            }
        }

        /// <summary>
        /// Releases one session lease and applies deterministic session-health policy.
        /// </summary>
        /// <param name="slotIndex">Slot index that was leased.</param>
        /// <param name="failureCode">Terminal acquisition outcome for the completed work item.</param>
        /// <returns>A task that completes once release/reconnect handling has finished.</returns>
        internal async ValueTask ReleaseAsync(int slotIndex, NntpArticleAcquisitionFailureCode failureCode)
        {
            SessionSlot slot;
            NntpArticleAcquisitionSession? retiredSession = null;
            bool shouldRecycle = !NntpArticleSessionHealthClassifier.IsSessionReusable(failureCode);
            bool reconnectAfterRetire;

            lock (_gate)
            {
                slot = _slots[slotIndex];
                if (!slot.Busy)
                {
                    return;
                }

                slot.Busy = false;
                slot.LastArticleActivityUtc = _timeProvider.GetUtcNow();
                slot.LastKeepAliveProbeUtc = null;

                bool shouldRetire = shouldRecycle || slot.RetireRequested;
                reconnectAfterRetire = shouldRetire && (shouldRecycle || slot.ReconnectOnRelease);
                if (shouldRetire)
                {
                    retiredSession = slot.Session;
                    slot.Session = null;
                    slot.RetireRequested = false;
                    slot.ReconnectOnRelease = false;
                }
            }

            if (retiredSession is not null)
            {
                await retiredSession.DisposeAsync().ConfigureAwait(false);
                LogSessionRetired(_logger, slot.SlotId, slot.Account.EntryId, failureCode);

                if (reconnectAfterRetire)
                {
                    (NntpArticleAcquisitionSession? replacement, NntpArticleAcquisitionResult connectResult) = await NntpArticleAcquisitionSession.ConnectAsync(
                        slot.Endpoint,
                        _options,
                        slot.Logger,
                        CancellationToken.None,
                        _serverCertificateValidationCallback).ConfigureAwait(false);

                    using (connectResult)
                    {
                        if (replacement is not null)
                        {
                            lock (_gate)
                            {
                                slot.Session = replacement;
                                slot.LastArticleActivityUtc = _timeProvider.GetUtcNow();
                                slot.LastKeepAliveProbeUtc = null;
                            }

                            LogSessionReconnected(_logger, slot.SlotId, slot.Account.EntryId, slot.Endpoint.Host, slot.Endpoint.Port);
                        }
                        else
                        {
                            LogSessionReconnectFailed(_logger, slot.SlotId, slot.Account.EntryId, connectResult.FailureCode, connectResult.ResponseCode, connectResult.ResponseText);
                        }
                    }
                }
            }

            bool requeueSlot;
            lock (_gate)
            {
                requeueSlot = !_disposeRequested && slot.Session is not null && !slot.Enqueued;
                if (requeueSlot)
                {
                    slot.Enqueued = true;
                }
            }

            if (requeueSlot)
            {
                _ = _availableSlots.Writer.TryWrite(slotIndex);
            }

            TaskCompletionSource<bool>? leasesCompleted = null;
            lock (_gate)
            {
                if (_activeLeases > 0)
                {
                    _activeLeases--;
                }

                if (_activeLeases == 0)
                {
                    leasesCompleted = _allLeasesReturned;
                }
            }

            _ = leasesCompleted?.TrySetResult(true);
        }

        /// <summary>
        /// Disposes all owned sessions after waiting for active leases to return.
        /// </summary>
        /// <returns>A task that completes when all sessions are disposed.</returns>
        public async ValueTask DisposeAsync()
        {
            Task waitForLeases;
            List<NntpArticleAcquisitionSession> sessionsToDispose;

            Task? maintenanceTask;

            lock (_gate)
            {
                if (_disposeRequested)
                {
                    return;
                }

                _disposeRequested = true;
                _ = _availableSlots.Writer.TryComplete();
                _maintenanceCancellationSource.Cancel();
                maintenanceTask = _keepAliveMaintenanceTask;
                waitForLeases = _activeLeases == 0 ? Task.CompletedTask : _allLeasesReturned.Task;
            }

            if (maintenanceTask is not null)
            {
                try
                {
                    await maintenanceTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
            }

            await waitForLeases.ConfigureAwait(false);

            lock (_gate)
            {
                sessionsToDispose = [.. _slots
                    .Select(static slot => slot.Session)
                    .Where(static session => session is not null)
                    .Cast<NntpArticleAcquisitionSession>()];

                foreach (SessionSlot slot in _slots)
                {
                    slot.Session = null;
                    slot.Busy = false;
                }
            }

            foreach (NntpArticleAcquisitionSession session in sessionsToDispose)
            {
                await session.DisposeAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Creates one session slot for the specified account and enqueues it when ready.
        /// </summary>
        /// <param name="account">Source account snapshot entry.</param>
        /// <param name="connectionIndex">0-based connection index for this account.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns><see langword="true"/> when a connected slot was registered; otherwise <see langword="false"/>.</returns>
        private async Task<bool> CreateAndRegisterSlotAsync(NntpAccountSnapshot account, int connectionIndex, CancellationToken cancellationToken)
        {
            NntpArticleAcquisitionEndpoint endpoint = BuildEndpoint(account);

            ILogger<NntpArticleAcquisitionSession> sessionLogger = _loggerFactory.CreateLogger<NntpArticleAcquisitionSession>();

            (NntpArticleAcquisitionSession? session, NntpArticleAcquisitionResult result) = await NntpArticleAcquisitionSession.ConnectAsync(
                endpoint,
                _options,
                sessionLogger,
                cancellationToken,
                _serverCertificateValidationCallback).ConfigureAwait(false);

            using (result)
            {
                if (session is null)
                {
                    if (result.FailureCode == NntpArticleAcquisitionFailureCode.Cancelled && cancellationToken.IsCancellationRequested)
                    {
                        throw new OperationCanceledException(cancellationToken);
                    }

                    string detail = result.ResponseCode.HasValue
                        ? string.Create(System.Globalization.CultureInfo.InvariantCulture, $"ResponseCode={result.ResponseCode.Value}, Detail={result.ResponseText}")
                        : result.ResponseText;
                    LogSessionSlotInitializationFailed(_logger, account.EntryId, connectionIndex, endpoint.Host, endpoint.Port, result.FailureCode, detail);
                    return false;
                }
            }

            NntpArticleAcquisitionSession connectedSession = session;
            int slotIndex;
            lock (_gate)
            {
                slotIndex = _slots.Count;
                _slots.Add(new SessionSlot(slotIndex, account, endpoint, connectedSession, sessionLogger, _timeProvider.GetUtcNow()));
            }

            lock (_gate)
            {
                _slots[slotIndex].Enqueued = true;
            }

            _ = _availableSlots.Writer.TryWrite(slotIndex);
            LogSessionSlotReady(_logger, slotIndex, account.EntryId, endpoint.Host, endpoint.Port);
            return true;
        }

        /// <summary>
        /// Runs background keepalive maintenance for idle authenticated sessions.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token controlling maintenance shutdown.</param>
        /// <returns>A task that completes when maintenance stops.</returns>
        private async Task RunKeepAliveMaintenanceAsync(CancellationToken cancellationToken)
        {
            using PeriodicTimer timer = new(KeepAliveMaintenanceInterval, _timeProvider);

            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                await ServiceIdleKeepAlivesAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Probes idle slots with DATE keepalive while preserving one-session/one-active-ARTICLE semantics.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token controlling shutdown.</param>
        /// <returns>A task that completes after one maintenance pass.</returns>
        private async Task ServiceIdleKeepAlivesAsync(CancellationToken cancellationToken)
        {
            DateTimeOffset nowUtc = _timeProvider.GetUtcNow();
            List<(int SlotIndex, SessionSlot Slot)> candidates = [];

            lock (_gate)
            {
                if (_disposeRequested)
                {
                    return;
                }

                if (_pendingAcquireWaiters > 0)
                {
                    return;
                }

                for (int slotIndex = 0; slotIndex < _slots.Count; slotIndex++)
                {
                    SessionSlot slot = _slots[slotIndex];
                    if (slot.Session is null || slot.Busy || slot.RetireRequested)
                    {
                        continue;
                    }

                    TimeSpan idleThreshold = CalculateKeepAliveThreshold(slot.Account.KeepAliveSeconds);
                    if (idleThreshold <= TimeSpan.Zero)
                    {
                        continue;
                    }

                    if (nowUtc - slot.LastArticleActivityUtc <= idleThreshold)
                    {
                        continue;
                    }

                    if (slot.LastKeepAliveProbeUtc is not null && nowUtc - slot.LastKeepAliveProbeUtc <= idleThreshold)
                    {
                        continue;
                    }

                    slot.Busy = true;
                    slot.Enqueued = false;
                    candidates.Add((slotIndex, slot));
                }
            }

            foreach ((int slotIndex, SessionSlot slot) in candidates)
            {
                await ProbeSlotKeepAliveAsync(slotIndex, slot, nowUtc, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Sends DATE to one idle slot and applies deterministic reuse/reconnect handling.
        /// </summary>
        /// <param name="slotIndex">Slot index being probed.</param>
        /// <param name="slot">Slot state snapshot captured under lock.</param>
        /// <param name="probeUtc">UTC timestamp when probe pass began.</param>
        /// <param name="cancellationToken">Cancellation token controlling shutdown.</param>
        /// <returns>A task that completes after keepalive handling for the slot.</returns>
        private async Task ProbeSlotKeepAliveAsync(int slotIndex, SessionSlot slot, DateTimeOffset probeUtc, CancellationToken cancellationToken)
        {
            NntpArticleAcquisitionSession? session = slot.Session;
            if (session is null)
            {
                bool shouldRequeue;
                lock (_gate)
                {
                    slot.Busy = false;
                    shouldRequeue = !_disposeRequested && !slot.Enqueued;
                    if (shouldRequeue)
                    {
                        slot.Enqueued = true;
                    }
                }

                if (shouldRequeue)
                {
                    _ = _availableSlots.Writer.TryWrite(slotIndex);
                }

                return;
            }

            NntpArticleAcquisitionResult keepAliveResult = await session.KeepAliveWithDateAsync(cancellationToken).ConfigureAwait(false);
            using (keepAliveResult)
            {
                if (keepAliveResult.FailureCode == NntpArticleAcquisitionFailureCode.None)
                {
                    bool shouldRequeue;
                    lock (_gate)
                    {
                        slot.Busy = false;
                        slot.LastKeepAliveProbeUtc = probeUtc;
                        shouldRequeue = !_disposeRequested && !slot.Enqueued;
                        if (shouldRequeue)
                        {
                            slot.Enqueued = true;
                        }
                    }

                    LogSessionKeepAliveSucceeded(_logger, slot.SlotId, slot.Account.EntryId, slot.Endpoint.Host, slot.Endpoint.Port);
                    if (shouldRequeue)
                    {
                        _ = _availableSlots.Writer.TryWrite(slotIndex);
                    }

                    return;
                }

                if (keepAliveResult.FailureCode == NntpArticleAcquisitionFailureCode.Cancelled && cancellationToken.IsCancellationRequested)
                {
                    bool shouldRequeue;
                    lock (_gate)
                    {
                        slot.Busy = false;
                        shouldRequeue = !_disposeRequested && !slot.Enqueued;
                        if (shouldRequeue)
                        {
                            slot.Enqueued = true;
                        }
                    }

                    if (shouldRequeue)
                    {
                        _ = _availableSlots.Writer.TryWrite(slotIndex);
                    }

                    return;
                }

                LogSessionKeepAliveFailed(_logger, slot.SlotId, slot.Account.EntryId, keepAliveResult.FailureCode, keepAliveResult.ResponseCode, keepAliveResult.ResponseText);
                await ReleaseKeepAliveFailureAsync(slotIndex, slot, keepAliveResult.FailureCode, keepAliveResult.ResponseText).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Applies deterministic recycle/reconnect behavior after a keepalive failure.
        /// </summary>
        /// <param name="slotIndex">Slot index that failed keepalive.</param>
        /// <param name="slot">Slot state to recycle.</param>
        /// <param name="failureCode">Typed keepalive failure code.</param>
        /// <param name="detail">Failure detail text for diagnostics.</param>
        /// <returns>A task that completes after recycle/reconnect handling.</returns>
        private async Task ReleaseKeepAliveFailureAsync(int slotIndex, SessionSlot slot, NntpArticleAcquisitionFailureCode failureCode, string detail)
        {
            NntpArticleAcquisitionSession? retiredSession;

            lock (_gate)
            {
                retiredSession = slot.Session;
                slot.Session = null;
                slot.Busy = false;
                slot.Enqueued = false;
            }

            if (retiredSession is not null)
            {
                await retiredSession.DisposeAsync().ConfigureAwait(false);
            }

            LogSessionRetired(_logger, slot.SlotId, slot.Account.EntryId, failureCode);

            (NntpArticleAcquisitionSession? replacement, NntpArticleAcquisitionResult connectResult) = await NntpArticleAcquisitionSession.ConnectAsync(
                slot.Endpoint,
                _options,
                slot.Logger,
                CancellationToken.None,
                _serverCertificateValidationCallback).ConfigureAwait(false);

            using (connectResult)
            {
                if (replacement is not null)
                {
                    bool shouldRequeue;
                    lock (_gate)
                    {
                        slot.Session = replacement;
                        slot.LastArticleActivityUtc = _timeProvider.GetUtcNow();
                        slot.LastKeepAliveProbeUtc = null;
                        shouldRequeue = !_disposeRequested && !slot.Enqueued;
                        if (shouldRequeue)
                        {
                            slot.Enqueued = true;
                        }
                    }

                    LogSessionReconnected(_logger, slot.SlotId, slot.Account.EntryId, slot.Endpoint.Host, slot.Endpoint.Port);
                    if (shouldRequeue)
                    {
                        _ = _availableSlots.Writer.TryWrite(slotIndex);
                    }
                }
                else
                {
                    LogSessionReconnectFailed(_logger, slot.SlotId, slot.Account.EntryId, connectResult.FailureCode, connectResult.ResponseCode, string.IsNullOrEmpty(connectResult.ResponseText) ? detail : connectResult.ResponseText);
                }
            }
        }

        /// <summary>
        /// Requests graceful retirement for sessions belonging to one account.
        /// </summary>
        /// <param name="accountEntryId">Stable account identifier.</param>
        /// <param name="retireCount">Maximum number of sessions to retire.</param>
        /// <param name="reconnectOnRetire">When <see langword="true"/>, sessions reconnect after retirement using current endpoint settings.</param>
        /// <returns>Retirement request summary containing requested count and immediately-retirable idle sessions.</returns>
        private (int Requested, List<SessionSlot> ImmediateRetirements) RequestRetirement(
            Guid accountEntryId,
            int retireCount,
            bool reconnectOnRetire)
        {
            if (retireCount <= 0)
            {
                return (0, []);
            }

            int requested = 0;
            List<SessionSlot> immediateRetirements = [];

            lock (_gate)
            {
                foreach (SessionSlot slot in _slots)
                {
                    if (requested >= retireCount)
                    {
                        break;
                    }

                    if (slot.Account.EntryId != accountEntryId || slot.Session is null || slot.RetireRequested)
                    {
                        continue;
                    }

                    slot.RetireRequested = true;
                    slot.ReconnectOnRelease = reconnectOnRetire;
                    requested++;

                    if (!slot.Busy)
                    {
                        slot.Busy = true;
                        slot.Enqueued = false;
                        immediateRetirements.Add(slot);
                    }
                }
            }

            return (requested, immediateRetirements);
        }

        /// <summary>
        /// Processes immediately-retirable idle sessions that were marked for retirement by reconciliation.
        /// </summary>
        /// <param name="immediateRetirements">Idle session slots already marked busy and retire-requested under the manager gate.</param>
        /// <param name="cancellationToken">Cancellation token for shutdown-aware reconciliation processing.</param>
        /// <returns>A task that completes after all immediate retirements are applied.</returns>
        private async Task ProcessImmediateRetirementsAsync(IReadOnlyList<SessionSlot> immediateRetirements, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(immediateRetirements);

            foreach (SessionSlot slot in immediateRetirements)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await ReleaseAsync(slot.SlotId, NntpArticleAcquisitionFailureCode.None).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Builds immutable connection endpoint settings from one account snapshot.
        /// </summary>
        /// <param name="account">Account snapshot.</param>
        /// <returns>Endpoint settings used for NNTP session connects.</returns>
        private static NntpArticleAcquisitionEndpoint BuildEndpoint(NntpAccountSnapshot account)
        {
            return new NntpArticleAcquisitionEndpoint(
                Host: account.Hostname,
                Port: account.Port,
                UseSsl: account.UseSsl,
                Username: account.Username,
                Password: account.Password);
        }

        /// <summary>
        /// Determines whether account changes require transport/session recreation.
        /// </summary>
        /// <param name="current">Current account state.</param>
        /// <param name="desired">Desired account state.</param>
        /// <returns><see langword="true"/> when session recreation is required; otherwise <see langword="false"/>.</returns>
        private static bool HasConnectionSettingsChanged(NntpAccountSnapshot current, NntpAccountSnapshot desired)
        {
            return !string.Equals(current.Hostname, desired.Hostname, StringComparison.Ordinal) ||
                current.Port != desired.Port ||
                current.UseSsl != desired.UseSsl ||
                !string.Equals(current.Username, desired.Username, StringComparison.Ordinal) ||
                !string.Equals(current.Password, desired.Password, StringComparison.Ordinal);
        }

        /// <summary>
        /// Calculates the proactive keepalive threshold from configured remote idle timeout seconds.
        /// </summary>
        /// <param name="keepAliveSeconds">Configured account keepalive timeout in seconds.</param>
        /// <returns>Threshold used to trigger DATE before expected remote idle close.</returns>
        private static TimeSpan CalculateKeepAliveThreshold(byte keepAliveSeconds)
        {
            if (keepAliveSeconds == 0)
            {
                return TimeSpan.Zero;
            }

            TimeSpan configuredIdleTimeout = TimeSpan.FromSeconds(keepAliveSeconds);
            TimeSpan safetyMargin = TimeSpan.FromSeconds(Math.Max(1, keepAliveSeconds / 10));
            return configuredIdleTimeout <= safetyMargin ? TimeSpan.FromSeconds(1) : configuredIdleTimeout - safetyMargin;
        }

        /// <summary>
        /// Creates a completed completion source for zero-active-lease initialization state.
        /// </summary>
        /// <returns>Completed lease completion source.</returns>
        private static TaskCompletionSource<bool> CreateCompletedLeaseCompletionSource()
        {
            TaskCompletionSource<bool> source = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _ = source.TrySetResult(true);
            return source;
        }

        /// <summary>
        /// Mutable slot state containing one account association and optional connected session.
        /// </summary>
        private sealed class SessionSlot
        {
            /// <summary>
            /// Initializes a new mutable session slot.
            /// </summary>
            /// <param name="slotId">Stable slot index.</param>
            /// <param name="account">Owning account snapshot.</param>
            /// <param name="endpoint">Endpoint associated with this slot.</param>
            /// <param name="session">Current connected session when healthy.</param>
            /// <param name="logger">Per-slot acquisition session logger.</param>
            /// <param name="articleActivityUtc">UTC timestamp of the most recent completed ARTICLE operation.</param>
            internal SessionSlot(
                int slotId,
                NntpAccountSnapshot account,
                NntpArticleAcquisitionEndpoint endpoint,
                NntpArticleAcquisitionSession? session,
                ILogger<NntpArticleAcquisitionSession> logger,
                DateTimeOffset articleActivityUtc)
            {
                SlotId = slotId;
                Account = account;
                Endpoint = endpoint;
                Session = session;
                Logger = logger;
                LastArticleActivityUtc = articleActivityUtc;
            }

            /// <summary>
            /// Gets the stable slot index.
            /// </summary>
            internal int SlotId { get; }

            /// <summary>
            /// Gets or sets the account snapshot associated with this slot.
            /// </summary>
            internal NntpAccountSnapshot Account { get; set; }

            /// <summary>
            /// Gets or sets the endpoint associated with this slot.
            /// </summary>
            internal NntpArticleAcquisitionEndpoint Endpoint { get; set; }

            /// <summary>
            /// Gets or sets the currently connected session for this slot.
            /// </summary>
            internal NntpArticleAcquisitionSession? Session { get; set; }

            /// <summary>
            /// Gets the logger used by this slot's acquisition session.
            /// </summary>
            internal ILogger<NntpArticleAcquisitionSession> Logger { get; }

            /// <summary>
            /// Gets or sets a value indicating whether the slot is currently leased.
            /// </summary>
            internal bool Busy { get; set; }

            /// <summary>
            /// Gets or sets the UTC time of the most recently completed ARTICLE operation for this slot.
            /// </summary>
            internal DateTimeOffset LastArticleActivityUtc { get; set; }

            /// <summary>
            /// Gets or sets the UTC time of the last successful DATE keepalive probe for this slot.
            /// </summary>
            internal DateTimeOffset? LastKeepAliveProbeUtc { get; set; }

            /// <summary>
            /// Gets or sets a value indicating whether a ready-slot token is currently present in the lease queue.
            /// </summary>
            internal bool Enqueued { get; set; }

            /// <summary>
            /// Gets or sets a value indicating whether this slot should be retired on lease release instead of being reused.
            /// </summary>
            internal bool RetireRequested { get; set; }

            /// <summary>
            /// Gets or sets a value indicating whether retirement should reconnect the session using current endpoint settings.
            /// </summary>
            internal bool ReconnectOnRelease { get; set; }
        }

        /// <summary>
        /// Precompiled delegate for slot-ready lifecycle entries.
        /// </summary>
        private static readonly Action<ILogger, int, Guid, string, int, Exception?> LogSessionSlotReadyMessage =
            LoggerMessage.Define<int, Guid, string, int>(
                LogLevel.Information,
                new EventId(3100, nameof(LogSessionSlotReady)),
                "Grabber session slot ready: Slot={SlotId}, Account={AccountEntryId}, Endpoint={Host}:{Port}");

        /// <summary>
        /// Precompiled delegate for slot-initialization failures.
        /// </summary>
        private static readonly Action<ILogger, Guid, int, string, int, NntpArticleAcquisitionFailureCode, string, Exception?> LogSessionSlotInitializationFailedMessage =
            LoggerMessage.Define<Guid, int, string, int, NntpArticleAcquisitionFailureCode, string>(
                LogLevel.Warning,
                new EventId(3101, nameof(LogSessionSlotInitializationFailed)),
                "Grabber session slot initialization failed: Account={AccountEntryId}, ConnectionIndex={ConnectionIndex}, Endpoint={Host}:{Port}, FailureCode={FailureCode}, Detail={Detail}");

        /// <summary>
        /// Precompiled delegate for lease-acquired entries.
        /// </summary>
        private static readonly Action<ILogger, string, int, Guid, string, int, Exception?> LogSessionLeaseAcquiredMessage =
            LoggerMessage.Define<string, int, Guid, string, int>(
                LogLevel.Debug,
                new EventId(3102, nameof(LogSessionLeaseAcquired)),
                "Grabber session lease acquired: MessageId={MessageId}, Slot={SlotId}, Account={AccountEntryId}, Endpoint={Host}:{Port}");

        /// <summary>
        /// Precompiled delegate for deterministic session retirement entries.
        /// </summary>
        private static readonly Action<ILogger, int, Guid, NntpArticleAcquisitionFailureCode, Exception?> LogSessionRetiredMessage =
            LoggerMessage.Define<int, Guid, NntpArticleAcquisitionFailureCode>(
                LogLevel.Warning,
                new EventId(3103, nameof(LogSessionRetired)),
                "Grabber session retired: Slot={SlotId}, Account={AccountEntryId}, FailureCode={FailureCode}");

        /// <summary>
        /// Precompiled delegate for successful slot reconnection entries.
        /// </summary>
        private static readonly Action<ILogger, int, Guid, string, int, Exception?> LogSessionReconnectedMessage =
            LoggerMessage.Define<int, Guid, string, int>(
                LogLevel.Information,
                new EventId(3104, nameof(LogSessionReconnected)),
                "Grabber session reconnected: Slot={SlotId}, Account={AccountEntryId}, Endpoint={Host}:{Port}");

        /// <summary>
        /// Precompiled delegate for failed slot reconnection entries.
        /// </summary>
        private static readonly Action<ILogger, int, Guid, NntpArticleAcquisitionFailureCode, int?, string, Exception?> LogSessionReconnectFailedMessage =
            LoggerMessage.Define<int, Guid, NntpArticleAcquisitionFailureCode, int?, string>(
                LogLevel.Warning,
                new EventId(3105, nameof(LogSessionReconnectFailed)),
                "Grabber session reconnect failed: Slot={SlotId}, Account={AccountEntryId}, FailureCode={FailureCode}, ResponseCode={ResponseCode}, Detail={Detail}");

        /// <summary>
        /// Precompiled delegate for successful DATE keepalive probes.
        /// </summary>
        private static readonly Action<ILogger, int, Guid, string, int, Exception?> LogSessionKeepAliveSucceededMessage =
            LoggerMessage.Define<int, Guid, string, int>(
                LogLevel.Debug,
                new EventId(3106, nameof(LogSessionKeepAliveSucceeded)),
                "Grabber session keepalive succeeded: Slot={SlotId}, Account={AccountEntryId}, Endpoint={Host}:{Port}");

        /// <summary>
        /// Precompiled delegate for failed DATE keepalive probes.
        /// </summary>
        private static readonly Action<ILogger, int, Guid, NntpArticleAcquisitionFailureCode, int?, string, Exception?> LogSessionKeepAliveFailedMessage =
            LoggerMessage.Define<int, Guid, NntpArticleAcquisitionFailureCode, int?, string>(
                LogLevel.Warning,
                new EventId(3107, nameof(LogSessionKeepAliveFailed)),
                "Grabber session keepalive failed: Slot={SlotId}, Account={AccountEntryId}, FailureCode={FailureCode}, ResponseCode={ResponseCode}, Detail={Detail}");

        /// <summary>
        /// Logs successful slot readiness.
        /// </summary>
        /// <param name="logger">Logger.</param>
        /// <param name="slotId">Slot identifier.</param>
        /// <param name="accountEntryId">Owning account identifier.</param>
        /// <param name="host">Endpoint host.</param>
        /// <param name="port">Endpoint port.</param>
        private static void LogSessionSlotReady(ILogger logger, int slotId, Guid accountEntryId, string host, int port)
        {
            LogSessionSlotReadyMessage(logger, slotId, accountEntryId, host, port, null);
        }

        /// <summary>
        /// Logs slot initialization failure details.
        /// </summary>
        /// <param name="logger">Logger.</param>
        /// <param name="accountEntryId">Owning account identifier.</param>
        /// <param name="connectionIndex">Per-account connection index.</param>
        /// <param name="host">Endpoint host.</param>
        /// <param name="port">Endpoint port.</param>
        /// <param name="failureCode">Deterministic acquisition failure code.</param>
        /// <param name="detail">Failure detail text.</param>
        private static void LogSessionSlotInitializationFailed(ILogger logger, Guid accountEntryId, int connectionIndex, string host, int port, NntpArticleAcquisitionFailureCode failureCode, string detail)
        {
            LogSessionSlotInitializationFailedMessage(logger, accountEntryId, connectionIndex, host, port, failureCode, detail, null);
        }

        /// <summary>
        /// Logs lease acquisition for one work item and slot.
        /// </summary>
        /// <param name="logger">Logger.</param>
        /// <param name="messageId">Message-ID correlation value.</param>
        /// <param name="slotId">Slot identifier.</param>
        /// <param name="accountEntryId">Owning account identifier.</param>
        /// <param name="host">Endpoint host.</param>
        /// <param name="port">Endpoint port.</param>
        private static void LogSessionLeaseAcquired(ILogger logger, string messageId, int slotId, Guid accountEntryId, string host, int port)
        {
            LogSessionLeaseAcquiredMessage(logger, messageId, slotId, accountEntryId, host, port, null);
        }

        /// <summary>
        /// Logs deterministic retirement of a previously active session.
        /// </summary>
        /// <param name="logger">Logger.</param>
        /// <param name="slotId">Slot identifier.</param>
        /// <param name="accountEntryId">Owning account identifier.</param>
        /// <param name="failureCode">Outcome that triggered session retirement.</param>
        private static void LogSessionRetired(ILogger logger, int slotId, Guid accountEntryId, NntpArticleAcquisitionFailureCode failureCode)
        {
            LogSessionRetiredMessage(logger, slotId, accountEntryId, failureCode, null);
        }

        /// <summary>
        /// Logs successful session reconnection for an existing slot.
        /// </summary>
        /// <param name="logger">Logger.</param>
        /// <param name="slotId">Slot identifier.</param>
        /// <param name="accountEntryId">Owning account identifier.</param>
        /// <param name="host">Endpoint host.</param>
        /// <param name="port">Endpoint port.</param>
        private static void LogSessionReconnected(ILogger logger, int slotId, Guid accountEntryId, string host, int port)
        {
            LogSessionReconnectedMessage(logger, slotId, accountEntryId, host, port, null);
        }

        /// <summary>
        /// Logs failed reconnection details for an existing slot.
        /// </summary>
        /// <param name="logger">Logger.</param>
        /// <param name="slotId">Slot identifier.</param>
        /// <param name="accountEntryId">Owning account identifier.</param>
        /// <param name="failureCode">Reconnect attempt failure code.</param>
        /// <param name="responseCode">Optional NNTP response code when available.</param>
        /// <param name="detail">Reconnect attempt detail text.</param>
        private static void LogSessionReconnectFailed(ILogger logger, int slotId, Guid accountEntryId, NntpArticleAcquisitionFailureCode failureCode, int? responseCode, string detail)
        {
            LogSessionReconnectFailedMessage(logger, slotId, accountEntryId, failureCode, responseCode, detail, null);
        }

        /// <summary>
        /// Logs a successful DATE keepalive probe for one slot.
        /// </summary>
        /// <param name="logger">Logger.</param>
        /// <param name="slotId">Slot identifier.</param>
        /// <param name="accountEntryId">Owning account identifier.</param>
        /// <param name="host">Endpoint host.</param>
        /// <param name="port">Endpoint port.</param>
        private static void LogSessionKeepAliveSucceeded(ILogger logger, int slotId, Guid accountEntryId, string host, int port)
        {
            LogSessionKeepAliveSucceededMessage(logger, slotId, accountEntryId, host, port, null);
        }

        /// <summary>
        /// Logs a failed DATE keepalive probe for one slot.
        /// </summary>
        /// <param name="logger">Logger.</param>
        /// <param name="slotId">Slot identifier.</param>
        /// <param name="accountEntryId">Owning account identifier.</param>
        /// <param name="failureCode">Deterministic keepalive failure code.</param>
        /// <param name="responseCode">Optional NNTP response code when available.</param>
        /// <param name="detail">Failure detail text.</param>
        private static void LogSessionKeepAliveFailed(ILogger logger, int slotId, Guid accountEntryId, NntpArticleAcquisitionFailureCode failureCode, int? responseCode, string detail)
        {
            LogSessionKeepAliveFailedMessage(logger, slotId, accountEntryId, failureCode, responseCode, detail, null);
        }

    }

    /// <summary>
    /// Represents deterministic reconciliation results for one account session-manager pass.
    /// </summary>
    /// <param name="AccountEntryId">Stable account identifier reconciled by this pass.</param>
    /// <param name="DesiredSessionCount">Desired persistent session count from the authoritative account snapshot.</param>
    /// <param name="ActiveSessionCountBefore">Active connected session count before reconciliation.</param>
    /// <param name="ActiveSessionCountAfter">Active connected session count after reconciliation.</param>
    /// <param name="AddedSessionCount">Number of sessions added during this pass.</param>
    /// <param name="RetiredSessionCount">Number of sessions marked for retirement during this pass.</param>
    /// <param name="KeepAliveUpdated">Whether keepalive settings were updated in place for existing sessions.</param>
    /// <param name="ConnectionSettingsReplaced">Whether connection settings changed and required session replacement behavior.</param>
    internal readonly record struct NntpAccountSessionReconcileResult(
        Guid AccountEntryId,
        int DesiredSessionCount,
        int ActiveSessionCountBefore,
        int ActiveSessionCountAfter,
        int AddedSessionCount,
        int RetiredSessionCount,
        bool KeepAliveUpdated,
        bool ConnectionSettingsReplaced);

    /// <summary>
    /// Represents one exclusive assignment of an acquisition session from the manager.
    /// </summary>
    internal sealed class NntpArticleSessionLease : IAsyncDisposable
    {
        /// <summary>
        /// Owning manager that issued the lease.
        /// </summary>
        private readonly NntpArticleExecutionSessionManager _owner;

        /// <summary>
        /// Slot index associated with this lease.
        /// </summary>
        private readonly int _slotIndex;

        /// <summary>
        /// Ensures release runs exactly once.
        /// </summary>
        private int _released;

        /// <summary>
        /// Terminal acquisition outcome reported by the caller for session-health classification.
        /// </summary>
        private NntpArticleAcquisitionFailureCode _completionCode = NntpArticleAcquisitionFailureCode.None;

        /// <summary>
        /// Initializes a new lease.
        /// </summary>
        /// <param name="owner">Owning manager.</param>
        /// <param name="slotIndex">Assigned slot index.</param>
        /// <param name="session">Assigned acquisition session.</param>
        internal NntpArticleSessionLease(
            NntpArticleExecutionSessionManager owner,
            int slotIndex,
            NntpArticleAcquisitionSession session)
        {
            _owner = owner ?? throw new ArgumentNullException(nameof(owner));
            _slotIndex = slotIndex;
            Session = session ?? throw new ArgumentNullException(nameof(session));
        }

        /// <summary>
        /// Gets the leased session that may execute exactly one active ARTICLE operation at a time.
        /// </summary>
        internal NntpArticleAcquisitionSession Session { get; }

        /// <summary>
        /// Reports the terminal acquisition outcome for this lease operation.
        /// </summary>
        /// <param name="failureCode">Terminal acquisition outcome.</param>
        internal void ReportAcquisitionOutcome(NntpArticleAcquisitionFailureCode failureCode)
        {
            _completionCode = failureCode;
        }

        /// <summary>
        /// Releases this lease back to the manager.
        /// </summary>
        /// <returns>A task that completes when manager release/reconnect handling is finished.</returns>
        public ValueTask DisposeAsync()
        {
            return Interlocked.Exchange(ref _released, 1) != 0 ? ValueTask.CompletedTask : _owner.ReleaseAsync(_slotIndex, _completionCode);
        }
    }
}
