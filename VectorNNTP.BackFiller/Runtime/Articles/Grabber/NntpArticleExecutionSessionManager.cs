// <copyright file="NntpArticleExecutionSessionManager.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Grabber
// Session-management foundation that owns reusable authenticated acquisition-session lifetimes,
// single-work-item leasing, and deterministic session-health based recycle/reconnect behavior.

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
        /// Initializes a new execution session manager.
        /// </summary>
        /// <param name="logger">Logger used for lifecycle diagnostics.</param>
        /// <param name="options">Optional acquisition guardrails; defaults when null.</param>
        internal NntpArticleExecutionSessionManager(
            ILogger<NntpArticleExecutionSessionManager> logger,
            NntpArticleAcquisitionOptions? options = null)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _options = options ?? NntpArticleAcquisitionOptions.Default;
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

            foreach (NntpAccountSnapshot account in accounts)
            {
                int desiredConnections = Math.Max(0, (int)account.MaxConnections);
                for (int connectionIndex = 0; connectionIndex < desiredConnections; connectionIndex++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await CreateAndRegisterSlotAsync(account, connectionIndex, cancellationToken).ConfigureAwait(false);
                }
            }

            lock (_gate)
            {
                _initialized = true;
            }

            if (TotalSessionCount == 0)
            {
                throw new InvalidOperationException("No acquisition sessions could be initialized from the current account snapshot.");
            }
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
                int slotIndex = await _availableSlots.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                SessionSlot slot;

                lock (_gate)
                {
                    ObjectDisposedException.ThrowIf(_disposeRequested, this);

                    slot = _slots[slotIndex];
                    if (slot.Session is null || slot.Busy)
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

            lock (_gate)
            {
                slot = _slots[slotIndex];
                if (!slot.Busy)
                {
                    return;
                }

                slot.Busy = false;
                if (shouldRecycle)
                {
                    retiredSession = slot.Session;
                    slot.Session = null;
                }
            }

            if (retiredSession is not null)
            {
                await retiredSession.DisposeAsync().ConfigureAwait(false);
                LogSessionRetired(_logger, slot.SlotId, slot.Account.EntryId, failureCode);

                (NntpArticleAcquisitionSession? replacement, NntpArticleAcquisitionResult connectResult) = await NntpArticleAcquisitionSession.ConnectAsync(
                    slot.Endpoint,
                    _options,
                    slot.Logger,
                    CancellationToken.None).ConfigureAwait(false);

                using (connectResult)
                {
                    if (replacement is not null)
                    {
                        lock (_gate)
                        {
                            slot.Session = replacement;
                        }

                        LogSessionReconnected(_logger, slot.SlotId, slot.Account.EntryId, slot.Endpoint.Host, slot.Endpoint.Port);
                    }
                    else
                    {
                        LogSessionReconnectFailed(_logger, slot.SlotId, slot.Account.EntryId, connectResult.FailureCode, connectResult.ResponseText);
                    }
                }
            }

            bool requeueSlot;
            lock (_gate)
            {
                requeueSlot = !_disposeRequested && slot.Session is not null;
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

            lock (_gate)
            {
                if (_disposeRequested)
                {
                    return;
                }

                _disposeRequested = true;
                _ = _availableSlots.Writer.TryComplete();
                waitForLeases = _activeLeases == 0 ? Task.CompletedTask : _allLeasesReturned.Task;
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
        /// <returns>A task that completes when the slot is registered or skipped due to connection failure.</returns>
        private async Task CreateAndRegisterSlotAsync(NntpAccountSnapshot account, int connectionIndex, CancellationToken cancellationToken)
        {
            NntpArticleAcquisitionEndpoint endpoint = new(
                Host: account.Hostname,
                Port: account.Port,
                UseSsl: account.UseSsl,
                Username: account.Username,
                Password: account.Password);

            ILogger<NntpArticleAcquisitionSession> sessionLogger = NullLogger<NntpArticleAcquisitionSession>.Instance;

            (NntpArticleAcquisitionSession? session, NntpArticleAcquisitionResult result) = await NntpArticleAcquisitionSession.ConnectAsync(
                endpoint,
                _options,
                sessionLogger,
                cancellationToken).ConfigureAwait(false);

            using (result)
            {
                if (session is null)
                {
                    LogSessionSlotInitializationFailed(_logger, account.EntryId, connectionIndex, endpoint.Host, endpoint.Port, result.FailureCode, result.ResponseText);
                    return;
                }
            }

            NntpArticleAcquisitionSession connectedSession = session;
            int slotIndex;
            lock (_gate)
            {
                slotIndex = _slots.Count;
                _slots.Add(new SessionSlot(slotIndex, account, endpoint, connectedSession, sessionLogger));
            }

            _ = _availableSlots.Writer.TryWrite(slotIndex);
            LogSessionSlotReady(_logger, slotIndex, account.EntryId, endpoint.Host, endpoint.Port);
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
            internal SessionSlot(
                int slotId,
                NntpAccountSnapshot account,
                NntpArticleAcquisitionEndpoint endpoint,
                NntpArticleAcquisitionSession? session,
                ILogger<NntpArticleAcquisitionSession> logger)
            {
                SlotId = slotId;
                Account = account;
                Endpoint = endpoint;
                Session = session;
                Logger = logger;
            }

            /// <summary>
            /// Gets the stable slot index.
            /// </summary>
            internal int SlotId { get; }

            /// <summary>
            /// Gets the account snapshot associated with this slot.
            /// </summary>
            internal NntpAccountSnapshot Account { get; }

            /// <summary>
            /// Gets the endpoint associated with this slot.
            /// </summary>
            internal NntpArticleAcquisitionEndpoint Endpoint { get; }

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
        private static readonly Action<ILogger, int, Guid, NntpArticleAcquisitionFailureCode, string, Exception?> LogSessionReconnectFailedMessage =
            LoggerMessage.Define<int, Guid, NntpArticleAcquisitionFailureCode, string>(
                LogLevel.Warning,
                new EventId(3105, nameof(LogSessionReconnectFailed)),
                "Grabber session reconnect failed: Slot={SlotId}, Account={AccountEntryId}, FailureCode={FailureCode}, Detail={Detail}");

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
        /// <param name="detail">Reconnect attempt detail text.</param>
        private static void LogSessionReconnectFailed(ILogger logger, int slotId, Guid accountEntryId, NntpArticleAcquisitionFailureCode failureCode, string detail)
        {
            LogSessionReconnectFailedMessage(logger, slotId, accountEntryId, failureCode, detail, null);
        }
    }

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
