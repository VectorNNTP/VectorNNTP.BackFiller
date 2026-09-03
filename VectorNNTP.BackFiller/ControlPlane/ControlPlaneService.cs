// <copyright file="ControlPlaneService.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller ControlPlane
// Desired-state account reconciliation service that projects authoritative MySQL account snapshots
// into persistent NNTP execution session managers.

using System.Net.Security;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Accounts;
using VectorNNTP.Backfiller.Runtime.Articles.Grabber;
using VectorNNTP.Backfiller.Runtime.RabbitMq;

namespace VectorNNTP.Backfiller.ControlPlane
{
    /// <summary>
    /// Supplies backbone-scoped NNTP session leases from the control-plane managed account runtimes.
    /// </summary>
    internal interface IBackboneSessionLeaseProvider
    {
        /// <summary>
        /// Acquires one NNTP session lease for a requested backbone.
        /// </summary>
        /// <param name="backbone">Backbone namespace to route work against.</param>
        /// <param name="messageId">Message-ID used for lease correlation logging.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Exclusive NNTP session lease scoped to the selected account runtime.</returns>
        public ValueTask<NntpArticleSessionLease> AcquireSessionLeaseAsync(string backbone, string messageId, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Reconciles authoritative NNTP account snapshots into long-lived per-account session-manager runtimes for the
    /// process lifetime.
    /// </summary>
    /// <remarks>
    /// The service applies snapshot deltas (add/remove/reconcile), coordinates RabbitMQ capacity retirement boundaries,
    /// publishes backbone usable-capacity snapshots, and emits operational lifecycle diagnostics. It is not part of
    /// startup configuration validation; validation and startup-failure decisions are handled by startup pipeline types.
    /// </remarks>
    /// <param name="logger">The logger used for control-plane diagnostics.</param>
    /// <param name="timeProvider">The unified time provider used for control-plane timestamps.</param>
    /// <param name="snapshotProvider">The runtime NNTP account snapshot provider.</param>
    /// <param name="rabbitMqCapacityRetirementCoordinator">Manages retirement of RabbitMQ capacity during account reconciliation.</param>
    /// <param name="backboneUsableCapacityStateWriter">Optional writer for publishing usable backbone capacity state.</param>
    /// <param name="loggerFactory">The logger factory used to create account session-manager loggers.</param>
    /// <param name="serverCertificateValidationCallback">Optional per-acquisition-session TLS server-certificate validation callback. When <see langword="null"/>, acquisition sessions retain platform default certificate validation behavior.</param>
    internal sealed partial class ControlPlaneService(
        ILogger<ControlPlaneService> logger,
        TimeProvider timeProvider,
        MySqlNntpAccountSnapshotProvider snapshotProvider,
        IRabbitMqCapacityRetirementCoordinator rabbitMqCapacityRetirementCoordinator,
        IBackboneUsableCapacityStateWriter? backboneUsableCapacityStateWriter = null,
        ILoggerFactory? loggerFactory = null,
        RemoteCertificateValidationCallback? serverCertificateValidationCallback = null) : BackgroundService, IBackboneSessionLeaseProvider
    {
        /// <summary>
        /// Fixed cadence used for low-cost background-loop wakeups and debug heartbeat emission.
        /// </summary>
        private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(30);
        /// <summary>
        /// Target interval between snapshot refresh attempts driven by the heartbeat loop.
        /// </summary>
        private static readonly TimeSpan RefreshInterval = TimeSpan.FromSeconds(60);

        /// <summary>
        /// The logger used by this control-plane service instance.
        /// </summary>
        private readonly ILogger<ControlPlaneService> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        /// <summary>
        /// The unified time provider used for startup and heartbeat timestamps.
        /// </summary>
        private readonly TimeProvider _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));

        /// <summary>
        /// The runtime NNTP account snapshot provider.
        /// </summary>
        private readonly MySqlNntpAccountSnapshotProvider _snapshotProvider = snapshotProvider ?? throw new ArgumentNullException(nameof(snapshotProvider));

        /// <summary>
        /// The logger factory used for account-scoped session manager logging.
        /// </summary>
        private readonly ILoggerFactory _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;

        /// <summary>
        /// Optional per-acquisition-session TLS server-certificate validation callback used by created session managers.
        /// </summary>
        private readonly RemoteCertificateValidationCallback? _serverCertificateValidationCallback = serverCertificateValidationCallback;

        /// <summary>
        /// Cross-plane coordinator enforcing RabbitMQ retirement-drain before NNTP capacity retirement becomes effective.
        /// </summary>
        private readonly IRabbitMqCapacityRetirementCoordinator _rabbitMqCapacityRetirementCoordinator = rabbitMqCapacityRetirementCoordinator ?? throw new ArgumentNullException(nameof(rabbitMqCapacityRetirementCoordinator));

        /// <summary>
        /// Writes authoritative usable NNTP capacity snapshots for admission-control consumers.
        /// </summary>
        private readonly IBackboneUsableCapacityStateWriter _backboneUsableCapacityStateWriter = backboneUsableCapacityStateWriter ?? NoOpBackboneUsableCapacityStateWriter.Instance;

        /// <summary>
        /// Mutable account runtime map keyed by authoritative account entry identifier.
        /// </summary>
        private readonly Dictionary<Guid, AccountRuntimeState> _accountRuntimes = [];

        /// <summary>
        /// Synchronizes account runtime map updates.
        /// </summary>
        private readonly object _accountRuntimeGate = new();

        /// <summary>
        /// Gets a value indicating whether mandatory control-plane startup initialization completed.
        /// </summary>
        /// <value>
        /// <see langword="true"/> after <see cref="InitializeControlPlaneAsync(CancellationToken)"/> completes the
        /// initial snapshot reconciliation and startup barrier log point; otherwise <see langword="false"/>.
        /// </value>
        internal bool IsStartupInitializationComplete { get; private set; }

        /// <summary>
        /// Returns the number of currently managed account runtimes.
        /// </summary>
        /// <value>Thread-safe count of entries currently tracked in <see cref="_accountRuntimes"/>.</value>
        internal int ManagedAccountCount
        {
            get
            {
                lock (_accountRuntimeGate)
                {
                    return _accountRuntimes.Count;
                }
            }
        }

        /// <summary>
        /// Returns the number of currently active sessions for one managed account runtime.
        /// </summary>
        /// <param name="accountId">Stable account identifier.</param>
        /// <returns>Active session count, or zero when the account is not currently managed.</returns>
        internal int GetManagedAccountActiveSessionCount(Guid accountId)
        {
            lock (_accountRuntimeGate)
            {
                return _accountRuntimes.TryGetValue(accountId, out AccountRuntimeState? runtime)
                    ? runtime.Manager.ActiveSessionCount
                    : 0;
            }
        }

        /// <summary>
        /// Acquires one session lease for the requested backbone from the currently managed account runtimes.
        /// </summary>
        /// <param name="backbone">Backbone namespace to route work against.</param>
        /// <param name="messageId">Message-ID correlation value for session-leasing diagnostics.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Exclusive session lease for one account runtime matching <paramref name="backbone"/>.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="backbone"/> or <paramref name="messageId"/> is blank.</exception>
        /// <exception cref="InvalidOperationException">Thrown when no runtime currently exists for the requested backbone.</exception>
        /// <remarks>
        /// Matching runtimes are snapshot-copied under the control-plane gate, ordered deterministically by entry identifier,
        /// and then probed one-by-one outside the lock. Runtimes disposed or concurrently retired after the snapshot is taken
        /// are skipped so lease acquisition can continue against the remaining candidates.
        /// </remarks>
        public async ValueTask<NntpArticleSessionLease> AcquireSessionLeaseAsync(string backbone, string messageId, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(backbone);
            ArgumentException.ThrowIfNullOrWhiteSpace(messageId);

            List<AccountRuntimeState> candidates;
            lock (_accountRuntimeGate)
            {
                candidates = [.. _accountRuntimes.Values
                    .Where(runtime => string.Equals(runtime.LastAppliedAccount.Backbone, backbone, StringComparison.OrdinalIgnoreCase))
                    .OrderBy(static runtime => runtime.LastAppliedAccount.EntryId)];
            }

            if (candidates.Count == 0)
            {
                throw new InvalidOperationException($"No NNTP account runtime is currently available for backbone '{backbone}'.");
            }

            for (int candidateIndex = 0; candidateIndex < candidates.Count; candidateIndex++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                AccountRuntimeState candidate = candidates[candidateIndex];
                try
                {
                    return await candidate.Manager.AcquireAsync(messageId, cancellationToken).ConfigureAwait(false);
                }
                catch (ObjectDisposedException)
                {
                    continue;
                }
                catch (InvalidOperationException)
                {
                    continue;
                }
            }

            throw new InvalidOperationException($"No active NNTP session lease could be acquired for backbone '{backbone}'.");
        }

        /// <summary>
        /// Performs control-plane startup initialization before the background loop begins.
        /// </summary>
        /// <param name="cancellationToken">Token that cancels startup initialization.</param>
        /// <returns>A task that completes when the control plane has established its startup barrier.</returns>
        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            await InitializeControlPlaneAsync(cancellationToken).ConfigureAwait(false);
            await base.StartAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Executes the control-plane loop until the host requests cancellation.
        /// </summary>
        /// <param name="stoppingToken">The token that signals when processing should stop.</param>
        /// <returns>A task that completes when control-plane execution stops.</returns>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            long heartbeatCount = 0;
            int refreshEveryHeartbeats = Math.Max(1, (int)Math.Ceiling(RefreshInterval.TotalSeconds / HeartbeatInterval.TotalSeconds));

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    heartbeatCount++;

                    if (_logger.IsEnabled(LogLevel.Debug))
                    {
                        DateTimeOffset currentTime = _timeProvider.GetUtcNow();
                        LogControlPlaneRunning(_logger, currentTime);
                    }

                    if (heartbeatCount % refreshEveryHeartbeats == 0)
                    {
                        await TryRefreshNntpAccountsAsync(stoppingToken).ConfigureAwait(false);
                    }

                    await Task.Delay(HeartbeatInterval, stoppingToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Normal Generic Host shutdown: Task.Delay observes cancellation and exits cooperatively.
            }
            finally
            {
                await DisposeAllAccountRuntimesAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Performs startup-time desired-state reconciliation from the already-loaded snapshot.
        /// </summary>
        /// <param name="cancellationToken">Token that cancels startup initialization.</param>
        /// <returns>A task that completes once startup reconciliation has converged as far as possible.</returns>
        private async Task InitializeControlPlaneAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await ReconcileSnapshotAsync(_snapshotProvider.CurrentSnapshot, cancellationToken).ConfigureAwait(false);

            cancellationToken.ThrowIfCancellationRequested();

            if (_logger.IsEnabled(LogLevel.Information))
            {
                DateTimeOffset currentTime = _timeProvider.GetUtcNow();
                LogControlPlaneStartupInitialized(_logger, currentTime);
            }

            IsStartupInitializationComplete = true;
        }

        /// <summary>
        /// Refreshes account snapshots and runs desired-state reconciliation when new snapshot data is published.
        /// </summary>
        /// <param name="stoppingToken">Token that signals service shutdown.</param>
        /// <returns>A task that completes when one refresh/reconcile cycle finishes.</returns>
        private async Task TryRefreshNntpAccountsAsync(CancellationToken stoppingToken)
        {
            try
            {
                bool refreshed = await _snapshotProvider.RefreshSnapshotAsync(stoppingToken).ConfigureAwait(false);
                if (!refreshed)
                {
                    return;
                }

                await ReconcileSnapshotAsync(_snapshotProvider.CurrentSnapshot, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Normal shutdown cancellation; not an operational refresh failure.
            }
            catch (Exception ex)
            {
                LogNntpAccountRefreshFailed(_logger, ex);
            }
        }

        /// <summary>
        /// Executes one explicit refresh-and-reconcile cycle for tests and controlled runtime invocations.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token controlling refresh and reconciliation.</param>
        /// <returns>A task that completes when one refresh/reconcile cycle has finished.</returns>
        internal Task RefreshAndReconcileOnceAsync(CancellationToken cancellationToken)
        {
            return TryRefreshNntpAccountsAsync(cancellationToken);
        }

        /// <summary>
        /// Reconciles runtime account session managers to one authoritative snapshot state.
        /// </summary>
        /// <param name="snapshot">Authoritative snapshot to apply.</param>
        /// <param name="cancellationToken">Shutdown-aware cancellation token.</param>
        /// <returns>A task that completes when this snapshot has been applied as far as possible.</returns>
        private async Task ReconcileSnapshotAsync(NntpAccountSnapshotState snapshot, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(snapshot);

            Dictionary<Guid, NntpAccountSnapshot> desiredAccounts = snapshot.Accounts
                .Where(static account => account.MaxConnections > 0)
                .GroupBy(static account => account.EntryId)
                .ToDictionary(static group => group.Key, static group => group.Last());

            LogAccountReconciliationStarted(_logger, snapshot.ServerId, desiredAccounts.Count);

            List<(Guid AccountId, AccountRuntimeState Runtime)> accountsToRemove = [];
            lock (_accountRuntimeGate)
            {
                foreach ((Guid accountId, AccountRuntimeState runtime) in _accountRuntimes)
                {
                    if (!desiredAccounts.ContainsKey(accountId))
                    {
                        accountsToRemove.Add((accountId, runtime));
                    }
                }

                foreach ((Guid accountId, _) in accountsToRemove)
                {
                    _ = _accountRuntimes.Remove(accountId);
                }
            }

            PublishBackboneUsableCapacitySnapshot();

            foreach ((Guid accountId, AccountRuntimeState runtime) in accountsToRemove)
            {
                try
                {
                    await RetireRabbitMqCapacityBoundaryAsync(accountId, retainConnectionCount: 0, cancellationToken).ConfigureAwait(false);
                    LogAccountRemoved(_logger, accountId, runtime.LastAppliedAccount.Hostname, runtime.LastAppliedAccount.Port, runtime.LastAppliedAccount.UseSsl);
                    await runtime.Manager.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    LogAccountRemovalFailed(_logger, accountId, ex);
                }
            }

            foreach ((Guid accountId, NntpAccountSnapshot desiredAccount) in desiredAccounts)
            {
                cancellationToken.ThrowIfCancellationRequested();

                AccountRuntimeState? existingRuntime;
                lock (_accountRuntimeGate)
                {
                    existingRuntime = _accountRuntimes.GetValueOrDefault(accountId);
                }

                if (existingRuntime is null)
                {
                    await AddAccountRuntimeAsync(desiredAccount, cancellationToken).ConfigureAwait(false);
                    PublishBackboneUsableCapacitySnapshot();
                    continue;
                }

                try
                {
                    int previousCapacity = existingRuntime.LastAppliedAccount.MaxConnections;
                    int desiredCapacity = desiredAccount.MaxConnections;
                    if (desiredCapacity < previousCapacity)
                    {
                        await RetireRabbitMqCapacityBoundaryAsync(accountId, desiredCapacity, cancellationToken).ConfigureAwait(false);
                    }

                    NntpAccountSessionReconcileResult result = await existingRuntime.Manager.ReconcileAccountAsync(desiredAccount, cancellationToken).ConfigureAwait(false);
                    existingRuntime.LastAppliedAccount = desiredAccount;
                    PublishBackboneUsableCapacitySnapshot();
                    LogAccountReconciled(
                        _logger,
                        result.AccountEntryId,
                        desiredAccount.Hostname,
                        desiredAccount.Port,
                        desiredAccount.UseSsl,
                        result.DesiredSessionCount,
                        result.ActiveSessionCountBefore,
                        result.ActiveSessionCountAfter,
                        result.AddedSessionCount,
                        result.RetiredSessionCount,
                        result.KeepAliveUpdated,
                        result.ConnectionSettingsReplaced);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    LogAccountReconcileFailed(_logger, desiredAccount.EntryId, desiredAccount.Hostname, desiredAccount.Port, desiredAccount.UseSsl, ex);
                }
            }

            cancellationToken.ThrowIfCancellationRequested();
            PublishBackboneUsableCapacitySnapshot();
            LogAccountReconciliationCompleted(_logger, snapshot.ServerId, desiredAccounts.Count);
        }

        /// <summary>
        /// Creates and initializes a new account runtime session manager.
        /// </summary>
        /// <param name="account">Authoritative desired account state.</param>
        /// <param name="cancellationToken">Shutdown-aware cancellation token.</param>
        /// <returns>A task that completes when runtime creation attempt finishes.</returns>
        private async Task AddAccountRuntimeAsync(NntpAccountSnapshot account, CancellationToken cancellationToken)
        {
            NntpArticleExecutionSessionManager manager = new(
                _loggerFactory.CreateLogger<NntpArticleExecutionSessionManager>(),
                options: null,
                _timeProvider,
                _loggerFactory,
                _serverCertificateValidationCallback);

            try
            {
                await manager.InitializeAsync([account], cancellationToken).ConfigureAwait(false);
                AccountRuntimeState runtime = new(account, manager);

                lock (_accountRuntimeGate)
                {
                    _accountRuntimes[account.EntryId] = runtime;
                }

                LogAccountAdded(_logger, account.EntryId, account.Hostname, account.Port, account.UseSsl, account.MaxConnections, manager.ActiveSessionCount);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                await manager.DisposeAsync().ConfigureAwait(false);
                throw;
            }
            catch (Exception ex)
            {
                LogAccountAddFailed(_logger, account.EntryId, account.Hostname, account.Port, account.UseSsl, ex);
                await manager.DisposeAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Disposes all account session manager runtimes.
        /// </summary>
        /// <returns>A task that completes when all account managers are disposed.</returns>
        private async Task DisposeAllAccountRuntimesAsync()
        {
            List<(Guid AccountId, AccountRuntimeState Runtime)> runtimes;
            lock (_accountRuntimeGate)
            {
                runtimes = [.. _accountRuntimes.Select(static pair => (pair.Key, pair.Value))];
                _accountRuntimes.Clear();
            }

            foreach ((Guid accountId, AccountRuntimeState runtime) in runtimes)
            {
                try
                {
                    await runtime.Manager.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    LogAccountRemovalFailed(_logger, accountId, ex);
                }
            }

            PublishBackboneUsableCapacitySnapshot();
        }

        /// <summary>
        /// Aggregates active-session counts by backbone and publishes the resulting usable-capacity snapshot.
        /// </summary>
        /// <remarks>
        /// Capacity is computed from currently managed runtimes under <see cref="_accountRuntimeGate"/> and then
        /// published through <see cref="_backboneUsableCapacityStateWriter"/>.
        /// </remarks>
        private void PublishBackboneUsableCapacitySnapshot()
        {
            Dictionary<string, int> capacityByBackbone = new(StringComparer.OrdinalIgnoreCase);

            lock (_accountRuntimeGate)
            {
                foreach (AccountRuntimeState runtime in _accountRuntimes.Values)
                {
                    string backbone = runtime.LastAppliedAccount.Backbone;
                    if (string.IsNullOrWhiteSpace(backbone))
                    {
                        continue;
                    }

                    int activeSessionCount = runtime.Manager.ActiveSessionCount;
                    if (!capacityByBackbone.TryGetValue(backbone, out int current))
                    {
                        capacityByBackbone[backbone] = activeSessionCount;
                        continue;
                    }

                    capacityByBackbone[backbone] = current + activeSessionCount;
                }
            }

            _backboneUsableCapacityStateWriter.PublishSnapshot(capacityByBackbone);
        }

        /// <summary>
        /// Delegates RabbitMQ capacity retirement coordination for one account before NNTP capacity changes are applied.
        /// </summary>
        /// <param name="accountId">Authoritative account identifier whose RabbitMQ capacity boundary is being retired.</param>
        /// <param name="retainConnectionCount">Target connection count that must remain available after retirement.</param>
        /// <param name="cancellationToken">Cancellation token controlling boundary-retirement work.</param>
        /// <returns>A task representing the retirement-boundary operation.</returns>
        private Task RetireRabbitMqCapacityBoundaryAsync(Guid accountId, int retainConnectionCount, CancellationToken cancellationToken)
        {
            return _rabbitMqCapacityRetirementCoordinator
                .RetireCapacityAsync(accountId, retainConnectionCount, cancellationToken);
        }

        /// <summary>
        /// No-op usable-capacity writer used when no concrete capacity-state publisher is supplied.
        /// </summary>
        private sealed class NoOpBackboneUsableCapacityStateWriter : IBackboneUsableCapacityStateWriter
        {
            /// <summary>
            /// Singleton no-op writer instance used as the default fallback.
            /// </summary>
            internal static readonly NoOpBackboneUsableCapacityStateWriter Instance = new();

            /// <summary>
            /// Validates the published snapshot argument and intentionally performs no persistence/output.
            /// </summary>
            /// <param name="capacityByBackbone">Backbone capacity snapshot supplied by the control plane.</param>
            /// <exception cref="ArgumentNullException"><paramref name="capacityByBackbone"/> is <see langword="null"/>.</exception>
            public void PublishSnapshot(IReadOnlyDictionary<string, int> capacityByBackbone)
            {
                ArgumentNullException.ThrowIfNull(capacityByBackbone);
            }
        }

        /// <summary>
        /// Bundles one authoritative account snapshot with the long-lived session manager currently realizing it.
        /// </summary>
        private sealed record AccountRuntimeState(
            NntpAccountSnapshot LastAppliedAccount,
            NntpArticleExecutionSessionManager Manager)
        {
            /// <summary>
            /// Tracks the last authoritative account snapshot successfully applied to this runtime.
            /// </summary>
            /// <value>The most recently applied authoritative snapshot state for this account runtime.</value>
            internal NntpAccountSnapshot LastAppliedAccount { get; set; } = LastAppliedAccount;

            /// <summary>
            /// Exposes the persistent session manager owned for this account runtime.
            /// </summary>
            /// <value>Session-manager instance that executes acquisition work for this account.</value>
            internal NntpArticleExecutionSessionManager Manager { get; } = Manager;
        }

        /// <summary>
        /// Logs successful completion of the control-plane startup barrier.
        /// </summary>
        /// <param name="logger">The logger receiving the startup entry.</param>
        /// <param name="currentTime">The current timestamp captured for the startup barrier completion.</param>
        /// <remarks>
        /// Source-generated logger method. Emits an <see cref="LogLevel.Information"/> event with id 999 and
        /// structured field <c>CurrentTime</c> when information logging is enabled.
        /// </remarks>
        [LoggerMessage(EventId = 999, Level = LogLevel.Information, Message = "Control plane startup initialization completed at: {CurrentTime}")]
        private static partial void LogControlPlaneStartupInitialized(ILogger logger, DateTimeOffset currentTime);

        /// <summary>
        /// Logs the periodic control-plane heartbeat.
        /// </summary>
        /// <param name="logger">The logger receiving the heartbeat entry.</param>
        /// <param name="currentTime">The current timestamp captured for the heartbeat.</param>
        /// <remarks>
        /// Source-generated logger method. Emits a <see cref="LogLevel.Debug"/> event with id 1000 and structured
        /// field <c>CurrentTime</c> when debug logging is enabled.
        /// </remarks>
        [LoggerMessage(EventId = 1000, Level = LogLevel.Debug, Message = "Control plane running at: {CurrentTime}")]
        private static partial void LogControlPlaneRunning(ILogger logger, DateTimeOffset currentTime);

        /// <summary>
        /// Logs a periodic snapshot refresh failure.
        /// </summary>
        /// <param name="logger">The logger receiving the refresh-failure event.</param>
        /// <param name="exception">Exception instance recorded with the warning log entry.</param>
        /// <remarks>
        /// Source-generated logger method. Emits a <see cref="LogLevel.Warning"/> event with id 1001 and includes
        /// <paramref name="exception"/> for exception-aware logging.
        /// </remarks>
        [LoggerMessage(EventId = 1001, Level = LogLevel.Warning, Message = "Periodic NNTP account snapshot refresh failed")]
        private static partial void LogNntpAccountRefreshFailed(ILogger logger, Exception exception);

        /// <summary>
        /// Logs account reconciliation cycle start.
        /// </summary>
        /// <param name="logger">Logger receiving reconciliation events.</param>
        /// <param name="serverId">Authoritative server identifier of the snapshot.</param>
        /// <param name="desiredAccountCount">Number of enabled desired accounts in the snapshot.</param>
        [LoggerMessage(EventId = 1010, Level = LogLevel.Information, Message = "Account reconciliation started: ServerId={ServerId}, DesiredAccounts={DesiredAccountCount}")]
        private static partial void LogAccountReconciliationStarted(ILogger logger, int serverId, int desiredAccountCount);

        /// <summary>
        /// Logs account reconciliation cycle completion.
        /// </summary>
        /// <param name="logger">Logger receiving reconciliation events.</param>
        /// <param name="serverId">Authoritative server identifier of the snapshot.</param>
        /// <param name="desiredAccountCount">Number of enabled desired accounts in the snapshot.</param>
        [LoggerMessage(EventId = 1011, Level = LogLevel.Information, Message = "Account reconciliation completed: ServerId={ServerId}, DesiredAccounts={DesiredAccountCount}")]
        private static partial void LogAccountReconciliationCompleted(ILogger logger, int serverId, int desiredAccountCount);

        /// <summary>
        /// Logs new account runtime creation.
        /// </summary>
        /// <param name="logger">Logger receiving account lifecycle events.</param>
        /// <param name="accountId">Account identifier.</param>
        /// <param name="host">Account host.</param>
        /// <param name="port">Account port.</param>
        /// <param name="useSsl">Account SSL setting.</param>
        /// <param name="desiredConnections">Desired configured connections.</param>
        /// <param name="activeConnections">Successfully active persistent sessions.</param>
        [LoggerMessage(EventId = 1012, Level = LogLevel.Information, Message = "Account added: AccountId={AccountId}, Hostname={Host}, Port={Port}, SSL={UseSsl}, DesiredConnections={DesiredConnections}, ActiveConnections={ActiveConnections}")]
        private static partial void LogAccountAdded(ILogger logger, Guid accountId, string host, int port, bool useSsl, int desiredConnections, int activeConnections);

        /// <summary>
        /// Logs account runtime add failure.
        /// </summary>
        /// <param name="logger">Logger receiving account lifecycle events.</param>
        /// <param name="accountId">Account identifier.</param>
        /// <param name="host">Account host.</param>
        /// <param name="port">Account port.</param>
        /// <param name="useSsl">Account SSL setting.</param>
        /// <param name="exception">Failure exception.</param>
        [LoggerMessage(EventId = 1013, Level = LogLevel.Warning, Message = "Account add failed: AccountId={AccountId}, Hostname={Host}, Port={Port}, SSL={UseSsl}")]
        private static partial void LogAccountAddFailed(ILogger logger, Guid accountId, string host, int port, bool useSsl, Exception exception);

        /// <summary>
        /// Logs account runtime removal.
        /// </summary>
        /// <param name="logger">Logger receiving account lifecycle events.</param>
        /// <param name="accountId">Account identifier.</param>
        /// <param name="host">Account host.</param>
        /// <param name="port">Account port.</param>
        /// <param name="useSsl">Account SSL setting.</param>
        [LoggerMessage(EventId = 1014, Level = LogLevel.Information, Message = "Account removed: AccountId={AccountId}, Hostname={Host}, Port={Port}, SSL={UseSsl}")]
        private static partial void LogAccountRemoved(ILogger logger, Guid accountId, string host, int port, bool useSsl);

        /// <summary>
        /// Logs account runtime removal failure.
        /// </summary>
        /// <param name="logger">Logger receiving account lifecycle events.</param>
        /// <param name="accountId">Account identifier.</param>
        /// <param name="exception">Failure exception.</param>
        [LoggerMessage(EventId = 1015, Level = LogLevel.Warning, Message = "Account remove failed: AccountId={AccountId}")]
        private static partial void LogAccountRemovalFailed(ILogger logger, Guid accountId, Exception exception);

        /// <summary>
        /// Logs account reconcile capacity outcome.
        /// </summary>
        /// <param name="logger">Logger receiving account reconcile events.</param>
        /// <param name="accountId">Account identifier.</param>
        /// <param name="desiredConnections">Desired session count.</param>
        /// <param name="activeBefore">Active session count before reconcile.</param>
        /// <param name="activeAfter">Active session count after reconcile.</param>
        /// <param name="addedSessions">Sessions added in reconcile pass.</param>
        /// <param name="retiredSessions">Sessions retired in reconcile pass.</param>
        [LoggerMessage(EventId = 1016, Level = LogLevel.Information, Message = "Account reconciled capacity: AccountId={AccountId}, DesiredConnections={DesiredConnections}, ActiveBefore={ActiveBefore}, ActiveAfter={ActiveAfter}, AddedSessions={AddedSessions}, RetiredSessions={RetiredSessions}")]
        private static partial void LogAccountReconciled(ILogger logger, Guid accountId, int desiredConnections, int activeBefore, int activeAfter, int addedSessions, int retiredSessions);

        /// <summary>
        /// Logs account reconcile configuration outcome.
        /// </summary>
        /// <param name="logger">Logger receiving account reconcile events.</param>
        /// <param name="accountId">Account identifier.</param>
        /// <param name="host">Account host.</param>
        /// <param name="port">Account port.</param>
        /// <param name="useSsl">Account SSL setting.</param>
        /// <param name="keepAliveUpdated">Whether keepalive was updated in place.</param>
        /// <param name="connectionSettingsReplaced">Whether connection settings required session replacement.</param>
        [LoggerMessage(EventId = 1018, Level = LogLevel.Information, Message = "Account reconciled configuration: AccountId={AccountId}, Hostname={Host}, Port={Port}, SSL={UseSsl}, KeepAliveUpdated={KeepAliveUpdated}, ConnectionSettingsReplaced={ConnectionSettingsReplaced}")]
        private static partial void LogAccountReconciledConfigurationMessage(ILogger logger, Guid accountId, string host, int port, bool useSsl, bool keepAliveUpdated, bool connectionSettingsReplaced);

        /// <summary>
        /// Logs account reconcile outcome.
        /// </summary>
        /// <param name="logger">Logger receiving account reconcile events.</param>
        /// <param name="accountId">Account identifier.</param>
        /// <param name="host">Account host.</param>
        /// <param name="port">Account port.</param>
        /// <param name="useSsl">Account SSL setting.</param>
        /// <param name="desiredConnections">Desired session count.</param>
        /// <param name="activeBefore">Active session count before reconcile.</param>
        /// <param name="activeAfter">Active session count after reconcile.</param>
        /// <param name="addedSessions">Sessions added in reconcile pass.</param>
        /// <param name="retiredSessions">Sessions retired in reconcile pass.</param>
        /// <param name="keepAliveUpdated">Whether keepalive was updated in place.</param>
        /// <param name="connectionSettingsReplaced">Whether connection settings required session replacement.</param>
        private static void LogAccountReconciled(ILogger logger, Guid accountId, string host, int port, bool useSsl, int desiredConnections, int activeBefore, int activeAfter, int addedSessions, int retiredSessions, bool keepAliveUpdated, bool connectionSettingsReplaced)
        {
            LogAccountReconciled(logger, accountId, desiredConnections, activeBefore, activeAfter, addedSessions, retiredSessions);
            LogAccountReconciledConfigurationMessage(logger, accountId, host, port, useSsl, keepAliveUpdated, connectionSettingsReplaced);
        }

        /// <summary>
        /// Logs account reconcile failure.
        /// </summary>
        /// <param name="logger">Logger receiving account reconcile events.</param>
        /// <param name="accountId">Account identifier.</param>
        /// <param name="host">Account host.</param>
        /// <param name="port">Account port.</param>
        /// <param name="useSsl">Account SSL setting.</param>
        /// <param name="exception">Failure exception.</param>
        [LoggerMessage(EventId = 1017, Level = LogLevel.Warning, Message = "Account reconcile failed: AccountId={AccountId}, Hostname={Host}, Port={Port}, SSL={UseSsl}")]
        private static partial void LogAccountReconcileFailed(ILogger logger, Guid accountId, string host, int port, bool useSsl, Exception exception);
    }
}
