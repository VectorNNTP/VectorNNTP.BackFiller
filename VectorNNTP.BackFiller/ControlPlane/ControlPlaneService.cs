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

namespace VectorNNTP.Backfiller.ControlPlane
{
    /// <summary>
    /// Runs the control-plane background loop for the service lifetime.
    /// </summary>
    /// <param name="logger">The logger used for control-plane diagnostics.</param>
    /// <param name="timeProvider">The unified time provider used for control-plane timestamps.</param>
    /// <param name="snapshotProvider">The runtime NNTP account snapshot provider.</param>
    /// <param name="loggerFactory">The logger factory used to create account session-manager loggers.</param>
    /// <param name="serverCertificateValidationCallback">Optional per-acquisition-session TLS server-certificate validation callback. When <see langword="null"/>, acquisition sessions retain platform default certificate validation behavior.</param>
    internal sealed partial class ControlPlaneService(
        ILogger<ControlPlaneService> logger,
        TimeProvider timeProvider,
        MySqlNntpAccountSnapshotProvider snapshotProvider,
        ILoggerFactory? loggerFactory = null,
        RemoteCertificateValidationCallback? serverCertificateValidationCallback = null) : BackgroundService
    {
        private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(30);
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
        internal bool IsStartupInitializationComplete { get; private set; }

        /// <summary>
        /// Gets the number of currently managed account runtimes.
        /// </summary>
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
        /// Gets the number of currently active sessions for one managed account runtime.
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

            foreach ((Guid accountId, AccountRuntimeState runtime) in accountsToRemove)
            {
                try
                {
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
                    continue;
                }

                try
                {
                    NntpAccountSessionReconcileResult result = await existingRuntime.Manager.ReconcileAccountAsync(desiredAccount, cancellationToken).ConfigureAwait(false);
                    existingRuntime.LastAppliedAccount = desiredAccount;
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
        }

        /// <summary>
        /// Holds the runtime state for one managed account session pool.
        /// </summary>
        /// <param name="LastAppliedAccount">Most recent desired account snapshot applied to this runtime.</param>
        /// <param name="Manager">Owned session manager implementing persistent session lifecycle for the account.</param>
        private sealed record AccountRuntimeState(
            NntpAccountSnapshot LastAppliedAccount,
            NntpArticleExecutionSessionManager Manager)
        {
            /// <summary>
            /// Gets or sets the latest desired account state applied to the runtime.
            /// </summary>
            internal NntpAccountSnapshot LastAppliedAccount { get; set; } = LastAppliedAccount;

            /// <summary>
            /// Gets the persistent session manager owned for this account runtime.
            /// </summary>
            internal NntpArticleExecutionSessionManager Manager { get; } = Manager;
        }

        /// <summary>
        /// Logs successful completion of the control-plane startup barrier.
        /// </summary>
        /// <param name="logger">The logger receiving the startup entry.</param>
        /// <param name="currentTime">The current timestamp captured for the startup barrier completion.</param>
        [LoggerMessage(EventId = 999, Level = LogLevel.Information, Message = "Control plane startup initialization completed at: {CurrentTime}")]
        private static partial void LogControlPlaneStartupInitialized(ILogger logger, DateTimeOffset currentTime);

        /// <summary>
        /// Logs the periodic control-plane heartbeat.
        /// </summary>
        /// <param name="logger">The logger receiving the heartbeat entry.</param>
        /// <param name="currentTime">The current timestamp captured for the heartbeat.</param>
        [LoggerMessage(EventId = 1000, Level = LogLevel.Debug, Message = "Control plane running at: {CurrentTime}")]
        private static partial void LogControlPlaneRunning(ILogger logger, DateTimeOffset currentTime);

        /// <summary>
        /// Logs a periodic snapshot refresh failure.
        /// </summary>
        /// <param name="logger">The logger receiving the refresh-failure event.</param>
        /// <param name="exception">Exception describing the refresh failure.</param>
        [LoggerMessage(EventId = 1001, Level = LogLevel.Warning, Message = "Periodic NNTP account snapshot refresh failed")]
        private static partial void LogNntpAccountRefreshFailed(ILogger logger, Exception exception);

        /// <summary>
        /// Precompiled delegate for account reconciliation start diagnostics.
        /// </summary>
        private static readonly Action<ILogger, int, int, Exception?> LogAccountReconciliationStartedMessage =
            LoggerMessage.Define<int, int>(
                LogLevel.Information,
                new EventId(1010, nameof(LogAccountReconciliationStarted)),
                "Account reconciliation started: ServerId={ServerId}, DesiredAccounts={DesiredAccountCount}");

        /// <summary>
        /// Precompiled delegate for account reconciliation completion diagnostics.
        /// </summary>
        private static readonly Action<ILogger, int, int, Exception?> LogAccountReconciliationCompletedMessage =
            LoggerMessage.Define<int, int>(
                LogLevel.Information,
                new EventId(1011, nameof(LogAccountReconciliationCompleted)),
                "Account reconciliation completed: ServerId={ServerId}, DesiredAccounts={DesiredAccountCount}");

        /// <summary>
        /// Precompiled delegate for account-added diagnostics.
        /// </summary>
        private static readonly Action<ILogger, Guid, string, int, bool, int, int, Exception?> LogAccountAddedMessage =
            LoggerMessage.Define<Guid, string, int, bool, int, int>(
                LogLevel.Information,
                new EventId(1012, nameof(LogAccountAdded)),
                "Account added: AccountId={AccountId}, Hostname={Host}, Port={Port}, SSL={UseSsl}, DesiredConnections={DesiredConnections}, ActiveConnections={ActiveConnections}");

        /// <summary>
        /// Precompiled delegate for account-add-failed diagnostics.
        /// </summary>
        private static readonly Action<ILogger, Guid, string, int, bool, Exception?> LogAccountAddFailedMessage =
            LoggerMessage.Define<Guid, string, int, bool>(
                LogLevel.Warning,
                new EventId(1013, nameof(LogAccountAddFailed)),
                "Account add failed: AccountId={AccountId}, Hostname={Host}, Port={Port}, SSL={UseSsl}");

        /// <summary>
        /// Precompiled delegate for account-removed diagnostics.
        /// </summary>
        private static readonly Action<ILogger, Guid, string, int, bool, Exception?> LogAccountRemovedMessage =
            LoggerMessage.Define<Guid, string, int, bool>(
                LogLevel.Information,
                new EventId(1014, nameof(LogAccountRemoved)),
                "Account removed: AccountId={AccountId}, Hostname={Host}, Port={Port}, SSL={UseSsl}");

        /// <summary>
        /// Precompiled delegate for account-removal-failed diagnostics.
        /// </summary>
        private static readonly Action<ILogger, Guid, Exception?> LogAccountRemovalFailedMessage =
            LoggerMessage.Define<Guid>(
                LogLevel.Warning,
                new EventId(1015, nameof(LogAccountRemovalFailed)),
                "Account remove failed: AccountId={AccountId}");

        /// <summary>
        /// Precompiled delegate for account-reconciled capacity diagnostics.
        /// </summary>
        private static readonly Action<ILogger, Guid, int, int, int, int, Exception?> LogAccountReconciledCapacityMessage =
            LoggerMessage.Define<Guid, int, int, int, int>(
                LogLevel.Information,
                new EventId(1016, nameof(LogAccountReconciled)),
                "Account reconciled capacity: AccountId={AccountId}, DesiredConnections={DesiredConnections}, ActiveBefore={ActiveBefore}, ActiveAfter={ActiveAfter}, AddedSessions={AddedSessions}");

        /// <summary>
        /// Precompiled delegate for account-reconciled retirement diagnostics.
        /// </summary>
        private static readonly Action<ILogger, Guid, int, Exception?> LogAccountReconciledRetiredMessage =
            LoggerMessage.Define<Guid, int>(
                LogLevel.Information,
                new EventId(1019, nameof(LogAccountReconciledRetiredMessage)),
                "Account reconciled retired sessions: AccountId={AccountId}, RetiredSessions={RetiredSessions}");

        /// <summary>
        /// Precompiled delegate for account-reconciled configuration diagnostics.
        /// </summary>
        private static readonly Action<ILogger, Guid, string, int, bool, bool, bool, Exception?> LogAccountReconciledConfigurationMessage =
            LoggerMessage.Define<Guid, string, int, bool, bool, bool>(
                LogLevel.Information,
                new EventId(1018, nameof(LogAccountReconciledConfigurationMessage)),
                "Account reconciled configuration: AccountId={AccountId}, Hostname={Host}, Port={Port}, SSL={UseSsl}, KeepAliveUpdated={KeepAliveUpdated}, ConnectionSettingsReplaced={ConnectionSettingsReplaced}");

        /// <summary>
        /// Precompiled delegate for account-reconcile-failed diagnostics.
        /// </summary>
        private static readonly Action<ILogger, Guid, string, int, bool, Exception?> LogAccountReconcileFailedMessage =
            LoggerMessage.Define<Guid, string, int, bool>(
                LogLevel.Warning,
                new EventId(1017, nameof(LogAccountReconcileFailed)),
                "Account reconcile failed: AccountId={AccountId}, Hostname={Host}, Port={Port}, SSL={UseSsl}");

        /// <summary>
        /// Logs account reconciliation cycle start.
        /// </summary>
        /// <param name="logger">Logger receiving reconciliation events.</param>
        /// <param name="serverId">Authoritative server identifier of the snapshot.</param>
        /// <param name="desiredAccountCount">Number of enabled desired accounts in the snapshot.</param>
        private static void LogAccountReconciliationStarted(ILogger logger, int serverId, int desiredAccountCount)
        {
            LogAccountReconciliationStartedMessage(logger, serverId, desiredAccountCount, null);
        }

        /// <summary>
        /// Logs account reconciliation cycle completion.
        /// </summary>
        /// <param name="logger">Logger receiving reconciliation events.</param>
        /// <param name="serverId">Authoritative server identifier of the snapshot.</param>
        /// <param name="desiredAccountCount">Number of enabled desired accounts in the snapshot.</param>
        private static void LogAccountReconciliationCompleted(ILogger logger, int serverId, int desiredAccountCount)
        {
            LogAccountReconciliationCompletedMessage(logger, serverId, desiredAccountCount, null);
        }

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
        private static void LogAccountAdded(ILogger logger, Guid accountId, string host, int port, bool useSsl, int desiredConnections, int activeConnections)
        {
            LogAccountAddedMessage(logger, accountId, host, port, useSsl, desiredConnections, activeConnections, null);
        }

        /// <summary>
        /// Logs account runtime add failure.
        /// </summary>
        /// <param name="logger">Logger receiving account lifecycle events.</param>
        /// <param name="accountId">Account identifier.</param>
        /// <param name="host">Account host.</param>
        /// <param name="port">Account port.</param>
        /// <param name="useSsl">Account SSL setting.</param>
        /// <param name="exception">Failure exception.</param>
        private static void LogAccountAddFailed(ILogger logger, Guid accountId, string host, int port, bool useSsl, Exception exception)
        {
            LogAccountAddFailedMessage(logger, accountId, host, port, useSsl, exception);
        }

        /// <summary>
        /// Logs account runtime removal.
        /// </summary>
        /// <param name="logger">Logger receiving account lifecycle events.</param>
        /// <param name="accountId">Account identifier.</param>
        /// <param name="host">Account host.</param>
        /// <param name="port">Account port.</param>
        /// <param name="useSsl">Account SSL setting.</param>
        private static void LogAccountRemoved(ILogger logger, Guid accountId, string host, int port, bool useSsl)
        {
            LogAccountRemovedMessage(logger, accountId, host, port, useSsl, null);
        }

        /// <summary>
        /// Logs account runtime removal failure.
        /// </summary>
        /// <param name="logger">Logger receiving account lifecycle events.</param>
        /// <param name="accountId">Account identifier.</param>
        /// <param name="exception">Failure exception.</param>
        private static void LogAccountRemovalFailed(ILogger logger, Guid accountId, Exception exception)
        {
            LogAccountRemovalFailedMessage(logger, accountId, exception);
        }

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
            LogAccountReconciledCapacityMessage(logger, accountId, desiredConnections, activeBefore, activeAfter, addedSessions, null);
            LogAccountReconciledRetiredMessage(logger, accountId, retiredSessions, null);
            LogAccountReconciledConfigurationMessage(logger, accountId, host, port, useSsl, keepAliveUpdated, connectionSettingsReplaced, null);
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
        private static void LogAccountReconcileFailed(ILogger logger, Guid accountId, string host, int port, bool useSsl, Exception exception)
        {
            LogAccountReconcileFailedMessage(logger, accountId, host, port, useSsl, exception);
        }
    }
}
