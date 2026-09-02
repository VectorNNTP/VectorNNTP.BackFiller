// <copyright file="RabbitMqConsumerSessionFactory.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / RabbitMq
// Implements the rabbit mq consumer session factory behavior.

using System.Threading.Channels;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.ControlPlane;
using VectorNNTP.Backfiller.Runtime.Accounts;
using VectorNNTP.Backfiller.Runtime.Shutdown;

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// Creates owned RabbitMQ consumer sessions for reconciliation-managed runtime lifecycles.
    /// </summary>
    internal interface IRabbitMqConsumerSessionFactory
    {
        /// <summary>
        /// Creates one consumer session with dedicated channel ownership and delivery sink handoff.
        /// </summary>
        /// <param name="identity">Logical session identity.</param>
        /// <param name="deliverySink">Infrastructure delivery sink.</param>
        /// <param name="prefetchCount">Optional prefetch count.</param>
        /// <returns>New consumer session instance.</returns>
        public IRabbitMqConsumerSession CreateSession(
            RabbitMqConsumerSessionIdentity identity,
            IRabbitMqDeliverySink deliverySink,
            ushort? prefetchCount);
    }

    /// <summary>
    /// Default factory for concrete RabbitMQ consumer session instances.
    /// </summary>
    internal sealed class RabbitMqConsumerSessionFactory : IRabbitMqConsumerSessionFactory
    {
        /// <summary>
        /// Stores connection manager used by rabbit mq consumer session factory.
        /// </summary>
        private readonly RabbitMqConnectionManager _connectionManager;
        /// <summary>
        /// Stores topology initializer used by rabbit mq consumer session factory.
        /// </summary>
        private readonly RabbitMqTopologyInitializer _topologyInitializer;
        /// <summary>
        /// Supplies the logger used by rabbit mq consumer session factory.
        /// </summary>
        private readonly ILoggerFactory _loggerFactory;
        /// <summary>
        /// Stores diagnostic correlation id used by rabbit mq consumer session factory.
        /// </summary>
        private readonly string? _diagnosticCorrelationId;

        /// <summary>
        /// Handles rabbit mq consumer session factory for rabbit mq consumer session factory.
        /// </summary>
        public RabbitMqConsumerSessionFactory(
            RabbitMqConnectionManager connectionManager,
            RabbitMqTopologyInitializer topologyInitializer,
            ILoggerFactory loggerFactory,
            BackFillerRuntimeOptions runtimeOptions)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);

            _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
            _topologyInitializer = topologyInitializer ?? throw new ArgumentNullException(nameof(topologyInitializer));
            _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));

            RabbitMqRuntimeOptions rabbitMq = runtimeOptions.RabbitMq
                ?? throw new InvalidOperationException("Validated runtime RabbitMQ settings were not provided.");

            _diagnosticCorrelationId = string.IsNullOrWhiteSpace(rabbitMq.DiagnosticPayloadCorrelationId)
                ? null
                : rabbitMq.DiagnosticPayloadCorrelationId.Trim();
        }

        /// <inheritdoc/>
        public IRabbitMqConsumerSession CreateSession(
            RabbitMqConsumerSessionIdentity identity,
            IRabbitMqDeliverySink deliverySink,
            ushort? prefetchCount)
        {
            ArgumentNullException.ThrowIfNull(identity);
            ArgumentNullException.ThrowIfNull(deliverySink);

            return new RabbitMqBackboneConsumerSession(
                identity,
                _connectionManager,
                _topologyInitializer,
                deliverySink,
                _loggerFactory.CreateLogger<RabbitMqBackboneConsumerSession>(),
                prefetchCount,
                _diagnosticCorrelationId);
        }
    }

    /// <summary>
    /// Reconciles desired RabbitMQ consumer sessions from authoritative account snapshot state.
    /// </summary>
    internal sealed partial class RabbitMqConsumerService : BackgroundService, IRabbitMqCapacityRetirementCoordinator
    {
        /// <summary>
        /// Configures reconcile interval for rabbit mq consumer session factory.
        /// </summary>
        private static readonly TimeSpan ReconcileInterval = TimeSpan.FromSeconds(15);

        /// <summary>
        /// Limits account snapshot provider for rabbit mq consumer session factory.
        /// </summary>
        private readonly MySqlNntpAccountSnapshotProvider _accountSnapshotProvider;
        /// <summary>
        /// Stores connection manager used by rabbit mq consumer session factory.
        /// </summary>
        private readonly RabbitMqConnectionManager _connectionManager;
        /// <summary>
        /// Stores session factory used by rabbit mq consumer session factory.
        /// </summary>
        private readonly IRabbitMqConsumerSessionFactory _sessionFactory;
        /// <summary>
        /// Stores shutdown coordinator used by rabbit mq consumer session factory.
        /// </summary>
        private readonly ShutdownCoordinator _shutdownCoordinator;
        /// <summary>
        /// Stores consumer options used by rabbit mq consumer session factory.
        /// </summary>
        private readonly RabbitMqConsumerInfrastructureOptions _consumerOptions;
        /// <summary>
        /// Supplies the logger used by rabbit mq consumer session factory.
        /// </summary>
        private readonly ILogger<RabbitMqConsumerService> _logger;
        /// <summary>
        /// Limits capacity provider for rabbit mq consumer session factory.
        /// </summary>
        private readonly IBackboneUsableCapacityProvider _capacityProvider;
        /// <summary>
        /// Stores state gate used by rabbit mq consumer session factory.
        /// </summary>
        private readonly SemaphoreSlim _stateGate = new(1, 1);
        /// <summary>
        /// Stores shutdown cts used by rabbit mq consumer session factory.
        /// </summary>
        private readonly CancellationTokenSource _shutdownCts = new();
        /// <summary>
        /// Stores session runtimes used by rabbit mq consumer session factory.
        /// </summary>
        private readonly Dictionary<string, SessionRuntimeState> _sessionRuntimes = new(StringComparer.Ordinal);
        /// <summary>
        /// Stores retiring session runtimes used by rabbit mq consumer session factory.
        /// </summary>
        private readonly Dictionary<string, RetiringSessionRuntimeState> _retiringSessionRuntimes = new(StringComparer.Ordinal);
        /// <summary>
        /// Stores delivery channel used by rabbit mq consumer session factory.
        /// </summary>
        private readonly Channel<RabbitMqArticleDelivery> _deliveryChannel;
        /// <summary>
        /// Stores graceful shutdown registration used by rabbit mq consumer session factory.
        /// </summary>
        private readonly IDisposable _gracefulShutdownRegistration;
        /// <summary>
        /// Stores forced shutdown registration used by rabbit mq consumer session factory.
        /// </summary>
        private readonly IDisposable _forcedShutdownRegistration;

        /// <summary>
        /// Stores shutdown requested used by rabbit mq consumer session factory.
        /// </summary>
        private volatile bool _shutdownRequested;
        /// <summary>
        /// Stores callbacks disposed used by rabbit mq consumer session factory.
        /// </summary>
        private int _callbacksDisposed;

        /// <summary>
        /// Handles rabbit mq consumer service for rabbit mq consumer session factory.
        /// </summary>
        public RabbitMqConsumerService(
            BackFillerRuntimeOptions runtimeOptions,
            MySqlNntpAccountSnapshotProvider accountSnapshotProvider,
            RabbitMqConnectionManager connectionManager,
            IRabbitMqConsumerSessionFactory sessionFactory,
            ShutdownCoordinator shutdownCoordinator,
            ILogger<RabbitMqConsumerService> logger)
            : this(runtimeOptions, accountSnapshotProvider, connectionManager, sessionFactory, shutdownCoordinator, AlwaysAvailableBackboneCapacityProvider.Instance, logger)
        {
        }

        /// <summary>
        /// Handles rabbit mq consumer service for rabbit mq consumer session factory.
        /// </summary>
        public RabbitMqConsumerService(
            BackFillerRuntimeOptions runtimeOptions,
            MySqlNntpAccountSnapshotProvider accountSnapshotProvider,
            RabbitMqConnectionManager connectionManager,
            IRabbitMqConsumerSessionFactory sessionFactory,
            ShutdownCoordinator shutdownCoordinator,
            IBackboneUsableCapacityProvider capacityProvider,
            ILogger<RabbitMqConsumerService> logger)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);
            ArgumentNullException.ThrowIfNull(accountSnapshotProvider);
            ArgumentNullException.ThrowIfNull(connectionManager);
            ArgumentNullException.ThrowIfNull(sessionFactory);
            ArgumentNullException.ThrowIfNull(shutdownCoordinator);
            ArgumentNullException.ThrowIfNull(capacityProvider);
            ArgumentNullException.ThrowIfNull(logger);

            _accountSnapshotProvider = accountSnapshotProvider;
            _connectionManager = connectionManager;
            _sessionFactory = sessionFactory;
            _shutdownCoordinator = shutdownCoordinator;
            _capacityProvider = capacityProvider;
            _logger = logger;

            _consumerOptions = RabbitMqConsumerInfrastructureOptions.FromRuntimeOptions(runtimeOptions);
            _deliveryChannel = Channel.CreateBounded<RabbitMqArticleDelivery>(new BoundedChannelOptions(_consumerOptions.DeliveryBufferCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
            });

            _connectionManager.ConnectionReplaced += OnConnectionReplaced;
            _gracefulShutdownRegistration = _shutdownCoordinator.GracefulShutdownStartedToken.Register(OnShutdownSignaled);
            _forcedShutdownRegistration = _shutdownCoordinator.ForcedShutdownToken.Register(OnShutdownSignaled);
        }

        /// <summary>
        /// Returns the bounded infrastructure delivery stream for the next processing layer.
        /// </summary>
        internal ChannelReader<RabbitMqArticleDelivery> DeliveryReader => _deliveryChannel.Reader;

        /// <summary>
        /// Runs one explicit reconciliation cycle.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A task that completes when reconciliation is finished.</returns>
        internal Task ReconcileOnceAsync(CancellationToken cancellationToken)
        {
            return ReconcileSessionsAsync(cancellationToken);
        }

        /// <inheritdoc/>
        public async Task RetireCapacityAsync(Guid accountId, int retainConnectionCount, CancellationToken cancellationToken)
        {
            if (retainConnectionCount < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(retainConnectionCount));
            }

            List<RetirementOperation> retirements = [];
            List<Task> pendingRetirements = [];

            await _stateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (_shutdownRequested)
                {
                    return;
                }

                PruneCompletedRetirementsNoLock();

                foreach ((string sessionKey, SessionRuntimeState runtimeState) in _sessionRuntimes)
                {
                    if (runtimeState.Identity.AccountId != accountId || runtimeState.Identity.ConnectionNumber <= retainConnectionCount)
                    {
                        continue;
                    }

                    TaskCompletionSource<bool> completionSource = new(TaskCreationOptions.RunContinuationsAsynchronously);
                    _retiringSessionRuntimes[sessionKey] = new RetiringSessionRuntimeState(runtimeState.Identity, completionSource.Task);
                    retirements.Add(new RetirementOperation(sessionKey, runtimeState, completionSource));
                }

                foreach (RetirementOperation retirement in retirements)
                {
                    _ = _sessionRuntimes.Remove(retirement.SessionKey);
                }

                foreach ((string sessionKey, RetiringSessionRuntimeState retiring) in _retiringSessionRuntimes)
                {
                    if (retiring.Identity.AccountId == accountId && retiring.Identity.ConnectionNumber > retainConnectionCount)
                    {
                        pendingRetirements.Add(retiring.RetirementTask);
                    }
                }
            }
            finally
            {
                _ = _stateGate.Release();
            }

            for (int index = 0; index < retirements.Count; index++)
            {
                await ExecuteRetirementOperationAsync(retirements[index], cancellationToken, cancelAdmittedWork: false).ConfigureAwait(false);
            }

            for (int index = 0; index < pendingRetirements.Count; index++)
            {
                await pendingRetirements[index].WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Returns the currently active consumer session count.
        /// </summary>
        internal int ActiveSessionCount
        {
            get
            {
                _stateGate.Wait(CancellationToken.None);
                try
                {
                    return _sessionRuntimes.Count;
                }
                finally
                {
                    _ = _stateGate.Release();
                }
            }
        }

        /// <summary>
        /// Handles execute async for rabbit mq consumer session factory.
        /// </summary>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            LogConsumerServiceStarting(_logger, _consumerOptions.DeliveryBufferCapacity, _consumerOptions.PrefetchCount);

            using CancellationTokenSource linkedCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, _shutdownCts.Token);
            CancellationToken linkedToken = linkedCts.Token;

            while (!linkedToken.IsCancellationRequested)
            {
                try
                {
                    await ReconcileSessionsAsync(linkedToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (linkedToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    LogConsumerServiceReconcileFailed(_logger, ex);
                }

                try
                {
                    await Task.Delay(ReconcileInterval, linkedToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (linkedToken.IsCancellationRequested)
                {
                    break;
                }
            }

            await StopAllSessionsAsync(CancellationToken.None).ConfigureAwait(false);
            _ = _deliveryChannel.Writer.TryComplete();
            DisposeLifecycleCallbacks();
            _shutdownCts.Dispose();

            LogConsumerServiceStopped(_logger);
        }

        /// <summary>
        /// Handles stop async for rabbit mq consumer session factory.
        /// </summary>
        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            OnShutdownSignaled();
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Handles on connection replaced for rabbit mq consumer session factory.
        /// </summary>
        private async void OnConnectionReplaced(object? sender, RabbitMqConnectionReplacedEventArgs args)
        {
            if (_shutdownRequested || !args.IsReplacement)
            {
                return;
            }

            try
            {
                await _stateGate.WaitAsync(_shutdownCts.Token).ConfigureAwait(false);
                try
                {
                    if (_shutdownRequested)
                    {
                        return;
                    }

                    IRabbitMqConsumerSession[] sessions = [.. _sessionRuntimes.Values.Select(static x => x.Session)];
                    for (int i = 0; i < sessions.Length; i++)
                    {
                        if (sessions[i] is RabbitMqBackboneConsumerSession concreteSession)
                        {
                            await concreteSession.HandleConnectionReplacedAsync(args, _shutdownCts.Token).ConfigureAwait(false);
                        }
                    }
                }
                finally
                {
                    _ = _stateGate.Release();
                }
            }
            catch (OperationCanceledException) when (_shutdownRequested)
            {
            }
            catch (Exception ex)
            {
                LogConnectionReplacedDispatchFailed(_logger, ex);
            }
        }

        /// <summary>
        /// Handles reconcile sessions async for rabbit mq consumer session factory.
        /// </summary>
        private async Task ReconcileSessionsAsync(CancellationToken cancellationToken)
        {
            List<RetirementOperation> retirements = [];
            List<SessionRuntimeState> starts = [];
            Dictionary<string, RabbitMqConsumerSessionIdentity> desiredSessions;
            int activeCount;

            await _stateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (_shutdownRequested)
                {
                    return;
                }

                PruneCompletedRetirementsNoLock();

                foreach (SessionRuntimeState runtime in _sessionRuntimes.Values)
                {
                    runtime.Desired = false;
                }

                NntpAccountSnapshotState snapshot = _accountSnapshotProvider.CurrentSnapshot;
                desiredSessions = BuildDesiredSessions(snapshot, ResolveBackboneUsableCapacity);

                foreach ((string sessionKey, RabbitMqConsumerSessionIdentity desiredIdentity) in desiredSessions)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (_retiringSessionRuntimes.TryGetValue(sessionKey, out RetiringSessionRuntimeState? retiring))
                    {
                        if (!retiring.RetirementTask.IsCompletedSuccessfully)
                        {
                            continue;
                        }

                        _ = _retiringSessionRuntimes.Remove(sessionKey);
                    }

                    if (!_sessionRuntimes.TryGetValue(sessionKey, out SessionRuntimeState? runtimeState))
                    {
                        IRabbitMqConsumerSession created = _sessionFactory.CreateSession(
                            desiredIdentity,
                            new RabbitMqDeliveryChannelSink(_deliveryChannel.Writer),
                            _consumerOptions.PrefetchCount);

                        runtimeState = new SessionRuntimeState(desiredIdentity, created);
                        _sessionRuntimes[sessionKey] = runtimeState;

                        LogConsumerSessionCreated(
                            _logger,
                            desiredIdentity.Backbone,
                            desiredIdentity.AccountUsername,
                            desiredIdentity.ConnectionNumber,
                            desiredIdentity.ConnectionLimit,
                            sessionKey);
                    }
                    else if (RequiresSessionReplacement(runtimeState.Identity, desiredIdentity))
                    {
                        TaskCompletionSource<bool> completionSource = new(TaskCreationOptions.RunContinuationsAsynchronously);
                        _retiringSessionRuntimes[sessionKey] = new RetiringSessionRuntimeState(runtimeState.Identity, completionSource.Task);
                        retirements.Add(new RetirementOperation(sessionKey, runtimeState, completionSource));
                        _ = _sessionRuntimes.Remove(sessionKey);

                        LogConsumerSessionReplaced(
                            _logger,
                            desiredIdentity.Backbone,
                            desiredIdentity.AccountUsername,
                            desiredIdentity.ConnectionNumber,
                            desiredIdentity.ConnectionLimit,
                            sessionKey);

                        continue;
                    }
                    else
                    {
                        runtimeState.Identity = desiredIdentity;
                    }

                    runtimeState.Desired = true;

                    if (!runtimeState.Session.IsRunning)
                    {
                        starts.Add(runtimeState);
                    }
                }

                List<KeyValuePair<string, SessionRuntimeState>> staleSessions = [.. _sessionRuntimes.Where(static kvp => !kvp.Value.Desired)];
                for (int i = 0; i < staleSessions.Count; i++)
                {
                    KeyValuePair<string, SessionRuntimeState> stale = staleSessions[i];
                    TaskCompletionSource<bool> completionSource = new(TaskCreationOptions.RunContinuationsAsynchronously);
                    _retiringSessionRuntimes[stale.Key] = new RetiringSessionRuntimeState(stale.Value.Identity, completionSource.Task);
                    retirements.Add(new RetirementOperation(stale.Key, stale.Value, completionSource));
                    _ = _sessionRuntimes.Remove(stale.Key);
                }

                activeCount = _sessionRuntimes.Count;
            }
            finally
            {
                foreach (SessionRuntimeState runtime in _sessionRuntimes.Values)
                {
                    runtime.Desired = false;
                }

                _ = _stateGate.Release();
            }

            for (int i = 0; i < retirements.Count; i++)
            {
                await ExecuteRetirementOperationAsync(retirements[i], cancellationToken, cancelAdmittedWork: false).ConfigureAwait(false);
            }

            for (int i = 0; i < starts.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await starts[i].Session.StartAsync(cancellationToken).ConfigureAwait(false);
            }

            LogConsumerReconcileCompleted(_logger, desiredSessions.Count, activeCount);
        }

        /// <summary>
        /// Handles build desired sessions for rabbit mq consumer session factory.
        /// </summary>
        private Dictionary<string, RabbitMqConsumerSessionIdentity> BuildDesiredSessions(
            NntpAccountSnapshotState snapshot,
            Func<string, bool> hasUsableBackboneCapacity)
        {
            ArgumentNullException.ThrowIfNull(snapshot);
            ArgumentNullException.ThrowIfNull(hasUsableBackboneCapacity);

            Dictionary<string, RabbitMqConsumerSessionIdentity> desired = new(StringComparer.Ordinal);
            foreach (NntpAccountSnapshot account in snapshot.Accounts)
            {
                if (string.IsNullOrWhiteSpace(account.Backbone) || string.IsNullOrWhiteSpace(account.Username) || string.IsNullOrWhiteSpace(account.Hostname))
                {
                    continue;
                }

                if (account.MaxConnections <= 0)
                {
                    continue;
                }

                if (!hasUsableBackboneCapacity(account.Backbone))
                {
                    continue;
                }

                for (int connectionNumber = 1; connectionNumber <= account.MaxConnections; connectionNumber++)
                {
                    RabbitMqConsumerSessionIdentity identity = new(
                        Backbone: account.Backbone,
                        AccountId: account.EntryId,
                        AccountUsername: account.Username,
                        ConnectionNumber: connectionNumber,
                        ConnectionLimit: account.MaxConnections,
                        ServerId: account.ServerId,
                        Host: account.Hostname,
                        Port: account.Port,
                        UseSsl: account.UseSsl);

                    desired[identity.SessionKey] = identity;
                }
            }

            return desired;
        }

        /// <summary>
        /// Handles resolve backbone usable capacity for rabbit mq consumer session factory.
        /// </summary>
        private bool ResolveBackboneUsableCapacity(string backbone)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(backbone);
            return _capacityProvider.HasUsableCapacityForBackbone(backbone);
        }

        /// <summary>
        /// Handles stop all sessions async for rabbit mq consumer session factory.
        /// </summary>
        private async Task StopAllSessionsAsync(CancellationToken cancellationToken)
        {
            List<RetirementOperation> retirements = [];

            await _stateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                PruneCompletedRetirementsNoLock();

                foreach ((string sessionKey, SessionRuntimeState runtime) in _sessionRuntimes)
                {
                    TaskCompletionSource<bool> completionSource = new(TaskCreationOptions.RunContinuationsAsynchronously);
                    _retiringSessionRuntimes[sessionKey] = new RetiringSessionRuntimeState(runtime.Identity, completionSource.Task);
                    retirements.Add(new RetirementOperation(sessionKey, runtime, completionSource));
                }

                _sessionRuntimes.Clear();
            }
            finally
            {
                _ = _stateGate.Release();
            }

            for (int i = 0; i < retirements.Count; i++)
            {
                try
                {
                    await ExecuteRetirementOperationAsync(retirements[i], cancellationToken, cancelAdmittedWork: true).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    LogConsumerSessionStopFailed(_logger, retirements[i].Runtime.Identity.SessionKey, ex);
                }
            }

            Task[] pendingRetirements = [.. _retiringSessionRuntimes.Values.Select(static runtime => runtime.RetirementTask)];
            for (int i = 0; i < pendingRetirements.Length; i++)
            {
                try
                {
                    await pendingRetirements[i].ConfigureAwait(false);
                }
                catch
                {
                }
            }
        }

        /// <summary>
        /// Handles on shutdown signaled for rabbit mq consumer session factory.
        /// </summary>
        private void OnShutdownSignaled()
        {
            if (_shutdownRequested)
            {
                return;
            }

            _shutdownRequested = true;
            DisposeLifecycleCallbacks();
            _shutdownCts.Cancel();
        }

        /// <summary>
        /// Handles dispose lifecycle callbacks for rabbit mq consumer session factory.
        /// </summary>
        private void DisposeLifecycleCallbacks()
        {
            if (Interlocked.Exchange(ref _callbacksDisposed, 1) != 0)
            {
                return;
            }

            _connectionManager.ConnectionReplaced -= OnConnectionReplaced;
            _gracefulShutdownRegistration.Dispose();
            _forcedShutdownRegistration.Dispose();
        }

        /// <summary>
        /// Handles execute retirement operation async for rabbit mq consumer session factory.
        /// </summary>
        private async Task ExecuteRetirementOperationAsync(RetirementOperation operation, CancellationToken cancellationToken, bool cancelAdmittedWork)
        {
            ArgumentNullException.ThrowIfNull(operation);

            Exception? failure = null;
            bool retirementCompleted = false;
            try
            {
                await operation.Runtime.Session.StopAsync(cancellationToken, cancelAdmittedWork).ConfigureAwait(false);
                await operation.Runtime.Session.DisposeAsync().ConfigureAwait(false);
                retirementCompleted = true;

                LogConsumerSessionRetired(
                    _logger,
                    operation.Runtime.Identity.Backbone,
                    operation.Runtime.Identity.AccountUsername,
                    operation.Runtime.Identity.ConnectionNumber,
                    operation.Runtime.Identity.ConnectionLimit,
                    operation.SessionKey);
            }
            catch (Exception ex)
            {
                failure = ex;
                throw;
            }
            finally
            {
                await _stateGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
                try
                {
                    if (retirementCompleted &&
                        _retiringSessionRuntimes.TryGetValue(operation.SessionKey, out RetiringSessionRuntimeState? tracked) &&
                        ReferenceEquals(tracked.RetirementTask, operation.CompletionSource.Task))
                    {
                        _ = _retiringSessionRuntimes.Remove(operation.SessionKey);
                    }

                    if (failure is null)
                    {
                        _ = operation.CompletionSource.TrySetResult(true);
                    }
                    else
                    {
                        _ = operation.CompletionSource.TrySetException(failure);
                    }
                }
                finally
                {
                    _ = _stateGate.Release();
                }
            }
        }

        /// <summary>
        /// Handles prune completed retirements no lock for rabbit mq consumer session factory.
        /// </summary>
        private void PruneCompletedRetirementsNoLock()
        {
            List<string> completedKeys = [];
            foreach ((string sessionKey, RetiringSessionRuntimeState retiring) in _retiringSessionRuntimes)
            {
                if (retiring.RetirementTask.IsCompletedSuccessfully)
                {
                    completedKeys.Add(sessionKey);
                }
            }

            for (int i = 0; i < completedKeys.Count; i++)
            {
                _ = _retiringSessionRuntimes.Remove(completedKeys[i]);
            }
        }

        /// <summary>
        /// Handles requires session replacement for rabbit mq consumer session factory.
        /// </summary>
        private static bool RequiresSessionReplacement(RabbitMqConsumerSessionIdentity existingIdentity, RabbitMqConsumerSessionIdentity desiredIdentity)
        {
            ArgumentNullException.ThrowIfNull(existingIdentity);
            ArgumentNullException.ThrowIfNull(desiredIdentity);

            return !string.Equals(existingIdentity.Backbone, desiredIdentity.Backbone, StringComparison.Ordinal);
        }

        /// <summary>
        /// Defines session runtime state and its rabbit mq consumer session factory contract.
        /// </summary>
        private sealed class SessionRuntimeState(RabbitMqConsumerSessionIdentity identity, IRabbitMqConsumerSession session)
        {
            /// <summary>
            /// Stores identity used by rabbit mq consumer session factory.
            /// </summary>
            internal RabbitMqConsumerSessionIdentity Identity { get; set; } = identity;

            /// <summary>
            /// Stores session used by rabbit mq consumer session factory.
            /// </summary>
            internal IRabbitMqConsumerSession Session { get; } = session;

            /// <summary>
            /// Stores desired used by rabbit mq consumer session factory.
            /// </summary>
            internal bool Desired { get; set; }
        }

        /// <summary>
        /// Defines always available backbone capacity provider and its rabbit mq consumer session factory contract.
        /// </summary>
        private sealed class AlwaysAvailableBackboneCapacityProvider : IBackboneUsableCapacityProvider
        {
            /// <summary>
            /// Stores instance used by rabbit mq consumer session factory.
            /// </summary>
            internal static readonly AlwaysAvailableBackboneCapacityProvider Instance = new();

            /// <summary>
            /// Determines whether the backbone currently has capacity for another delivery.
            /// </summary>
            public bool HasUsableCapacityForBackbone(string backbone)
            {
                return !string.IsNullOrWhiteSpace(backbone);
            }
        }

        /// <summary>
        /// Defines retiring session runtime state and its rabbit mq consumer session factory contract.
        /// </summary>
        private sealed class RetiringSessionRuntimeState(RabbitMqConsumerSessionIdentity identity, Task retirementTask)
        {
            /// <summary>
            /// Stores identity used by rabbit mq consumer session factory.
            /// </summary>
            internal RabbitMqConsumerSessionIdentity Identity { get; } = identity;

            /// <summary>
            /// Stores retirement task used by rabbit mq consumer session factory.
            /// </summary>
            internal Task RetirementTask { get; } = retirementTask;
        }

        /// <summary>
        /// Defines retirement operation and its rabbit mq consumer session factory contract.
        /// </summary>
        private sealed class RetirementOperation(string sessionKey, SessionRuntimeState runtime, TaskCompletionSource<bool> completionSource)
        {
            /// <summary>
            /// Stores session key used by rabbit mq consumer session factory.
            /// </summary>
            internal string SessionKey { get; } = sessionKey;

            /// <summary>
            /// Stores runtime used by rabbit mq consumer session factory.
            /// </summary>
            internal SessionRuntimeState Runtime { get; } = runtime;

            /// <summary>
            /// Stores completion source used by rabbit mq consumer session factory.
            /// </summary>
            internal TaskCompletionSource<bool> CompletionSource { get; } = completionSource;
        }

                /// <summary>
        /// Emits the consumer service starting log event for rabbit mq consumer session factory.
        /// </summary>
        [LoggerMessage(EventId = 4400, Level = LogLevel.Information, Message = "RabbitMQ consumer service starting. DeliveryBufferCapacity={DeliveryBufferCapacity}, Prefetch={PrefetchCount}")]
        private static partial void LogConsumerServiceStarting(ILogger logger, int deliveryBufferCapacity, ushort? prefetchCount);

                /// <summary>
        /// Emits the consumer service stopped log event for rabbit mq consumer session factory.
        /// </summary>
        [LoggerMessage(EventId = 4401, Level = LogLevel.Information, Message = "RabbitMQ consumer service stopped")]
        private static partial void LogConsumerServiceStopped(ILogger logger);

                /// <summary>
        /// Emits the consumer service reconcile failed log event for rabbit mq consumer session factory.
        /// </summary>
        [LoggerMessage(EventId = 4402, Level = LogLevel.Warning, Message = "RabbitMQ consumer reconciliation cycle failed unexpectedly")]
        private static partial void LogConsumerServiceReconcileFailed(ILogger logger, Exception exception);

                /// <summary>
        /// Emits the connection replaced dispatch failed log event for rabbit mq consumer session factory.
        /// </summary>
        [LoggerMessage(EventId = 4403, Level = LogLevel.Warning, Message = "RabbitMQ connection replacement dispatch to consumer sessions failed")]
        private static partial void LogConnectionReplacedDispatchFailed(ILogger logger, Exception exception);

                /// <summary>
        /// Emits the consumer session created log event for rabbit mq consumer session factory.
        /// </summary>
        [LoggerMessage(EventId = 4404, Level = LogLevel.Information, Message = "RabbitMQ consumer session created. Backbone={Backbone} Account={Account} Connection={ConnectionNumber}/{ConnectionLimit} SessionKey={SessionKey}")]
        private static partial void LogConsumerSessionCreated(ILogger logger, string backbone, string account, int connectionNumber, int connectionLimit, string sessionKey);

                /// <summary>
        /// Emits the consumer session replaced log event for rabbit mq consumer session factory.
        /// </summary>
        [LoggerMessage(EventId = 4405, Level = LogLevel.Information, Message = "RabbitMQ consumer session replaced due to account configuration change. Backbone={Backbone} Account={Account} Connection={ConnectionNumber}/{ConnectionLimit} SessionKey={SessionKey}")]
        private static partial void LogConsumerSessionReplaced(ILogger logger, string backbone, string account, int connectionNumber, int connectionLimit, string sessionKey);

                /// <summary>
        /// Emits the consumer session retired log event for rabbit mq consumer session factory.
        /// </summary>
        [LoggerMessage(EventId = 4406, Level = LogLevel.Information, Message = "RabbitMQ consumer session retired. Backbone={Backbone} Account={Account} Connection={ConnectionNumber}/{ConnectionLimit} SessionKey={SessionKey}")]
        private static partial void LogConsumerSessionRetired(ILogger logger, string backbone, string account, int connectionNumber, int connectionLimit, string sessionKey);

                /// <summary>
        /// Emits the consumer reconcile completed log event for rabbit mq consumer session factory.
        /// </summary>
        [LoggerMessage(EventId = 4407, Level = LogLevel.Information, Message = "RabbitMQ consumer reconciliation completed. DesiredSessions={DesiredSessions} ActiveSessions={ActiveSessions}")]
        private static partial void LogConsumerReconcileCompleted(ILogger logger, int desiredSessions, int activeSessions);

                /// <summary>
        /// Emits the consumer session stop failed log event for rabbit mq consumer session factory.
        /// </summary>
        [LoggerMessage(EventId = 4408, Level = LogLevel.Warning, Message = "RabbitMQ consumer session stop/dispose failed during service shutdown. SessionKey={SessionKey}")]
        private static partial void LogConsumerSessionStopFailed(ILogger logger, string sessionKey, Exception exception);
    }
}
