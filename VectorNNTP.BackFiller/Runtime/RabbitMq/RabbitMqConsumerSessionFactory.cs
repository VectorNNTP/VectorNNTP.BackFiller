using System.Threading.Channels;
using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Runtime.RabbitMq;

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
    IRabbitMqConsumerSession CreateSession(
        RabbitMqConsumerSessionIdentity identity,
        IRabbitMqDeliverySink deliverySink,
        ushort? prefetchCount);
}

/// <summary>
/// Default factory for concrete RabbitMQ consumer session instances.
/// </summary>
internal sealed class RabbitMqConsumerSessionFactory : IRabbitMqConsumerSessionFactory
{
    private readonly RabbitMqConnectionManager _connectionManager;
    private readonly RabbitMqTopologyInitializer _topologyInitializer;
    private readonly ILoggerFactory _loggerFactory;

    internal RabbitMqConsumerSessionFactory(
        RabbitMqConnectionManager connectionManager,
        RabbitMqTopologyInitializer topologyInitializer,
        ILoggerFactory loggerFactory)
    {
        _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        _topologyInitializer = topologyInitializer ?? throw new ArgumentNullException(nameof(topologyInitializer));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
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
            prefetchCount);
    }
}

/// <summary>
/// Reconciles desired RabbitMQ consumer sessions from authoritative account snapshot state.
/// </summary>
internal sealed class RabbitMqConsumerService : BackgroundService
{
    private static readonly TimeSpan ReconcileInterval = TimeSpan.FromSeconds(15);

    private readonly MySqlNntpAccountSnapshotProvider _accountSnapshotProvider;
    private readonly RabbitMqConnectionManager _connectionManager;
    private readonly IRabbitMqConsumerSessionFactory _sessionFactory;
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly RabbitMqConsumerInfrastructureOptions _consumerOptions;
    private readonly ILogger<RabbitMqConsumerService> _logger;
    private readonly SemaphoreSlim _stateGate = new(1, 1);
    private readonly CancellationTokenSource _shutdownCts = new();
    private readonly Dictionary<string, SessionRuntimeState> _sessionRuntimes = new(StringComparer.Ordinal);
    private readonly Channel<RabbitMqArticleDelivery> _deliveryChannel;

    private volatile bool _shutdownRequested;

    internal RabbitMqConsumerService(
        BackFillerRuntimeOptions runtimeOptions,
        MySqlNntpAccountSnapshotProvider accountSnapshotProvider,
        RabbitMqConnectionManager connectionManager,
        IRabbitMqConsumerSessionFactory sessionFactory,
        ShutdownCoordinator shutdownCoordinator,
        ILogger<RabbitMqConsumerService> logger)
    {
        ArgumentNullException.ThrowIfNull(runtimeOptions);
        ArgumentNullException.ThrowIfNull(accountSnapshotProvider);
        ArgumentNullException.ThrowIfNull(connectionManager);
        ArgumentNullException.ThrowIfNull(sessionFactory);
        ArgumentNullException.ThrowIfNull(shutdownCoordinator);
        ArgumentNullException.ThrowIfNull(logger);

        _accountSnapshotProvider = accountSnapshotProvider;
        _connectionManager = connectionManager;
        _sessionFactory = sessionFactory;
        _shutdownCoordinator = shutdownCoordinator;
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
        _shutdownCoordinator.GracefulShutdownStartedToken.Register(OnShutdownSignaled);
        _shutdownCoordinator.ForcedShutdownToken.Register(OnShutdownSignaled);
    }

    /// <summary>
    /// Gets the bounded infrastructure delivery stream for the next processing layer.
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

    /// <summary>
    /// Gets the currently active consumer session count.
    /// </summary>
    internal int ActiveSessionCount
    {
        get
        {
            lock (_sessionRuntimes)
            {
                return _sessionRuntimes.Count;
            }
        }
    }

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
        _deliveryChannel.Writer.TryComplete();
        _connectionManager.ConnectionReplaced -= OnConnectionReplaced;
        _shutdownCts.Dispose();

        LogConsumerServiceStopped(_logger);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        OnShutdownSignaled();
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }

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
                _stateGate.Release();
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

    private async Task ReconcileSessionsAsync(CancellationToken cancellationToken)
    {
        await _stateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_shutdownRequested)
            {
                return;
            }

            NntpAccountSnapshotState snapshot = _accountSnapshotProvider.CurrentSnapshot;
            Dictionary<string, RabbitMqConsumerSessionIdentity> desiredSessions = BuildDesiredSessions(snapshot);

            foreach ((string sessionKey, RabbitMqConsumerSessionIdentity desiredIdentity) in desiredSessions)
            {
                cancellationToken.ThrowIfCancellationRequested();

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
                else if (runtimeState.Identity != desiredIdentity)
                {
                    await runtimeState.Session.StopAsync(cancellationToken).ConfigureAwait(false);
                    await runtimeState.Session.DisposeAsync().ConfigureAwait(false);

                    IRabbitMqConsumerSession replacement = _sessionFactory.CreateSession(
                        desiredIdentity,
                        new RabbitMqDeliveryChannelSink(_deliveryChannel.Writer),
                        _consumerOptions.PrefetchCount);

                    runtimeState = new SessionRuntimeState(desiredIdentity, replacement);
                    _sessionRuntimes[sessionKey] = runtimeState;

                    LogConsumerSessionReplaced(
                        _logger,
                        desiredIdentity.Backbone,
                        desiredIdentity.AccountUsername,
                        desiredIdentity.ConnectionNumber,
                        desiredIdentity.ConnectionLimit,
                        sessionKey);
                }

                if (!runtimeState.Session.IsRunning)
                {
                    await runtimeState.Session.StartAsync(cancellationToken).ConfigureAwait(false);
                }
            }

            List<KeyValuePair<string, SessionRuntimeState>> staleSessions = [.. _sessionRuntimes.Where(static kvp => !kvp.Value.Desired)];
            for (int i = 0; i < staleSessions.Count; i++)
            {
                KeyValuePair<string, SessionRuntimeState> stale = staleSessions[i];
                _sessionRuntimes.Remove(stale.Key);

                await stale.Value.Session.StopAsync(cancellationToken).ConfigureAwait(false);
                await stale.Value.Session.DisposeAsync().ConfigureAwait(false);

                LogConsumerSessionRetired(
                    _logger,
                    stale.Value.Identity.Backbone,
                    stale.Value.Identity.AccountUsername,
                    stale.Value.Identity.ConnectionNumber,
                    stale.Value.Identity.ConnectionLimit,
                    stale.Key);
            }

            LogConsumerReconcileCompleted(_logger, desiredSessions.Count, _sessionRuntimes.Count);
        }
        finally
        {
            foreach (SessionRuntimeState runtime in _sessionRuntimes.Values)
            {
                runtime.Desired = false;
            }

            _stateGate.Release();
        }
    }

    private Dictionary<string, RabbitMqConsumerSessionIdentity> BuildDesiredSessions(NntpAccountSnapshotState snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

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

        foreach (SessionRuntimeState runtime in _sessionRuntimes.Values)
        {
            runtime.Desired = false;
        }

        foreach (string key in desired.Keys)
        {
            if (_sessionRuntimes.TryGetValue(key, out SessionRuntimeState? runtime))
            {
                runtime.Desired = true;
            }
        }

        return desired;
    }

    private async Task StopAllSessionsAsync(CancellationToken cancellationToken)
    {
        SessionRuntimeState[] runtimes = [.. _sessionRuntimes.Values];
        _sessionRuntimes.Clear();

        for (int i = 0; i < runtimes.Length; i++)
        {
            try
            {
                await runtimes[i].Session.StopAsync(cancellationToken).ConfigureAwait(false);
                await runtimes[i].Session.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogConsumerSessionStopFailed(_logger, runtimes[i].Identity.SessionKey, ex);
            }
        }
    }

    private void OnShutdownSignaled()
    {
        if (_shutdownRequested)
        {
            return;
        }

        _shutdownRequested = true;
        _shutdownCts.Cancel();
    }

    private sealed class SessionRuntimeState(RabbitMqConsumerSessionIdentity identity, IRabbitMqConsumerSession session)
    {
        internal RabbitMqConsumerSessionIdentity Identity { get; } = identity;

        internal IRabbitMqConsumerSession Session { get; } = session;

        internal bool Desired { get; set; }
    }

    [LoggerMessage(EventId = 4400, Level = LogLevel.Information, Message = "RabbitMQ consumer service starting. DeliveryBufferCapacity={DeliveryBufferCapacity}, Prefetch={PrefetchCount}")]
    private static partial void LogConsumerServiceStarting(ILogger logger, int deliveryBufferCapacity, ushort? prefetchCount);

    [LoggerMessage(EventId = 4401, Level = LogLevel.Information, Message = "RabbitMQ consumer service stopped")]
    private static partial void LogConsumerServiceStopped(ILogger logger);

    [LoggerMessage(EventId = 4402, Level = LogLevel.Warning, Message = "RabbitMQ consumer reconciliation cycle failed unexpectedly")]
    private static partial void LogConsumerServiceReconcileFailed(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 4403, Level = LogLevel.Warning, Message = "RabbitMQ connection replacement dispatch to consumer sessions failed")]
    private static partial void LogConnectionReplacedDispatchFailed(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 4404, Level = LogLevel.Information, Message = "RabbitMQ consumer session created. Backbone={Backbone} Account={Account} Connection={ConnectionNumber}/{ConnectionLimit} SessionKey={SessionKey}")]
    private static partial void LogConsumerSessionCreated(ILogger logger, string backbone, string account, int connectionNumber, int connectionLimit, string sessionKey);

    [LoggerMessage(EventId = 4405, Level = LogLevel.Information, Message = "RabbitMQ consumer session replaced due to account configuration change. Backbone={Backbone} Account={Account} Connection={ConnectionNumber}/{ConnectionLimit} SessionKey={SessionKey}")]
    private static partial void LogConsumerSessionReplaced(ILogger logger, string backbone, string account, int connectionNumber, int connectionLimit, string sessionKey);

    [LoggerMessage(EventId = 4406, Level = LogLevel.Information, Message = "RabbitMQ consumer session retired. Backbone={Backbone} Account={Account} Connection={ConnectionNumber}/{ConnectionLimit} SessionKey={SessionKey}")]
    private static partial void LogConsumerSessionRetired(ILogger logger, string backbone, string account, int connectionNumber, int connectionLimit, string sessionKey);

    [LoggerMessage(EventId = 4407, Level = LogLevel.Information, Message = "RabbitMQ consumer reconciliation completed. DesiredSessions={DesiredSessions} ActiveSessions={ActiveSessions}")]
    private static partial void LogConsumerReconcileCompleted(ILogger logger, int desiredSessions, int activeSessions);

    [LoggerMessage(EventId = 4408, Level = LogLevel.Warning, Message = "RabbitMQ consumer session stop/dispose failed during service shutdown. SessionKey={SessionKey}")]
    private static partial void LogConsumerSessionStopFailed(ILogger logger, string sessionKey, Exception exception);
}
