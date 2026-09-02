// <copyright file="RabbitMqConnectionManager.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / RabbitMq
// Implements the rabbit mq connection manager behavior.

using System.Diagnostics;
using RabbitMQ.Client.Events;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Shutdown;

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// Owns RabbitMQ connection lifecycle, readiness state, and application-level recovery.
    /// </summary>
    internal sealed partial class RabbitMqConnectionManager : IAsyncDisposable
    {
        /// <summary>
        /// Tracks options for rabbit mq connection manager.
        /// </summary>
        private readonly RabbitMqRuntimeOptions _options;
        /// <summary>
        /// Tracks connection name for rabbit mq connection manager.
        /// </summary>
        private readonly string _connectionName;
        /// <summary>
        /// Tracks connector for rabbit mq connection manager.
        /// </summary>
        private readonly IRabbitMqBrokerConnector _connector;
        /// <summary>
        /// Tracks shutdown coordinator for rabbit mq connection manager.
        /// </summary>
        private readonly ShutdownCoordinator _shutdownCoordinator;
        /// <summary>
        /// Tracks time provider for rabbit mq connection manager.
        /// </summary>
        private readonly TimeProvider _timeProvider;
        /// <summary>
        /// Provides logging for rabbit mq connection manager.
        /// </summary>
        private readonly ILogger<RabbitMqConnectionManager> _logger;
        /// <summary>
        /// Tracks state gate for rabbit mq connection manager.
        /// </summary>
        private readonly SemaphoreSlim _stateGate = new(1, 1);
        /// <summary>
        /// Tracks recovery signal for rabbit mq connection manager.
        /// </summary>
        private readonly SemaphoreSlim _recoverySignal = new(0, int.MaxValue);
        /// <summary>
        /// Tracks shutdown cts for rabbit mq connection manager.
        /// </summary>
        private readonly CancellationTokenSource _shutdownCts = new();

        /// <summary>
        /// Tracks connection for rabbit mq connection manager.
        /// </summary>
        private IRabbitMqBrokerConnection? _connection;
        /// <summary>
        /// Tracks recovery task for rabbit mq connection manager.
        /// </summary>
        private Task? _recoveryTask;
        /// <summary>
        /// Tracks graceful shutdown registration for rabbit mq connection manager.
        /// </summary>
        private IDisposable? _gracefulShutdownRegistration;
        /// <summary>
        /// Tracks forced shutdown registration for rabbit mq connection manager.
        /// </summary>
        private IDisposable? _forcedShutdownRegistration;

        /// <summary>
        /// Tracks state for rabbit mq connection manager.
        /// </summary>
        private volatile RabbitMqInfrastructureState _state = RabbitMqInfrastructureState.NotInitialized;
        /// <summary>
        /// Tracks dispose requested for rabbit mq connection manager.
        /// </summary>
        private volatile bool _disposeRequested;
        /// <summary>
        /// Tracks recovery queued for rabbit mq connection manager.
        /// </summary>
        private int _recoveryQueued;
        /// <summary>
        /// Tracks recovery attempt for rabbit mq connection manager.
        /// </summary>
        private int _recoveryAttempt;
        /// <summary>
        /// Tracks consecutive client recovery errors for rabbit mq connection manager.
        /// </summary>
        private int _consecutiveClientRecoveryErrors;
        /// <summary>
        /// Tracks connection generation for rabbit mq connection manager.
        /// </summary>
        private long _connectionGeneration;
        /// <summary>
        /// Tracks topology initialized for rabbit mq connection manager.
        /// </summary>
        private volatile bool _topologyInitialized;
        /// <summary>
        /// Tracks last connected at utc for rabbit mq connection manager.
        /// </summary>
        private DateTimeOffset? _lastConnectedAtUtc;

        /// <summary>
        /// Initializes a new RabbitMQ connection manager.
        /// </summary>
        /// <param name="runtimeOptions">Validated runtime options snapshot.</param>
        /// <param name="shutdownCoordinator">Application shutdown coordinator.</param>
        /// <param name="timeProvider">Unified time provider.</param>
        /// <param name="logger">RabbitMQ lifecycle logger.</param>
        /// <param name="connector">Optional connector implementation for tests.</param>
        public RabbitMqConnectionManager(
            BackFillerRuntimeOptions runtimeOptions,
            ShutdownCoordinator shutdownCoordinator,
            TimeProvider timeProvider,
            ILogger<RabbitMqConnectionManager> logger,
            IRabbitMqBrokerConnector? connector = null)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);
            ArgumentNullException.ThrowIfNull(shutdownCoordinator);
            ArgumentNullException.ThrowIfNull(timeProvider);
            ArgumentNullException.ThrowIfNull(logger);

            _options = runtimeOptions.RabbitMq ?? throw new InvalidOperationException("Validated runtime RabbitMQ settings were not provided.");
            _connectionName = RabbitMqRuntimeOptions.GetDefaultConnectionName(runtimeOptions.CanonicalBackFillerFqdn);
            _shutdownCoordinator = shutdownCoordinator;
            _timeProvider = timeProvider;
            _logger = logger;
            _connector = connector ?? new RabbitMqBrokerConnector();

            _gracefulShutdownRegistration = _shutdownCoordinator.GracefulShutdownStartedToken.Register(() => OnShutdownSignaled("graceful"));
            _forcedShutdownRegistration = _shutdownCoordinator.ForcedShutdownToken.Register(() => OnShutdownSignaled("forced"));
        }

        /// <summary>
        /// Gets the current RabbitMQ lifecycle state.
        /// </summary>
        internal RabbitMqInfrastructureState State => _state;

        /// <summary>
        /// Gets a value indicating whether RabbitMQ is fully ready for topology/channel operations.
        /// </summary>
        internal bool IsReady => _state is RabbitMqInfrastructureState.TopologyReady or RabbitMqInfrastructureState.Connected;

        /// <summary>
        /// Gets the current monotonic RabbitMQ connection generation.
        /// </summary>
        internal long ConnectionGeneration => Interlocked.Read(ref _connectionGeneration);

        /// <summary>
        /// Raised after a RabbitMQ connection is established or replaced.
        /// </summary>
        internal event EventHandler<RabbitMqConnectionReplacedEventArgs>? ConnectionReplaced;

        /// <summary>
        /// Initializes and connects to RabbitMQ for startup dependency readiness.
        /// </summary>
        /// <param name="cancellationToken">Startup cancellation token.</param>
        /// <returns>A task that completes when an initial RabbitMQ connection is ready.</returns>
        internal async Task EnsureConnectedAsync(CancellationToken cancellationToken)
        {
            ThrowIfStopping();

            if (State is RabbitMqInfrastructureState.Connected or RabbitMqInfrastructureState.TopologyReady)
            {
                return;
            }

            await _stateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                ThrowIfStopping();
                if (State is RabbitMqInfrastructureState.Connected or RabbitMqInfrastructureState.TopologyReady)
                {
                    return;
                }

                await ConnectCoreAsync(cancellationToken).ConfigureAwait(false);

                _recoveryTask ??= Task.Run(RecoveryLoopAsync, cancellationToken);
            }
            finally
            {
                _ = _stateGate.Release();
            }
        }

        /// <summary>
        /// Marks the connection as topology-ready after topology declaration completes.
        /// </summary>
        internal void MarkTopologyReady()
        {
            _topologyInitialized = true;

            if (State is RabbitMqInfrastructureState.Connected or RabbitMqInfrastructureState.TopologyReady)
            {
                _state = RabbitMqInfrastructureState.TopologyReady;
            }
        }

        /// <summary>
        /// Creates a dedicated owned channel lease for one logical owner.
        /// </summary>
        /// <param name="owner">Logical owner name for diagnostics.</param>
        /// <param name="cancellationToken">Channel-creation cancellation token.</param>
        /// <param name="enablePublisherConfirmations">Whether publisher confirmations should be enabled on the created channel.</param>
        /// <returns>Independently owned channel lease.</returns>
        internal async Task<RabbitMqOwnedChannel> CreateOwnedChannelAsync(string owner, CancellationToken cancellationToken, bool enablePublisherConfirmations = false)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(owner);
            ThrowIfStopping();

            IRabbitMqBrokerConnection connection = _connection
                ?? throw new InvalidOperationException("RabbitMQ connection has not been established.");

            if (!connection.IsOpen)
            {
                throw new InvalidOperationException("RabbitMQ connection is not open.");
            }

            long generation = ConnectionGeneration;
            IRabbitMqChannel channel = await connection.CreateChannelAsync(cancellationToken, enablePublisherConfirmations).ConfigureAwait(false);
            return new RabbitMqOwnedChannel(channel, owner, generation);
        }

        /// <summary>
        /// Gets the current connection for topology operations.
        /// </summary>
        /// <returns>The current connection when connected.</returns>
        internal IRabbitMqBrokerConnection GetRequiredConnection()
        {
            IRabbitMqBrokerConnection connection = _connection
                ?? throw new InvalidOperationException("RabbitMQ connection has not been established.");

            return !connection.IsOpen ? throw new InvalidOperationException("RabbitMQ connection is not open.") : connection;
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            if (_disposeRequested)
            {
                return;
            }

            _disposeRequested = true;
            _state = RabbitMqInfrastructureState.Stopping;
            _shutdownCts.Cancel();

            _gracefulShutdownRegistration?.Dispose();
            _gracefulShutdownRegistration = null;

            _forcedShutdownRegistration?.Dispose();
            _forcedShutdownRegistration = null;

            _ = _recoverySignal.Release();

            try
            {
                if (_recoveryTask is not null)
                {
                    await _recoveryTask.ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // No-op: expected during shutdown.
            }

            await DisposeConnectionAsync().ConfigureAwait(false);

            _state = RabbitMqInfrastructureState.Stopped;
            LogShutdownCompleted(_logger);

            _stateGate.Dispose();
            _recoverySignal.Dispose();
            _shutdownCts.Dispose();
        }

        /// <summary>
        /// Coordinates connect core async for rabbit mq connection manager.
        /// </summary>
        private async Task ConnectCoreAsync(CancellationToken cancellationToken)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();

            _state = RabbitMqInfrastructureState.Connecting;
            RabbitMqConnectionFactorySnapshot snapshot = RabbitMqConnectionFactoryBuilder.BuildSanitizedSnapshot(_options, _connectionName);
            LogConnectionAttempt(_logger, snapshot.Hosts, snapshot.Port, snapshot.VirtualHost, snapshot.ClientProvidedConnectionName, snapshot.EnableSsl);

            try
            {
                IRabbitMqBrokerConnection connection = await _connector
                    .ConnectAsync(_options, _connectionName, cancellationToken)
                    .ConfigureAwait(false);

                AttachConnectionEvents(connection);
                _connection = connection;
                _recoveryAttempt = 0;
                _ = Interlocked.Exchange(ref _consecutiveClientRecoveryErrors, 0);
                _lastConnectedAtUtc = _timeProvider.GetUtcNow();
                _state = RabbitMqInfrastructureState.Connected;

                long generation = Interlocked.Increment(ref _connectionGeneration);
                bool isReplacement = generation > 1;

                LogConnectionSucceeded(
                    _logger,
                    connection.EndpointHostName,
                    connection.EndpointPort,
                    connection.VirtualHost,
                    connection.ClientProvidedName,
                    stopwatch.Elapsed.TotalMilliseconds);

                ConnectionReplaced?.Invoke(this, new RabbitMqConnectionReplacedEventArgs(generation, isReplacement));
            }
            catch (Exception ex)
            {
                _state = RabbitMqInfrastructureState.Failed;
                LogConnectionFailed(_logger, snapshot.Hosts, snapshot.Port, snapshot.VirtualHost, snapshot.ClientProvidedConnectionName, stopwatch.Elapsed.TotalMilliseconds, ex);
                throw;
            }
        }

        /// <summary>
        /// Coordinates recovery loop async for rabbit mq connection manager.
        /// </summary>
        private async Task RecoveryLoopAsync()
        {
            while (!_shutdownCts.IsCancellationRequested)
            {
                try
                {
                    await _recoverySignal.WaitAsync(_shutdownCts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                if (_shutdownCts.IsCancellationRequested || _disposeRequested)
                {
                    break;
                }

                _ = Interlocked.Exchange(ref _recoveryQueued, 0);

                await _stateGate.WaitAsync(_shutdownCts.Token).ConfigureAwait(false);
                try
                {
                    if (_shutdownCts.IsCancellationRequested || _disposeRequested)
                    {
                        break;
                    }

                    _state = RabbitMqInfrastructureState.Reconnecting;
                    int failureCount = 0;

                    while (!_shutdownCts.IsCancellationRequested && !_disposeRequested)
                    {
                        _recoveryAttempt++;
                        TimeSpan delay = ComputeRecoveryBackoff(_recoveryAttempt);
                        LogRecoveryStarting(_logger, _recoveryAttempt, delay.TotalMilliseconds);

                        try
                        {
                            await Task.Delay(delay, _shutdownCts.Token).ConfigureAwait(false);
                            await DisposeConnectionAsync().ConfigureAwait(false);
                            await ConnectCoreAsync(_shutdownCts.Token).ConfigureAwait(false);

                            LogRecoverySucceeded(_logger, _recoveryAttempt);
                            break;
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                        catch (Exception ex)
                        {
                            failureCount++;
                            LogRecoveryFailed(_logger, _recoveryAttempt, failureCount, ex.Message);

                            if (failureCount >= _options.MaxConsecutiveRecoveryFailures)
                            {
                                _state = RabbitMqInfrastructureState.Failed;
                                LogRecoveryFailureThresholdReached(_logger, failureCount);
                                break;
                            }
                        }
                    }
                }
                finally
                {
                    _ = _stateGate.Release();
                }
            }
        }

        /// <summary>
        /// Coordinates queue recovery for rabbit mq connection manager.
        /// </summary>
        private void QueueRecovery(string reason)
        {
            if (_disposeRequested || _shutdownCts.IsCancellationRequested)
            {
                return;
            }

            if (Interlocked.Exchange(ref _recoveryQueued, 1) == 0)
            {
                LogRecoveryQueued(_logger, reason);
                _ = _recoverySignal.Release();
            }
        }

        /// <summary>
        /// Coordinates dispose connection async for rabbit mq connection manager.
        /// </summary>
        private async Task DisposeConnectionAsync()
        {
            IRabbitMqBrokerConnection? connection = Interlocked.Exchange(ref _connection, null);
            if (connection is null)
            {
                return;
            }

            DetachConnectionEvents(connection);

            try
            {
                await connection.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                LogConnectionDisposeFailed(_logger, ex);
            }
        }

        /// <summary>
        /// Coordinates compute recovery backoff for rabbit mq connection manager.
        /// </summary>
        private TimeSpan ComputeRecoveryBackoff(int attempt)
        {
            int boundedAttempt = Math.Clamp(attempt, 1, 30);
            double exponential = Math.Pow(2, boundedAttempt - 1);
            double delayMs = _options.PoolReconnectBaseDelayMs * exponential;
            delayMs = Math.Min(delayMs, _options.PoolReconnectMaxDelayMs);
            return TimeSpan.FromMilliseconds(delayMs);
        }

        /// <summary>
        /// Coordinates attach connection events for rabbit mq connection manager.
        /// </summary>
        private void AttachConnectionEvents(IRabbitMqBrokerConnection connection)
        {
            connection.ConnectionShutdown += OnConnectionShutdown;
            connection.CallbackException += OnCallbackException;
            connection.ConnectionBlocked += OnConnectionBlocked;
            connection.ConnectionUnblocked += OnConnectionUnblocked;
            connection.ConnectionRecoveryError += OnConnectionRecoveryError;
            connection.RecoverySucceeded += OnRecoverySucceeded;
        }

        /// <summary>
        /// Coordinates detach connection events for rabbit mq connection manager.
        /// </summary>
        private void DetachConnectionEvents(IRabbitMqBrokerConnection connection)
        {
            connection.ConnectionShutdown -= OnConnectionShutdown;
            connection.CallbackException -= OnCallbackException;
            connection.ConnectionBlocked -= OnConnectionBlocked;
            connection.ConnectionUnblocked -= OnConnectionUnblocked;
            connection.ConnectionRecoveryError -= OnConnectionRecoveryError;
            connection.RecoverySucceeded -= OnRecoverySucceeded;
        }

        /// <summary>
        /// Coordinates on connection shutdown for rabbit mq connection manager.
        /// </summary>
        private void OnConnectionShutdown(object? sender, ShutdownEventArgs eventArgs)
        {
            LogConnectionShutdown(_logger, eventArgs.ReplyCode, eventArgs.ReplyText, eventArgs.Initiator.ToString());

            if (_disposeRequested || _shutdownCts.IsCancellationRequested)
            {
                return;
            }

            _state = RabbitMqInfrastructureState.Reconnecting;
            QueueRecovery($"connection-shutdown:{eventArgs.ReplyCode}");
        }

        /// <summary>
        /// Coordinates on callback exception for rabbit mq connection manager.
        /// </summary>
        private void OnCallbackException(object? sender, CallbackExceptionEventArgs eventArgs)
        {
            LogConnectionCallbackException(_logger, eventArgs.Exception.Message);
        }

        /// <summary>
        /// Coordinates on connection blocked for rabbit mq connection manager.
        /// </summary>
        private void OnConnectionBlocked(object? sender, ConnectionBlockedEventArgs eventArgs)
        {
            LogConnectionBlocked(_logger, eventArgs.Reason);
        }

        /// <summary>
        /// Coordinates on connection unblocked for rabbit mq connection manager.
        /// </summary>
        private void OnConnectionUnblocked(object? sender, AsyncEventArgs eventArgs)
        {
            LogConnectionUnblocked(_logger);
        }

        /// <summary>
        /// Coordinates on connection recovery error for rabbit mq connection manager.
        /// </summary>
        private void OnConnectionRecoveryError(object? sender, ConnectionRecoveryErrorEventArgs eventArgs)
        {
            int consecutiveErrors = Interlocked.Increment(ref _consecutiveClientRecoveryErrors);
            LogClientAutomaticRecoveryError(_logger, consecutiveErrors, eventArgs.Exception.Message);

            if (consecutiveErrors >= _options.MaxConsecutiveRecoveryFailures)
            {
                LogClientAutomaticRecoveryThresholdReached(_logger, consecutiveErrors);
                QueueRecovery("automatic-recovery-error-threshold");
            }
        }

        /// <summary>
        /// Coordinates on recovery succeeded for rabbit mq connection manager.
        /// </summary>
        private void OnRecoverySucceeded(object? sender, AsyncEventArgs eventArgs)
        {
            _ = Interlocked.Exchange(ref _consecutiveClientRecoveryErrors, 0);
            _state = _topologyInitialized
                ? RabbitMqInfrastructureState.TopologyReady
                : RabbitMqInfrastructureState.Connected;
            LogClientAutomaticRecoverySucceeded(_logger);
        }

        /// <summary>
        /// Coordinates on shutdown signaled for rabbit mq connection manager.
        /// </summary>
        private void OnShutdownSignaled(string shutdownType)
        {
            if (_disposeRequested)
            {
                return;
            }

            _disposeRequested = true;
            _state = RabbitMqInfrastructureState.Stopping;
            _shutdownCts.Cancel();
            _ = _recoverySignal.Release();
            LogShutdownSignalObserved(_logger, shutdownType);
        }

        /// <summary>
        /// Coordinates throw if stopping for rabbit mq connection manager.
        /// </summary>
        private void ThrowIfStopping()
        {
            if (_disposeRequested || _shutdownCts.IsCancellationRequested)
            {
                throw new InvalidOperationException("RabbitMQ connection manager is stopping.");
            }
        }

        [LoggerMessage(EventId = 4000, Level = LogLevel.Information, Message = "RabbitMQ connection attempt started. Hosts={Hosts} Port={Port} VirtualHost={VirtualHost} ConnectionName={ConnectionName} EnableSsl={EnableSsl}")]
        /// <summary>
        /// Coordinates log connection attempt for rabbit mq connection manager.
        /// </summary>
        private static partial void LogConnectionAttempt(ILogger logger, IReadOnlyList<string> hosts, int port, string virtualHost, string connectionName, bool enableSsl);

        [LoggerMessage(EventId = 4001, Level = LogLevel.Information, Message = "RabbitMQ connection established. Host={Host} Port={Port} VirtualHost={VirtualHost} ConnectionName={ConnectionName} DurationMs={DurationMs}")]
        /// <summary>
        /// Coordinates log connection succeeded for rabbit mq connection manager.
        /// </summary>
        private static partial void LogConnectionSucceeded(ILogger logger, string host, int port, string virtualHost, string connectionName, double durationMs);

        [LoggerMessage(EventId = 4002, Level = LogLevel.Error, Message = "RabbitMQ connection attempt failed. Hosts={Hosts} Port={Port} VirtualHost={VirtualHost} ConnectionName={ConnectionName} DurationMs={DurationMs}")]
        /// <summary>
        /// Coordinates log connection failed for rabbit mq connection manager.
        /// </summary>
        private static partial void LogConnectionFailed(ILogger logger, IReadOnlyList<string> hosts, int port, string virtualHost, string connectionName, double durationMs, Exception exception);

        [LoggerMessage(EventId = 4003, Level = LogLevel.Warning, Message = "RabbitMQ connection shutdown observed. ReplyCode={ReplyCode} ReplyText={ReplyText} Initiator={Initiator}")]
        /// <summary>
        /// Coordinates log connection shutdown for rabbit mq connection manager.
        /// </summary>
        private static partial void LogConnectionShutdown(ILogger logger, ushort replyCode, string replyText, string initiator);

        [LoggerMessage(EventId = 4004, Level = LogLevel.Warning, Message = "RabbitMQ callback exception observed. Message={Message}")]
        /// <summary>
        /// Coordinates log connection callback exception for rabbit mq connection manager.
        /// </summary>
        private static partial void LogConnectionCallbackException(ILogger logger, string message);

        [LoggerMessage(EventId = 4005, Level = LogLevel.Warning, Message = "RabbitMQ broker blocked the connection. Reason={Reason}")]
        /// <summary>
        /// Coordinates log connection blocked for rabbit mq connection manager.
        /// </summary>
        private static partial void LogConnectionBlocked(ILogger logger, string reason);

        [LoggerMessage(EventId = 4006, Level = LogLevel.Information, Message = "RabbitMQ broker unblocked the connection")]
        /// <summary>
        /// Coordinates log connection unblocked for rabbit mq connection manager.
        /// </summary>
        private static partial void LogConnectionUnblocked(ILogger logger);

        [LoggerMessage(EventId = 4007, Level = LogLevel.Warning, Message = "RabbitMQ recovery queued. Reason={Reason}")]
        /// <summary>
        /// Coordinates log recovery queued for rabbit mq connection manager.
        /// </summary>
        private static partial void LogRecoveryQueued(ILogger logger, string reason);

        [LoggerMessage(EventId = 4008, Level = LogLevel.Information, Message = "RabbitMQ recovery attempt starting. Attempt={Attempt} BackoffMs={BackoffMs}")]
        /// <summary>
        /// Coordinates log recovery starting for rabbit mq connection manager.
        /// </summary>
        private static partial void LogRecoveryStarting(ILogger logger, int attempt, double backoffMs);

        [LoggerMessage(EventId = 4009, Level = LogLevel.Information, Message = "RabbitMQ recovery attempt succeeded. Attempt={Attempt}")]
        /// <summary>
        /// Coordinates log recovery succeeded for rabbit mq connection manager.
        /// </summary>
        private static partial void LogRecoverySucceeded(ILogger logger, int attempt);

        [LoggerMessage(EventId = 4010, Level = LogLevel.Error, Message = "RabbitMQ recovery attempt failed. Attempt={Attempt} ConsecutiveFailures={ConsecutiveFailures} Reason={Reason}")]
        /// <summary>
        /// Coordinates log recovery failed for rabbit mq connection manager.
        /// </summary>
        private static partial void LogRecoveryFailed(ILogger logger, int attempt, int consecutiveFailures, string reason);

        [LoggerMessage(EventId = 4011, Level = LogLevel.Error, Message = "RabbitMQ recovery failure threshold reached. ConsecutiveFailures={ConsecutiveFailures}")]
        /// <summary>
        /// Coordinates log recovery failure threshold reached for rabbit mq connection manager.
        /// </summary>
        private static partial void LogRecoveryFailureThresholdReached(ILogger logger, int consecutiveFailures);

        [LoggerMessage(EventId = 4012, Level = LogLevel.Warning, Message = "RabbitMQ client automatic recovery error observed. ConsecutiveErrors={ConsecutiveErrors} Reason={Reason}")]
        /// <summary>
        /// Coordinates log client automatic recovery error for rabbit mq connection manager.
        /// </summary>
        private static partial void LogClientAutomaticRecoveryError(ILogger logger, int consecutiveErrors, string reason);

        [LoggerMessage(EventId = 4013, Level = LogLevel.Warning, Message = "RabbitMQ client automatic recovery error threshold reached. ConsecutiveErrors={ConsecutiveErrors}")]
        /// <summary>
        /// Coordinates log client automatic recovery threshold reached for rabbit mq connection manager.
        /// </summary>
        private static partial void LogClientAutomaticRecoveryThresholdReached(ILogger logger, int consecutiveErrors);

        [LoggerMessage(EventId = 4014, Level = LogLevel.Information, Message = "RabbitMQ client automatic recovery succeeded")]
        /// <summary>
        /// Coordinates log client automatic recovery succeeded for rabbit mq connection manager.
        /// </summary>
        private static partial void LogClientAutomaticRecoverySucceeded(ILogger logger);

        [LoggerMessage(EventId = 4015, Level = LogLevel.Warning, Message = "RabbitMQ shutdown signal observed from ShutdownCoordinator. Type={ShutdownType}")]
        /// <summary>
        /// Coordinates log shutdown signal observed for rabbit mq connection manager.
        /// </summary>
        private static partial void LogShutdownSignalObserved(ILogger logger, string shutdownType);

        [LoggerMessage(EventId = 4016, Level = LogLevel.Error, Message = "RabbitMQ connection disposal failed")]
        /// <summary>
        /// Coordinates log connection dispose failed for rabbit mq connection manager.
        /// </summary>
        private static partial void LogConnectionDisposeFailed(ILogger logger, Exception exception);

        [LoggerMessage(EventId = 4017, Level = LogLevel.Information, Message = "RabbitMQ connection manager shutdown completed")]
        /// <summary>
        /// Coordinates log shutdown completed for rabbit mq connection manager.
        /// </summary>
        private static partial void LogShutdownCompleted(ILogger logger);
    }
}
