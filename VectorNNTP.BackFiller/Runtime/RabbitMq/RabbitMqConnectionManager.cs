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
        /// Validated RabbitMQ runtime options that drive connection and recovery behavior.
        /// </summary>
        private readonly RabbitMqRuntimeOptions _options;
        /// <summary>
        /// Client-provided connection name exposed in broker diagnostics.
        /// </summary>
        private readonly string _connectionName;
        /// <summary>
        /// Connector abstraction responsible for opening new broker connections.
        /// </summary>
        private readonly IRabbitMqBrokerConnector _connector;
        /// <summary>
        /// Application shutdown coordinator that requests graceful or forced stop.
        /// </summary>
        private readonly ShutdownCoordinator _shutdownCoordinator;
        /// <summary>
        /// Supplies the logger used by rabbit mq connection manager.
        /// </summary>
        private readonly ILogger<RabbitMqConnectionManager> _logger;
        /// <summary>
        /// Gate that serializes connect, recover, and dispose transitions.
        /// </summary>
        private readonly SemaphoreSlim _stateGate = new(1, 1);
        /// <summary>
        /// Signal used to wake the background recovery loop when reconnect work is queued.
        /// </summary>
        private readonly SemaphoreSlim _recoverySignal = new(0, int.MaxValue);
        /// <summary>
        /// Session-wide cancellation source for recovery-loop and shutdown coordination.
        /// </summary>
        private readonly CancellationTokenSource _shutdownCts = new();

        /// <summary>
        /// Current live broker connection, if one has been established.
        /// </summary>
        private IRabbitMqBrokerConnection? _connection;
        /// <summary>
        /// Background recovery loop task started after the initial connect.
        /// </summary>
        private Task? _recoveryTask;
        /// <summary>
        /// Registration for graceful-shutdown notifications.
        /// </summary>
        private IDisposable? _gracefulShutdownRegistration;
        /// <summary>
        /// Registration for forced-shutdown notifications.
        /// </summary>
        private IDisposable? _forcedShutdownRegistration;

        /// <summary>
        /// Observable RabbitMQ infrastructure state reported to other startup and runtime components.
        /// </summary>
        private volatile RabbitMqInfrastructureState _state = RabbitMqInfrastructureState.NotInitialized;
        /// <summary>
        /// Indicates that shutdown has started and no new connect work should begin.
        /// </summary>
        private volatile bool _disposeRequested;
        /// <summary>
        /// Single-bit flag that coalesces concurrent recovery requests.
        /// </summary>
        private int _recoveryQueued;
        /// <summary>
        /// Number of the current application-managed recovery attempt sequence.
        /// </summary>
        private int _recoveryAttempt;
        /// <summary>
        /// Count of consecutive automatic-recovery errors reported by RabbitMQ.Client.
        /// </summary>
        private int _consecutiveClientRecoveryErrors;
        /// <summary>
        /// Monotonic generation number incremented each time a new live connection is installed.
        /// </summary>
        private long _connectionGeneration;
        /// <summary>
        /// Indicates whether topology declaration has completed for the current connection lifecycle.
        /// </summary>
        private volatile bool _topologyInitialized;

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
        /// <value>The generation assigned to the current live connection, or zero before the first connect.</value>
        internal long ConnectionGeneration => Interlocked.Read(ref _connectionGeneration);

        /// <summary>
        /// Raised after a new broker connection generation becomes active.
        /// </summary>
        /// <remarks>
        /// The first successful connect also raises this event with <c>IsReplacement</c> set to <see langword="false"/>.
        /// Later generations set <c>IsReplacement</c> to <see langword="true"/> so consumers can recreate stale channels.
        /// </remarks>
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
        /// Marks the current connection generation as ready for topology-dependent operations.
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
        /// Creates a dedicated channel lease bound to the current connection generation.
        /// </summary>
        /// <param name="owner">Logical owner name used for diagnostics.</param>
        /// <param name="cancellationToken">Cancellation token for channel creation.</param>
        /// <param name="enablePublisherConfirmations"><see langword="true"/> to enable publisher confirmations on the new channel.</param>
        /// <returns>An independently owned channel lease.</returns>
        /// <exception cref="InvalidOperationException">Thrown when no open connection is currently available.</exception>
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
        /// Returns the current open connection for topology operations.
        /// </summary>
        /// <returns>The current broker connection.</returns>
        /// <exception cref="InvalidOperationException">Thrown when no open connection is available.</exception>
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
        /// Opens a new broker connection, installs it as the active generation, and notifies listeners.
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
        /// Background loop that performs application-managed reconnect attempts after failures.
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
        /// Coalesces and signals a request for application-managed recovery.
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
        /// Detaches events and disposes the currently installed broker connection, if any.
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
        /// Computes the bounded exponential backoff used between reconnect attempts.
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
        /// Attaches connection lifecycle event handlers to a newly installed broker connection.
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
        /// Detaches lifecycle event handlers from a broker connection before disposal.
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
        /// Reacts to broker shutdown notifications by transitioning into reconnecting state and queueing recovery.
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
        /// Logs callback exceptions surfaced by RabbitMQ.Client without changing ownership state.
        /// </summary>
        private void OnCallbackException(object? sender, CallbackExceptionEventArgs eventArgs)
        {
            LogConnectionCallbackException(_logger, eventArgs.Exception.Message);
        }

        /// <summary>
        /// Logs broker-side connection blocking notifications.
        /// </summary>
        private void OnConnectionBlocked(object? sender, ConnectionBlockedEventArgs eventArgs)
        {
            LogConnectionBlocked(_logger, eventArgs.Reason);
        }

        /// <summary>
        /// Logs broker-side connection unblock notifications.
        /// </summary>
        private void OnConnectionUnblocked(object? sender, AsyncEventArgs eventArgs)
        {
            LogConnectionUnblocked(_logger);
        }

        /// <summary>
        /// Tracks RabbitMQ.Client automatic-recovery errors and escalates persistent failure back to the application loop.
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
        /// Restores the observable state after RabbitMQ.Client automatic recovery succeeds.
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
        /// Converts shutdown-coordinator notifications into connection-manager stop state.
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
        /// Throws when the manager is already stopping and can no longer start new work.
        /// </summary>
        private void ThrowIfStopping()
        {
            if (_disposeRequested || _shutdownCts.IsCancellationRequested)
            {
                throw new InvalidOperationException("RabbitMQ connection manager is stopping.");
            }
        }

        /// <summary>
        /// Emits the connection attempt log event for rabbit mq connection manager.
        /// </summary>
        [LoggerMessage(EventId = 4000, Level = LogLevel.Information, Message = "RabbitMQ connection attempt started. Hosts={Hosts} Port={Port} VirtualHost={VirtualHost} ConnectionName={ConnectionName} EnableSsl={EnableSsl}")]
        private static partial void LogConnectionAttempt(ILogger logger, IReadOnlyList<string> hosts, int port, string virtualHost, string connectionName, bool enableSsl);

        /// <summary>
        /// Emits the connection succeeded log event for rabbit mq connection manager.
        /// </summary>
        [LoggerMessage(EventId = 4001, Level = LogLevel.Information, Message = "RabbitMQ connection established. Host={Host} Port={Port} VirtualHost={VirtualHost} ConnectionName={ConnectionName} DurationMs={DurationMs}")]
        private static partial void LogConnectionSucceeded(ILogger logger, string host, int port, string virtualHost, string connectionName, double durationMs);

        /// <summary>
        /// Emits the connection failed log event for rabbit mq connection manager.
        /// </summary>
        [LoggerMessage(EventId = 4002, Level = LogLevel.Error, Message = "RabbitMQ connection attempt failed. Hosts={Hosts} Port={Port} VirtualHost={VirtualHost} ConnectionName={ConnectionName} DurationMs={DurationMs}")]
        private static partial void LogConnectionFailed(ILogger logger, IReadOnlyList<string> hosts, int port, string virtualHost, string connectionName, double durationMs, Exception exception);

        /// <summary>
        /// Emits the connection shutdown log event for rabbit mq connection manager.
        /// </summary>
        [LoggerMessage(EventId = 4003, Level = LogLevel.Warning, Message = "RabbitMQ connection shutdown observed. ReplyCode={ReplyCode} ReplyText={ReplyText} Initiator={Initiator}")]
        private static partial void LogConnectionShutdown(ILogger logger, ushort replyCode, string replyText, string initiator);

        /// <summary>
        /// Emits the connection callback exception log event for rabbit mq connection manager.
        /// </summary>
        [LoggerMessage(EventId = 4004, Level = LogLevel.Warning, Message = "RabbitMQ callback exception observed. Message={Message}")]
        private static partial void LogConnectionCallbackException(ILogger logger, string message);

        /// <summary>
        /// Emits the connection blocked log event for rabbit mq connection manager.
        /// </summary>
        [LoggerMessage(EventId = 4005, Level = LogLevel.Warning, Message = "RabbitMQ broker blocked the connection. Reason={Reason}")]
        private static partial void LogConnectionBlocked(ILogger logger, string reason);

        /// <summary>
        /// Emits the connection unblocked log event for rabbit mq connection manager.
        /// </summary>
        [LoggerMessage(EventId = 4006, Level = LogLevel.Information, Message = "RabbitMQ broker unblocked the connection")]
        private static partial void LogConnectionUnblocked(ILogger logger);

        /// <summary>
        /// Emits the recovery queued log event for rabbit mq connection manager.
        /// </summary>
        [LoggerMessage(EventId = 4007, Level = LogLevel.Warning, Message = "RabbitMQ recovery queued. Reason={Reason}")]
        private static partial void LogRecoveryQueued(ILogger logger, string reason);

        /// <summary>
        /// Emits the recovery starting log event for rabbit mq connection manager.
        /// </summary>
        [LoggerMessage(EventId = 4008, Level = LogLevel.Information, Message = "RabbitMQ recovery attempt starting. Attempt={Attempt} BackoffMs={BackoffMs}")]
        private static partial void LogRecoveryStarting(ILogger logger, int attempt, double backoffMs);

        /// <summary>
        /// Emits the recovery succeeded log event for rabbit mq connection manager.
        /// </summary>
        [LoggerMessage(EventId = 4009, Level = LogLevel.Information, Message = "RabbitMQ recovery attempt succeeded. Attempt={Attempt}")]
        private static partial void LogRecoverySucceeded(ILogger logger, int attempt);

        /// <summary>
        /// Emits the recovery failed log event for rabbit mq connection manager.
        /// </summary>
        [LoggerMessage(EventId = 4010, Level = LogLevel.Error, Message = "RabbitMQ recovery attempt failed. Attempt={Attempt} ConsecutiveFailures={ConsecutiveFailures} Reason={Reason}")]
        private static partial void LogRecoveryFailed(ILogger logger, int attempt, int consecutiveFailures, string reason);

        /// <summary>
        /// Emits the recovery failure threshold reached log event for rabbit mq connection manager.
        /// </summary>
        [LoggerMessage(EventId = 4011, Level = LogLevel.Error, Message = "RabbitMQ recovery failure threshold reached. ConsecutiveFailures={ConsecutiveFailures}")]
        private static partial void LogRecoveryFailureThresholdReached(ILogger logger, int consecutiveFailures);

        /// <summary>
        /// Emits the client automatic recovery error log event for rabbit mq connection manager.
        /// </summary>
        [LoggerMessage(EventId = 4012, Level = LogLevel.Warning, Message = "RabbitMQ client automatic recovery error observed. ConsecutiveErrors={ConsecutiveErrors} Reason={Reason}")]
        private static partial void LogClientAutomaticRecoveryError(ILogger logger, int consecutiveErrors, string reason);

        /// <summary>
        /// Emits the client automatic recovery threshold reached log event for rabbit mq connection manager.
        /// </summary>
        [LoggerMessage(EventId = 4013, Level = LogLevel.Warning, Message = "RabbitMQ client automatic recovery error threshold reached. ConsecutiveErrors={ConsecutiveErrors}")]
        private static partial void LogClientAutomaticRecoveryThresholdReached(ILogger logger, int consecutiveErrors);

        /// <summary>
        /// Emits the client automatic recovery succeeded log event for rabbit mq connection manager.
        /// </summary>
        [LoggerMessage(EventId = 4014, Level = LogLevel.Information, Message = "RabbitMQ client automatic recovery succeeded")]
        private static partial void LogClientAutomaticRecoverySucceeded(ILogger logger);

        /// <summary>
        /// Emits the shutdown signal observed log event for rabbit mq connection manager.
        /// </summary>
        [LoggerMessage(EventId = 4015, Level = LogLevel.Warning, Message = "RabbitMQ shutdown signal observed from ShutdownCoordinator. Type={ShutdownType}")]
        private static partial void LogShutdownSignalObserved(ILogger logger, string shutdownType);

        /// <summary>
        /// Emits the connection dispose failed log event for rabbit mq connection manager.
        /// </summary>
        [LoggerMessage(EventId = 4016, Level = LogLevel.Error, Message = "RabbitMQ connection disposal failed")]
        private static partial void LogConnectionDisposeFailed(ILogger logger, Exception exception);

        /// <summary>
        /// Emits the shutdown completed log event for rabbit mq connection manager.
        /// </summary>
        [LoggerMessage(EventId = 4017, Level = LogLevel.Information, Message = "RabbitMQ connection manager shutdown completed")]
        private static partial void LogShutdownCompleted(ILogger logger);
    }
}
