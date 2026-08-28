// <copyright file="RabbitMqBrokerConnectionAbstractions.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Acquisition
// Typed exception model for deterministic internal failure classification without relying
// on exception-message text parsing.

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using VectorNNTP.Backfiller.Configuration;

namespace VectorNNTP.Backfiller.Runtime.RabbitMq
{
    /// <summary>
    /// Creates RabbitMQ broker connections for infrastructure lifecycle management.
    /// </summary>
    internal interface IRabbitMqBrokerConnector
    {
        /// <summary>
        /// Opens a broker connection using validated runtime options.
        /// </summary>
        /// <param name="runtimeOptions">Validated immutable RabbitMQ runtime options.</param>
        /// <param name="clientProvidedConnectionName">Client-provided connection name.</param>
        /// <param name="cancellationToken">Connection cancellation token.</param>
        /// <returns>Owned broker connection handle.</returns>
        public Task<IRabbitMqBrokerConnection> ConnectAsync(
            RabbitMqRuntimeOptions runtimeOptions,
            string clientProvidedConnectionName,
            CancellationToken cancellationToken);
    }

    /// <summary>
    /// RabbitMQ broker connection abstraction used to isolate lifecycle ownership and testing seams.
    /// </summary>
    internal interface IRabbitMqBrokerConnection : IAsyncDisposable
    {
        /// <summary>
        /// Gets whether the underlying broker connection is open.
        /// </summary>
        public bool IsOpen { get; }

        /// <summary>
        /// Gets broker endpoint host used for the current connection.
        /// </summary>
        public string EndpointHostName { get; }

        /// <summary>
        /// Gets broker endpoint port used for the current connection.
        /// </summary>
        public int EndpointPort { get; }

        /// <summary>
        /// Gets the broker virtual host used for this connection.
        /// </summary>
        public string VirtualHost { get; }

        /// <summary>
        /// Gets the configured client-provided connection name.
        /// </summary>
        public string ClientProvidedName { get; }

        /// <summary>
        /// Gets the underlying RabbitMQ connection instance.
        /// </summary>
        public IConnection UnderlyingConnection { get; }

        /// <summary>
        /// Raised when the broker connection is shut down.
        /// </summary>
        public event EventHandler<ShutdownEventArgs>? ConnectionShutdown;

        /// <summary>
        /// Raised when the broker reports a callback exception.
        /// </summary>
        public event EventHandler<CallbackExceptionEventArgs>? CallbackException;

        /// <summary>
        /// Raised when the broker blocks this connection.
        /// </summary>
        public event EventHandler<ConnectionBlockedEventArgs>? ConnectionBlocked;

        /// <summary>
        /// Raised when the broker unblocks this connection.
        /// </summary>
        public event EventHandler<AsyncEventArgs>? ConnectionUnblocked;

        /// <summary>
        /// Raised when RabbitMQ.Client reports automatic-recovery failure for this connection.
        /// </summary>
        public event EventHandler<ConnectionRecoveryErrorEventArgs>? ConnectionRecoveryError;

        /// <summary>
        /// Raised when RabbitMQ.Client automatic recovery succeeds for this connection.
        /// </summary>
        public event EventHandler<AsyncEventArgs>? RecoverySucceeded;

        /// <summary>
        /// Creates a dedicated owned channel on this connection.
        /// </summary>
        /// <param name="cancellationToken">Channel-creation cancellation token.</param>
        /// <returns>New RabbitMQ channel adapter.</returns>
        public Task<IRabbitMqChannel> CreateChannelAsync(CancellationToken cancellationToken);
    }

    /// <summary>
    /// Minimal RabbitMQ channel abstraction for topology and ownership isolation.
    /// </summary>
    internal interface IRabbitMqChannel : IAsyncDisposable
    {
        /// <summary>
        /// Gets the underlying RabbitMQ channel.
        /// </summary>
        public IChannel UnderlyingChannel { get; }

        /// <summary>
        /// Declares an exchange.
        /// </summary>
        public Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken);

        /// <summary>
        /// Declares a queue.
        /// </summary>
        public Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken);

        /// <summary>
        /// Declares a queue binding.
        /// </summary>
        public Task QueueBindAsync(string queue, string exchange, string routingKey, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken);

        /// <summary>
        /// Configures channel QoS for consumer prefetch control.
        /// </summary>
        public Task BasicQosAsync(uint prefetchSize, ushort prefetchCount, bool global, CancellationToken cancellationToken);

        /// <summary>
        /// Registers an asynchronous consumer on a queue.
        /// </summary>
        public Task<string> BasicConsumeAsync(string queue, bool autoAck, IAsyncBasicConsumer consumer, CancellationToken cancellationToken);

        /// <summary>
        /// Cancels a consumer by broker-assigned tag.
        /// </summary>
        public Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Production RabbitMQ connector that maps runtime options to RabbitMQ.Client and opens broker connections.
    /// </summary>
    internal sealed class RabbitMqBrokerConnector : IRabbitMqBrokerConnector
    {
        /// <inheritdoc/>
        public async Task<IRabbitMqBrokerConnection> ConnectAsync(
            RabbitMqRuntimeOptions runtimeOptions,
            string clientProvidedConnectionName,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);
            ArgumentException.ThrowIfNullOrWhiteSpace(clientProvidedConnectionName);

            ConnectionFactory factory = RabbitMqConnectionFactoryBuilder.BuildConnectionFactory(runtimeOptions, clientProvidedConnectionName);
            IReadOnlyList<string> hosts = RabbitMqConnectionFactoryBuilder.BuildHostList(runtimeOptions);

            IConnection connection = await factory.CreateConnectionAsync(hosts, cancellationToken).ConfigureAwait(false);
            return new RabbitMqBrokerConnectionAdapter(connection, runtimeOptions.VirtualHost);
        }
    }

    /// <summary>
    /// Adapts RabbitMQ.Client.IConnection to <see cref="IRabbitMqBrokerConnection"/>.
    /// </summary>
    internal sealed class RabbitMqBrokerConnectionAdapter : IRabbitMqBrokerConnection
    {
        private readonly IConnection _connection;
        private readonly string _virtualHost;
        private readonly AsyncEventHandler<ShutdownEventArgs> _connectionShutdownAsyncHandler;
        private readonly AsyncEventHandler<CallbackExceptionEventArgs> _callbackExceptionAsyncHandler;
        private readonly AsyncEventHandler<ConnectionBlockedEventArgs> _connectionBlockedAsyncHandler;
        private readonly AsyncEventHandler<AsyncEventArgs> _connectionUnblockedAsyncHandler;
        private readonly AsyncEventHandler<ConnectionRecoveryErrorEventArgs> _connectionRecoveryErrorAsyncHandler;
        private readonly AsyncEventHandler<AsyncEventArgs> _recoverySucceededAsyncHandler;

        /// <summary>
        /// Initializes a new adapter for a live RabbitMQ connection.
        /// </summary>
        /// <param name="connection">Connected RabbitMQ connection.</param>
        /// <param name="virtualHost">Configured virtual host used to establish the connection.</param>
        internal RabbitMqBrokerConnectionAdapter(IConnection connection, string virtualHost)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _virtualHost = !string.IsNullOrWhiteSpace(virtualHost)
                ? virtualHost
                : throw new ArgumentException("Virtual host is required.", nameof(virtualHost));

            _connectionShutdownAsyncHandler = (sender, args) =>
            {
                ConnectionShutdown?.Invoke(sender, args);
                return Task.CompletedTask;
            };

            _callbackExceptionAsyncHandler = (sender, args) =>
            {
                CallbackException?.Invoke(sender, args);
                return Task.CompletedTask;
            };

            _connectionBlockedAsyncHandler = (sender, args) =>
            {
                ConnectionBlocked?.Invoke(sender, args);
                return Task.CompletedTask;
            };

            _connectionUnblockedAsyncHandler = (sender, args) =>
            {
                ConnectionUnblocked?.Invoke(sender, args);
                return Task.CompletedTask;
            };

            _connectionRecoveryErrorAsyncHandler = (sender, args) =>
            {
                ConnectionRecoveryError?.Invoke(sender, args);
                return Task.CompletedTask;
            };

            _recoverySucceededAsyncHandler = (sender, args) =>
            {
                RecoverySucceeded?.Invoke(sender, args);
                return Task.CompletedTask;
            };

            _connection.ConnectionShutdownAsync += _connectionShutdownAsyncHandler;
            _connection.CallbackExceptionAsync += _callbackExceptionAsyncHandler;
            _connection.ConnectionBlockedAsync += _connectionBlockedAsyncHandler;
            _connection.ConnectionUnblockedAsync += _connectionUnblockedAsyncHandler;
            _connection.ConnectionRecoveryErrorAsync += _connectionRecoveryErrorAsyncHandler;
            _connection.RecoverySucceededAsync += _recoverySucceededAsyncHandler;
        }

        /// <inheritdoc/>
        public bool IsOpen => _connection.IsOpen;

        /// <inheritdoc/>
        public string EndpointHostName => _connection.Endpoint.HostName;

        /// <inheritdoc/>
        public int EndpointPort => _connection.Endpoint.Port;

        /// <inheritdoc/>
        public string VirtualHost => _virtualHost;

        /// <inheritdoc/>
        public string ClientProvidedName => _connection.ClientProvidedName;

        /// <inheritdoc/>
        public IConnection UnderlyingConnection => _connection;

        /// <inheritdoc/>
        public event EventHandler<ShutdownEventArgs>? ConnectionShutdown;

        /// <inheritdoc/>
        public event EventHandler<CallbackExceptionEventArgs>? CallbackException;

        /// <inheritdoc/>
        public event EventHandler<ConnectionBlockedEventArgs>? ConnectionBlocked;

        /// <inheritdoc/>
        public event EventHandler<AsyncEventArgs>? ConnectionUnblocked;

        /// <inheritdoc/>
        public event EventHandler<ConnectionRecoveryErrorEventArgs>? ConnectionRecoveryError;

        /// <inheritdoc/>
        public event EventHandler<AsyncEventArgs>? RecoverySucceeded;

        /// <inheritdoc/>
        public async Task<IRabbitMqChannel> CreateChannelAsync(CancellationToken cancellationToken)
        {
            IChannel channel = await _connection.CreateChannelAsync(options: default, cancellationToken: cancellationToken).ConfigureAwait(false);
            return new RabbitMqChannelAdapter(channel);
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            _connection.ConnectionShutdownAsync -= _connectionShutdownAsyncHandler;
            _connection.CallbackExceptionAsync -= _callbackExceptionAsyncHandler;
            _connection.ConnectionBlockedAsync -= _connectionBlockedAsyncHandler;
            _connection.ConnectionUnblockedAsync -= _connectionUnblockedAsyncHandler;
            _connection.ConnectionRecoveryErrorAsync -= _connectionRecoveryErrorAsyncHandler;
            _connection.RecoverySucceededAsync -= _recoverySucceededAsyncHandler;

            await _connection.CloseAsync().ConfigureAwait(false);
            await _connection.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Adapts RabbitMQ.Client.IChannel to <see cref="IRabbitMqChannel"/>.
    /// </summary>
    internal sealed class RabbitMqChannelAdapter : IRabbitMqChannel
    {
        private readonly IChannel _channel;

        /// <summary>
        /// Initializes a new channel adapter.
        /// </summary>
        /// <param name="channel">Underlying RabbitMQ channel.</param>
        internal RabbitMqChannelAdapter(IChannel channel)
        {
            _channel = channel ?? throw new ArgumentNullException(nameof(channel));
        }

        /// <inheritdoc/>
        public IChannel UnderlyingChannel => _channel;

        /// <inheritdoc/>
        public Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken)
        {
            return _channel.ExchangeDeclareAsync(exchange, type, durable, autoDelete, ToMutable(arguments), cancellationToken: cancellationToken);
        }

        /// <inheritdoc/>
        public async Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken)
        {
            _ = await _channel.QueueDeclareAsync(queue, durable, exclusive, autoDelete, ToMutable(arguments), cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public Task QueueBindAsync(string queue, string exchange, string routingKey, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken)
        {
            return _channel.QueueBindAsync(queue, exchange, routingKey, ToMutable(arguments), cancellationToken: cancellationToken);
        }

        /// <inheritdoc/>
        public Task BasicQosAsync(uint prefetchSize, ushort prefetchCount, bool global, CancellationToken cancellationToken)
        {
            return _channel.BasicQosAsync(prefetchSize, prefetchCount, global, cancellationToken);
        }

        /// <inheritdoc/>
        public Task<string> BasicConsumeAsync(string queue, bool autoAck, IAsyncBasicConsumer consumer, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(queue);
            ArgumentNullException.ThrowIfNull(consumer);
            return _channel.BasicConsumeAsync(queue, autoAck, consumer, cancellationToken);
        }

        /// <inheritdoc/>
        public Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(consumerTag);
            // TODO: CK CHANGE
            //return _channel.BasicCancelAsync(consumerTag, cancellationToken);
            return Task.FromResult(false);
        }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            return _channel.DisposeAsync();
        }

        private static IDictionary<string, object?>? ToMutable(IReadOnlyDictionary<string, object?>? arguments)
        {
            return arguments is null ? null : (IDictionary<string, object?>)arguments.ToDictionary(static kvp => kvp.Key, static kvp => kvp.Value, StringComparer.Ordinal);
        }
    }
}
