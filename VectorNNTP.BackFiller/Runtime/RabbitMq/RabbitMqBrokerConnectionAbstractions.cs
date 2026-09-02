// <copyright file="RabbitMqBrokerConnectionAbstractions.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / RabbitMq
// Implements the rabbit mq broker connection abstractions behavior.

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
        /// <typeparam name="IRabbitMqBrokerConnection">The IRabbitMqBrokerConnection type parameter.</typeparam>
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
        /// Returns the broker virtual host used for this connection.
        /// </summary>
        public string VirtualHost { get; }

        /// <summary>
        /// Returns the configured client-provided connection name.
        /// </summary>
        public string ClientProvidedName { get; }

        /// <summary>
        /// Returns the underlying RabbitMQ connection instance.
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
        /// <param name="enablePublisherConfirmations">Whether publisher confirmation mode should be enabled for this channel.</param>
        /// <returns>New RabbitMQ channel adapter.</returns>
        /// <typeparam name="IRabbitMqChannel">The IRabbitMqChannel type parameter.</typeparam>
        public Task<IRabbitMqChannel> CreateChannelAsync(CancellationToken cancellationToken, bool enablePublisherConfirmations = false);
    }

    /// <summary>
    /// Minimal RabbitMQ channel abstraction for topology and ownership isolation.
    /// </summary>
    internal interface IRabbitMqChannel : IAsyncDisposable
    {
        /// <summary>
        /// Returns the underlying RabbitMQ channel.
        /// </summary>
        public IChannel UnderlyingChannel { get; }

        /// <summary>
        /// Declares an exchange.
        /// </summary>
        /// <param name="exchange">The exchange value.</param>
        /// <param name="type">The type value.</param>
        /// <param name="durable">The durable value.</param>
        /// <param name="autoDelete">The autoDelete value.</param>
        /// <param name="arguments">The arguments value.</param>
        /// <param name="cancellationToken">The cancellationToken value.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <typeparam name="string">The string type parameter.</typeparam>
        public Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken);

        /// <summary>
        /// Declares a queue.
        /// </summary>
        /// <param name="queue">The queue value.</param>
        /// <param name="durable">The durable value.</param>
        /// <param name="exclusive">The exclusive value.</param>
        /// <param name="autoDelete">The autoDelete value.</param>
        /// <param name="arguments">The arguments value.</param>
        /// <param name="cancellationToken">The cancellationToken value.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <typeparam name="string">The string type parameter.</typeparam>
        public Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken);

        /// <summary>
        /// Declares a queue binding.
        /// </summary>
        /// <param name="queue">The queue value.</param>
        /// <param name="exchange">The exchange value.</param>
        /// <param name="routingKey">The routingKey value.</param>
        /// <param name="arguments">The arguments value.</param>
        /// <param name="cancellationToken">The cancellationToken value.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <typeparam name="string">The string type parameter.</typeparam>
        public Task QueueBindAsync(string queue, string exchange, string routingKey, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken);

        /// <summary>
        /// Configures channel QoS for consumer prefetch control.
        /// </summary>
        /// <param name="prefetchSize">The prefetchSize value.</param>
        /// <param name="prefetchCount">The prefetchCount value.</param>
        /// <param name="global">The global value.</param>
        /// <param name="cancellationToken">The cancellationToken value.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public Task BasicQosAsync(uint prefetchSize, ushort prefetchCount, bool global, CancellationToken cancellationToken);

        /// <summary>
        /// Registers an asynchronous consumer on a queue.
        /// </summary>
        /// <param name="queue">The queue value.</param>
        /// <param name="autoAck">The autoAck value.</param>
        /// <param name="consumer">The consumer value.</param>
        /// <param name="cancellationToken">The cancellationToken value.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public Task<string> BasicConsumeAsync(string queue, bool autoAck, IAsyncBasicConsumer consumer, CancellationToken cancellationToken);

        /// <summary>
        /// Cancels a consumer by broker-assigned tag.
        /// </summary>
        /// <param name="consumerTag">The consumerTag value.</param>
        /// <param name="cancellationToken">The cancellationToken value.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken);

        /// <summary>
        /// Acknowledges a delivery tag.
        /// </summary>
        /// <param name="deliveryTag">The deliveryTag value.</param>
        /// <param name="multiple">The multiple value.</param>
        /// <param name="cancellationToken">The cancellationToken value.</param>
        /// <returns>A value task representing the asynchronous operation.</returns>
        public ValueTask BasicAckAsync(ulong deliveryTag, bool multiple, CancellationToken cancellationToken);

        /// <summary>
        /// Negatively acknowledges a delivery tag.
        /// </summary>
        /// <param name="deliveryTag">The deliveryTag value.</param>
        /// <param name="multiple">The multiple value.</param>
        /// <param name="requeue">The requeue value.</param>
        /// <param name="cancellationToken">The cancellationToken value.</param>
        /// <returns>A value task representing the asynchronous operation.</returns>
        public ValueTask BasicNackAsync(ulong deliveryTag, bool multiple, bool requeue, CancellationToken cancellationToken);

        /// <summary>
        /// Publishes one message payload.
        /// </summary>
        /// <param name="exchange">The exchange value.</param>
        /// <param name="routingKey">The routingKey value.</param>
        /// <param name="mandatory">The mandatory value.</param>
        /// <param name="basicProperties">The basicProperties value.</param>
        /// <param name="body">The body value.</param>
        /// <param name="cancellationToken">The cancellationToken value.</param>
        /// <returns>A value task representing the asynchronous operation.</returns>
        public ValueTask BasicPublishAsync(string exchange, string routingKey, bool mandatory, BasicProperties basicProperties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken);
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
        /// <summary>
        /// Stores connection used by rabbit mq broker connection abstractions.
        /// </summary>
        private readonly IConnection _connection;
        /// <summary>
        /// Stores virtual host used by rabbit mq broker connection abstractions.
        /// </summary>
        private readonly string _virtualHost;
        /// <summary>
        /// Stores connection shutdown async handler used by rabbit mq broker connection abstractions.
        /// </summary>
        private readonly AsyncEventHandler<ShutdownEventArgs> _connectionShutdownAsyncHandler;
        /// <summary>
        /// Stores callback exception async handler used by rabbit mq broker connection abstractions.
        /// </summary>
        private readonly AsyncEventHandler<CallbackExceptionEventArgs> _callbackExceptionAsyncHandler;
        /// <summary>
        /// Stores connection blocked async handler used by rabbit mq broker connection abstractions.
        /// </summary>
        private readonly AsyncEventHandler<ConnectionBlockedEventArgs> _connectionBlockedAsyncHandler;
        /// <summary>
        /// Stores connection unblocked async handler used by rabbit mq broker connection abstractions.
        /// </summary>
        private readonly AsyncEventHandler<AsyncEventArgs> _connectionUnblockedAsyncHandler;
        /// <summary>
        /// Stores connection recovery error async handler used by rabbit mq broker connection abstractions.
        /// </summary>
        private readonly AsyncEventHandler<ConnectionRecoveryErrorEventArgs> _connectionRecoveryErrorAsyncHandler;
        /// <summary>
        /// Stores recovery succeeded async handler used by rabbit mq broker connection abstractions.
        /// </summary>
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
        public async Task<IRabbitMqChannel> CreateChannelAsync(CancellationToken cancellationToken, bool enablePublisherConfirmations = false)
        {
            CreateChannelOptions channelOptions = new(
                publisherConfirmationsEnabled: enablePublisherConfirmations,
                publisherConfirmationTrackingEnabled: enablePublisherConfirmations,
                outstandingPublisherConfirmationsRateLimiter: null,
                consumerDispatchConcurrency: null);

            IChannel channel = await _connection.CreateChannelAsync(options: channelOptions, cancellationToken: cancellationToken).ConfigureAwait(false);
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
        /// <summary>
        /// Stores channel used by rabbit mq broker connection abstractions.
        /// </summary>
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
            return _channel.BasicCancelAsync(consumerTag, cancellationToken: cancellationToken);
        }

        /// <inheritdoc/>
        public ValueTask BasicAckAsync(ulong deliveryTag, bool multiple, CancellationToken cancellationToken)
        {
            return _channel.BasicAckAsync(deliveryTag, multiple, cancellationToken: cancellationToken);
        }

        /// <inheritdoc/>
        public ValueTask BasicNackAsync(ulong deliveryTag, bool multiple, bool requeue, CancellationToken cancellationToken)
        {
            return _channel.BasicNackAsync(deliveryTag, multiple, requeue, cancellationToken: cancellationToken);
        }

        /// <inheritdoc/>
        public ValueTask BasicPublishAsync(string exchange, string routingKey, bool mandatory, BasicProperties basicProperties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(basicProperties);
            return _channel.BasicPublishAsync(exchange, routingKey, mandatory, basicProperties, body, cancellationToken);
        }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            return _channel.DisposeAsync();
        }

        /// <summary>
        /// Adapts the immutable broker-connection contract to the mutable RabbitMQ connection implementation.
        /// </summary>
        private static IDictionary<string, object?>? ToMutable(IReadOnlyDictionary<string, object?>? arguments)
        {
            return arguments is null ? null : (IDictionary<string, object?>)arguments.ToDictionary(static kvp => kvp.Key, static kvp => kvp.Value, StringComparer.Ordinal);
        }
    }
}
