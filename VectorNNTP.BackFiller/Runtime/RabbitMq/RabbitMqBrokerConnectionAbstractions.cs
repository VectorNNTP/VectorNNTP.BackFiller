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
        /// Opens a broker connection using the validated runtime snapshot and client-provided connection name.
        /// </summary>
        /// <param name="runtimeOptions">Validated immutable RabbitMQ runtime options.</param>
        /// <param name="clientProvidedConnectionName">Connection name exposed to the broker for diagnostics.</param>
        /// <param name="cancellationToken">Cancellation token for the connect attempt.</param>
        /// <returns>An owned broker connection abstraction.</returns>
        public Task<IRabbitMqBrokerConnection> ConnectAsync(
            RabbitMqRuntimeOptions runtimeOptions,
            string clientProvidedConnectionName,
            CancellationToken cancellationToken);
    }

    /// <summary>
    /// RabbitMQ broker connection abstraction used to isolate lifecycle ownership and testing seams.
    /// </summary>
    /// <remarks>
    /// The abstraction exposes only the connection members required by the backfiller so production code can manage
    /// connection replacement, channel ownership, and event wiring without depending directly on RabbitMQ.Client types.
    /// </remarks>
    internal interface IRabbitMqBrokerConnection : IAsyncDisposable
    {
        /// <summary>
        /// Gets a value indicating whether the underlying broker connection is currently open.
        /// </summary>
        public bool IsOpen { get; }

        /// <summary>
        /// Gets the broker endpoint host name selected for the current connection.
        /// </summary>
        public string EndpointHostName { get; }

        /// <summary>
        /// Gets the broker endpoint port selected for the current connection.
        /// </summary>
        public int EndpointPort { get; }

        /// <summary>
        /// Gets the virtual host used to establish the current connection.
        /// </summary>
        public string VirtualHost { get; }

        /// <summary>
        /// Gets the client-provided connection name visible in broker diagnostics.
        /// </summary>
        public string ClientProvidedName { get; }

        /// <summary>
        /// Gets the underlying RabbitMQ.Client connection instance.
        /// </summary>
        public IConnection UnderlyingConnection { get; }

        /// <summary>
        /// Raised when the broker shuts the connection down.
        /// </summary>
        public event EventHandler<ShutdownEventArgs>? ConnectionShutdown;

        /// <summary>
        /// Raised when RabbitMQ.Client surfaces an asynchronous callback exception.
        /// </summary>
        public event EventHandler<CallbackExceptionEventArgs>? CallbackException;

        /// <summary>
        /// Raised when the broker blocks publishing or consumption on the connection.
        /// </summary>
        public event EventHandler<ConnectionBlockedEventArgs>? ConnectionBlocked;

        /// <summary>
        /// Raised when the broker lifts a prior connection block.
        /// </summary>
        public event EventHandler<AsyncEventArgs>? ConnectionUnblocked;

        /// <summary>
        /// Raised when RabbitMQ.Client automatic recovery reports a failure.
        /// </summary>
        public event EventHandler<ConnectionRecoveryErrorEventArgs>? ConnectionRecoveryError;

        /// <summary>
        /// Raised when RabbitMQ.Client automatic recovery reports success.
        /// </summary>
        public event EventHandler<AsyncEventArgs>? RecoverySucceeded;

        /// <summary>
        /// Creates an independently owned channel on this connection.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token for channel creation.</param>
        /// <param name="enablePublisherConfirmations"><see langword="true"/> to enable publisher confirmations for the new channel.</param>
        /// <returns>A new channel abstraction owned by the caller.</returns>
        public Task<IRabbitMqChannel> CreateChannelAsync(CancellationToken cancellationToken, bool enablePublisherConfirmations = false);
    }

    /// <summary>
    /// Minimal RabbitMQ channel abstraction for topology and ownership isolation.
    /// </summary>
    internal interface IRabbitMqChannel : IAsyncDisposable
    {
        /// <summary>
        /// Gets the underlying RabbitMQ.Client channel instance.
        /// </summary>
        public IChannel UnderlyingChannel { get; }

        /// <summary>
        /// Declares an exchange on the channel.
        /// </summary>
        /// <param name="exchange">Exchange name to declare.</param>
        /// <param name="type">Exchange type, such as <c>fanout</c>.</param>
        /// <param name="durable"><see langword="true"/> to keep the exchange durable.</param>
        /// <param name="autoDelete"><see langword="true"/> to auto-delete the exchange when unused.</param>
        /// <param name="arguments">Optional broker-specific declaration arguments.</param>
        /// <param name="cancellationToken">Cancellation token for the declaration.</param>
        /// <returns>A task that completes when the declaration succeeds.</returns>
        public Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken);

        /// <summary>
        /// Declares a queue on the channel.
        /// </summary>
        /// <param name="queue">Queue name to declare.</param>
        /// <param name="durable"><see langword="true"/> to keep the queue durable.</param>
        /// <param name="exclusive"><see langword="true"/> to make the queue exclusive to the connection.</param>
        /// <param name="autoDelete"><see langword="true"/> to auto-delete the queue when unused.</param>
        /// <param name="arguments">Optional broker-specific declaration arguments.</param>
        /// <param name="cancellationToken">Cancellation token for the declaration.</param>
        /// <returns>A task that completes when the declaration succeeds.</returns>
        public Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken);

        /// <summary>
        /// Declares a queue binding on the channel.
        /// </summary>
        /// <param name="queue">Queue name to bind.</param>
        /// <param name="exchange">Exchange providing the messages.</param>
        /// <param name="routingKey">Routing key used for the binding.</param>
        /// <param name="arguments">Optional broker-specific binding arguments.</param>
        /// <param name="cancellationToken">Cancellation token for the bind operation.</param>
        /// <returns>A task that completes when the binding succeeds.</returns>
        public Task QueueBindAsync(string queue, string exchange, string routingKey, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken);

        /// <summary>
        /// Configures consumer QoS on the channel.
        /// </summary>
        /// <param name="prefetchSize">Prefetch size requested from the broker.</param>
        /// <param name="prefetchCount">Prefetch count requested from the broker.</param>
        /// <param name="global"><see langword="true"/> to apply the limit to the whole channel instead of one consumer.</param>
        /// <param name="cancellationToken">Cancellation token for the QoS change.</param>
        /// <returns>A task that completes when QoS is applied.</returns>
        public Task BasicQosAsync(uint prefetchSize, ushort prefetchCount, bool global, CancellationToken cancellationToken);

        /// <summary>
        /// Registers an asynchronous consumer on a queue.
        /// </summary>
        /// <param name="queue">Queue to consume from.</param>
        /// <param name="autoAck"><see langword="true"/> to enable broker auto-acknowledgement.</param>
        /// <param name="consumer">Consumer callback target.</param>
        /// <param name="cancellationToken">Cancellation token for broker registration.</param>
        /// <returns>The broker-assigned consumer tag.</returns>
        public Task<string> BasicConsumeAsync(string queue, bool autoAck, IAsyncBasicConsumer consumer, CancellationToken cancellationToken);

        /// <summary>
        /// Cancels a previously registered consumer.
        /// </summary>
        /// <param name="consumerTag">Broker-assigned consumer tag.</param>
        /// <param name="cancellationToken">Cancellation token for broker cancellation.</param>
        /// <returns>A task that completes when broker cancellation is issued.</returns>
        public Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken);

        /// <summary>
        /// Positively acknowledges one delivery.
        /// </summary>
        /// <param name="deliveryTag">Broker delivery tag to acknowledge.</param>
        /// <param name="multiple"><see langword="true"/> to acknowledge all deliveries up to the tag.</param>
        /// <param name="cancellationToken">Cancellation token for the acknowledgement.</param>
        /// <returns>A value task that completes when the acknowledgement is sent.</returns>
        public ValueTask BasicAckAsync(ulong deliveryTag, bool multiple, CancellationToken cancellationToken);

        /// <summary>
        /// Negatively acknowledges one delivery.
        /// </summary>
        /// <param name="deliveryTag">Broker delivery tag to reject.</param>
        /// <param name="multiple"><see langword="true"/> to reject all deliveries up to the tag.</param>
        /// <param name="requeue"><see langword="true"/> to request broker requeue of the delivery.</param>
        /// <param name="cancellationToken">Cancellation token for the rejection.</param>
        /// <returns>A value task that completes when the negative acknowledgement is sent.</returns>
        public ValueTask BasicNackAsync(ulong deliveryTag, bool multiple, bool requeue, CancellationToken cancellationToken);

        /// <summary>
        /// Publishes one message payload.
        /// </summary>
        /// <param name="exchange">Exchange to publish to.</param>
        /// <param name="routingKey">Routing key supplied with the publish.</param>
        /// <param name="mandatory"><see langword="true"/> to require a routable destination.</param>
        /// <param name="basicProperties">Basic properties attached to the published message.</param>
        /// <param name="body">Published payload bytes.</param>
        /// <param name="cancellationToken">Cancellation token for the publish.</param>
        /// <returns>A value task that completes when the client has issued the publish call.</returns>
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
            return await CreateOwnedConnectionAsync(connection, runtimeOptions.VirtualHost).ConfigureAwait(false);
        }

        /// <summary>
        /// Transfers ownership of an opened RabbitMQ connection into the adapter boundary.
        /// </summary>
        /// <param name="connection">Opened broker connection about to be owned by the adapter.</param>
        /// <param name="virtualHost">Configured virtual host used to establish the connection.</param>
        /// <returns>The owned broker connection abstraction.</returns>
        internal static async Task<IRabbitMqBrokerConnection> CreateOwnedConnectionAsync(IConnection connection, string virtualHost)
        {
            ArgumentNullException.ThrowIfNull(connection);
            ArgumentException.ThrowIfNullOrWhiteSpace(virtualHost);

            try
            {
                return new RabbitMqBrokerConnectionAdapter(connection, virtualHost);
            }
            catch
            {
                await connection.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
    }

    /// <summary>
    /// Adapts RabbitMQ.Client <see cref="IConnection"/> to <see cref="IRabbitMqBrokerConnection"/>.
    /// </summary>
    internal sealed class RabbitMqBrokerConnectionAdapter : IRabbitMqBrokerConnection
    {
        /// <summary>
        /// Underlying live broker connection owned by the adapter.
        /// </summary>
        private readonly IConnection _connection;

        /// <summary>
        /// Validated virtual host associated with the connection.
        /// </summary>
        private readonly string _virtualHost;

        /// <summary>
        /// Validated client-provided connection name preserved as a non-null adapter boundary invariant.
        /// </summary>
        private readonly string _clientProvidedName;

        /// <summary>
        /// Forwarder attached to the client's asynchronous shutdown event.
        /// </summary>
        private readonly AsyncEventHandler<ShutdownEventArgs> _connectionShutdownAsyncHandler;

        /// <summary>
        /// Forwarder attached to the client's asynchronous callback-exception event.
        /// </summary>
        private readonly AsyncEventHandler<CallbackExceptionEventArgs> _callbackExceptionAsyncHandler;

        /// <summary>
        /// Forwarder attached to the client's asynchronous connection-blocked event.
        /// </summary>
        private readonly AsyncEventHandler<ConnectionBlockedEventArgs> _connectionBlockedAsyncHandler;

        /// <summary>
        /// Forwarder attached to the client's asynchronous connection-unblocked event.
        /// </summary>
        private readonly AsyncEventHandler<AsyncEventArgs> _connectionUnblockedAsyncHandler;

        /// <summary>
        /// Forwarder attached to the client's automatic-recovery-error event.
        /// </summary>
        private readonly AsyncEventHandler<ConnectionRecoveryErrorEventArgs> _connectionRecoveryErrorAsyncHandler;

        /// <summary>
        /// Forwarder attached to the client's recovery-succeeded event.
        /// </summary>
        private readonly AsyncEventHandler<AsyncEventArgs> _recoverySucceededAsyncHandler;

        /// <summary>
        /// Initializes a new adapter for a live RabbitMQ connection.
        /// </summary>
        /// <param name="connection">Connected RabbitMQ connection owned by the adapter.</param>
        /// <param name="virtualHost">Configured virtual host used to establish the connection.</param>
        internal RabbitMqBrokerConnectionAdapter(IConnection connection, string virtualHost)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _virtualHost = !string.IsNullOrWhiteSpace(virtualHost)
                ? virtualHost
                : throw new ArgumentException("Virtual host is required.", nameof(virtualHost));
            _clientProvidedName = !string.IsNullOrWhiteSpace(_connection.ClientProvidedName)
                ? _connection.ClientProvidedName
                : throw new InvalidOperationException("RabbitMQ connection invariant violated: IConnection.ClientProvidedName must be non-null and non-whitespace at RabbitMqBrokerConnectionAdapter boundary.");

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
        public string ClientProvidedName => _clientProvidedName;

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
    /// Adapts RabbitMQ.Client <see cref="IChannel"/> to <see cref="IRabbitMqChannel"/>.
    /// </summary>
    internal sealed class RabbitMqChannelAdapter : IRabbitMqChannel
    {
        /// <summary>
        /// Underlying channel owned by the adapter.
        /// </summary>
        private readonly IChannel _channel;

        /// <summary>
        /// Initializes a new channel adapter.
        /// </summary>
        /// <param name="channel">Underlying RabbitMQ channel owned by the adapter.</param>
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
        /// Copies immutable argument dictionaries into the mutable shape expected by RabbitMQ.Client.
        /// </summary>
        /// <param name="arguments">Immutable declaration arguments.</param>
        /// <returns>A mutable dictionary copy, or <see langword="null"/> when no arguments were supplied.</returns>
        private static Dictionary<string, object?>? ToMutable(IReadOnlyDictionary<string, object?>? arguments)
        {
            return arguments is null ? null : arguments.ToDictionary(static kvp => kvp.Key, static kvp => kvp.Value, StringComparer.Ordinal);
        }
    }
}
