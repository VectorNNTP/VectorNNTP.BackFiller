// <copyright file="RabbitMqConsumerPhase2Tests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Tests / RabbitMQ
// Deterministic Phase 2 consumer session lifecycle tests covering topology/queue identity,
// manual-ack registration semantics, generation-based recreation, and shutdown safety.

using System.Reflection;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.RabbitMq;
using VectorNNTP.Backfiller.Runtime.Shutdown;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Focused deterministic tests for RabbitMQ Phase 2 consumer session runtime behavior.
    /// </summary>
    public sealed class RabbitMqConsumerPhase2Tests
    {
        /// <summary>
        /// Verifies consumer startup applies configured prefetch, targets the Backbone queue, and uses manual ACK semantics.
        /// </summary>
        [Fact]
        public async Task StartAsync_ConfiguresPrefetchTargetsBackboneQueueAndUsesManualAck()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 32, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            RecordingDeliverySink sink = new();

            RabbitMqBackboneConsumerSession session = new(
                CreateIdentity("Giganews", connectionNumber: 23, connectionLimit: 100),
                manager,
                topologyInitializer,
                sink,
                NullLogger<RabbitMqBackboneConsumerSession>.Instance,
                prefetchCount: 32);

            await session.StartAsync(CancellationToken.None).ConfigureAwait(false);

            TrackingChannel consumerChannel = connector.RequireLastConnection().Channels.Single(static channel => channel.ConsumeCallCount == 1);

            Assert.Equal("grabbers.giganews", consumerChannel.LastConsumeQueue);
            Assert.False(consumerChannel.LastConsumeAutoAck);
            Assert.Equal((ushort)32, consumerChannel.LastPrefetchCount);
            Assert.Equal(manager.ConnectionGeneration, session.ActiveConnectionGeneration);

            await session.StopAsync(CancellationToken.None).ConfigureAwait(false);
            await session.DisposeAsync().ConfigureAwait(false);
            await manager.DisposeAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Verifies delivery metadata is propagated to the infrastructure sink without auto-acknowledging on receipt.
        /// </summary>
        [Fact]
        public async Task Delivery_PropagatesMetadataWithoutAutomaticAck()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: null, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            RecordingDeliverySink sink = new();

            RabbitMqConsumerSessionIdentity identity = CreateIdentity("Eweka", connectionNumber: 11, connectionLimit: 50);
            RabbitMqBackboneConsumerSession session = new(
                identity,
                manager,
                topologyInitializer,
                sink,
                NullLogger<RabbitMqBackboneConsumerSession>.Instance,
                prefetchCount: null);

            await session.StartAsync(CancellationToken.None).ConfigureAwait(false);

            TrackingChannel consumerChannel = connector.RequireLastConnection().Channels.Single(static channel => channel.ConsumeCallCount == 1);
            byte[] payload = [0x11, 0x22, 0x33, 0x44];
            await consumerChannel.DeliverAsync(
                deliveryTag: 781,
                redelivered: true,
                exchange: "grabbers.eweka",
                routingKey: "grabbers.eweka",
                payload: payload,
                cancellationToken: CancellationToken.None).ConfigureAwait(false);

            RabbitMqArticleDelivery delivery = Assert.Single(sink.Deliveries);
            Assert.Equal("Eweka", delivery.Backbone);
            Assert.Equal("grabbers.eweka", delivery.Queue);
            Assert.Equal("grabbers.eweka", delivery.Exchange);
            Assert.Equal("grabbers.eweka", delivery.RoutingKey);
            Assert.Equal(781UL, delivery.DeliveryTag);
            Assert.True(delivery.Redelivered);
            Assert.Equal(identity.SessionKey, delivery.ConsumerIdentity);
            Assert.Equal(manager.ConnectionGeneration, delivery.ConnectionGeneration);
            Assert.Equal(payload, delivery.Payload.ToArray());

            Assert.False(consumerChannel.LastConsumeAutoAck);

            await session.StopAsync(CancellationToken.None).ConfigureAwait(false);
            await session.DisposeAsync().ConfigureAwait(false);
            await manager.DisposeAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Verifies each logical consumer session owns an independent channel while sharing the same Backbone queue identity.
        /// </summary>
        [Fact]
        public async Task MultipleSessions_ShareBackboneQueueIdentityButUseIndependentChannels()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 8, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            RecordingDeliverySink sink = new();

            RabbitMqBackboneConsumerSession first = new(
                CreateIdentity("Giganews", connectionNumber: 1, connectionLimit: 3),
                manager,
                topologyInitializer,
                sink,
                NullLogger<RabbitMqBackboneConsumerSession>.Instance,
                prefetchCount: 8);

            RabbitMqBackboneConsumerSession second = new(
                CreateIdentity("Giganews", connectionNumber: 2, connectionLimit: 3),
                manager,
                topologyInitializer,
                sink,
                NullLogger<RabbitMqBackboneConsumerSession>.Instance,
                prefetchCount: 8);

            await first.StartAsync(CancellationToken.None).ConfigureAwait(false);
            await second.StartAsync(CancellationToken.None).ConfigureAwait(false);

            IReadOnlyList<TrackingChannel> consumerChannels = connector.RequireLastConnection().Channels
                .Where(static channel => channel.ConsumeCallCount == 1)
                .ToArray();

            Assert.Equal(2, consumerChannels.Count);
            Assert.All(consumerChannels, static channel => Assert.Equal("grabbers.giganews", channel.LastConsumeQueue));
            Assert.NotSame(consumerChannels[0], consumerChannels[1]);

            await first.StopAsync(CancellationToken.None).ConfigureAwait(false);
            await second.StopAsync(CancellationToken.None).ConfigureAwait(false);
            await first.DisposeAsync().ConfigureAwait(false);
            await second.DisposeAsync().ConfigureAwait(false);
            await manager.DisposeAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Verifies application-level connection generation replacement retires stale consumer/channel and recreates on new generation.
        /// </summary>
        [Fact]
        public async Task ConnectionReplacement_RecreatesSessionConsumerOnNewGeneration()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 4, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            RecordingDeliverySink sink = new();

            RabbitMqBackboneConsumerSession session = new(
                CreateIdentity("Giganews", connectionNumber: 3, connectionLimit: 10),
                manager,
                topologyInitializer,
                sink,
                NullLogger<RabbitMqBackboneConsumerSession>.Instance,
                prefetchCount: 4);

            await session.StartAsync(CancellationToken.None).ConfigureAwait(false);

            TrackingConnection firstConnection = connector.RequireLastConnection();
            TrackingChannel firstConsumerChannel = firstConnection.Channels.Single(static channel => channel.ConsumeCallCount == 1);
            long firstGeneration = session.ActiveConnectionGeneration;

            firstConnection.RaiseConnectionRecoveryError(new InvalidOperationException("simulated recovery error"));

            bool replaced = await WaitForAsync(
                () => manager.ConnectionGeneration > firstGeneration && connector.ConnectCallCount >= 2,
                TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            Assert.True(replaced);

            long replacementGeneration = manager.ConnectionGeneration;
            await session.HandleConnectionReplacedAsync(new RabbitMqConnectionReplacedEventArgs(replacementGeneration, IsReplacement: true), CancellationToken.None).ConfigureAwait(false);

            TrackingConnection secondConnection = connector.RequireLastConnection();
            TrackingChannel secondConsumerChannel = secondConnection.Channels.Single(static channel => channel.ConsumeCallCount == 1);

            Assert.True(firstConsumerChannel.CancelCallCount >= 1);
            Assert.True(firstConsumerChannel.Disposed);
            Assert.True(firstConnection.Disposed);
            Assert.NotSame(firstConsumerChannel, secondConsumerChannel);
            Assert.Equal(replacementGeneration, session.ActiveConnectionGeneration);

            int activeConsumers = connector.AllConnections
                .SelectMany(static connection => connection.Channels)
                .Count(static channel => channel.IsConsumerCurrentlyActive);
            Assert.Equal(1, activeConsumers);

            await session.StopAsync(CancellationToken.None).ConfigureAwait(false);
            await session.DisposeAsync().ConfigureAwait(false);
            await manager.DisposeAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Verifies non-replacement notifications (automatic client recovery path) do not force unnecessary consumer recreation.
        /// </summary>
        [Fact]
        public async Task NonReplacementGenerationEvent_DoesNotRecreateConsumer()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 2, maxConsecutiveRecoveryFailures: 2);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);

            RabbitMqBackboneConsumerSession session = new(
                CreateIdentity("Eweka", connectionNumber: 2, connectionLimit: 10),
                manager,
                topologyInitializer,
                new RecordingDeliverySink(),
                NullLogger<RabbitMqBackboneConsumerSession>.Instance,
                prefetchCount: 2);

            await session.StartAsync(CancellationToken.None).ConfigureAwait(false);

            TrackingChannel consumerChannel = connector.RequireLastConnection().Channels.Single(static channel => channel.ConsumeCallCount == 1);
            int initialConsumeCount = consumerChannel.ConsumeCallCount;
            int initialCancelCount = consumerChannel.CancelCallCount;

            await session.HandleConnectionReplacedAsync(
                new RabbitMqConnectionReplacedEventArgs(manager.ConnectionGeneration, IsReplacement: false),
                CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(initialConsumeCount, consumerChannel.ConsumeCallCount);
            Assert.Equal(initialCancelCount, consumerChannel.CancelCallCount);
            Assert.Equal(1, connector.ConnectCallCount);

            await session.StopAsync(CancellationToken.None).ConfigureAwait(false);
            await session.DisposeAsync().ConfigureAwait(false);
            await manager.DisposeAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Verifies stopping a session prevents connection-generation callbacks from recreating a consumer after shutdown starts.
        /// </summary>
        [Fact]
        public async Task StopAsync_PreventsConsumerRecreationAfterShutdownBegins()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 2, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);

            RabbitMqBackboneConsumerSession session = new(
                CreateIdentity("Giganews", connectionNumber: 5, connectionLimit: 10),
                manager,
                topologyInitializer,
                new RecordingDeliverySink(),
                NullLogger<RabbitMqBackboneConsumerSession>.Instance,
                prefetchCount: 2);

            await session.StartAsync(CancellationToken.None).ConfigureAwait(false);
            TrackingChannel channel = connector.RequireLastConnection().Channels.Single(static c => c.ConsumeCallCount == 1);

            await session.StopAsync(CancellationToken.None).ConfigureAwait(false);
            int consumeCountAfterStop = channel.ConsumeCallCount;

            await session.HandleConnectionReplacedAsync(new RabbitMqConnectionReplacedEventArgs(manager.ConnectionGeneration + 1, IsReplacement: true), CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(consumeCountAfterStop, channel.ConsumeCallCount);
            Assert.True(channel.CancelCallCount >= 1);
            Assert.True(channel.Disposed);

            await session.DisposeAsync().ConfigureAwait(false);
            await manager.DisposeAsync().ConfigureAwait(false);
        }

        private static RabbitMqConsumerSessionIdentity CreateIdentity(string backbone, int connectionNumber, int connectionLimit)
        {
            return new RabbitMqConsumerSessionIdentity(
                Backbone: backbone,
                AccountId: Guid.NewGuid(),
                AccountUsername: backbone.ToLowerInvariant(),
                ConnectionNumber: connectionNumber,
                ConnectionLimit: connectionLimit,
                ServerId: 12,
                Host: "provider.local",
                Port: 119,
                UseSsl: false);
        }

        private static BackFillerRuntimeOptions CreateRuntimeOptions(ushort? prefetchCount, int maxConsecutiveRecoveryFailures)
        {
            RabbitMqRuntimeOptions rabbitMq = new(
                Hosts: ["localhost"],
                Port: 5672,
                Username: "nntparticles",
                Password: "top-secret",
                VirtualHost: "/",
                EnableSsl: false,
                ChannelLeaseTimeoutSeconds: 60,
                RpcTimeoutSeconds: 30,
                ConnectionBlockedTimeoutSeconds: 30,
                ChannelPoolSize: 128,
                MinConnections: 1,
                MaxConnections: 4,
                MaxConsecutiveRecoveryFailures: maxConsecutiveRecoveryFailures,
                MaxPendingLeaseWaiters: 512,
                ConnectionScaleDownIdleSeconds: 120,
                ScaleDownCooldownSeconds: 30,
                NetworkRecoveryIntervalSeconds: 5,
                PoolReconnectBaseDelayMs: 25,
                PoolReconnectMaxDelayMs: 100,
                MinimumConnectionLifetimeSeconds: 30,
                PublishConfirmTimeoutSeconds: 10,
                MaximumShutdownDrainTimeoutSeconds: 30,
                DegradedThreshold: 0.75,
                UnhealthyThreshold: 5,
                RequestedHeartbeatSeconds: 30,
                SocketTimeoutSeconds: 30,
                RequestedChannelMax: 1024,
                ConsumerPrefetchCount: prefetchCount);

            return new BackFillerRuntimeOptions(
                CanonicalBackFillerFqdn: "bf.local",
                BackFillerId: 12,
                CanonicalDnsSuffix: "local",
                ValidatedLogDirectory: Path.GetTempPath(),
                ValidatedCertificateDirectory: Path.GetTempPath(),
                RabbitMqHosts: ["localhost"],
                RabbitMqPort: 5672,
                RabbitMqEnableSsl: false,
                TransitServerHost: "localhost",
                TransitServerPort: 119,
                TransitServerUseSsl: false,
                BindPort: 1190,
                ConfiguredBindAddressTokens: ["127.0.0.1"],
                ShutdownGracePeriodSeconds: 30,
                ShutdownDrainQueuedWork: true,
                ShutdownFinishActiveArticles: true,
                RabbitMqMaximumShutdownDrainTimeoutSeconds: 30,
                WriteBatchCoalesceMicroseconds: 250,
                RabbitMq: rabbitMq);
        }

        private static async Task<bool> WaitForAsync(Func<bool> predicate, TimeSpan timeout)
        {
            DateTimeOffset deadline = DateTimeOffset.UtcNow + timeout;
            while (DateTimeOffset.UtcNow < deadline)
            {
                if (predicate())
                {
                    return true;
                }

                await Task.Delay(25).ConfigureAwait(false);
            }

            return false;
        }

        /// <summary>
        /// In-memory sink that captures deliveries forwarded by RabbitMQ consumer sessions.
        /// </summary>
        private sealed class RecordingDeliverySink : IRabbitMqDeliverySink
        {
            internal List<RabbitMqArticleDelivery> Deliveries { get; } = [];

            public ValueTask OnDeliveryAsync(RabbitMqArticleDelivery delivery, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                Deliveries.Add(delivery);
                return ValueTask.CompletedTask;
            }
        }

        /// <summary>
        /// Tracks connector calls and emits owned broker connections with deterministic test observability.
        /// </summary>
        private sealed class TrackingBrokerConnector : IRabbitMqBrokerConnector
        {
            private readonly List<TrackingConnection> _connections = [];
            private int _connectCallCount;

            internal int ConnectCallCount => Volatile.Read(ref _connectCallCount);

            internal IReadOnlyList<TrackingConnection> AllConnections => _connections;

            public Task<IRabbitMqBrokerConnection> ConnectAsync(RabbitMqRuntimeOptions runtimeOptions, string clientProvidedConnectionName, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = Interlocked.Increment(ref _connectCallCount);

                TrackingConnection connection = new(runtimeOptions.Hosts[0], runtimeOptions.Port, runtimeOptions.VirtualHost, clientProvidedConnectionName);
                _connections.Add(connection);
                return Task.FromResult<IRabbitMqBrokerConnection>(connection);
            }

            internal TrackingConnection RequireLastConnection()
            {
                return _connections.LastOrDefault() ?? throw new InvalidOperationException("Expected an active tracked connection.");
            }
        }

        /// <summary>
        /// Test broker connection implementation that tracks channels and exposes recovery/failure events.
        /// </summary>
        private sealed class TrackingConnection(string host, int port, string virtualHost, string clientProvidedName) : IRabbitMqBrokerConnection
        {
            public bool IsOpen { get; private set; } = true;

            public string EndpointHostName { get; } = host;

            public int EndpointPort { get; } = port;

            public string VirtualHost { get; } = virtualHost;

            public string ClientProvidedName { get; } = clientProvidedName;

            public IConnection UnderlyingConnection => throw new NotSupportedException();

            public List<TrackingChannel> Channels { get; } = [];

            public bool Disposed { get; private set; }

            public event EventHandler<ShutdownEventArgs>? ConnectionShutdown;

            public event EventHandler<CallbackExceptionEventArgs>? CallbackException;

            public event EventHandler<ConnectionBlockedEventArgs>? ConnectionBlocked;

            public event EventHandler<AsyncEventArgs>? ConnectionUnblocked;

            public event EventHandler<ConnectionRecoveryErrorEventArgs>? ConnectionRecoveryError;

            public event EventHandler<AsyncEventArgs>? RecoverySucceeded;

            public Task<IRabbitMqChannel> CreateChannelAsync(CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                TrackingChannel channel = new();
                Channels.Add(channel);
                return Task.FromResult<IRabbitMqChannel>(channel);
            }

            public ValueTask DisposeAsync()
            {
                IsOpen = false;
                Disposed = true;
                return ValueTask.CompletedTask;
            }

            internal void RaiseConnectionRecoveryError(Exception exception)
            {
                ConnectionRecoveryError?.Invoke(this, new ConnectionRecoveryErrorEventArgs(exception, default));
            }

            internal void RaiseRecoverySucceeded()
            {
                RecoverySucceeded?.Invoke(this, new AsyncEventArgs());
            }
        }

        /// <summary>
        /// Test channel implementation that tracks QoS/consume/cancel lifecycle and can inject deliveries to registered consumers.
        /// </summary>
        private sealed class TrackingChannel : IRabbitMqChannel
        {
            private readonly IChannel _underlyingChannel = DispatchProxy.Create<IChannel, NoOpChannelProxy>();
            private IAsyncBasicConsumer? _consumer;
            private string? _consumerTag;

            public IChannel UnderlyingChannel => _underlyingChannel;

            public bool Disposed { get; private set; }

            public int ConsumeCallCount { get; private set; }

            public int CancelCallCount { get; private set; }

            public bool LastConsumeAutoAck { get; private set; }

            public string LastConsumeQueue { get; private set; } = string.Empty;

            public ushort? LastPrefetchCount { get; private set; }

            public bool IsConsumerCurrentlyActive => ConsumeCallCount > CancelCallCount && !Disposed;

            public Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }

            public Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }

            public Task QueueBindAsync(string queue, string exchange, string routingKey, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }

            public Task BasicQosAsync(uint prefetchSize, ushort prefetchCount, bool global, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = prefetchSize;
                _ = global;
                LastPrefetchCount = prefetchCount;
                return Task.CompletedTask;
            }

            public Task<string> BasicConsumeAsync(string queue, bool autoAck, IAsyncBasicConsumer consumer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                ConsumeCallCount++;
                LastConsumeQueue = queue;
                LastConsumeAutoAck = autoAck;
                _consumer = consumer;
                _consumerTag = $"ctag-{ConsumeCallCount}";
                return Task.FromResult(_consumerTag);
            }

            public Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (string.Equals(_consumerTag, consumerTag, StringComparison.Ordinal))
                {
                    CancelCallCount++;
                    _consumerTag = null;
                }

                return Task.CompletedTask;
            }

            public async Task DeliverAsync(ulong deliveryTag, bool redelivered, string exchange, string routingKey, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_consumer is null)
                {
                    throw new InvalidOperationException("No consumer has been registered for this tracking channel.");
                }

                string consumerTag = _consumerTag ?? "ctag-0";
                await _consumer.HandleBasicDeliverAsync(
                    consumerTag,
                    deliveryTag,
                    redelivered,
                    exchange,
                    routingKey,
                    new BasicProperties(),
                    payload,
                    cancellationToken).ConfigureAwait(false);
            }

            public ValueTask DisposeAsync()
            {
                Disposed = true;
                _consumer = null;
                _consumerTag = null;
                return ValueTask.CompletedTask;
            }
        }

        /// <summary>
        /// Dynamic no-op proxy for RabbitMQ.Client.IChannel needed by AsyncEventingBasicConsumer construction in tests.
        /// </summary>
        private class NoOpChannelProxy : DispatchProxy
        {
            protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
            {
                _ = args;
                if (targetMethod is null)
                {
                    return null;
                }

                Type returnType = targetMethod.ReturnType;
                if (returnType == typeof(void))
                {
                    return null;
                }

                if (returnType == typeof(Task))
                {
                    return Task.CompletedTask;
                }

                if (returnType == typeof(ValueTask))
                {
                    return ValueTask.CompletedTask;
                }

                if (returnType.IsGenericType && returnType.GetGenericTypeDefinition() == typeof(Task<>))
                {
                    Type resultType = returnType.GenericTypeArguments[0];
                    MethodInfo fromResult = typeof(Task).GetMethod(nameof(Task.FromResult))!.MakeGenericMethod(resultType);
                    return fromResult.Invoke(null, [CreateDefaultValue(resultType)]);
                }

                if (returnType.IsGenericType && returnType.GetGenericTypeDefinition() == typeof(ValueTask<>))
                {
                    Type resultType = returnType.GenericTypeArguments[0];
                    object? value = CreateDefaultValue(resultType);
                    return Activator.CreateInstance(returnType, value);
                }

                return CreateDefaultValue(returnType);
            }

            private static object? CreateDefaultValue(Type type)
            {
                if (!type.IsValueType)
                {
                    return null;
                }

                return Activator.CreateInstance(type);
            }
        }
    }
}
