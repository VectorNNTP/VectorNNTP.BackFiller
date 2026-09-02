// <copyright file="RabbitMqInfrastructureTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Behavior and contract tests for rabbit mq infrastructure.

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
    /// Unit tests for RabbitMQ Phase 1 infrastructure behaviors.
    /// </summary>
    public sealed class RabbitMqInfrastructureTests
    {
        /// <summary>
        /// Verifies the BuildConnectionFactory_MapsConfiguredRuntimeSettings scenario and expected contract.
        /// </summary>
        [Fact]
        public void BuildConnectionFactory_MapsConfiguredRuntimeSettings()
        {
            RabbitMqRuntimeOptions options = CreateRabbitMqRuntimeOptions(enableSsl: true);

            ConnectionFactory factory = RabbitMqConnectionFactoryBuilder.BuildConnectionFactory(options, "VectorNNTP.BackFiller:test");

            Assert.Equal(options.Port, factory.Port);
            Assert.Equal(options.VirtualHost, factory.VirtualHost);
            Assert.Equal(options.RequestedHeartbeatSeconds, (int)factory.RequestedHeartbeat.TotalSeconds);
            Assert.Equal(options.ConnectionBlockedTimeoutSeconds, (int)factory.RequestedConnectionTimeout.TotalSeconds);
            Assert.Equal(options.RpcTimeoutSeconds, (int)factory.ContinuationTimeout.TotalSeconds);
            Assert.Equal(options.SocketTimeoutSeconds, (int)factory.SocketReadTimeout.TotalSeconds);
            Assert.Equal(options.SocketTimeoutSeconds, (int)factory.SocketWriteTimeout.TotalSeconds);
            Assert.Equal((ushort)options.RequestedChannelMax, factory.RequestedChannelMax);
            Assert.Equal(options.Username, factory.UserName);
            Assert.Equal(options.Password, factory.Password);
            Assert.True(factory.Ssl.Enabled);
            Assert.False(factory.AutomaticRecoveryEnabled);
            Assert.False(factory.TopologyRecoveryEnabled);
        }
        /// <summary>
        /// Verifies the BuildSanitizedSnapshot_DoesNotLogPasswordMaterial scenario and expected contract.
        /// </summary>
        [Fact]
        public void BuildSanitizedSnapshot_DoesNotLogPasswordMaterial()
        {
            RabbitMqRuntimeOptions options = CreateRabbitMqRuntimeOptions(enableSsl: false);

            RabbitMqConnectionFactorySnapshot snapshot = RabbitMqConnectionFactoryBuilder.BuildSanitizedSnapshot(options, "VectorNNTP.BackFiller:test");

            Assert.True(snapshot.UsesUsernameAuthentication);
            Assert.True(snapshot.HasPassword);
            Assert.DoesNotContain(options.Password!, snapshot.ToString(), StringComparison.Ordinal);
        }
        /// <summary>
        /// Verifies the TopologyBuilder_BackboneNamespaces_AreIsolated scenario and expected contract.
        /// </summary>
        [Fact]
        public void TopologyBuilder_BackboneNamespaces_AreIsolated()
        {
            IReadOnlyList<RabbitMqBackboneTopologyDefinition> definitions = RabbitMqTopologyBuilder.BuildDefinitions(
                serverId: 11,
                backbones: ["Giganews", "Eweka"]);

            RabbitMqBackboneTopologyDefinition giganews = Assert.Single(definitions, static x => x.Backbone == "Giganews");
            RabbitMqBackboneTopologyDefinition eweka = Assert.Single(definitions, static x => x.Backbone == "Eweka");

            Assert.Equal("grabbers.giganews", giganews.ExchangeName);
            Assert.Equal("grabbers.giganews", giganews.QueueName);
            Assert.Equal("grabbers.giganews", giganews.RoutingKey);

            Assert.Equal("grabbers.eweka", eweka.ExchangeName);
            Assert.Equal("grabbers.eweka", eweka.QueueName);
            Assert.Equal("grabbers.eweka", eweka.RoutingKey);

            Assert.NotEqual(giganews.ExchangeName, eweka.ExchangeName);
            Assert.NotEqual(giganews.QueueName, eweka.QueueName);
            Assert.NotEqual(giganews.RoutingKey, eweka.RoutingKey);
        }
        /// <summary>
        /// Verifies the TopologyBuilder_DeclaresExpectedExchangeAndBindingProperties scenario and expected contract.
        /// </summary>
        [Fact]
        public void TopologyBuilder_DeclaresExpectedExchangeAndBindingProperties()
        {
            RabbitMqBackboneTopologyDefinition definition = Assert.Single(RabbitMqTopologyBuilder.BuildDefinitions(11, ["Giganews"]));

            Assert.Equal("grabbers.giganews", definition.ExchangeName);
            Assert.Equal("grabbers.giganews", definition.QueueName);
            Assert.Equal(definition.ExchangeName, definition.QueueName);
            Assert.Equal(ExchangeType.Fanout, definition.ExchangeType);
            Assert.True(definition.ExchangeDurable);
            Assert.False(definition.ExchangeAutoDelete);
            Assert.Null(definition.ExchangeArguments);

            Assert.Equal("grabbers.giganews", definition.RoutingKey);
            Assert.Null(definition.BindingArguments);
        }
        /// <summary>
        /// Verifies the ConnectionManager_WhenConnectionShutdownObserved_AttemptsConnectionReplacement scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task ConnectionManager_WhenConnectionShutdownObserved_AttemptsConnectionReplacement()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            FakeRabbitMqBrokerConnector connector = new();
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions();
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);

            List<RabbitMqConnectionReplacedEventArgs> replacements = [];
            manager.ConnectionReplaced += (_, args) => replacements.Add(args);

            await manager.EnsureConnectedAsync(CancellationToken.None);
            Assert.Equal(RabbitMqInfrastructureState.Connected, manager.State);
            Assert.Equal(1, manager.ConnectionGeneration);

            FakeRabbitMqBrokerConnection firstConnection = connector.LastConnection ?? throw new InvalidOperationException("Expected first connection.");
            firstConnection.RaiseConnectionShutdown();

            bool recovered = await WaitForAsync(
                () => connector.ConnectCallCount >= 2 &&
                    (manager.State == RabbitMqInfrastructureState.Connected || manager.State == RabbitMqInfrastructureState.TopologyReady),
                TimeSpan.FromSeconds(5)).ConfigureAwait(false);

            Assert.True(recovered);
            Assert.True(manager.ConnectionGeneration >= 2);
            Assert.Contains(replacements, static args => args.ConnectionGeneration == 1 && !args.IsReplacement);
            Assert.Contains(replacements, static args => args.ConnectionGeneration >= 2 && args.IsReplacement);

            await manager.DisposeAsync();
        }
        /// <summary>
        /// Verifies the ConnectionManager_ShutdownPreventsRecoveryReplacement scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task ConnectionManager_ShutdownPreventsRecoveryReplacement()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            FakeRabbitMqBrokerConnector connector = new();
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions();
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);

            await manager.EnsureConnectedAsync(CancellationToken.None);
            int initialConnectCount = connector.ConnectCallCount;

            shutdownCoordinator.SignalGracefulShutdown(TimeSpan.FromSeconds(1), ShutdownCoordinator.ShutdownReason.HostStopping);

            FakeRabbitMqBrokerConnection firstConnection = connector.LastConnection ?? throw new InvalidOperationException("Expected first connection.");
            firstConnection.RaiseConnectionShutdown();

            await Task.Delay(250).ConfigureAwait(false);
            Assert.Equal(initialConnectCount, connector.ConnectCallCount);

            await manager.DisposeAsync();
        }
        /// <summary>
        /// Verifies the ConnectionManager_CreateOwnedChannelAsync_ReturnsIndependentOwnedChannels scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task ConnectionManager_CreateOwnedChannelAsync_ReturnsIndependentOwnedChannels()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            FakeRabbitMqBrokerConnector connector = new();
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions();
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);

            await manager.EnsureConnectedAsync(CancellationToken.None);

            await using RabbitMqOwnedChannel first = await manager.CreateOwnedChannelAsync("test-owner-1", CancellationToken.None);
            await using RabbitMqOwnedChannel second = await manager.CreateOwnedChannelAsync("test-owner-2", CancellationToken.None);

            Assert.NotSame(first.Channel, second.Channel);
            Assert.Equal("test-owner-1", first.Owner);
            Assert.Equal("test-owner-2", second.Owner);
            Assert.Equal(manager.ConnectionGeneration, first.ConnectionGeneration);
            Assert.Equal(manager.ConnectionGeneration, second.ConnectionGeneration);

            await manager.DisposeAsync();
        }
        /// <summary>
        /// Verifies the TopologyBuilder_SameBackboneDifferentServerIds_ProduceIdenticalTopologyIdentity scenario and expected contract.
        /// </summary>
        [Fact]
        public void TopologyBuilder_SameBackboneDifferentServerIds_ProduceIdenticalTopologyIdentity()
        {
            RabbitMqBackboneTopologyDefinition server1 = Assert.Single(RabbitMqTopologyBuilder.BuildDefinitions(1, ["Giganews"]));
            RabbitMqBackboneTopologyDefinition server2 = Assert.Single(RabbitMqTopologyBuilder.BuildDefinitions(2, ["Giganews"]));

            Assert.Equal("grabbers.giganews", server1.ExchangeName);
            Assert.Equal("grabbers.giganews", server1.QueueName);
            Assert.Equal("grabbers.giganews", server1.RoutingKey);

            Assert.Equal(server1.ExchangeName, server2.ExchangeName);
            Assert.Equal(server1.QueueName, server2.QueueName);
            Assert.Equal(server1.RoutingKey, server2.RoutingKey);
        }
        /// <summary>
        /// Verifies the TopologyBuilder_DeclaresQuorumQueue scenario and expected contract.
        /// </summary>
        [Fact]
        public void TopologyBuilder_DeclaresQuorumQueue()
        {
            RabbitMqBackboneTopologyDefinition definition = Assert.Single(RabbitMqTopologyBuilder.BuildDefinitions(1, ["Giganews"]));

            Assert.NotNull(definition.QueueArguments);
            Assert.Single(definition.QueueArguments!);
            Assert.True(definition.QueueArguments.TryGetValue("x-queue-type", out object? queueType));
            Assert.Equal("quorum", queueType as string);
            Assert.False(definition.QueueArguments.ContainsKey("x-message-ttl"));
            Assert.False(definition.QueueArguments.ContainsKey("x-expires"));

            Assert.True(definition.QueueDurable);
            Assert.False(definition.QueueExclusive);
            Assert.False(definition.QueueAutoDelete);
            Assert.True(definition.ExchangeDurable);
            Assert.False(definition.ExchangeAutoDelete);
            Assert.Equal(ExchangeType.Fanout, definition.ExchangeType);
        }
        /// <summary>
        /// Verifies the TopologyInitializer_CanBeCalledRepeatedly_IdempotentFromInfrastructurePerspective scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task TopologyInitializer_CanBeCalledRepeatedly_IdempotentFromInfrastructurePerspective()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            FakeRabbitMqBrokerConnector connector = new();
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions();
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer initializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);

            await manager.EnsureConnectedAsync(CancellationToken.None);

            await initializer.InitializeAsync(runtimeOptions.BackFillerId, ["Giganews", "Eweka"], CancellationToken.None);
            int channelsAfterFirstInit = connector.LastConnection?.ChannelCreateCount ?? throw new InvalidOperationException("Expected initialized RabbitMQ connection.");

            await initializer.InitializeAsync(runtimeOptions.BackFillerId, ["Giganews", "Eweka"], CancellationToken.None);
            int channelsAfterSecondInit = connector.LastConnection?.ChannelCreateCount ?? throw new InvalidOperationException("Expected initialized RabbitMQ connection.");

            Assert.Equal(RabbitMqInfrastructureState.TopologyReady, manager.State);
            Assert.Equal(2, channelsAfterFirstInit);
            Assert.Equal(2, channelsAfterSecondInit);
            await manager.DisposeAsync();
        }

        /// <summary>
        /// Verifies the WaitForAsync scenario and expected contract.
        /// </summary>
        private static async Task<bool> WaitForAsync(Func<bool> condition, TimeSpan timeout)
        {
            DateTime deadline = DateTime.UtcNow.Add(timeout);
            while (DateTime.UtcNow < deadline)
            {
                if (condition())
                {
                    return true;
                }

                await Task.Delay(50).ConfigureAwait(false);
            }

            return condition();
        }

        /// <summary>
        /// Verifies the CreateRuntimeOptions scenario and expected contract.
        /// </summary>
        private static BackFillerRuntimeOptions CreateRuntimeOptions()
        {
            return new BackFillerRuntimeOptions(
                CanonicalBackFillerFqdn: "backfiller-1.usenet.ninja",
                BackFillerId: 1,
                CanonicalDnsSuffix: "usenet.ninja",
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
                ShutdownGracePeriodSeconds: 120,
                ShutdownDrainQueuedWork: true,
                ShutdownFinishActiveArticles: true,
                RabbitMqMaximumShutdownDrainTimeoutSeconds: 30,
                WriteBatchCoalesceMicroseconds: 250,
                RabbitMq: CreateRabbitMqRuntimeOptions(enableSsl: false));
        }

        /// <summary>
        /// Verifies the CreateRabbitMqRuntimeOptions scenario and expected contract.
        /// </summary>
        private static RabbitMqRuntimeOptions CreateRabbitMqRuntimeOptions(bool enableSsl)
        {
            return new RabbitMqRuntimeOptions(
                Hosts: ["localhost"],
                Port: 5672,
                Username: "nntparticles",
                Password: "super-secret",
                VirtualHost: "/",
                EnableSsl: enableSsl,
                ChannelLeaseTimeoutSeconds: 60,
                RpcTimeoutSeconds: 30,
                ConnectionBlockedTimeoutSeconds: 30,
                ChannelPoolSize: 512,
                MinConnections: 4,
                MaxConnections: 16,
                MaxConsecutiveRecoveryFailures: 5,
                MaxPendingLeaseWaiters: 1024,
                ConnectionScaleDownIdleSeconds: 300,
                ScaleDownCooldownSeconds: 30,
                NetworkRecoveryIntervalSeconds: 5,
                PoolReconnectBaseDelayMs: 50,
                PoolReconnectMaxDelayMs: 250,
                MinimumConnectionLifetimeSeconds: 300,
                PublishConfirmTimeoutSeconds: 10,
                MaximumShutdownDrainTimeoutSeconds: 30,
                DegradedThreshold: 0.75,
                UnhealthyThreshold: 5,
                RequestedHeartbeatSeconds: 60,
                SocketTimeoutSeconds: 30,
                RequestedChannelMax: 2047,
                ConsumerPrefetchCount: null);
        }

        /// <summary>
        /// Documents the FakeRabbitMqBrokerConnector test type and its protected contract.
        /// </summary>
        private sealed class FakeRabbitMqBrokerConnector : IRabbitMqBrokerConnector
        {
            /// <summary>
            /// Stores the _connectCallCount fixture value used by these tests.
            /// </summary>
            private int _connectCallCount;

            /// <summary>
            /// Stores the ConnectCallCount value used by this test fixture.
            /// </summary>
            internal int ConnectCallCount => Volatile.Read(ref _connectCallCount);

            /// <summary>
            /// Stores the LastConnection value used by this test fixture.
            /// </summary>
            internal FakeRabbitMqBrokerConnection? LastConnection { get; private set; }

            /// <summary>
            /// Verifies the ConnectAsync scenario and expected contract.
            /// </summary>
            public Task<IRabbitMqBrokerConnection> ConnectAsync(RabbitMqRuntimeOptions runtimeOptions, string clientProvidedConnectionName, CancellationToken cancellationToken)
            {
                _ = Interlocked.Increment(ref _connectCallCount);

                FakeRabbitMqBrokerConnection connection = new(runtimeOptions.Hosts[0], runtimeOptions.Port, runtimeOptions.VirtualHost, clientProvidedConnectionName);
                LastConnection = connection;
                return Task.FromResult<IRabbitMqBrokerConnection>(connection);
            }
        }

        /// <summary>
        /// Documents the FakeRabbitMqBrokerConnection test type and its protected contract.
        /// </summary>
        private sealed class FakeRabbitMqBrokerConnection(string host, int port, string virtualHost, string connectionName) : IRabbitMqBrokerConnection
        {
            /// <summary>
            /// Stores the _channelCounter fixture value used by these tests.
            /// </summary>
            private int _channelCounter;

            /// <summary>
            /// Stores the IsOpen value used by this test fixture.
            /// </summary>
            public bool IsOpen { get; private set; } = true;

            /// <summary>
            /// Stores the ChannelCreateCount value used by this test fixture.
            /// </summary>
            internal int ChannelCreateCount => Volatile.Read(ref _channelCounter);

            /// <summary>
            /// Stores the EndpointHostName value used by this test fixture.
            /// </summary>
            public string EndpointHostName { get; } = host;

            /// <summary>
            /// Stores the EndpointPort value used by this test fixture.
            /// </summary>
            public int EndpointPort { get; } = port;

            /// <summary>
            /// Stores the VirtualHost value used by this test fixture.
            /// </summary>
            public string VirtualHost { get; } = virtualHost;

            /// <summary>
            /// Stores the ClientProvidedName value used by this test fixture.
            /// </summary>
            public string ClientProvidedName { get; } = connectionName;

            /// <summary>
            /// Stores the UnderlyingConnection value used by this test fixture.
            /// </summary>
            public IConnection UnderlyingConnection => throw new NotSupportedException();

            /// <summary>
            /// Documents the ConnectionShutdown member and its test-supporting contract.
            /// </summary>
            public event EventHandler<ShutdownEventArgs>? ConnectionShutdown;

            /// <summary>
            /// Documents the CallbackException member and its test-supporting contract.
            /// </summary>
            public event EventHandler<CallbackExceptionEventArgs>? CallbackException;

            /// <summary>
            /// Documents the ConnectionBlocked member and its test-supporting contract.
            /// </summary>
            public event EventHandler<ConnectionBlockedEventArgs>? ConnectionBlocked;

            /// <summary>
            /// Documents the ConnectionUnblocked member and its test-supporting contract.
            /// </summary>
            public event EventHandler<AsyncEventArgs>? ConnectionUnblocked;

            /// <summary>
            /// Documents the ConnectionRecoveryError member and its test-supporting contract.
            /// </summary>
            public event EventHandler<ConnectionRecoveryErrorEventArgs>? ConnectionRecoveryError;

            /// <summary>
            /// Documents the RecoverySucceeded member and its test-supporting contract.
            /// </summary>
            public event EventHandler<AsyncEventArgs>? RecoverySucceeded;

            /// <summary>
            /// Verifies the CreateChannelAsync scenario and expected contract.
            /// </summary>
            public Task<IRabbitMqChannel> CreateChannelAsync(CancellationToken cancellationToken, bool enablePublisherConfirmations = false)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = enablePublisherConfirmations;
                int next = Interlocked.Increment(ref _channelCounter);
                IRabbitMqChannel channel = new FakeRabbitMqChannel(next);
                return Task.FromResult(channel);
            }

            /// <summary>
            /// Verifies the DisposeAsync scenario and expected contract.
            /// </summary>
            public ValueTask DisposeAsync()
            {
                IsOpen = false;
                return ValueTask.CompletedTask;
            }

            /// <summary>
            /// Verifies the RaiseConnectionShutdown scenario and expected contract.
            /// </summary>
            internal void RaiseConnectionShutdown()
            {
                ConnectionShutdown?.Invoke(this, new ShutdownEventArgs(ShutdownInitiator.Peer, 320, "Closed by broker"));
            }

            /// <summary>
            /// Verifies the RaiseCallbackException scenario and expected contract.
            /// </summary>
            internal void RaiseCallbackException(Exception exception)
            {
                CallbackException?.Invoke(this, new CallbackExceptionEventArgs(new Dictionary<string, object>(), exception, default));
            }

            /// <summary>
            /// Verifies the RaiseConnectionBlocked scenario and expected contract.
            /// </summary>
            internal void RaiseConnectionBlocked(string reason)
            {
                ConnectionBlocked?.Invoke(this, new ConnectionBlockedEventArgs(reason));
            }

            /// <summary>
            /// Verifies the RaiseConnectionUnblocked scenario and expected contract.
            /// </summary>
            internal void RaiseConnectionUnblocked()
            {
                ConnectionUnblocked?.Invoke(this, new AsyncEventArgs());
            }

            /// <summary>
            /// Verifies the RaiseConnectionRecoveryError scenario and expected contract.
            /// </summary>
            internal void RaiseConnectionRecoveryError(Exception exception)
            {
                ConnectionRecoveryError?.Invoke(this, new ConnectionRecoveryErrorEventArgs(exception, default));
            }

            /// <summary>
            /// Verifies the RaiseRecoverySucceeded scenario and expected contract.
            /// </summary>
            internal void RaiseRecoverySucceeded()
            {
                RecoverySucceeded?.Invoke(this, new AsyncEventArgs());
            }
        }

        /// <summary>
        /// Documents the FakeRabbitMqChannel test type and its protected contract.
        /// </summary>
        private sealed class FakeRabbitMqChannel(int id) : IRabbitMqChannel
        {
            /// <summary>
            /// Stores the UnderlyingChannel value used by this test fixture.
            /// </summary>
            public IChannel UnderlyingChannel => throw new NotSupportedException();

            /// <summary>
            /// Verifies the ExchangeDeclareAsync scenario and expected contract.
            /// </summary>
            public Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }

            /// <summary>
            /// Verifies the QueueDeclareAsync scenario and expected contract.
            /// </summary>
            public Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }

            /// <summary>
            /// Verifies the QueueBindAsync scenario and expected contract.
            /// </summary>
            public Task QueueBindAsync(string queue, string exchange, string routingKey, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }

            /// <summary>
            /// Verifies the BasicQosAsync scenario and expected contract.
            /// </summary>
            public Task BasicQosAsync(uint prefetchSize, ushort prefetchCount, bool global, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = prefetchSize;
                _ = prefetchCount;
                _ = global;
                return Task.CompletedTask;
            }

            /// <summary>
            /// Verifies the BasicConsumeAsync scenario and expected contract.
            /// </summary>
            public Task<string> BasicConsumeAsync(string queue, bool autoAck, IAsyncBasicConsumer consumer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = queue;
                _ = autoAck;
                _ = consumer;
                return Task.FromResult($"ctag-{id}");
            }

            /// <summary>
            /// Verifies the BasicCancelAsync scenario and expected contract.
            /// </summary>
            public Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = consumerTag;
                return Task.CompletedTask;
            }

            /// <summary>
            /// Verifies the BasicAckAsync scenario and expected contract.
            /// </summary>
            public ValueTask BasicAckAsync(ulong deliveryTag, bool multiple, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = deliveryTag;
                _ = multiple;
                return ValueTask.CompletedTask;
            }

            /// <summary>
            /// Verifies the BasicNackAsync scenario and expected contract.
            /// </summary>
            public ValueTask BasicNackAsync(ulong deliveryTag, bool multiple, bool requeue, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = deliveryTag;
                _ = multiple;
                _ = requeue;
                return ValueTask.CompletedTask;
            }

            /// <summary>
            /// Verifies the BasicPublishAsync scenario and expected contract.
            /// </summary>
            public ValueTask BasicPublishAsync(string exchange, string routingKey, bool mandatory, BasicProperties basicProperties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = exchange;
                _ = routingKey;
                _ = mandatory;
                _ = basicProperties;
                _ = body;
                return ValueTask.CompletedTask;
            }

            /// <summary>
            /// Verifies the DisposeAsync scenario and expected contract.
            /// </summary>
            public ValueTask DisposeAsync()
            {
                _ = id;
                return ValueTask.CompletedTask;
            }
        }
    }
}
