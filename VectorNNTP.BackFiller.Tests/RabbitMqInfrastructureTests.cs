// <copyright file="RabbitMqInfrastructureTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for rabbit mq infrastructure, covering dependency integration and failure handling.
// Primary responsibility: documents the executable contracts covered by the rabbit mq infrastructure test suite.

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
        /// Verifies the build connection factory maps configured runtime settings scenario and its documented contract.
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
        /// Verifies the build sanitized snapshot does not log password material scenario and its documented contract.
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
        /// Verifies the topology builder backbone namespaces are isolated scenario and its documented contract.
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
        /// Verifies the topology builder declares expected exchange and binding properties scenario and its documented contract.
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
        /// Verifies the connection manager when connection shutdown observed attempts connection replacement scenario and its documented contract.
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
        /// Verifies the connection manager shutdown prevents recovery replacement scenario and its documented contract.
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
        /// Verifies the connection manager create owned channel async returns independent owned channels scenario and its documented contract.
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
        /// Verifies the topology builder same backbone different server ids produce identical topology identity scenario and its documented contract.
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
        /// Verifies the topology builder declares quorum queue scenario and its documented contract.
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
        /// Verifies the topology initializer can be called repeatedly idempotent from infrastructure perspective scenario and its documented contract.
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
        /// Verifies the wait for async scenario and its documented contract.
        /// </summary>
        /// <returns>The wait for async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the wait for async scenario and its documented contract.
        /// </summary>
        /// <param name="condition">The condition supplied to the helper.</param>
        /// <param name="timeout">The timeout supplied to the helper.</param>
        /// <returns>The wait for async value produced for the requested scenario.</returns>
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
        /// Verifies the create runtime options scenario and its documented contract.
        /// </summary>
        /// <returns>The create runtime options value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the create runtime options scenario and its documented contract.
        /// </summary>
        /// <returns>The create runtime options value produced for the requested scenario.</returns>
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
        /// Verifies the create rabbit mq runtime options scenario and its documented contract.
        /// </summary>
        /// <returns>The create rabbit mq runtime options value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the create rabbit mq runtime options scenario and its documented contract.
        /// </summary>
        /// <param name="enableSsl">The enable ssl supplied to the helper.</param>
        /// <returns>The create rabbit mq runtime options value produced for the requested scenario.</returns>
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
        /// Verifies the fake rabbit mq broker connector scenario and its documented contract.
        /// </summary>
        private sealed class FakeRabbitMqBrokerConnector : IRabbitMqBrokerConnector
        {
            /// <summary>
            /// Supplies  connect call count for the fixture or scenario under test.
            /// </summary>
            private int _connectCallCount;

            /// <summary>
            /// Exercises connect call count behavior, including the expected result and failure semantics.
            /// </summary>
            internal int ConnectCallCount => Volatile.Read(ref _connectCallCount);

            /// <summary>
            /// Supplies last connection for the fixture or scenario under test.
            /// </summary>
            internal FakeRabbitMqBrokerConnection? LastConnection { get; private set; }

            /// <summary>
        /// Verifies the connect async scenario and its documented contract.
            /// </summary>
        /// <returns>The connect async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the connect async scenario and its documented contract.
        /// </summary>
        /// <param name="runtimeOptions">The runtime options supplied to the helper.</param>
        /// <param name="clientProvidedConnectionName">The client provided connection name supplied to the helper.</param>
        /// <param name="cancellationToken">The cancellation token supplied to the helper.</param>
        /// <returns>The connect async value produced for the requested scenario.</returns>
            public Task<IRabbitMqBrokerConnection> ConnectAsync(RabbitMqRuntimeOptions runtimeOptions, string clientProvidedConnectionName, CancellationToken cancellationToken)
            {
                _ = Interlocked.Increment(ref _connectCallCount);

                FakeRabbitMqBrokerConnection connection = new(runtimeOptions.Hosts[0], runtimeOptions.Port, runtimeOptions.VirtualHost, clientProvidedConnectionName);
                LastConnection = connection;
                return Task.FromResult<IRabbitMqBrokerConnection>(connection);
            }
        }

        /// <summary>
        /// Verifies the fake rabbit mq broker connection scenario and its documented contract.
        /// </summary>
        /// <returns>The fake rabbit mq broker connection value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the fake rabbit mq broker connection scenario and its documented contract.
        /// </summary>
        /// <param name="host">The host supplied to the helper.</param>
        /// <param name="port">The port supplied to the helper.</param>
        /// <param name="virtualHost">The virtual host supplied to the helper.</param>
        /// <param name="connectionName">The connection name supplied to the helper.</param>
        /// <returns>The fake rabbit mq broker connection value produced for the requested scenario.</returns>
        private sealed class FakeRabbitMqBrokerConnection(string host, int port, string virtualHost, string connectionName) : IRabbitMqBrokerConnection
        {
            /// <summary>
            /// Supplies  channel counter for the fixture or scenario under test.
            /// </summary>
            private int _channelCounter;

            /// <summary>
            /// Supplies is open for the fixture or scenario under test.
            /// </summary>
            public bool IsOpen { get; private set; } = true;

            /// <summary>
            /// Exercises channel create count behavior, including the expected result and failure semantics.
            /// </summary>
            internal int ChannelCreateCount => Volatile.Read(ref _channelCounter);

            /// <summary>
            /// Supplies endpoint host name for the fixture or scenario under test.
            /// </summary>
            public string EndpointHostName { get; } = host;

            /// <summary>
            /// Supplies endpoint port for the fixture or scenario under test.
            /// </summary>
            public int EndpointPort { get; } = port;

            /// <summary>
            /// Supplies virtual host for the fixture or scenario under test.
            /// </summary>
            public string VirtualHost { get; } = virtualHost;

            /// <summary>
            /// Supplies client provided name for the fixture or scenario under test.
            /// </summary>
            public string ClientProvidedName { get; } = connectionName;

            /// <summary>
            /// Exercises underlying connection behavior, including the expected result and failure semantics.
            /// </summary>
            public IConnection UnderlyingConnection => throw new NotSupportedException();

            /// <summary>
            /// Supplies connection shutdown for the fixture or scenario under test.
            /// </summary>
            public event EventHandler<ShutdownEventArgs>? ConnectionShutdown;

            /// <summary>
            /// Supplies callback exception for the fixture or scenario under test.
            /// </summary>
            public event EventHandler<CallbackExceptionEventArgs>? CallbackException;

            /// <summary>
            /// Supplies connection blocked for the fixture or scenario under test.
            /// </summary>
            public event EventHandler<ConnectionBlockedEventArgs>? ConnectionBlocked;

            /// <summary>
            /// Supplies connection unblocked for the fixture or scenario under test.
            /// </summary>
            public event EventHandler<AsyncEventArgs>? ConnectionUnblocked;

            /// <summary>
            /// Supplies connection recovery error for the fixture or scenario under test.
            /// </summary>
            public event EventHandler<ConnectionRecoveryErrorEventArgs>? ConnectionRecoveryError;

            /// <summary>
            /// Supplies recovery succeeded for the fixture or scenario under test.
            /// </summary>
            public event EventHandler<AsyncEventArgs>? RecoverySucceeded;

            /// <summary>
        /// Verifies the create channel async scenario and its documented contract.
            /// </summary>
        /// <returns>The create channel async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the create channel async scenario and its documented contract.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token supplied to the helper.</param>
        /// <param name="enablePublisherConfirmations">The enable publisher confirmations supplied to the helper.</param>
        /// <returns>The create channel async value produced for the requested scenario.</returns>
            public Task<IRabbitMqChannel> CreateChannelAsync(CancellationToken cancellationToken, bool enablePublisherConfirmations = false)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = enablePublisherConfirmations;
                int next = Interlocked.Increment(ref _channelCounter);
                IRabbitMqChannel channel = new FakeRabbitMqChannel(next);
                return Task.FromResult(channel);
            }

            /// <summary>
        /// Verifies the dispose async scenario and its documented contract.
            /// </summary>
        /// <returns>The dispose async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the dispose async scenario and its documented contract.
        /// </summary>
        /// <returns>The dispose async value produced for the requested scenario.</returns>
            public ValueTask DisposeAsync()
            {
                IsOpen = false;
                return ValueTask.CompletedTask;
            }

            /// <summary>
        /// Verifies the raise connection shutdown scenario and its documented contract.
            /// </summary>
            internal void RaiseConnectionShutdown()
            {
                ConnectionShutdown?.Invoke(this, new ShutdownEventArgs(ShutdownInitiator.Peer, 320, "Closed by broker"));
            }

            /// <summary>
        /// Verifies the raise callback exception scenario and its documented contract.
            /// </summary>
            internal void RaiseCallbackException(Exception exception)
            {
                CallbackException?.Invoke(this, new CallbackExceptionEventArgs(new Dictionary<string, object>(), exception, default));
            }

            /// <summary>
        /// Verifies the raise connection blocked scenario and its documented contract.
            /// </summary>
            internal void RaiseConnectionBlocked(string reason)
            {
                ConnectionBlocked?.Invoke(this, new ConnectionBlockedEventArgs(reason));
            }

            /// <summary>
        /// Verifies the raise connection unblocked scenario and its documented contract.
            /// </summary>
            internal void RaiseConnectionUnblocked()
            {
                ConnectionUnblocked?.Invoke(this, new AsyncEventArgs());
            }

            /// <summary>
        /// Verifies the raise connection recovery error scenario and its documented contract.
            /// </summary>
            internal void RaiseConnectionRecoveryError(Exception exception)
            {
                ConnectionRecoveryError?.Invoke(this, new ConnectionRecoveryErrorEventArgs(exception, default));
            }

            /// <summary>
        /// Verifies the raise recovery succeeded scenario and its documented contract.
            /// </summary>
            internal void RaiseRecoverySucceeded()
            {
                RecoverySucceeded?.Invoke(this, new AsyncEventArgs());
            }
        }

        /// <summary>
        /// Verifies the fake rabbit mq channel scenario and its documented contract.
        /// </summary>
        /// <returns>The fake rabbit mq channel value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the fake rabbit mq channel scenario and its documented contract.
        /// </summary>
        /// <param name="id">The id supplied to the helper.</param>
        /// <returns>The fake rabbit mq channel value produced for the requested scenario.</returns>
        private sealed class FakeRabbitMqChannel(int id) : IRabbitMqChannel
        {
            /// <summary>
            /// Exercises underlying channel behavior, including the expected result and failure semantics.
            /// </summary>
            public IChannel UnderlyingChannel => throw new NotSupportedException();

            /// <summary>
        /// Verifies the exchange declare async scenario and its documented contract.
            /// </summary>
        /// <returns>The exchange declare async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the exchange declare async scenario and its documented contract.
        /// </summary>
        /// <param name="exchange">The exchange supplied to the helper.</param>
        /// <param name="type">The type supplied to the helper.</param>
        /// <param name="durable">The durable supplied to the helper.</param>
        /// <param name="autoDelete">The auto delete supplied to the helper.</param>
        /// <param name="string">The string supplied to the helper.</param>
        /// <param name="arguments">The arguments supplied to the helper.</param>
        /// <param name="cancellationToken">The cancellation token supplied to the helper.</param>
        /// <returns>The exchange declare async value produced for the requested scenario.</returns>
            public Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }

            /// <summary>
        /// Verifies the queue declare async scenario and its documented contract.
            /// </summary>
        /// <returns>The queue declare async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the queue declare async scenario and its documented contract.
        /// </summary>
        /// <param name="queue">The queue supplied to the helper.</param>
        /// <param name="durable">The durable supplied to the helper.</param>
        /// <param name="exclusive">The exclusive supplied to the helper.</param>
        /// <param name="autoDelete">The auto delete supplied to the helper.</param>
        /// <param name="string">The string supplied to the helper.</param>
        /// <param name="arguments">The arguments supplied to the helper.</param>
        /// <param name="cancellationToken">The cancellation token supplied to the helper.</param>
        /// <returns>The queue declare async value produced for the requested scenario.</returns>
            public Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }

            /// <summary>
        /// Verifies the queue bind async scenario and its documented contract.
            /// </summary>
        /// <returns>The queue bind async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the queue bind async scenario and its documented contract.
        /// </summary>
        /// <param name="queue">The queue supplied to the helper.</param>
        /// <param name="exchange">The exchange supplied to the helper.</param>
        /// <param name="routingKey">The routing key supplied to the helper.</param>
        /// <param name="string">The string supplied to the helper.</param>
        /// <param name="arguments">The arguments supplied to the helper.</param>
        /// <param name="cancellationToken">The cancellation token supplied to the helper.</param>
        /// <returns>The queue bind async value produced for the requested scenario.</returns>
            public Task QueueBindAsync(string queue, string exchange, string routingKey, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }

            /// <summary>
        /// Verifies the basic qos async scenario and its documented contract.
            /// </summary>
        /// <returns>The basic qos async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the basic qos async scenario and its documented contract.
        /// </summary>
        /// <param name="prefetchSize">The prefetch size supplied to the helper.</param>
        /// <param name="prefetchCount">The prefetch count supplied to the helper.</param>
        /// <param name="global">The global supplied to the helper.</param>
        /// <param name="cancellationToken">The cancellation token supplied to the helper.</param>
        /// <returns>The basic qos async value produced for the requested scenario.</returns>
            public Task BasicQosAsync(uint prefetchSize, ushort prefetchCount, bool global, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = prefetchSize;
                _ = prefetchCount;
                _ = global;
                return Task.CompletedTask;
            }

            /// <summary>
        /// Verifies the basic consume async scenario and its documented contract.
            /// </summary>
        /// <returns>The basic consume async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the basic consume async scenario and its documented contract.
        /// </summary>
        /// <param name="queue">The queue supplied to the helper.</param>
        /// <param name="autoAck">The auto ack supplied to the helper.</param>
        /// <param name="consumer">The consumer supplied to the helper.</param>
        /// <param name="cancellationToken">The cancellation token supplied to the helper.</param>
        /// <returns>The basic consume async value produced for the requested scenario.</returns>
            public Task<string> BasicConsumeAsync(string queue, bool autoAck, IAsyncBasicConsumer consumer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = queue;
                _ = autoAck;
                _ = consumer;
                return Task.FromResult($"ctag-{id}");
            }

            /// <summary>
        /// Verifies the basic cancel async scenario and its documented contract.
            /// </summary>
        /// <returns>The basic cancel async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the basic cancel async scenario and its documented contract.
        /// </summary>
        /// <param name="consumerTag">The consumer tag supplied to the helper.</param>
        /// <param name="cancellationToken">The cancellation token supplied to the helper.</param>
        /// <returns>The basic cancel async value produced for the requested scenario.</returns>
            public Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = consumerTag;
                return Task.CompletedTask;
            }

            /// <summary>
        /// Verifies the basic ack async scenario and its documented contract.
            /// </summary>
        /// <returns>The basic ack async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the basic ack async scenario and its documented contract.
        /// </summary>
        /// <param name="deliveryTag">The delivery tag supplied to the helper.</param>
        /// <param name="multiple">The multiple supplied to the helper.</param>
        /// <param name="cancellationToken">The cancellation token supplied to the helper.</param>
        /// <returns>The basic ack async value produced for the requested scenario.</returns>
            public ValueTask BasicAckAsync(ulong deliveryTag, bool multiple, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = deliveryTag;
                _ = multiple;
                return ValueTask.CompletedTask;
            }

            /// <summary>
        /// Verifies the basic nack async scenario and its documented contract.
            /// </summary>
        /// <returns>The basic nack async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the basic nack async scenario and its documented contract.
        /// </summary>
        /// <param name="deliveryTag">The delivery tag supplied to the helper.</param>
        /// <param name="multiple">The multiple supplied to the helper.</param>
        /// <param name="requeue">The requeue supplied to the helper.</param>
        /// <param name="cancellationToken">The cancellation token supplied to the helper.</param>
        /// <returns>The basic nack async value produced for the requested scenario.</returns>
            public ValueTask BasicNackAsync(ulong deliveryTag, bool multiple, bool requeue, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = deliveryTag;
                _ = multiple;
                _ = requeue;
                return ValueTask.CompletedTask;
            }

            /// <summary>
        /// Verifies the basic publish async scenario and its documented contract.
            /// </summary>
        /// <returns>The basic publish async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the basic publish async scenario and its documented contract.
        /// </summary>
        /// <param name="exchange">The exchange supplied to the helper.</param>
        /// <param name="routingKey">The routing key supplied to the helper.</param>
        /// <param name="mandatory">The mandatory supplied to the helper.</param>
        /// <param name="basicProperties">The basic properties supplied to the helper.</param>
        /// <param name="body">The body supplied to the helper.</param>
        /// <param name="cancellationToken">The cancellation token supplied to the helper.</param>
        /// <returns>The basic publish async value produced for the requested scenario.</returns>
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
        /// Verifies the dispose async scenario and its documented contract.
            /// </summary>
        /// <returns>The dispose async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the dispose async scenario and its documented contract.
        /// </summary>
        /// <returns>The dispose async value produced for the requested scenario.</returns>
            public ValueTask DisposeAsync()
            {
                _ = id;
                return ValueTask.CompletedTask;
            }
        }
    }
}
