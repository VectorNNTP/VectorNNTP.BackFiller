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
        /// Confirms the build connection factory maps configured runtime settings behavior.
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
        /// Confirms the build sanitized snapshot does not log password material behavior.
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
        /// Confirms the topology builder backbone namespaces are isolated behavior.
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
        /// Confirms the topology builder declares expected exchange and binding properties behavior.
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
        /// Confirms the connection manager when connection shutdown observed attempts connection replacement behavior.
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
        /// Confirms the connection manager shutdown prevents recovery replacement behavior.
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
        /// Confirms the connection manager create owned channel async returns independent owned channels behavior.
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
        /// Confirms the topology builder same backbone different server ids produce identical topology identity behavior.
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
        /// Confirms the topology builder declares quorum queue behavior.
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
        /// Confirms the topology initializer can be called repeatedly idempotent from infrastructure perspective behavior.
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
        /// Confirms the wait for async behavior.
        /// </summary>
        /// <returns>The value returned by the wait for async helper.</returns>
        /// <summary>
        /// Confirms the wait for async behavior.
        /// </summary>
        /// <param name="condition">The condition used by this test scenario.</param>
        /// <param name="timeout">The timeout used by this test scenario.</param>
        /// <returns>The value returned by the wait for async helper.</returns>
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
        /// Confirms the create runtime options behavior.
        /// </summary>
        /// <returns>The value returned by the create runtime options helper.</returns>
        /// <summary>
        /// Confirms the create runtime options behavior.
        /// </summary>
        /// <returns>The value returned by the create runtime options helper.</returns>
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
        /// Confirms the create rabbit mq runtime options behavior.
        /// </summary>
        /// <returns>The value returned by the create rabbit mq runtime options helper.</returns>
        /// <summary>
        /// Confirms the create rabbit mq runtime options behavior.
        /// </summary>
        /// <param name="enableSsl">The enable ssl used by this test scenario.</param>
        /// <returns>The value returned by the create rabbit mq runtime options helper.</returns>
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
        /// Confirms the fake rabbit mq broker connector behavior.
        /// </summary>
        private sealed class FakeRabbitMqBrokerConnector : IRabbitMqBrokerConnector
        {
            /// <summary>
            /// Supplies  connect call count for the fixture or scenario under test.
            /// </summary>
            private int _connectCallCount;

            /// <summary>
            /// Confirms connect call count behavior.
            /// </summary>
            internal int ConnectCallCount => Volatile.Read(ref _connectCallCount);

            /// <summary>
            /// Supplies last connection for the fixture or scenario under test.
            /// </summary>
            internal FakeRabbitMqBrokerConnection? LastConnection { get; private set; }

            /// <summary>
            /// Confirms the connect async behavior.
            /// </summary>
            /// <returns>The value returned by the connect async helper.</returns>
            /// <summary>
            /// Confirms the connect async behavior.
            /// </summary>
            /// <param name="runtimeOptions">The runtime options used by this test scenario.</param>
            /// <param name="clientProvidedConnectionName">The client provided connection name used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the connect async helper.</returns>
            public Task<IRabbitMqBrokerConnection> ConnectAsync(RabbitMqRuntimeOptions runtimeOptions, string clientProvidedConnectionName, CancellationToken cancellationToken)
            {
                _ = Interlocked.Increment(ref _connectCallCount);

                FakeRabbitMqBrokerConnection connection = new(runtimeOptions.Hosts[0], runtimeOptions.Port, runtimeOptions.VirtualHost, clientProvidedConnectionName);
                LastConnection = connection;
                return Task.FromResult<IRabbitMqBrokerConnection>(connection);
            }
        }

        /// <summary>
        /// Confirms the fake rabbit mq broker connection behavior.
        /// </summary>
        /// <returns>The value returned by the fake rabbit mq broker connection helper.</returns>
        /// <summary>
        /// Confirms the fake rabbit mq broker connection behavior.
        /// </summary>
        /// <param name="host">The host used by this test scenario.</param>
        /// <param name="port">The port used by this test scenario.</param>
        /// <param name="virtualHost">The virtual host used by this test scenario.</param>
        /// <param name="connectionName">The connection name used by this test scenario.</param>
        /// <returns>The value returned by the fake rabbit mq broker connection helper.</returns>
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
            /// Confirms channel create count behavior.
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
            /// Confirms underlying connection behavior.
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
            /// Confirms the create channel async behavior.
            /// </summary>
            /// <returns>The value returned by the create channel async helper.</returns>
            /// <summary>
            /// Confirms the create channel async behavior.
            /// </summary>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <param name="enablePublisherConfirmations">The enable publisher confirmations used by this test scenario.</param>
            /// <returns>The value returned by the create channel async helper.</returns>
            public Task<IRabbitMqChannel> CreateChannelAsync(CancellationToken cancellationToken, bool enablePublisherConfirmations = false)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = enablePublisherConfirmations;
                int next = Interlocked.Increment(ref _channelCounter);
                IRabbitMqChannel channel = new FakeRabbitMqChannel(next);
                return Task.FromResult(channel);
            }

            /// <summary>
            /// Confirms the dispose async behavior.
            /// </summary>
            /// <returns>The value returned by the dispose async helper.</returns>
            /// <summary>
            /// Confirms the dispose async behavior.
            /// </summary>
            /// <returns>The value returned by the dispose async helper.</returns>
            public ValueTask DisposeAsync()
            {
                IsOpen = false;
                return ValueTask.CompletedTask;
            }

            /// <summary>
            /// Confirms the raise connection shutdown behavior.
            /// </summary>
            internal void RaiseConnectionShutdown()
            {
                ConnectionShutdown?.Invoke(this, new ShutdownEventArgs(ShutdownInitiator.Peer, 320, "Closed by broker"));
            }

            /// <summary>
            /// Confirms the raise callback exception behavior.
            /// </summary>
            internal void RaiseCallbackException(Exception exception)
            {
                CallbackException?.Invoke(this, new CallbackExceptionEventArgs(new Dictionary<string, object>(), exception, default));
            }

            /// <summary>
            /// Confirms the raise connection blocked behavior.
            /// </summary>
            internal void RaiseConnectionBlocked(string reason)
            {
                ConnectionBlocked?.Invoke(this, new ConnectionBlockedEventArgs(reason));
            }

            /// <summary>
            /// Confirms the raise connection unblocked behavior.
            /// </summary>
            internal void RaiseConnectionUnblocked()
            {
                ConnectionUnblocked?.Invoke(this, new AsyncEventArgs());
            }

            /// <summary>
            /// Confirms the raise connection recovery error behavior.
            /// </summary>
            internal void RaiseConnectionRecoveryError(Exception exception)
            {
                ConnectionRecoveryError?.Invoke(this, new ConnectionRecoveryErrorEventArgs(exception, default));
            }

            /// <summary>
            /// Confirms the raise recovery succeeded behavior.
            /// </summary>
            internal void RaiseRecoverySucceeded()
            {
                RecoverySucceeded?.Invoke(this, new AsyncEventArgs());
            }
        }

        /// <summary>
        /// Confirms the fake rabbit mq channel behavior.
        /// </summary>
        /// <returns>The value returned by the fake rabbit mq channel helper.</returns>
        /// <summary>
        /// Confirms the fake rabbit mq channel behavior.
        /// </summary>
        /// <param name="id">The id used by this test scenario.</param>
        /// <returns>The value returned by the fake rabbit mq channel helper.</returns>
        private sealed class FakeRabbitMqChannel(int id) : IRabbitMqChannel
        {
            /// <summary>
            /// Confirms underlying channel behavior.
            /// </summary>
            public IChannel UnderlyingChannel => throw new NotSupportedException();

            /// <summary>
            /// Confirms the exchange declare async behavior.
            /// </summary>
            /// <returns>The value returned by the exchange declare async helper.</returns>
            /// <summary>
            /// Confirms the exchange declare async behavior.
            /// </summary>
            /// <param name="exchange">The exchange used by this test scenario.</param>
            /// <param name="type">The type used by this test scenario.</param>
            /// <param name="durable">The durable used by this test scenario.</param>
            /// <param name="autoDelete">The auto delete used by this test scenario.</param>
            /// <param name="string">The string used by this test scenario.</param>
            /// <param name="arguments">The arguments used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the exchange declare async helper.</returns>
            public Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }

            /// <summary>
            /// Confirms the queue declare async behavior.
            /// </summary>
            /// <returns>The value returned by the queue declare async helper.</returns>
            /// <summary>
            /// Confirms the queue declare async behavior.
            /// </summary>
            /// <param name="queue">The queue used by this test scenario.</param>
            /// <param name="durable">The durable used by this test scenario.</param>
            /// <param name="exclusive">The exclusive used by this test scenario.</param>
            /// <param name="autoDelete">The auto delete used by this test scenario.</param>
            /// <param name="string">The string used by this test scenario.</param>
            /// <param name="arguments">The arguments used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the queue declare async helper.</returns>
            public Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }

            /// <summary>
            /// Confirms the queue bind async behavior.
            /// </summary>
            /// <returns>The value returned by the queue bind async helper.</returns>
            /// <summary>
            /// Confirms the queue bind async behavior.
            /// </summary>
            /// <param name="queue">The queue used by this test scenario.</param>
            /// <param name="exchange">The exchange used by this test scenario.</param>
            /// <param name="routingKey">The routing key used by this test scenario.</param>
            /// <param name="string">The string used by this test scenario.</param>
            /// <param name="arguments">The arguments used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the queue bind async helper.</returns>
            public Task QueueBindAsync(string queue, string exchange, string routingKey, IReadOnlyDictionary<string, object?>? arguments, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }

            /// <summary>
            /// Confirms the basic qos async behavior.
            /// </summary>
            /// <returns>The value returned by the basic qos async helper.</returns>
            /// <summary>
            /// Confirms the basic qos async behavior.
            /// </summary>
            /// <param name="prefetchSize">The prefetch size used by this test scenario.</param>
            /// <param name="prefetchCount">The prefetch count used by this test scenario.</param>
            /// <param name="global">The global used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the basic qos async helper.</returns>
            public Task BasicQosAsync(uint prefetchSize, ushort prefetchCount, bool global, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = prefetchSize;
                _ = prefetchCount;
                _ = global;
                return Task.CompletedTask;
            }

            /// <summary>
            /// Confirms the basic consume async behavior.
            /// </summary>
            /// <returns>The value returned by the basic consume async helper.</returns>
            /// <summary>
            /// Confirms the basic consume async behavior.
            /// </summary>
            /// <param name="queue">The queue used by this test scenario.</param>
            /// <param name="autoAck">The auto ack used by this test scenario.</param>
            /// <param name="consumer">The consumer used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the basic consume async helper.</returns>
            public Task<string> BasicConsumeAsync(string queue, bool autoAck, IAsyncBasicConsumer consumer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = queue;
                _ = autoAck;
                _ = consumer;
                return Task.FromResult($"ctag-{id}");
            }

            /// <summary>
            /// Confirms the basic cancel async behavior.
            /// </summary>
            /// <returns>The value returned by the basic cancel async helper.</returns>
            /// <summary>
            /// Confirms the basic cancel async behavior.
            /// </summary>
            /// <param name="consumerTag">The consumer tag used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the basic cancel async helper.</returns>
            public Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = consumerTag;
                return Task.CompletedTask;
            }

            /// <summary>
            /// Confirms the basic ack async behavior.
            /// </summary>
            /// <returns>The value returned by the basic ack async helper.</returns>
            /// <summary>
            /// Confirms the basic ack async behavior.
            /// </summary>
            /// <param name="deliveryTag">The delivery tag used by this test scenario.</param>
            /// <param name="multiple">The multiple used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the basic ack async helper.</returns>
            public ValueTask BasicAckAsync(ulong deliveryTag, bool multiple, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = deliveryTag;
                _ = multiple;
                return ValueTask.CompletedTask;
            }

            /// <summary>
            /// Confirms the basic nack async behavior.
            /// </summary>
            /// <returns>The value returned by the basic nack async helper.</returns>
            /// <summary>
            /// Confirms the basic nack async behavior.
            /// </summary>
            /// <param name="deliveryTag">The delivery tag used by this test scenario.</param>
            /// <param name="multiple">The multiple used by this test scenario.</param>
            /// <param name="requeue">The requeue used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the basic nack async helper.</returns>
            public ValueTask BasicNackAsync(ulong deliveryTag, bool multiple, bool requeue, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = deliveryTag;
                _ = multiple;
                _ = requeue;
                return ValueTask.CompletedTask;
            }

            /// <summary>
            /// Confirms the basic publish async behavior.
            /// </summary>
            /// <returns>The value returned by the basic publish async helper.</returns>
            /// <summary>
            /// Confirms the basic publish async behavior.
            /// </summary>
            /// <param name="exchange">The exchange used by this test scenario.</param>
            /// <param name="routingKey">The routing key used by this test scenario.</param>
            /// <param name="mandatory">The mandatory used by this test scenario.</param>
            /// <param name="basicProperties">The basic properties used by this test scenario.</param>
            /// <param name="body">The body used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the basic publish async helper.</returns>
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
            /// Confirms the dispose async behavior.
            /// </summary>
            /// <returns>The value returned by the dispose async helper.</returns>
            /// <summary>
            /// Confirms the dispose async behavior.
            /// </summary>
            /// <returns>The value returned by the dispose async helper.</returns>
            public ValueTask DisposeAsync()
            {
                _ = id;
                return ValueTask.CompletedTask;
            }
        }
    }
}
