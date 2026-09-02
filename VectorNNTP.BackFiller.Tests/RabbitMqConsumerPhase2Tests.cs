// <copyright file="RabbitMqConsumerPhase2Tests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Behavior and contract tests for rabbit mq consumer phase2.

using System.Reflection;
using System.Security.Cryptography;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.ControlPlane;
using VectorNNTP.Backfiller.Runtime.Accounts;
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

            await session.StopAsync(CancellationToken.None, cancelAdmittedWork: true).ConfigureAwait(false);
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

            await session.StopAsync(CancellationToken.None, cancelAdmittedWork: true).ConfigureAwait(false);
            await session.DisposeAsync().ConfigureAwait(false);
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the Delivery_HandoffRetainsOwnedPayloadAfterSourceBufferMutation scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task Delivery_HandoffRetainsOwnedPayloadAfterSourceBufferMutation()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: null, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            RecordingDeliverySink sink = new();

            RabbitMqBackboneConsumerSession session = new(
                CreateIdentity("Giganews", connectionNumber: 7, connectionLimit: 16),
                manager,
                topologyInitializer,
                sink,
                NullLogger<RabbitMqBackboneConsumerSession>.Instance,
                prefetchCount: null);

            await session.StartAsync(CancellationToken.None).ConfigureAwait(false);

            TrackingChannel consumerChannel = connector.RequireLastConnection().Channels.Single(static channel => channel.ConsumeCallCount == 1);
            byte[] sourcePayload = [0x7B, 0x22, 0x61, 0x22, 0x3A, 0x31, 0x7D];
            byte[] expectedPayload = sourcePayload.ToArray();
            string expectedSha256 = Convert.ToHexString(SHA256.HashData(expectedPayload));

            await consumerChannel.DeliverAsync(
                deliveryTag: 9001,
                redelivered: false,
                exchange: "grabbers.giganews",
                routingKey: "grabbers.giganews",
                payload: sourcePayload,
                cancellationToken: CancellationToken.None).ConfigureAwait(false);

            for (int i = 0; i < sourcePayload.Length; i++)
            {
                sourcePayload[i] ^= 0xFF;
            }

            RabbitMqArticleDelivery delivery = Assert.Single(sink.Deliveries);
            byte[] deliveryPayload = delivery.Payload.ToArray();
            string actualSha256 = Convert.ToHexString(SHA256.HashData(deliveryPayload));

            Assert.Equal(expectedPayload, deliveryPayload);
            Assert.Equal(expectedSha256, actualSha256);

            await session.StopAsync(CancellationToken.None, cancelAdmittedWork: true).ConfigureAwait(false);
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

            await first.StopAsync(CancellationToken.None, cancelAdmittedWork: true).ConfigureAwait(false);
            await second.StopAsync(CancellationToken.None, cancelAdmittedWork: true).ConfigureAwait(false);
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

            firstConnection.RaiseConnectionShutdown();

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

            await session.StopAsync(CancellationToken.None, cancelAdmittedWork: true).ConfigureAwait(false);
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

            await session.StopAsync(CancellationToken.None, cancelAdmittedWork: true).ConfigureAwait(false);
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

            await session.StopAsync(CancellationToken.None, cancelAdmittedWork: true).ConfigureAwait(false);
            int consumeCountAfterStop = channel.ConsumeCallCount;

            await session.HandleConnectionReplacedAsync(new RabbitMqConnectionReplacedEventArgs(manager.ConnectionGeneration + 1, IsReplacement: true), CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(consumeCountAfterStop, channel.ConsumeCallCount);
            Assert.True(channel.CancelCallCount >= 1);
            Assert.True(channel.Disposed);

            await session.DisposeAsync().ConfigureAwait(false);
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the StopAsync_WhenInFlightDeliveryExists_WaitsForSettlementBeforeChannelDispose scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task StopAsync_WhenInFlightDeliveryExists_WaitsForSettlementBeforeChannelDispose()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: null, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            RecordingDeliverySink sink = new();

            RabbitMqBackboneConsumerSession session = new(
                CreateIdentity("Giganews", connectionNumber: 9, connectionLimit: 10),
                manager,
                topologyInitializer,
                sink,
                NullLogger<RabbitMqBackboneConsumerSession>.Instance,
                prefetchCount: null);

            await session.StartAsync(CancellationToken.None).ConfigureAwait(false);
            TrackingChannel channel = connector.RequireLastConnection().Channels.Single(static c => c.ConsumeCallCount == 1);

            await channel.DeliverAsync(501UL, redelivered: false, exchange: "grabbers.giganews", routingKey: "grabbers.giganews", payload: new byte[] { 0x10 }, cancellationToken: CancellationToken.None).ConfigureAwait(false);
            RabbitMqArticleDelivery admitted = Assert.Single(sink.Deliveries);

            Task stopTask = session.StopAsync(CancellationToken.None, cancelAdmittedWork: false);

            bool waitingForDrain = await WaitForAsync(() => channel.CancelCallCount == 1 && !stopTask.IsCompleted, TimeSpan.FromSeconds(2)).ConfigureAwait(false);
            Assert.True(waitingForDrain);
            Assert.False(channel.Disposed);

            await admitted.Settlement.AckAsync(CancellationToken.None).ConfigureAwait(false);
            await stopTask.ConfigureAwait(false);

            Assert.True(channel.Disposed);
            Assert.Equal(1, channel.CancelCallCount);
            Assert.Equal(1, channel.AckCallCount);
            Assert.True(channel.OperationLog.IndexOf("ack") >= 0);
            Assert.True(channel.OperationLog.IndexOf("dispose") > channel.OperationLog.IndexOf("ack"));

            await session.DisposeAsync().ConfigureAwait(false);
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the StopAsync_WhenRetiring_RejectsNewDeliveryAdmission scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task StopAsync_WhenRetiring_RejectsNewDeliveryAdmission()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: null, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            RecordingDeliverySink sink = new();

            RabbitMqBackboneConsumerSession session = new(
                CreateIdentity("Giganews", connectionNumber: 10, connectionLimit: 10),
                manager,
                topologyInitializer,
                sink,
                NullLogger<RabbitMqBackboneConsumerSession>.Instance,
                prefetchCount: null);

            await session.StartAsync(CancellationToken.None).ConfigureAwait(false);
            TrackingChannel channel = connector.RequireLastConnection().Channels.Single(static c => c.ConsumeCallCount == 1);

            await channel.DeliverAsync(601UL, redelivered: false, exchange: "grabbers.giganews", routingKey: "grabbers.giganews", payload: new byte[] { 0x11 }, cancellationToken: CancellationToken.None).ConfigureAwait(false);
            RabbitMqArticleDelivery first = Assert.Single(sink.Deliveries);

            Task stopTask = session.StopAsync(CancellationToken.None, cancelAdmittedWork: false);
            bool retiring = await WaitForAsync(() => channel.CancelCallCount == 1, TimeSpan.FromSeconds(2)).ConfigureAwait(false);
            Assert.True(retiring);

            await channel.DeliverAsync(602UL, redelivered: false, exchange: "grabbers.giganews", routingKey: "grabbers.giganews", payload: new byte[] { 0x12 }, cancellationToken: CancellationToken.None).ConfigureAwait(false);
            Assert.Single(sink.Deliveries);

            await first.Settlement.AckAsync(CancellationToken.None).ConfigureAwait(false);
            await stopTask.ConfigureAwait(false);

            await session.DisposeAsync().ConfigureAwait(false);
            await manager.DisposeAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Verifies the CreateIdentity scenario and expected contract.
        /// </summary>
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

        /// <summary>
        /// Verifies the CreateRuntimeOptions scenario and expected contract.
        /// </summary>
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
        /// <summary>
        /// Verifies the ReconcileSessions_WhenCapacityIncreases_RetainsExistingSessionAndAddsOnlyDeltaChannels scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task ReconcileSessions_WhenCapacityIncreases_RetainsExistingSessionAndAddsOnlyDeltaChannels()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            MutableAccountSnapshotProvider snapshotProvider = new(serverId: 12);
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 4, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            TrackingSessionFactory sessionFactory = new(manager, topologyInitializer);
            RabbitMqConsumerService service = new(runtimeOptions, snapshotProvider.Provider, manager, sessionFactory, shutdownCoordinator, NullLogger<RabbitMqConsumerService>.Instance);

            Guid accountId = Guid.NewGuid();
            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 1)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(1, service.ActiveSessionCount);
            Assert.Equal(1, manager.ConnectionGeneration);
            Assert.Equal(1, connector.ConnectCallCount);

            TrackingSession first = sessionFactory.RequireSession($"{accountId:N}:1");
            Assert.Equal(1, first.StartCallCount);
            long firstGeneration = first.ActiveConnectionGeneration;

            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 2)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(2, service.ActiveSessionCount);
            Assert.Equal(1, manager.ConnectionGeneration);
            Assert.Equal(1, connector.ConnectCallCount);

            TrackingSession firstAfterScale = sessionFactory.RequireSession($"{accountId:N}:1");
            TrackingSession second = sessionFactory.RequireSession($"{accountId:N}:2");

            Assert.Same(first, firstAfterScale);
            Assert.Equal(1, firstAfterScale.StartCallCount);
            Assert.Equal(0, firstAfterScale.StopCallCount);
            Assert.False(firstAfterScale.DisposeCalled);
            Assert.Equal(firstGeneration, firstAfterScale.ActiveConnectionGeneration);

            Assert.Equal(1, second.StartCallCount);
            Assert.Equal(0, second.StopCallCount);
            Assert.False(second.DisposeCalled);
            Assert.NotSame(firstAfterScale, second);
            Assert.Equal(1, connector.RequireLastConnection().Channels.Count(static channel => channel.ConsumeCallCount == 0));

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            service.Dispose();
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the ReconcileSessions_WhenCapacityChanges_RetainsDeltasAndKeepsConnectionGenerationStable scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task ReconcileSessions_WhenCapacityChanges_RetainsDeltasAndKeepsConnectionGenerationStable()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            MutableAccountSnapshotProvider snapshotProvider = new(serverId: 12);
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 4, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            TrackingSessionFactory sessionFactory = new(manager, topologyInitializer);
            RabbitMqConsumerService service = new(runtimeOptions, snapshotProvider.Provider, manager, sessionFactory, shutdownCoordinator, NullLogger<RabbitMqConsumerService>.Instance);

            Guid accountId = Guid.NewGuid();
            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 2)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            TrackingSession session1 = sessionFactory.RequireSession($"{accountId:N}:1");
            TrackingSession session2 = sessionFactory.RequireSession($"{accountId:N}:2");

            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 3)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            TrackingSession session3 = sessionFactory.RequireSession($"{accountId:N}:3");
            Assert.Same(session1, sessionFactory.RequireSession($"{accountId:N}:1"));
            Assert.Same(session2, sessionFactory.RequireSession($"{accountId:N}:2"));
            Assert.Equal(1, session1.StartCallCount);
            Assert.Equal(1, session2.StartCallCount);
            Assert.Equal(1, session3.StartCallCount);
            Assert.Equal(1, manager.ConnectionGeneration);
            Assert.Equal(1, connector.ConnectCallCount);

            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 2)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(2, service.ActiveSessionCount);
            Assert.Equal(1, manager.ConnectionGeneration);
            Assert.Equal(1, connector.ConnectCallCount);

            TrackingSession retired = sessionFactory.RequireSession($"{accountId:N}:3");
            Assert.True(retired.DisposeCalled);
            Assert.Equal(1, retired.StopCallCount);
            Assert.False(retired.LastCancelAdmittedWork);
            Assert.False(session1.DisposeCalled);
            Assert.False(session2.DisposeCalled);

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            service.Dispose();
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the CapacityChanges_DoNotCreateRabbitMqTcpConnections scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task CapacityChanges_DoNotCreateRabbitMqTcpConnections()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            MutableAccountSnapshotProvider snapshotProvider = new(serverId: 12);
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 4, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            TrackingSessionFactory sessionFactory = new(manager, topologyInitializer);
            RabbitMqConsumerService service = new(runtimeOptions, snapshotProvider.Provider, manager, sessionFactory, shutdownCoordinator, NullLogger<RabbitMqConsumerService>.Instance);

            Guid accountId = Guid.NewGuid();
            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 2)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 3)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 1)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 3)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(1, connector.ConnectCallCount);

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            service.Dispose();
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the CapacityChanges_DoNotChangeConnectionGeneration scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task CapacityChanges_DoNotChangeConnectionGeneration()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            MutableAccountSnapshotProvider snapshotProvider = new(serverId: 12);
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 4, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            TrackingSessionFactory sessionFactory = new(manager, topologyInitializer);
            RabbitMqConsumerService service = new(runtimeOptions, snapshotProvider.Provider, manager, sessionFactory, shutdownCoordinator, NullLogger<RabbitMqConsumerService>.Instance);

            Guid accountId = Guid.NewGuid();
            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 2)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);
            long initialGeneration = manager.ConnectionGeneration;

            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 3)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 1)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 3)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(initialGeneration, manager.ConnectionGeneration);

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            service.Dispose();
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the ReconcileSessions_WhenUnrelatedAccountCapacityMetadataChanges_DoesNotReplaceExistingSession scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task ReconcileSessions_WhenUnrelatedAccountCapacityMetadataChanges_DoesNotReplaceExistingSession()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            MutableAccountSnapshotProvider snapshotProvider = new(serverId: 12);
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 2, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            TrackingSessionFactory sessionFactory = new(manager, topologyInitializer);
            RabbitMqConsumerService service = new(runtimeOptions, snapshotProvider.Provider, manager, sessionFactory, shutdownCoordinator, NullLogger<RabbitMqConsumerService>.Instance);

            Guid accountId = Guid.NewGuid();
            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 1)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            TrackingSession session = sessionFactory.RequireSession($"{accountId:N}:1");
            long firstGeneration = manager.ConnectionGeneration;

            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 2)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            TrackingSession sessionAfterReconcile = sessionFactory.RequireSession($"{accountId:N}:1");
            Assert.Same(session, sessionAfterReconcile);
            Assert.Equal(1, sessionAfterReconcile.StartCallCount);
            Assert.Equal(0, sessionAfterReconcile.StopCallCount);
            Assert.False(sessionAfterReconcile.DisposeCalled);
            Assert.Equal(firstGeneration, manager.ConnectionGeneration);
            Assert.Equal(1, connector.ConnectCallCount);

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            service.Dispose();
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the RetireCapacityAsync_WhenReducingCapacity_RetiresOnlySessionsAboveBoundary scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task RetireCapacityAsync_WhenReducingCapacity_RetiresOnlySessionsAboveBoundary()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            MutableAccountSnapshotProvider snapshotProvider = new(serverId: 12);
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 2, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            TrackingSessionFactory sessionFactory = new(manager, topologyInitializer);
            RabbitMqConsumerService service = new(runtimeOptions, snapshotProvider.Provider, manager, sessionFactory, shutdownCoordinator, NullLogger<RabbitMqConsumerService>.Instance);

            Guid accountId = Guid.NewGuid();
            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 3)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            TrackingSession first = sessionFactory.RequireSession($"{accountId:N}:1");
            TrackingSession second = sessionFactory.RequireSession($"{accountId:N}:2");
            TrackingSession third = sessionFactory.RequireSession($"{accountId:N}:3");

            await service.RetireCapacityAsync(accountId, retainConnectionCount: 1, CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(1, service.ActiveSessionCount);
            Assert.False(first.DisposeCalled);
            Assert.Equal(0, first.StopCallCount);

            Assert.True(second.DisposeCalled);
            Assert.Equal(1, second.StopCallCount);
            Assert.False(second.LastCancelAdmittedWork);

            Assert.True(third.DisposeCalled);
            Assert.Equal(1, third.StopCallCount);
            Assert.False(third.LastCancelAdmittedWork);

            Assert.Equal(1, manager.ConnectionGeneration);
            Assert.Equal(1, connector.ConnectCallCount);

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            service.Dispose();
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the RetireCapacityAsync_WhenAdmittedDeliveryIsInFlight_DrainsSettlementBeforeChannelDispose scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task RetireCapacityAsync_WhenAdmittedDeliveryIsInFlight_DrainsSettlementBeforeChannelDispose()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            MutableAccountSnapshotProvider snapshotProvider = new(serverId: 12);
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 2, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            ObservingRealSessionFactory sessionFactory = new(manager, topologyInitializer);
            RabbitMqConsumerService service = new(runtimeOptions, snapshotProvider.Provider, manager, sessionFactory, shutdownCoordinator, NullLogger<RabbitMqConsumerService>.Instance);

            Guid accountId = Guid.NewGuid();
            string sessionKey1 = $"{accountId:N}:1";
            string sessionKey2 = $"{accountId:N}:2";
            using CancellationTokenSource timeoutCts = new(TimeSpan.FromSeconds(10));
            CancellationToken timeoutToken = timeoutCts.Token;

            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 2)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(timeoutToken).ConfigureAwait(false);

            Assert.Equal(2, service.ActiveSessionCount);
            Assert.Equal(1, connector.ConnectCallCount);
            long initialGeneration = manager.ConnectionGeneration;

            ObservedSessionRuntime firstSession = sessionFactory.RequireLatestSession(sessionKey1);
            ObservedSessionRuntime secondSession = sessionFactory.RequireLatestSession(sessionKey2);
            Assert.True(firstSession.IsRunning);
            Assert.True(secondSession.IsRunning);

            TrackingConnection connection = connector.RequireLastConnection();
            List<TrackingChannel> consumerChannels = [.. connection.Channels.Where(static channel => channel.ConsumeCallCount == 1)];
            Assert.Equal(2, consumerChannels.Count);

            TrackingChannel firstChannel = consumerChannels[0];
            TrackingChannel secondChannel = consumerChannels[1];
            Assert.True(firstChannel.IsConsumerCurrentlyActive);
            Assert.True(secondChannel.IsConsumerCurrentlyActive);

            await secondChannel.DeliverAsync(1101UL, redelivered: false, exchange: "grabbers.giganews", routingKey: "grabbers.giganews", payload: new byte[] { 0x41 }, cancellationToken: timeoutToken).ConfigureAwait(false);
            RabbitMqArticleDelivery admitted = await sessionFactory.WaitForDeliveryAsync(sessionKey2, timeoutToken).ConfigureAwait(false);

            Assert.Equal(sessionKey2, admitted.ConsumerIdentity);
            Assert.Single(sessionFactory.GetDeliveriesForSession(sessionKey2));

            Task retirementTask = service.RetireCapacityAsync(accountId, retainConnectionCount: 1, timeoutToken);
            await secondChannel.CancelObserved.WaitAsync(timeoutToken).ConfigureAwait(false);

            Assert.False(retirementTask.IsCompleted);

            await secondChannel.DeliverAsync(1102UL, redelivered: false, exchange: "grabbers.giganews", routingKey: "grabbers.giganews", payload: new byte[] { 0x42 }, cancellationToken: timeoutToken).ConfigureAwait(false);
            Assert.Single(sessionFactory.GetDeliveriesForSession(sessionKey2));
            Assert.False(retirementTask.IsCompleted);

            await admitted.Settlement.AckAsync(timeoutToken).ConfigureAwait(false);
            await secondChannel.DisposedObserved.WaitAsync(timeoutToken).ConfigureAwait(false);

            int cancelIndex = secondChannel.OperationLog.IndexOf("cancel");
            int ackIndex = secondChannel.OperationLog.IndexOf("ack");
            int disposeIndex = secondChannel.OperationLog.IndexOf("dispose");
            Assert.True(cancelIndex >= 0);
            Assert.True(ackIndex > cancelIndex);
            Assert.True(disposeIndex > ackIndex);

            await retirementTask.WaitAsync(timeoutToken).ConfigureAwait(false);

            Assert.Equal(1, service.ActiveSessionCount);
            Assert.True(firstSession.IsRunning);
            Assert.False(secondSession.IsRunning);

            Assert.Equal(0, firstSession.StopCallCount);
            Assert.Equal(0, firstSession.DisposeCallCount);
            Assert.Equal(1, sessionFactory.GetCreatedCount(sessionKey1));
            Assert.Equal(1, sessionFactory.GetCreatedCount(sessionKey2));

            Assert.Equal(1, secondSession.StopCallCount);
            Assert.Equal(1, secondSession.DisposeCallCount);
            Assert.Equal(0, firstChannel.AckCallCount);
            Assert.Equal(1, secondChannel.AckCallCount);
            Assert.Equal(1, connector.ConnectCallCount);
            Assert.Equal(initialGeneration, manager.ConnectionGeneration);

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            service.Dispose();
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the RetireCapacityAsync_WhenShutdownSignaled_CompletesWithoutDeadlock scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task RetireCapacityAsync_WhenShutdownSignaled_CompletesWithoutDeadlock()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            MutableAccountSnapshotProvider snapshotProvider = new(serverId: 12);
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 2, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            TrackingSessionFactory sessionFactory = new(manager, topologyInitializer);
            RabbitMqConsumerService service = new(runtimeOptions, snapshotProvider.Provider, manager, sessionFactory, shutdownCoordinator, NullLogger<RabbitMqConsumerService>.Instance);

            Guid accountId = Guid.NewGuid();
            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 2)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            shutdownCoordinator.SignalForcedShutdown();

            await service.RetireCapacityAsync(accountId, retainConnectionCount: 0, CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(2, service.ActiveSessionCount);

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            service.Dispose();
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the NToNMinusOneThenNPlusOne_WhileRetirementDraining_DoesNotCreateDuplicateLogicalSession scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task NToNMinusOneThenNPlusOne_WhileRetirementDraining_DoesNotCreateDuplicateLogicalSession()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            MutableAccountSnapshotProvider snapshotProvider = new(serverId: 12);
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 2, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            BlockingStopSessionFactory sessionFactory = new(manager, topologyInitializer);
            RabbitMqConsumerService service = new(runtimeOptions, snapshotProvider.Provider, manager, sessionFactory, shutdownCoordinator, NullLogger<RabbitMqConsumerService>.Instance);

            Guid accountId = Guid.NewGuid();
            string sessionKey3 = $"{accountId:N}:3";
            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 3)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            BlockingStopTrackingSession session1 = sessionFactory.RequireLatestSession($"{accountId:N}:1");
            BlockingStopTrackingSession session2 = sessionFactory.RequireLatestSession($"{accountId:N}:2");
            BlockingStopTrackingSession retiringSession3 = sessionFactory.RequireLatestSession(sessionKey3);
            sessionFactory.BlockStopForSession(sessionKey3);

            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 2)).ConfigureAwait(false);
            Task retireTask = service.RetireCapacityAsync(accountId, retainConnectionCount: 2, CancellationToken.None);
            await sessionFactory.WaitForStopStartedAsync(sessionKey3, CancellationToken.None).ConfigureAwait(false);

            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 3)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(1, sessionFactory.GetCreatedCount(sessionKey3));
            Assert.Equal(0, sessionFactory.GetRunningCount(sessionKey3));
            Assert.Equal(2, service.ActiveSessionCount);

            sessionFactory.ReleaseStop(sessionKey3);
            await retireTask.ConfigureAwait(false);

            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            BlockingStopTrackingSession recreatedSession3 = sessionFactory.RequireLatestSession(sessionKey3);
            Assert.NotSame(retiringSession3, recreatedSession3);
            Assert.Equal(2, sessionFactory.GetCreatedCount(sessionKey3));
            Assert.Equal(1, sessionFactory.GetRunningCount(sessionKey3));
            Assert.Same(session1, sessionFactory.RequireLatestSession($"{accountId:N}:1"));
            Assert.Same(session2, sessionFactory.RequireLatestSession($"{accountId:N}:2"));
            Assert.Equal(1, manager.ConnectionGeneration);
            Assert.Equal(1, connector.ConnectCallCount);

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            service.Dispose();
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the CapacityIncreaseBeforeRetirementBegins_DoesNotRetireNowDesiredSession scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task CapacityIncreaseBeforeRetirementBegins_DoesNotRetireNowDesiredSession()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            MutableAccountSnapshotProvider snapshotProvider = new(serverId: 12);
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 2, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            TrackingSessionFactory sessionFactory = new(manager, topologyInitializer);
            RabbitMqConsumerService service = new(runtimeOptions, snapshotProvider.Provider, manager, sessionFactory, shutdownCoordinator, NullLogger<RabbitMqConsumerService>.Instance);

            Guid accountId = Guid.NewGuid();
            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 3)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 2)).ConfigureAwait(false);
            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 3)).ConfigureAwait(false);

            TrackingSession session3 = sessionFactory.RequireSession($"{accountId:N}:3");
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.False(session3.DisposeCalled);
            Assert.Equal(0, session3.StopCallCount);
            Assert.Equal(3, service.ActiveSessionCount);
            Assert.Equal(1, manager.ConnectionGeneration);
            Assert.Equal(1, connector.ConnectCallCount);

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            service.Dispose();
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the RetireCapacityAsync_PreCanceledToken_DoesNotStrandReservation scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task RetireCapacityAsync_PreCanceledToken_DoesNotStrandReservation()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            MutableAccountSnapshotProvider snapshotProvider = new(serverId: 12);
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 2, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            FaultInjectingSessionFactory sessionFactory = new(manager, topologyInitializer);
            RabbitMqConsumerService service = new(runtimeOptions, snapshotProvider.Provider, manager, sessionFactory, shutdownCoordinator, NullLogger<RabbitMqConsumerService>.Instance);

            Guid accountId = Guid.NewGuid();
            string sessionKey2 = $"{accountId:N}:2";
            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 2)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            using CancellationTokenSource cts = new();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await service.RetireCapacityAsync(accountId, retainConnectionCount: 1, cts.Token).ConfigureAwait(false)).ConfigureAwait(false);

            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 2)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(1, sessionFactory.GetCreatedCount(sessionKey2));
            Assert.Equal(2, service.ActiveSessionCount);

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            service.Dispose();
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the RetireCapacityAsync_StopFailure_DoesNotAllowDuplicateLogicalSession scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task RetireCapacityAsync_StopFailure_DoesNotAllowDuplicateLogicalSession()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            MutableAccountSnapshotProvider snapshotProvider = new(serverId: 12);
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 2, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            FaultInjectingSessionFactory sessionFactory = new(manager, topologyInitializer);
            RabbitMqConsumerService service = new(runtimeOptions, snapshotProvider.Provider, manager, sessionFactory, shutdownCoordinator, NullLogger<RabbitMqConsumerService>.Instance);

            Guid accountId = Guid.NewGuid();
            string sessionKey3 = $"{accountId:N}:3";
            sessionFactory.SetFailureMode(sessionKey3, RetirementFailureMode.StopThrowsBeforeStopping);
            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 3)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await service.RetireCapacityAsync(accountId, retainConnectionCount: 2, CancellationToken.None).ConfigureAwait(false)).ConfigureAwait(false);

            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 3)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(1, sessionFactory.GetCreatedCount(sessionKey3));

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            service.Dispose();
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the RetireCapacityAsync_DisposeFailure_DoesNotAllowDuplicateLogicalSession scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task RetireCapacityAsync_DisposeFailure_DoesNotAllowDuplicateLogicalSession()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            MutableAccountSnapshotProvider snapshotProvider = new(serverId: 12);
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 2, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            FaultInjectingSessionFactory sessionFactory = new(manager, topologyInitializer);
            RabbitMqConsumerService service = new(runtimeOptions, snapshotProvider.Provider, manager, sessionFactory, shutdownCoordinator, NullLogger<RabbitMqConsumerService>.Instance);

            Guid accountId = Guid.NewGuid();
            string sessionKey3 = $"{accountId:N}:3";
            sessionFactory.SetFailureMode(sessionKey3, RetirementFailureMode.DisposeThrowsAfterStop);
            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 3)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await service.RetireCapacityAsync(accountId, retainConnectionCount: 2, CancellationToken.None).ConfigureAwait(false)).ConfigureAwait(false);

            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 3)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(1, sessionFactory.GetCreatedCount(sessionKey3));

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            service.Dispose();
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the RetireCapacityAsync_CancellationDuringDrain_DoesNotStrandReservation scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task RetireCapacityAsync_CancellationDuringDrain_DoesNotStrandReservation()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            MutableAccountSnapshotProvider snapshotProvider = new(serverId: 12);
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 2, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            BlockingStopSessionFactory sessionFactory = new(manager, topologyInitializer);
            RabbitMqConsumerService service = new(runtimeOptions, snapshotProvider.Provider, manager, sessionFactory, shutdownCoordinator, NullLogger<RabbitMqConsumerService>.Instance);

            Guid accountId = Guid.NewGuid();
            string sessionKey2 = $"{accountId:N}:2";
            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 2)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            using CancellationTokenSource cts = new();
            sessionFactory.BlockStopForSession(sessionKey2);
            Task retireTask = service.RetireCapacityAsync(accountId, retainConnectionCount: 1, cts.Token);
            await sessionFactory.WaitForStopStartedAsync(sessionKey2, CancellationToken.None).ConfigureAwait(false);
            cts.Cancel();
            sessionFactory.ReleaseStop(sessionKey2);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await retireTask.ConfigureAwait(false)).ConfigureAwait(false);

            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 2)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(1, sessionFactory.GetCreatedCount(sessionKey2));

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            service.Dispose();
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the ConcurrentRetireCalls_DoNotDoubleDispose scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task ConcurrentRetireCalls_DoNotDoubleDispose()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            MutableAccountSnapshotProvider snapshotProvider = new(serverId: 12);
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 2, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            BlockingStopSessionFactory sessionFactory = new(manager, topologyInitializer);
            RabbitMqConsumerService service = new(runtimeOptions, snapshotProvider.Provider, manager, sessionFactory, shutdownCoordinator, NullLogger<RabbitMqConsumerService>.Instance);

            Guid accountId = Guid.NewGuid();
            string sessionKey2 = $"{accountId:N}:2";
            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 2)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            BlockingStopTrackingSession session2 = sessionFactory.RequireLatestSession(sessionKey2);
            sessionFactory.BlockStopForSession(sessionKey2);

            Task retireA = service.RetireCapacityAsync(accountId, retainConnectionCount: 1, CancellationToken.None);
            await sessionFactory.WaitForStopStartedAsync(sessionKey2, CancellationToken.None).ConfigureAwait(false);
            Task retireB = service.RetireCapacityAsync(accountId, retainConnectionCount: 1, CancellationToken.None);
            sessionFactory.ReleaseStop(sessionKey2);

            await retireA.ConfigureAwait(false);
            await retireB.ConfigureAwait(false);

            Assert.Equal(1, session2.StopCallCount);
            Assert.True(session2.DisposeCalled);

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            service.Dispose();
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the ConcurrentReconcileAndRetire_DoNotCreateDuplicateLogicalSession scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task ConcurrentReconcileAndRetire_DoNotCreateDuplicateLogicalSession()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            MutableAccountSnapshotProvider snapshotProvider = new(serverId: 12);
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 2, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            BlockingStopSessionFactory sessionFactory = new(manager, topologyInitializer);
            RabbitMqConsumerService service = new(runtimeOptions, snapshotProvider.Provider, manager, sessionFactory, shutdownCoordinator, NullLogger<RabbitMqConsumerService>.Instance);

            Guid accountId = Guid.NewGuid();
            string sessionKey3 = $"{accountId:N}:3";
            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 3)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            sessionFactory.BlockStopForSession(sessionKey3);
            Task retireTask = service.RetireCapacityAsync(accountId, retainConnectionCount: 2, CancellationToken.None);
            await sessionFactory.WaitForStopStartedAsync(sessionKey3, CancellationToken.None).ConfigureAwait(false);

            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 4)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(1, sessionFactory.GetCreatedCount(sessionKey3));
            Assert.Equal(0, sessionFactory.GetRunningCount(sessionKey3));

            sessionFactory.ReleaseStop(sessionKey3);
            await retireTask.ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(2, sessionFactory.GetCreatedCount(sessionKey3));

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            service.Dispose();
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the ReconcileSessions_WhenStartupCapacityIsUnavailable_DoesNotStartConsumerUntilCapacityBecomesAvailable scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task ReconcileSessions_WhenStartupCapacityIsUnavailable_DoesNotStartConsumerUntilCapacityBecomesAvailable()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            MutableAccountSnapshotProvider snapshotProvider = new(serverId: 12);
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 2, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            TrackingSessionFactory sessionFactory = new(manager, topologyInitializer);
            CapacityStateBackboneCapacityProvider controlPlane = new();
            RabbitMqConsumerService service = new(runtimeOptions, snapshotProvider.Provider, manager, sessionFactory, shutdownCoordinator, controlPlane, NullLogger<RabbitMqConsumerService>.Instance);

            Guid accountId = Guid.NewGuid();
            string sessionKey1 = $"{accountId:N}:1";
            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 1)).ConfigureAwait(false);

            controlPlane.SetBackboneCapacity("Giganews", hasCapacity: false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(0, service.ActiveSessionCount);
            Assert.Throws<InvalidOperationException>(() => sessionFactory.RequireSession(sessionKey1));

            controlPlane.SetBackboneCapacity("Giganews", hasCapacity: true);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            TrackingSession started = sessionFactory.RequireSession(sessionKey1);
            Assert.Equal(1, started.StartCallCount);
            Assert.True(started.IsRunning);

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            service.Dispose();
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the ReconcileSessions_WhenRuntimeCapacityDropsToZero_StopsNewAdmissionAndResumesAfterRecovery scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task ReconcileSessions_WhenRuntimeCapacityDropsToZero_StopsNewAdmissionAndResumesAfterRecovery()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            MutableAccountSnapshotProvider snapshotProvider = new(serverId: 12);
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: null, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            ObservingRealSessionFactory sessionFactory = new(manager, topologyInitializer);
            CapacityStateBackboneCapacityProvider capacityProvider = new();
            RabbitMqConsumerService service = new(runtimeOptions, snapshotProvider.Provider, manager, sessionFactory, shutdownCoordinator, capacityProvider, NullLogger<RabbitMqConsumerService>.Instance);

            Guid accountId = Guid.NewGuid();
            string sessionKey1 = $"{accountId:N}:1";
            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 1)).ConfigureAwait(false);

            capacityProvider.SetBackboneCapacity("Giganews", hasCapacity: true);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            ObservedSessionRuntime firstRuntime = sessionFactory.RequireLatestSession(sessionKey1);
            TrackingChannel firstChannel = connector.RequireLastConnection().Channels.Single(static channel => channel.ConsumeCallCount == 1);

            await firstChannel.DeliverAsync(3001UL, redelivered: false, exchange: "grabbers.giganews", routingKey: "grabbers.giganews", payload: new byte[] { 0x01 }, cancellationToken: CancellationToken.None).ConfigureAwait(false);
            RabbitMqArticleDelivery admitted = await sessionFactory.WaitForDeliveryAsync(sessionKey1, CancellationToken.None).ConfigureAwait(false);

            capacityProvider.SetBackboneCapacity("Giganews", hasCapacity: false);
            Task retireReconcile = service.ReconcileOnceAsync(CancellationToken.None);
            await firstChannel.CancelObserved.ConfigureAwait(false);

            await admitted.Settlement.AckAsync(CancellationToken.None).ConfigureAwait(false);
            await retireReconcile.ConfigureAwait(false);

            Assert.Equal(1, firstRuntime.StopCallCount);
            Assert.False(firstRuntime.LastCancelAdmittedWork ?? true);
            Assert.True(firstChannel.CancelCallCount >= 1);
            Assert.False(firstChannel.IsConsumerCurrentlyActive);
            Assert.Single(firstRuntime.Deliveries);
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await firstChannel.DeliverAsync(3002UL, redelivered: false, exchange: "grabbers.giganews", routingKey: "grabbers.giganews", payload: new byte[] { 0x02 }, cancellationToken: CancellationToken.None).ConfigureAwait(false)).ConfigureAwait(false);
            Assert.Single(firstRuntime.Deliveries);

            capacityProvider.SetBackboneCapacity("Giganews", hasCapacity: true);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(2, sessionFactory.GetCreatedCount(sessionKey1));
            ObservedSessionRuntime recoveredRuntime = sessionFactory.RequireLatestSession(sessionKey1);
            Assert.True(recoveredRuntime.IsRunning);

            TrackingChannel secondChannel = connector.RequireLastConnection().Channels.Where(static channel => channel.ConsumeCallCount == 1).Last();
            await secondChannel.DeliverAsync(3003UL, redelivered: false, exchange: "grabbers.giganews", routingKey: "grabbers.giganews", payload: new byte[] { 0x03 }, cancellationToken: CancellationToken.None).ConfigureAwait(false);
            RabbitMqArticleDelivery resumed = await recoveredRuntime.WaitForDeliveryAsync(CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(3003UL, resumed.DeliveryTag);

            await resumed.Settlement.AckAsync(CancellationToken.None).ConfigureAwait(false);

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            service.Dispose();
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the ActiveSessionCount_IsConsistentlySynchronized scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task ActiveSessionCount_IsConsistentlySynchronized()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            MutableAccountSnapshotProvider snapshotProvider = new(serverId: 12);
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 2, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            BlockingStopSessionFactory sessionFactory = new(manager, topologyInitializer);
            RabbitMqConsumerService service = new(runtimeOptions, snapshotProvider.Provider, manager, sessionFactory, shutdownCoordinator, NullLogger<RabbitMqConsumerService>.Instance);

            Guid accountId = Guid.NewGuid();
            string sessionKey3 = $"{accountId:N}:3";
            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 3)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            sessionFactory.BlockStopForSession(sessionKey3);
            Task retireTask = service.RetireCapacityAsync(accountId, retainConnectionCount: 2, CancellationToken.None);
            await sessionFactory.WaitForStopStartedAsync(sessionKey3, CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(2, service.ActiveSessionCount);

            sessionFactory.ReleaseStop(sessionKey3);
            await retireTask.ConfigureAwait(false);

            await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            service.Dispose();
            await manager.DisposeAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the ShutdownWhileRetirementDraining_CompletesWithoutDeadlock scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task ShutdownWhileRetirementDraining_CompletesWithoutDeadlock()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            TrackingBrokerConnector connector = new();
            MutableAccountSnapshotProvider snapshotProvider = new(serverId: 12);
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(prefetchCount: 2, maxConsecutiveRecoveryFailures: 1);
            RabbitMqConnectionManager manager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            RabbitMqTopologyInitializer topologyInitializer = new(manager, NullLogger<RabbitMqTopologyInitializer>.Instance);
            BlockingStopSessionFactory sessionFactory = new(manager, topologyInitializer);
            RabbitMqConsumerService service = new(runtimeOptions, snapshotProvider.Provider, manager, sessionFactory, shutdownCoordinator, NullLogger<RabbitMqConsumerService>.Instance);

            Guid accountId = Guid.NewGuid();
            string sessionKey2 = $"{accountId:N}:2";
            await snapshotProvider.SetSingleAccountAsync(CreateAccountSnapshot(accountId, maxConnections: 2)).ConfigureAwait(false);
            await service.ReconcileOnceAsync(CancellationToken.None).ConfigureAwait(false);

            sessionFactory.BlockStopForSession(sessionKey2);
            Task retireTask = service.RetireCapacityAsync(accountId, retainConnectionCount: 1, CancellationToken.None);
            await sessionFactory.WaitForStopStartedAsync(sessionKey2, CancellationToken.None).ConfigureAwait(false);

            shutdownCoordinator.SignalForcedShutdown();
            Task stopTask = service.StopAsync(CancellationToken.None);
            sessionFactory.ReleaseStop(sessionKey2);

            await retireTask.ConfigureAwait(false);
            await stopTask.ConfigureAwait(false);

            service.Dispose();
            await manager.DisposeAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Verifies the CreateAccountSnapshot scenario and expected contract.
        /// </summary>
        private static NntpAccountSnapshot CreateAccountSnapshot(Guid entryId, byte maxConnections)
        {
            return new NntpAccountSnapshot(
                EntryId: entryId,
                Backbone: "Giganews",
                Hostname: "provider.local",
                KeepAliveSeconds: 120,
                MaxConnections: maxConnections,
                Password: "pass",
                Port: 119,
                ServerId: 12,
                Username: "optgiga01",
                UseSsl: false);
        }

        /// <summary>
        /// Verifies the WaitForAsync scenario and expected contract.
        /// </summary>
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
        /// Documents the MutableAccountSnapshotProvider test type and its protected contract.
        /// </summary>
        private sealed class MutableAccountSnapshotProvider
        {
            /// <summary>
            /// Stores the _gate fixture value used by these tests.
            /// </summary>
            private readonly object _gate = new();
            /// <summary>
            /// Stores the _accounts fixture value used by these tests.
            /// </summary>
            private List<NntpAccountSnapshot> _accounts = [];

            /// <summary>
            /// Verifies the MutableAccountSnapshotProvider scenario and expected contract.
            /// </summary>
            internal MutableAccountSnapshotProvider(byte serverId)
            {
                Provider = new MySqlNntpAccountSnapshotProvider(
                    serverId,
                    NullLogger<MySqlNntpAccountSnapshotProvider>.Instance,
                    QueryAccountsAsync);
            }

            /// <summary>
            /// Stores the Provider value used by this test fixture.
            /// </summary>
            internal MySqlNntpAccountSnapshotProvider Provider { get; }

            /// <summary>
            /// Verifies the SetSingleAccountAsync scenario and expected contract.
            /// </summary>
            internal async Task SetSingleAccountAsync(NntpAccountSnapshot account)
            {
                ArgumentNullException.ThrowIfNull(account);

                lock (_gate)
                {
                    _accounts = [account];
                }

                _ = await Provider.RefreshSnapshotAsync(CancellationToken.None).ConfigureAwait(false);
            }

            /// <summary>
            /// Verifies the QueryAccountsAsync scenario and expected contract.
            /// </summary>
            private Task<List<NntpAccountSnapshot>> QueryAccountsAsync(CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();

                lock (_gate)
                {
                    return Task.FromResult(new List<NntpAccountSnapshot>(_accounts));
                }
            }
        }

        /// <summary>
        /// Documents the TrackingSessionFactory test type and its protected contract.
        /// </summary>
        private sealed class TrackingSessionFactory(RabbitMqConnectionManager connectionManager, RabbitMqTopologyInitializer topologyInitializer) : IRabbitMqConsumerSessionFactory
        {
            /// <summary>
            /// Documents the new member and its test-supporting contract.
            /// </summary>
            private readonly Dictionary<string, TrackingSession> _sessions = new(StringComparer.Ordinal);
            /// <summary>
            /// Stores the _connectionManager fixture value used by these tests.
            /// </summary>
            private readonly RabbitMqConnectionManager _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
            /// <summary>
            /// Stores the _topologyInitializer fixture value used by these tests.
            /// </summary>
            private readonly RabbitMqTopologyInitializer _topologyInitializer = topologyInitializer ?? throw new ArgumentNullException(nameof(topologyInitializer));

            /// <summary>
            /// Verifies the CreateSession scenario and expected contract.
            /// </summary>
            public IRabbitMqConsumerSession CreateSession(RabbitMqConsumerSessionIdentity identity, IRabbitMqDeliverySink deliverySink, ushort? prefetchCount)
            {
                ArgumentNullException.ThrowIfNull(identity);
                ArgumentNullException.ThrowIfNull(deliverySink);

                TrackingSession session = new(identity, _connectionManager, _topologyInitializer, deliverySink, prefetchCount);
                _sessions[identity.SessionKey] = session;
                return session;
            }

            /// <summary>
            /// Verifies the RequireSession scenario and expected contract.
            /// </summary>
            internal TrackingSession RequireSession(string sessionKey)
            {
                return _sessions.TryGetValue(sessionKey, out TrackingSession? session)
                    ? session
                    : throw new InvalidOperationException($"Expected tracked RabbitMQ session '{sessionKey}'.");
            }
        }

        /// <summary>
        /// Documents the CapacityStateBackboneCapacityProvider test type and its protected contract.
        /// </summary>
        private sealed class CapacityStateBackboneCapacityProvider : IBackboneUsableCapacityProvider
        {
            /// <summary>
            /// Documents the new member and its test-supporting contract.
            /// </summary>
            private readonly Dictionary<string, bool> _capacityByBackbone = new(StringComparer.OrdinalIgnoreCase);

            /// <summary>
            /// Verifies the SetBackboneCapacity scenario and expected contract.
            /// </summary>
            internal void SetBackboneCapacity(string backbone, bool hasCapacity)
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(backbone);
                _capacityByBackbone[backbone] = hasCapacity;
            }

            /// <summary>
            /// Verifies the HasUsableCapacityForBackbone scenario and expected contract.
            /// </summary>
            public bool HasUsableCapacityForBackbone(string backbone)
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(backbone);
                return _capacityByBackbone.TryGetValue(backbone, out bool hasCapacity) && hasCapacity;
            }
        }

        /// <summary>
        /// Documents the BlockingStopSessionFactory test type and its protected contract.
        /// </summary>
        private sealed class BlockingStopSessionFactory(RabbitMqConnectionManager connectionManager, RabbitMqTopologyInitializer topologyInitializer) : IRabbitMqConsumerSessionFactory
        {
            /// <summary>
            /// Documents the new member and its test-supporting contract.
            /// </summary>
            private readonly Dictionary<string, List<BlockingStopTrackingSession>> _sessionsByKey = new(StringComparer.Ordinal);
            /// <summary>
            /// Documents the new member and its test-supporting contract.
            /// </summary>
            private readonly Dictionary<string, TaskCompletionSource<bool>> _stopBlocks = new(StringComparer.Ordinal);
            /// <summary>
            /// Documents the new member and its test-supporting contract.
            /// </summary>
            private readonly Dictionary<string, TaskCompletionSource<bool>> _stopStarted = new(StringComparer.Ordinal);
            /// <summary>
            /// Stores the _gate fixture value used by these tests.
            /// </summary>
            private readonly object _gate = new();
            /// <summary>
            /// Stores the _connectionManager fixture value used by these tests.
            /// </summary>
            private readonly RabbitMqConnectionManager _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
            /// <summary>
            /// Stores the _topologyInitializer fixture value used by these tests.
            /// </summary>
            private readonly RabbitMqTopologyInitializer _topologyInitializer = topologyInitializer ?? throw new ArgumentNullException(nameof(topologyInitializer));

            /// <summary>
            /// Verifies the CreateSession scenario and expected contract.
            /// </summary>
            public IRabbitMqConsumerSession CreateSession(RabbitMqConsumerSessionIdentity identity, IRabbitMqDeliverySink deliverySink, ushort? prefetchCount)
            {
                ArgumentNullException.ThrowIfNull(identity);
                ArgumentNullException.ThrowIfNull(deliverySink);

                BlockingStopTrackingSession session = new(identity, _connectionManager, _topologyInitializer, deliverySink, prefetchCount, this);

                lock (_gate)
                {
                    if (!_sessionsByKey.TryGetValue(identity.SessionKey, out List<BlockingStopTrackingSession>? sessions))
                    {
                        sessions = [];
                        _sessionsByKey[identity.SessionKey] = sessions;
                    }

                    sessions.Add(session);
                }

                return session;
            }

            /// <summary>
            /// Verifies the RequireLatestSession scenario and expected contract.
            /// </summary>
            internal BlockingStopTrackingSession RequireLatestSession(string sessionKey)
            {
                lock (_gate)
                {
                    if (!_sessionsByKey.TryGetValue(sessionKey, out List<BlockingStopTrackingSession>? sessions) || sessions.Count == 0)
                    {
                        throw new InvalidOperationException($"Expected tracked RabbitMQ session '{sessionKey}'.");
                    }

                    return sessions[^1];
                }
            }

            /// <summary>
            /// Verifies the GetCreatedCount scenario and expected contract.
            /// </summary>
            internal int GetCreatedCount(string sessionKey)
            {
                lock (_gate)
                {
                    return _sessionsByKey.TryGetValue(sessionKey, out List<BlockingStopTrackingSession>? sessions) ? sessions.Count : 0;
                }
            }

            /// <summary>
            /// Verifies the GetRunningCount scenario and expected contract.
            /// </summary>
            internal int GetRunningCount(string sessionKey)
            {
                lock (_gate)
                {
                    if (!_sessionsByKey.TryGetValue(sessionKey, out List<BlockingStopTrackingSession>? sessions))
                    {
                        return 0;
                    }

                    return sessions.Count(static session => session.IsRunning);
                }
            }

            /// <summary>
            /// Verifies the BlockStopForSession scenario and expected contract.
            /// </summary>
            internal void BlockStopForSession(string sessionKey)
            {
                lock (_gate)
                {
                    _stopBlocks[sessionKey] = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    _stopStarted[sessionKey] = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                }
            }

            /// <summary>
            /// Verifies the WaitForStopStartedAsync scenario and expected contract.
            /// </summary>
            internal async Task WaitForStopStartedAsync(string sessionKey, CancellationToken cancellationToken)
            {
                Task task;
                lock (_gate)
                {
                    if (!_stopStarted.TryGetValue(sessionKey, out TaskCompletionSource<bool>? source))
                    {
                        throw new InvalidOperationException($"No stop-start signal exists for session '{sessionKey}'.");
                    }

                    task = source.Task;
                }

                await task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            /// <summary>
            /// Verifies the ReleaseStop scenario and expected contract.
            /// </summary>
            internal void ReleaseStop(string sessionKey)
            {
                lock (_gate)
                {
                    if (_stopBlocks.TryGetValue(sessionKey, out TaskCompletionSource<bool>? source))
                    {
                        _ = source.TrySetResult(true);
                    }
                }
            }

            /// <summary>
            /// Verifies the AwaitStopGateAsync scenario and expected contract.
            /// </summary>
            internal async Task AwaitStopGateAsync(string sessionKey, CancellationToken cancellationToken)
            {
                Task? gateTask = null;
                TaskCompletionSource<bool>? startedSignal = null;

                lock (_gate)
                {
                    _stopStarted.TryGetValue(sessionKey, out startedSignal);
                    if (_stopBlocks.TryGetValue(sessionKey, out TaskCompletionSource<bool>? source))
                    {
                        gateTask = source.Task;
                    }
                }

                startedSignal?.TrySetResult(true);

                if (gateTask is not null)
                {
                    await gateTask.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Documents the TrackingSession test type and its protected contract.
        /// </summary>
        private sealed class TrackingSession : IRabbitMqConsumerSession
        {
            /// <summary>
            /// Stores the _connectionManager fixture value used by these tests.
            /// </summary>
            private readonly RabbitMqConnectionManager _connectionManager;
            /// <summary>
            /// Stores the _topologyInitializer fixture value used by these tests.
            /// </summary>
            private readonly RabbitMqTopologyInitializer _topologyInitializer;
            /// <summary>
            /// Stores the _deliverySink fixture value used by these tests.
            /// </summary>
            private readonly IRabbitMqDeliverySink _deliverySink;
            /// <summary>
            /// Stores the _prefetchCount fixture value used by these tests.
            /// </summary>
            private readonly ushort? _prefetchCount;
            /// <summary>
            /// Stores the _ownedChannel fixture value used by these tests.
            /// </summary>
            private RabbitMqOwnedChannel? _ownedChannel;

            /// <summary>
            /// Verifies the TrackingSession scenario and expected contract.
            /// </summary>
            internal TrackingSession(
                RabbitMqConsumerSessionIdentity identity,
                RabbitMqConnectionManager connectionManager,
                RabbitMqTopologyInitializer topologyInitializer,
                IRabbitMqDeliverySink deliverySink,
                ushort? prefetchCount)
            {
                Identity = identity ?? throw new ArgumentNullException(nameof(identity));
                _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
                _topologyInitializer = topologyInitializer ?? throw new ArgumentNullException(nameof(topologyInitializer));
                _deliverySink = deliverySink ?? throw new ArgumentNullException(nameof(deliverySink));
                _prefetchCount = prefetchCount;
            }

            /// <summary>
            /// Stores the Identity value used by this test fixture.
            /// </summary>
            public RabbitMqConsumerSessionIdentity Identity { get; }

            /// <summary>
            /// Stores the IsRunning value used by this test fixture.
            /// </summary>
            public bool IsRunning { get; private set; }

            /// <summary>
            /// Stores the ActiveConnectionGeneration value used by this test fixture.
            /// </summary>
            public long ActiveConnectionGeneration { get; private set; }

            /// <summary>
            /// Stores the StartCallCount value used by this test fixture.
            /// </summary>
            internal int StartCallCount { get; private set; }

            /// <summary>
            /// Stores the StopCallCount value used by this test fixture.
            /// </summary>
            internal int StopCallCount { get; private set; }

            /// <summary>
            /// Stores the LastCancelAdmittedWork value used by this test fixture.
            /// </summary>
            internal bool? LastCancelAdmittedWork { get; private set; }

            /// <summary>
            /// Stores the DisposeCalled value used by this test fixture.
            /// </summary>
            internal bool DisposeCalled { get; private set; }

            /// <summary>
            /// Verifies the StartAsync scenario and expected contract.
            /// </summary>
            public async Task StartAsync(CancellationToken cancellationToken)
            {
                if (IsRunning)
                {
                    return;
                }

                await _connectionManager.EnsureConnectedAsync(cancellationToken).ConfigureAwait(false);
                await _topologyInitializer.InitializeAsync(Identity.ServerId, [Identity.Backbone], cancellationToken).ConfigureAwait(false);

                _ownedChannel = await _connectionManager.CreateOwnedChannelAsync($"tracking-consumer:{Identity.SessionKey}", cancellationToken).ConfigureAwait(false);

                if (_prefetchCount.HasValue)
                {
                    await _ownedChannel.Channel.BasicQosAsync(0u, _prefetchCount.Value, false, cancellationToken).ConfigureAwait(false);
                }

                string queueName = $"grabbers.{Identity.Backbone.Trim().ToLowerInvariant()}";
                AsyncEventingBasicConsumer consumer = new(_ownedChannel.Channel.UnderlyingChannel);
                _ = await _ownedChannel.Channel.BasicConsumeAsync(queueName, autoAck: false, consumer: consumer, cancellationToken).ConfigureAwait(false);
                ActiveConnectionGeneration = _ownedChannel.ConnectionGeneration;
                IsRunning = true;
                StartCallCount++;
                _ = _deliverySink;
            }

            /// <summary>
            /// Verifies the StopAsync scenario and expected contract.
            /// </summary>
            public async Task StopAsync(CancellationToken cancellationToken, bool cancelAdmittedWork)
            {
                if (!IsRunning)
                {
                    return;
                }

                if (_ownedChannel is not null)
                {
                    await _ownedChannel.DisposeAsync().ConfigureAwait(false);
                    _ownedChannel = null;
                }

                _ = cancellationToken;
                LastCancelAdmittedWork = cancelAdmittedWork;
                IsRunning = false;
                ActiveConnectionGeneration = 0;
                StopCallCount++;
            }

            /// <summary>
            /// Verifies the DisposeAsync scenario and expected contract.
            /// </summary>
            public async ValueTask DisposeAsync()
            {
                DisposeCalled = true;
                await StopAsync(CancellationToken.None, cancelAdmittedWork: true).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Documents the ObservingRealSessionFactory test type and its protected contract.
        /// </summary>
        private sealed class ObservingRealSessionFactory : IRabbitMqConsumerSessionFactory
        {
            /// <summary>
            /// Documents the new member and its test-supporting contract.
            /// </summary>
            private readonly Dictionary<string, List<ObservedSessionRuntime>> _sessionsByKey = new(StringComparer.Ordinal);
            /// <summary>
            /// Stores the _gate fixture value used by these tests.
            /// </summary>
            private readonly object _gate = new();
            /// <summary>
            /// Stores the _connectionManager fixture value used by these tests.
            /// </summary>
            private readonly RabbitMqConnectionManager _connectionManager;
            /// <summary>
            /// Stores the _topologyInitializer fixture value used by these tests.
            /// </summary>
            private readonly RabbitMqTopologyInitializer _topologyInitializer;

            /// <summary>
            /// Verifies the ObservingRealSessionFactory scenario and expected contract.
            /// </summary>
            internal ObservingRealSessionFactory(RabbitMqConnectionManager connectionManager, RabbitMqTopologyInitializer topologyInitializer)
            {
                _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
                _topologyInitializer = topologyInitializer ?? throw new ArgumentNullException(nameof(topologyInitializer));
            }

            /// <summary>
            /// Verifies the CreateSession scenario and expected contract.
            /// </summary>
            public IRabbitMqConsumerSession CreateSession(RabbitMqConsumerSessionIdentity identity, IRabbitMqDeliverySink deliverySink, ushort? prefetchCount)
            {
                ArgumentNullException.ThrowIfNull(identity);
                ArgumentNullException.ThrowIfNull(deliverySink);

                ObservedSessionRuntime runtime = new(identity, deliverySink);
                IRabbitMqDeliverySink observingSink = new ForwardingObservingDeliverySink(runtime, deliverySink);
                RabbitMqBackboneConsumerSession innerSession = new(identity, _connectionManager, _topologyInitializer, observingSink, NullLogger<RabbitMqBackboneConsumerSession>.Instance, prefetchCount);
                runtime.AttachSession(innerSession);

                lock (_gate)
                {
                    if (!_sessionsByKey.TryGetValue(identity.SessionKey, out List<ObservedSessionRuntime>? sessions))
                    {
                        sessions = [];
                        _sessionsByKey[identity.SessionKey] = sessions;
                    }

                    sessions.Add(runtime);
                }

                return runtime;
            }

            /// <summary>
            /// Verifies the RequireLatestSession scenario and expected contract.
            /// </summary>
            internal ObservedSessionRuntime RequireLatestSession(string sessionKey)
            {
                lock (_gate)
                {
                    if (!_sessionsByKey.TryGetValue(sessionKey, out List<ObservedSessionRuntime>? sessions) || sessions.Count == 0)
                    {
                        throw new InvalidOperationException($"Expected observed session '{sessionKey}'.");
                    }

                    return sessions[^1];
                }
            }

            /// <summary>
            /// Verifies the GetCreatedCount scenario and expected contract.
            /// </summary>
            internal int GetCreatedCount(string sessionKey)
            {
                lock (_gate)
                {
                    return _sessionsByKey.TryGetValue(sessionKey, out List<ObservedSessionRuntime>? sessions)
                        ? sessions.Count
                        : 0;
                }
            }

            /// <summary>
            /// Verifies the WaitForDeliveryAsync scenario and expected contract.
            /// </summary>
            internal async Task<RabbitMqArticleDelivery> WaitForDeliveryAsync(string sessionKey, CancellationToken cancellationToken)
            {
                ObservedSessionRuntime session = RequireLatestSession(sessionKey);
                return await session.WaitForDeliveryAsync(cancellationToken).ConfigureAwait(false);
            }

            /// <summary>
            /// Verifies the GetDeliveriesForSession scenario and expected contract.
            /// </summary>
            internal IReadOnlyList<RabbitMqArticleDelivery> GetDeliveriesForSession(string sessionKey)
            {
                return RequireLatestSession(sessionKey).Deliveries;
            }
        }

        /// <summary>
        /// Documents the ObservedSessionRuntime test type and its protected contract.
        /// </summary>
        private sealed class ObservedSessionRuntime : IRabbitMqConsumerSession
        {
            /// <summary>
            /// Stores the _identity fixture value used by these tests.
            /// </summary>
            private readonly RabbitMqConsumerSessionIdentity _identity;
            /// <summary>
            /// Stores the _downstreamSink fixture value used by these tests.
            /// </summary>
            private readonly IRabbitMqDeliverySink _downstreamSink;
            /// <summary>
            /// Stores the _deliveries fixture value used by these tests.
            /// </summary>
            private readonly List<RabbitMqArticleDelivery> _deliveries = [];
            /// <summary>
            /// Stores the _firstDelivery fixture value used by these tests.
            /// </summary>
            private readonly TaskCompletionSource<RabbitMqArticleDelivery> _firstDelivery = new(TaskCreationOptions.RunContinuationsAsynchronously);
            /// <summary>
            /// Stores the _inner fixture value used by these tests.
            /// </summary>
            private RabbitMqBackboneConsumerSession? _inner;

            /// <summary>
            /// Verifies the ObservedSessionRuntime scenario and expected contract.
            /// </summary>
            internal ObservedSessionRuntime(RabbitMqConsumerSessionIdentity identity, IRabbitMqDeliverySink downstreamSink)
            {
                _identity = identity ?? throw new ArgumentNullException(nameof(identity));
                _downstreamSink = downstreamSink ?? throw new ArgumentNullException(nameof(downstreamSink));
            }

            /// <summary>
            /// Stores the Identity value used by this test fixture.
            /// </summary>
            public RabbitMqConsumerSessionIdentity Identity => _identity;

            /// <summary>
            /// Stores the IsRunning value used by this test fixture.
            /// </summary>
            public bool IsRunning => _inner?.IsRunning ?? false;

            /// <summary>
            /// Stores the ActiveConnectionGeneration value used by this test fixture.
            /// </summary>
            public long ActiveConnectionGeneration => _inner?.ActiveConnectionGeneration ?? 0;

            /// <summary>
            /// Stores the StopCallCount value used by this test fixture.
            /// </summary>
            internal int StopCallCount { get; private set; }

            /// <summary>
            /// Stores the DisposeCallCount value used by this test fixture.
            /// </summary>
            internal int DisposeCallCount { get; private set; }

            /// <summary>
            /// Stores the LastCancelAdmittedWork value used by this test fixture.
            /// </summary>
            internal bool? LastCancelAdmittedWork { get; private set; }

            /// <summary>
            /// Stores the Deliveries value used by this test fixture.
            /// </summary>
            internal IReadOnlyList<RabbitMqArticleDelivery> Deliveries => _deliveries;

            /// <summary>
            /// Verifies the AttachSession scenario and expected contract.
            /// </summary>
            internal void AttachSession(RabbitMqBackboneConsumerSession session)
            {
                _inner = session ?? throw new ArgumentNullException(nameof(session));
            }

            /// <summary>
            /// Verifies the OnDeliveryObservedAsync scenario and expected contract.
            /// </summary>
            internal ValueTask OnDeliveryObservedAsync(RabbitMqArticleDelivery delivery, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _deliveries.Add(delivery);
                _ = _firstDelivery.TrySetResult(delivery);
                return ValueTask.CompletedTask;
            }

            /// <summary>
            /// Verifies the WaitForDeliveryAsync scenario and expected contract.
            /// </summary>
            internal async Task<RabbitMqArticleDelivery> WaitForDeliveryAsync(CancellationToken cancellationToken)
            {
                return await _firstDelivery.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            /// <summary>
            /// Verifies the StartAsync scenario and expected contract.
            /// </summary>
            public async Task StartAsync(CancellationToken cancellationToken)
            {
                if (_inner is null)
                {
                    throw new InvalidOperationException("Observed session has not been attached to an inner RabbitMQ session.");
                }

                await _inner.StartAsync(cancellationToken).ConfigureAwait(false);
            }

            /// <summary>
            /// Verifies the StopAsync scenario and expected contract.
            /// </summary>
            public async Task StopAsync(CancellationToken cancellationToken, bool cancelAdmittedWork)
            {
                StopCallCount++;
                LastCancelAdmittedWork = cancelAdmittedWork;
                if (_inner is null)
                {
                    throw new InvalidOperationException("Observed session has not been attached to an inner RabbitMQ session.");
                }

                await _inner.StopAsync(cancellationToken, cancelAdmittedWork).ConfigureAwait(false);
            }

            /// <summary>
            /// Verifies the DisposeAsync scenario and expected contract.
            /// </summary>
            public async ValueTask DisposeAsync()
            {
                DisposeCallCount++;
                if (_inner is null)
                {
                    return;
                }

                await _inner.DisposeAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Documents the ForwardingObservingDeliverySink test type and its protected contract.
        /// </summary>
        private sealed class ForwardingObservingDeliverySink : IRabbitMqDeliverySink
        {
            /// <summary>
            /// Stores the _owner fixture value used by these tests.
            /// </summary>
            private readonly ObservedSessionRuntime _owner;
            /// <summary>
            /// Stores the _inner fixture value used by these tests.
            /// </summary>
            private readonly IRabbitMqDeliverySink _inner;

            /// <summary>
            /// Verifies the ForwardingObservingDeliverySink scenario and expected contract.
            /// </summary>
            internal ForwardingObservingDeliverySink(ObservedSessionRuntime owner, IRabbitMqDeliverySink inner)
            {
                _owner = owner ?? throw new ArgumentNullException(nameof(owner));
                _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            }

            /// <summary>
            /// Verifies the OnDeliveryAsync scenario and expected contract.
            /// </summary>
            public async ValueTask OnDeliveryAsync(RabbitMqArticleDelivery delivery, CancellationToken cancellationToken)
            {
                await _owner.OnDeliveryObservedAsync(delivery, cancellationToken).ConfigureAwait(false);
                await _inner.OnDeliveryAsync(delivery, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Documents the BlockingStopTrackingSession test type and its protected contract.
        /// </summary>
        private sealed class BlockingStopTrackingSession : IRabbitMqConsumerSession
        {
            /// <summary>
            /// Stores the _connectionManager fixture value used by these tests.
            /// </summary>
            private readonly RabbitMqConnectionManager _connectionManager;
            /// <summary>
            /// Stores the _topologyInitializer fixture value used by these tests.
            /// </summary>
            private readonly RabbitMqTopologyInitializer _topologyInitializer;
            /// <summary>
            /// Stores the _deliverySink fixture value used by these tests.
            /// </summary>
            private readonly IRabbitMqDeliverySink _deliverySink;
            /// <summary>
            /// Stores the _prefetchCount fixture value used by these tests.
            /// </summary>
            private readonly ushort? _prefetchCount;
            /// <summary>
            /// Stores the _owner fixture value used by these tests.
            /// </summary>
            private readonly BlockingStopSessionFactory _owner;
            /// <summary>
            /// Stores the _ownedChannel fixture value used by these tests.
            /// </summary>
            private RabbitMqOwnedChannel? _ownedChannel;

            /// <summary>
            /// Verifies the BlockingStopTrackingSession scenario and expected contract.
            /// </summary>
            internal BlockingStopTrackingSession(
                RabbitMqConsumerSessionIdentity identity,
                RabbitMqConnectionManager connectionManager,
                RabbitMqTopologyInitializer topologyInitializer,
                IRabbitMqDeliverySink deliverySink,
                ushort? prefetchCount,
                BlockingStopSessionFactory owner)
            {
                Identity = identity ?? throw new ArgumentNullException(nameof(identity));
                _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
                _topologyInitializer = topologyInitializer ?? throw new ArgumentNullException(nameof(topologyInitializer));
                _deliverySink = deliverySink ?? throw new ArgumentNullException(nameof(deliverySink));
                _prefetchCount = prefetchCount;
                _owner = owner ?? throw new ArgumentNullException(nameof(owner));
            }

            /// <summary>
            /// Stores the Identity value used by this test fixture.
            /// </summary>
            public RabbitMqConsumerSessionIdentity Identity { get; }

            /// <summary>
            /// Stores the IsRunning value used by this test fixture.
            /// </summary>
            public bool IsRunning { get; private set; }

            /// <summary>
            /// Stores the ActiveConnectionGeneration value used by this test fixture.
            /// </summary>
            public long ActiveConnectionGeneration { get; private set; }

            /// <summary>
            /// Stores the StartCallCount value used by this test fixture.
            /// </summary>
            internal int StartCallCount { get; private set; }

            /// <summary>
            /// Stores the StopCallCount value used by this test fixture.
            /// </summary>
            internal int StopCallCount { get; private set; }

            /// <summary>
            /// Stores the DisposeCalled value used by this test fixture.
            /// </summary>
            internal bool DisposeCalled { get; private set; }

            /// <summary>
            /// Verifies the StartAsync scenario and expected contract.
            /// </summary>
            public async Task StartAsync(CancellationToken cancellationToken)
            {
                if (IsRunning)
                {
                    return;
                }

                await _connectionManager.EnsureConnectedAsync(cancellationToken).ConfigureAwait(false);
                await _topologyInitializer.InitializeAsync(Identity.ServerId, [Identity.Backbone], cancellationToken).ConfigureAwait(false);

                _ownedChannel = await _connectionManager.CreateOwnedChannelAsync($"blocking-tracking-consumer:{Identity.SessionKey}", cancellationToken).ConfigureAwait(false);

                if (_prefetchCount.HasValue)
                {
                    await _ownedChannel.Channel.BasicQosAsync(0u, _prefetchCount.Value, false, cancellationToken).ConfigureAwait(false);
                }

                string queueName = $"grabbers.{Identity.Backbone.Trim().ToLowerInvariant()}";
                AsyncEventingBasicConsumer consumer = new(_ownedChannel.Channel.UnderlyingChannel);
                _ = await _ownedChannel.Channel.BasicConsumeAsync(queueName, autoAck: false, consumer: consumer, cancellationToken).ConfigureAwait(false);
                ActiveConnectionGeneration = _ownedChannel.ConnectionGeneration;
                IsRunning = true;
                StartCallCount++;
                _ = _deliverySink;
            }

            /// <summary>
            /// Verifies the StopAsync scenario and expected contract.
            /// </summary>
            public async Task StopAsync(CancellationToken cancellationToken, bool cancelAdmittedWork)
            {
                bool wasRunning = IsRunning;
                if (wasRunning)
                {
                    IsRunning = false;
                }

                await _owner.AwaitStopGateAsync(Identity.SessionKey, cancellationToken).ConfigureAwait(false);

                if (!wasRunning)
                {
                    return;
                }

                if (_ownedChannel is not null)
                {
                    await _ownedChannel.DisposeAsync().ConfigureAwait(false);
                    _ownedChannel = null;
                }

                _ = cancelAdmittedWork;
                ActiveConnectionGeneration = 0;
                StopCallCount++;
            }

            /// <summary>
            /// Verifies the DisposeAsync scenario and expected contract.
            /// </summary>
            public async ValueTask DisposeAsync()
            {
                DisposeCalled = true;
                await StopAsync(CancellationToken.None, cancelAdmittedWork: true).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Documents the RetirementFailureMode test type and its protected contract.
        /// </summary>
        private enum RetirementFailureMode
        {
            /// <summary>
            /// Defines the None value covered by this test contract.
            /// </summary>
            None,
            /// <summary>
            /// Defines the StopThrowsBeforeStopping value covered by this test contract.
            /// </summary>
            StopThrowsBeforeStopping,
            /// <summary>
            /// Defines the StopThrowsAfterStopping value covered by this test contract.
            /// </summary>
            StopThrowsAfterStopping,
            /// <summary>
            /// Defines the DisposeThrowsAfterStop value covered by this test contract.
            /// </summary>
            DisposeThrowsAfterStop,
        }

        /// <summary>
        /// Documents the FaultInjectingSessionFactory test type and its protected contract.
        /// </summary>
        private sealed class FaultInjectingSessionFactory(RabbitMqConnectionManager connectionManager, RabbitMqTopologyInitializer topologyInitializer) : IRabbitMqConsumerSessionFactory
        {
            /// <summary>
            /// Documents the new member and its test-supporting contract.
            /// </summary>
            private readonly Dictionary<string, List<FaultInjectingSession>> _sessionsByKey = new(StringComparer.Ordinal);
            /// <summary>
            /// Documents the new member and its test-supporting contract.
            /// </summary>
            private readonly Dictionary<string, RetirementFailureMode> _failureModesBySessionKey = new(StringComparer.Ordinal);
            /// <summary>
            /// Stores the _gate fixture value used by these tests.
            /// </summary>
            private readonly object _gate = new();
            /// <summary>
            /// Stores the _connectionManager fixture value used by these tests.
            /// </summary>
            private readonly RabbitMqConnectionManager _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
            /// <summary>
            /// Stores the _topologyInitializer fixture value used by these tests.
            /// </summary>
            private readonly RabbitMqTopologyInitializer _topologyInitializer = topologyInitializer ?? throw new ArgumentNullException(nameof(topologyInitializer));

            /// <summary>
            /// Verifies the CreateSession scenario and expected contract.
            /// </summary>
            public IRabbitMqConsumerSession CreateSession(RabbitMqConsumerSessionIdentity identity, IRabbitMqDeliverySink deliverySink, ushort? prefetchCount)
            {
                ArgumentNullException.ThrowIfNull(identity);
                ArgumentNullException.ThrowIfNull(deliverySink);

                RetirementFailureMode failureMode;
                lock (_gate)
                {
                    failureMode = _failureModesBySessionKey.GetValueOrDefault(identity.SessionKey, RetirementFailureMode.None);
                }

                FaultInjectingSession session = new(identity, _connectionManager, _topologyInitializer, deliverySink, prefetchCount, failureMode);

                lock (_gate)
                {
                    if (!_sessionsByKey.TryGetValue(identity.SessionKey, out List<FaultInjectingSession>? sessions))
                    {
                        sessions = [];
                        _sessionsByKey[identity.SessionKey] = sessions;
                    }

                    sessions.Add(session);
                }

                return session;
            }

            /// <summary>
            /// Verifies the SetFailureMode scenario and expected contract.
            /// </summary>
            internal void SetFailureMode(string sessionKey, RetirementFailureMode mode)
            {
                lock (_gate)
                {
                    _failureModesBySessionKey[sessionKey] = mode;
                }
            }

            /// <summary>
            /// Verifies the GetCreatedCount scenario and expected contract.
            /// </summary>
            internal int GetCreatedCount(string sessionKey)
            {
                lock (_gate)
                {
                    return _sessionsByKey.TryGetValue(sessionKey, out List<FaultInjectingSession>? sessions) ? sessions.Count : 0;
                }
            }
        }

        /// <summary>
        /// Documents the FaultInjectingSession test type and its protected contract.
        /// </summary>
        private sealed class FaultInjectingSession : IRabbitMqConsumerSession
        {
            /// <summary>
            /// Stores the _connectionManager fixture value used by these tests.
            /// </summary>
            private readonly RabbitMqConnectionManager _connectionManager;
            /// <summary>
            /// Stores the _topologyInitializer fixture value used by these tests.
            /// </summary>
            private readonly RabbitMqTopologyInitializer _topologyInitializer;
            /// <summary>
            /// Stores the _deliverySink fixture value used by these tests.
            /// </summary>
            private readonly IRabbitMqDeliverySink _deliverySink;
            /// <summary>
            /// Stores the _prefetchCount fixture value used by these tests.
            /// </summary>
            private readonly ushort? _prefetchCount;
            /// <summary>
            /// Stores the _failureMode fixture value used by these tests.
            /// </summary>
            private readonly RetirementFailureMode _failureMode;
            /// <summary>
            /// Stores the _ownedChannel fixture value used by these tests.
            /// </summary>
            private RabbitMqOwnedChannel? _ownedChannel;

            /// <summary>
            /// Verifies the FaultInjectingSession scenario and expected contract.
            /// </summary>
            internal FaultInjectingSession(
                RabbitMqConsumerSessionIdentity identity,
                RabbitMqConnectionManager connectionManager,
                RabbitMqTopologyInitializer topologyInitializer,
                IRabbitMqDeliverySink deliverySink,
                ushort? prefetchCount,
                RetirementFailureMode failureMode)
            {
                Identity = identity ?? throw new ArgumentNullException(nameof(identity));
                _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
                _topologyInitializer = topologyInitializer ?? throw new ArgumentNullException(nameof(topologyInitializer));
                _deliverySink = deliverySink ?? throw new ArgumentNullException(nameof(deliverySink));
                _prefetchCount = prefetchCount;
                _failureMode = failureMode;
            }

            /// <summary>
            /// Stores the Identity value used by this test fixture.
            /// </summary>
            public RabbitMqConsumerSessionIdentity Identity { get; }

            /// <summary>
            /// Stores the IsRunning value used by this test fixture.
            /// </summary>
            public bool IsRunning { get; private set; }

            /// <summary>
            /// Stores the ActiveConnectionGeneration value used by this test fixture.
            /// </summary>
            public long ActiveConnectionGeneration { get; private set; }

            /// <summary>
            /// Stores the StopCallCount value used by this test fixture.
            /// </summary>
            internal int StopCallCount { get; private set; }

            /// <summary>
            /// Stores the DisposeCallCount value used by this test fixture.
            /// </summary>
            internal int DisposeCallCount { get; private set; }

            /// <summary>
            /// Verifies the StartAsync scenario and expected contract.
            /// </summary>
            public async Task StartAsync(CancellationToken cancellationToken)
            {
                if (IsRunning)
                {
                    return;
                }

                await _connectionManager.EnsureConnectedAsync(cancellationToken).ConfigureAwait(false);
                await _topologyInitializer.InitializeAsync(Identity.ServerId, [Identity.Backbone], cancellationToken).ConfigureAwait(false);

                _ownedChannel = await _connectionManager.CreateOwnedChannelAsync($"fault-injecting-consumer:{Identity.SessionKey}", cancellationToken).ConfigureAwait(false);

                if (_prefetchCount.HasValue)
                {
                    await _ownedChannel.Channel.BasicQosAsync(0u, _prefetchCount.Value, false, cancellationToken).ConfigureAwait(false);
                }

                string queueName = $"grabbers.{Identity.Backbone.Trim().ToLowerInvariant()}";
                AsyncEventingBasicConsumer consumer = new(_ownedChannel.Channel.UnderlyingChannel);
                _ = await _ownedChannel.Channel.BasicConsumeAsync(queueName, autoAck: false, consumer: consumer, cancellationToken).ConfigureAwait(false);
                ActiveConnectionGeneration = _ownedChannel.ConnectionGeneration;
                IsRunning = true;
                _ = _deliverySink;
            }

            /// <summary>
            /// Verifies the StopAsync scenario and expected contract.
            /// </summary>
            public async Task StopAsync(CancellationToken cancellationToken, bool cancelAdmittedWork)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = cancelAdmittedWork;

                if (_failureMode is RetirementFailureMode.StopThrowsBeforeStopping)
                {
                    throw new InvalidOperationException($"Injected stop failure before stopping for {Identity.SessionKey}.");
                }

                if (!IsRunning)
                {
                    StopCallCount++;
                    return;
                }

                IsRunning = false;

                if (_ownedChannel is not null)
                {
                    await _ownedChannel.DisposeAsync().ConfigureAwait(false);
                    _ownedChannel = null;
                }

                ActiveConnectionGeneration = 0;
                StopCallCount++;

                if (_failureMode is RetirementFailureMode.StopThrowsAfterStopping)
                {
                    throw new InvalidOperationException($"Injected stop failure after stopping for {Identity.SessionKey}.");
                }
            }

            /// <summary>
            /// Verifies the DisposeAsync scenario and expected contract.
            /// </summary>
            public async ValueTask DisposeAsync()
            {
                DisposeCallCount++;

                if (_failureMode is RetirementFailureMode.DisposeThrowsAfterStop)
                {
                    throw new InvalidOperationException($"Injected dispose failure for {Identity.SessionKey}.");
                }

                await StopAsync(CancellationToken.None, cancelAdmittedWork: true).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// In-memory sink that captures deliveries forwarded by RabbitMQ consumer sessions.
        /// </summary>
        private sealed class RecordingDeliverySink : IRabbitMqDeliverySink
        {
            /// <summary>
            /// Stores the Deliveries value used by this test fixture.
            /// </summary>
            internal List<RabbitMqArticleDelivery> Deliveries { get; } = [];

            /// <summary>
            /// Verifies the OnDeliveryAsync scenario and expected contract.
            /// </summary>
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
            /// <summary>
            /// Stores the _connections fixture value used by these tests.
            /// </summary>
            private readonly List<TrackingConnection> _connections = [];
            /// <summary>
            /// Stores the _connectCallCount fixture value used by these tests.
            /// </summary>
            private int _connectCallCount;

            /// <summary>
            /// Stores the ConnectCallCount value used by this test fixture.
            /// </summary>
            internal int ConnectCallCount => Volatile.Read(ref _connectCallCount);

            /// <summary>
            /// Stores the AllConnections value used by this test fixture.
            /// </summary>
            internal IReadOnlyList<TrackingConnection> AllConnections => _connections;

            /// <summary>
            /// Verifies the ConnectAsync scenario and expected contract.
            /// </summary>
            public Task<IRabbitMqBrokerConnection> ConnectAsync(RabbitMqRuntimeOptions runtimeOptions, string clientProvidedConnectionName, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = Interlocked.Increment(ref _connectCallCount);

                TrackingConnection connection = new(runtimeOptions.Hosts[0], runtimeOptions.Port, runtimeOptions.VirtualHost, clientProvidedConnectionName);
                _connections.Add(connection);
                return Task.FromResult<IRabbitMqBrokerConnection>(connection);
            }

            /// <summary>
            /// Verifies the RequireLastConnection scenario and expected contract.
            /// </summary>
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
            /// <summary>
            /// Stores the IsOpen value used by this test fixture.
            /// </summary>
            public bool IsOpen { get; private set; } = true;

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
            public string ClientProvidedName { get; } = clientProvidedName;

            /// <summary>
            /// Stores the UnderlyingConnection value used by this test fixture.
            /// </summary>
            public IConnection UnderlyingConnection => throw new NotSupportedException();

            /// <summary>
            /// Stores the Channels value used by this test fixture.
            /// </summary>
            public List<TrackingChannel> Channels { get; } = [];

            /// <summary>
            /// Stores the Disposed value used by this test fixture.
            /// </summary>
            public bool Disposed { get; private set; }

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
                TrackingChannel channel = new();
                Channels.Add(channel);
                return Task.FromResult<IRabbitMqChannel>(channel);
            }

            /// <summary>
            /// Verifies the DisposeAsync scenario and expected contract.
            /// </summary>
            public ValueTask DisposeAsync()
            {
                IsOpen = false;
                Disposed = true;
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
            /// Verifies the RaiseRecoverySucceeded scenario and expected contract.
            /// </summary>
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
            /// <summary>
            /// Stores the _underlyingChannel fixture value used by these tests.
            /// </summary>
            private readonly IChannel _underlyingChannel = DispatchProxy.Create<IChannel, NoOpChannelProxy>();
            /// <summary>
            /// Stores the _consumer fixture value used by these tests.
            /// </summary>
            private IAsyncBasicConsumer? _consumer;
            /// <summary>
            /// Stores the _consumerTag fixture value used by these tests.
            /// </summary>
            private string? _consumerTag;
            /// <summary>
            /// Stores the _cancelObserved fixture value used by these tests.
            /// </summary>
            private readonly TaskCompletionSource<bool> _cancelObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);
            /// <summary>
            /// Stores the _disposedObserved fixture value used by these tests.
            /// </summary>
            private readonly TaskCompletionSource<bool> _disposedObserved = new(TaskCreationOptions.RunContinuationsAsynchronously);

            /// <summary>
            /// Stores the UnderlyingChannel value used by this test fixture.
            /// </summary>
            public IChannel UnderlyingChannel => _underlyingChannel;

            /// <summary>
            /// Stores the Disposed value used by this test fixture.
            /// </summary>
            public bool Disposed { get; private set; }

            /// <summary>
            /// Stores the ConsumeCallCount value used by this test fixture.
            /// </summary>
            public int ConsumeCallCount { get; private set; }

            /// <summary>
            /// Stores the CancelCallCount value used by this test fixture.
            /// </summary>
            public int CancelCallCount { get; private set; }

            /// <summary>
            /// Stores the LastConsumeAutoAck value used by this test fixture.
            /// </summary>
            public bool LastConsumeAutoAck { get; private set; }

            /// <summary>
            /// Stores the LastConsumeQueue value used by this test fixture.
            /// </summary>
            public string LastConsumeQueue { get; private set; } = string.Empty;

            /// <summary>
            /// Stores the LastPrefetchCount value used by this test fixture.
            /// </summary>
            public ushort? LastPrefetchCount { get; private set; }

            /// <summary>
            /// Stores the AckCallCount value used by this test fixture.
            /// </summary>
            public int AckCallCount { get; private set; }

            /// <summary>
            /// Stores the NackCallCount value used by this test fixture.
            /// </summary>
            public int NackCallCount { get; private set; }

            /// <summary>
            /// Stores the OperationLog value used by this test fixture.
            /// </summary>
            public List<string> OperationLog { get; } = [];

            /// <summary>
            /// Stores the IsConsumerCurrentlyActive value used by this test fixture.
            /// </summary>
            public bool IsConsumerCurrentlyActive => ConsumeCallCount > CancelCallCount && !Disposed;

            /// <summary>
            /// Stores the CancelObserved value used by this test fixture.
            /// </summary>
            public Task CancelObserved => _cancelObserved.Task;

            /// <summary>
            /// Stores the DisposedObserved value used by this test fixture.
            /// </summary>
            public Task DisposedObserved => _disposedObserved.Task;

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
                _ = global;
                LastPrefetchCount = prefetchCount;
                return Task.CompletedTask;
            }

            /// <summary>
            /// Verifies the BasicConsumeAsync scenario and expected contract.
            /// </summary>
            public Task<string> BasicConsumeAsync(string queue, bool autoAck, IAsyncBasicConsumer consumer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                ConsumeCallCount++;
                LastConsumeQueue = queue;
                LastConsumeAutoAck = autoAck;
                _consumer = consumer;
                _consumerTag = $"ctag-{ConsumeCallCount}";
                OperationLog.Add("consume");
                return Task.FromResult(_consumerTag);
            }

            /// <summary>
            /// Verifies the BasicCancelAsync scenario and expected contract.
            /// </summary>
            public Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (string.Equals(_consumerTag, consumerTag, StringComparison.Ordinal))
                {
                    CancelCallCount++;
                    _consumerTag = null;
                    OperationLog.Add("cancel");
                    _ = _cancelObserved.TrySetResult(true);
                }

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
                AckCallCount++;
                OperationLog.Add("ack");
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
                NackCallCount++;
                OperationLog.Add("nack");
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
            /// Verifies the DeliverAsync scenario and expected contract.
            /// </summary>
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

            /// <summary>
            /// Verifies the DisposeAsync scenario and expected contract.
            /// </summary>
            public ValueTask DisposeAsync()
            {
                Disposed = true;
                _consumer = null;
                _consumerTag = null;
                OperationLog.Add("dispose");
                _ = _disposedObserved.TrySetResult(true);
                return ValueTask.CompletedTask;
            }
        }

        /// <summary>
        /// Dynamic no-op proxy for RabbitMQ.Client.IChannel needed by AsyncEventingBasicConsumer construction in tests.
        /// </summary>
        private class NoOpChannelProxy : DispatchProxy
        {
            /// <summary>
            /// Verifies the Invoke scenario and expected contract.
            /// </summary>
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

            /// <summary>
            /// Verifies the CreateDefaultValue scenario and expected contract.
            /// </summary>
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


