// <copyright file="RabbitMqArticleResponsePublisherPhase4Tests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for rabbit mq article response publisher phase4, covering NNTP article and transport behavior; dependency integration and failure handling.
// Primary responsibility: documents the executable contracts covered by the rabbit mq article response publisher phase 4 test suite.

using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Articles.Processing;
using VectorNNTP.Backfiller.Runtime.RabbitMq;
using VectorNNTP.Backfiller.Runtime.Shutdown;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Verifies Phase 4 RPC response publisher behavior against channel ownership and generation invariants.
    /// </summary>
    public sealed class RabbitMqArticleResponsePublisherPhase4Tests
    {
        /// <summary>
        /// Confirms the publish and confirm async when successful uses reply to and correlation id and returns confirmed async behavior.
        /// </summary>
        [Fact]
        public async Task PublishAndConfirmAsync_WhenSuccessful_UsesReplyToAndCorrelationIdAndReturnsConfirmedAsync()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            RecordingBrokerConnector connector = new();
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(publishConfirmTimeoutSeconds: 10);
            RabbitMqConnectionManager connectionManager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            await connectionManager.EnsureConnectedAsync(CancellationToken.None);

            RabbitMqArticleResponsePublisher publisher = new(runtimeOptions, connectionManager, NullLogger<RabbitMqArticleResponsePublisher>.Instance);
            ArticleWorkProcessingResult result = CreateSuccessResult(
                deliveryTag: 701,
                connectionGeneration: connectionManager.ConnectionGeneration,
                correlationId: "corr-publisher-success",
                replyTo: "rpc.reply.success");
            RabbitMqArticleWorkResponse response = new(
                Version: 1,
                RequestId: result.Request.RequestId,
                MessageId: result.Request.MessageId,
                Backbone: result.Request.Backbone,
                Outcome: "Success",
                Uri: null,
                Error: null);

            RabbitMqResponsePublishResult publishResult = await publisher.PublishAndConfirmAsync(result, response, CancellationToken.None);

            RecordingBrokerConnection connection = connector.RequireLastConnection();
            RecordingChannel channel = Assert.Single(connection.CreatedChannels);
            Assert.Equal(RabbitMqResponsePublishStatus.Confirmed, publishResult.Status);
            Assert.Equal(result.Delivery.ReplyTo, channel.LastPublishRoutingKey);
            Assert.Equal(result.Delivery.CorrelationId, channel.LastPublishCorrelationId);
            Assert.Equal(result.Delivery.ConnectionGeneration, publishResult.ConnectionGeneration);
            Assert.NotNull(channel.LastPublishBody);
            string json = Encoding.UTF8.GetString(channel.LastPublishBody!);
            Assert.Contains("\"requestId\"", json, StringComparison.Ordinal);
            Assert.DoesNotContain("correlationId", json, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("replyTo", json, StringComparison.OrdinalIgnoreCase);

            RabbitMqArticleWorkResponse parsed = RabbitMqArticleWorkResponseWireProtocol.ParseV1(channel.LastPublishBody!);
            Assert.Equal(nameof(ArticleWorkProcessingOutcome.Success), parsed.Outcome);
            Assert.Null(parsed.Uri);
            Assert.Null(parsed.Error);

            await connectionManager.DisposeAsync();
            result.Dispose();
        }
        /// <summary>
        /// Confirms the publish and confirm async when publish throws returns failed async behavior.
        /// </summary>
        [Fact]
        public async Task PublishAndConfirmAsync_WhenPublishThrows_ReturnsFailedAsync()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            RecordingBrokerConnector connector = new();
            connector.FailPublishWith = new InvalidOperationException("publish failed");

            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(publishConfirmTimeoutSeconds: 10);
            RabbitMqConnectionManager connectionManager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            await connectionManager.EnsureConnectedAsync(CancellationToken.None);

            RabbitMqArticleResponsePublisher publisher = new(runtimeOptions, connectionManager, NullLogger<RabbitMqArticleResponsePublisher>.Instance);
            ArticleWorkProcessingResult result = CreateSuccessResult(
                deliveryTag: 702,
                connectionGeneration: connectionManager.ConnectionGeneration,
                correlationId: "corr-publisher-failed",
                replyTo: "rpc.reply.failed");

            RabbitMqArticleWorkResponse response = new(1, result.Request.RequestId, result.Request.MessageId, result.Request.Backbone, "Success", null, null);
            RabbitMqResponsePublishResult publishResult = await publisher.PublishAndConfirmAsync(result, response, CancellationToken.None);

            Assert.Equal(RabbitMqResponsePublishStatus.Failed, publishResult.Status);
            Assert.NotNull(publishResult.Exception);

            await connectionManager.DisposeAsync();
            result.Dispose();
        }
        /// <summary>
        /// Confirms the publish and confirm async when publish cancellation timeouts returns timed out async behavior.
        /// </summary>
        [Fact]
        public async Task PublishAndConfirmAsync_WhenPublishCancellationTimeouts_ReturnsTimedOutAsync()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            RecordingBrokerConnector connector = new();
            connector.BlockPublishUntilCancelled = true;

            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(publishConfirmTimeoutSeconds: 1);
            RabbitMqConnectionManager connectionManager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            await connectionManager.EnsureConnectedAsync(CancellationToken.None).ConfigureAwait(false);

            RabbitMqArticleResponsePublisher publisher = new(runtimeOptions, connectionManager, NullLogger<RabbitMqArticleResponsePublisher>.Instance);
            ArticleWorkProcessingResult result = CreateSuccessResult(
                deliveryTag: 703,
                connectionGeneration: connectionManager.ConnectionGeneration,
                correlationId: "corr-publisher-timeout",
                replyTo: "rpc.reply.timeout");

            RabbitMqArticleWorkResponse response = new(1, result.Request.RequestId, result.Request.MessageId, result.Request.Backbone, "Success", null, null);
            RabbitMqResponsePublishResult publishResult = await publisher.PublishAndConfirmAsync(result, response, CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(RabbitMqResponsePublishStatus.TimedOut, publishResult.Status);

            await connectionManager.DisposeAsync().ConfigureAwait(false);
            result.Dispose();
        }
        /// <summary>
        /// Confirms the publish and confirm async when connection generation changes during publish returns failed async behavior.
        /// </summary>
        [Fact]
        public async Task PublishAndConfirmAsync_WhenConnectionGenerationChangesDuringPublish_ReturnsFailedAsync()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            RecordingBrokerConnector connector = new();

            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(publishConfirmTimeoutSeconds: 10);
            RabbitMqConnectionManager connectionManager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            await connectionManager.EnsureConnectedAsync(CancellationToken.None).ConfigureAwait(false);

            RabbitMqArticleResponsePublisher publisher = new(runtimeOptions, connectionManager, NullLogger<RabbitMqArticleResponsePublisher>.Instance);
            ArticleWorkProcessingResult result = CreateSuccessResult(
                deliveryTag: 704,
                connectionGeneration: connectionManager.ConnectionGeneration,
                correlationId: "corr-publisher-generation-change",
                replyTo: "rpc.reply.generation-change");

            RabbitMqArticleWorkResponse response = new(1, result.Request.RequestId, result.Request.MessageId, result.Request.Backbone, "Success", null, null);

            connector.BlockPublishUntilCancelled = true;
            Task<RabbitMqResponsePublishResult> publishTask = publisher.PublishAndConfirmAsync(result, response, CancellationToken.None).AsTask();
            await connector.WaitForFirstPublishStartedAsync().ConfigureAwait(false);
            connector.RequireLastConnection().RaiseConnectionShutdown();
            await connector.WaitForConnectCountAsync(2, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            connector.ReleasePublishBlock();

            RabbitMqResponsePublishResult publishResult = await publishTask.ConfigureAwait(false);
            Assert.Equal(RabbitMqResponsePublishStatus.Failed, publishResult.Status);

            await connectionManager.DisposeAsync().ConfigureAwait(false);
            result.Dispose();
        }
        /// <summary>
        /// Confirms the publish and confirm async when connection was replaced before publish uses current generation channel async behavior.
        /// </summary>
        [Fact]
        public async Task PublishAndConfirmAsync_WhenConnectionWasReplacedBeforePublish_UsesCurrentGenerationChannelAsync()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            RecordingBrokerConnector connector = new();

            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(publishConfirmTimeoutSeconds: 10);
            RabbitMqConnectionManager connectionManager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            await connectionManager.EnsureConnectedAsync(CancellationToken.None).ConfigureAwait(false);

            connector.RequireLastConnection().RaiseConnectionShutdown();
            await connector.WaitForConnectCountAsync(2, TimeSpan.FromSeconds(5)).ConfigureAwait(false);

            RabbitMqArticleResponsePublisher publisher = new(runtimeOptions, connectionManager, NullLogger<RabbitMqArticleResponsePublisher>.Instance);
            ArticleWorkProcessingResult result = CreateSuccessResult(
                deliveryTag: 705,
                connectionGeneration: connectionManager.ConnectionGeneration,
                correlationId: "corr-publisher-replaced-before",
                replyTo: "rpc.reply.replaced-before");

            RabbitMqArticleWorkResponse response = new(1, result.Request.RequestId, result.Request.MessageId, result.Request.Backbone, "Success", null, null);
            RabbitMqResponsePublishResult publishResult = await publisher.PublishAndConfirmAsync(result, response, CancellationToken.None).ConfigureAwait(false);

            RecordingBrokerConnection connection = connector.RequireLastConnection();
            RecordingChannel channel = Assert.Single(connection.CreatedChannels);
            Assert.Equal(connectionManager.ConnectionGeneration, channel.CreatedAtGeneration);
            Assert.Equal(RabbitMqResponsePublishStatus.Confirmed, publishResult.Status);

            await connectionManager.DisposeAsync().ConfigureAwait(false);
            result.Dispose();
        }
        /// <summary>
        /// Confirms the publish and confirm async when shutdown starts rejects publish infrastructure async behavior.
        /// </summary>
        [Fact]
        public async Task PublishAndConfirmAsync_WhenShutdownStarts_RejectsPublishInfrastructureAsync()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            RecordingBrokerConnector connector = new();

            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(publishConfirmTimeoutSeconds: 10);
            RabbitMqConnectionManager connectionManager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            await connectionManager.EnsureConnectedAsync(CancellationToken.None).ConfigureAwait(false);

            shutdownCoordinator.SignalGracefulShutdown(TimeSpan.FromSeconds(1), ShutdownCoordinator.ShutdownReason.HostStopping);

            RabbitMqArticleResponsePublisher publisher = new(runtimeOptions, connectionManager, NullLogger<RabbitMqArticleResponsePublisher>.Instance);
            ArticleWorkProcessingResult result = CreateSuccessResult(
                deliveryTag: 706,
                connectionGeneration: connectionManager.ConnectionGeneration,
                correlationId: "corr-publisher-shutdown",
                replyTo: "rpc.reply.shutdown");

            RabbitMqArticleWorkResponse response = new(1, result.Request.RequestId, result.Request.MessageId, result.Request.Backbone, "Success", null, null);
            RabbitMqResponsePublishResult publishResult = await publisher.PublishAndConfirmAsync(result, response, CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(RabbitMqResponsePublishStatus.Failed, publishResult.Status);

            await connectionManager.DisposeAsync().ConfigureAwait(false);
            result.Dispose();
        }
        /// <summary>
        /// Confirms the publish and confirm async when concurrent publishes reuses single owned channel async behavior.
        /// </summary>
        [Fact]
        public async Task PublishAndConfirmAsync_WhenConcurrentPublishes_ReusesSingleOwnedChannelAsync()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            RecordingBrokerConnector connector = new();

            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(publishConfirmTimeoutSeconds: 10);
            RabbitMqConnectionManager connectionManager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            await connectionManager.EnsureConnectedAsync(CancellationToken.None).ConfigureAwait(false);

            RabbitMqArticleResponsePublisher publisher = new(runtimeOptions, connectionManager, NullLogger<RabbitMqArticleResponsePublisher>.Instance);

            ArticleWorkProcessingResult firstResult = CreateSuccessResult(801, connectionManager.ConnectionGeneration, "corr-concurrent-1", "rpc.reply.concurrent.1");
            ArticleWorkProcessingResult secondResult = CreateSuccessResult(802, connectionManager.ConnectionGeneration, "corr-concurrent-2", "rpc.reply.concurrent.2");

            RabbitMqArticleWorkResponse firstResponse = new(1, firstResult.Request.RequestId, firstResult.Request.MessageId, firstResult.Request.Backbone, "Success", null, null);
            RabbitMqArticleWorkResponse secondResponse = new(1, secondResult.Request.RequestId, secondResult.Request.MessageId, secondResult.Request.Backbone, "Success", null, null);

            Task<RabbitMqResponsePublishResult> firstPublish = publisher.PublishAndConfirmAsync(firstResult, firstResponse, CancellationToken.None).AsTask();
            Task<RabbitMqResponsePublishResult> secondPublish = publisher.PublishAndConfirmAsync(secondResult, secondResponse, CancellationToken.None).AsTask();

            RabbitMqResponsePublishResult[] results = await Task.WhenAll(firstPublish, secondPublish).ConfigureAwait(false);

            Assert.All(results, static publishResult => Assert.Equal(RabbitMqResponsePublishStatus.Confirmed, publishResult.Status));

            RecordingBrokerConnection connection = connector.RequireLastConnection();
            Assert.Single(connection.CreatedChannels);

            await connectionManager.DisposeAsync().ConfigureAwait(false);
            firstResult.Dispose();
            secondResult.Dispose();
        }

        /// <summary>
        /// Confirms the create success result behavior.
        /// </summary>
        /// <returns>The value returned by the create success result helper.</returns>
        /// <summary>
        /// Confirms the create success result behavior.
        /// </summary>
        /// <param name="deliveryTag">The delivery tag used by this test scenario.</param>
        /// <param name="connectionGeneration">The connection generation used by this test scenario.</param>
        /// <param name="correlationId">The correlation id used by this test scenario.</param>
        /// <param name="replyTo">The reply to used by this test scenario.</param>
        /// <returns>The value returned by the create success result helper.</returns>
        private static ArticleWorkProcessingResult CreateSuccessResult(ulong deliveryTag, long connectionGeneration, string correlationId, string replyTo)
        {
            Guid requestId = Guid.NewGuid();
            string messageId = $"<{requestId:N}@example.invalid>";
            RabbitMqArticleWorkRequest request = new(1, requestId, messageId, "BackboneA");
            RabbitMqArticleDelivery delivery = new(
                Backbone: "BackboneA",
                Queue: "grabbers.backbonea",
                ConsumerTag: "ctag-response",
                ConsumerIdentity: "consumer-response",
                DeliveryTag: deliveryTag,
                Redelivered: false,
                RoutingKey: "grabbers.backbonea",
                Exchange: "grabbers.backbonea",
                ConnectionGeneration: connectionGeneration,
                RabbitMqMessageId: "rmq-mid",
                CorrelationId: correlationId,
                ReplyTo: replyTo,
                Payload: Encoding.UTF8.GetBytes(CreateValidPayload(requestId, messageId, "BackboneA")),
                CancellationToken: CancellationToken.None,
                Settlement: new NoOpSettlement());

            return new ArticleWorkProcessingResult(
                Request: request,
                Delivery: delivery,
                Outcome: ArticleWorkProcessingOutcome.Success,
                Disposition: ArticleWorkDispositionRecommendation.Ack,
                GrabberResult: null,
                ProviderFailureCode: null,
                ResponseCode: null,
                ResponseText: null,
                UnexpectedException: null);
        }

        /// <summary>
        /// Confirms the create valid payload behavior.
        /// </summary>
        /// <returns>The value returned by the create valid payload helper.</returns>
        /// <summary>
        /// Confirms the create valid payload behavior.
        /// </summary>
        /// <param name="requestId">The request id used by this test scenario.</param>
        /// <param name="messageId">The message id used by this test scenario.</param>
        /// <param name="backbone">The backbone used by this test scenario.</param>
        /// <returns>The value returned by the create valid payload helper.</returns>
        private static string CreateValidPayload(Guid requestId, string messageId, string backbone)
        {
            return $"{{\"version\":1,\"requestId\":\"{requestId}\",\"messageId\":\"{messageId}\",\"backbone\":\"{backbone}\"}}";
        }

        /// <summary>
        /// Confirms the create runtime options behavior.
        /// </summary>
        /// <returns>The value returned by the create runtime options helper.</returns>
        /// <summary>
        /// Confirms the create runtime options behavior.
        /// </summary>
        /// <param name="publishConfirmTimeoutSeconds">The publish confirm timeout seconds used by this test scenario.</param>
        /// <returns>The value returned by the create runtime options helper.</returns>
        private static BackFillerRuntimeOptions CreateRuntimeOptions(int publishConfirmTimeoutSeconds)
        {
            RabbitMqRuntimeOptions rabbitMqOptions = new(
                Hosts: ["localhost"],
                Port: 5672,
                Username: "user",
                Password: "password",
                VirtualHost: "/",
                EnableSsl: false,
                ChannelLeaseTimeoutSeconds: 60,
                RpcTimeoutSeconds: 30,
                ConnectionBlockedTimeoutSeconds: 30,
                ChannelPoolSize: 64,
                MinConnections: 1,
                MaxConnections: 4,
                MaxConsecutiveRecoveryFailures: 1,
                MaxPendingLeaseWaiters: 128,
                ConnectionScaleDownIdleSeconds: 60,
                ScaleDownCooldownSeconds: 10,
                NetworkRecoveryIntervalSeconds: 1,
                PoolReconnectBaseDelayMs: 5,
                PoolReconnectMaxDelayMs: 50,
                MinimumConnectionLifetimeSeconds: 1,
                PublishConfirmTimeoutSeconds: publishConfirmTimeoutSeconds,
                MaximumShutdownDrainTimeoutSeconds: 10,
                DegradedThreshold: 0.75,
                UnhealthyThreshold: 3,
                RequestedHeartbeatSeconds: 30,
                SocketTimeoutSeconds: 30,
                RequestedChannelMax: 128,
                ConsumerPrefetchCount: 16);

            return new BackFillerRuntimeOptions(
                CanonicalBackFillerFqdn: "backfiller-phase4.usenet.ninja",
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
                ShutdownGracePeriodSeconds: 30,
                ShutdownDrainQueuedWork: true,
                ShutdownFinishActiveArticles: true,
                RabbitMqMaximumShutdownDrainTimeoutSeconds: 10,
                WriteBatchCoalesceMicroseconds: 250,
                RabbitMq: rabbitMqOptions);
        }

        /// <summary>
        /// Confirms the recording broker connector behavior.
        /// </summary>
        private sealed class RecordingBrokerConnector : IRabbitMqBrokerConnector
        {
            /// <summary>
            /// Confirms  publish started behavior.
            /// </summary>
            private readonly SemaphoreSlim _publishStarted = new(0, 1);
            /// <summary>
            /// Confirms  gate behavior.
            /// </summary>
            private readonly object _gate = new();
            /// <summary>
            /// Supplies  connections for the fixture or scenario under test.
            /// </summary>
            private readonly List<RecordingBrokerConnection> _connections = [];
            /// <summary>
            /// Supplies  connect count for the fixture or scenario under test.
            /// </summary>
            private int _connectCount;

            /// <summary>
            /// Supplies block publish until cancelled for the fixture or scenario under test.
            /// </summary>
            internal bool BlockPublishUntilCancelled { get; set; }

            /// <summary>
            /// Supplies fail publish with for the fixture or scenario under test.
            /// </summary>
            internal Exception? FailPublishWith { get; set; }

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
                cancellationToken.ThrowIfCancellationRequested();
                int nextCount = Interlocked.Increment(ref _connectCount);
                RecordingBrokerConnection connection = new(
                    endpointHostName: runtimeOptions.Hosts[0],
                    endpointPort: runtimeOptions.Port,
                    virtualHost: runtimeOptions.VirtualHost,
                    clientProvidedName: clientProvidedConnectionName,
                    generation: nextCount,
                    this);

                lock (_gate)
                {
                    _connections.Add(connection);
                }

                return Task.FromResult<IRabbitMqBrokerConnection>(connection);
            }

            /// <summary>
            /// Confirms the require last connection behavior.
            /// </summary>
            /// <returns>The value returned by the require last connection helper.</returns>
            /// <summary>
            /// Confirms the require last connection behavior.
            /// </summary>
            /// <returns>The value returned by the require last connection helper.</returns>
            internal RecordingBrokerConnection RequireLastConnection()
            {
                lock (_gate)
                {
                    return _connections.Count > 0
                        ? _connections[^1]
                        : throw new InvalidOperationException("Expected at least one connection.");
                }
            }

            /// <summary>
            /// Confirms the wait for first publish started async behavior.
            /// </summary>
            /// <returns>The value returned by the wait for first publish started async helper.</returns>
            /// <summary>
            /// Confirms the wait for first publish started async behavior.
            /// </summary>
            /// <returns>The value returned by the wait for first publish started async helper.</returns>
            internal async Task WaitForFirstPublishStartedAsync()
            {
                await _publishStarted.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            }

            /// <summary>
            /// Confirms the wait for connect count async behavior.
            /// </summary>
            /// <returns>The value returned by the wait for connect count async helper.</returns>
            /// <summary>
            /// Confirms the wait for connect count async behavior.
            /// </summary>
            /// <param name="expectedMinimum">The expected minimum used by this test scenario.</param>
            /// <param name="timeout">The timeout used by this test scenario.</param>
            /// <returns>The value returned by the wait for connect count async helper.</returns>
            internal async Task WaitForConnectCountAsync(int expectedMinimum, TimeSpan timeout)
            {
                DateTime deadline = DateTime.UtcNow + timeout;
                while (DateTime.UtcNow < deadline)
                {
                    if (Volatile.Read(ref _connectCount) >= expectedMinimum)
                    {
                        return;
                    }

                    await Task.Delay(20).ConfigureAwait(false);
                }

                Assert.True(Volatile.Read(ref _connectCount) >= expectedMinimum, "Expected connection replacement did not occur before timeout.");
            }

            /// <summary>
            /// Confirms the release publish block behavior.
            /// </summary>
            internal void ReleasePublishBlock()
            {
                BlockPublishUntilCancelled = false;
            }

            /// <summary>
            /// Confirms the mark publish started behavior.
            /// </summary>
            internal void MarkPublishStarted()
            {
                if (_publishStarted.CurrentCount == 0)
                {
                    _ = _publishStarted.Release();
                }
            }
        }

        /// <summary>
        /// Confirms the recording broker connection behavior.
        /// </summary>
        private sealed class RecordingBrokerConnection : IRabbitMqBrokerConnection
        {
            /// <summary>
            /// Supplies  owner for the fixture or scenario under test.
            /// </summary>
            private readonly RecordingBrokerConnector _owner;

            /// <summary>
            /// Confirms the recording broker connection behavior.
            /// </summary>
            internal RecordingBrokerConnection(string endpointHostName, int endpointPort, string virtualHost, string clientProvidedName, int generation, RecordingBrokerConnector owner)
            {
                EndpointHostName = endpointHostName;
                EndpointPort = endpointPort;
                VirtualHost = virtualHost;
                ClientProvidedName = clientProvidedName;
                Generation = generation;
                _owner = owner;
            }

            /// <summary>
            /// Supplies generation for the fixture or scenario under test.
            /// </summary>
            internal int Generation { get; }

            /// <summary>
            /// Supplies created channels for the fixture or scenario under test.
            /// </summary>
            internal List<RecordingChannel> CreatedChannels { get; } = [];

            /// <summary>
            /// Supplies is open for the fixture or scenario under test.
            /// </summary>
            public bool IsOpen { get; private set; } = true;

            /// <summary>
            /// Supplies endpoint host name for the fixture or scenario under test.
            /// </summary>
            public string EndpointHostName { get; }

            /// <summary>
            /// Supplies endpoint port for the fixture or scenario under test.
            /// </summary>
            public int EndpointPort { get; }

            /// <summary>
            /// Supplies virtual host for the fixture or scenario under test.
            /// </summary>
            public string VirtualHost { get; }

            /// <summary>
            /// Supplies client provided name for the fixture or scenario under test.
            /// </summary>
            public string ClientProvidedName { get; }

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
                RecordingChannel channel = new(enablePublisherConfirmations, Generation, _owner);
                CreatedChannels.Add(channel);
                return Task.FromResult<IRabbitMqChannel>(channel);
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
        }

        /// <summary>
        /// Confirms the recording channel behavior.
        /// </summary>
        private sealed class RecordingChannel : IRabbitMqChannel
        {
            /// <summary>
            /// Supplies  owner for the fixture or scenario under test.
            /// </summary>
            private readonly RecordingBrokerConnector _owner;

            /// <summary>
            /// Confirms the recording channel behavior.
            /// </summary>
            internal RecordingChannel(bool enablePublisherConfirmations, int createdAtGeneration, RecordingBrokerConnector owner)
            {
                PublisherConfirmationsEnabled = enablePublisherConfirmations;
                CreatedAtGeneration = createdAtGeneration;
                _owner = owner;
            }

            /// <summary>
            /// Supplies publisher confirmations enabled for the fixture or scenario under test.
            /// </summary>
            internal bool PublisherConfirmationsEnabled { get; }

            /// <summary>
            /// Supplies created at generation for the fixture or scenario under test.
            /// </summary>
            internal int CreatedAtGeneration { get; }

            /// <summary>
            /// Supplies last publish routing key for the fixture or scenario under test.
            /// </summary>
            internal string? LastPublishRoutingKey { get; private set; }

            /// <summary>
            /// Supplies last publish correlation id for the fixture or scenario under test.
            /// </summary>
            internal string? LastPublishCorrelationId { get; private set; }

            /// <summary>
            /// Supplies last publish body for the fixture or scenario under test.
            /// </summary>
            internal byte[]? LastPublishBody { get; private set; }

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
                return Task.FromResult("ctag-unused");
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
            public async ValueTask BasicPublishAsync(string exchange, string routingKey, bool mandatory, BasicProperties basicProperties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken)
            {
                _owner.MarkPublishStarted();
                cancellationToken.ThrowIfCancellationRequested();
                LastPublishRoutingKey = routingKey;
                LastPublishCorrelationId = basicProperties.CorrelationId;
                LastPublishBody = body.ToArray();
                _ = exchange;
                _ = mandatory;

                if (_owner.FailPublishWith is not null)
                {
                    throw _owner.FailPublishWith;
                }

                while (_owner.BlockPublishUntilCancelled)
                {
                    await Task.Delay(20, cancellationToken).ConfigureAwait(false);
                }
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
                return ValueTask.CompletedTask;
            }
        }

        /// <summary>
        /// Confirms the no op settlement behavior.
        /// </summary>
        private sealed class NoOpSettlement : IRabbitMqDeliverySettlement
        {
            /// <summary>
            /// Confirms the ack async behavior.
            /// </summary>
            /// <returns>The value returned by the ack async helper.</returns>
            /// <summary>
            /// Confirms the ack async behavior.
            /// </summary>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the ack async helper.</returns>
            public ValueTask AckAsync(CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return ValueTask.CompletedTask;
            }

            /// <summary>
            /// Confirms the nack async behavior.
            /// </summary>
            /// <returns>The value returned by the nack async helper.</returns>
            /// <summary>
            /// Confirms the nack async behavior.
            /// </summary>
            /// <param name="requeue">The requeue used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the nack async helper.</returns>
            public ValueTask NackAsync(bool requeue, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = requeue;
                return ValueTask.CompletedTask;
            }
        }
    }
}
