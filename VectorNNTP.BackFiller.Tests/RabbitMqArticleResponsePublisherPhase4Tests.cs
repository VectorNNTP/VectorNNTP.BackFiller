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
        /// Verifies the publish and confirm async when successful uses reply to and correlation id and returns confirmed async scenario and its documented contract.
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
        /// Verifies the publish and confirm async when publish throws returns failed async scenario and its documented contract.
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
        /// Verifies the publish and confirm async when publish cancellation timeouts returns timed out async scenario and its documented contract.
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
        /// Verifies the publish and confirm async when connection generation changes during publish returns failed async scenario and its documented contract.
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
        /// Verifies the publish and confirm async when connection was replaced before publish uses current generation channel async scenario and its documented contract.
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
        /// Verifies the publish and confirm async when shutdown starts rejects publish infrastructure async scenario and its documented contract.
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
        /// Verifies the publish and confirm async when concurrent publishes reuses single owned channel async scenario and its documented contract.
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
        /// Verifies the create success result scenario and its documented contract.
        /// </summary>
        /// <returns>The create success result value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the create success result scenario and its documented contract.
        /// </summary>
        /// <param name="deliveryTag">The delivery tag supplied to the helper.</param>
        /// <param name="connectionGeneration">The connection generation supplied to the helper.</param>
        /// <param name="correlationId">The correlation id supplied to the helper.</param>
        /// <param name="replyTo">The reply to supplied to the helper.</param>
        /// <returns>The create success result value produced for the requested scenario.</returns>
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
        /// Verifies the create valid payload scenario and its documented contract.
        /// </summary>
        /// <returns>The create valid payload value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the create valid payload scenario and its documented contract.
        /// </summary>
        /// <param name="requestId">The request id supplied to the helper.</param>
        /// <param name="messageId">The message id supplied to the helper.</param>
        /// <param name="backbone">The backbone supplied to the helper.</param>
        /// <returns>The create valid payload value produced for the requested scenario.</returns>
        private static string CreateValidPayload(Guid requestId, string messageId, string backbone)
        {
            return $"{{\"version\":1,\"requestId\":\"{requestId}\",\"messageId\":\"{messageId}\",\"backbone\":\"{backbone}\"}}";
        }

        /// <summary>
        /// Verifies the create runtime options scenario and its documented contract.
        /// </summary>
        /// <returns>The create runtime options value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the create runtime options scenario and its documented contract.
        /// </summary>
        /// <param name="publishConfirmTimeoutSeconds">The publish confirm timeout seconds supplied to the helper.</param>
        /// <returns>The create runtime options value produced for the requested scenario.</returns>
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
        /// Verifies the recording broker connector scenario and its documented contract.
        /// </summary>
        private sealed class RecordingBrokerConnector : IRabbitMqBrokerConnector
        {
            /// <summary>
            /// Exercises  publish started behavior, including the expected result and failure semantics.
            /// </summary>
            private readonly SemaphoreSlim _publishStarted = new(0, 1);
            /// <summary>
            /// Exercises  gate behavior, including the expected result and failure semantics.
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
        /// Verifies the require last connection scenario and its documented contract.
            /// </summary>
        /// <returns>The require last connection value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the require last connection scenario and its documented contract.
        /// </summary>
        /// <returns>The require last connection value produced for the requested scenario.</returns>
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
        /// Verifies the wait for first publish started async scenario and its documented contract.
            /// </summary>
        /// <returns>The wait for first publish started async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the wait for first publish started async scenario and its documented contract.
        /// </summary>
        /// <returns>The wait for first publish started async value produced for the requested scenario.</returns>
            internal async Task WaitForFirstPublishStartedAsync()
            {
                await _publishStarted.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            }

            /// <summary>
        /// Verifies the wait for connect count async scenario and its documented contract.
            /// </summary>
        /// <returns>The wait for connect count async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the wait for connect count async scenario and its documented contract.
        /// </summary>
        /// <param name="expectedMinimum">The expected minimum supplied to the helper.</param>
        /// <param name="timeout">The timeout supplied to the helper.</param>
        /// <returns>The wait for connect count async value produced for the requested scenario.</returns>
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
        /// Verifies the release publish block scenario and its documented contract.
            /// </summary>
            internal void ReleasePublishBlock()
            {
                BlockPublishUntilCancelled = false;
            }

            /// <summary>
        /// Verifies the mark publish started scenario and its documented contract.
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
        /// Verifies the recording broker connection scenario and its documented contract.
        /// </summary>
        private sealed class RecordingBrokerConnection : IRabbitMqBrokerConnection
        {
            /// <summary>
            /// Supplies  owner for the fixture or scenario under test.
            /// </summary>
            private readonly RecordingBrokerConnector _owner;

            /// <summary>
        /// Verifies the recording broker connection scenario and its documented contract.
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
                RecordingChannel channel = new(enablePublisherConfirmations, Generation, _owner);
                CreatedChannels.Add(channel);
                return Task.FromResult<IRabbitMqChannel>(channel);
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
        }

        /// <summary>
        /// Verifies the recording channel scenario and its documented contract.
        /// </summary>
        private sealed class RecordingChannel : IRabbitMqChannel
        {
            /// <summary>
            /// Supplies  owner for the fixture or scenario under test.
            /// </summary>
            private readonly RecordingBrokerConnector _owner;

            /// <summary>
        /// Verifies the recording channel scenario and its documented contract.
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
                return Task.FromResult("ctag-unused");
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
        /// Verifies the dispose async scenario and its documented contract.
            /// </summary>
        /// <returns>The dispose async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the dispose async scenario and its documented contract.
        /// </summary>
        /// <returns>The dispose async value produced for the requested scenario.</returns>
            public ValueTask DisposeAsync()
            {
                return ValueTask.CompletedTask;
            }
        }

        /// <summary>
        /// Verifies the no op settlement scenario and its documented contract.
        /// </summary>
        private sealed class NoOpSettlement : IRabbitMqDeliverySettlement
        {
            /// <summary>
        /// Verifies the ack async scenario and its documented contract.
            /// </summary>
        /// <returns>The ack async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the ack async scenario and its documented contract.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token supplied to the helper.</param>
        /// <returns>The ack async value produced for the requested scenario.</returns>
            public ValueTask AckAsync(CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return ValueTask.CompletedTask;
            }

            /// <summary>
        /// Verifies the nack async scenario and its documented contract.
            /// </summary>
        /// <returns>The nack async value produced for the requested scenario.</returns>
        /// <summary>
        /// Verifies the nack async scenario and its documented contract.
        /// </summary>
        /// <param name="requeue">The requeue supplied to the helper.</param>
        /// <param name="cancellationToken">The cancellation token supplied to the helper.</param>
        /// <returns>The nack async value produced for the requested scenario.</returns>
            public ValueTask NackAsync(bool requeue, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = requeue;
                return ValueTask.CompletedTask;
            }
        }
    }
}
