// <copyright file="RabbitMqArticleResponsePublisherPhase4Tests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Tests / RabbitMQ
// Focused Phase 4 tests for RPC response publisher channel ownership, generation handling, and AMQP metadata propagation.

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
        [Fact]
        public async Task PublishAndConfirmAsync_WhenSuccessful_UsesReplyToAndCorrelationIdAndReturnsConfirmedAsync()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            RecordingBrokerConnector connector = new();
            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(publishConfirmTimeoutSeconds: 10);
            RabbitMqConnectionManager connectionManager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            await connectionManager.EnsureConnectedAsync(CancellationToken.None).ConfigureAwait(false);

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

            RabbitMqResponsePublishResult publishResult = await publisher.PublishAndConfirmAsync(result, response, CancellationToken.None).ConfigureAwait(false);

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

            await connectionManager.DisposeAsync().ConfigureAwait(false);
            result.Dispose();
        }

        [Fact]
        public async Task PublishAndConfirmAsync_WhenPublishThrows_ReturnsFailedAsync()
        {
            using ShutdownCoordinator shutdownCoordinator = new();
            RecordingBrokerConnector connector = new();
            connector.FailPublishWith = new InvalidOperationException("publish failed");

            BackFillerRuntimeOptions runtimeOptions = CreateRuntimeOptions(publishConfirmTimeoutSeconds: 10);
            RabbitMqConnectionManager connectionManager = new(runtimeOptions, shutdownCoordinator, TimeProvider.System, NullLogger<RabbitMqConnectionManager>.Instance, connector);
            await connectionManager.EnsureConnectedAsync(CancellationToken.None).ConfigureAwait(false);

            RabbitMqArticleResponsePublisher publisher = new(runtimeOptions, connectionManager, NullLogger<RabbitMqArticleResponsePublisher>.Instance);
            ArticleWorkProcessingResult result = CreateSuccessResult(
                deliveryTag: 702,
                connectionGeneration: connectionManager.ConnectionGeneration,
                correlationId: "corr-publisher-failed",
                replyTo: "rpc.reply.failed");

            RabbitMqArticleWorkResponse response = new(1, result.Request.RequestId, result.Request.MessageId, result.Request.Backbone, "Success", null, null);
            RabbitMqResponsePublishResult publishResult = await publisher.PublishAndConfirmAsync(result, response, CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(RabbitMqResponsePublishStatus.Failed, publishResult.Status);
            Assert.NotNull(publishResult.Exception);

            await connectionManager.DisposeAsync().ConfigureAwait(false);
            result.Dispose();
        }

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

        private static string CreateValidPayload(Guid requestId, string messageId, string backbone)
        {
            return $"{{\"version\":1,\"requestId\":\"{requestId}\",\"messageId\":\"{messageId}\",\"backbone\":\"{backbone}\"}}";
        }

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

        private sealed class RecordingBrokerConnector : IRabbitMqBrokerConnector
        {
            private readonly SemaphoreSlim _publishStarted = new(0, 1);
            private readonly object _gate = new();
            private readonly List<RecordingBrokerConnection> _connections = [];
            private int _connectCount;

            internal bool BlockPublishUntilCancelled { get; set; }

            internal Exception? FailPublishWith { get; set; }

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

            internal RecordingBrokerConnection RequireLastConnection()
            {
                lock (_gate)
                {
                    return _connections.Count > 0
                        ? _connections[^1]
                        : throw new InvalidOperationException("Expected at least one connection.");
                }
            }

            internal async Task WaitForFirstPublishStartedAsync()
            {
                await _publishStarted.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            }

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

            internal void ReleasePublishBlock()
            {
                BlockPublishUntilCancelled = false;
            }

            internal void MarkPublishStarted()
            {
                if (_publishStarted.CurrentCount == 0)
                {
                    _ = _publishStarted.Release();
                }
            }
        }

        private sealed class RecordingBrokerConnection : IRabbitMqBrokerConnection
        {
            private readonly RecordingBrokerConnector _owner;

            internal RecordingBrokerConnection(string endpointHostName, int endpointPort, string virtualHost, string clientProvidedName, int generation, RecordingBrokerConnector owner)
            {
                EndpointHostName = endpointHostName;
                EndpointPort = endpointPort;
                VirtualHost = virtualHost;
                ClientProvidedName = clientProvidedName;
                Generation = generation;
                _owner = owner;
            }

            internal int Generation { get; }

            internal List<RecordingChannel> CreatedChannels { get; } = [];

            public bool IsOpen { get; private set; } = true;

            public string EndpointHostName { get; }

            public int EndpointPort { get; }

            public string VirtualHost { get; }

            public string ClientProvidedName { get; }

            public IConnection UnderlyingConnection => throw new NotSupportedException();

            public event EventHandler<ShutdownEventArgs>? ConnectionShutdown;

            public event EventHandler<CallbackExceptionEventArgs>? CallbackException;

            public event EventHandler<ConnectionBlockedEventArgs>? ConnectionBlocked;

            public event EventHandler<AsyncEventArgs>? ConnectionUnblocked;

            public event EventHandler<ConnectionRecoveryErrorEventArgs>? ConnectionRecoveryError;

            public event EventHandler<AsyncEventArgs>? RecoverySucceeded;

            public Task<IRabbitMqChannel> CreateChannelAsync(CancellationToken cancellationToken, bool enablePublisherConfirmations = false)
            {
                cancellationToken.ThrowIfCancellationRequested();
                RecordingChannel channel = new(enablePublisherConfirmations, Generation, _owner);
                CreatedChannels.Add(channel);
                return Task.FromResult<IRabbitMqChannel>(channel);
            }

            public ValueTask DisposeAsync()
            {
                IsOpen = false;
                return ValueTask.CompletedTask;
            }

            internal void RaiseConnectionShutdown()
            {
                ConnectionShutdown?.Invoke(this, new ShutdownEventArgs(ShutdownInitiator.Peer, 320, "Closed by broker"));
            }
        }

        private sealed class RecordingChannel : IRabbitMqChannel
        {
            private readonly RecordingBrokerConnector _owner;

            internal RecordingChannel(bool enablePublisherConfirmations, int createdAtGeneration, RecordingBrokerConnector owner)
            {
                PublisherConfirmationsEnabled = enablePublisherConfirmations;
                CreatedAtGeneration = createdAtGeneration;
                _owner = owner;
            }

            internal bool PublisherConfirmationsEnabled { get; }

            internal int CreatedAtGeneration { get; }

            internal string? LastPublishRoutingKey { get; private set; }

            internal string? LastPublishCorrelationId { get; private set; }

            internal byte[]? LastPublishBody { get; private set; }

            public IChannel UnderlyingChannel => throw new NotSupportedException();

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
                return Task.CompletedTask;
            }

            public Task<string> BasicConsumeAsync(string queue, bool autoAck, IAsyncBasicConsumer consumer, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.FromResult("ctag-unused");
            }

            public Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }

            public ValueTask BasicAckAsync(ulong deliveryTag, bool multiple, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = deliveryTag;
                _ = multiple;
                return ValueTask.CompletedTask;
            }

            public ValueTask BasicNackAsync(ulong deliveryTag, bool multiple, bool requeue, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = deliveryTag;
                _ = multiple;
                _ = requeue;
                return ValueTask.CompletedTask;
            }

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

            public ValueTask DisposeAsync()
            {
                return ValueTask.CompletedTask;
            }
        }

        private sealed class NoOpSettlement : IRabbitMqDeliverySettlement
        {
            public ValueTask AckAsync(CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return ValueTask.CompletedTask;
            }

            public ValueTask NackAsync(bool requeue, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = requeue;
                return ValueTask.CompletedTask;
            }
        }
    }
}
