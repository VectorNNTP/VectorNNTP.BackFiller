// <copyright file="RabbitMqArticleResponsePublisher.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: rabbit mq article response publisher in the articles processing subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

// <copyright file="RabbitMqArticleResponsePublisher.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Processing
// Publishes terminal RPC responses to RabbitMQ using AMQP metadata from deliveries and
// confirms publication before ACK/NACK disposition is finalized.

using RabbitMQ.Client;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.RabbitMq;

namespace VectorNNTP.Backfiller.Runtime.Articles.Processing
{
    /// <summary>
    /// RabbitMQ response publisher using owned-channel isolation and publish confirms.
    /// </summary>
    internal sealed class RabbitMqArticleResponsePublisher : IRabbitMqArticleResponsePublisher, IAsyncDisposable
    {
        /// <summary>
        /// Stores the connection manager state used to enforce this component's runtime contract.
        /// </summary>
        private readonly RabbitMqConnectionManager _connectionManager;
        /// <summary>
        /// Stores the options state used to enforce this component's runtime contract.
        /// </summary>
        private readonly RabbitMqRuntimeOptions _options;
        /// <summary>
        /// Stores the logger state used to enforce this component's runtime contract.
        /// </summary>
        private readonly ILogger<RabbitMqArticleResponsePublisher> _logger;
        /// <summary>
        /// Stores the publish gate state used to enforce this component's runtime contract.
        /// </summary>
        private readonly SemaphoreSlim _publishGate = new(1, 1);
        /// <summary>
        /// Stores the owned publish channel state used to enforce this component's runtime contract.
        /// </summary>
        private RabbitMqOwnedChannel? _ownedPublishChannel;

        /// <summary>
        /// Initializes a new response publisher instance.
        /// </summary>
        /// <param name="runtimeOptions">Validated runtime options.</param>
        /// <param name="connectionManager">RabbitMQ connection/channel owner.</param>
        /// <param name="logger">Logger.</param>
        public RabbitMqArticleResponsePublisher(
            BackFillerRuntimeOptions runtimeOptions,
            RabbitMqConnectionManager connectionManager,
            ILogger<RabbitMqArticleResponsePublisher> logger)
        {
            ArgumentNullException.ThrowIfNull(runtimeOptions);
            _options = runtimeOptions.RabbitMq ?? throw new InvalidOperationException("Validated runtime RabbitMQ settings were not provided.");
            _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <inheritdoc/>
        public async ValueTask<RabbitMqResponsePublishResult> PublishAndConfirmAsync(
            ArticleWorkProcessingResult result,
            RabbitMqArticleWorkResponse response,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(result);
            ArgumentNullException.ThrowIfNull(response);

            if (string.IsNullOrWhiteSpace(result.CorrelationId))
            {
                return new RabbitMqResponsePublishResult(RabbitMqResponsePublishStatus.Failed, result.Delivery.ConnectionGeneration, new InvalidOperationException("AMQP CorrelationId is required for RPC response publishing."));
            }

            if (string.IsNullOrWhiteSpace(result.ReplyTo))
            {
                return new RabbitMqResponsePublishResult(RabbitMqResponsePublishStatus.Failed, result.Delivery.ConnectionGeneration, new InvalidOperationException("AMQP ReplyTo is required for RPC response publishing."));
            }

            await _publishGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                RabbitMqOwnedChannel? ownedChannel;
                try
                {
                    ownedChannel = await GetOrCreatePublishChannelAsync(result.Request.Backbone, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    return new RabbitMqResponsePublishResult(RabbitMqResponsePublishStatus.Failed, result.Delivery.ConnectionGeneration, ex);
                }

                if (ownedChannel is null)
                {
                    return new RabbitMqResponsePublishResult(
                        RabbitMqResponsePublishStatus.Failed,
                        result.Delivery.ConnectionGeneration,
                        new InvalidOperationException("RabbitMQ publish channel is unavailable."));
                }

                if (ownedChannel.ConnectionGeneration != _connectionManager.ConnectionGeneration)
                {
                    await ResetPublishChannelAsync().ConfigureAwait(false);
                    return new RabbitMqResponsePublishResult(
                        RabbitMqResponsePublishStatus.Failed,
                        ownedChannel.ConnectionGeneration,
                        new InvalidOperationException("Owned publish channel generation is stale."));
                }

                try
                {
                    using CancellationTokenSource confirmTimeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    confirmTimeoutCts.CancelAfter(TimeSpan.FromSeconds(_options.PublishConfirmTimeoutSeconds));

                    byte[] payload = RabbitMqArticleWorkResponseWireProtocol.SerializeV1(response);
                    BasicProperties properties = new()
                    {
                        ContentType = "application/json",
                        CorrelationId = result.CorrelationId,
                        DeliveryMode = DeliveryModes.Transient,
                    };

                    await ownedChannel.Channel
                        .BasicPublishAsync(
                            exchange: string.Empty,
                            routingKey: result.ReplyTo,
                            mandatory: false,
                            basicProperties: properties,
                            body: payload,
                            confirmTimeoutCts.Token)
                        .ConfigureAwait(false);

                    if (ownedChannel.ConnectionGeneration != _connectionManager.ConnectionGeneration)
                    {
                        await ResetPublishChannelAsync().ConfigureAwait(false);
                        return new RabbitMqResponsePublishResult(
                            RabbitMqResponsePublishStatus.Failed,
                            ownedChannel.ConnectionGeneration,
                            new InvalidOperationException("RabbitMQ connection generation changed during response publication."));
                    }

                    _logger.LogInformation(
                        "RabbitMQ RPC response published and confirmed. RequestId={RequestId} CorrelationId={CorrelationId} MessageId={MessageId} Backbone={Backbone}",
                        result.Request.RequestId,
                        result.CorrelationId,
                        result.Request.MessageId,
                        result.Request.Backbone);

                    return new RabbitMqResponsePublishResult(RabbitMqResponsePublishStatus.Confirmed, ownedChannel.ConnectionGeneration, null);
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    return new RabbitMqResponsePublishResult(RabbitMqResponsePublishStatus.TimedOut, ownedChannel.ConnectionGeneration, null);
                }
                catch (Exception ex)
                {
                    await ResetPublishChannelAsync().ConfigureAwait(false);

                    _logger.LogWarning(
                        ex,
                        "RabbitMQ RPC response publish failed. RequestId={RequestId} CorrelationId={CorrelationId} MessageId={MessageId} Backbone={Backbone}",
                        result.Request.RequestId,
                        result.CorrelationId,
                        result.Request.MessageId,
                        result.Request.Backbone);

                    return new RabbitMqResponsePublishResult(RabbitMqResponsePublishStatus.Failed, ownedChannel.ConnectionGeneration, ex);
                }
            }
            finally
            {
                _ = _publishGate.Release();
            }
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            await _publishGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            try
            {
                await ResetPublishChannelAsync().ConfigureAwait(false);
            }
            finally
            {
                _ = _publishGate.Release();
                _publishGate.Dispose();
            }
        }

        /// <summary>
        /// Performs the get or create publish channel operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private async Task<RabbitMqOwnedChannel?> GetOrCreatePublishChannelAsync(string backbone, CancellationToken cancellationToken)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(backbone);

            long currentGeneration = _connectionManager.ConnectionGeneration;
            if (_ownedPublishChannel is not null && _ownedPublishChannel.ConnectionGeneration == currentGeneration)
            {
                return _ownedPublishChannel;
            }

            await ResetPublishChannelAsync().ConfigureAwait(false);

            try
            {
                _ownedPublishChannel = await _connectionManager
                    .CreateOwnedChannelAsync($"rabbitmq-rpc-response:{backbone}", cancellationToken, enablePublisherConfirmations: true)
                    .ConfigureAwait(false);

                return _ownedPublishChannel;
            }
            catch
            {
                _ownedPublishChannel = null;
                throw;
            }
        }

        /// <summary>
        /// Performs the reset publish channel operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private async ValueTask ResetPublishChannelAsync()
        {
            RabbitMqOwnedChannel? owned = _ownedPublishChannel;
            _ownedPublishChannel = null;
            if (owned is not null)
            {
                await owned.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}
