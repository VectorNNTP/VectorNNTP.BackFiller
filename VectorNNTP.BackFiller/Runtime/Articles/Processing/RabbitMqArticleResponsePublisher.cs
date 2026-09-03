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
    /// Publishes article-work RPC responses on RabbitMQ and waits for broker confirmation before Phase 4 can settle the source delivery.
    /// </summary>
    /// <remarks>
    /// A single owned publish channel is reused behind a semaphore so concurrent callers serialize confirm-sensitive work and stale channels can be discarded when the connection generation changes.
    /// </remarks>
    internal sealed class RabbitMqArticleResponsePublisher : IRabbitMqArticleResponsePublisher, IAsyncDisposable
    {
        /// <summary>
        /// Connection manager that creates and recovers owned RabbitMQ channels.
        /// </summary>
        private readonly RabbitMqConnectionManager _connectionManager;
        /// <summary>
        /// Validated RabbitMQ runtime options, including publish-confirm timeout policy.
        /// </summary>
        private readonly RabbitMqRuntimeOptions _options;
        /// <summary>
        /// Supplies the logger used by rabbit mq article response publisher.
        /// </summary>
        private readonly ILogger<RabbitMqArticleResponsePublisher> _logger;
        /// <summary>
        /// Serializes access to the cached owned publish channel and its confirmation sequence.
        /// </summary>
        private readonly SemaphoreSlim _publishGate = new(1, 1);
        /// <summary>
        /// Cached owned channel currently used for RPC response publication, when one is available.
        /// </summary>
        private RabbitMqOwnedChannel? _ownedPublishChannel;

        /// <summary>
        /// Initializes a response publisher bound to the validated runtime snapshot and RabbitMQ connection manager.
        /// </summary>
        /// <param name="runtimeOptions">Validated runtime options that supply RabbitMQ publish-confirm timeout configuration.</param>
        /// <param name="connectionManager">RabbitMQ connection manager that creates owned publish channels.</param>
        /// <param name="logger">Logger used for publish failure diagnostics.</param>
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

        /// <summary>
        /// Publishes one response payload to the delivery's reply queue and waits for broker confirmation.
        /// </summary>
        /// <param name="result">Processed result that supplies authoritative reply queue, correlation identifier, and source connection generation.</param>
        /// <param name="response">Application-level response body to publish.</param>
        /// <param name="cancellationToken">Cancellation token for gate acquisition, channel acquisition, publish, and confirm waiting.</param>
        /// <returns>A publish result describing confirmed, timed-out, or failed publication.</returns>
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

        /// <summary>
        /// Disposes the owned publish channel and the serialization gate.
        /// </summary>
        /// <returns>A value task that completes after any cached publish channel has been released.</returns>
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
        /// Returns the cached owned publish channel for the current connection generation, or creates a new one when needed.
        /// </summary>
        /// <param name="backbone">Backbone name used only to label the owned channel lease in connection-manager diagnostics.</param>
        /// <param name="cancellationToken">Cancellation token for channel creation.</param>
        /// <returns>The owned publish channel for the current connection generation.</returns>
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
        /// Disposes the cached publish channel and clears the local reference.
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
