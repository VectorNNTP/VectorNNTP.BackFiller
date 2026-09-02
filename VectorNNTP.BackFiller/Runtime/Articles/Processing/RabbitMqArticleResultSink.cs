// <copyright file="RabbitMqArticleResultSink.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Processing
// Executes RabbitMQ RPC response publishing and final ACK/NACK settlement for processed
// article-work deliveries.

namespace VectorNNTP.Backfiller.Runtime.Articles.Processing
{
    /// <summary>
    /// Default Phase 4 result sink that enforces response-confirm-before-ACK ordering.
    /// </summary>
    internal sealed class RabbitMqArticleResultSink : IArticleWorkResultSink
    {
        /// <summary>
        /// Tracks planner for rabbit mq article result sink.
        /// </summary>
        private readonly IArticleWorkDispositionPlanner _planner;
        /// <summary>
        /// Tracks response factory for rabbit mq article result sink.
        /// </summary>
        private readonly IArticleWorkResponseFactory _responseFactory;
        /// <summary>
        /// Tracks response publisher for rabbit mq article result sink.
        /// </summary>
        private readonly IRabbitMqArticleResponsePublisher _responsePublisher;
        /// <summary>
        /// Provides logging for rabbit mq article result sink.
        /// </summary>
        private readonly ILogger<RabbitMqArticleResultSink> _logger;

        /// <summary>
        /// Initializes a new result sink.
        /// </summary>
        public RabbitMqArticleResultSink(
            IArticleWorkDispositionPlanner planner,
            IArticleWorkResponseFactory responseFactory,
            IRabbitMqArticleResponsePublisher responsePublisher,
            ILogger<RabbitMqArticleResultSink> logger)
        {
            _planner = planner ?? throw new ArgumentNullException(nameof(planner));
            _responseFactory = responseFactory ?? throw new ArgumentNullException(nameof(responseFactory));
            _responsePublisher = responsePublisher ?? throw new ArgumentNullException(nameof(responsePublisher));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <inheritdoc/>
        public async ValueTask OnProcessedAsync(ArticleWorkProcessingResult result, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(result);

            RabbitMqDispositionPlan plan = _planner.CreatePlan(result, cancellationToken);

            try
            {
                if (plan.PublishResponse)
                {
                    RabbitMqArticleWorkResponse response = _responseFactory.CreateResponse(result)
                        ?? throw new InvalidOperationException("Response publication was required but no response payload was produced.");

                    RabbitMqResponsePublishResult publishResult = await _responsePublisher
                        .PublishAndConfirmAsync(result, response, cancellationToken)
                        .ConfigureAwait(false);

                    if (publishResult.Status is not RabbitMqResponsePublishStatus.Confirmed)
                    {
                        await result.Delivery.Settlement.NackAsync(requeue: true, cancellationToken).ConfigureAwait(false);
                        _logger.LogWarning(
                            "RabbitMQ response publish was not confirmed; request will be requeued. RequestId={RequestId} CorrelationId={CorrelationId} MessageId={MessageId} Backbone={Backbone} PublishStatus={PublishStatus}",
                            result.Request.RequestId,
                            result.CorrelationId,
                            result.Request.MessageId,
                            result.Request.Backbone,
                            publishResult.Status);
                        return;
                    }
                }

                if (plan.Action is RabbitMqDispositionAction.Ack)
                {
                    await result.Delivery.Settlement.AckAsync(cancellationToken).ConfigureAwait(false);
                    _logger.LogInformation(
                        "RabbitMQ delivery acknowledged. RequestId={RequestId} CorrelationId={CorrelationId} MessageId={MessageId} Backbone={Backbone} DeliveryTag={DeliveryTag}",
                        result.Request.RequestId,
                        result.CorrelationId,
                        result.Request.MessageId,
                        result.Request.Backbone,
                        result.Delivery.DeliveryTag);
                }
                else
                {
                    await result.Delivery.Settlement.NackAsync(plan.Requeue, cancellationToken).ConfigureAwait(false);
                    _logger.LogInformation(
                        "RabbitMQ delivery negatively acknowledged. RequestId={RequestId} CorrelationId={CorrelationId} MessageId={MessageId} Backbone={Backbone} DeliveryTag={DeliveryTag} Requeue={Requeue}",
                        result.Request.RequestId,
                        result.CorrelationId,
                        result.Request.MessageId,
                        result.Request.Backbone,
                        result.Delivery.DeliveryTag,
                        plan.Requeue);
                }
            }
            finally
            {
                result.Dispose();
            }
        }
    }
}
