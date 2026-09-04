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
    /// Executes Phase 4 RPC publication and broker settlement for processed article-work results.
    /// </summary>
    /// <remarks>
    /// When a terminal response is required, the sink waits for broker confirmation before acknowledging or dropping the delivery. The result object is always disposed when processing completes.
    /// </remarks>
    internal sealed partial class RabbitMqArticleResultSink : IArticleWorkResultSink
    {
        /// <summary>
        /// Planner that maps processing results to broker settlement and response-publication requirements.
        /// </summary>
        private readonly IArticleWorkDispositionPlanner _planner;
        /// <summary>
        /// Factory that produces terminal JSON response payloads for caller-visible outcomes.
        /// </summary>
        private readonly IArticleWorkResponseFactory _responseFactory;
        /// <summary>
        /// Publisher that emits response payloads to RabbitMQ and waits for broker confirmation.
        /// </summary>
        private readonly IRabbitMqArticleResponsePublisher _responsePublisher;
        /// <summary>
        /// Supplies the logger used by rabbit mq article result sink.
        /// </summary>
        private readonly ILogger<RabbitMqArticleResultSink> _logger;

        /// <summary>
        /// Initializes a sink that owns final response-publication and delivery-settlement orchestration.
        /// </summary>
        /// <param name="planner">Planner that maps processing outcomes and cancellation into broker actions.</param>
        /// <param name="responseFactory">Factory that builds terminal RPC response payloads when required.</param>
        /// <param name="responsePublisher">Publisher that emits and confirms RPC responses on RabbitMQ.</param>
        /// <param name="logger">Logger used for publication fallback and settlement diagnostics.</param>
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

        /// <summary>
        /// Applies the Phase 4 publication and settlement policy for one completed processing result.
        /// </summary>
        /// <param name="result">Completed result whose response publication and broker settlement must now be finalized.</param>
        /// <param name="cancellationToken">Cancellation token for response publication and settlement operations.</param>
        /// <returns>A value task that completes after the result has been published or settled according to policy.</returns>
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
                        LogRabbitMqResponsePublishNotConfirmedRequeue(
                            _logger,
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
                    LogRabbitMqDeliveryAcknowledged(
                        _logger,
                        result.Request.RequestId,
                        result.CorrelationId,
                        result.Request.MessageId,
                        result.Request.Backbone,
                        result.Delivery.DeliveryTag);
                }
                else
                {
                    await result.Delivery.Settlement.NackAsync(plan.Requeue, cancellationToken).ConfigureAwait(false);
                    LogRabbitMqDeliveryNegativelyAcknowledged(
                        _logger,
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

        /// <summary>
        /// Emits the response publish-not-confirmed requeue log event.
        /// </summary>
        [LoggerMessage(
            Level = LogLevel.Warning,
            Message = "RabbitMQ response publish was not confirmed; request will be requeued. RequestId={RequestId} CorrelationId={CorrelationId} MessageId={MessageId} Backbone={Backbone} PublishStatus={PublishStatus}")]
        private static partial void LogRabbitMqResponsePublishNotConfirmedRequeue(
            ILogger logger,
            Guid requestId,
            string? correlationId,
            string messageId,
            string backbone,
            RabbitMqResponsePublishStatus publishStatus);

        /// <summary>
        /// Emits the RabbitMQ delivery acknowledged log event.
        /// </summary>
        [LoggerMessage(
            Level = LogLevel.Information,
            Message = "RabbitMQ delivery acknowledged. RequestId={RequestId} CorrelationId={CorrelationId} MessageId={MessageId} Backbone={Backbone} DeliveryTag={DeliveryTag}")]
        private static partial void LogRabbitMqDeliveryAcknowledged(
            ILogger logger,
            Guid requestId,
            string? correlationId,
            string messageId,
            string backbone,
            ulong deliveryTag);

        /// <summary>
        /// Emits the RabbitMQ delivery negatively acknowledged log event.
        /// </summary>
        [LoggerMessage(
            Level = LogLevel.Information,
            Message = "RabbitMQ delivery negatively acknowledged. RequestId={RequestId} CorrelationId={CorrelationId} MessageId={MessageId} Backbone={Backbone} DeliveryTag={DeliveryTag} Requeue={Requeue}")]
        private static partial void LogRabbitMqDeliveryNegativelyAcknowledged(
            ILogger logger,
            Guid requestId,
            string? correlationId,
            string messageId,
            string backbone,
            ulong deliveryTag,
            bool requeue);
    }
}
