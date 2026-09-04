// <copyright file="RabbitMqArticleProcessingService.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Processing
// Hosted processing loop that consumes RabbitMQ deliveries from Phase 2 infrastructure,
// parses Message-ID work requests, executes provider retrieval/classification, and emits
// explicit Phase 3 processing results without directly performing ACK/NACK or RPC publish.

using VectorNNTP.Backfiller.Runtime.RabbitMq;

namespace VectorNNTP.Backfiller.Runtime.Articles.Processing
{
    /// <summary>
    /// Hosts the Phase 3 loop that parses admitted RabbitMQ deliveries, executes article retrieval, and forwards deterministic results to Phase 4.
    /// </summary>
    /// <remarks>
    /// The service does not settle deliveries itself. It links host shutdown with per-delivery cancellation so downstream processing can classify cancellation without losing delivery identity.
    /// </remarks>
    internal sealed partial class RabbitMqArticleProcessingService : BackgroundService
    {
        /// <summary>
        /// Consumer infrastructure that owns admitted-delivery buffering for the processing loop.
        /// </summary>
        private readonly RabbitMqConsumerService _consumerService;
        /// <summary>
        /// Request parser that validates the JSON body and required AMQP RPC properties.
        /// </summary>
        private readonly IRabbitMqArticleWorkRequestParser _requestParser;
        /// <summary>
        /// Processor that performs backbone retrieval and deterministic outcome classification.
        /// </summary>
        private readonly IArticleWorkProcessor _processor;
        /// <summary>
        /// Result sink that owns Phase 4 publication and settlement policy.
        /// </summary>
        private readonly IArticleWorkResultSink _resultSink;
        /// <summary>
        /// Supplies the logger used by rabbit mq article processing service.
        /// </summary>
        private readonly ILogger<RabbitMqArticleProcessingService> _logger;

        /// <summary>
        /// Initializes the hosted processing loop and its Phase 3/Phase 4 collaborators.
        /// </summary>
        /// <param name="consumerService">RabbitMQ consumer service that owns delivery admission and exposes the bounded delivery reader.</param>
        /// <param name="requestParser">Parser that validates the JSON application payload and required AMQP RPC properties.</param>
        /// <param name="processor">Processor that executes backbone retrieval and outcome classification for valid requests.</param>
        /// <param name="resultSink">Sink that receives completed results for RPC publication and final broker settlement.</param>
        /// <param name="logger">Logger used for per-delivery forwarding diagnostics.</param>
        public RabbitMqArticleProcessingService(
            RabbitMqConsumerService consumerService,
            IRabbitMqArticleWorkRequestParser requestParser,
            IArticleWorkProcessor processor,
            IArticleWorkResultSink resultSink,
            ILogger<RabbitMqArticleProcessingService> logger)
        {
            _consumerService = consumerService ?? throw new ArgumentNullException(nameof(consumerService));
            _requestParser = requestParser ?? throw new ArgumentNullException(nameof(requestParser));
            _processor = processor ?? throw new ArgumentNullException(nameof(processor));
            _resultSink = resultSink ?? throw new ArgumentNullException(nameof(resultSink));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Runs the delivery-processing loop until the host requests shutdown.
        /// </summary>
        /// <param name="stoppingToken">Host stopping token that halts reader waits and per-delivery processing.</param>
        /// <returns>A task that completes after admitted deliveries stop being consumed.</returns>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (await _consumerService.DeliveryReader.WaitToReadAsync(stoppingToken).ConfigureAwait(false))
            {
                while (_consumerService.DeliveryReader.TryRead(out RabbitMqArticleDelivery? delivery))
                {
                    stoppingToken.ThrowIfCancellationRequested();
                    if (delivery.CancellationToken.IsCancellationRequested)
                    {
                        continue;
                    }

                    using CancellationTokenSource? linkedCts = CreateLinkedTokenSource(stoppingToken, delivery.CancellationToken);
                    CancellationToken operationToken = linkedCts?.Token ?? stoppingToken;

                    RabbitMqArticleWorkParseResult parseResult = await _requestParser
                        .ParseAsync(delivery, operationToken)
                        .ConfigureAwait(false);

                    ArticleWorkProcessingResult result;
                    if (parseResult.IsSuccess)
                    {
                        result = await _processor.ProcessAsync(parseResult.Request!, delivery, operationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        result = parseResult.Failure ?? throw new InvalidOperationException("Parse failure result was not provided.");
                    }

                    await _resultSink.OnProcessedAsync(result, operationToken).ConfigureAwait(false);
                    LogArticleProcessingResultForwarded(
                        _logger,
                        result.Request.RequestId,
                        result.CorrelationId,
                        result.Request.MessageId,
                        result.Request.Backbone,
                        result.Outcome,
                        result.Disposition,
                        result.Delivery.Redelivered);
                }
            }
        }

        /// <summary>
        /// Creates the effective operation token for one delivery by combining host shutdown and per-delivery cancellation when both matter.
        /// </summary>
        /// <remarks>
        /// When the delivery token is identical to the host token or cannot be canceled, the method avoids allocating a linked source and the caller continues with the host token directly.
        /// </remarks>
        /// <param name="hostToken">Host-level stopping token for the background service.</param>
        /// <param name="deliveryToken">Per-delivery token supplied by the admitting consumer session.</param>
        /// <returns>A linked token source when both inputs should participate in cancellation, or <see langword="null"/> when no additional source is required.</returns>
        private static CancellationTokenSource? CreateLinkedTokenSource(CancellationToken hostToken, CancellationToken deliveryToken)
        {
            if (!deliveryToken.CanBeCanceled || deliveryToken == hostToken)
            {
                return null;
            }

            if (!hostToken.CanBeCanceled)
            {
                return CancellationTokenSource.CreateLinkedTokenSource(deliveryToken);
            }

            return CancellationTokenSource.CreateLinkedTokenSource(hostToken, deliveryToken);
        }

        /// <summary>
        /// Emits the article processing result forwarded log event.
        /// </summary>
        [LoggerMessage(
            Level = LogLevel.Information,
            Message = "Article processing result forwarded. RequestId={RequestId} CorrelationId={CorrelationId} MessageId={MessageId} Backbone={Backbone} Outcome={Outcome} Disposition={Disposition} Redelivered={Redelivered}")]
        private static partial void LogArticleProcessingResultForwarded(
            ILogger logger,
            Guid requestId,
            string? correlationId,
            string messageId,
            string backbone,
            ArticleWorkProcessingOutcome outcome,
            ArticleWorkDispositionRecommendation disposition,
            bool redelivered);

    }

}
