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
    /// Runs the Phase 3 article-processing loop over RabbitMQ delivery envelopes.
    /// </summary>
    internal sealed partial class RabbitMqArticleProcessingService : BackgroundService
    {
        /// <summary>
        /// Stores the consumer service state used to enforce this component's runtime contract.
        /// </summary>
        private readonly RabbitMqConsumerService _consumerService;
        /// <summary>
        /// Stores the request parser state used to enforce this component's runtime contract.
        /// </summary>
        private readonly IRabbitMqArticleWorkRequestParser _requestParser;
        /// <summary>
        /// Stores the processor state used to enforce this component's runtime contract.
        /// </summary>
        private readonly IArticleWorkProcessor _processor;
        /// <summary>
        /// Stores the result sink state used to enforce this component's runtime contract.
        /// </summary>
        private readonly IArticleWorkResultSink _resultSink;
        /// <summary>
        /// Stores the logger state used to enforce this component's runtime contract.
        /// </summary>
        private readonly ILogger<RabbitMqArticleProcessingService> _logger;

        /// <summary>
        /// Initializes a new processing hosted service instance.
        /// </summary>
        /// <param name="consumerService">RabbitMQ consumer service exposing delivery reader handoff.</param>
        /// <param name="requestParser">Delivery payload parser.</param>
        /// <param name="processor">Article-work processor.</param>
        /// <param name="resultSink">Result sink boundary for future ACK/NACK/RPC integration.</param>
        /// <param name="logger">Logger.</param>
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

        /// <inheritdoc/>
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
                    _logger.LogInformation(
                        "Article processing result forwarded. RequestId={RequestId} CorrelationId={CorrelationId} MessageId={MessageId} Backbone={Backbone} Outcome={Outcome} Disposition={Disposition} Redelivered={Redelivered}",
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
        /// Performs the create linked token source operation while preserving this component's lifecycle and state contracts.
        /// </summary>
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

    }

}
