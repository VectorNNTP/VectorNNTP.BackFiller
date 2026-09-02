// <copyright file="ArticleWorkProcessor.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Processing
// Deterministic article-work processor that maps existing grabber workflow results into
// explicit Phase 3 outcomes and deferred RabbitMQ disposition recommendations.

using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Grabber;
using VectorNNTP.Backfiller.Runtime.RabbitMq;

namespace VectorNNTP.Backfiller.Runtime.Articles.Processing
{
    /// <summary>
    /// Default article-work processor implementation for Phase 3.
    /// </summary>
    internal sealed partial class ArticleWorkProcessor : IArticleWorkProcessor
    {
        /// <summary>
        /// Stores the retriever state used to enforce this component's runtime contract.
        /// </summary>
        private readonly IBackboneArticleRetriever _retriever;
        /// <summary>
        /// Stores the logger state used to enforce this component's runtime contract.
        /// </summary>
        private readonly ILogger<ArticleWorkProcessor> _logger;

        /// <summary>
        /// Initializes a new article-work processor.
        /// </summary>
        /// <param name="retriever">Backbone retrieval adapter using existing session/workflow architecture.</param>
        /// <param name="logger">Logger.</param>
        public ArticleWorkProcessor(
            IBackboneArticleRetriever retriever,
            ILogger<ArticleWorkProcessor> logger)
        {
            _retriever = retriever ?? throw new ArgumentNullException(nameof(retriever));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <inheritdoc/>
        public async ValueTask<ArticleWorkProcessingResult> ProcessAsync(RabbitMqArticleWorkRequest request, RabbitMqArticleDelivery delivery, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(request);
            ArgumentNullException.ThrowIfNull(delivery);

            try
            {
                BackboneArticleRetrievalResult retrieval = await _retriever.RetrieveAsync(request, cancellationToken).ConfigureAwait(false);
                NntpArticleSessionLease? lease = retrieval.Lease;

                try
                {
                    NntpArticleGrabberResult grabberResult = retrieval.GrabberResult;
                    (ArticleWorkProcessingOutcome outcome, ArticleWorkDispositionRecommendation disposition) = Classify(grabberResult);
                    ArticleWorkProcessingResult result = new(
                        Request: request,
                        Delivery: delivery,
                        Outcome: outcome,
                        Disposition: disposition,
                        GrabberResult: grabberResult,
                        ProviderFailureCode: grabberResult.AcquisitionFailureCode,
                        ResponseCode: grabberResult.ResponseCode,
                        ResponseText: grabberResult.ResponseText,
                        UnexpectedException: null);

                    _logger.LogInformation(
                        "Article processing completed. RequestId={RequestId} CorrelationId={CorrelationId} MessageId={MessageId} Backbone={Backbone} Outcome={Outcome} Disposition={Disposition} ProviderFailureCode={ProviderFailureCode} ResponseCode={ResponseCode}",
                        request.RequestId,
                        result.CorrelationId,
                        request.MessageId,
                        request.Backbone,
                        result.Outcome,
                        result.Disposition,
                        result.ProviderFailureCode,
                        result.ResponseCode);

                    return result;
                }
                finally
                {
                    if (lease is not null)
                    {
                        await lease.DisposeAsync().ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return new ArticleWorkProcessingResult(
                    Request: request,
                    Delivery: delivery,
                    Outcome: ArticleWorkProcessingOutcome.Cancelled,
                    Disposition: ArticleWorkDispositionRecommendation.None,
                    GrabberResult: null,
                    ProviderFailureCode: NntpArticleAcquisitionFailureCode.Cancelled,
                    ResponseCode: null,
                    ResponseText: "Processing canceled.",
                    UnexpectedException: null);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Article processing failed unexpectedly. RequestId={RequestId} CorrelationId={CorrelationId} MessageId={MessageId} Backbone={Backbone}", request.RequestId, delivery.CorrelationId, request.MessageId, request.Backbone);
                return new ArticleWorkProcessingResult(
                    Request: request,
                    Delivery: delivery,
                    Outcome: ArticleWorkProcessingOutcome.UnexpectedFailure,
                    Disposition: ArticleWorkDispositionRecommendation.None,
                    GrabberResult: null,
                    ProviderFailureCode: null,
                    ResponseCode: null,
                    ResponseText: ex.Message,
                    UnexpectedException: ex);
            }
        }

        /// <summary>
        /// Performs the static operation while preserving this component's lifecycle and state contracts.
        /// </summary>
        private static (ArticleWorkProcessingOutcome Outcome, ArticleWorkDispositionRecommendation Disposition) Classify(NntpArticleGrabberResult result)
        {
            return result.FailureCode switch
            {
                NntpArticleGrabberFailureCode.None => (ArticleWorkProcessingOutcome.Success, ArticleWorkDispositionRecommendation.Ack),
                NntpArticleGrabberFailureCode.InvalidMessageId => (ArticleWorkProcessingOutcome.InvalidRequest, ArticleWorkDispositionRecommendation.NackDrop),
                NntpArticleGrabberFailureCode.ArticleNotFound => (ArticleWorkProcessingOutcome.ArticleNotFound, ArticleWorkDispositionRecommendation.NackDrop),
                NntpArticleGrabberFailureCode.MalformedArticle => (ArticleWorkProcessingOutcome.InvalidArticle, ArticleWorkDispositionRecommendation.NackDrop),
                NntpArticleGrabberFailureCode.InvalidDate => (ArticleWorkProcessingOutcome.InvalidArticle, ArticleWorkDispositionRecommendation.NackDrop),
                NntpArticleGrabberFailureCode.InvalidHeaders => (ArticleWorkProcessingOutcome.InvalidArticle, ArticleWorkDispositionRecommendation.NackDrop),
                NntpArticleGrabberFailureCode.YEncValidationFailure => (ArticleWorkProcessingOutcome.InvalidArticle, ArticleWorkDispositionRecommendation.NackDrop),
                NntpArticleGrabberFailureCode.Cancelled => (ArticleWorkProcessingOutcome.Cancelled, ArticleWorkDispositionRecommendation.None),
                NntpArticleGrabberFailureCode.AuthenticationFailure => (ArticleWorkProcessingOutcome.ProviderFailure, ArticleWorkDispositionRecommendation.NackRequeue),
                NntpArticleGrabberFailureCode.ConnectionFailure => (ArticleWorkProcessingOutcome.ProviderFailure, ArticleWorkDispositionRecommendation.NackRequeue),
                NntpArticleGrabberFailureCode.Timeout => (ArticleWorkProcessingOutcome.ProviderFailure, ArticleWorkDispositionRecommendation.NackRequeue),
                NntpArticleGrabberFailureCode.ProtocolFailure => (ArticleWorkProcessingOutcome.ProviderFailure, ArticleWorkDispositionRecommendation.NackRequeue),
                NntpArticleGrabberFailureCode.ArticleFramingFailure => (ArticleWorkProcessingOutcome.ProviderFailure, ArticleWorkDispositionRecommendation.NackRequeue),
                NntpArticleGrabberFailureCode.AcquisitionFailure => (ArticleWorkProcessingOutcome.ProviderFailure, ArticleWorkDispositionRecommendation.NackRequeue),
                _ => (ArticleWorkProcessingOutcome.UnexpectedFailure, ArticleWorkDispositionRecommendation.None),
            };
        }

    }
}
