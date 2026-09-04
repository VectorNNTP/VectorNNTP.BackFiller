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
    /// Executes one backbone retrieval workflow and normalizes its result into Phase 3 outcome and disposition classifications.
    /// </summary>
    /// <remarks>
    /// The processor disposes any acquired NNTP session lease before returning so the result object carries only grabber-result ownership into later phases.
    /// </remarks>
    internal sealed partial class ArticleWorkProcessor : IArticleWorkProcessor
    {
        /// <summary>
        /// Retrieval boundary that acquires a backbone session and runs the underlying grabber workflow.
        /// </summary>
        private readonly IBackboneArticleRetriever _retriever;
        /// <summary>
        /// Supplies the logger used by article work processor.
        /// </summary>
        private readonly ILogger<ArticleWorkProcessor> _logger;

        /// <summary>
        /// Initializes a processor bound to the backbone retrieval adapter and workflow diagnostics logger.
        /// </summary>
        /// <param name="retriever">Retrieval boundary that acquires a backbone session, runs the grabber workflow, and returns a classified result.</param>
        /// <param name="logger">Logger used for terminal per-request outcome diagnostics.</param>
        public ArticleWorkProcessor(
            IBackboneArticleRetriever retriever,
            ILogger<ArticleWorkProcessor> logger)
        {
            _retriever = retriever ?? throw new ArgumentNullException(nameof(retriever));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Processes one parsed request by running backbone retrieval and translating the grabber outcome into Phase 3 classifications.
        /// </summary>
        /// <param name="request">Parsed article-work request.</param>
        /// <param name="delivery">Authoritative delivery envelope that supplies AMQP correlation and settlement context.</param>
        /// <param name="cancellationToken">Cancellation token for retrieval and classification work.</param>
        /// <returns>
        /// A processing result that captures the terminal outcome, deferred disposition recommendation, and any grabber payload ownership transferred to later phases.
        /// </returns>
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

                    LogArticleProcessingCompleted(
                        _logger,
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
                LogArticleProcessingFailedUnexpectedly(ex, _logger, request.RequestId, delivery.CorrelationId, request.MessageId, request.Backbone);
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
        /// Maps grabber workflow failures into terminal Phase 3 outcome classes and downstream RabbitMQ settlement guidance.
        /// </summary>
        /// <param name="result">Grabber workflow result whose deterministic failure code is being translated.</param>
        /// <returns>
        /// The externally reported outcome together with the recommended broker action: terminal content errors drop, transient provider failures requeue, and success acknowledges.
        /// </returns>
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

        /// <summary>
        /// Emits the article processing completed log event.
        /// </summary>
        [LoggerMessage(
            Level = LogLevel.Information,
            Message = "Article processing completed. RequestId={RequestId} CorrelationId={CorrelationId} MessageId={MessageId} Backbone={Backbone} Outcome={Outcome} Disposition={Disposition} ProviderFailureCode={ProviderFailureCode} ResponseCode={ResponseCode}")]
        private static partial void LogArticleProcessingCompleted(
            ILogger logger,
            Guid requestId,
            string? correlationId,
            string messageId,
            string backbone,
            ArticleWorkProcessingOutcome outcome,
            ArticleWorkDispositionRecommendation disposition,
            NntpArticleAcquisitionFailureCode? providerFailureCode,
            int? responseCode);

        /// <summary>
        /// Emits the article processing failed unexpectedly log event.
        /// </summary>
        [LoggerMessage(
            Level = LogLevel.Error,
            Message = "Article processing failed unexpectedly. RequestId={RequestId} CorrelationId={CorrelationId} MessageId={MessageId} Backbone={Backbone}")]
        private static partial void LogArticleProcessingFailedUnexpectedly(
            Exception exception,
            ILogger logger,
            Guid requestId,
            string? correlationId,
            string messageId,
            string backbone);
    }
}
