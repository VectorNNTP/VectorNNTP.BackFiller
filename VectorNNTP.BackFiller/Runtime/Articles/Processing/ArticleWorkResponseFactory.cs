// <copyright file="ArticleWorkResponseFactory.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Processing
// Constructs terminal RabbitMQ RPC response payloads for deterministic processing outcomes.

namespace VectorNNTP.Backfiller.Runtime.Articles.Processing
{
    /// <summary>
    /// Builds the application-level RPC payload for outcomes that are terminal from the caller's perspective.
    /// </summary>
    /// <remarks>
    /// Transient provider failures, cancellation, and unexpected failures intentionally return <see langword="null"/> so the broker can requeue without emitting a misleading terminal response.
    /// </remarks>
    internal sealed class ArticleWorkResponseFactory : IArticleWorkResponseFactory
    {
        /// <summary>
        /// Canonical response schema version written into every emitted RPC payload.
        /// </summary>
        private const int ResponseVersion = 1;

        /// <summary>
        /// Creates the version-1 RPC response payload for a terminal processing result when one should be published.
        /// </summary>
        /// <param name="result">Completed processing result.</param>
        /// <returns>
        /// A compact response payload for success and terminal request/content failures, or <see langword="null"/> when the delivery should instead be retried.
        /// </returns>
        public RabbitMqArticleWorkResponse? CreateResponse(ArticleWorkProcessingResult result)
        {
            ArgumentNullException.ThrowIfNull(result);

            return result.Outcome switch
            {
                ArticleWorkProcessingOutcome.Success => new RabbitMqArticleWorkResponse(
                    Version: ResponseVersion,
                    RequestId: result.Request.RequestId,
                    MessageId: result.Request.MessageId,
                    Backbone: result.Request.Backbone,
                    Outcome: result.Outcome.ToString(),
                    Uri: null,
                    Error: null),

                ArticleWorkProcessingOutcome.ArticleNotFound => new RabbitMqArticleWorkResponse(
                    Version: ResponseVersion,
                    RequestId: result.Request.RequestId,
                    MessageId: result.Request.MessageId,
                    Backbone: result.Request.Backbone,
                    Outcome: result.Outcome.ToString(),
                    Uri: null,
                    Error: result.ResponseText ?? "Article not found."),

                ArticleWorkProcessingOutcome.InvalidArticle => new RabbitMqArticleWorkResponse(
                    Version: ResponseVersion,
                    RequestId: result.Request.RequestId,
                    MessageId: result.Request.MessageId,
                    Backbone: result.Request.Backbone,
                    Outcome: result.Outcome.ToString(),
                    Uri: null,
                    Error: result.ResponseText ?? "Article content was invalid."),

                ArticleWorkProcessingOutcome.InvalidRequest => new RabbitMqArticleWorkResponse(
                    Version: ResponseVersion,
                    RequestId: result.Request.RequestId,
                    MessageId: result.Request.MessageId,
                    Backbone: result.Request.Backbone,
                    Outcome: result.Outcome.ToString(),
                    Uri: null,
                    Error: result.ResponseText ?? "Request payload was invalid."),

                ArticleWorkProcessingOutcome.ProviderFailure => null,

                ArticleWorkProcessingOutcome.Cancelled => null,

                ArticleWorkProcessingOutcome.UnexpectedFailure => null,

                _ => null,
            };
        }
    }
}
