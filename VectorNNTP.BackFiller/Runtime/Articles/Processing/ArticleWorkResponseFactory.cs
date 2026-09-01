// <copyright file="ArticleWorkResponseFactory.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Processing
// Constructs terminal RabbitMQ RPC response payloads for deterministic processing outcomes.

namespace VectorNNTP.Backfiller.Runtime.Articles.Processing
{
    /// <summary>
    /// Default response factory for terminal article-work outcomes.
    /// </summary>
    internal sealed class ArticleWorkResponseFactory : IArticleWorkResponseFactory
    {
        private const int ResponseVersion = 1;

        /// <inheritdoc/>
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

                _ => null,
            };
        }
    }
}
