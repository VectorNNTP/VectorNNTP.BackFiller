// <copyright file="ArticleWorkDispositionPlanner.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
// Architectural responsibility: article work disposition planner in the articles processing subsystem.
// The file owns this boundary; executable behavior is intentionally unchanged.

// <copyright file="ArticleWorkDispositionPlanner.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Processing
// Maps deterministic processing outcomes to explicit RabbitMQ disposition plans and
// response-publication requirements.

namespace VectorNNTP.Backfiller.Runtime.Articles.Processing
{
    /// <summary>
    /// Default deterministic disposition planner for article-work outcomes.
    /// </summary>
    internal sealed class ArticleWorkDispositionPlanner : IArticleWorkDispositionPlanner
    {
        /// <inheritdoc/>
        public RabbitMqDispositionPlan CreatePlan(ArticleWorkProcessingResult result, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(result);

            if (cancellationToken.IsCancellationRequested)
            {
                return new RabbitMqDispositionPlan(
                    Action: RabbitMqDispositionAction.Nack,
                    Requeue: true,
                    PublishResponse: false);
            }

            return result.Outcome switch
            {
                ArticleWorkProcessingOutcome.Success => new RabbitMqDispositionPlan(
                    Action: RabbitMqDispositionAction.Ack,
                    Requeue: false,
                    PublishResponse: true),

                ArticleWorkProcessingOutcome.ArticleNotFound => new RabbitMqDispositionPlan(
                    Action: RabbitMqDispositionAction.Nack,
                    Requeue: false,
                    PublishResponse: true),

                ArticleWorkProcessingOutcome.InvalidArticle => new RabbitMqDispositionPlan(
                    Action: RabbitMqDispositionAction.Nack,
                    Requeue: false,
                    PublishResponse: true),

                ArticleWorkProcessingOutcome.InvalidRequest => new RabbitMqDispositionPlan(
                    Action: RabbitMqDispositionAction.Nack,
                    Requeue: false,
                    PublishResponse: true),

                ArticleWorkProcessingOutcome.ProviderFailure => new RabbitMqDispositionPlan(
                    Action: RabbitMqDispositionAction.Nack,
                    Requeue: true,
                    PublishResponse: false),

                ArticleWorkProcessingOutcome.Cancelled => new RabbitMqDispositionPlan(
                    Action: RabbitMqDispositionAction.Nack,
                    Requeue: true,
                    PublishResponse: false),

                _ => new RabbitMqDispositionPlan(
                    Action: RabbitMqDispositionAction.Nack,
                    Requeue: true,
                    PublishResponse: false),
            };
        }
    }
}
