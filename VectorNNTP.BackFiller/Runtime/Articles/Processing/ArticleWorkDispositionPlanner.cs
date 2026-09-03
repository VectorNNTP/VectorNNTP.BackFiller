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
    /// Translates Phase 3 processing outcomes into the response-publication and ACK/NACK policy applied by Phase 4.
    /// </summary>
    internal sealed class ArticleWorkDispositionPlanner : IArticleWorkDispositionPlanner
    {
        /// <summary>
        /// Creates the RabbitMQ settlement plan for one processed result.
        /// </summary>
        /// <param name="result">Completed processing result to classify for response publication and broker settlement.</param>
        /// <param name="cancellationToken">
        /// Host or operation cancellation token. A canceled token forces requeue semantics so work is retried instead of being treated as terminal.
        /// </param>
        /// <returns>
        /// A deterministic plan that either publishes a terminal RPC response before settlement or requests broker requeue for transient and shutdown paths.
        /// </returns>
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
