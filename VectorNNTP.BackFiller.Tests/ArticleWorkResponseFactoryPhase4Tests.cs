// <copyright file="ArticleWorkResponseFactoryPhase4Tests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for article work response factory phase4, covering NNTP article and transport behavior.
// Primary responsibility: documents the executable contracts covered by the article work response factory phase 4 test suite.

using System.Text;
using VectorNNTP.Backfiller.Runtime.Articles.Processing;
using VectorNNTP.Backfiller.Runtime.RabbitMq;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Verifies canonical response factory mapping for terminal and retryable outcomes.
    /// </summary>
    public sealed class ArticleWorkResponseFactoryPhase4Tests
    {
        /// <summary>
        /// Confirms the create response maps outcome to canonical payload behavior.
        /// </summary>
        [Theory]
        [InlineData(0, true, false)]
        [InlineData(2, false, true)]
        [InlineData(4, false, true)]
        [InlineData(1, false, true)]
        [InlineData(3, false, false)]
        [InlineData(5, false, false)]
        [InlineData(6, false, false)]
        public void CreateResponse_MapsOutcomeToCanonicalPayload(int outcomeValue, bool expectUriField, bool expectError)
        {
            ArticleWorkResponseFactory factory = new();
            ArticleWorkProcessingOutcome outcome = (ArticleWorkProcessingOutcome)outcomeValue;
            Guid requestId = Guid.NewGuid();
            ArticleWorkProcessingResult result = CreateResult(
                outcome,
                requestId,
                "<factory@example.invalid>",
                "BackboneA",
                responseText: "terminal error");

            RabbitMqArticleWorkResponse? response = factory.CreateResponse(result);

            if (!expectUriField && !expectError)
            {
                Assert.Null(response);
                return;
            }

            RabbitMqArticleWorkResponse payload = Assert.IsType<RabbitMqArticleWorkResponse>(response);
            Assert.Equal(1, payload.Version);
            Assert.Equal(requestId, payload.RequestId);
            Assert.Equal("<factory@example.invalid>", payload.MessageId);
            Assert.Equal("BackboneA", payload.Backbone);
            Assert.Equal(outcome.ToString(), payload.Outcome);

            if (expectUriField)
            {
                byte[] json = RabbitMqArticleWorkResponseWireProtocol.SerializeV1(payload);
                string text = Encoding.UTF8.GetString(json);
                Assert.Contains("\"uri\":null", text, StringComparison.Ordinal);
                Assert.Null(payload.Error);
            }

            if (expectError)
            {
                Assert.False(string.IsNullOrWhiteSpace(payload.Error));
                Assert.Null(payload.Uri);
            }
        }

        /// <summary>
        /// Confirms the create result behavior.
        /// </summary>
        private static ArticleWorkProcessingResult CreateResult(
            ArticleWorkProcessingOutcome outcome,
            Guid requestId,
            string messageId,
            string backbone,
            string responseText)
        {
            RabbitMqArticleWorkRequest request = new(1, requestId, messageId, backbone);
            RabbitMqArticleDelivery delivery = new(
                Backbone: backbone,
                Queue: "grabbers.backbonea",
                ConsumerTag: "ctag-factory",
                ConsumerIdentity: "consumer-factory",
                DeliveryTag: 321,
                Redelivered: false,
                RoutingKey: "grabbers.backbonea",
                Exchange: "grabbers.backbonea",
                ConnectionGeneration: 7,
                RabbitMqMessageId: "rmq-msgid",
                CorrelationId: "corr-factory",
                ReplyTo: "rpc.responses",
                Payload: Encoding.UTF8.GetBytes("{}"),
                CancellationToken: CancellationToken.None,
                Settlement: new NoOpSettlement());

            return new ArticleWorkProcessingResult(
                Request: request,
                Delivery: delivery,
                Outcome: outcome,
                Disposition: ArticleWorkDispositionRecommendation.None,
                GrabberResult: null,
                ProviderFailureCode: null,
                ResponseCode: null,
                ResponseText: responseText,
                UnexpectedException: null);
        }

        /// <summary>
        /// Confirms the no op settlement behavior.
        /// </summary>
        private sealed class NoOpSettlement : IRabbitMqDeliverySettlement
        {
            /// <summary>
            /// Confirms the ack async behavior.
            /// </summary>
            /// <returns>The value returned by the ack async helper.</returns>
            /// <summary>
            /// Confirms the ack async behavior.
            /// </summary>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the ack async helper.</returns>
            public ValueTask AckAsync(CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return ValueTask.CompletedTask;
            }

            /// <summary>
            /// Confirms the nack async behavior.
            /// </summary>
            /// <returns>The value returned by the nack async helper.</returns>
            /// <summary>
            /// Confirms the nack async behavior.
            /// </summary>
            /// <param name="requeue">The requeue used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the nack async helper.</returns>
            public ValueTask NackAsync(bool requeue, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = requeue;
                return ValueTask.CompletedTask;
            }
        }
    }
}
