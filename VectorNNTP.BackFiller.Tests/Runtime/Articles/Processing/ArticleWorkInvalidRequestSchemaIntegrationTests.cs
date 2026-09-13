// <copyright file="ArticleWorkInvalidRequestSchemaIntegrationTests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime / Articles / Processing
// End-to-end invalid-request response contract tests from parser through schema validation.

using NJsonSchema;
using NJsonSchema.Validation;
using VectorNNTP.Backfiller.Configuration;
using VectorNNTP.Backfiller.Runtime.Articles.Processing;
using VectorNNTP.Backfiller.Runtime.RabbitMq;
using Xunit;

namespace VectorNNTP.BackFiller.Tests.Runtime.Articles.Processing
{
    /// <summary>
    /// Validates parser-generated invalid-request responses against the published v1 response schema.
    /// </summary>
    public sealed class ArticleWorkInvalidRequestSchemaIntegrationTests
    {
        private static readonly Lazy<JsonSchema> ResponseSchema = new(LoadSchema);

        [Theory]
        [InlineData("", true, true, true, false, false)]
        [InlineData("{", true, true, true, false, false)]
        [InlineData("[]", true, true, true, false, false)]
        [InlineData("{\"version\":1,\"messageId\":\"<missing-requestid@example.com>\",\"backbone\":\"BackboneA\"}", true, false, false, false, true)]
        [InlineData("{\"version\":1,\"requestId\":\"not-a-guid\",\"messageId\":\"<invalid-requestid@example.com>\",\"backbone\":\"BackboneA\"}", true, false, false, false, true)]
        [InlineData("{\"version\":1,\"requestId\":\"00000000-0000-0000-0000-000000000000\",\"messageId\":\"<guid-empty-requestid@example.com>\",\"backbone\":\"BackboneA\"}", true, false, false, false, true)]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"backbone\":\"BackboneA\"}", false, true, false, true, false)]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"not-message-id\",\"backbone\":\"BackboneA\"}", false, true, false, true, false)]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<missing-backbone@example.com>\"}", false, false, true, true, true)]
        [InlineData("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<mismatch@example.com>\",\"backbone\":\"Eweka\"}", false, false, true, true, true)]
        public async Task ParseToSchema_WhenInvalidRequestAndReplyable_ProducesSchemaValidResponse(
            string payload,
            bool expectRequestIdUnavailable,
            bool expectMessageIdUnavailable,
            bool expectBackboneUnavailable,
            bool expectRequestIdAvailable,
            bool expectMessageIdAvailable)
        {
            RabbitMqArticleDelivery delivery = CreateDelivery(payload, backbone: "BackboneA", correlationId: "corr-invalid-request", replyTo: "rpc.responses");
            RabbitMqArticleWorkRequestParser parser = new();

            RabbitMqArticleWorkParseResult parseResult = await parser.ParseAsync(delivery, CancellationToken.None).ConfigureAwait(false);

            Assert.False(parseResult.IsSuccess);
            ArticleWorkProcessingResult failure = Assert.IsType<ArticleWorkProcessingResult>(parseResult.Failure);
            Assert.Equal(ArticleWorkProcessingOutcome.InvalidRequest, failure.Outcome);
            Assert.Equal(InvalidRequestReplyability.Replyable, failure.InvalidRequestReplyability);
            Assert.False(string.IsNullOrWhiteSpace(failure.ResponseText));

            if (expectRequestIdUnavailable)
            {
                Assert.Null(failure.Request.RequestId);
            }

            if (expectMessageIdUnavailable)
            {
                Assert.Null(failure.Request.MessageId);
            }

            if (expectBackboneUnavailable)
            {
                Assert.Null(failure.Request.Backbone);
            }

            if (expectRequestIdAvailable)
            {
                Assert.NotNull(failure.Request.RequestId);
            }

            if (expectMessageIdAvailable)
            {
                Assert.False(string.IsNullOrWhiteSpace(failure.Request.MessageId));
            }

            ArticleWorkDispositionPlanner planner = new();
            RabbitMqDispositionPlan plan = planner.CreatePlan(failure, CancellationToken.None);
            Assert.True(plan.PublishResponse);

            ArticleWorkResponseFactory responseFactory = new(CreateRuntimeOptions());
            RabbitMqArticleWorkResponse response = Assert.IsType<RabbitMqArticleWorkResponse>(responseFactory.CreateResponse(failure));
            Assert.Equal(nameof(ArticleWorkProcessingOutcome.InvalidRequest), response.Outcome);
            Assert.False(string.IsNullOrWhiteSpace(response.Error));

            byte[] payloadBytes = RabbitMqArticleWorkResponseWireProtocol.SerializeV1(response);
            RabbitMqArticleWorkResponse parsedResponse = RabbitMqArticleWorkResponseWireProtocol.ParseV1(payloadBytes);

            Assert.Equal(response.RequestId, parsedResponse.RequestId);
            Assert.Equal(response.MessageId, parsedResponse.MessageId);
            Assert.Equal(response.Backbone, parsedResponse.Backbone);
            Assert.Equal(response.Error, parsedResponse.Error);

            ICollection<ValidationError> schemaErrors = ResponseSchema.Value.Validate(System.Text.Encoding.UTF8.GetString(payloadBytes));
            Assert.Empty(schemaErrors);
        }

        [Fact]
        public async Task ParseToDisposition_WhenCorrelationIdMissing_RemainsNonReplyableAndDoesNotPublishResponse()
        {
            string payload = "{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<missing-correlation@example.com>\",\"backbone\":\"BackboneA\"}";
            RabbitMqArticleDelivery delivery = CreateDelivery(payload, backbone: "BackboneA", correlationId: null, replyTo: "rpc.responses");

            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(delivery, CancellationToken.None)
                .ConfigureAwait(false);

            ArticleWorkProcessingResult failure = Assert.IsType<ArticleWorkProcessingResult>(parseResult.Failure);
            Assert.Equal(InvalidRequestReplyability.NonReplyableMissingMetadata, failure.InvalidRequestReplyability);

            RabbitMqDispositionPlan plan = new ArticleWorkDispositionPlanner().CreatePlan(failure, CancellationToken.None);
            Assert.False(plan.PublishResponse);
        }

        [Fact]
        public async Task ParseToDisposition_WhenReplyToMissing_RemainsNonReplyableAndDoesNotPublishResponse()
        {
            string payload = "{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<missing-replyto@example.com>\",\"backbone\":\"BackboneA\"}";
            RabbitMqArticleDelivery delivery = CreateDelivery(payload, backbone: "BackboneA", correlationId: "corr-missing-replyto", replyTo: null);

            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(delivery, CancellationToken.None)
                .ConfigureAwait(false);

            ArticleWorkProcessingResult failure = Assert.IsType<ArticleWorkProcessingResult>(parseResult.Failure);
            Assert.Equal(InvalidRequestReplyability.NonReplyableMissingMetadata, failure.InvalidRequestReplyability);

            RabbitMqDispositionPlan plan = new ArticleWorkDispositionPlanner().CreatePlan(failure, CancellationToken.None);
            Assert.False(plan.PublishResponse);
        }

        [Fact]
        public async Task ParseToSchema_WhenVersionMissing_PreservesUnavailableIdentityWithoutSyntheticValues()
        {
            string payload = "{\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<missing-version@example.com>\",\"backbone\":\"BackboneA\"}";
            RabbitMqArticleDelivery delivery = CreateDelivery(payload, backbone: "BackboneA", correlationId: "corr-missing-version", replyTo: "rpc.responses");

            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(delivery, CancellationToken.None)
                .ConfigureAwait(false);

            ArticleWorkProcessingResult failure = Assert.IsType<ArticleWorkProcessingResult>(parseResult.Failure);
            Assert.Null(failure.Request.RequestId);
            Assert.Null(failure.Request.MessageId);
            Assert.Null(failure.Request.Backbone);

            RabbitMqArticleWorkResponse response = Assert.IsType<RabbitMqArticleWorkResponse>(new ArticleWorkResponseFactory(CreateRuntimeOptions()).CreateResponse(failure));
            Assert.Null(response.RequestId);
            Assert.Null(response.MessageId);
            Assert.Null(response.Backbone);

            byte[] responsePayload = RabbitMqArticleWorkResponseWireProtocol.SerializeV1(response);
            ICollection<ValidationError> schemaErrors = ResponseSchema.Value.Validate(System.Text.Encoding.UTF8.GetString(responsePayload));
            Assert.Empty(schemaErrors);
        }

        private static RabbitMqArticleDelivery CreateDelivery(
            string payloadText,
            string backbone,
            string? correlationId,
            string? replyTo)
        {
            return new RabbitMqArticleDelivery(
                Backbone: backbone,
                Queue: "grabbers.backbonea",
                ConsumerTag: "ctag-invalidrequest",
                ConsumerIdentity: "consumer-invalidrequest",
                DeliveryTag: 701,
                Redelivered: false,
                RoutingKey: "grabbers.backbonea",
                Exchange: "grabbers.backbonea",
                ConnectionGeneration: 15,
                RabbitMqMessageId: "rmq-invalidrequest-701",
                CorrelationId: correlationId,
                ReplyTo: replyTo,
                Payload: System.Text.Encoding.UTF8.GetBytes(payloadText),
                CancellationToken: CancellationToken.None,
                Settlement: new NoOpSettlement());
        }

        private static BackFillerRuntimeOptions CreateRuntimeOptions()
        {
            return new BackFillerRuntimeOptions(
                CanonicalBackFillerFqdn: "backfiller01.usenet.ninja",
                BackFillerId: 1,
                CanonicalDnsSuffix: "usenet.ninja",
                ValidatedLogDirectory: "C:\\logs",
                ValidatedCertificateDirectory: "C:\\certs",
                RabbitMqHosts: ["rabbit01.usenet.ninja"],
                RabbitMqPort: 5672,
                RabbitMqEnableSsl: true,
                TransitServerHost: "transit01.usenet.ninja",
                TransitServerPort: 563,
                TransitServerUseSsl: true,
                BindPort: 119);
        }

        private static JsonSchema LoadSchema()
        {
            string schemaPath = Path.Combine(
                AppContext.BaseDirectory,
                "Docs",
                "Protocols",
                "RabbitMqArticleWorkResponse.v1.schema.json");

            if (!File.Exists(schemaPath))
            {
                throw new FileNotFoundException($"Schema file was not found at '{schemaPath}'.", schemaPath);
            }

            string schemaJson = File.ReadAllText(schemaPath);
            return JsonSchema.FromJsonAsync(schemaJson).GetAwaiter().GetResult();
        }

        private sealed class NoOpSettlement : IRabbitMqDeliverySettlement
        {
            public ValueTask AckAsync(CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return ValueTask.CompletedTask;
            }

            public ValueTask NackAsync(bool requeue, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _ = requeue;
                return ValueTask.CompletedTask;
            }
        }
    }
}
