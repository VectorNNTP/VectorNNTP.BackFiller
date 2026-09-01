// <copyright file="ArticleWorkProcessingPhase3Tests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Tests / Article Processing
// Focused Phase 3 tests for JSON application payload parsing, AMQP RPC metadata separation,
// deterministic classification, and identity preservation boundaries.

using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Grabber;
using VectorNNTP.Backfiller.Runtime.Articles.Processing;
using VectorNNTP.Backfiller.Runtime.Articles.Validation;
using VectorNNTP.Backfiller.Runtime.RabbitMq;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Verifies Phase 3 article-work parsing and deterministic processing contracts.
    /// </summary>
    public sealed class ArticleWorkProcessingPhase3Tests
    {
        /// <summary>
        /// Verifies a valid JSON version-1 payload parses all required application fields.
        /// </summary>
        [Fact]
        public async Task ParseAsync_WhenPayloadIsValidJsonV1_ReturnsParsedRequestWithAllRequiredFieldsAsync()
        {
            RabbitMqArticleWorkRequestParser parser = new();
            Guid requestId = Guid.NewGuid();
            string messageId = "<phase3-valid@example.com>";
            RabbitMqArticleDelivery delivery = CreateDelivery(
                CreateValidJsonPayload(requestId, messageId, "BackboneA"),
                correlationId: "rpc-correlation-001",
                replyTo: "nnrpd.rpc.responses");

            RabbitMqArticleWorkParseResult parseResult = await parser.ParseAsync(delivery, CancellationToken.None).ConfigureAwait(false);

            Assert.True(parseResult.IsSuccess);
            RabbitMqArticleWorkRequest request = Assert.IsType<RabbitMqArticleWorkRequest>(parseResult.Request);
            Assert.Equal(1, request.Version);
            Assert.Equal(requestId, request.RequestId);
            Assert.Equal(messageId, request.MessageId);
            Assert.Equal("BackboneA", request.Backbone);
            Assert.Null(parseResult.Failure);
        }

        /// <summary>
        /// Verifies canonical JSON contains only version, requestId, messageId, and backbone.
        /// </summary>
        [Fact]
        public void SerializeV1_ContainsOnlyCanonicalApplicationFields()
        {
            RabbitMqArticleWorkRequest request = new(
                Version: 1,
                RequestId: Guid.Parse("7c1cb8a0-95f9-4c13-8e53-339773e3afaa"),
                MessageId: "<12345@example.invalid>",
                Backbone: "Giganews");

            byte[] payload = RabbitMqArticleWorkRequestWireProtocol.SerializeV1(request);
            string json = Encoding.UTF8.GetString(payload);

            Assert.Equal("{\"version\":1,\"requestId\":\"7c1cb8a0-95f9-4c13-8e53-339773e3afaa\",\"messageId\":\"<12345@example.invalid>\",\"backbone\":\"Giganews\"}", json);

            using JsonDocument doc = JsonDocument.Parse(payload);
            JsonElement root = doc.RootElement;
            Assert.True(root.TryGetProperty("version", out _));
            Assert.True(root.TryGetProperty("requestId", out _));
            Assert.True(root.TryGetProperty("messageId", out _));
            Assert.True(root.TryGetProperty("backbone", out _));
            Assert.False(root.TryGetProperty("correlationId", out _));
            Assert.False(root.TryGetProperty("replyTo", out _));
        }

        /// <summary>
        /// Verifies JSON property ordering and whitespace do not affect parsing.
        /// </summary>
        [Fact]
        public async Task ParseAsync_WhenPropertyOrderingAndWhitespaceVary_ParsesSuccessfullyAsync()
        {
            RabbitMqArticleWorkRequestParser parser = new();
            Guid requestId = Guid.NewGuid();
            string payload = $$"""
                {
                  "messageId" : "<ordering@example.com>",
                  "backbone" : "BackboneA",
                  "version" : 1,
                  "requestId" : "{{requestId}}"
                }
                """;

            RabbitMqArticleWorkParseResult parseResult = await parser.ParseAsync(
                CreateDelivery(payload, correlationId: "rpc-ordering", replyTo: "rpc.replies"),
                CancellationToken.None).ConfigureAwait(false);

            Assert.True(parseResult.IsSuccess);
            RabbitMqArticleWorkRequest request = Assert.IsType<RabbitMqArticleWorkRequest>(parseResult.Request);
            Assert.Equal(requestId, request.RequestId);
        }

        [Fact]
        public async Task ParseAsync_WhenPayloadIsEmpty_ReturnsInvalidRequestAsync()
        {
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(string.Empty, correlationId: "rpc-empty", replyTo: "rpc.replies"), CancellationToken.None)
                .ConfigureAwait(false);

            AssertInvalidRequest(parseResult);
        }

        [Fact]
        public async Task ParseAsync_WhenPayloadIsInvalidJson_ReturnsInvalidRequestAsync()
        {
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery("{invalid", correlationId: "rpc-invalid-json", replyTo: "rpc.replies"), CancellationToken.None)
                .ConfigureAwait(false);

            AssertInvalidRequest(parseResult);
        }

        [Fact]
        public async Task ParseAsync_WhenPayloadIsJsonArray_ReturnsInvalidRequestAsync()
        {
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery("[]", correlationId: "rpc-array", replyTo: "rpc.replies"), CancellationToken.None)
                .ConfigureAwait(false);

            AssertInvalidRequest(parseResult);
        }

        [Fact]
        public async Task ParseAsync_WhenPayloadIsJsonPrimitive_ReturnsInvalidRequestAsync()
        {
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery("42", correlationId: "rpc-primitive", replyTo: "rpc.replies"), CancellationToken.None)
                .ConfigureAwait(false);

            AssertInvalidRequest(parseResult);
        }

        [Fact]
        public async Task ParseAsync_WhenVersionMissing_ReturnsInvalidRequestAsync()
        {
            string payload = $$"""{"requestId":"{{Guid.NewGuid()}}","messageId":"<v-missing@example.com>","backbone":"BackboneA"}""";
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, correlationId: "rpc-v-missing", replyTo: "rpc.replies"), CancellationToken.None)
                .ConfigureAwait(false);

            AssertInvalidRequest(parseResult);
        }

        [Fact]
        public async Task ParseAsync_WhenVersionUnsupported_ReturnsInvalidRequestAsync()
        {
            string payload = $$"""{"version":2,"requestId":"{{Guid.NewGuid()}}","messageId":"<v-unsupported@example.com>","backbone":"BackboneA"}""";
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, correlationId: "rpc-v-unsupported", replyTo: "rpc.replies"), CancellationToken.None)
                .ConfigureAwait(false);

            AssertInvalidRequest(parseResult);
        }

        [Fact]
        public async Task ParseAsync_WhenRequestIdMissing_ReturnsInvalidRequestAsync()
        {
            string payload = "{\"version\":1,\"messageId\":\"<missing-requestid@example.com>\",\"backbone\":\"BackboneA\"}";
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, correlationId: "rpc-requestid-missing", replyTo: "rpc.replies"), CancellationToken.None)
                .ConfigureAwait(false);

            AssertInvalidRequest(parseResult);
        }

        [Fact]
        public async Task ParseAsync_WhenRequestIdInvalid_ReturnsInvalidRequestAsync()
        {
            string payload = "{\"version\":1,\"requestId\":\"not-a-guid\",\"messageId\":\"<invalid-requestid@example.com>\",\"backbone\":\"BackboneA\"}";
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, correlationId: "rpc-requestid-invalid", replyTo: "rpc.replies"), CancellationToken.None)
                .ConfigureAwait(false);

            AssertInvalidRequest(parseResult);
        }

        [Fact]
        public async Task ParseAsync_WhenMessageIdMissing_ReturnsInvalidRequestAsync()
        {
            string payload = $$"""{"version":1,"requestId":"{{Guid.NewGuid()}}","backbone":"BackboneA"}""";
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, correlationId: "rpc-messageid-missing", replyTo: "rpc.replies"), CancellationToken.None)
                .ConfigureAwait(false);

            AssertInvalidRequest(parseResult);
        }

        [Fact]
        public async Task ParseAsync_WhenMessageIdEmpty_ReturnsInvalidRequestAsync()
        {
            string payload = $$"""{"version":1,"requestId":"{{Guid.NewGuid()}}","messageId":"","backbone":"BackboneA"}""";
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, correlationId: "rpc-messageid-empty", replyTo: "rpc.replies"), CancellationToken.None)
                .ConfigureAwait(false);

            AssertInvalidRequest(parseResult);
        }

        [Fact]
        public async Task ParseAsync_WhenBackboneMissing_ReturnsInvalidRequestAsync()
        {
            string payload = $$"""{"version":1,"requestId":"{{Guid.NewGuid()}}","messageId":"<missing-backbone@example.com>"}""";
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, correlationId: "rpc-backbone-missing", replyTo: "rpc.replies"), CancellationToken.None)
                .ConfigureAwait(false);

            AssertInvalidRequest(parseResult);
        }

        [Fact]
        public async Task ParseAsync_WhenBackboneEmpty_ReturnsInvalidRequestAsync()
        {
            string payload = $$"""{"version":1,"requestId":"{{Guid.NewGuid()}}","messageId":"<empty-backbone@example.com>","backbone":""}""";
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, correlationId: "rpc-backbone-empty", replyTo: "rpc.replies"), CancellationToken.None)
                .ConfigureAwait(false);

            AssertInvalidRequest(parseResult);
        }

        [Fact]
        public async Task ParseAsync_WhenBackboneMismatchesDeliveryContext_ReturnsInvalidRequestAsync()
        {
            string payload = CreateValidJsonPayload(Guid.NewGuid(), "<mismatch@example.com>", "Eweka");
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, backbone: "Giganews", correlationId: "rpc-backbone-mismatch", replyTo: "rpc.replies"), CancellationToken.None)
                .ConfigureAwait(false);

            AssertInvalidRequest(parseResult);
        }

        [Fact]
        public async Task ParseAsync_WhenBackboneCaseDiffers_UsesCaseInsensitiveMatchingAsync()
        {
            string payload = CreateValidJsonPayload(Guid.NewGuid(), "<case-backbone@example.com>", "giganews");
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, backbone: "Giganews", correlationId: "rpc-case", replyTo: "rpc.replies"), CancellationToken.None)
                .ConfigureAwait(false);

            Assert.True(parseResult.IsSuccess);
        }

        [Fact]
        public async Task ParseAsync_WhenCorrelationIdMissingInAmqpProperties_ReturnsInvalidRequestAsync()
        {
            string payload = CreateValidJsonPayload(Guid.NewGuid(), "<missing-correlation@example.com>", "BackboneA");
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, correlationId: null, replyTo: "rpc.replies"), CancellationToken.None)
                .ConfigureAwait(false);

            AssertInvalidRequest(parseResult);
        }

        [Fact]
        public async Task ParseAsync_WhenReplyToMissingInAmqpProperties_ReturnsInvalidRequestAsync()
        {
            string payload = CreateValidJsonPayload(Guid.NewGuid(), "<missing-replyto@example.com>", "BackboneA");
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, correlationId: "rpc-missing-replyto", replyTo: null), CancellationToken.None)
                .ConfigureAwait(false);

            AssertInvalidRequest(parseResult);
        }

        /// <summary>
        /// Verifies processing preserves RequestId from JSON and CorrelationId/ReplyTo from delivery metadata.
        /// </summary>
        [Fact]
        public async Task ProcessAsync_PreservesJsonAndAmqpIdentitiesDistinctlyAsync()
        {
            Guid requestId = Guid.NewGuid();
            RabbitMqArticleWorkRequest request = new(1, requestId, "<identity-preserve@example.com>", "BackboneA");
            RabbitMqArticleDelivery delivery = CreateDelivery(
                CreateValidJsonPayload(requestId, request.MessageId, request.Backbone),
                correlationId: "rpc-identity-001",
                replyTo: "nnrpd.rpc.responses",
                redelivered: true,
                deliveryTag: 987,
                connectionGeneration: 12,
                consumerIdentity: "consumer-A");

            FakeBackboneArticleRetriever retriever = new(static req =>
            {
                NntpArticleGrabberResult grabberResult = NntpArticleGrabberResult.Failed(
                    req.MessageId,
                    NntpArticleGrabberFailureCode.ArticleNotFound,
                    NntpArticleAcquisitionFailureCode.ArticleNotFound,
                    parseFailureCode: null,
                    yEncStatus: null,
                    responseCode: 430,
                    responseText: "No article with that message-id");

                return ValueTask.FromResult(new BackboneArticleRetrievalResult(Lease: null, grabberResult));
            });

            ArticleWorkProcessor processor = new(retriever, NullLogger<ArticleWorkProcessor>.Instance);
            ArticleWorkProcessingResult result = await processor.ProcessAsync(request, delivery, CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(requestId, result.Request.RequestId);
            Assert.Equal("rpc-identity-001", result.CorrelationId);
            Assert.Equal("nnrpd.rpc.responses", result.ReplyTo);
            Assert.Equal(987UL, result.Delivery.DeliveryTag);
            Assert.Equal(12L, result.Delivery.ConnectionGeneration);
            Assert.Equal("consumer-A", result.Delivery.ConsumerIdentity);
            Assert.NotEqual(result.Request.RequestId.ToString(), result.CorrelationId);
            result.Dispose();
        }

        /// <summary>
        /// Verifies redelivery semantics preserve JSON request identity and AMQP RPC metadata.
        /// </summary>
        [Fact]
        public async Task ParseAsync_WhenRedelivered_PreservesRequestIdCorrelationIdAndReplyToAsync()
        {
            Guid requestId = Guid.NewGuid();
            string payload = CreateValidJsonPayload(requestId, "<redelivery@example.com>", "BackboneA");

            RabbitMqArticleDelivery firstDelivery = CreateDelivery(payload, correlationId: "rpc-redeliver-1", replyTo: "rpc.responses", redelivered: false, deliveryTag: 10, connectionGeneration: 2, consumerIdentity: "consumer-1");
            RabbitMqArticleDelivery redelivery = CreateDelivery(payload, correlationId: "rpc-redeliver-1", replyTo: "rpc.responses", redelivered: true, deliveryTag: 22, connectionGeneration: 4, consumerIdentity: "consumer-2");

            RabbitMqArticleWorkRequestParser parser = new();
            RabbitMqArticleWorkParseResult firstResult = await parser.ParseAsync(firstDelivery, CancellationToken.None).ConfigureAwait(false);
            RabbitMqArticleWorkParseResult secondResult = await parser.ParseAsync(redelivery, CancellationToken.None).ConfigureAwait(false);

            RabbitMqArticleWorkRequest firstRequest = Assert.IsType<RabbitMqArticleWorkRequest>(firstResult.Request);
            RabbitMqArticleWorkRequest secondRequest = Assert.IsType<RabbitMqArticleWorkRequest>(secondResult.Request);

            Assert.Equal(firstRequest.RequestId, secondRequest.RequestId);
            Assert.Equal(firstDelivery.CorrelationId, redelivery.CorrelationId);
            Assert.Equal(firstDelivery.ReplyTo, redelivery.ReplyTo);
            Assert.NotEqual(firstDelivery.DeliveryTag, redelivery.DeliveryTag);
            Assert.NotEqual(firstDelivery.ConnectionGeneration, redelivery.ConnectionGeneration);
            Assert.NotEqual(firstDelivery.ConsumerIdentity, redelivery.ConsumerIdentity);
        }

        [Fact]
        public async Task ProcessAsync_WhenGrabberReportsArticleNotFound_ReturnsArticleNotFoundClassificationAsync()
        {
            FakeBackboneArticleRetriever retriever = new(static request =>
            {
                ArgumentNullException.ThrowIfNull(request);
                NntpArticleGrabberResult grabberResult = NntpArticleGrabberResult.Failed(
                    request.MessageId,
                    NntpArticleGrabberFailureCode.ArticleNotFound,
                    NntpArticleAcquisitionFailureCode.ArticleNotFound,
                    parseFailureCode: null,
                    yEncStatus: null,
                    responseCode: 430,
                    responseText: "No article with that message-id");

                return ValueTask.FromResult(new BackboneArticleRetrievalResult(Lease: null, grabberResult));
            });

            ArticleWorkProcessor processor = new(retriever, NullLogger<ArticleWorkProcessor>.Instance);
            RabbitMqArticleWorkRequest request = new(1, Guid.NewGuid(), "<phase3-notfound@example.com>", "BackboneA");
            RabbitMqArticleDelivery delivery = CreateDelivery(
                CreateValidJsonPayload(request.RequestId, request.MessageId, request.Backbone),
                correlationId: "rpc-notfound",
                replyTo: "rpc.responses");

            ArticleWorkProcessingResult result = await processor.ProcessAsync(request, delivery, CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(ArticleWorkProcessingOutcome.ArticleNotFound, result.Outcome);
            Assert.Equal(ArticleWorkDispositionRecommendation.NackDrop, result.Disposition);
            Assert.Equal(NntpArticleAcquisitionFailureCode.ArticleNotFound, result.ProviderFailureCode);
            result.Dispose();
        }

        [Fact]
        public async Task ProcessAsync_WhenOperationTokenIsCanceled_ReturnsCancelledClassificationAsync()
        {
            FakeBackboneArticleRetriever retriever = new(static _ => throw new OperationCanceledException("Canceled by test."));
            ArticleWorkProcessor processor = new(retriever, NullLogger<ArticleWorkProcessor>.Instance);
            RabbitMqArticleWorkRequest request = new(1, Guid.NewGuid(), "<phase3-cancel@example.com>", "BackboneA");
            RabbitMqArticleDelivery delivery = CreateDelivery(
                CreateValidJsonPayload(request.RequestId, request.MessageId, request.Backbone),
                correlationId: "rpc-cancel",
                replyTo: "rpc.responses");

            using CancellationTokenSource cancellationTokenSource = new();
            cancellationTokenSource.Cancel();
            ArticleWorkProcessingResult result = await processor.ProcessAsync(request, delivery, cancellationTokenSource.Token).ConfigureAwait(false);

            Assert.Equal(ArticleWorkProcessingOutcome.Cancelled, result.Outcome);
            Assert.Equal(ArticleWorkDispositionRecommendation.None, result.Disposition);
            Assert.Equal(NntpArticleAcquisitionFailureCode.Cancelled, result.ProviderFailureCode);
        }

        private static void AssertInvalidRequest(RabbitMqArticleWorkParseResult parseResult)
        {
            Assert.False(parseResult.IsSuccess);
            ArticleWorkProcessingResult failure = Assert.IsType<ArticleWorkProcessingResult>(parseResult.Failure);
            Assert.Equal(ArticleWorkProcessingOutcome.InvalidRequest, failure.Outcome);
            Assert.Equal(ArticleWorkDispositionRecommendation.NackDrop, failure.Disposition);
            Assert.NotEqual(ArticleWorkProcessingOutcome.ProviderFailure, failure.Outcome);
        }

        private static string CreateValidJsonPayload(Guid requestId, string messageId, string backbone)
        {
            return $"{{\"version\":1,\"requestId\":\"{requestId}\",\"messageId\":\"{messageId}\",\"backbone\":\"{backbone}\"}}";
        }

        private static RabbitMqArticleDelivery CreateDelivery(
            string payloadText,
            string backbone = "BackboneA",
            string? correlationId = "corr-17",
            string? replyTo = "reply.queue",
            bool redelivered = false,
            ulong deliveryTag = 17,
            long connectionGeneration = 3,
            string consumerIdentity = "consumer-1")
        {
            return new RabbitMqArticleDelivery(
                Backbone: backbone,
                Queue: "grabbers.backbonea",
                ConsumerTag: "ctag-1",
                ConsumerIdentity: consumerIdentity,
                DeliveryTag: deliveryTag,
                Redelivered: redelivered,
                RoutingKey: "grabbers.backbonea",
                Exchange: "grabbers.backbonea",
                ConnectionGeneration: connectionGeneration,
                RabbitMqMessageId: "rmq-id-17",
                CorrelationId: correlationId,
                ReplyTo: replyTo,
                Payload: Encoding.UTF8.GetBytes(payloadText),
                CancellationToken: CancellationToken.None,
                Settlement: new NoOpDeliverySettlement());
        }

        private sealed class FakeBackboneArticleRetriever : IBackboneArticleRetriever
        {
            private readonly Func<RabbitMqArticleWorkRequest, ValueTask<BackboneArticleRetrievalResult>> _implementation;

            internal FakeBackboneArticleRetriever(Func<RabbitMqArticleWorkRequest, ValueTask<BackboneArticleRetrievalResult>> implementation)
            {
                _implementation = implementation ?? throw new ArgumentNullException(nameof(implementation));
            }

            public ValueTask<BackboneArticleRetrievalResult> RetrieveAsync(RabbitMqArticleWorkRequest request, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return _implementation(request);
            }
        }

        private sealed class NoOpDeliverySettlement : IRabbitMqDeliverySettlement
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
