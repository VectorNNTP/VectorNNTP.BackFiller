// <copyright file="ArticleWorkProcessingPhase3Tests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Focused tests for article work processing phase3, covering NNTP article and transport behavior.
// Primary responsibility: documents the executable contracts covered by the article work processing phase 3 test suite.

using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Accounts;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Grabber;
using VectorNNTP.Backfiller.Runtime.Articles.Processing;
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

            RabbitMqArticleWorkParseResult parseResult = await parser.ParseAsync(delivery, CancellationToken.None);

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
                CancellationToken.None);

            Assert.True(parseResult.IsSuccess);
            RabbitMqArticleWorkRequest request = Assert.IsType<RabbitMqArticleWorkRequest>(parseResult.Request);
            Assert.Equal(requestId, request.RequestId);
        }
        /// <summary>
        /// Confirms the parse async when payload is empty returns invalid request async behavior.
        /// </summary>
        [Fact]
        public async Task ParseAsync_WhenPayloadIsEmpty_ReturnsInvalidRequestAsync()
        {
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(string.Empty, correlationId: "rpc-empty", replyTo: "rpc.replies"), CancellationToken.None);

            AssertInvalidRequest(parseResult);
        }
        /// <summary>
        /// Confirms the parse async when payload is invalid json returns invalid request async behavior.
        /// </summary>
        [Fact]
        public async Task ParseAsync_WhenPayloadIsInvalidJson_ReturnsInvalidRequestAsync()
        {
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery("{invalid", correlationId: "rpc-invalid-json", replyTo: "rpc.replies"), CancellationToken.None);

            AssertInvalidRequest(parseResult);
        }
        /// <summary>
        /// Confirms the parse async when invalid request emits warning with exact failure reason and delivery context async behavior.
        /// </summary>
        [Fact]
        public async Task ParseAsync_WhenInvalidRequest_EmitsWarningWithExactFailureReasonAndDeliveryContextAsync()
        {
            List<CapturedLogEntry> entries = [];
            ILogger<RabbitMqArticleWorkRequestParser> logger = new CapturingLogger<RabbitMqArticleWorkRequestParser>(entries);
            RabbitMqArticleWorkRequestParser parser = new(logger: logger);
            RabbitMqArticleDelivery delivery = CreateDelivery(
                "{invalid",
                backbone: "Giganews",
                correlationId: "corr-parser-failure",
                replyTo: "rpc.reply.queue",
                deliveryTag: 37);

            RabbitMqArticleWorkParseResult parseResult = await parser.ParseAsync(delivery, CancellationToken.None);

            AssertInvalidRequest(parseResult);

            CapturedLogEntry warning = Assert.Single(entries, static entry => entry.Level == LogLevel.Warning && entry.Message.Contains("RabbitMQ article-work request rejected.", StringComparison.Ordinal));
            Assert.Contains("Reason=RabbitMQ article-work payload was not valid JSON.", warning.Message, StringComparison.Ordinal);
            Assert.Contains("CorrelationId=corr-parser-failure", warning.Message, StringComparison.Ordinal);
            Assert.Contains("ReplyTo=rpc.reply.queue", warning.Message, StringComparison.Ordinal);
            Assert.Contains("RabbitMqMessageId=rmq-id-17", warning.Message, StringComparison.Ordinal);
            Assert.Contains("DeliveryTag=37", warning.Message, StringComparison.Ordinal);
            Assert.Contains("Backbone=Giganews", warning.Message, StringComparison.Ordinal);
            Assert.Contains("PayloadLength=8", warning.Message, StringComparison.Ordinal);
            string expectedPayloadSha256 = Convert.ToHexString(SHA256.HashData(delivery.Payload.Span));
            Assert.Contains($"PayloadSha256={expectedPayloadSha256}", warning.Message, StringComparison.Ordinal);
            Assert.Equal("RabbitMQ article-work payload was not valid JSON.", parseResult.Failure?.ResponseText);
        }
        /// <summary>
        /// Confirms the parse async when payload is json array returns invalid request async behavior.
        /// </summary>
        [Fact]
        public async Task ParseAsync_WhenPayloadIsJsonArray_ReturnsInvalidRequestAsync()
        {
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery("[]", correlationId: "rpc-array", replyTo: "rpc.replies"), CancellationToken.None);

            AssertInvalidRequest(parseResult);
        }
        /// <summary>
        /// Confirms the parse async when payload is json primitive returns invalid request async behavior.
        /// </summary>
        [Fact]
        public async Task ParseAsync_WhenPayloadIsJsonPrimitive_ReturnsInvalidRequestAsync()
        {
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery("42", correlationId: "rpc-primitive", replyTo: "rpc.replies"), CancellationToken.None);

            AssertInvalidRequest(parseResult);
        }
        /// <summary>
        /// Confirms the parse async when version missing returns invalid request async behavior.
        /// </summary>
        [Fact]
        public async Task ParseAsync_WhenVersionMissing_ReturnsInvalidRequestAsync()
        {
            string payload = $$"""{"requestId":"{{Guid.NewGuid()}}","messageId":"<v-missing@example.com>","backbone":"BackboneA"}""";
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, correlationId: "rpc-v-missing", replyTo: "rpc.replies"), CancellationToken.None);

            AssertInvalidRequest(parseResult);
        }
        /// <summary>
        /// Confirms the parse async when version unsupported returns invalid request async behavior.
        /// </summary>
        [Fact]
        public async Task ParseAsync_WhenVersionUnsupported_ReturnsInvalidRequestAsync()
        {
            string payload = $$"""{"version":2,"requestId":"{{Guid.NewGuid()}}","messageId":"<v-unsupported@example.com>","backbone":"BackboneA"}""";
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, correlationId: "rpc-v-unsupported", replyTo: "rpc.replies"), CancellationToken.None);

            AssertInvalidRequest(parseResult);
        }
        /// <summary>
        /// Confirms the parse async when request id missing returns invalid request async behavior.
        /// </summary>
        [Fact]
        public async Task ParseAsync_WhenRequestIdMissing_ReturnsInvalidRequestAsync()
        {
            string payload = "{\"version\":1,\"messageId\":\"<missing-requestid@example.com>\",\"backbone\":\"BackboneA\"}";
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, correlationId: "rpc-requestid-missing", replyTo: "rpc.replies"), CancellationToken.None);

            AssertInvalidRequest(parseResult);
        }
        /// <summary>
        /// Confirms the parse async when request id invalid returns invalid request async behavior.
        /// </summary>
        [Fact]
        public async Task ParseAsync_WhenRequestIdInvalid_ReturnsInvalidRequestAsync()
        {
            string payload = "{\"version\":1,\"requestId\":\"not-a-guid\",\"messageId\":\"<invalid-requestid@example.com>\",\"backbone\":\"BackboneA\"}";
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, correlationId: "rpc-requestid-invalid", replyTo: "rpc.replies"), CancellationToken.None);

            AssertInvalidRequest(parseResult);
        }
        /// <summary>
        /// Confirms the parse async when message id missing returns invalid request async behavior.
        /// </summary>
        [Fact]
        public async Task ParseAsync_WhenMessageIdMissing_ReturnsInvalidRequestAsync()
        {
            string payload = $$"""{"version":1,"requestId":"{{Guid.NewGuid()}}","backbone":"BackboneA"}""";
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, correlationId: "rpc-messageid-missing", replyTo: "rpc.replies"), CancellationToken.None);

            AssertInvalidRequest(parseResult);
        }
        /// <summary>
        /// Confirms the parse async when message id empty returns invalid request async behavior.
        /// </summary>
        [Fact]
        public async Task ParseAsync_WhenMessageIdEmpty_ReturnsInvalidRequestAsync()
        {
            string payload = $$"""{"version":1,"requestId":"{{Guid.NewGuid()}}","messageId":"","backbone":"BackboneA"}""";
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, correlationId: "rpc-messageid-empty", replyTo: "rpc.replies"), CancellationToken.None);

            AssertInvalidRequest(parseResult);
        }
        /// <summary>
        /// Confirms the parse async when backbone missing returns invalid request async behavior.
        /// </summary>
        [Fact]
        public async Task ParseAsync_WhenBackboneMissing_ReturnsInvalidRequestAsync()
        {
            string payload = $$"""{"version":1,"requestId":"{{Guid.NewGuid()}}","messageId":"<missing-backbone@example.com>"}""";
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, correlationId: "rpc-backbone-missing", replyTo: "rpc.replies"), CancellationToken.None);

            AssertInvalidRequest(parseResult);
        }
        /// <summary>
        /// Confirms the parse async when backbone empty returns invalid request async behavior.
        /// </summary>
        [Fact]
        public async Task ParseAsync_WhenBackboneEmpty_ReturnsInvalidRequestAsync()
        {
            string payload = $$"""{"version":1,"requestId":"{{Guid.NewGuid()}}","messageId":"<empty-backbone@example.com>","backbone":""}""";
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, correlationId: "rpc-backbone-empty", replyTo: "rpc.replies"), CancellationToken.None);

            AssertInvalidRequest(parseResult);
        }
        /// <summary>
        /// Confirms the parse async when backbone mismatches delivery context returns invalid request async behavior.
        /// </summary>
        [Fact]
        public async Task ParseAsync_WhenBackboneMismatchesDeliveryContext_ReturnsInvalidRequestAsync()
        {
            string payload = CreateValidJsonPayload(Guid.NewGuid(), "<mismatch@example.com>", "Eweka");
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, backbone: "Giganews", correlationId: "rpc-backbone-mismatch", replyTo: "rpc.replies"), CancellationToken.None);

            AssertInvalidRequest(parseResult);
        }
        /// <summary>
        /// Confirms the parse async when backbone case differs uses case insensitive matching async behavior.
        /// </summary>
        [Fact]
        public async Task ParseAsync_WhenBackboneCaseDiffers_UsesCaseInsensitiveMatchingAsync()
        {
            string payload = CreateValidJsonPayload(Guid.NewGuid(), "<case-backbone@example.com>", "giganews");
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, backbone: "Giganews", correlationId: "rpc-case", replyTo: "rpc.replies"), CancellationToken.None);

            Assert.True(parseResult.IsSuccess);
        }
        /// <summary>
        /// Confirms the parse async when correlation id missing in amqp properties returns invalid request async behavior.
        /// </summary>
        [Fact]
        public async Task ParseAsync_WhenCorrelationIdMissingInAmqpProperties_ReturnsInvalidRequestAsync()
        {
            string payload = CreateValidJsonPayload(Guid.NewGuid(), "<missing-correlation@example.com>", "BackboneA");
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, correlationId: null, replyTo: "rpc.replies"), CancellationToken.None);

            AssertInvalidRequest(parseResult);
        }
        /// <summary>
        /// Confirms the parse async when reply to missing in amqp properties returns invalid request async behavior.
        /// </summary>
        [Fact]
        public async Task ParseAsync_WhenReplyToMissingInAmqpProperties_ReturnsInvalidRequestAsync()
        {
            string payload = CreateValidJsonPayload(Guid.NewGuid(), "<missing-replyto@example.com>", "BackboneA");
            RabbitMqArticleWorkParseResult parseResult = await new RabbitMqArticleWorkRequestParser()
                .ParseAsync(CreateDelivery(payload, correlationId: "rpc-missing-replyto", replyTo: null), CancellationToken.None);

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
            RabbitMqArticleWorkParseResult firstResult = await parser.ParseAsync(firstDelivery, CancellationToken.None);
            RabbitMqArticleWorkParseResult secondResult = await parser.ParseAsync(redelivery, CancellationToken.None);

            RabbitMqArticleWorkRequest firstRequest = Assert.IsType<RabbitMqArticleWorkRequest>(firstResult.Request);
            RabbitMqArticleWorkRequest secondRequest = Assert.IsType<RabbitMqArticleWorkRequest>(secondResult.Request);

            Assert.Equal(firstRequest.RequestId, secondRequest.RequestId);
            Assert.Equal(firstDelivery.CorrelationId, redelivery.CorrelationId);
            Assert.Equal(firstDelivery.ReplyTo, redelivery.ReplyTo);
            Assert.NotEqual(firstDelivery.DeliveryTag, redelivery.DeliveryTag);
            Assert.NotEqual(firstDelivery.ConnectionGeneration, redelivery.ConnectionGeneration);
            Assert.NotEqual(firstDelivery.ConsumerIdentity, redelivery.ConsumerIdentity);
        }
        /// <summary>
        /// Confirms the process async when grabber reports article not found returns article not found classification async behavior.
        /// </summary>
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

            ArticleWorkProcessingResult result = await processor.ProcessAsync(request, delivery, CancellationToken.None);

            Assert.Equal(ArticleWorkProcessingOutcome.ArticleNotFound, result.Outcome);
            Assert.Equal(ArticleWorkDispositionRecommendation.NackDrop, result.Disposition);
            Assert.Equal(NntpArticleAcquisitionFailureCode.ArticleNotFound, result.ProviderFailureCode);
            result.Dispose();
        }
        /// <summary>
        /// Confirms the process async when operation token is canceled returns cancelled classification async behavior.
        /// </summary>
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
            ArticleWorkProcessingResult result = await processor.ProcessAsync(request, delivery, cancellationTokenSource.Token);

            Assert.Equal(ArticleWorkProcessingOutcome.Cancelled, result.Outcome);
            Assert.Equal(ArticleWorkDispositionRecommendation.None, result.Disposition);
            Assert.Equal(NntpArticleAcquisitionFailureCode.Cancelled, result.ProviderFailureCode);
        }
        /// <summary>
        /// Confirms the process async when lease is acquired and workflow succeeds disposes lease and allows immediate reuse async behavior.
        /// </summary>
        [Fact]
        public async Task ProcessAsync_WhenLeaseIsAcquiredAndWorkflowSucceeds_DisposesLeaseAndAllowsImmediateReuseAsync()
        {
            await using FakeLeaseServer server = await FakeLeaseServer.StartAsync().ConfigureAwait(false);
            await using NntpArticleExecutionSessionManager manager = await CreateSingleSlotManagerAsync(server.Port).ConfigureAwait(false);

            int acquiredSlotId = -1;
            FakeBackboneArticleRetriever retriever = new(async request =>
            {
                NntpArticleSessionLease lease = await manager.AcquireAsync(request.MessageId, CancellationToken.None).ConfigureAwait(false);
                acquiredSlotId = lease.SlotId;
                return new BackboneArticleRetrievalResult(lease, CreateSuccessfulGrabberResult(request.MessageId));
            });

            ArticleWorkProcessor processor = new(retriever, NullLogger<ArticleWorkProcessor>.Instance);
            RabbitMqArticleWorkRequest request = new(1, Guid.NewGuid(), "<lease-success@example.com>", "BackboneA");
            RabbitMqArticleDelivery delivery = CreateDelivery(CreateValidJsonPayload(request.RequestId, request.MessageId, request.Backbone), correlationId: "rpc-lease-success", replyTo: "rpc.responses");

            ArticleWorkProcessingResult result = await processor.ProcessAsync(request, delivery, CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(ArticleWorkProcessingOutcome.Success, result.Outcome);
            Assert.Equal(ArticleWorkDispositionRecommendation.Ack, result.Disposition);

            await using NntpArticleSessionLease reacquiredLease = await manager.AcquireAsync(request.MessageId, CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(acquiredSlotId, reacquiredLease.SlotId);
            result.Dispose();
        }
        /// <summary>
        /// Confirms the process async when failure occurs after lease acquisition releases lease exactly once for reuse async behavior.
        /// </summary>
        [Fact]
        public async Task ProcessAsync_WhenFailureOccursAfterLeaseAcquisition_ReleasesLeaseExactlyOnceForReuseAsync()
        {
            await using FakeLeaseServer server = await FakeLeaseServer.StartAsync().ConfigureAwait(false);
            await using NntpArticleExecutionSessionManager manager = await CreateSingleSlotManagerAsync(server.Port).ConfigureAwait(false);

            int acquiredSlotId = -1;
            FakeBackboneArticleRetriever retriever = new(async request =>
            {
                NntpArticleSessionLease lease = await manager.AcquireAsync(request.MessageId, CancellationToken.None).ConfigureAwait(false);
                acquiredSlotId = lease.SlotId;
                return new BackboneArticleRetrievalResult(lease, GrabberResult: null!);
            });

            ArticleWorkProcessor processor = new(retriever, NullLogger<ArticleWorkProcessor>.Instance);
            RabbitMqArticleWorkRequest request = new(1, Guid.NewGuid(), "<lease-failure@example.com>", "BackboneA");
            RabbitMqArticleDelivery delivery = CreateDelivery(CreateValidJsonPayload(request.RequestId, request.MessageId, request.Backbone), correlationId: "rpc-lease-failure", replyTo: "rpc.responses");

            ArticleWorkProcessingResult result = await processor.ProcessAsync(request, delivery, CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(ArticleWorkProcessingOutcome.UnexpectedFailure, result.Outcome);
            Assert.NotNull(result.UnexpectedException);

            await using NntpArticleSessionLease reacquiredLease = await manager.AcquireAsync(request.MessageId, CancellationToken.None).ConfigureAwait(false);
            Assert.Equal(acquiredSlotId, reacquiredLease.SlotId);
        }
        /// <summary>
        /// Confirms the process async releases lease before downstream processing stage begins async behavior.
        /// </summary>
        [Fact]
        public async Task ProcessAsync_ReleasesLeaseBeforeDownstreamProcessingStageBeginsAsync()
        {
            await using FakeLeaseServer server = await FakeLeaseServer.StartAsync().ConfigureAwait(false);
            await using NntpArticleExecutionSessionManager manager = await CreateSingleSlotManagerAsync(server.Port).ConfigureAwait(false);

            FakeBackboneArticleRetriever retriever = new(async request =>
            {
                NntpArticleSessionLease lease = await manager.AcquireAsync(request.MessageId, CancellationToken.None).ConfigureAwait(false);
                return new BackboneArticleRetrievalResult(lease, CreateSuccessfulGrabberResult(request.MessageId));
            });

            ArticleWorkProcessor processor = new(retriever, NullLogger<ArticleWorkProcessor>.Instance);
            RabbitMqArticleWorkRequest request = new(1, Guid.NewGuid(), "<lease-ordering@example.com>", "BackboneA");
            RabbitMqArticleDelivery delivery = CreateDelivery(CreateValidJsonPayload(request.RequestId, request.MessageId, request.Backbone), correlationId: "rpc-lease-order", replyTo: "rpc.responses");

            ArticleWorkProcessingResult result = await processor.ProcessAsync(request, delivery, CancellationToken.None).ConfigureAwait(false);

            TaskCompletionSource<bool> allowDownstream = new(TaskCreationOptions.RunContinuationsAsynchronously);
            Task downstreamTask = Task.Run(async () =>
            {
                await allowDownstream.Task.ConfigureAwait(false);
                result.Dispose();
            });

            await using NntpArticleSessionLease downstreamStageLease = await manager.AcquireAsync(request.MessageId, CancellationToken.None).ConfigureAwait(false);
            Assert.False(allowDownstream.Task.IsCompleted);

            allowDownstream.SetResult(true);
            await downstreamTask.ConfigureAwait(false);
        }
        /// <summary>
        /// Confirms the process async when two requests run sequentially reuses same slot without delay async behavior.
        /// </summary>
        [Fact]
        public async Task ProcessAsync_WhenTwoRequestsRunSequentially_ReusesSameSlotWithoutDelayAsync()
        {
            await using FakeLeaseServer server = await FakeLeaseServer.StartAsync().ConfigureAwait(false);
            await using NntpArticleExecutionSessionManager manager = await CreateSingleSlotManagerAsync(server.Port).ConfigureAwait(false);

            List<int> slotIds = [];
            FakeBackboneArticleRetriever retriever = new(async request =>
            {
                NntpArticleSessionLease lease = await manager.AcquireAsync(request.MessageId, CancellationToken.None).ConfigureAwait(false);
                slotIds.Add(lease.SlotId);
                return new BackboneArticleRetrievalResult(lease, CreateSuccessfulGrabberResult(request.MessageId));
            });

            ArticleWorkProcessor processor = new(retriever, NullLogger<ArticleWorkProcessor>.Instance);

            RabbitMqArticleWorkRequest firstRequest = new(1, Guid.NewGuid(), "<lease-reuse-1@example.com>", "BackboneA");
            RabbitMqArticleDelivery firstDelivery = CreateDelivery(CreateValidJsonPayload(firstRequest.RequestId, firstRequest.MessageId, firstRequest.Backbone), correlationId: "rpc-lease-reuse-1", replyTo: "rpc.responses");
            ArticleWorkProcessingResult firstResult = await processor.ProcessAsync(firstRequest, firstDelivery, CancellationToken.None).ConfigureAwait(false);

            RabbitMqArticleWorkRequest secondRequest = new(1, Guid.NewGuid(), "<lease-reuse-2@example.com>", "BackboneA");
            RabbitMqArticleDelivery secondDelivery = CreateDelivery(CreateValidJsonPayload(secondRequest.RequestId, secondRequest.MessageId, secondRequest.Backbone), correlationId: "rpc-lease-reuse-2", replyTo: "rpc.responses");
            ArticleWorkProcessingResult secondResult = await processor.ProcessAsync(secondRequest, secondDelivery, CancellationToken.None).ConfigureAwait(false);

            Assert.Equal(2, slotIds.Count);
            Assert.Equal(slotIds[0], slotIds[1]);

            firstResult.Dispose();
            secondResult.Dispose();
        }
        /// <summary>
        /// Confirms the process async disposing processing result does not trigger second lease release async behavior.
        /// </summary>
        [Fact]
        public async Task ProcessAsync_DisposingProcessingResultDoesNotTriggerSecondLeaseReleaseAsync()
        {
            await using FakeLeaseServer server = await FakeLeaseServer.StartAsync().ConfigureAwait(false);
            await using NntpArticleExecutionSessionManager manager = await CreateSingleSlotManagerAsync(server.Port).ConfigureAwait(false);

            FakeBackboneArticleRetriever retriever = new(async request =>
            {
                NntpArticleSessionLease lease = await manager.AcquireAsync(request.MessageId, CancellationToken.None).ConfigureAwait(false);
                return new BackboneArticleRetrievalResult(lease, CreateSuccessfulGrabberResult(request.MessageId));
            });

            ArticleWorkProcessor processor = new(retriever, NullLogger<ArticleWorkProcessor>.Instance);
            RabbitMqArticleWorkRequest request = new(1, Guid.NewGuid(), "<lease-no-double-dispose@example.com>", "BackboneA");
            RabbitMqArticleDelivery delivery = CreateDelivery(CreateValidJsonPayload(request.RequestId, request.MessageId, request.Backbone), correlationId: "rpc-lease-nodouble", replyTo: "rpc.responses");

            ArticleWorkProcessingResult result = await processor.ProcessAsync(request, delivery, CancellationToken.None).ConfigureAwait(false);

            result.Dispose();
            result.Dispose();

            await using NntpArticleSessionLease heldLease = await manager.AcquireAsync(request.MessageId, CancellationToken.None).ConfigureAwait(false);
            Task<NntpArticleSessionLease> blockedAcquireTask = manager.AcquireAsync(request.MessageId, CancellationToken.None).AsTask();

            Assert.False(blockedAcquireTask.IsCompleted);

            await heldLease.DisposeAsync().ConfigureAwait(false);
            await using NntpArticleSessionLease nextLease = await blockedAcquireTask.ConfigureAwait(false);
        }

        /// <summary>
        /// Confirms the create successful grabber result behavior.
        /// </summary>
        /// <returns>The value returned by the create successful grabber result helper.</returns>
        /// <summary>
        /// Confirms the create successful grabber result behavior.
        /// </summary>
        /// <param name="messageId">The message id used by this test scenario.</param>
        /// <returns>The value returned by the create successful grabber result helper.</returns>
        private static NntpArticleGrabberResult CreateSuccessfulGrabberResult(string messageId)
        {
            return new NntpArticleGrabberResult(
                MessageId: messageId,
                IsSuccess: true,
                FailureCode: NntpArticleGrabberFailureCode.None,
                AcquisitionFailureCode: null,
                ParseFailureCode: null,
                YEncStatus: null,
                ResponseCode: 220,
                ResponseText: "Article retrieved.",
                Success: null);
        }

        /// <summary>
        /// Confirms the create single slot manager async behavior.
        /// </summary>
        /// <returns>The value returned by the create single slot manager async helper.</returns>
        /// <summary>
        /// Confirms the create single slot manager async behavior.
        /// </summary>
        /// <param name="port">The port used by this test scenario.</param>
        /// <returns>The value returned by the create single slot manager async helper.</returns>
        private static async Task<NntpArticleExecutionSessionManager> CreateSingleSlotManagerAsync(int port)
        {
            NntpAccountSnapshot account = new(
                EntryId: Guid.NewGuid(),
                Backbone: "BackboneA",
                Hostname: "127.0.0.1",
                KeepAliveSeconds: 30,
                MaxConnections: 1,
                Password: string.Empty,
                Port: (ushort)port,
                ServerId: 1,
                Username: string.Empty,
                UseSsl: false);

            NntpArticleExecutionSessionManager manager = new(NullLogger<NntpArticleExecutionSessionManager>.Instance);
            await manager.InitializeAsync([account], CancellationToken.None).ConfigureAwait(false);
            return manager;
        }

        /// <summary>
        /// Confirms the assert invalid request behavior.
        /// </summary>
        private static void AssertInvalidRequest(RabbitMqArticleWorkParseResult parseResult)
        {
            Assert.False(parseResult.IsSuccess);
            ArticleWorkProcessingResult failure = Assert.IsType<ArticleWorkProcessingResult>(parseResult.Failure);
            Assert.Equal(ArticleWorkProcessingOutcome.InvalidRequest, failure.Outcome);
            Assert.Equal(ArticleWorkDispositionRecommendation.NackDrop, failure.Disposition);
            Assert.NotEqual(ArticleWorkProcessingOutcome.ProviderFailure, failure.Outcome);
        }

        /// <summary>
        /// Confirms the create valid json payload behavior.
        /// </summary>
        /// <returns>The value returned by the create valid json payload helper.</returns>
        /// <summary>
        /// Confirms the create valid json payload behavior.
        /// </summary>
        /// <param name="requestId">The request id used by this test scenario.</param>
        /// <param name="messageId">The message id used by this test scenario.</param>
        /// <param name="backbone">The backbone used by this test scenario.</param>
        /// <returns>The value returned by the create valid json payload helper.</returns>
        private static string CreateValidJsonPayload(Guid requestId, string messageId, string backbone)
        {
            return $"{{\"version\":1,\"requestId\":\"{requestId}\",\"messageId\":\"{messageId}\",\"backbone\":\"{backbone}\"}}";
        }

        /// <summary>
        /// Confirms the create delivery behavior.
        /// </summary>
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

        /// <summary>
        /// Confirms the fake backbone article retriever behavior.
        /// </summary>
        private sealed class FakeBackboneArticleRetriever : IBackboneArticleRetriever
        {
            /// <summary>
            /// Supplies  implementation for the fixture or scenario under test.
            /// </summary>
            private readonly Func<RabbitMqArticleWorkRequest, ValueTask<BackboneArticleRetrievalResult>> _implementation;

            /// <summary>
            /// Confirms the fake backbone article retriever behavior.
            /// </summary>
            internal FakeBackboneArticleRetriever(Func<RabbitMqArticleWorkRequest, ValueTask<BackboneArticleRetrievalResult>> implementation)
            {
                _implementation = implementation ?? throw new ArgumentNullException(nameof(implementation));
            }

            /// <summary>
            /// Confirms the retrieve async behavior.
            /// </summary>
            /// <returns>The value returned by the retrieve async helper.</returns>
            /// <summary>
            /// Confirms the retrieve async behavior.
            /// </summary>
            /// <param name="request">The request used by this test scenario.</param>
            /// <param name="cancellationToken">The cancellation token used by this test scenario.</param>
            /// <returns>The value returned by the retrieve async helper.</returns>
            public ValueTask<BackboneArticleRetrievalResult> RetrieveAsync(RabbitMqArticleWorkRequest request, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return _implementation(request);
            }
        }

        /// <summary>
        /// Confirms the no op delivery settlement behavior.
        /// </summary>
        private sealed class NoOpDeliverySettlement : IRabbitMqDeliverySettlement
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

        /// <summary>
        /// Confirms the captured log entry behavior.
        /// </summary>
        /// <returns>The value returned by the captured log entry helper.</returns>
        /// <summary>
        /// Confirms the captured log entry behavior.
        /// </summary>
        /// <param name="Level">The level used by this test scenario.</param>
        /// <param name="Message">The message used by this test scenario.</param>
        /// <returns>The value returned by the captured log entry helper.</returns>
        private sealed record CapturedLogEntry(LogLevel Level, string Message);

        /// <summary>
        /// Confirms the capturing logger behavior.
        /// </summary>
        private sealed class CapturingLogger<T>(List<CapturedLogEntry> entries) : ILogger<T>
        {
            /// <summary>
            /// Confirms  entries behavior.
            /// </summary>
            private readonly List<CapturedLogEntry> _entries = entries ?? throw new ArgumentNullException(nameof(entries));

            /// <summary>
            /// Begins a logger scope for test logging and returns a no-op disposable scope instance.
            /// </summary>
            public IDisposable BeginScope<TState>(TState state)
                where TState : notnull
            {
                return NullScope.Instance;
            }

            /// <summary>
            /// Confirms the is enabled behavior.
            /// </summary>
            /// <returns>The value returned by the is enabled helper.</returns>
            /// <summary>
            /// Confirms the is enabled behavior.
            /// </summary>
            /// <param name="logLevel">The log level used by this test scenario.</param>
            /// <returns>The value returned by the is enabled helper.</returns>
            public bool IsEnabled(LogLevel logLevel)
            {
                return true;
            }

            /// <summary>
            /// Captures a formatted log entry emitted by the system under test.
            /// </summary>
            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                string message = formatter(state, exception);
                _entries.Add(new CapturedLogEntry(logLevel, message));
            }

            /// <summary>
            /// Confirms the null scope behavior.
            /// </summary>
            private sealed class NullScope : IDisposable
            {
                /// <summary>
                /// Confirms instance behavior.
                /// </summary>
                internal static readonly NullScope Instance = new();

                /// <summary>
                /// Confirms the dispose behavior.
                /// </summary>
                public void Dispose()
                {
                }
            }
        }

        /// <summary>
        /// Confirms the fake lease server behavior.
        /// </summary>
        private sealed class FakeLeaseServer : IAsyncDisposable
        {
            /// <summary>
            /// Supplies  listener for the fixture or scenario under test.
            /// </summary>
            private readonly TcpListener _listener;
            /// <summary>
            /// Supplies  shutdown for the fixture or scenario under test.
            /// </summary>
            private readonly CancellationTokenSource _shutdown;
            /// <summary>
            /// Supplies  accept loop for the fixture or scenario under test.
            /// </summary>
            private readonly Task _acceptLoop;

            /// <summary>
            /// Confirms the fake lease server behavior.
            /// </summary>
            private FakeLeaseServer(TcpListener listener)
            {
                _listener = listener;
                _shutdown = new CancellationTokenSource();
                _acceptLoop = AcceptLoopAsync();
            }

            /// <summary>
            /// Confirms port behavior.
            /// </summary>
            internal int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

            /// <summary>
            /// Confirms the start async behavior.
            /// </summary>
            /// <returns>The value returned by the start async helper.</returns>
            /// <summary>
            /// Confirms the start async behavior.
            /// </summary>
            /// <returns>The value returned by the start async helper.</returns>
            internal static Task<FakeLeaseServer> StartAsync()
            {
                TcpListener listener = new(IPAddress.Loopback, 0);
                listener.Start();
                return Task.FromResult(new FakeLeaseServer(listener));
            }

            /// <summary>
            /// Confirms the dispose async behavior.
            /// </summary>
            /// <returns>The value returned by the dispose async helper.</returns>
            /// <summary>
            /// Confirms the dispose async behavior.
            /// </summary>
            /// <returns>The value returned by the dispose async helper.</returns>
            public async ValueTask DisposeAsync()
            {
                _shutdown.Cancel();
                _listener.Stop();

                try
                {
                    await _acceptLoop.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }

                _shutdown.Dispose();
            }

            /// <summary>
            /// Confirms the accept loop async behavior.
            /// </summary>
            /// <returns>The value returned by the accept loop async helper.</returns>
            /// <summary>
            /// Confirms the accept loop async behavior.
            /// </summary>
            /// <returns>The value returned by the accept loop async helper.</returns>
            private async Task AcceptLoopAsync()
            {
                while (!_shutdown.IsCancellationRequested)
                {
                    try
                    {
                        using TcpClient client = await _listener.AcceptTcpClientAsync(_shutdown.Token).ConfigureAwait(false);
                        using NetworkStream stream = client.GetStream();
                        await WriteAsciiLineAsync(stream, "200 ready").ConfigureAwait(false);

                        try
                        {
                            await Task.Delay(Timeout.Infinite, _shutdown.Token).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            return;
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    catch (ObjectDisposedException)
                    {
                        return;
                    }
                }
            }

            /// <summary>
            /// Confirms the write ascii line async behavior.
            /// </summary>
            /// <returns>The value returned by the write ascii line async helper.</returns>
            /// <summary>
            /// Confirms the write ascii line async behavior.
            /// </summary>
            /// <param name="stream">The stream used by this test scenario.</param>
            /// <param name="line">The line used by this test scenario.</param>
            /// <returns>The value returned by the write ascii line async helper.</returns>
            private static async Task WriteAsciiLineAsync(Stream stream, string line)
            {
                byte[] bytes = Encoding.ASCII.GetBytes(line + "\r\n");
                await stream.WriteAsync(bytes, CancellationToken.None).ConfigureAwait(false);
                await stream.FlushAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
    }
}
