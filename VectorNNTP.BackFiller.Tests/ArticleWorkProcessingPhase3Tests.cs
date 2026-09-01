// <copyright file="ArticleWorkProcessingPhase3Tests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Tests / Article Processing
// Focused Phase 3 tests for JSON application payload parsing, AMQP RPC metadata separation,
// deterministic classification, and identity preservation boundaries.

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

            RabbitMqArticleWorkParseResult parseResult = await parser.ParseAsync(delivery, CancellationToken.None).ConfigureAwait(false);

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

        private sealed record CapturedLogEntry(LogLevel Level, string Message);

        private sealed class CapturingLogger<T>(List<CapturedLogEntry> entries) : ILogger<T>
        {
            private readonly List<CapturedLogEntry> _entries = entries ?? throw new ArgumentNullException(nameof(entries));

            public IDisposable BeginScope<TState>(TState state)
                where TState : notnull
            {
                return NullScope.Instance;
            }

            public bool IsEnabled(LogLevel logLevel)
            {
                return true;
            }

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                string message = formatter(state, exception);
                _entries.Add(new CapturedLogEntry(logLevel, message));
            }

            private sealed class NullScope : IDisposable
            {
                internal static readonly NullScope Instance = new();

                public void Dispose()
                {
                }
            }
        }

        private sealed class FakeLeaseServer : IAsyncDisposable
        {
            private readonly TcpListener _listener;
            private readonly CancellationTokenSource _shutdown;
            private readonly Task _acceptLoop;

            private FakeLeaseServer(TcpListener listener)
            {
                _listener = listener;
                _shutdown = new CancellationTokenSource();
                _acceptLoop = AcceptLoopAsync();
            }

            internal int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

            internal static Task<FakeLeaseServer> StartAsync()
            {
                TcpListener listener = new(IPAddress.Loopback, 0);
                listener.Start();
                return Task.FromResult(new FakeLeaseServer(listener));
            }

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

            private static async Task WriteAsciiLineAsync(Stream stream, string line)
            {
                byte[] bytes = Encoding.ASCII.GetBytes(line + "\r\n");
                await stream.WriteAsync(bytes, CancellationToken.None).ConfigureAwait(false);
                await stream.FlushAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
    }
}
