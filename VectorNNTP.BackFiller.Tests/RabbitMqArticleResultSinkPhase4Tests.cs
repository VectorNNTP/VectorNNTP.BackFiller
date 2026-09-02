// <copyright file="RabbitMqArticleResultSinkPhase4Tests.cs" company="Usenet Ninja">
// Copyright © Chris Knipe cknipe@opticnetworks.net
// </copyright>
//
// VectorNNTP.Backfiller Tests / Runtime and startup
// Behavior and contract tests for rabbit mq article result sink phase4.

using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using VectorNNTP.Backfiller.Runtime.Articles.Processing;
using VectorNNTP.Backfiller.Runtime.RabbitMq;
using Xunit;

namespace VectorNNTP.Backfiller.Tests
{
    /// <summary>
    /// Verifies Phase 4 RPC response/disposition orchestration and settlement ownership invariants.
    /// </summary>
    public sealed class RabbitMqArticleResultSinkPhase4Tests
    {
        /// <summary>
        /// Verifies the OnProcessedAsync_WhenSuccess_PublishesThenConfirmsThenAcknowledgesDeliveryTagAsync scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task OnProcessedAsync_WhenSuccess_PublishesThenConfirmsThenAcknowledgesDeliveryTagAsync()
        {
            List<string> operationLog = [];
            TrackingDeliverySettlement settlement = new(operationLog);
            RabbitMqArticleDelivery delivery = CreateDelivery(
                payloadText: CreateValidJsonPayload(Guid.NewGuid(), "<success-ordering@example.com>", "BackboneA"),
                correlationId: "corr-success-order",
                replyTo: "rpc.responses",
                deliveryTag: 812,
                connectionGeneration: 41,
                settlement: settlement);

            ArticleWorkProcessingResult result = CreateResult(
                delivery,
                outcome: ArticleWorkProcessingOutcome.Success,
                requestId: Guid.NewGuid(),
                messageId: "<success-ordering@example.com>",
                backbone: "BackboneA");

            TrackingResponsePublisher publisher = new(RabbitMqResponsePublishStatus.Confirmed, operationLog);
            RabbitMqArticleResultSink sink = CreateSink(responsePublisher: publisher);

            await sink.OnProcessedAsync(result, CancellationToken.None).ConfigureAwait(false);

            int publishIndex = operationLog.IndexOf("publish");
            int confirmIndex = operationLog.IndexOf("confirm");
            int ackIndex = operationLog.IndexOf("ack");
            Assert.True(publishIndex >= 0);
            Assert.True(confirmIndex > publishIndex);
            Assert.True(ackIndex > confirmIndex);
            Assert.Equal(812UL, settlement.AckDeliveryTag);
            Assert.Equal("rpc.responses", publisher.LastRoutingKey);
            Assert.Equal("corr-success-order", publisher.LastCorrelationId);
            Assert.Equal("corr-success-order", delivery.CorrelationId);
            Assert.Equal("rpc.responses", delivery.ReplyTo);
            Assert.DoesNotContain("correlationId", publisher.LastResponseJson!, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("replyTo", publisher.LastResponseJson!, StringComparison.OrdinalIgnoreCase);
            RabbitMqArticleWorkResponse successResponse = RabbitMqArticleWorkResponseWireProtocol.ParseV1(publisher.LastResponsePayload!);
            Assert.Equal(nameof(ArticleWorkProcessingOutcome.Success), successResponse.Outcome);
            Assert.Null(successResponse.Uri);
            Assert.Null(successResponse.Error);
        }
        /// <summary>
        /// Verifies the OnProcessedAsync_WhenPublishFails_DoesNotAckAndNacksRequeueTrueAsync scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task OnProcessedAsync_WhenPublishFails_DoesNotAckAndNacksRequeueTrueAsync()
        {
            TrackingDeliverySettlement settlement = new();
            RabbitMqArticleDelivery delivery = CreateDelivery(
                payloadText: CreateValidJsonPayload(Guid.NewGuid(), "<publish-failure@example.com>", "BackboneA"),
                correlationId: "corr-publish-failure",
                replyTo: "rpc.responses",
                deliveryTag: 913,
                connectionGeneration: 42,
                settlement: settlement);

            ArticleWorkProcessingResult result = CreateResult(
                delivery,
                outcome: ArticleWorkProcessingOutcome.Success,
                requestId: Guid.NewGuid(),
                messageId: "<publish-failure@example.com>",
                backbone: "BackboneA");

            TrackingResponsePublisher publisher = new(RabbitMqResponsePublishStatus.Failed);
            RabbitMqArticleResultSink sink = CreateSink(responsePublisher: publisher);

            await sink.OnProcessedAsync(result, CancellationToken.None);

            Assert.Null(settlement.AckDeliveryTag);
            Assert.Equal(913UL, settlement.NackDeliveryTag);
            Assert.True(settlement.NackRequeue);
        }
        /// <summary>
        /// Verifies the OnProcessedAsync_WhenPublishTimesOut_DoesNotAckAndNacksRequeueTrueAsync scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task OnProcessedAsync_WhenPublishTimesOut_DoesNotAckAndNacksRequeueTrueAsync()
        {
            TrackingDeliverySettlement settlement = new();
            RabbitMqArticleDelivery delivery = CreateDelivery(
                payloadText: CreateValidJsonPayload(Guid.NewGuid(), "<publish-timeout@example.com>", "BackboneA"),
                correlationId: "corr-publish-timeout",
                replyTo: "rpc.responses",
                deliveryTag: 914,
                connectionGeneration: 43,
                settlement: settlement);

            ArticleWorkProcessingResult result = CreateResult(
                delivery,
                outcome: ArticleWorkProcessingOutcome.Success,
                requestId: Guid.NewGuid(),
                messageId: "<publish-timeout@example.com>",
                backbone: "BackboneA");

            TrackingResponsePublisher publisher = new(RabbitMqResponsePublishStatus.TimedOut);
            RabbitMqArticleResultSink sink = CreateSink(responsePublisher: publisher);

            await sink.OnProcessedAsync(result, CancellationToken.None).ConfigureAwait(false);

            Assert.Null(settlement.AckDeliveryTag);
            Assert.Equal(914UL, settlement.NackDeliveryTag);
            Assert.True(settlement.NackRequeue);
        }
        /// <summary>
        /// Verifies the OnProcessedAsync_WhenArticleNotFound_NacksWithoutRequeueAsync scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task OnProcessedAsync_WhenArticleNotFound_NacksWithoutRequeueAsync()
        {
            TrackingDeliverySettlement settlement = new();
            RabbitMqArticleDelivery delivery = CreateDelivery(
                payloadText: CreateValidJsonPayload(Guid.NewGuid(), "<notfound@example.com>", "BackboneA"),
                correlationId: "corr-notfound",
                replyTo: "rpc.responses",
                deliveryTag: 1001,
                connectionGeneration: 44,
                settlement: settlement);

            ArticleWorkProcessingResult result = CreateResult(
                delivery,
                outcome: ArticleWorkProcessingOutcome.ArticleNotFound,
                requestId: Guid.NewGuid(),
                messageId: "<notfound@example.com>",
                backbone: "BackboneA");

            TrackingResponsePublisher publisher = new(RabbitMqResponsePublishStatus.Confirmed);
            RabbitMqArticleResultSink sink = CreateSink(responsePublisher: publisher);

            await sink.OnProcessedAsync(result, CancellationToken.None);

            Assert.Equal(1001UL, settlement.NackDeliveryTag);
            Assert.False(settlement.NackRequeue);
            Assert.Equal(1, publisher.PublishCallCount);
            RabbitMqArticleWorkResponse response = RabbitMqArticleWorkResponseWireProtocol.ParseV1(publisher.LastResponsePayload!);
            Assert.Equal(nameof(ArticleWorkProcessingOutcome.ArticleNotFound), response.Outcome);
            Assert.Null(response.Uri);
            Assert.False(string.IsNullOrWhiteSpace(response.Error));
        }
        /// <summary>
        /// Verifies the OnProcessedAsync_WhenInvalidArticle_NacksWithoutRequeueAsync scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task OnProcessedAsync_WhenInvalidArticle_NacksWithoutRequeueAsync()
        {
            TrackingDeliverySettlement settlement = new();
            RabbitMqArticleDelivery delivery = CreateDelivery(
                payloadText: CreateValidJsonPayload(Guid.NewGuid(), "<invalid-article@example.com>", "BackboneA"),
                correlationId: "corr-invalid-article",
                replyTo: "rpc.responses",
                deliveryTag: 1002,
                connectionGeneration: 45,
                settlement: settlement);

            ArticleWorkProcessingResult result = CreateResult(
                delivery,
                outcome: ArticleWorkProcessingOutcome.InvalidArticle,
                requestId: Guid.NewGuid(),
                messageId: "<invalid-article@example.com>",
                backbone: "BackboneA");

            TrackingResponsePublisher publisher = new(RabbitMqResponsePublishStatus.Confirmed);
            RabbitMqArticleResultSink sink = CreateSink(responsePublisher: publisher);

            await sink.OnProcessedAsync(result, CancellationToken.None);

            Assert.Equal(1002UL, settlement.NackDeliveryTag);
            Assert.False(settlement.NackRequeue);
            Assert.Equal(1, publisher.PublishCallCount);
            RabbitMqArticleWorkResponse response = RabbitMqArticleWorkResponseWireProtocol.ParseV1(publisher.LastResponsePayload!);
            Assert.Equal(nameof(ArticleWorkProcessingOutcome.InvalidArticle), response.Outcome);
            Assert.Null(response.Uri);
            Assert.False(string.IsNullOrWhiteSpace(response.Error));
        }
        /// <summary>
        /// Verifies the OnProcessedAsync_WhenInvalidRequest_NacksWithoutRequeueAsync scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task OnProcessedAsync_WhenInvalidRequest_NacksWithoutRequeueAsync()
        {
            TrackingDeliverySettlement settlement = new();
            RabbitMqArticleDelivery delivery = CreateDelivery(
                payloadText: "{invalid",
                correlationId: "corr-invalid-request",
                replyTo: "rpc.responses",
                deliveryTag: 1003,
                connectionGeneration: 46,
                settlement: settlement);

            ArticleWorkProcessingResult result = CreateResult(
                delivery,
                outcome: ArticleWorkProcessingOutcome.InvalidRequest,
                requestId: Guid.Empty,
                messageId: string.Empty,
                backbone: "BackboneA");

            TrackingResponsePublisher publisher = new(RabbitMqResponsePublishStatus.Confirmed);
            RabbitMqArticleResultSink sink = CreateSink(responsePublisher: publisher);

            await sink.OnProcessedAsync(result, CancellationToken.None);

            Assert.Equal(1003UL, settlement.NackDeliveryTag);
            Assert.False(settlement.NackRequeue);
            Assert.Equal(1, publisher.PublishCallCount);
            RabbitMqArticleWorkResponse response = RabbitMqArticleWorkResponseWireProtocol.ParseV1(publisher.LastResponsePayload!);
            Assert.Equal(nameof(ArticleWorkProcessingOutcome.InvalidRequest), response.Outcome);
            Assert.Null(response.Uri);
            Assert.False(string.IsNullOrWhiteSpace(response.Error));
        }
        /// <summary>
        /// Verifies the OnProcessedAsync_WhenProviderFailure_NacksWithRequeueAsync scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task OnProcessedAsync_WhenProviderFailure_NacksWithRequeueAsync()
        {
            TrackingDeliverySettlement settlement = new();
            RabbitMqArticleDelivery delivery = CreateDelivery(
                payloadText: CreateValidJsonPayload(Guid.NewGuid(), "<provider-failure@example.com>", "BackboneA"),
                correlationId: "corr-provider-failure",
                replyTo: "rpc.responses",
                deliveryTag: 1004,
                connectionGeneration: 47,
                settlement: settlement);

            ArticleWorkProcessingResult result = CreateResult(
                delivery,
                outcome: ArticleWorkProcessingOutcome.ProviderFailure,
                requestId: Guid.NewGuid(),
                messageId: "<provider-failure@example.com>",
                backbone: "BackboneA");

            TrackingResponsePublisher publisher = new(RabbitMqResponsePublishStatus.Confirmed);
            RabbitMqArticleResultSink sink = CreateSink(responsePublisher: publisher);

            await sink.OnProcessedAsync(result, CancellationToken.None);

            Assert.Equal(1004UL, settlement.NackDeliveryTag);
            Assert.True(settlement.NackRequeue);
            Assert.Equal(0, publisher.PublishCallCount);
        }
        /// <summary>
        /// Verifies the OnProcessedAsync_WhenCancelled_NacksWithRequeueAndDoesNotAckAsync scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task OnProcessedAsync_WhenCancelled_NacksWithRequeueAndDoesNotAckAsync()
        {
            TrackingDeliverySettlement settlement = new();
            RabbitMqArticleDelivery delivery = CreateDelivery(
                payloadText: CreateValidJsonPayload(Guid.NewGuid(), "<cancelled@example.com>", "BackboneA"),
                correlationId: "corr-cancelled",
                replyTo: "rpc.responses",
                deliveryTag: 1005,
                connectionGeneration: 48,
                settlement: settlement);

            ArticleWorkProcessingResult result = CreateResult(
                delivery,
                outcome: ArticleWorkProcessingOutcome.Cancelled,
                requestId: Guid.NewGuid(),
                messageId: "<cancelled@example.com>",
                backbone: "BackboneA");

            RabbitMqArticleResultSink sink = CreateSink(responsePublisher: new TrackingResponsePublisher(RabbitMqResponsePublishStatus.Confirmed));

            await sink.OnProcessedAsync(result, CancellationToken.None).ConfigureAwait(false);

            Assert.Null(settlement.AckDeliveryTag);
            Assert.Equal(1005UL, settlement.NackDeliveryTag);
            Assert.True(settlement.NackRequeue);
        }
        /// <summary>
        /// Verifies the OnProcessedAsync_WhenUnexpectedFailure_NacksWithRequeueAndDoesNotAckAsync scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task OnProcessedAsync_WhenUnexpectedFailure_NacksWithRequeueAndDoesNotAckAsync()
        {
            TrackingDeliverySettlement settlement = new();
            RabbitMqArticleDelivery delivery = CreateDelivery(
                payloadText: CreateValidJsonPayload(Guid.NewGuid(), "<unexpected@example.com>", "BackboneA"),
                correlationId: "corr-unexpected",
                replyTo: "rpc.responses",
                deliveryTag: 1006,
                connectionGeneration: 49,
                settlement: settlement);

            ArticleWorkProcessingResult result = CreateResult(
                delivery,
                outcome: ArticleWorkProcessingOutcome.UnexpectedFailure,
                requestId: Guid.NewGuid(),
                messageId: "<unexpected@example.com>",
                backbone: "BackboneA");

            RabbitMqArticleResultSink sink = CreateSink(responsePublisher: new TrackingResponsePublisher(RabbitMqResponsePublishStatus.Confirmed));

            await sink.OnProcessedAsync(result, CancellationToken.None).ConfigureAwait(false);

            Assert.Null(settlement.AckDeliveryTag);
            Assert.Equal(1006UL, settlement.NackDeliveryTag);
            Assert.True(settlement.NackRequeue);
        }
        /// <summary>
        /// Verifies the OnProcessedAsync_WhenSettlementAlreadyAcked_ThrowsOnSecondSettlementAsync scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task OnProcessedAsync_WhenSettlementAlreadyAcked_ThrowsOnSecondSettlementAsync()
        {
            TrackingDeliverySettlement settlement = new();
            RabbitMqArticleDelivery firstDelivery = CreateDelivery(
                payloadText: CreateValidJsonPayload(Guid.NewGuid(), "<exactly-once-1@example.com>", "BackboneA"),
                correlationId: "corr-exactly-once",
                replyTo: "rpc.responses",
                deliveryTag: 1901,
                connectionGeneration: 60,
                settlement: settlement);
            RabbitMqArticleDelivery secondDelivery = firstDelivery with { DeliveryTag = 1902 };

            ArticleWorkProcessingResult firstResult = CreateResult(firstDelivery, ArticleWorkProcessingOutcome.Success, Guid.NewGuid(), "<exactly-once-1@example.com>", "BackboneA");
            ArticleWorkProcessingResult secondResult = CreateResult(secondDelivery, ArticleWorkProcessingOutcome.Success, Guid.NewGuid(), "<exactly-once-2@example.com>", "BackboneA");

            RabbitMqArticleResultSink sink = CreateSink(responsePublisher: new TrackingResponsePublisher(RabbitMqResponsePublishStatus.Confirmed));

            await sink.OnProcessedAsync(firstResult, CancellationToken.None).ConfigureAwait(false);

            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await sink.OnProcessedAsync(secondResult, CancellationToken.None).ConfigureAwait(false)).ConfigureAwait(false);
        }
        /// <summary>
        /// Verifies the OnProcessedAsync_WhenSuccessAndShutdownOccursBeforePublish_DoesNotAckAndNacksWithRequeueAsync scenario and expected contract.
        /// </summary>
        [Fact]
        public async Task OnProcessedAsync_WhenSuccessAndShutdownOccursBeforePublish_DoesNotAckAndNacksWithRequeueAsync()
        {
            TrackingDeliverySettlement settlement = new();
            RabbitMqArticleDelivery delivery = CreateDelivery(
                payloadText: CreateValidJsonPayload(Guid.NewGuid(), "<shutdown-before-publish@example.com>", "BackboneA"),
                correlationId: "corr-shutdown-before-publish",
                replyTo: "rpc.responses",
                deliveryTag: 2001,
                connectionGeneration: 61,
                settlement: settlement);

            ArticleWorkProcessingResult result = CreateResult(
                delivery,
                outcome: ArticleWorkProcessingOutcome.Success,
                requestId: Guid.NewGuid(),
                messageId: "<shutdown-before-publish@example.com>",
                backbone: "BackboneA");

            TrackingResponsePublisher publisher = new(RabbitMqResponsePublishStatus.TimedOut);
            RabbitMqArticleResultSink sink = CreateSink(responsePublisher: publisher);

            await sink.OnProcessedAsync(result, CancellationToken.None).ConfigureAwait(false);

            Assert.Null(settlement.AckDeliveryTag);
            Assert.Equal(2001UL, settlement.NackDeliveryTag);
            Assert.True(settlement.NackRequeue);
        }

        /// <summary>
        /// Verifies the CreateSink scenario and expected contract.
        /// </summary>
        private static RabbitMqArticleResultSink CreateSink(IRabbitMqArticleResponsePublisher responsePublisher)
        {
            return new RabbitMqArticleResultSink(
                planner: new ArticleWorkDispositionPlanner(),
                responseFactory: new ArticleWorkResponseFactory(),
                responsePublisher: responsePublisher,
                logger: NullLogger<RabbitMqArticleResultSink>.Instance);
        }

        /// <summary>
        /// Verifies the CreateResult scenario and expected contract.
        /// </summary>
        private static ArticleWorkProcessingResult CreateResult(
            RabbitMqArticleDelivery delivery,
            ArticleWorkProcessingOutcome outcome,
            Guid requestId,
            string messageId,
            string backbone)
        {
            RabbitMqArticleWorkRequest request = new(1, requestId, messageId, backbone);
            return new ArticleWorkProcessingResult(
                Request: request,
                Delivery: delivery,
                Outcome: outcome,
                Disposition: ArticleWorkDispositionRecommendation.None,
                GrabberResult: null,
                ProviderFailureCode: null,
                ResponseCode: null,
                ResponseText: null,
                UnexpectedException: null);
        }

        /// <summary>
        /// Verifies the CreateDelivery scenario and expected contract.
        /// </summary>
        private static RabbitMqArticleDelivery CreateDelivery(
            string payloadText,
            string backbone = "BackboneA",
            string correlationId = "corr-phase4",
            string replyTo = "rpc.responses",
            ulong deliveryTag = 1,
            long connectionGeneration = 1,
            IRabbitMqDeliverySettlement? settlement = null,
            bool redelivered = false,
            string consumerTag = "ctag-phase4",
            string consumerIdentity = "consumer-phase4")
        {
            settlement ??= new TrackingDeliverySettlement();
            if (settlement is TrackingDeliverySettlement trackingSettlement)
            {
                trackingSettlement.BindDeliveryTag(deliveryTag);
            }

            return new RabbitMqArticleDelivery(
                Backbone: backbone,
                Queue: "grabbers.backbonea",
                ConsumerTag: consumerTag,
                ConsumerIdentity: consumerIdentity,
                DeliveryTag: deliveryTag,
                Redelivered: redelivered,
                RoutingKey: "grabbers.backbonea",
                Exchange: "grabbers.backbonea",
                ConnectionGeneration: connectionGeneration,
                RabbitMqMessageId: "rmq-message-id",
                CorrelationId: correlationId,
                ReplyTo: replyTo,
                Payload: Encoding.UTF8.GetBytes(payloadText),
                CancellationToken: CancellationToken.None,
                Settlement: settlement);
        }

        /// <summary>
        /// Verifies the CreateValidJsonPayload scenario and expected contract.
        /// </summary>
        private static string CreateValidJsonPayload(Guid requestId, string messageId, string backbone)
        {
            return $"{{\"version\":1,\"requestId\":\"{requestId}\",\"messageId\":\"{messageId}\",\"backbone\":\"{backbone}\"}}";
        }

        /// <summary>
        /// Documents the TrackingResponsePublisher test type and its protected contract.
        /// </summary>
        private sealed class TrackingResponsePublisher : IRabbitMqArticleResponsePublisher
        {
            /// <summary>
            /// Stores the _status fixture value used by these tests.
            /// </summary>
            private readonly RabbitMqResponsePublishStatus _status;
            /// <summary>
            /// Stores the _sharedOperationLog fixture value used by these tests.
            /// </summary>
            private readonly List<string>? _sharedOperationLog;

            /// <summary>
            /// Verifies the TrackingResponsePublisher scenario and expected contract.
            /// </summary>
            internal TrackingResponsePublisher(RabbitMqResponsePublishStatus status, List<string>? sharedOperationLog = null)
            {
                _status = status;
                _sharedOperationLog = sharedOperationLog;
            }

            /// <summary>
            /// Stores the PublishCallCount value used by this test fixture.
            /// </summary>
            internal int PublishCallCount { get; private set; }

            /// <summary>
            /// Stores the LastRoutingKey value used by this test fixture.
            /// </summary>
            internal string? LastRoutingKey { get; private set; }

            /// <summary>
            /// Stores the LastCorrelationId value used by this test fixture.
            /// </summary>
            internal string? LastCorrelationId { get; private set; }

            /// <summary>
            /// Stores the LastResponseJson value used by this test fixture.
            /// </summary>
            internal string? LastResponseJson { get; private set; }

            /// <summary>
            /// Stores the LastResponsePayload value used by this test fixture.
            /// </summary>
            internal byte[]? LastResponsePayload { get; private set; }

            /// <summary>
            /// Stores the OperationLog value used by this test fixture.
            /// </summary>
            internal List<string> OperationLog { get; } = [];

            /// <summary>
            /// Verifies the PublishAndConfirmAsync scenario and expected contract.
            /// </summary>
            public ValueTask<RabbitMqResponsePublishResult> PublishAndConfirmAsync(
                ArticleWorkProcessingResult result,
                RabbitMqArticleWorkResponse response,
                CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                PublishCallCount++;
                LastRoutingKey = result.ReplyTo;
                LastCorrelationId = result.CorrelationId;
                LastResponsePayload = RabbitMqArticleWorkResponseWireProtocol.SerializeV1(response);
                LastResponseJson = Encoding.UTF8.GetString(LastResponsePayload);
                OperationLog.Add("publish");
                _sharedOperationLog?.Add("publish");

                if (_status == RabbitMqResponsePublishStatus.Confirmed)
                {
                    OperationLog.Add("confirm");
                    _sharedOperationLog?.Add("confirm");
                }

                return ValueTask.FromResult(new RabbitMqResponsePublishResult(_status, result.Delivery.ConnectionGeneration, null));
            }
        }

        /// <summary>
        /// Documents the TrackingDeliverySettlement test type and its protected contract.
        /// </summary>
        private sealed class TrackingDeliverySettlement : IRabbitMqDeliverySettlement
        {
            /// <summary>
            /// Stores the _sharedOperationLog fixture value used by these tests.
            /// </summary>
            private readonly List<string>? _sharedOperationLog;
            /// <summary>
            /// Stores the _settled fixture value used by these tests.
            /// </summary>
            private int _settled;
            /// <summary>
            /// Stores the _deliveryTag fixture value used by these tests.
            /// </summary>
            private ulong _deliveryTag;

            /// <summary>
            /// Verifies the TrackingDeliverySettlement scenario and expected contract.
            /// </summary>
            internal TrackingDeliverySettlement(List<string>? sharedOperationLog = null)
            {
                _sharedOperationLog = sharedOperationLog;
            }

            /// <summary>
            /// Stores the AckDeliveryTag value used by this test fixture.
            /// </summary>
            internal ulong? AckDeliveryTag { get; private set; }

            /// <summary>
            /// Stores the NackDeliveryTag value used by this test fixture.
            /// </summary>
            internal ulong? NackDeliveryTag { get; private set; }

            /// <summary>
            /// Stores the NackRequeue value used by this test fixture.
            /// </summary>
            internal bool NackRequeue { get; private set; }

            /// <summary>
            /// Stores the OperationLog value used by this test fixture.
            /// </summary>
            internal List<string> OperationLog { get; } = [];

            /// <summary>
            /// Verifies the AckAsync scenario and expected contract.
            /// </summary>
            public ValueTask AckAsync(CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (Interlocked.Exchange(ref _settled, 1) != 0)
                {
                    throw new InvalidOperationException("Delivery already settled.");
                }

                AckDeliveryTag = _deliveryTag;
                OperationLog.Add("ack");
                _sharedOperationLog?.Add("ack");
                return ValueTask.CompletedTask;
            }

            /// <summary>
            /// Verifies the NackAsync scenario and expected contract.
            /// </summary>
            public ValueTask NackAsync(bool requeue, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (Interlocked.Exchange(ref _settled, 1) != 0)
                {
                    throw new InvalidOperationException("Delivery already settled.");
                }

                NackDeliveryTag = _deliveryTag;
                NackRequeue = requeue;
                OperationLog.Add("nack");
                _sharedOperationLog?.Add("nack");
                return ValueTask.CompletedTask;
            }

            /// <summary>
            /// Verifies the BindDeliveryTag scenario and expected contract.
            /// </summary>
            internal void BindDeliveryTag(ulong deliveryTag)
            {
                _deliveryTag = deliveryTag;
            }
        }
    }
}
