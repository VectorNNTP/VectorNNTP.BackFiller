// <copyright file="RabbitMqArticleResultSink.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Processing
// Executes RabbitMQ RPC response publishing and final ACK/NACK settlement for processed
// article-work deliveries.

using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Parsing;
using VectorNNTP.Backfiller.Runtime.Articles.Retention;
using VectorNNTP.Backfiller.Runtime.Transit;

namespace VectorNNTP.Backfiller.Runtime.Articles.Processing
{
    /// <summary>
    /// Executes Phase 4 RPC publication and broker settlement for processed article-work results.
    /// </summary>
    /// <remarks>
    /// When a terminal response is required, the sink waits for broker confirmation before acknowledging or dropping the delivery. The result object is always disposed when processing completes.
    /// </remarks>
    internal sealed partial class RabbitMqArticleResultSink : IArticleWorkResultSink
    {
        /// <summary>
        /// Planner that maps processing results to broker settlement and response-publication requirements.
        /// </summary>
        private readonly IArticleWorkDispositionPlanner _planner;
        /// <summary>
        /// Factory that produces terminal JSON response payloads for caller-visible outcomes.
        /// </summary>
        private readonly IArticleWorkResponseFactory _responseFactory;
        /// <summary>
        /// Publisher that emits response payloads to RabbitMQ and waits for broker confirmation.
        /// </summary>
        private readonly IRabbitMqArticleResponsePublisher _responsePublisher;
        /// <summary>
        /// Shared in-memory retention authority used to admit successful payload ownership before response publication.
        /// </summary>
        private readonly IArticleRetentionAuthority _retentionAuthority;
        /// <summary>
        /// Transit admission gateway used to transfer Message-ID responsibility into bounded transit ownership.
        /// </summary>
        private readonly ITransitAdmissionGateway _transitAdmissionGateway;
        /// <summary>
        /// Logger that records retention-admission, response-publication, and broker-settlement outcomes.
        /// </summary>
        private readonly ILogger<RabbitMqArticleResultSink> _logger;

        /// <summary>
        /// Initializes a sink that owns final response-publication and delivery-settlement orchestration.
        /// </summary>
        /// <param name="planner">Planner that maps processing outcomes and cancellation into broker actions.</param>
        /// <param name="responseFactory">Factory that builds terminal RPC response payloads when required.</param>
        /// <param name="responsePublisher">Publisher that emits and confirms RPC responses on RabbitMQ.</param>
        /// <param name="retentionAuthority">Shared in-memory retention authority for successful payload ownership admission.</param>
        /// <param name="transitAdmissionGateway">Gateway that admits Message-ID transit responsibility before RabbitMQ success publication/acknowledgement.</param>
        /// <param name="logger">Logger used for publication fallback and settlement diagnostics.</param>
        public RabbitMqArticleResultSink(
            IArticleWorkDispositionPlanner planner,
            IArticleWorkResponseFactory responseFactory,
            IRabbitMqArticleResponsePublisher responsePublisher,
            IArticleRetentionAuthority retentionAuthority,
            ITransitAdmissionGateway transitAdmissionGateway,
            ILogger<RabbitMqArticleResultSink> logger)
        {
            _planner = planner ?? throw new ArgumentNullException(nameof(planner));
            _responseFactory = responseFactory ?? throw new ArgumentNullException(nameof(responseFactory));
            _responsePublisher = responsePublisher ?? throw new ArgumentNullException(nameof(responsePublisher));
            _retentionAuthority = retentionAuthority ?? throw new ArgumentNullException(nameof(retentionAuthority));
            _transitAdmissionGateway = transitAdmissionGateway ?? throw new ArgumentNullException(nameof(transitAdmissionGateway));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Applies the Phase 4 publication and settlement policy for one completed processing result.
        /// </summary>
        /// <remarks>
        /// <para>On <see cref="ArticleWorkProcessingOutcome.Success"/>, the sink detaches the successful payload owner from <paramref name="result"/>, admits that ownership into retention,
        /// then attempts Transit admission before any success response publication or delivery acknowledgement occurs.</para>
        /// <para>When retention admission is rejected for non-duplicate reasons, the sink reattaches payload ownership when possible, otherwise disposes it, and negatively acknowledges with requeue.
        /// Duplicate Message-ID admission keeps existing retained ownership authoritative and disposes the duplicate payload owner.</para>
        /// <para>If Transit admission is not accepted after retention is available, the sink negatively acknowledges without requeue and does not publish a response.</para>
        /// <para>When response publication is required, RabbitMQ publish/confirm must succeed before final settlement; non-confirmed publication paths negatively acknowledge with requeue.</para>
        /// <para>Successful broker settlement acknowledges only after required prior steps complete. Non-success outcomes follow the disposition plan for ACK/NACK and requeue behavior.</para>
        /// <para>All paths guarantee cleanup in <c>finally</c>: any detached payload owner not transferred is disposed, and <paramref name="result"/> is disposed exactly once.</para>
        /// <para>Cancellation is observed through the supplied token and is also reflected in the disposition plan used for final settlement behavior.</para>
        /// </remarks>
        /// <param name="result">Completed result whose response publication and broker settlement must now be finalized.</param>
        /// <param name="cancellationToken">Cancellation token for response publication and settlement operations.</param>
        /// <returns>A value task that completes after the result has been published or settled according to policy.</returns>
        public async ValueTask OnProcessedAsync(ArticleWorkProcessingResult result, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(result);

            RabbitMqDispositionPlan plan = _planner.CreatePlan(result, cancellationToken);
            DownloadedArticleBuffer? detachedPayloadOwner = null;

            try
            {
                if (result.Outcome is ArticleWorkProcessingOutcome.Success)
                {
                    NntpArticleParseResult parseResult = result.GrabberResult?.Success?.Parse
                        ?? throw new InvalidOperationException("Successful article processing result did not provide parser metadata required for canonical materialization.");

                    if (!MessageIdMatchesRequestIdentity(parseResult, result.Request.MessageId))
                    {
                        await result.Delivery.Settlement.NackAsync(requeue: false, cancellationToken).ConfigureAwait(false);
                        LogRabbitMqArticleMessageIdMismatchRejected(
                            _logger,
                            result.Request.RequestId,
                            result.CorrelationId,
                            result.Request.MessageId,
                            result.Request.Backbone,
                            result.Delivery.DeliveryTag);
                        return;
                    }

                    DownloadedArticleBuffer originalPayloadOwner = result.TryDetachSuccessfulPayloadOwner()
                        ?? throw new InvalidOperationException("Successful article processing result did not provide a retained payload owner for admission.");
                    detachedPayloadOwner = originalPayloadOwner;

                    DownloadedArticleBuffer payloadOwner = NntpArticleCanonicalMaterializer.Materialize(parseResult);
                    originalPayloadOwner.Dispose();
                    detachedPayloadOwner = payloadOwner;

                    ArticleRetentionAdmissionResult retentionAdmissionResult = _retentionAuthority.TryRetainSuccessArticle(result.Request.MessageId, payloadOwner);
                    bool retainedAvailableForTransit = retentionAdmissionResult.Status switch
                    {
                        ArticleRetentionAdmissionStatus.Admitted => true,
                        ArticleRetentionAdmissionStatus.DuplicateMessageId => true,
                        ArticleRetentionAdmissionStatus.AdmissionClosed => false,
                        ArticleRetentionAdmissionStatus.Md5Collision => false,
                        ArticleRetentionAdmissionStatus.PayloadExceedsCapacity => false,
                        ArticleRetentionAdmissionStatus.CapacityUnavailable => false,
                        ArticleRetentionAdmissionStatus.InvalidPayload => false,
                        _ => false,
                    };

                    if (retainedAvailableForTransit)
                    {
                        if (retentionAdmissionResult.Status is ArticleRetentionAdmissionStatus.DuplicateMessageId)
                        {
                            payloadOwner.Dispose();
                        }

                        detachedPayloadOwner = null;

                        TransitAdmissionResult transitAdmissionResult = await _transitAdmissionGateway
                            .AdmitAsync(result.Request.MessageId, cancellationToken)
                            .ConfigureAwait(false);

                        if (!transitAdmissionResult.IsAccepted)
                        {
                            CancellationToken settlementToken = SelectTransitRejectionSettlementToken(
                                admissionStatus: transitAdmissionResult.Status,
                                processingToken: cancellationToken,
                                deliveryToken: result.Delivery.CancellationToken);
                            await result.Delivery.Settlement.NackAsync(requeue: false, settlementToken).ConfigureAwait(false);
                            if (transitAdmissionResult.Status is TransitAdmissionStatus.Failed or TransitAdmissionStatus.Unavailable)
                            {
                                LogRabbitMqTransitAdmissionRejectedDropWarning(
                                    _logger,
                                    result.Request.RequestId,
                                    result.CorrelationId,
                                    result.Request.MessageId,
                                    result.Request.Backbone,
                                    result.Delivery.DeliveryTag,
                                    transitAdmissionResult.Status,
                                    transitAdmissionResult.Error);
                            }
                            else
                            {
                                LogRabbitMqTransitAdmissionRejectedDropInformation(
                                    _logger,
                                    result.Request.RequestId,
                                    result.CorrelationId,
                                    result.Request.MessageId,
                                    result.Request.Backbone,
                                    result.Delivery.DeliveryTag,
                                    transitAdmissionResult.Status,
                                    transitAdmissionResult.Error);
                            }

                            return;
                        }
                    }
                    else
                    {
                        if (!result.TryAttachSuccessfulPayloadOwner(payloadOwner))
                        {
                            payloadOwner.Dispose();
                        }

                        detachedPayloadOwner = null;
                        await result.Delivery.Settlement.NackAsync(requeue: true, cancellationToken).ConfigureAwait(false);
                        LogRabbitMqRetentionAdmissionFailedRequeue(
                            _logger,
                            result.Request.RequestId,
                            result.CorrelationId,
                            result.Request.MessageId,
                            result.Request.Backbone,
                            retentionAdmissionResult.Status);
                        return;
                    }
                }

                if (plan.PublishResponse)
                {
                    RabbitMqArticleWorkResponse response = _responseFactory.CreateResponse(result)
                        ?? throw new InvalidOperationException("Response publication was required but no response payload was produced.");

                    RabbitMqResponsePublishResult publishResult = await _responsePublisher
                        .PublishAndConfirmAsync(result, response, cancellationToken)
                        .ConfigureAwait(false);

                    if (publishResult.Status is not RabbitMqResponsePublishStatus.Confirmed)
                    {
                        await result.Delivery.Settlement.NackAsync(requeue: true, cancellationToken).ConfigureAwait(false);
                        LogRabbitMqResponsePublishNotConfirmedRequeue(
                            _logger,
                            result.Request.RequestId,
                            result.CorrelationId,
                            result.Request.MessageId,
                            result.Request.Backbone,
                            publishResult.Status);
                        return;
                    }
                }

                if (plan.Action is RabbitMqDispositionAction.Ack)
                {
                    await result.Delivery.Settlement.AckAsync(cancellationToken).ConfigureAwait(false);
                    LogRabbitMqDeliveryAcknowledged(
                        _logger,
                        result.Request.RequestId,
                        result.CorrelationId,
                        result.Request.MessageId,
                        result.Request.Backbone,
                        result.Delivery.DeliveryTag);
                }
                else
                {
                    await result.Delivery.Settlement.NackAsync(plan.Requeue, cancellationToken).ConfigureAwait(false);
                    LogRabbitMqDeliveryNegativelyAcknowledged(
                        _logger,
                        result.Request.RequestId,
                        result.CorrelationId,
                        result.Request.MessageId,
                        result.Request.Backbone,
                        result.Delivery.DeliveryTag,
                        plan.Requeue);
                }
            }
            finally
            {
                detachedPayloadOwner?.Dispose();
                result.Dispose();
            }
        }

        private static bool MessageIdMatchesRequestIdentity(NntpArticleParseResult parseResult, string requestedMessageId)
        {
            ReadOnlySpan<byte> parsedMessageId = parseResult.OriginalMessageIdValue.Span;
            if (parsedMessageId.Length != requestedMessageId.Length)
            {
                return false;
            }

            for (int i = 0; i < parsedMessageId.Length; i++)
            {
                if (parsedMessageId[i] != requestedMessageId[i])
                {
                    return false;
                }
            }

            return true;
        }

        private static CancellationToken SelectTransitRejectionSettlementToken(
            TransitAdmissionStatus admissionStatus,
            CancellationToken processingToken,
            CancellationToken deliveryToken)
        {
            if (admissionStatus != TransitAdmissionStatus.Canceled)
            {
                return processingToken;
            }

            if (!deliveryToken.IsCancellationRequested)
            {
                return deliveryToken;
            }

            return processingToken;
        }

        /// <summary>
        /// Emits an invalid-article drop event when the parsed article Message-ID does not match the requested Message-ID identity.
        /// </summary>
        /// <param name="logger">Logger receiving the mismatch rejection event.</param>
        /// <param name="requestId">Phase 3 request identifier associated with the completed work item.</param>
        /// <param name="correlationId">AMQP correlation identifier copied from the delivery when one is available.</param>
        /// <param name="requestedMessageId">Requested canonical Message-ID identity from the RabbitMQ work request.</param>
        /// <param name="backbone">Backbone name for the retrieval target used for the request.</param>
        /// <param name="deliveryTag">RabbitMQ delivery tag negatively acknowledged by the broker.</param>
        [LoggerMessage(
            EventId = 3408,
            Level = LogLevel.Warning,
            Message = "RabbitMQ success-path article identity mismatch rejected. RequestId={RequestId} CorrelationId={CorrelationId} RequestedMessageId={RequestedMessageId} Backbone={Backbone} DeliveryTag={DeliveryTag}")]
        private static partial void LogRabbitMqArticleMessageIdMismatchRejected(
            ILogger logger,
            Guid requestId,
            string? correlationId,
            string requestedMessageId,
            string backbone,
            ulong deliveryTag);

        /// <summary>
        /// Emits the retention-admission-failed requeue log event when successful payload ownership could not be admitted.
        /// </summary>
        /// <param name="logger">Logger receiving the requeue event.</param>
        /// <param name="requestId">Phase 3 request identifier associated with the completed work item.</param>
        /// <param name="correlationId">AMQP correlation identifier copied from the delivery when one is available.</param>
        /// <param name="messageId">Canonical Message-ID associated with the processed article.</param>
        /// <param name="backbone">Backbone name for the retrieval target used for the request.</param>
        /// <param name="admissionStatus">Retention admission status that forced requeue semantics.</param>
        [LoggerMessage(
            EventId = 3409,
            Level = LogLevel.Warning,
            Message = "RabbitMQ success-path retention admission failed; request will be requeued. RequestId={RequestId} CorrelationId={CorrelationId} MessageId={MessageId} Backbone={Backbone} AdmissionStatus={AdmissionStatus}")]
        private static partial void LogRabbitMqRetentionAdmissionFailedRequeue(
            ILogger logger,
            Guid requestId,
            string? correlationId,
            string messageId,
            string backbone,
            ArticleRetentionAdmissionStatus admissionStatus);

        /// <summary>
        /// Emits the response publish-not-confirmed requeue log event when RabbitMQ refuses confirmation and the delivery must be requeued.
        /// </summary>
        /// <param name="logger">Logger receiving the requeue event.</param>
        /// <param name="requestId">Phase 3 request identifier associated with the completed work item.</param>
        /// <param name="correlationId">AMQP correlation identifier copied from the delivery when one is available.</param>
        /// <param name="messageId">Canonical Message-ID associated with the processed article.</param>
        /// <param name="backbone">Backbone name for the retrieval target used for the request.</param>
        /// <param name="publishStatus">Terminal publish status that caused the requeue decision.</param>
        [LoggerMessage(
            EventId = 3403,
            Level = LogLevel.Warning,
            Message = "RabbitMQ response publish was not confirmed; request will be requeued. RequestId={RequestId} CorrelationId={CorrelationId} MessageId={MessageId} Backbone={Backbone} PublishStatus={PublishStatus}")]
        private static partial void LogRabbitMqResponsePublishNotConfirmedRequeue(
            ILogger logger,
            Guid requestId,
            string? correlationId,
            string messageId,
            string backbone,
            RabbitMqResponsePublishStatus publishStatus);

        /// <summary>
        /// Emits a warning when transit admission rejects a retained success-path article and the delivery is dropped without requeue.
        /// </summary>
        /// <param name="logger">Logger receiving the transit-admission rejection warning.</param>
        /// <param name="requestId">Phase 3 request identifier associated with the completed work item.</param>
        /// <param name="correlationId">AMQP correlation identifier copied from the delivery when one is available.</param>
        /// <param name="messageId">Canonical Message-ID associated with the processed article.</param>
        /// <param name="backbone">Backbone name for the retrieval target used for the request.</param>
        /// <param name="deliveryTag">RabbitMQ delivery tag negatively acknowledged by the broker.</param>
        /// <param name="admissionStatus">Transit admission status that rejected ownership transfer.</param>
        /// <param name="admissionError">Transit admission error detail when available.</param>
        /// <param name="requeue">Whether the broker was instructed to requeue the delivery.</param>
        [LoggerMessage(
            EventId = 3406,
            Level = LogLevel.Warning,
            Message = "Transit admission rejected retained success-path article; dropping RabbitMQ delivery with requeue=false. RequestId={RequestId} CorrelationId={CorrelationId} MessageId={MessageId} Backbone={Backbone} DeliveryTag={DeliveryTag} TransitAdmissionStatus={AdmissionStatus} TransitAdmissionError={AdmissionError} Requeue={Requeue}")]
        private static partial void LogRabbitMqTransitAdmissionRejectedDropWarning(
            ILogger logger,
            Guid requestId,
            string? correlationId,
            string messageId,
            string backbone,
            ulong deliveryTag,
            TransitAdmissionStatus admissionStatus,
            string? admissionError,
            bool requeue = false);

        /// <summary>
        /// Emits an informational transit-admission rejection event for shutdown-expected non-accepted outcomes.
        /// </summary>
        /// <param name="logger">Logger receiving the transit-admission rejection informational event.</param>
        /// <param name="requestId">Phase 3 request identifier associated with the completed work item.</param>
        /// <param name="correlationId">AMQP correlation identifier copied from the delivery when one is available.</param>
        /// <param name="messageId">Canonical Message-ID associated with the processed article.</param>
        /// <param name="backbone">Backbone name for the retrieval target used for the request.</param>
        /// <param name="deliveryTag">RabbitMQ delivery tag negatively acknowledged by the broker.</param>
        /// <param name="admissionStatus">Transit admission status that rejected ownership transfer.</param>
        /// <param name="admissionError">Transit admission error detail when available.</param>
        /// <param name="requeue">Whether the broker was instructed to requeue the delivery.</param>
        [LoggerMessage(
            EventId = 3407,
            Level = LogLevel.Information,
            Message = "Transit admission rejected retained success-path article; dropping RabbitMQ delivery with requeue=false. RequestId={RequestId} CorrelationId={CorrelationId} MessageId={MessageId} Backbone={Backbone} DeliveryTag={DeliveryTag} TransitAdmissionStatus={AdmissionStatus} TransitAdmissionError={AdmissionError} Requeue={Requeue}")]
        private static partial void LogRabbitMqTransitAdmissionRejectedDropInformation(
            ILogger logger,
            Guid requestId,
            string? correlationId,
            string messageId,
            string backbone,
            ulong deliveryTag,
            TransitAdmissionStatus admissionStatus,
            string? admissionError,
            bool requeue = false);

        /// <summary>
        /// Emits the RabbitMQ delivery acknowledged log event after the delivery has been settled successfully.
        /// </summary>
        /// <param name="logger">Logger receiving the acknowledgement event.</param>
        /// <param name="requestId">Phase 3 request identifier associated with the completed work item.</param>
        /// <param name="correlationId">AMQP correlation identifier copied from the delivery when one is available.</param>
        /// <param name="messageId">Canonical Message-ID associated with the processed article.</param>
        /// <param name="backbone">Backbone name for the retrieval target used for the request.</param>
        /// <param name="deliveryTag">RabbitMQ delivery tag acknowledged by the broker.</param>
        [LoggerMessage(
            EventId = 3404,
            Level = LogLevel.Information,
            Message = "RabbitMQ delivery acknowledged. RequestId={RequestId} CorrelationId={CorrelationId} MessageId={MessageId} Backbone={Backbone} DeliveryTag={DeliveryTag}")]
        private static partial void LogRabbitMqDeliveryAcknowledged(
            ILogger logger,
            Guid requestId,
            string? correlationId,
            string messageId,
            string backbone,
            ulong deliveryTag);

        /// <summary>
        /// Emits the RabbitMQ delivery negatively acknowledged log event after the delivery has been settled unsuccessfully.
        /// </summary>
        /// <param name="logger">Logger receiving the negative-acknowledgement event.</param>
        /// <param name="requestId">Phase 3 request identifier associated with the completed work item.</param>
        /// <param name="correlationId">AMQP correlation identifier copied from the delivery when one is available.</param>
        /// <param name="messageId">Canonical Message-ID associated with the processed article.</param>
        /// <param name="backbone">Backbone name for the retrieval target used for the request.</param>
        /// <param name="deliveryTag">RabbitMQ delivery tag negatively acknowledged by the broker.</param>
        /// <param name="requeue">Whether the broker was instructed to requeue the delivery.</param>
        [LoggerMessage(
            EventId = 3405,
            Level = LogLevel.Information,
            Message = "RabbitMQ delivery negatively acknowledged. RequestId={RequestId} CorrelationId={CorrelationId} MessageId={MessageId} Backbone={Backbone} DeliveryTag={DeliveryTag} Requeue={Requeue}")]
        private static partial void LogRabbitMqDeliveryNegativelyAcknowledged(
            ILogger logger,
            Guid requestId,
            string? correlationId,
            string messageId,
            string backbone,
            ulong deliveryTag,
            bool requeue);
    }
}
