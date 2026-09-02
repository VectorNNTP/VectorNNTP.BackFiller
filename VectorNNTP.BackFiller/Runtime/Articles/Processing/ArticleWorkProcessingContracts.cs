// <copyright file="ArticleWorkProcessingContracts.cs" company="Usenet Ninja">
// Copyright © Chris Knipe <cknipe@opticnetworks.net>
// </copyright>
//
// VectorNNTP.Backfiller Runtime / Articles / Processing
// Phase 3 contracts for RabbitMQ delivery-to-NNTP ARTICLE processing, deterministic outcome
// classification, and deferred ACK/NACK disposition guidance boundaries.

using System.Threading.Channels;
using VectorNNTP.Backfiller.Runtime.Articles.Acquisition;
using VectorNNTP.Backfiller.Runtime.Articles.Grabber;
using VectorNNTP.Backfiller.Runtime.RabbitMq;

namespace VectorNNTP.Backfiller.Runtime.Articles.Processing
{
    /// <summary>
    /// Represents one parsed application-level article-work request from the JSON payload body.
    /// </summary>
    /// <param name="Version">Application-level article-work request schema version.</param>
    /// <param name="RequestId">Application request identifier that remains stable across redelivery.</param>
    /// <param name="MessageId">Canonical NNTP Message-ID requested by work payload.</param>
    /// <param name="Backbone">Backbone namespace declared by payload and validated against delivery queue context.</param>
    internal sealed record RabbitMqArticleWorkRequest(
        int Version,
        Guid RequestId,
        string MessageId,
        string Backbone);

    /// <summary>
    /// Represents deterministic high-level terminal outcomes for one article-work request.
    /// </summary>
    internal enum ArticleWorkProcessingOutcome
    {
        /// <summary>
        /// Work request was fulfilled successfully and downstream payload is available.
        /// </summary>
        Success = 0,

        /// <summary>
        /// Work payload was syntactically invalid or missing required Message-ID information.
        /// </summary>
        InvalidRequest = 1,

        /// <summary>
        /// Provider explicitly reported article absence for the requested backbone.
        /// </summary>
        ArticleNotFound = 2,

        /// <summary>
        /// Provider/session/transport operation failed in a transient or connectivity-related manner.
        /// </summary>
        ProviderFailure = 3,

        /// <summary>
        /// Provider returned article payload that failed parser/validation contracts.
        /// </summary>
        InvalidArticle = 4,

        /// <summary>
        /// Work processing was canceled due to application/session shutdown signals.
        /// </summary>
        Cancelled = 5,

        /// <summary>
        /// Work failed for an unexpected reason outside explicitly modeled classifications.
        /// </summary>
        UnexpectedFailure = 6,
    }

    /// <summary>
    /// Represents deferred ACK/NACK guidance emitted by Phase 3 without directly performing RabbitMQ disposition.
    /// </summary>
    internal enum ArticleWorkDispositionRecommendation
    {
        /// <summary>
        /// No disposition action is recommended at this phase boundary.
        /// </summary>
        None = 0,

        /// <summary>
        /// Delivery should be acknowledged as successfully processed.
        /// </summary>
        Ack = 1,

        /// <summary>
        /// Delivery should be negatively acknowledged and requeued for another same-backbone consumer.
        /// </summary>
        NackRequeue = 2,

        /// <summary>
        /// Delivery should be negatively acknowledged without requeue.
        /// </summary>
        NackDrop = 3,
    }

    /// <summary>
    /// Represents one explicit article-work processing result used by RabbitMQ disposition and RPC stages.
    /// </summary>
    /// <param name="Request">Parsed application JSON request payload.</param>
    /// <param name="Delivery">RabbitMQ delivery envelope carrying AMQP RPC and transport metadata.</param>
    /// <param name="Outcome">Deterministic processing outcome classification.</param>
    /// <param name="Disposition">Deferred ACK/NACK recommendation emitted by processing logic.</param>
    /// <param name="GrabberResult">Optional grabber workflow result, including success payload ownership when present.</param>
    /// <param name="ProviderFailureCode">Optional provider acquisition classification for failed outcomes.</param>
    /// <param name="ResponseCode">Optional provider protocol response code.</param>
    /// <param name="ResponseText">Optional provider protocol/local detail text.</param>
    /// <param name="UnexpectedException">Unexpected exception captured for diagnostics when <paramref name="Outcome"/> is <see cref="ArticleWorkProcessingOutcome.UnexpectedFailure"/>.</param>
    internal sealed record ArticleWorkProcessingResult(
        RabbitMqArticleWorkRequest Request,
        RabbitMqArticleDelivery Delivery,
        ArticleWorkProcessingOutcome Outcome,
        ArticleWorkDispositionRecommendation Disposition,
        NntpArticleGrabberResult? GrabberResult,
        NntpArticleAcquisitionFailureCode? ProviderFailureCode,
        int? ResponseCode,
        string? ResponseText,
        Exception? UnexpectedException) : IDisposable
    {
        /// <summary>
        /// Returns the AMQP correlation identifier from the authoritative delivery metadata.
        /// </summary>
        internal string? CorrelationId => Delivery.CorrelationId;

        /// <summary>
        /// Returns the AMQP reply destination from the authoritative delivery metadata.
        /// </summary>
        internal string? ReplyTo => Delivery.ReplyTo;

        /// <summary>
        /// Disposes any owned success payload held by the optional workflow result.
        /// </summary>
        public void Dispose()
        {
            GrabberResult?.Dispose();
        }
    }

    /// <summary>
    /// Parses RabbitMQ delivery envelopes into deterministic article-work requests.
    /// </summary>
    internal interface IRabbitMqArticleWorkRequestParser
    {
        /// <summary>
        /// Parses one delivery into a structured request, or returns an invalid-request processing result.
        /// </summary>
        /// <param name="delivery">Source RabbitMQ delivery envelope.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Parse result containing either request payload or parse-failure classification.</returns>
        public ValueTask<RabbitMqArticleWorkParseResult> ParseAsync(RabbitMqArticleDelivery delivery, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Represents one request-parser output.
    /// </summary>
    /// <param name="Request">Parsed article-work request when parsing succeeded.</param>
    /// <param name="Failure">Processing result for invalid request when parsing failed.</param>
    internal sealed record RabbitMqArticleWorkParseResult(
        RabbitMqArticleWorkRequest? Request,
        ArticleWorkProcessingResult? Failure)
    {
        /// <summary>
        /// Gets a value indicating whether parsing produced a valid request payload.
        /// </summary>
        internal bool IsSuccess => Request is not null;
    }

    /// <summary>
    /// Processes parsed article-work requests using backbone-scoped NNTP retrieval workflows.
    /// </summary>
    internal interface IArticleWorkProcessor
    {
        /// <summary>
        /// Processes one parsed work request and returns a deterministic classification result.
        /// </summary>
        /// <param name="request">Parsed application article-work request.</param>
        /// <param name="delivery">Authoritative RabbitMQ delivery envelope for transport/RPC metadata.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Processing classification and deferred disposition recommendation.</returns>
        public ValueTask<ArticleWorkProcessingResult> ProcessAsync(RabbitMqArticleWorkRequest request, RabbitMqArticleDelivery delivery, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Represents RabbitMQ disposition actions for one processed delivery.
    /// </summary>
    internal enum RabbitMqDispositionAction
    {
        /// <summary>
        /// Acknowledge the delivery.
        /// </summary>
        Ack = 0,

        /// <summary>
        /// Negatively acknowledge the delivery.
        /// </summary>
        Nack = 1,
    }

    /// <summary>
    /// Defines a deterministic RabbitMQ disposition plan for one processed delivery.
    /// </summary>
    /// <param name="Action">RabbitMQ disposition action to apply.</param>
    /// <param name="Requeue">Whether RabbitMQ should requeue the message when <paramref name="Action"/> is <see cref="RabbitMqDispositionAction.Nack"/>.</param>
    /// <param name="PublishResponse">Whether an RPC response must be published before disposition.</param>
    internal sealed record RabbitMqDispositionPlan(
        RabbitMqDispositionAction Action,
        bool Requeue,
        bool PublishResponse);

    /// <summary>
    /// Represents one terminal RabbitMQ RPC response payload.
    /// </summary>
    /// <param name="Version">Application-level response protocol version.</param>
    /// <param name="RequestId">Application request identifier from JSON request payload.</param>
    /// <param name="MessageId">Canonical Message-ID from JSON request payload.</param>
    /// <param name="Backbone">Backbone from JSON request payload.</param>
    /// <param name="Outcome">Terminal outcome string for RPC consumers.</param>
    /// <param name="Uri">Optional retrieval URI when a stable location exists in runtime architecture.</param>
    /// <param name="Error">Optional terminal error detail for non-success terminal outcomes.</param>
    internal sealed record RabbitMqArticleWorkResponse(
        int Version,
        Guid RequestId,
        string MessageId,
        string Backbone,
        string Outcome,
        string? Uri,
        string? Error);

    /// <summary>
    /// Represents RPC response publication confirmation status.
    /// </summary>
    internal enum RabbitMqResponsePublishStatus
    {
        /// <summary>
        /// Response was published and positively confirmed by RabbitMQ.
        /// </summary>
        Confirmed = 0,

        /// <summary>
        /// Response publish operation failed before confirmation.
        /// </summary>
        Failed = 1,

        /// <summary>
        /// Response publish confirmation timed out.
        /// </summary>
        TimedOut = 2,
    }

    /// <summary>
    /// Represents one response publication attempt result.
    /// </summary>
    /// <param name="Status">Publication completion status.</param>
    /// <param name="ConnectionGeneration">Connection generation used for the publish attempt.</param>
    /// <param name="Exception">Optional failure exception when publication failed.</param>
    internal sealed record RabbitMqResponsePublishResult(
        RabbitMqResponsePublishStatus Status,
        long ConnectionGeneration,
        Exception? Exception);

    /// <summary>
    /// Produces terminal RPC response payloads for processed article-work results.
    /// </summary>
    internal interface IArticleWorkResponseFactory
    {
        /// <summary>
        /// Creates a terminal response payload when the processing outcome requires one.
        /// </summary>
        /// <param name="result">Processed result.</param>
        /// <returns>Response payload, or <see langword="null"/> when outcome has no terminal response.</returns>
        public RabbitMqArticleWorkResponse? CreateResponse(ArticleWorkProcessingResult result);
    }

    /// <summary>
    /// Publishes RabbitMQ RPC responses using AMQP CorrelationId/ReplyTo and confirm semantics.
    /// </summary>
    internal interface IRabbitMqArticleResponsePublisher
    {
        /// <summary>
        /// Publishes and confirms one RPC response.
        /// </summary>
        /// <param name="result">Processed source result holding authoritative AMQP metadata.</param>
        /// <param name="response">Response payload to publish.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Publish/confirm attempt result.</returns>
        public ValueTask<RabbitMqResponsePublishResult> PublishAndConfirmAsync(
            ArticleWorkProcessingResult result,
            RabbitMqArticleWorkResponse response,
            CancellationToken cancellationToken);
    }

    /// <summary>
    /// Maps processed outcomes into RabbitMQ disposition plans.
    /// </summary>
    internal interface IArticleWorkDispositionPlanner
    {
        /// <summary>
        /// Creates one RabbitMQ disposition plan for the processed result.
        /// </summary>
        /// <param name="result">Processed result.</param>
        /// <param name="cancellationToken">Cancellation token used to infer shutdown/cancellation semantics.</param>
        /// <returns>Disposition plan.</returns>
        public RabbitMqDispositionPlan CreatePlan(ArticleWorkProcessingResult result, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Receives completed processing results and executes RPC response + RabbitMQ disposition policy.
    /// </summary>
    internal interface IArticleWorkResultSink
    {
        /// <summary>
        /// Accepts one completed processing result.
        /// </summary>
        /// <param name="result">Completed processing result.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public ValueTask OnProcessedAsync(ArticleWorkProcessingResult result, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Default bounded-channel result sink for intra-process processing-result handoff.
    /// </summary>
    internal sealed class ArticleWorkResultChannelSink : IArticleWorkResultSink
    {
        private readonly ChannelWriter<ArticleWorkProcessingResult> _writer;

        /// <summary>
        /// Initializes a channel-backed result sink.
        /// </summary>
        /// <param name="writer">Bounded result writer.</param>
        internal ArticleWorkResultChannelSink(ChannelWriter<ArticleWorkProcessingResult> writer)
        {
            _writer = writer ?? throw new ArgumentNullException(nameof(writer));
        }

        /// <inheritdoc/>
        public ValueTask OnProcessedAsync(ArticleWorkProcessingResult result, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(result);
            cancellationToken.ThrowIfCancellationRequested();
            return _writer.WriteAsync(result, cancellationToken);
        }
    }
}

